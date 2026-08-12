//! Worker implementation on `tokio-postgres`.
//!
//! Each pooled connection still lives on its own OS thread, but the thread
//! runs a current-thread Tokio runtime: the connection's I/O task and the
//! request loop run concurrently on a `LocalSet`, so queries execute without
//! the synchronous wrapper's extra runtime layer. The crate stays
//! runtime-agnostic — callers do not need to be inside a Tokio runtime.

use std::sync::Arc;
use std::thread;

use async_channel::Receiver;
use futures::StreamExt;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

use super::{CachedStatement, postgres_params, postgres_placeholders, postgres_row};
use crate::error::Error;
use crate::pool::{CacheSlots, Request, STATEMENT_CACHE_CAPACITY, STREAM_CHUNK_ROWS, is_ddl};
use crate::query::{QueryResult, Row, Value};

pub(super) fn spawn_worker(
    url: &str,
    request_rx: Receiver<Request>,
    fail_fast: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = url.to_string();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    thread::Builder::new()
        .name("almostsql-postgres-worker".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            };

            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                let (client, connection) = match tokio_postgres::connect(&url, NoTls).await {
                    Ok(pair) => pair,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                // The connection task drives all socket I/O; it must be
                // polled concurrently with the request loop.
                tokio::task::spawn_local(async move {
                    let _ = connection.await;
                });
                let _ = ready_tx.send(Ok(()));
                worker_loop(&client, request_rx).await;
            });
        })?;

    if fail_fast {
        ready_rx
            .recv()
            .map_err(|_| "postgres worker stopped before connecting".to_string())??;
    }
    Ok(())
}

async fn worker_loop(client: &Client, request_rx: Receiver<Request>) {
    let mut cache: CacheSlots<CachedStatement> = CacheSlots::new(STATEMENT_CACHE_CAPACITY);

    while let Ok(request) = request_rx.recv().await {
        match request {
            Request::Checkout { response } => {
                let (private_tx, private_rx) = async_channel::unbounded();
                if response.send(private_tx).is_err() {
                    continue;
                }
                while let Ok(request) = private_rx.recv().await {
                    match request {
                        Request::Release => break,
                        Request::Checkout { .. } => {}
                        other => dispatch(client, &mut cache, other).await,
                    }
                }
            }
            Request::Release => {}
            other => dispatch(client, &mut cache, other).await,
        }
    }
}

async fn dispatch(client: &Client, cache: &mut CacheSlots<CachedStatement>, request: Request) {
    match request {
        Request::Query {
            sql,
            params,
            response,
        } => {
            let result = execute(client, cache, &sql, params).await;
            let _ = response.send(result);
        }
        Request::Prepare { sql, response } => {
            let result = ensure_cached(client, cache, &sql).await.map(|_| ());
            let _ = response.send(result);
        }
        Request::QueryStream {
            sql,
            params,
            chunks,
        } => {
            execute_stream(client, cache, &sql, params, &chunks).await;
        }
        Request::Checkout { .. } | Request::Release => {}
    }
}

async fn ensure_cached<'c>(
    client: &Client,
    cache: &'c mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
) -> Result<&'c mut CachedStatement, Error> {
    if cache.get_mut(sql).is_none() {
        // The `?` → `$n` rewrite happens once here, at prepare time.
        let rewritten = postgres_placeholders(sql);
        let statement = client
            .prepare(&rewritten)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        cache.insert(sql.clone(), CachedStatement { statement });
    }
    Ok(cache.get_mut(sql).expect("statement was just cached"))
}

async fn execute(
    client: &Client,
    cache: &mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
    params: Vec<Value>,
) -> Result<QueryResult, Error> {
    if is_ddl(sql) {
        // Run DDL outside the cache (it may contain several statements) and
        // drop cached statements that may reference the old schema.
        let result = client
            .batch_execute(sql)
            .await
            .map(|_| QueryResult::new(Vec::new(), 0))
            .map_err(|e| Error::Backend(e.to_string()));
        cache.clear();
        return result;
    }

    let cached = ensure_cached(client, cache, sql).await?;
    let statement = cached.statement.clone();
    let columns = cached.result_columns();
    let params = postgres_params(params);
    let param_refs = params
        .iter()
        .map(|p| &**p as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    // The prepared statement knows whether it returns rows — no query-text
    // sniffing needed.
    if statement.columns().is_empty() {
        let affected_rows = client
            .execute(&statement, &param_refs)
            .await
            .map_err(|e| Error::Backend(e.to_string()))? as usize;
        Ok(QueryResult::new(Vec::new(), affected_rows))
    } else {
        let rows = client
            .query(&statement, &param_refs)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        let rows = rows
            .iter()
            .map(|row| postgres_row(row, &columns))
            .collect::<Result<Vec<_>, _>>()?;
        let affected_rows = rows.len();
        Ok(QueryResult::new(rows, affected_rows))
    }
}

async fn execute_stream(
    client: &Client,
    cache: &mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
    params: Vec<Value>,
    chunks: &async_channel::Sender<Result<Vec<Row>, Error>>,
) {
    if is_ddl(sql) {
        let _ = chunks
            .send(Err(Error::InvalidQuery(
                "cannot stream a schema-changing statement".into(),
            )))
            .await;
        return;
    }

    let (statement, columns) = match ensure_cached(client, cache, sql).await {
        Ok(cached) => (cached.statement.clone(), cached.result_columns()),
        Err(error) => {
            let _ = chunks.send(Err(error)).await;
            return;
        }
    };
    let params = postgres_params(params);

    let result = async {
        let row_stream = client
            .query_raw(
                &statement,
                params.iter().map(|p| &**p as &(dyn ToSql + Sync)),
            )
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        futures::pin_mut!(row_stream);
        let mut chunk = Vec::with_capacity(STREAM_CHUNK_ROWS);
        while let Some(row) = row_stream.next().await {
            let row = row.map_err(|e| Error::Backend(e.to_string()))?;
            chunk.push(postgres_row(&row, &columns)?);
            if chunk.len() >= STREAM_CHUNK_ROWS {
                let full = std::mem::replace(&mut chunk, Vec::with_capacity(STREAM_CHUNK_ROWS));
                if chunks.send(Ok(full)).await.is_err() {
                    return Ok(());
                }
            }
        }
        if !chunk.is_empty() {
            let _ = chunks.send(Ok(chunk)).await;
        }
        Ok::<(), Error>(())
    }
    .await;

    if let Err(error) = result {
        let _ = chunks.send(Err(error)).await;
    }
}
