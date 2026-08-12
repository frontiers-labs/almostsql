//! Worker implementation on the synchronous `postgres` client.

use std::sync::Arc;
use std::thread;

use async_channel::Receiver;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, NoTls};

use super::{
    CachedStatement, POSTGRES_WORKERS, postgres_params, postgres_placeholders, postgres_row,
};
use crate::error::Error;
use crate::pool::{
    CacheSlots, Request, RequestQueue, RowStream, STATEMENT_CACHE_CAPACITY, STREAM_CHUNK_ROWS,
    is_ddl,
};
use crate::query::{QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;

#[derive(Clone)]
pub struct PostgresBackend {
    queue: RequestQueue,
}

impl PostgresBackend {
    pub fn new(url: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (queue, request_rx) = RequestQueue::new_shared();

        // The first worker connects synchronously so a bad URL fails here
        // rather than on the first query.
        spawn_worker(url, request_rx.clone(), true)?;
        for _ in 1..POSTGRES_WORKERS {
            spawn_worker(url, request_rx.clone(), false)?;
        }

        Ok(Self { queue })
    }

    pub(crate) async fn query(
        &self,
        sql: std::sync::Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        self.queue.query(sql, params).await
    }

    pub(crate) async fn prepare_only(&self, sql: std::sync::Arc<str>) -> Result<(), Error> {
        self.queue.prepare(sql).await
    }

    pub(crate) async fn query_stream(
        &self,
        sql: std::sync::Arc<str>,
        params: Vec<Value>,
    ) -> Result<RowStream, Error> {
        self.queue.query_stream(sql, params).await
    }

    /// Check a worker out and open a transaction on its connection. The
    /// returned private queue pins all further statements to that worker.
    pub(crate) async fn begin(&self) -> Result<RequestQueue, Error> {
        let private = self.queue.checkout().await?;
        private
            .query(std::sync::Arc::from("BEGIN;"), Vec::new())
            .await?;
        Ok(private)
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Postgres(super::PostgresBuilder {})
    }
}

fn spawn_worker(
    url: &str,
    request_rx: Receiver<Request>,
    fail_fast: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = url.to_string();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    // The synchronous `postgres` client drives its own Tokio runtime via
    // `block_on`, which panics when invoked from a thread already inside the
    // server's async runtime. Pin each client to a dedicated OS thread so its
    // internal runtime is the only one on that thread.
    thread::Builder::new()
        .name("almostsql-postgres-worker".to_string())
        .spawn(move || {
            let mut client = match Client::connect(&url, NoTls) {
                Ok(client) => {
                    let _ = ready_tx.send(Ok(()));
                    client
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            };
            worker_loop(&mut client, request_rx);
        })?;

    if fail_fast {
        ready_rx
            .recv()
            .map_err(|_| "postgres worker stopped before connecting".to_string())??;
    }
    Ok(())
}

fn worker_loop(client: &mut Client, request_rx: Receiver<Request>) {
    let mut cache: CacheSlots<CachedStatement> = CacheSlots::new(STATEMENT_CACHE_CAPACITY);

    while let Ok(request) = request_rx.recv_blocking() {
        match request {
            Request::Checkout { response } => {
                let (private_tx, private_rx) = async_channel::unbounded();
                if response.send(private_tx).is_err() {
                    continue;
                }
                while let Ok(request) = private_rx.recv_blocking() {
                    match request {
                        Request::Release => break,
                        Request::Checkout { .. } => {}
                        other => dispatch(client, &mut cache, other),
                    }
                }
            }
            Request::Release => {}
            other => dispatch(client, &mut cache, other),
        }
    }
}

fn dispatch(client: &mut Client, cache: &mut CacheSlots<CachedStatement>, request: Request) {
    match request {
        Request::Query {
            sql,
            params,
            response,
        } => {
            let result = execute(client, cache, &sql, params);
            let _ = response.send(result);
        }
        Request::Prepare { sql, response } => {
            let result = ensure_cached(client, cache, &sql).map(|_| ());
            let _ = response.send(result);
        }
        Request::QueryStream {
            sql,
            params,
            chunks,
        } => {
            execute_stream(client, cache, &sql, params, &chunks);
        }
        Request::Checkout { .. } | Request::Release => {}
    }
}

fn ensure_cached<'c>(
    client: &mut Client,
    cache: &'c mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
) -> Result<&'c mut CachedStatement, Error> {
    if cache.get_mut(sql).is_none() {
        // The `?` → `$n` rewrite happens once here, at prepare time.
        let rewritten = postgres_placeholders(sql);
        let statement = client
            .prepare(&rewritten)
            .map_err(|e| Error::Backend(e.to_string()))?;
        cache.insert(sql.clone(), CachedStatement { statement });
    }
    Ok(cache.get_mut(sql).expect("statement was just cached"))
}

fn execute(
    client: &mut Client,
    cache: &mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
    params: Vec<Value>,
) -> Result<QueryResult, Error> {
    if is_ddl(sql) {
        // Run DDL outside the cache (it may contain several statements) and
        // drop cached statements that may reference the old schema.
        let result = client
            .batch_execute(sql)
            .map(|_| QueryResult::new(Vec::new(), 0))
            .map_err(|e| Error::Backend(e.to_string()));
        cache.clear();
        return result;
    }

    let cached = ensure_cached(client, cache, sql)?;
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
            .map_err(|e| Error::Backend(e.to_string()))? as usize;
        Ok(QueryResult::new(Vec::new(), affected_rows))
    } else {
        let rows = client
            .query(&statement, &param_refs)
            .map_err(|e| Error::Backend(e.to_string()))?;
        let rows = rows
            .into_iter()
            .map(|row| postgres_row(&row, &columns))
            .collect::<Result<Vec<_>, _>>()?;
        let affected_rows = rows.len();
        Ok(QueryResult::new(rows, affected_rows))
    }
}

fn execute_stream(
    client: &mut Client,
    cache: &mut CacheSlots<CachedStatement>,
    sql: &Arc<str>,
    params: Vec<Value>,
    chunks: &async_channel::Sender<Result<Vec<Row>, Error>>,
) {
    if is_ddl(sql) {
        let _ = chunks.send_blocking(Err(Error::InvalidQuery(
            "cannot stream a schema-changing statement".into(),
        )));
        return;
    }

    let (statement, columns) = match ensure_cached(client, cache, sql) {
        Ok(cached) => (cached.statement.clone(), cached.result_columns()),
        Err(error) => {
            let _ = chunks.send_blocking(Err(error));
            return;
        }
    };
    let params = postgres_params(params);

    let result = (|| -> Result<(), Error> {
        let mut row_iter = client
            .query_raw(
                &statement,
                params.iter().map(|p| &**p as &(dyn ToSql + Sync)),
            )
            .map_err(|e| Error::Backend(e.to_string()))?;
        let mut chunk = Vec::with_capacity(STREAM_CHUNK_ROWS);
        while let Some(row) = row_iter.next().map_err(|e| Error::Backend(e.to_string()))? {
            chunk.push(postgres_row(&row, &columns)?);
            if chunk.len() >= STREAM_CHUNK_ROWS {
                let full = std::mem::replace(&mut chunk, Vec::with_capacity(STREAM_CHUNK_ROWS));
                if chunks.send_blocking(Ok(full)).is_err() {
                    return Ok(());
                }
            }
        }
        if !chunk.is_empty() {
            let _ = chunks.send_blocking(Ok(chunk));
        }
        Ok(())
    })();

    if let Err(error) = result {
        let _ = chunks.send_blocking(Err(error));
    }
}
