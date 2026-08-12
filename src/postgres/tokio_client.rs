//! Direct-execution Postgres backend on `tokio-postgres`.
//!
//! `tokio_postgres::Client` is `Send + Sync` and its futures poll from any
//! thread, so callers await the client directly — no request channel and no
//! per-query thread hand-off. A single background thread runs a
//! current-thread Tokio runtime whose only job is driving the connections'
//! socket I/O. The crate stays runtime-agnostic: callers do not need to be
//! inside a Tokio runtime.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use futures::Stream;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

use super::{
    CachedStatement, POSTGRES_WORKERS, postgres_params, postgres_placeholders, postgres_row,
};
use crate::error::Error;
use crate::pool::{CacheSlots, RowStream, STATEMENT_CACHE_CAPACITY, SlotGuard, SlotPool, is_ddl};
use crate::query::{Columns, QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;

#[derive(Clone)]
pub struct PostgresBackend {
    pool: SlotPool<PgConnection>,
    handle: tokio::runtime::Handle,
}

/// One pooled client plus its prepared-statement cache.
pub(crate) struct PgConnection {
    client: Client,
    cache: CacheSlots<CachedStatement>,
}

type ReadyPayload = Result<(Vec<Client>, tokio::runtime::Handle), String>;

impl PostgresBackend {
    pub fn new(url: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = url.to_string();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<ReadyPayload>();

        thread::Builder::new()
            .name("almostsql-postgres-io".to_string())
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

                runtime.block_on(async move {
                    let mut clients = Vec::with_capacity(POSTGRES_WORKERS);
                    for _ in 0..POSTGRES_WORKERS {
                        match tokio_postgres::connect(&url, NoTls).await {
                            Ok((client, connection)) => {
                                // Each connection task drives one socket; they
                                // all live on this runtime.
                                tokio::spawn(async move {
                                    let _ = connection.await;
                                });
                                clients.push(client);
                            }
                            Err(error) => {
                                let _ = ready_tx.send(Err(error.to_string()));
                                return;
                            }
                        }
                    }
                    let _ = ready_tx.send(Ok((clients, tokio::runtime::Handle::current())));
                    // Keep the runtime alive to drive connection I/O forever.
                    futures::future::pending::<()>().await;
                });
            })?;

        let (clients, handle) = ready_rx
            .recv()
            .map_err(|_| "postgres I/O thread stopped before connecting".to_string())??;

        let pool = SlotPool::new();
        for client in clients {
            pool.put(PgConnection {
                client,
                cache: CacheSlots::new(STATEMENT_CACHE_CAPACITY),
            });
        }

        Ok(Self { pool, handle })
    }

    pub(crate) async fn query(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        let mut slot = self.pool.acquire().await?;
        slot.execute(&sql, params).await
    }

    pub(crate) async fn prepare_only(&self, sql: Arc<str>) -> Result<(), Error> {
        let mut slot = self.pool.acquire().await?;
        slot.ensure_cached(&sql).await.map(|_| ())
    }

    pub(crate) async fn query_stream(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<RowStream, Error> {
        if is_ddl(&sql) {
            return Err(Error::InvalidQuery(
                "cannot stream a schema-changing statement".into(),
            ));
        }
        let mut guard = self.pool.acquire().await?;
        let cached = guard.ensure_cached(&sql).await?;
        let statement = cached.statement.clone();
        let columns = cached.result_columns();
        let params = postgres_params(params);
        let stream = guard
            .client
            .query_raw(
                &statement,
                params.iter().map(|p| &**p as &(dyn ToSql + Sync)),
            )
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(RowStream::from_postgres(PgRowStream {
            guard: Some(guard),
            inner: Box::pin(stream),
            columns,
        }))
    }

    pub(crate) async fn begin(&self) -> Result<PgTransaction, Error> {
        let guard = self.pool.acquire().await?;
        guard
            .client
            .batch_execute("BEGIN;")
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(PgTransaction {
            slot: futures::lock::Mutex::new(Some(guard)),
            handle: self.handle.clone(),
        })
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Postgres(super::PostgresBuilder {})
    }
}

/// A transaction's exclusively-held client.
pub(crate) struct PgTransaction {
    slot: futures::lock::Mutex<Option<SlotGuard<PgConnection>>>,
    handle: tokio::runtime::Handle,
}

impl PgTransaction {
    pub(crate) async fn query(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        let mut locked = self.slot.lock().await;
        let slot = locked.as_mut().ok_or(Error::WorkerGone)?;
        slot.execute(&sql, params).await
    }

    /// Roll back asynchronously from `Transaction`'s `Drop`: the slot is
    /// moved onto the I/O runtime, rolled back there, and returned to the
    /// pool afterwards, so a dirty connection is never handed out.
    pub(crate) fn rollback_spawn(&self) {
        let Some(mut locked) = self.slot.try_lock() else {
            return;
        };
        let Some(guard) = locked.take() else {
            return;
        };
        let (slot, home) = guard.into_parts();
        self.handle.spawn(async move {
            let _ = slot.client.batch_execute("ROLLBACK;").await;
            let _ = home.try_send(slot);
        });
    }
}

impl PgConnection {
    async fn ensure_cached(&mut self, sql: &Arc<str>) -> Result<&mut CachedStatement, Error> {
        if self.cache.get_mut(sql).is_none() {
            // The `?` → `$n` rewrite happens once here, at prepare time.
            let rewritten = postgres_placeholders(sql);
            let statement = self
                .client
                .prepare(&rewritten)
                .await
                .map_err(|e| Error::Backend(e.to_string()))?;
            self.cache
                .insert(sql.clone(), CachedStatement { statement });
        }
        Ok(self.cache.get_mut(sql).expect("statement was just cached"))
    }

    async fn execute(&mut self, sql: &Arc<str>, params: Vec<Value>) -> Result<QueryResult, Error> {
        if is_ddl(sql) {
            // Run DDL outside the cache (it may contain several statements)
            // and drop cached statements that may reference the old schema.
            let result = self
                .client
                .batch_execute(sql)
                .await
                .map(|_| QueryResult::new(Vec::new(), 0))
                .map_err(|e| Error::Backend(e.to_string()));
            self.cache.clear();
            return result;
        }

        let cached = self.ensure_cached(sql).await?;
        let statement = cached.statement.clone();
        let columns = cached.result_columns();
        let params = postgres_params(params);
        let param_refs = params
            .iter()
            .map(|p| &**p as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();

        // The prepared statement knows whether it returns rows — no
        // query-text sniffing needed.
        if statement.columns().is_empty() {
            let affected_rows =
                self.client
                    .execute(&statement, &param_refs)
                    .await
                    .map_err(|e| Error::Backend(e.to_string()))? as usize;
            Ok(QueryResult::new(Vec::new(), affected_rows))
        } else {
            let rows = self
                .client
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
}

/// Streamed rows over a checked-out client. The `tokio_postgres::RowStream`
/// is fully owned; the slot guard keeps the connection reserved until the
/// stream finishes or is dropped.
pub(crate) struct PgRowStream {
    guard: Option<SlotGuard<PgConnection>>,
    inner: Pin<Box<tokio_postgres::RowStream>>,
    columns: Arc<Columns>,
}

impl PgRowStream {
    pub(crate) fn poll_next_row(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Row, Error>>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => Poll::Ready(Some(postgres_row(&row, &self.columns))),
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(Error::Backend(error.to_string()))))
            }
            Poll::Ready(None) => {
                // Release the connection as soon as the rows are exhausted.
                self.guard.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
