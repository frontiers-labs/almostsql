//! Connection-pool plumbing shared by the backends.
//!
//! Two pooling designs live here:
//!
//! - [`SlotPool`]: an async free-list of owned connection slots. A query
//!   checks a slot out, runs directly against it (inline for SQLite, awaiting
//!   the client for tokio-postgres), and the guard returns the slot on drop.
//!   No worker threads and no per-query thread hand-offs.
//! - [`RequestQueue`]: an MPMC request channel serviced by worker threads,
//!   used by the synchronous `postgres` driver, whose blocking network calls
//!   must stay off the caller's executor.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures::Stream;

use crate::error::Error;
use crate::query::Row;

/// Rows per chunk shipped from a streaming query worker to the caller.
#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
pub(crate) const STREAM_CHUNK_ROWS: usize = 256;
/// Prepared statements kept per connection before evicting.
pub(crate) const STATEMENT_CACHE_CAPACITY: usize = 256;

// -----------------------
// Slot pool (direct execution)
// -----------------------

/// An async free-list of owned connection slots. `acquire` waits until a slot
/// is free; the returned guard gives exclusive access and returns the slot to
/// the pool when dropped.
#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
pub(crate) struct SlotPool<C> {
    tx: Sender<C>,
    rx: Receiver<C>,
}

#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
impl<C> Clone for SlotPool<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
impl<C> SlotPool<C> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self { tx, rx }
    }

    pub(crate) fn put(&self, slot: C) {
        // Unbounded channel: try_send only fails if the pool is gone.
        let _ = self.tx.try_send(slot);
    }

    pub(crate) async fn acquire(&self) -> Result<SlotGuard<C>, Error> {
        let slot = self.rx.recv().await.map_err(|_| Error::WorkerGone)?;
        Ok(SlotGuard {
            slot: Some(slot),
            home: self.tx.clone(),
        })
    }
}

/// Exclusive access to one pooled connection slot. Returns the slot to its
/// pool on drop.
#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
pub(crate) struct SlotGuard<C> {
    slot: Option<C>,
    home: Sender<C>,
}

#[cfg(feature = "postgres-tokio")]
impl<C> SlotGuard<C> {
    /// Take ownership of the slot and its way home, e.g. to finish work on
    /// another thread before returning it. The guard then returns nothing.
    pub(crate) fn into_parts(mut self) -> (C, Sender<C>) {
        let slot = self.slot.take().expect("slot already taken");
        (slot, self.home.clone())
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
impl<C> std::ops::Deref for SlotGuard<C> {
    type Target = C;
    fn deref(&self) -> &C {
        self.slot.as_ref().expect("slot already taken")
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
impl<C> std::ops::DerefMut for SlotGuard<C> {
    fn deref_mut(&mut self) -> &mut C {
        self.slot.as_mut().expect("slot already taken")
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres-tokio"))]
impl<C> Drop for SlotGuard<C> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let _ = self.home.try_send(slot);
        }
    }
}

// -----------------------
// Request queue (worker threads, sync postgres)
// -----------------------

#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
mod request_queue {
    use super::*;
    use crate::query::QueryResult;
    use futures::channel::oneshot;

    /// Chunks buffered in flight before a streaming worker blocks.
    pub(crate) const STREAM_CHUNK_BUFFER: usize = 4;

    pub(crate) enum Request {
        Query {
            sql: Arc<str>,
            params: Vec<crate::query::Value>,
            response: oneshot::Sender<Result<QueryResult, Error>>,
        },
        QueryStream {
            sql: Arc<str>,
            params: Vec<crate::query::Value>,
            chunks: Sender<Result<Vec<Row>, Error>>,
        },
        Prepare {
            sql: Arc<str>,
            response: oneshot::Sender<Result<(), Error>>,
        },
        Checkout {
            response: oneshot::Sender<Sender<Request>>,
        },
        Release,
    }

    /// Handle used to submit requests to a set of workers (the shared pool
    /// channel) or to one checked-out worker (a transaction's private channel).
    #[derive(Clone)]
    pub(crate) struct RequestQueue {
        tx: Sender<Request>,
    }

    impl RequestQueue {
        pub(crate) fn new_shared() -> (Self, Receiver<Request>) {
            let (tx, rx) = async_channel::unbounded();
            (Self { tx }, rx)
        }

        pub(crate) fn from_sender(tx: Sender<Request>) -> Self {
            Self { tx }
        }

        pub(crate) fn sender(&self) -> &Sender<Request> {
            &self.tx
        }

        pub(crate) async fn query(
            &self,
            sql: Arc<str>,
            params: Vec<crate::query::Value>,
        ) -> Result<QueryResult, Error> {
            let (response, receiver) = oneshot::channel();
            self.tx
                .send(Request::Query {
                    sql,
                    params,
                    response,
                })
                .await
                .map_err(|_| Error::WorkerGone)?;
            receiver.await.map_err(|_| Error::WorkerGone)?
        }

        pub(crate) async fn prepare(&self, sql: Arc<str>) -> Result<(), Error> {
            let (response, receiver) = oneshot::channel();
            self.tx
                .send(Request::Prepare { sql, response })
                .await
                .map_err(|_| Error::WorkerGone)?;
            receiver.await.map_err(|_| Error::WorkerGone)?
        }

        pub(crate) async fn query_stream(
            &self,
            sql: Arc<str>,
            params: Vec<crate::query::Value>,
        ) -> Result<RowStream, Error> {
            let (chunks, receiver) = async_channel::bounded(STREAM_CHUNK_BUFFER);
            self.tx
                .send(Request::QueryStream {
                    sql,
                    params,
                    chunks,
                })
                .await
                .map_err(|_| Error::WorkerGone)?;
            Ok(RowStream::from_chunks(receiver))
        }

        /// Reserve one worker connection. Requests sent through the returned
        /// queue run on that connection only, until [`Request::Release`].
        pub(crate) async fn checkout(&self) -> Result<RequestQueue, Error> {
            let (response, receiver) = oneshot::channel();
            self.tx
                .send(Request::Checkout { response })
                .await
                .map_err(|_| Error::WorkerGone)?;
            let private = receiver.await.map_err(|_| Error::WorkerGone)?;
            Ok(RequestQueue::from_sender(private))
        }
    }
}

#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
pub(crate) use request_queue::{Request, RequestQueue};

// -----------------------
// Row stream
// -----------------------

type ChunkReceiver = Receiver<Result<Vec<Row>, Error>>;

/// An asynchronous stream of rows produced by
/// [`ConnectionPool::query_stream`](crate::ConnectionPool::query_stream).
///
/// Rows are produced incrementally — from the checked-out connection itself
/// (SQLite, tokio-postgres) or in bounded chunks from a worker thread
/// (synchronous postgres) — so a large result set never has to be
/// materialized in memory at once. Dropping the stream releases the
/// connection and abandons the remaining rows.
pub struct RowStream {
    inner: RowStreamInner,
}

enum RowStreamInner {
    #[allow(dead_code)]
    Chunks {
        chunks: Pin<Box<ChunkReceiver>>,
        current: std::vec::IntoIter<Row>,
    },
    #[cfg(feature = "sqlite")]
    Sqlite(Box<crate::sqlite::SqliteRowStream>),
    #[cfg(feature = "postgres-tokio")]
    Postgres(Box<crate::postgres::PgRowStream>),
}

impl RowStream {
    #[allow(dead_code)]
    pub(crate) fn from_chunks(chunks: ChunkReceiver) -> Self {
        Self {
            inner: RowStreamInner::Chunks {
                chunks: Box::pin(chunks),
                current: Vec::new().into_iter(),
            },
        }
    }

    #[cfg(feature = "sqlite")]
    pub(crate) fn from_sqlite(stream: crate::sqlite::SqliteRowStream) -> Self {
        Self {
            inner: RowStreamInner::Sqlite(Box::new(stream)),
        }
    }

    #[cfg(feature = "postgres-tokio")]
    pub(crate) fn from_postgres(stream: crate::postgres::PgRowStream) -> Self {
        Self {
            inner: RowStreamInner::Postgres(Box::new(stream)),
        }
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().inner {
            RowStreamInner::Chunks { chunks, current } => loop {
                if let Some(row) = current.next() {
                    return Poll::Ready(Some(Ok(row)));
                }
                match chunks.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(rows))) => {
                        *current = rows.into_iter();
                    }
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            },
            #[cfg(feature = "sqlite")]
            RowStreamInner::Sqlite(stream) => Poll::Ready(stream.next_row()),
            #[cfg(feature = "postgres-tokio")]
            RowStreamInner::Postgres(stream) => stream.poll_next_row(cx),
        }
    }
}

/// True when the statement starts with a schema-changing keyword. Such
/// statements are executed without caching and flush the statement cache,
/// since cached statements may reference the old schema.
pub(crate) fn is_ddl(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    match words.next() {
        Some(word) => {
            word.eq_ignore_ascii_case("CREATE")
                || word.eq_ignore_ascii_case("ALTER")
                || word.eq_ignore_ascii_case("DROP")
                || word.eq_ignore_ascii_case("VACUUM")
        }
        None => false,
    }
}

/// A minimal LRU bookkeeping helper: maps SQL text to a cache slot and tracks
/// recency with a monotonic tick, evicting the least recently used entry when
/// the cache is full. The cached statement type is backend-specific.
pub(crate) struct CacheSlots<T> {
    entries: std::collections::HashMap<Arc<str>, (u64, T)>,
    tick: u64,
    capacity: usize,
}

impl<T> CacheSlots<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            tick: 0,
            capacity,
        }
    }

    pub(crate) fn get_mut(&mut self, sql: &str) -> Option<&mut T> {
        self.tick += 1;
        let tick = self.tick;
        self.entries.get_mut(sql).map(|(last_used, entry)| {
            *last_used = tick;
            entry
        })
    }

    pub(crate) fn insert(&mut self, sql: Arc<str>, entry: T) -> &mut T {
        if self.entries.len() >= self.capacity
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (last_used, _))| *last_used)
                .map(|(key, _)| key.clone())
        {
            self.entries.remove(&oldest);
        }
        self.tick += 1;
        let tick = self.tick;
        &mut self.entries.entry(sql).or_insert((tick, entry)).1
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}
