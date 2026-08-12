//! Connection-pool plumbing shared by the backends.
//!
//! Each backend owns N worker threads, each holding one database connection
//! and a per-connection prepared-statement cache. Workers pull requests from a
//! shared MPMC channel; a transaction checks a worker out through a private
//! channel so its statements never interleave with other traffic.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures::Stream;
use futures::channel::oneshot;

use crate::error::Error;
use crate::query::{QueryResult, Row};

/// Rows per chunk shipped from a streaming query worker to the caller.
pub(crate) const STREAM_CHUNK_ROWS: usize = 256;
/// Chunks buffered in flight before a streaming worker blocks (backpressure).
pub(crate) const STREAM_CHUNK_BUFFER: usize = 4;
/// Prepared statements kept per connection before evicting.
pub(crate) const STATEMENT_CACHE_CAPACITY: usize = 256;

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
        Ok(RowStream {
            chunks: Box::pin(receiver),
            current: Vec::new().into_iter(),
        })
    }

    /// Reserve one worker connection. Requests sent through the returned
    /// queue run on that connection only, until [`Request::Release`] is sent.
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

/// An asynchronous stream of rows produced by
/// [`ConnectionPool::query_stream`](crate::ConnectionPool::query_stream).
///
/// Rows arrive in bounded chunks from the connection worker, so a large
/// result set never has to be materialized in memory at once.
type ChunkReceiver = Receiver<Result<Vec<Row>, Error>>;

pub struct RowStream {
    chunks: Pin<Box<ChunkReceiver>>,
    current: std::vec::IntoIter<Row>,
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(row) = this.current.next() {
                return Poll::Ready(Some(Ok(row)));
            }
            match this.chunks.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(rows))) => {
                    this.current = rows.into_iter();
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
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
