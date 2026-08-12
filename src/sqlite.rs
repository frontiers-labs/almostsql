//! SQLite backend with direct, inline execution.
//!
//! SQLite is an in-process library, so queries run directly on the caller's
//! task against a connection checked out of a [`SlotPool`] — no worker
//! threads, no channel round-trips. A point query costs one free-list pop,
//! the statement execution itself, and one push.
//!
//! The trade-off: execution happens on the executor thread. Statements are
//! usually microseconds, but very large scans or a contended write (busy
//! timeout) will occupy the thread for their duration.

use std::sync::{Arc, Once};

use crate::error::Error;
use crate::migration::{AlterTable, Command, SqlType};
use crate::pool::{CacheSlots, STATEMENT_CACHE_CAPACITY, SlotGuard, SlotPool, is_ddl};
use crate::query::{Columns, QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;
use ::sqlite::{self, Connection, State, Statement, Value as SqliteValue};

#[derive(Clone)]
pub struct SqliteBackend {
    pool: SlotPool<SqliteConnection>,
}

pub struct SqliteBuilder;

static VEC_EXTENSION: Once = Once::new();

/// One pooled SQLite connection plus its prepared-statement cache.
///
/// The cache holds `Statement<'static>` values whose real lifetime is tied to
/// `connection`. That is sound because the connection is boxed (its heap
/// address never changes when the slot moves) and `cache` is declared before
/// `connection`, so statements are finalized before the connection closes.
pub(crate) struct SqliteConnection {
    cache: CacheSlots<CachedStatement>,
    connection: Box<Connection>,
}

// SAFETY: the slot is only ever accessed by one thread at a time — it moves
// through the pool channel and is used exclusively through a `SlotGuard`.
// SQLite itself is compiled in serialized threading mode (the bundled
// default), so a connection may be used from different threads serially. The
// `Rc`s inside cached `Statement`s never escape the slot, so their non-atomic
// reference counts are only touched by the thread currently holding the slot.
unsafe impl Send for SqliteConnection {}

struct CachedStatement {
    statement: Statement<'static>,
}

impl SqliteBackend {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        VEC_EXTENSION.call_once(|| unsafe {
            sqlite::ffi::sqlite3_auto_extension(Some(sqlite_vec::sqlite3_vec_init));
        });

        // Every connection to a `:memory:` path opens a distinct database, so
        // in-memory pools must stay at one connection.
        let memory = path.contains(":memory:") || path.contains("mode=memory");
        let connections = if memory {
            1
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(4)
        };

        let pool = SlotPool::new();
        for _ in 0..connections {
            pool.put(SqliteConnection::open(path, memory)?);
        }

        Ok(Self { pool })
    }

    pub(crate) async fn query(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        let mut slot = self.pool.acquire().await?;
        slot.execute(&sql, &params)
    }

    pub(crate) async fn prepare_only(&self, sql: Arc<str>) -> Result<(), Error> {
        let mut slot = self.pool.acquire().await?;
        slot.ensure_cached(&sql).map(|_| ())
    }

    pub(crate) async fn query_stream(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<crate::pool::RowStream, Error> {
        let mut guard = self.pool.acquire().await?;
        let (columns, column_count) = guard.start_streaming(&sql, &params)?;
        Ok(crate::pool::RowStream::from_sqlite(SqliteRowStream {
            guard,
            sql,
            columns,
            column_count,
            done: false,
        }))
    }

    pub(crate) async fn begin(&self) -> Result<SqliteTransaction, Error> {
        let mut guard = self.pool.acquire().await?;
        guard.execute(&Arc::from("BEGIN;"), &[])?;
        Ok(SqliteTransaction {
            guard: futures::lock::Mutex::new(guard),
        })
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Sqlite(SqliteBuilder {})
    }
}

/// A transaction's exclusively-held SQLite connection.
pub(crate) struct SqliteTransaction {
    guard: futures::lock::Mutex<SlotGuard<SqliteConnection>>,
}

impl SqliteTransaction {
    pub(crate) async fn query(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        self.guard.lock().await.execute(&sql, &params)
    }

    /// Roll back without consuming; used from `Transaction`'s `Drop`. The
    /// error (if any) is ignored — SQLite aborts the transaction itself on
    /// most failures, and the slot returns to the pool either way.
    pub(crate) fn rollback_blocking(&self) {
        if let Some(mut guard) = self.guard.try_lock() {
            let _ = guard.execute(&Arc::from("ROLLBACK;"), &[]);
        }
    }
}

impl SqliteConnection {
    fn open(path: &str, memory: bool) -> Result<Self, sqlite::Error> {
        let mut connection = Connection::open(path)?;
        if !memory {
            // WAL lets one writer and many readers proceed concurrently; the
            // busy timeout resolves writer contention between pool connections.
            connection.execute("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            connection.set_busy_timeout(5_000)?;
        }
        Ok(Self {
            cache: CacheSlots::new(STATEMENT_CACHE_CAPACITY),
            connection: Box::new(connection),
        })
    }

    fn ensure_cached(&mut self, sql: &Arc<str>) -> Result<&mut CachedStatement, Error> {
        if self.cache.get_mut(sql).is_none() {
            let statement = self
                .connection
                .prepare(&**sql)
                .map_err(|e| Error::Backend(e.to_string()))?;
            // SAFETY: erases the borrow of `self.connection`. The connection
            // is boxed and outlives the cache (see the struct invariant), and
            // the statement never leaves this slot.
            let statement =
                unsafe { std::mem::transmute::<Statement<'_>, Statement<'static>>(statement) };
            self.cache
                .insert(sql.clone(), CachedStatement { statement });
        }
        Ok(self.cache.get_mut(sql).expect("statement was just cached"))
    }

    fn execute(&mut self, sql: &Arc<str>, params: &[Value]) -> Result<QueryResult, Error> {
        if is_ddl(sql) {
            // `execute` runs the whole string through sqlite3_exec, so multi-
            // statement DDL (as raw migrations often are) works. Cached
            // statements may reference the old schema afterwards; drop them.
            let result = self
                .connection
                .execute(&**sql)
                .map(|_| QueryResult::new(Vec::new(), self.connection.change_count()))
                .map_err(|e| Error::Backend(e.to_string()));
            self.cache.clear();
            return result;
        }

        self.ensure_cached(sql)?;
        // Split field borrows: the statement borrows `cache`, the change
        // counter reads `connection`.
        let connection = &*self.connection;
        let cached = self.cache.get_mut(sql).expect("statement was just cached");
        let statement = &mut cached.statement;
        let result = run_statement(statement, params, || connection.change_count());
        let _ = statement.reset();
        result
    }

    fn start_streaming(
        &mut self,
        sql: &Arc<str>,
        params: &[Value],
    ) -> Result<(Arc<Columns>, usize), Error> {
        if is_ddl(sql) {
            return Err(Error::InvalidQuery(
                "cannot stream a schema-changing statement".into(),
            ));
        }
        let cached = self.ensure_cached(sql)?;
        let statement = &mut cached.statement;
        bind_params(statement, params)?;
        let column_count = statement.column_count();
        let columns = Arc::new(Columns::new(statement.column_names().to_vec()));
        Ok((columns, column_count))
    }
}

/// Incremental row stream over a checked-out connection: each `next_row`
/// steps the cached statement once, so rows are produced lazily and the
/// connection returns to the pool when the stream is dropped.
pub(crate) struct SqliteRowStream {
    guard: SlotGuard<SqliteConnection>,
    sql: Arc<str>,
    columns: Arc<Columns>,
    column_count: usize,
    done: bool,
}

impl SqliteRowStream {
    pub(crate) fn next_row(&mut self) -> Option<Result<Row, Error>> {
        if self.done {
            return None;
        }
        let sql = self.sql.clone();
        let Some(cached) = self.guard.cache.get_mut(&sql) else {
            self.done = true;
            return Some(Err(Error::InvalidQuery(
                "streamed statement was evicted mid-iteration".into(),
            )));
        };
        let statement = &mut cached.statement;
        match step(statement) {
            Ok(State::Row) => match read_row(statement, &self.columns, self.column_count) {
                Ok(row) => Some(Ok(row)),
                Err(error) => {
                    self.finish();
                    Some(Err(error))
                }
            },
            Ok(State::Done) => {
                self.finish();
                None
            }
            Err(error) => {
                self.finish();
                Some(Err(error))
            }
        }
    }

    fn finish(&mut self) {
        if let Some(cached) = self.guard.cache.get_mut(&self.sql) {
            let _ = cached.statement.reset();
        }
        self.done = true;
    }
}

impl Drop for SqliteRowStream {
    fn drop(&mut self) {
        if !self.done {
            self.finish();
        }
    }
}

fn run_statement(
    statement: &mut Statement<'static>,
    params: &[Value],
    change_count: impl Fn() -> usize,
) -> Result<QueryResult, Error> {
    bind_params(statement, params)?;

    let column_count = statement.column_count();
    if column_count == 0 {
        // No result columns: step to completion and report the real change
        // count instead of materializing anything.
        while let State::Row = step(statement)? {}
        return Ok(QueryResult::new(Vec::new(), change_count()));
    }

    let columns = Arc::new(Columns::new(statement.column_names().to_vec()));
    let mut rows = Vec::new();
    while let State::Row = step(statement)? {
        rows.push(read_row(statement, &columns, column_count)?);
    }
    let affected_rows = rows.len();
    Ok(QueryResult::new(rows, affected_rows))
}

fn step(statement: &mut Statement<'_>) -> Result<State, Error> {
    statement.next().map_err(|e| Error::Backend(e.to_string()))
}

fn bind_params(statement: &mut Statement<'_>, params: &[Value]) -> Result<(), Error> {
    let map_err = |e: sqlite::Error| Error::Backend(e.to_string());
    for (idx, param) in params.iter().enumerate() {
        match param {
            Value::Null => statement
                .bind((idx + 1, SqliteValue::Null))
                .map_err(map_err)?,
            Value::Integer(i) => statement.bind((idx + 1, *i)).map_err(map_err)?,
            Value::Real(r) => statement.bind((idx + 1, *r)).map_err(map_err)?,
            Value::Text(s) => statement.bind((idx + 1, s.as_str())).map_err(map_err)?,
            Value::Blob(b) => statement.bind((idx + 1, b.as_slice())).map_err(map_err)?,
            Value::FloatVector(v) => {
                let bytes = float_vector_bytes(v);
                statement
                    .bind((idx + 1, bytes.as_slice()))
                    .map_err(map_err)?
            }
            Value::Uuid(u) => statement
                .bind((idx + 1, u.as_bytes().as_slice()))
                .map_err(map_err)?,
        }
    }
    Ok(())
}

fn read_row(
    statement: &Statement<'_>,
    columns: &Arc<Columns>,
    column_count: usize,
) -> Result<Row, Error> {
    let map_err = |e: sqlite::Error| Error::Backend(e.to_string());
    let mut values = Vec::with_capacity(column_count);
    for i in 0..column_count {
        let value = match statement.column_type(i).map_err(map_err)? {
            sqlite::Type::Null => Value::Null,
            sqlite::Type::Integer => Value::Integer(statement.read::<i64, _>(i).map_err(map_err)?),
            sqlite::Type::Float => Value::Real(statement.read::<f64, _>(i).map_err(map_err)?),
            sqlite::Type::String => Value::Text(statement.read::<String, _>(i).map_err(map_err)?),
            sqlite::Type::Binary => Value::Blob(statement.read::<Vec<u8>, _>(i).map_err(map_err)?),
        };
        values.push(value);
    }
    Ok(Row::new(columns.clone(), values))
}

impl SqliteBuilder {
    pub(crate) fn build_table_setup(
        &self,
        _table: &crate::Table,
    ) -> Result<Vec<String>, crate::sql_builder::SQLError> {
        Ok(Vec::new())
    }

    pub(crate) fn build_table(
        &self,
        table: &crate::Table,
    ) -> Result<String, crate::sql_builder::SQLError> {
        let foreign_keys = table.foreign_keys.iter().map(|fk| {
            format!(
                "FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL",
                fk.fields.join(", "),
                fk.table,
                fk.foreign_fields.join(", ")
            )
        });

        let columns = table
            .columns
            .iter()
            .map(|cmd| match cmd {
                Command::AddColumn {
                    name,
                    r#type,
                    primary_key,
                    unique,
                } => {
                    let primary_key = if *primary_key { "PRIMARY KEY" } else { "" };

                    let unique = if *unique { "UNIQUE" } else { "" };

                    let ty = sql_type(r#type);

                    format!("{} {} {} {}", name, ty, primary_key, unique)
                }
            })
            .chain(foreign_keys)
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!("CREATE TABLE {} ({});", table.name, columns))
    }

    pub(crate) fn build_alter_table(
        &self,
        alter: &AlterTable,
    ) -> Result<Vec<String>, crate::sql_builder::SQLError> {
        Ok(crate::sql_builder::build_alter_statements(
            &alter.name,
            &alter.actions,
            sql_type,
        ))
    }

    pub(crate) fn build_vector_search<Tab>(
        &self,
        search: &crate::VectorSearch<Tab>,
    ) -> Result<(String, Vec<Value>), crate::sql_builder::SQLError> {
        let mut sql = format!(
            "SELECT {} AS id, vec_distance_cosine({}, ?) AS distance FROM {}",
            search.id_column(),
            search.vector_column(),
            search.table()
        );
        let mut params = vec![search.query()];
        search
            .append_where(&mut sql, &mut params)
            .map_err(crate::sql_builder::SQLError::Custom)?;
        sql.push_str(" ORDER BY distance ASC");
        search.append_limit(&mut sql, &mut params);
        Ok((sql, params))
    }
}

fn sql_type(ty: &SqlType) -> String {
    match ty {
        SqlType::Blob => "BLOB".into(),
        SqlType::Real => "REAL".into(),
        SqlType::Integer => "INTEGER".into(),
        SqlType::Text => "TEXT".into(),
        SqlType::Uuid => "BINARY(16)".into(),
        SqlType::Enum(_) => "TEXT".into(),
        SqlType::BitVector(dim) => format!("bit[{}]", dim),
        SqlType::FloatVector(_) => "BLOB".into(),
        SqlType::Int8Vector(dim) => format!("int8[{}]", dim),
    }
}

fn float_vector_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}
