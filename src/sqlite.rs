use std::sync::{Arc, Once};
use std::thread;

use crate::error::Error;
use crate::migration::{AlterTable, Command, SqlType};
use crate::pool::{
    CacheSlots, Request, RequestQueue, STATEMENT_CACHE_CAPACITY, STREAM_CHUNK_ROWS, is_ddl,
};
use crate::query::{Columns, QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;
use ::sqlite::{self, Connection, State, Statement, Value as SqliteValue};
use async_channel::Receiver;

#[derive(Clone)]
pub struct SqliteBackend {
    queue: RequestQueue,
}

pub struct SqliteBuilder;

static VEC_EXTENSION: Once = Once::new();

impl SqliteBackend {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        VEC_EXTENSION.call_once(|| unsafe {
            sqlite::ffi::sqlite3_auto_extension(Some(sqlite_vec::sqlite3_vec_init));
        });

        // Every connection to a `:memory:` path opens a distinct database, so
        // in-memory pools must stay at one connection.
        let memory = path.contains(":memory:") || path.contains("mode=memory");
        let workers = if memory {
            1
        } else {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(4)
        };

        let (queue, request_rx) = RequestQueue::new_shared();

        // Open the first connection synchronously so a bad path fails here
        // rather than on the first query.
        spawn_worker(path, request_rx.clone(), memory, true)?;
        for _ in 1..workers {
            spawn_worker(path, request_rx.clone(), memory, false)?;
        }

        Ok(Self { queue })
    }

    pub(crate) fn queue(&self) -> &RequestQueue {
        &self.queue
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Sqlite(SqliteBuilder {})
    }
}

fn spawn_worker(
    path: &str,
    request_rx: Receiver<Request>,
    memory: bool,
    fail_fast: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = path.to_string();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    thread::Builder::new()
        .name("almostsql-sqlite-worker".to_string())
        .spawn(move || {
            let connection = match open_connection(&path, memory) {
                Ok(connection) => {
                    let _ = ready_tx.send(Ok(()));
                    connection
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            };
            worker_loop(&connection, request_rx);
        })?;

    if fail_fast {
        ready_rx
            .recv()
            .map_err(|_| "sqlite worker stopped before opening".to_string())??;
    }
    Ok(())
}

fn open_connection(path: &str, memory: bool) -> Result<Connection, sqlite::Error> {
    let mut connection = Connection::open(path)?;
    if !memory {
        // WAL lets one writer and many readers proceed concurrently; the busy
        // timeout resolves writer contention between pool connections.
        connection.execute("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        connection.set_busy_timeout(5_000)?;
    }
    Ok(connection)
}

struct CachedStatement<'l> {
    statement: Statement<'l>,
}

fn worker_loop(connection: &Connection, request_rx: Receiver<Request>) {
    let mut cache: CacheSlots<CachedStatement<'_>> = CacheSlots::new(STATEMENT_CACHE_CAPACITY);

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
                        other => dispatch(connection, &mut cache, other),
                    }
                }
            }
            Request::Release => {}
            other => dispatch(connection, &mut cache, other),
        }
    }
}

fn dispatch<'l>(
    connection: &'l Connection,
    cache: &mut CacheSlots<CachedStatement<'l>>,
    request: Request,
) {
    match request {
        Request::Query {
            sql,
            params,
            response,
        } => {
            let result = execute(connection, cache, &sql, &params);
            let _ = response.send(result);
        }
        Request::Prepare { sql, response } => {
            let result = ensure_cached(connection, cache, &sql).map(|_| ());
            let _ = response.send(result);
        }
        Request::QueryStream {
            sql,
            params,
            chunks,
        } => {
            execute_stream(connection, cache, &sql, &params, &chunks);
        }
        Request::Checkout { .. } | Request::Release => {}
    }
}

fn ensure_cached<'l, 'c>(
    connection: &'l Connection,
    cache: &'c mut CacheSlots<CachedStatement<'l>>,
    sql: &Arc<str>,
) -> Result<&'c mut CachedStatement<'l>, Error> {
    // A get followed by an insert on miss would borrow the cache twice in
    // ways the borrow checker rejects, so probe by key first.
    if cache.get_mut(sql).is_none() {
        let statement = connection
            .prepare(&**sql)
            .map_err(|e| Error::Backend(e.to_string()))?;
        cache.insert(sql.clone(), CachedStatement { statement });
    }
    Ok(cache.get_mut(sql).expect("statement was just cached"))
}

fn execute<'l>(
    connection: &'l Connection,
    cache: &mut CacheSlots<CachedStatement<'l>>,
    sql: &Arc<str>,
    params: &[Value],
) -> Result<QueryResult, Error> {
    if is_ddl(sql) {
        // `execute` runs the whole string through sqlite3_exec, so multi-
        // statement DDL (as raw migrations often are) works. Cached statements
        // may reference the old schema afterwards; drop them.
        let result = connection
            .execute(&**sql)
            .map(|_| QueryResult::new(Vec::new(), connection.change_count()))
            .map_err(|e| Error::Backend(e.to_string()));
        cache.clear();
        return result;
    }

    let cached = ensure_cached(connection, cache, sql)?;
    let result = run_statement(connection, &mut cached.statement, params);
    let _ = cached.statement.reset();
    result
}

fn run_statement(
    connection: &Connection,
    statement: &mut Statement<'_>,
    params: &[Value],
) -> Result<QueryResult, Error> {
    bind_params(statement, params)?;

    let column_count = statement.column_count();
    if column_count == 0 {
        // No result columns: step to completion and report the real change
        // count instead of materializing anything.
        while let State::Row = step(statement)? {}
        return Ok(QueryResult::new(Vec::new(), connection.change_count()));
    }

    let columns = Arc::new(Columns::new(statement.column_names().to_vec()));
    let mut rows = Vec::new();
    while let State::Row = step(statement)? {
        rows.push(read_row(statement, &columns, column_count)?);
    }
    let affected_rows = rows.len();
    Ok(QueryResult::new(rows, affected_rows))
}

fn execute_stream<'l>(
    connection: &'l Connection,
    cache: &mut CacheSlots<CachedStatement<'l>>,
    sql: &Arc<str>,
    params: &[Value],
    chunks: &async_channel::Sender<Result<Vec<Row>, Error>>,
) {
    if is_ddl(sql) {
        let _ = chunks.send_blocking(Err(Error::InvalidQuery(
            "cannot stream a schema-changing statement".into(),
        )));
        return;
    }

    let cached = match ensure_cached(connection, cache, sql) {
        Ok(cached) => cached,
        Err(error) => {
            let _ = chunks.send_blocking(Err(error));
            return;
        }
    };
    let statement = &mut cached.statement;

    let result = (|| -> Result<(), Error> {
        bind_params(statement, params)?;
        let column_count = statement.column_count();
        let columns = Arc::new(Columns::new(statement.column_names().to_vec()));
        let mut chunk = Vec::with_capacity(STREAM_CHUNK_ROWS);
        while let State::Row = step(statement)? {
            chunk.push(read_row(statement, &columns, column_count)?);
            if chunk.len() >= STREAM_CHUNK_ROWS {
                let full = std::mem::replace(&mut chunk, Vec::with_capacity(STREAM_CHUNK_ROWS));
                if chunks.send_blocking(Ok(full)).is_err() {
                    // The consumer dropped the stream; stop reading.
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
    let _ = statement.reset();
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
        search.append_where(&mut sql, &mut params);
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
