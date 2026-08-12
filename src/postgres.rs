use std::sync::Arc;
use std::thread;

use async_channel::Receiver;
use bytes::BytesMut;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use postgres::{Client, NoTls};

use crate::error::Error;
use crate::migration::{AlterTable, Command, SqlType};
use crate::pool::{
    CacheSlots, Request, RequestQueue, STATEMENT_CACHE_CAPACITY, STREAM_CHUNK_ROWS, is_ddl,
};
use crate::query::{Columns, QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;

const POSTGRES_WORKERS: usize = 4;

#[derive(Clone)]
pub struct PostgresBackend {
    queue: RequestQueue,
}

pub struct PostgresBuilder;

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

    pub(crate) fn queue(&self) -> &RequestQueue {
        &self.queue
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Postgres(PostgresBuilder {})
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

struct CachedStatement {
    statement: postgres::Statement,
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

    let statement = ensure_cached(client, cache, sql)?.statement.clone();
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
        let columns = Arc::new(Columns::new(
            statement
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect(),
        ));
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

    let statement = match ensure_cached(client, cache, sql) {
        Ok(cached) => cached.statement.clone(),
        Err(error) => {
            let _ = chunks.send_blocking(Err(error));
            return;
        }
    };
    let columns = Arc::new(Columns::new(
        statement
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect(),
    ));
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

/// A SQL NULL that adapts to whatever column type Postgres expects. A typed
/// `Option::<T>::None` would force a parameter OID (e.g. int4) and fail against
/// a column of any other type, so bind NULLs through this instead.
#[derive(Debug)]
struct PgNull;

impl ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &Type,
        _out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn postgres_params(params: Vec<Value>) -> Vec<Box<dyn ToSql + Sync>> {
    params
        .into_iter()
        .map(|param| match param {
            Value::Null => Box::new(PgNull) as Box<dyn ToSql + Sync>,
            Value::Integer(i) => Box::new(i),
            Value::Real(r) => Box::new(r),
            Value::Text(s) => Box::new(s),
            Value::Blob(b) => Box::new(b),
            Value::FloatVector(v) => Box::new(PgVector(v)),
            Value::Uuid(u) => Box::new(u),
        })
        .collect()
}

fn postgres_placeholders(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut idx = 1;

    for ch in query.chars() {
        if ch == '?' {
            out.push('$');
            out.push_str(&idx.to_string());
            idx += 1;
        } else {
            out.push(ch);
        }
    }

    out
}

fn postgres_row(row: &postgres::Row, columns: &Arc<Columns>) -> Result<Row, Error> {
    let mut values = Vec::with_capacity(row.columns().len());

    for (idx, column) in row.columns().iter().enumerate() {
        let ty = column.type_();
        let value = if *ty == Type::INT2 {
            try_get(row, idx)?
                .map(|v: i16| Value::Integer(v as i64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::INT4 {
            try_get(row, idx)?
                .map(|v: i32| Value::Integer(v as i64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::INT8 {
            try_get(row, idx)?
                .map(Value::Integer)
                .unwrap_or(Value::Null)
        } else if *ty == Type::FLOAT4 {
            try_get(row, idx)?
                .map(|v: f32| Value::Real(v as f64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::FLOAT8 {
            try_get(row, idx)?.map(Value::Real).unwrap_or(Value::Null)
        } else if *ty == Type::BOOL {
            try_get(row, idx)?
                .map(|v: bool| Value::Integer(if v { 1 } else { 0 }))
                .unwrap_or(Value::Null)
        } else if *ty == Type::BYTEA {
            try_get(row, idx)?.map(Value::Blob).unwrap_or(Value::Null)
        } else if ty.name() == "vector" {
            try_get(row, idx)?
                .map(|v: PgVector| Value::FloatVector(v.0))
                .unwrap_or(Value::Null)
        } else if *ty == Type::UUID {
            try_get(row, idx)?.map(Value::Uuid).unwrap_or(Value::Null)
        } else if *ty == Type::TEXT
            || *ty == Type::VARCHAR
            || *ty == Type::BPCHAR
            || *ty == Type::NAME
        {
            try_get(row, idx)?.map(Value::Text).unwrap_or(Value::Null)
        } else {
            return Err(Error::Backend(format!(
                "unsupported postgres column type: {}",
                ty.name()
            )));
        };

        values.push(value);
    }

    Ok(Row::new(columns.clone(), values))
}

fn try_get<'a, T: FromSql<'a>>(row: &'a postgres::Row, idx: usize) -> Result<Option<T>, Error> {
    row.try_get::<_, Option<T>>(idx)
        .map_err(|e| Error::Backend(e.to_string()))
}

impl PostgresBuilder {
    pub(crate) fn build_table_setup(
        &self,
        table: &crate::Table,
    ) -> Result<Vec<String>, crate::sql_builder::SQLError> {
        if table.enable_vectors {
            Ok(vec!["CREATE EXTENSION IF NOT EXISTS vector;".to_string()])
        } else {
            Ok(Vec::new())
        }
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
            "SELECT {} AS id, {} <=> ? AS distance FROM {}",
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
        SqlType::Blob => "BYTEA".into(),
        SqlType::Real => "DOUBLE PRECISION".into(),
        SqlType::Integer => "BIGINT".into(),
        SqlType::Text => "TEXT".into(),
        SqlType::Uuid => "UUID".into(),
        SqlType::Enum(_) => "TEXT".into(),
        SqlType::BitVector(_) => "BYTEA".into(),
        SqlType::FloatVector(dim) if *dim == 0 => "vector".into(),
        SqlType::FloatVector(dim) => format!("vector({dim})"),
        SqlType::Int8Vector(_) => "BYTEA".into(),
    }
}

#[derive(Debug)]
struct PgVector(Vec<f32>);

impl ToSql for PgVector {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !<Self as ToSql>::accepts(ty) {
            return Err(format!("expected pgvector type, got {}", ty.name()).into());
        }
        let len = i16::try_from(self.0.len())?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&0_i16.to_be_bytes());
        for value in &self.0 {
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "vector"
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for PgVector {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if !<Self as FromSql>::accepts(ty) {
            return Err(format!("expected pgvector type, got {}", ty.name()).into());
        }
        if raw.len() < 4 {
            return Err("invalid pgvector value".into());
        }
        let dims = i16::from_be_bytes([raw[0], raw[1]]);
        if dims < 0 {
            return Err("invalid pgvector dimensions".into());
        }
        let dims = dims as usize;
        let values = &raw[4..];
        if values.len() != dims * std::mem::size_of::<f32>() {
            return Err("invalid pgvector length".into());
        }

        let values = values
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|chunk| {
                f32::from_bits(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            })
            .collect();
        Ok(PgVector(values))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "vector"
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresBuilder, postgres_placeholders};
    use crate::{AlterTable, SqlTag, Table};

    #[test]
    fn builds_postgres_alter_table_sql() {
        let alter = AlterTable::from_name("users")
            .add_column::<i64, _>("age")
            .drop_column("nickname")
            .rename_column("name", "full_name")
            .rename_to("accounts");

        assert_eq!(
            PostgresBuilder.build_alter_table(&alter).unwrap(),
            vec![
                "ALTER TABLE users ADD COLUMN age BIGINT;",
                "ALTER TABLE users DROP COLUMN nickname;",
                "ALTER TABLE users RENAME COLUMN name TO full_name;",
                "ALTER TABLE users RENAME TO accounts;",
            ]
        );
    }

    #[test]
    fn converts_question_mark_placeholders_to_postgres_placeholders() {
        assert_eq!(
            postgres_placeholders("SELECT * FROM users WHERE id = ? AND name = ? LIMIT ?"),
            "SELECT * FROM users WHERE id = $1 AND name = $2 LIMIT $3"
        );
    }

    #[test]
    fn builds_postgres_create_table_sql() {
        let table = Table::from_name("users")
            .add::<i64, _>("id", &[SqlTag::PrimaryKey])
            .add::<uuid::Uuid, _>("external_id", &[SqlTag::Unique])
            .add::<String, _>("name", &[])
            .add::<crate::FloatVec<3>, _>("embedding", &[]);

        assert_eq!(
            PostgresBuilder.build_table(&table).unwrap(),
            "CREATE TABLE users (id BIGINT PRIMARY KEY , external_id UUID  UNIQUE, name TEXT  , embedding vector(3)  );"
        );
    }
}
