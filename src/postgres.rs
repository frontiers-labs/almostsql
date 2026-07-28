use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::thread;

use bytes::BytesMut;
use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use postgres::{Client, NoTls};

use crate::migration::{AlterTable, Command, SqlType};
use crate::query::{QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;

#[derive(Clone)]
pub struct PostgresBackend {
    request_tx: Arc<mpsc::UnboundedSender<WorkerRequest>>,
}

enum WorkerRequest {
    Query {
        query: String,
        params: Vec<Value>,
        response: oneshot::Sender<Result<QueryResult, String>>,
    },
    Rollback,
}

pub struct PostgresBuilder;

impl PostgresBackend {
    pub fn new(url: &str) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        let url = url.to_string();
        let (request_tx, mut request_rx) = mpsc::unbounded::<WorkerRequest>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        // The synchronous `postgres` client drives its own Tokio runtime via
        // `block_on`, which panics when invoked from a thread already inside the
        // server's async runtime. Pin the client to a dedicated OS thread so its
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

                futures::executor::block_on(async move {
                    while let Some(request) = request_rx.next().await {
                        match request {
                            WorkerRequest::Query {
                                query,
                                params,
                                response,
                            } => {
                                let result = run_query(&mut client, &query, params);
                                let _ = response.send(result);
                            }
                            WorkerRequest::Rollback => {
                                let _ = client.simple_query("ROLLBACK;");
                            }
                        }
                    }
                });
            })?;

        ready_rx
            .recv()
            .map_err(|_| "postgres worker stopped before connecting".to_string())??;

        Ok(Self {
            request_tx: Arc::new(request_tx),
        })
    }

    pub async fn query(&self, query: &str) -> Result<QueryResult, Box<dyn StdError + Send + Sync>> {
        self.send_query(query, Vec::new()).await
    }

    pub async fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Box<dyn StdError + Send + Sync>> {
        self.send_query(query, params).await
    }

    async fn send_query(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Box<dyn StdError + Send + Sync>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx
            .unbounded_send(WorkerRequest::Query {
                query: query.to_string(),
                params,
                response: response_tx,
            })
            .map_err(|_| "postgres worker is unavailable".to_string())?;

        response_rx
            .await
            .map_err(|_| "postgres worker stopped before responding".to_string())?
            .map_err(Into::into)
    }

    pub async fn begin_transaction(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.query("BEGIN;").await.map(|_| ())
    }

    pub async fn commit_transaction(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.query("COMMIT;").await.map(|_| ())
    }

    pub(crate) fn rollback_transaction_fire_and_forget(&self) {
        let _ = self.request_tx.unbounded_send(WorkerRequest::Rollback);
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Postgres(PostgresBuilder {})
    }
}

fn run_query(client: &mut Client, query: &str, params: Vec<Value>) -> Result<QueryResult, String> {
    let query = postgres_placeholders(query);
    let params = postgres_params(params);
    let param_refs = params
        .iter()
        .map(|p| &**p as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    if returns_rows(&query) {
        let rows = client
            .query(&query, &param_refs)
            .map_err(|e| e.to_string())?;
        let rows = rows
            .into_iter()
            .map(postgres_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let affected_rows = rows.len();
        Ok(QueryResult::new(rows, affected_rows))
    } else {
        let affected_rows = client
            .execute(&query, &param_refs)
            .map_err(|e| e.to_string())? as usize;
        Ok(QueryResult::new(Vec::new(), affected_rows))
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
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
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

fn returns_rows(query: &str) -> bool {
    let query = query.trim_start().to_ascii_uppercase();
    query.starts_with("SELECT") || query.starts_with("WITH") || query.contains(" RETURNING ")
}

fn postgres_row(row: postgres::Row) -> Result<Row, Box<dyn StdError + Send + Sync>> {
    let mut values = HashMap::new();

    for (idx, column) in row.columns().iter().enumerate() {
        let ty = column.type_();
        let value = if *ty == Type::INT2 {
            row.try_get::<_, Option<i16>>(idx)?
                .map(|v| Value::Integer(v as i64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::INT4 {
            row.try_get::<_, Option<i32>>(idx)?
                .map(|v| Value::Integer(v as i64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::INT8 {
            row.try_get::<_, Option<i64>>(idx)?
                .map(Value::Integer)
                .unwrap_or(Value::Null)
        } else if *ty == Type::FLOAT4 {
            row.try_get::<_, Option<f32>>(idx)?
                .map(|v| Value::Real(v as f64))
                .unwrap_or(Value::Null)
        } else if *ty == Type::FLOAT8 {
            row.try_get::<_, Option<f64>>(idx)?
                .map(Value::Real)
                .unwrap_or(Value::Null)
        } else if *ty == Type::BOOL {
            row.try_get::<_, Option<bool>>(idx)?
                .map(|v| Value::Integer(if v { 1 } else { 0 }))
                .unwrap_or(Value::Null)
        } else if *ty == Type::BYTEA {
            row.try_get::<_, Option<Vec<u8>>>(idx)?
                .map(Value::Blob)
                .unwrap_or(Value::Null)
        } else if ty.name() == "vector" {
            row.try_get::<_, Option<PgVector>>(idx)?
                .map(|v| Value::FloatVector(v.0))
                .unwrap_or(Value::Null)
        } else if *ty == Type::UUID {
            row.try_get::<_, Option<uuid::Uuid>>(idx)?
                .map(Value::Uuid)
                .unwrap_or(Value::Null)
        } else if *ty == Type::TEXT
            || *ty == Type::VARCHAR
            || *ty == Type::BPCHAR
            || *ty == Type::NAME
        {
            row.try_get::<_, Option<String>>(idx)?
                .map(Value::Text)
                .unwrap_or(Value::Null)
        } else {
            return Err(format!("unsupported postgres column type: {}", ty.name()).into());
        };

        values.insert(column.name().to_string(), value);
    }

    Ok(Row::new(values))
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
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
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
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
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
