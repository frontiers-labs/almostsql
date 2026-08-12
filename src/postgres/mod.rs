//! Postgres backend. Two interchangeable drivers sit behind the same worker
//! protocol: the synchronous `postgres` client (feature `postgres`) and
//! `tokio-postgres` on a per-connection current-thread runtime (feature
//! `postgres-tokio`, which wins when both are enabled). All row/parameter
//! conversion is shared — both drivers expose the same `tokio_postgres` types.

use std::sync::Arc;

use bytes::BytesMut;

#[cfg(feature = "postgres-tokio")]
pub(crate) use tokio_postgres as driver;

#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
pub(crate) use postgres as driver;

#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
mod sync_worker;
#[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
use sync_worker::spawn_worker;

#[cfg(feature = "postgres-tokio")]
mod tokio_worker;
#[cfg(feature = "postgres-tokio")]
use tokio_worker::spawn_worker;

use driver::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

use crate::error::Error;
use crate::migration::{AlterTable, Command, SqlType};
use crate::pool::RequestQueue;
use crate::query::{Columns, Row, Value};
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

struct CachedStatement {
    statement: driver::Statement,
}

impl CachedStatement {
    fn result_columns(&self) -> Arc<Columns> {
        Arc::new(Columns::new(
            self.statement
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect(),
        ))
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

fn postgres_row(row: &driver::Row, columns: &Arc<Columns>) -> Result<Row, Error> {
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

fn try_get<'a, T: FromSql<'a>>(row: &'a driver::Row, idx: usize) -> Result<Option<T>, Error> {
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
