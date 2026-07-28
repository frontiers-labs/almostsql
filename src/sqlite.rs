use std::error::Error as StdError;
use std::thread;
use std::{collections::HashMap, sync::Arc};

use crate::migration::{AlterTable, Command, SqlType};
use crate::query::{QueryResult, Row, Value};
use crate::sql_builder::SQLBuilder;
use ::sqlite::{self, Connection, State, Value as SqliteValue};
use futures::StreamExt;
use futures::channel::{mpsc, oneshot};

#[derive(Clone)]
pub struct SqliteBackend {
    request_tx: Arc<mpsc::UnboundedSender<WorkerRequest>>,
}

enum WorkerRequest {
    Query {
        query: String,
        params: Vec<Value>,
        response: oneshot::Sender<Result<QueryResult, String>>,
    },
}

pub struct SqliteBuilder;

impl SqliteBackend {
    pub fn new(path: &str) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        unsafe {
            sqlite::ffi::sqlite3_auto_extension(Some(sqlite_vec::sqlite3_vec_init));
        }
        let connection = Connection::open(path)?;
        let (request_tx, mut request_rx) = mpsc::unbounded::<WorkerRequest>();

        thread::Builder::new()
            .name("almostsql-sqlite-worker".to_string())
            .spawn(move || {
                futures::executor::block_on(async move {
                    while let Some(request) = request_rx.next().await {
                        match request {
                            WorkerRequest::Query {
                                query,
                                params,
                                response,
                            } => {
                                let result = execute_query(&connection, &query, &params);
                                let _ = response.send(result);
                            }
                        }
                    }
                });
            })?;

        Ok(Self {
            request_tx: Arc::new(request_tx),
        })
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
            .map_err(|_| "sqlite worker is unavailable".to_string())?;

        response_rx
            .await
            .map_err(|_| "sqlite worker stopped before responding".to_string())?
            .map_err(Into::into)
    }

    /// Execute a raw SQL query asynchronously
    pub async fn query(&self, query: &str) -> Result<QueryResult, Box<dyn StdError + Send + Sync>> {
        self.send_query(query, Vec::new()).await
    }

    /// Execute a prepared statement with parameters asynchronously
    pub async fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Box<dyn StdError + Send + Sync>> {
        self.send_query(query, params).await
    }

    pub async fn begin_transaction(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.send_query("BEGIN TRANSACTION;", Vec::new())
            .await
            .map(|_| ())
    }

    pub async fn commit_transaction(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.send_query("COMMIT;", Vec::new()).await.map(|_| ())
    }

    pub async fn rollback_transaction(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.send_query("ROLLBACK;", Vec::new()).await.map(|_| ())
    }

    pub(crate) fn rollback_transaction_fire_and_forget(&self) {
        let (response_tx, _response_rx) = oneshot::channel();
        let _ = self.request_tx.unbounded_send(WorkerRequest::Query {
            query: "ROLLBACK;".to_string(),
            params: Vec::new(),
            response: response_tx,
        });
    }

    pub fn builder(&self) -> SQLBuilder {
        SQLBuilder::Sqlite(SqliteBuilder {})
    }
}

fn execute_query(
    connection: &Connection,
    query: &str,
    params: &[Value],
) -> Result<QueryResult, String> {
    let mut statement = connection.prepare(query).map_err(|e| e.to_string())?;

    for (idx, param) in params.iter().enumerate() {
        match param {
            Value::Null => statement
                .bind((idx + 1, SqliteValue::Null))
                .map_err(|e| e.to_string())?,
            Value::Integer(i) => statement.bind((idx + 1, *i)).map_err(|e| e.to_string())?,
            Value::Real(r) => statement.bind((idx + 1, *r)).map_err(|e| e.to_string())?,
            Value::Text(s) => statement
                .bind((idx + 1, s.as_str()))
                .map_err(|e| e.to_string())?,
            Value::Blob(b) => statement
                .bind((idx + 1, b.as_slice()))
                .map_err(|e| e.to_string())?,
            Value::FloatVector(v) => {
                let bytes = float_vector_bytes(v);
                statement
                    .bind((idx + 1, bytes.as_slice()))
                    .map_err(|e| e.to_string())?
            }
            Value::Uuid(u) => statement
                .bind((idx + 1, u.as_bytes().as_slice()))
                .map_err(|e| e.to_string())?,
        }
    }

    let mut rows = Vec::new();
    let mut affected_rows = 0;

    while let State::Row = statement.next().map_err(|e| e.to_string())? {
        let mut values = HashMap::new();

        for i in 0..statement.column_count() {
            let name = statement
                .column_name(i)
                .map_err(|e| e.to_string())?
                .to_string();
            let value = match statement.column_type(i).map_err(|e| e.to_string())? {
                sqlite::Type::Null => Value::Null,
                sqlite::Type::Integer => {
                    Value::Integer(statement.read::<i64, _>(i).map_err(|e| e.to_string())?)
                }
                sqlite::Type::Float => {
                    Value::Real(statement.read::<f64, _>(i).map_err(|e| e.to_string())?)
                }
                sqlite::Type::String => {
                    Value::Text(statement.read::<String, _>(i).map_err(|e| e.to_string())?)
                }
                sqlite::Type::Binary => {
                    Value::Blob(statement.read::<Vec<u8>, _>(i).map_err(|e| e.to_string())?)
                }
            };

            values.insert(name, value);
        }

        rows.push(Row::new(values));
        affected_rows += 1;
    }

    Ok(QueryResult::new(rows, affected_rows))
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
