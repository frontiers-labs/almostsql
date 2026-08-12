mod batch;
mod connection_pool;
mod dsl;
mod error;
mod migration;
mod pool;
#[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
mod postgres;
mod query;
mod sql_builder;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use batch::BatchInsert;
pub use connection_pool::{
    ConnectionPool, Executor, Migrator, PreparedQuery, PreparedStatement, Transaction,
};
pub use dsl::{
    Column, ColumnInput, Delete, Expr, Insert, IntoValue, PreparedExec, PreparedSelect,
    PreparedSelectCols, Select, SelectCols, SelectList, Update, VectorSearch, vector_search,
};
pub use error::Error;
pub use migration::{
    AlterTable, BitVec, FloatVec, Int8Vec, Migration, SchemaSet, SqlLikeType, SqlTag, SqlType,
    Table,
};
pub use pool::RowStream;
pub use query::{Columns, DecodeError, FromValue, QueryResult, Row, Value};

// Re-export proc macros for type-safe schema generation
pub use almostsql_macros::migrations;

/// Build a `Vec<Value>` of bind parameters from Rust values:
/// `params![id, "name", 42_i64]`.
#[macro_export]
macro_rules! params {
    () => { Vec::<$crate::Value>::new() };
    ($($value:expr),+ $(,)?) => {
        vec![$($crate::IntoValue::into_value($value)),+]
    };
}
