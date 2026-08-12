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
pub use connection_pool::{ConnectionPool, Executor, Migrator, PreparedQuery};
pub use dsl::{
    Column, ColumnInput, Delete, Expr, IntoValue, Select, SelectCols, SelectList, Update,
    VectorSearch, vector_search,
};
pub use error::Error;
pub use migration::{
    AlterTable, BitVec, FloatVec, Int8Vec, Migration, SchemaSet, SqlLikeType, SqlTag, SqlType,
    Table,
};
pub use pool::RowStream;
pub use query::{Columns, DecodeError, FromValue, QueryResult, Row, Transaction, Value};

// Re-export proc macros for type-safe schema generation
pub use almostsql_macros::migrations;
