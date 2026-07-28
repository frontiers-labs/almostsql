mod connection_pool;
mod dsl;
mod migration;
#[cfg(feature = "postgres")]
mod postgres;
mod query;
mod sql_builder;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use connection_pool::{ConnectionPool, Migrator};
pub use dsl::{
    Column, ColumnInput, Delete, Expr, IntoValue, Select, SelectCols, SelectList, Update,
    VectorSearch, vector_search,
};
pub use migration::{
    AlterTable, BitVec, FloatVec, Int8Vec, Migration, SchemaSet, SqlLikeType, SqlTag, SqlType,
    Table,
};
pub use query::{DecodeError, FromValue, QueryResult, Row, Value};

// Re-export proc macros for type-safe schema generation
pub use almostsql_macros::migrations;
