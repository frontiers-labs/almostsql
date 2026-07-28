use crate::migration::{AlterAction, AlterTable, SqlType};
use crate::{Table, Value, VectorSearch};

#[derive(Debug, Clone)]
pub enum SQLError {
    Custom(String),
}

pub enum SQLBuilder {
    #[cfg(feature = "postgres")]
    Postgres(crate::postgres::PostgresBuilder),
    #[cfg(feature = "sqlite")]
    Sqlite(crate::sqlite::SqliteBuilder),
}
impl SQLBuilder {
    pub fn build_table_setup(&self, table: &Table) -> Result<Vec<String>, SQLError> {
        match self {
            #[cfg(feature = "postgres")]
            SQLBuilder::Postgres(b) => b.build_table_setup(table),
            #[cfg(feature = "sqlite")]
            SQLBuilder::Sqlite(b) => b.build_table_setup(table),
        }
    }

    pub fn build_table(&self, table: &Table) -> Result<String, SQLError> {
        match self {
            #[cfg(feature = "postgres")]
            SQLBuilder::Postgres(b) => b.build_table(table),
            #[cfg(feature = "sqlite")]
            SQLBuilder::Sqlite(b) => b.build_table(table),
        }
    }

    pub fn build_alter_table(&self, alter: &AlterTable) -> Result<Vec<String>, SQLError> {
        match self {
            #[cfg(feature = "postgres")]
            SQLBuilder::Postgres(b) => b.build_alter_table(alter),
            #[cfg(feature = "sqlite")]
            SQLBuilder::Sqlite(b) => b.build_alter_table(alter),
        }
    }

    pub fn build_vector_search<Tab>(
        &self,
        search: &VectorSearch<Tab>,
    ) -> Result<(String, Vec<Value>), SQLError> {
        match self {
            #[cfg(feature = "postgres")]
            SQLBuilder::Postgres(b) => b.build_vector_search(search),
            #[cfg(feature = "sqlite")]
            SQLBuilder::Sqlite(b) => b.build_vector_search(search),
        }
    }
}

// ALTER TABLE syntax is shared across backends; only column type spelling
// differs, so each backend passes its own `sql_type` mapping.
pub(crate) fn build_alter_statements(
    table: &str,
    actions: &[AlterAction],
    sql_type: fn(&SqlType) -> String,
) -> Vec<String> {
    actions
        .iter()
        .map(|action| match action {
            AlterAction::AddColumn { name, r#type } => format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                table,
                name,
                sql_type(r#type)
            ),
            AlterAction::DropColumn { name } => {
                format!("ALTER TABLE {} DROP COLUMN {};", table, name)
            }
            AlterAction::RenameColumn { from, to } => {
                format!("ALTER TABLE {} RENAME COLUMN {} TO {};", table, from, to)
            }
            AlterAction::RenameTable { to } => {
                format!("ALTER TABLE {} RENAME TO {};", table, to)
            }
        })
        .collect()
}
