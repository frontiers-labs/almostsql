use crate::connection_pool::Executor;
use crate::error::Error;
use crate::query::Value;
use crate::{ConnectionPool, Transaction};

/// Placeholder budget per statement. SQLite's default variable limit is
/// 32766 (3.32+) and Postgres caps at 65535; stay under both.
const MAX_PARAMS_PER_STATEMENT: usize = 30_000;

/// A multi-row INSERT that writes in chunked `VALUES` statements instead of
/// one round-trip per row.
///
/// Every pushed row must bind the same columns. Chunks are sized to stay
/// under the backends' bind-parameter limits, and full-size chunks share one
/// SQL text so they hit the per-connection statement cache.
pub struct BatchInsert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    error: Option<Error>,
}

impl BatchInsert {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: Vec::new(),
            rows: Vec::new(),
            error: None,
        }
    }

    /// Add one row. The first row fixes the column set; later rows must use
    /// the same columns in the same order. A mismatch is reported when the
    /// batch executes.
    pub fn push<S: AsRef<str>>(&mut self, columns: &[S], values: Vec<Value>) {
        if self.error.is_some() {
            return;
        }
        if columns.len() != values.len() {
            self.error = Some(Error::InvalidQuery(format!(
                "batch insert row binds {} values for {} columns",
                values.len(),
                columns.len()
            )));
            return;
        }
        if self.rows.is_empty() && self.columns.is_empty() {
            self.columns = columns.iter().map(|c| c.as_ref().to_string()).collect();
        } else if self.columns.len() != columns.len()
            || !self
                .columns
                .iter()
                .zip(columns.iter())
                .all(|(a, b)| a == b.as_ref())
        {
            self.error = Some(Error::InvalidQuery(format!(
                "batch insert rows must share one column set; expected [{}], got [{}]",
                self.columns.join(", "),
                columns
                    .iter()
                    .map(|c| c.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
            return;
        }
        self.rows.push(values);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn rows_per_chunk(&self) -> usize {
        (MAX_PARAMS_PER_STATEMENT / self.columns.len().max(1)).max(1)
    }

    fn chunk_sql(&self, rows: usize) -> String {
        let placeholders_one_row = format!(
            "({})",
            std::iter::repeat_n("?", self.columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut sql = String::with_capacity(64 + rows * (placeholders_one_row.len() + 2));
        sql.push_str("INSERT INTO ");
        sql.push_str(&self.table);
        sql.push_str(" (");
        sql.push_str(&self.columns.join(", "));
        sql.push_str(") VALUES ");
        for i in 0..rows {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&placeholders_one_row);
        }
        sql.push(';');
        sql
    }

    fn take_validated(mut self) -> Result<Vec<Vec<Value>>, Error> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        Ok(std::mem::take(&mut self.rows))
    }

    /// Execute the batch on the pool. Multi-chunk batches run inside one
    /// transaction so the insert is atomic.
    pub async fn execute(self, db: &ConnectionPool) -> Result<usize, Error> {
        let rows_per_chunk = self.rows_per_chunk();
        if self.rows.len() <= rows_per_chunk {
            return self.execute_chunks(db).await;
        }
        let transaction = db.transaction().await?;
        let inserted = self.execute_chunks(&transaction).await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    /// Execute the batch on an existing transaction's connection.
    pub async fn execute_in(self, transaction: &Transaction) -> Result<usize, Error> {
        self.execute_chunks(transaction).await
    }

    async fn execute_chunks<E: Executor>(self, executor: &E) -> Result<usize, Error> {
        let rows_per_chunk = self.rows_per_chunk();
        let full_chunk_sql = self.chunk_sql(rows_per_chunk);
        let columns = self.columns.len().max(1);
        // Compute the tail chunk's SQL before consuming the rows.
        let tail = self.rows.len() % rows_per_chunk;
        let tail_sql = if tail > 0 {
            self.chunk_sql(tail)
        } else {
            String::new()
        };
        let rows = self.take_validated()?;
        if rows.is_empty() {
            return Ok(0);
        }

        let mut inserted = 0;
        let mut rows = rows.into_iter().peekable();
        while rows.peek().is_some() {
            let chunk: Vec<Vec<Value>> = rows.by_ref().take(rows_per_chunk).collect();
            let sql = if chunk.len() == rows_per_chunk {
                &full_chunk_sql
            } else {
                &tail_sql
            };
            let mut params = Vec::with_capacity(chunk.len() * columns);
            for row in chunk {
                params.extend(row);
            }
            inserted += executor
                .query_with_params(sql, params)
                .await?
                .affected_rows();
        }
        Ok(inserted)
    }
}
