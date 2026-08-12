use crate::error::Error;
use crate::pool::RowStream;
#[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
use crate::postgres::PostgresBackend;
use crate::query::{QueryResult, Value};
use crate::sql_builder::SQLBuilder;
#[cfg(feature = "sqlite")]
use crate::sqlite::SqliteBackend;
use crate::{Migration, SchemaSet};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;

/// Something queries can be executed on: a [`ConnectionPool`] (any pooled
/// connection) or a [`Transaction`] (its checked-out connection).
pub trait Executor {
    fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> impl Future<Output = Result<QueryResult, Error>> + Send;

    fn query(&self, query: &str) -> impl Future<Output = Result<QueryResult, Error>> + Send {
        self.query_with_params(query, Vec::new())
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    backend: Arc<Backend>,
}

#[derive(Clone)]
pub enum Backend {
    #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
    Postgres(PostgresBackend),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteBackend),
}

impl ConnectionPool {
    pub fn new(url: &str) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        let backend = if let Some(path) = url.strip_prefix("sqlite:") {
            #[cfg(feature = "sqlite")]
            {
                Backend::Sqlite(SqliteBackend::new(path)?)
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err(format!(
                    "SQLite support is not enabled; rebuild with the \"sqlite\" feature: {path}"
                )
                .into());
            }
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            {
                Backend::Postgres(PostgresBackend::new(url)?)
            }
            #[cfg(not(any(feature = "postgres", feature = "postgres-tokio")))]
            {
                return Err(format!(
                    "Postgres support is not enabled; rebuild with the \"postgres\" feature: {}",
                    url
                )
                .into());
            }
        } else {
            return Err(format!("Unsupported database URL format: {}", url).into());
        };

        Ok(ConnectionPool {
            backend: Arc::new(backend),
        })
    }

    /// Apply a single unnamed migration history. Equivalent to building a
    /// [`Migrator`] with one default-namespace [`SchemaSet`].
    pub async fn initialize_database(
        &self,
        migrations: Vec<Migration>,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.migrator()
            .apply(SchemaSet::new("", migrations))
            .run()
            .await
    }

    /// Start composing several named schema fragments onto this pool.
    pub fn migrator(&self) -> Migrator<'_> {
        Migrator {
            pool: self,
            sets: Vec::new(),
        }
    }

    /// Create the ledger if missing and upgrade legacy `(id, hash)` ledgers to
    /// the namespaced `(namespace, id, hash)` layout. Runs in autocommit so the
    /// column probe can fail harmlessly on Postgres.
    pub async fn ensure_migrations_table(&self) -> Result<(), Error> {
        self.query(
            "CREATE TABLE IF NOT EXISTS __migrations (namespace TEXT NOT NULL DEFAULT '', \
             id BIGINT NOT NULL, hash BIGINT NOT NULL, PRIMARY KEY (namespace, id));",
        )
        .await?;

        let needs_upgrade = self
            .query("SELECT namespace FROM __migrations LIMIT 1;")
            .await
            .is_err();

        if needs_upgrade {
            // Rebuild rather than ADD COLUMN so the composite (namespace, id)
            // primary key is in place; otherwise a second namespace's id=0 would
            // collide with the default namespace's id=0.
            let transaction = self.transaction().await?;
            transaction
                .query("ALTER TABLE __migrations RENAME TO __migrations_legacy;")
                .await?;
            transaction
                .query(
                    "CREATE TABLE __migrations (namespace TEXT NOT NULL DEFAULT '', \
                     id BIGINT NOT NULL, hash BIGINT NOT NULL, PRIMARY KEY (namespace, id));",
                )
                .await?;
            transaction
                .query(
                    "INSERT INTO __migrations (namespace, id, hash) \
                     SELECT '', id, hash FROM __migrations_legacy;",
                )
                .await?;
            transaction.query("DROP TABLE __migrations_legacy;").await?;
            transaction.commit().await?;
        }

        Ok(())
    }

    /// Apply the pending migrations of one namespace on the transaction's
    /// connection. Assumes the ledger table exists.
    async fn apply_pending(
        &self,
        transaction: &Transaction,
        namespace: &str,
        migrations: &[Migration],
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let hashes = migrations.iter().map(migration_hash).collect::<Vec<_>>();

        let existing = transaction
            .query_with_params(
                "SELECT hash FROM __migrations WHERE namespace = ? ORDER BY id;",
                vec![Value::Text(namespace.to_string())],
            )
            .await?;
        let existing = existing
            .rows()
            .iter()
            .map(|m| m.get_int("hash").unwrap() as u64)
            .collect::<Vec<_>>();

        for (idx, (e, m)) in existing.iter().zip(hashes.iter()).enumerate() {
            if e != m {
                panic!(
                    "Migration mismatched for namespace {:?} index {}: {:?}",
                    namespace, idx, migrations[idx]
                );
            }
        }

        if existing.len() >= migrations.len() {
            // The database already has this many (or more) migrations applied.
            return Ok(());
        }

        for (idx, (m, hash)) in migrations
            .iter()
            .zip(hashes.iter())
            .enumerate()
            .skip(existing.len())
        {
            for t in m.get_tables() {
                for sql in self.backend.builder().build_table_setup(t).unwrap() {
                    transaction.query(&sql).await?;
                }
                // FIXME correctly handle error
                let sql = t.to_sql(&self.backend).unwrap();
                transaction.query(&sql).await?;
            }

            for a in m.get_alters() {
                // FIXME correctly handle error
                for sql in a.to_sql(&self.backend).unwrap() {
                    transaction.query(&sql).await?;
                }
            }

            for r in m.get_raw_queries() {
                transaction.query(r).await?;
            }

            transaction
                .query_with_params(
                    "INSERT INTO __migrations (namespace, id, hash) VALUES(?, ?, ?)",
                    vec![
                        Value::Text(namespace.to_string()),
                        Value::Integer(idx as i64),
                        Value::Integer(*hash as i64),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    /// Execute a raw SQL query asynchronously
    pub async fn query(&self, query: &str) -> Result<QueryResult, Error> {
        self.backend.query(Arc::from(query), Vec::new()).await
    }

    /// Execute a parameterized statement asynchronously. Statements are
    /// prepared once per pooled connection and cached, so repeated calls with
    /// the same SQL text skip re-compilation.
    pub async fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        self.backend.query(Arc::from(query), params).await
    }

    /// Precompile a statement and return a reusable handle. The statement is
    /// prepared eagerly (so syntax errors surface here) and cached on each
    /// pooled connection it runs on; executing the handle skips SQL
    /// generation and re-preparation entirely.
    pub async fn prepare(&self, query: &str) -> Result<PreparedQuery, Error> {
        let sql: Arc<str> = Arc::from(query);
        self.backend.prepare_only(sql.clone()).await?;
        Ok(PreparedQuery {
            backend: self.backend.clone(),
            sql,
        })
    }

    /// Execute a query and stream its rows without materializing the whole
    /// result set. Rows are produced incrementally from a checked-out
    /// connection; dropping the stream releases it and abandons the rest.
    pub async fn query_stream(&self, query: &str, params: Vec<Value>) -> Result<RowStream, Error> {
        self.backend.query_stream(Arc::from(query), params).await
    }

    pub async fn vector_search<Tab>(
        &self,
        search: crate::VectorSearch<Tab>,
    ) -> Result<QueryResult, Error> {
        let (query, params) = self.backend.builder().build_vector_search(&search).unwrap();
        self.query_with_params(&query, params).await
    }

    pub async fn repair_legacy_vector_column(
        &self,
        schema: &SchemaSet,
        migration_id: usize,
        table: &'static str,
        id_column: &'static str,
        vector_column: &'static str,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        validate_identifier(table)?;
        validate_identifier(id_column)?;
        validate_identifier(vector_column)?;

        match &*self.backend {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            Backend::Postgres(_) => {
                self.repair_postgres_legacy_vector_column(table, id_column, vector_column)
                    .await?;
            }
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(_) => {
                self.query(&format!(
                    "SELECT {id_column}, {vector_column} FROM {table} LIMIT 0"
                ))
                .await?;
            }
        }

        self.repair_migration_hash(schema, migration_id).await
    }

    #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
    async fn repair_postgres_legacy_vector_column(
        &self,
        table: &'static str,
        id_column: &'static str,
        vector_column: &'static str,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.query("CREATE EXTENSION IF NOT EXISTS vector;").await?;

        let column = self
            .query_with_params(
                "SELECT udt_name FROM information_schema.columns \
                 WHERE table_name = ? AND column_name = ?",
                vec![
                    Value::Text(table.to_string()),
                    Value::Text(vector_column.to_string()),
                ],
            )
            .await?;
        let Some(row) = column.rows().first() else {
            return Err(format!("missing column {table}.{vector_column}").into());
        };
        match row.get_text("udt_name") {
            Some("vector") => return Ok(()),
            Some("bytea") => {}
            Some(other) => {
                return Err(format!("cannot repair {table}.{vector_column}: got {other}").into());
            }
            None => return Err(format!("cannot read type of {table}.{vector_column}").into()),
        }

        let repair_column = "__almostsql_vector_repair";
        let select_sql =
            format!("SELECT {id_column} AS id, {vector_column} AS vector FROM {table}");
        let rows = self.query(&select_sql).await?;
        let mut converted = Vec::new();
        for row in rows.rows() {
            let id = row.decode::<uuid::Uuid>("id")?;
            match row.get("vector") {
                Some(Value::Null) => {}
                Some(Value::Blob(bytes)) => {
                    converted.push((id, legacy_float_vector(bytes)?));
                }
                Some(other) => {
                    return Err(format!("expected legacy vector blob, got {other}").into());
                }
                None => return Err("missing selected vector column".into()),
            }
        }

        self.query(&format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {repair_column} vector;"
        ))
        .await?;
        for (id, vector) in converted {
            self.query_with_params(
                &format!("UPDATE {table} SET {repair_column} = ? WHERE {id_column} = ?"),
                vec![Value::FloatVector(vector), Value::Uuid(id)],
            )
            .await?;
        }

        let transaction = self.transaction().await?;
        transaction
            .query(&format!("ALTER TABLE {table} DROP COLUMN {vector_column};"))
            .await?;
        transaction
            .query(&format!(
                "ALTER TABLE {table} RENAME COLUMN {repair_column} TO {vector_column};"
            ))
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn repair_migration_hash(
        &self,
        schema: &SchemaSet,
        migration_id: usize,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.ensure_migrations_table().await?;
        let Some(migration) = schema.migrations().get(migration_id) else {
            return Err(format!("missing migration index {migration_id}").into());
        };
        let namespace = schema.name().to_string();
        let migration_id = migration_id as i64;
        let hash = migration_hash(migration) as i64;
        let existing = self
            .query_with_params(
                "SELECT hash FROM __migrations WHERE namespace = ? AND id = ?",
                vec![Value::Text(namespace.clone()), Value::Integer(migration_id)],
            )
            .await?;
        if existing.is_empty() {
            self.query_with_params(
                "INSERT INTO __migrations (namespace, id, hash) VALUES (?, ?, ?)",
                vec![
                    Value::Text(namespace),
                    Value::Integer(migration_id),
                    Value::Integer(hash),
                ],
            )
            .await?;
        } else {
            self.query_with_params(
                "UPDATE __migrations SET hash = ? WHERE namespace = ? AND id = ?",
                vec![
                    Value::Integer(hash),
                    Value::Text(namespace),
                    Value::Integer(migration_id),
                ],
            )
            .await?;
        }
        Ok(())
    }

    /// Begin a transaction on a dedicated connection checked out from the
    /// pool. Issue the transaction's statements through the returned
    /// [`Transaction`]; queries made on the pool meanwhile run on other
    /// connections and do not join the transaction.
    pub async fn transaction(&self) -> Result<Transaction, Error> {
        let inner = match &*self.backend {
            #[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
            Backend::Postgres(postgres) => TransactionInner::PgQueue(postgres.begin().await?),
            #[cfg(feature = "postgres-tokio")]
            Backend::Postgres(postgres) => TransactionInner::Postgres(postgres.begin().await?),
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => TransactionInner::Sqlite(sqlite.begin().await?),
        };
        Ok(Transaction {
            inner,
            finished: false,
        })
    }
}

/// A database transaction pinned to one connection checked out of the pool.
///
/// Queries issued through the transaction run on that connection; queries
/// issued through the [`ConnectionPool`] while a transaction is open run on
/// *other* connections and do not see uncommitted changes. Dropping the
/// transaction without calling [`commit`](Self::commit) rolls it back.
pub struct Transaction {
    inner: TransactionInner,
    finished: bool,
}

enum TransactionInner {
    #[cfg(feature = "sqlite")]
    Sqlite(crate::sqlite::SqliteTransaction),
    #[cfg(feature = "postgres-tokio")]
    Postgres(crate::postgres::PgTransaction),
    #[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
    PgQueue(crate::pool::RequestQueue),
}

impl Transaction {
    /// Execute a raw SQL query on the transaction's connection.
    pub async fn query(&self, query: &str) -> Result<QueryResult, Error> {
        self.query_with_params(query, Vec::new()).await
    }

    /// Execute a parameterized statement on the transaction's connection.
    pub async fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        match &self.inner {
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => tx.query(Arc::from(query), params).await,
            #[cfg(feature = "postgres-tokio")]
            TransactionInner::Postgres(tx) => tx.query(Arc::from(query), params).await,
            #[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
            TransactionInner::PgQueue(queue) => queue.query(Arc::from(query), params).await,
        }
    }

    pub async fn commit(mut self) -> Result<(), Error> {
        self.query("COMMIT;").await?;
        self.finish();
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        self.query("ROLLBACK;").await?;
        self.finish();
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
        match &self.inner {
            #[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
            TransactionInner::PgQueue(queue) => {
                let _ = queue.sender().try_send(crate::pool::Request::Release);
            }
            // Slot-based transactions release their connection when the
            // guard inside the inner handle drops.
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        match &self.inner {
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => tx.rollback_blocking(),
            #[cfg(feature = "postgres-tokio")]
            TransactionInner::Postgres(tx) => tx.rollback_spawn(),
            #[cfg(all(feature = "postgres", not(feature = "postgres-tokio")))]
            TransactionInner::PgQueue(queue) => {
                let (response, _) = futures::channel::oneshot::channel();
                let _ = queue.sender().try_send(crate::pool::Request::Query {
                    sql: Arc::from("ROLLBACK;"),
                    params: Vec::new(),
                    response,
                });
                let _ = queue.sender().try_send(crate::pool::Request::Release);
            }
        }
    }
}

impl Executor for ConnectionPool {
    fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> impl Future<Output = Result<QueryResult, Error>> + Send {
        ConnectionPool::query_with_params(self, query, params)
    }
}

impl Executor for Transaction {
    fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> impl Future<Output = Result<QueryResult, Error>> + Send {
        Transaction::query_with_params(self, query, params)
    }
}

/// A precompiled statement bound to a connection pool.
///
/// Created with [`ConnectionPool::prepare`]. Executing it ships only the
/// parameter values to a connection worker; the SQL text travels as a shared
/// reference and the statement itself is prepared at most once per pooled
/// connection.
#[derive(Clone)]
pub struct PreparedQuery {
    backend: Arc<Backend>,
    sql: Arc<str>,
}

impl PreparedQuery {
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Execute the statement and return its rows.
    pub async fn query(&self, params: Vec<Value>) -> Result<QueryResult, Error> {
        self.backend.query(self.sql.clone(), params).await
    }

    /// Execute the statement and return the number of affected rows.
    pub async fn execute(&self, params: Vec<Value>) -> Result<usize, Error> {
        Ok(self.query(params).await?.affected_rows())
    }

    /// Execute the statement and stream its rows.
    pub async fn query_stream(&self, params: Vec<Value>) -> Result<RowStream, Error> {
        self.backend.query_stream(self.sql.clone(), params).await
    }
}

/// A precompiled statement built from a typed builder: a [`PreparedQuery`]
/// plus the builder's fixed parameter values and the positions of its
/// `*_param()` holes. Executing it merges the caller's arguments into the
/// holes, in order of appearance.
pub struct PreparedStatement {
    prepared: PreparedQuery,
    slots: Vec<crate::dsl::ParamSlot>,
    holes: usize,
}

impl PreparedStatement {
    pub(crate) fn new(prepared: PreparedQuery, slots: Vec<crate::dsl::ParamSlot>) -> Self {
        let holes = slots
            .iter()
            .filter(|slot| matches!(slot, crate::dsl::ParamSlot::Hole))
            .count();
        Self {
            prepared,
            slots,
            holes,
        }
    }

    pub fn sql(&self) -> &str {
        self.prepared.sql()
    }

    pub(crate) async fn query(&self, args: Vec<Value>) -> Result<QueryResult, Error> {
        if args.len() != self.holes {
            return Err(Error::InvalidQuery(format!(
                "prepared query takes {} parameter(s), got {}",
                self.holes,
                args.len()
            )));
        }
        let mut merged = Vec::with_capacity(self.slots.len());
        let mut args = args.into_iter();
        for slot in &self.slots {
            match slot {
                crate::dsl::ParamSlot::Fixed(value) => merged.push(value.clone()),
                crate::dsl::ParamSlot::Hole => {
                    merged.push(args.next().expect("hole count was checked"))
                }
            }
        }
        self.prepared.query(merged).await
    }
}

impl Backend {
    pub(crate) fn builder(&self) -> SQLBuilder {
        match self {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            Backend::Postgres(postgres) => postgres.builder(),
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.builder(),
        }
    }

    pub(crate) async fn query(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        match self {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            Backend::Postgres(postgres) => postgres.query(sql, params).await,
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.query(sql, params).await,
        }
    }

    pub(crate) async fn prepare_only(&self, sql: Arc<str>) -> Result<(), Error> {
        match self {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            Backend::Postgres(postgres) => postgres.prepare_only(sql).await,
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.prepare_only(sql).await,
        }
    }

    pub(crate) async fn query_stream(
        &self,
        sql: Arc<str>,
        params: Vec<Value>,
    ) -> Result<RowStream, Error> {
        match self {
            #[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
            Backend::Postgres(postgres) => postgres.query_stream(sql, params).await,
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.query_stream(sql, params).await,
        }
    }
}

fn migration_hash(m: &Migration) -> u64 {
    m.fingerprint()
}

fn validate_identifier(identifier: &str) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err("empty SQL identifier".into());
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(format!("invalid SQL identifier: {identifier}").into());
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(format!("invalid SQL identifier: {identifier}").into());
    }
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "postgres-tokio"))]
fn legacy_float_vector(bytes: &[u8]) -> Result<Vec<f32>, Box<dyn StdError + Send + Sync>> {
    if !bytes.len().is_multiple_of(std::mem::size_of::<f32>()) {
        return Err("legacy vector blob length is not a multiple of 4".into());
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Composes several named [`SchemaSet`]s onto a single pool. Each namespace
/// keeps its own append-only history in the `__migrations` ledger, so fragments
/// defined in different crates can share one database without their migration
/// indices colliding.
///
/// Apply order matters: list a fragment before any fragment that references its
/// tables via foreign keys.
pub struct Migrator<'a> {
    pool: &'a ConnectionPool,
    sets: Vec<SchemaSet>,
}

impl<'a> Migrator<'a> {
    pub fn apply(mut self, set: SchemaSet) -> Self {
        self.sets.push(set);
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.pool.ensure_migrations_table().await?;

        let transaction = self.pool.transaction().await?;
        for set in &self.sets {
            self.pool
                .apply_pending(&transaction, set.name(), set.migrations())
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}
