# almostsql

A minimalist async Rust database library that feels almost like writing raw
SQL. It provides a parameterized query builder, append-only migrations, a
type-safe schema macro, and vector search for SQLite and Postgres.

The API is pre-1.0 and may change between minor releases.

## Features

- SQLite with bundled SQLite and `sqlite-vec` support (enabled by default)
- Postgres via the optional `postgres` feature
- Typed schema, insert, select, update, and delete builders
- Named migration sets that let independent crates share one database
- Explicit safeguards against accidental unfiltered updates and deletes
- Connection pooling with per-connection prepared-statement caching
- Precompiled queries, batched inserts, and streaming result sets

## Installation

SQLite:

```toml
[dependencies]
almostsql = "0.1"
```

Postgres (synchronous `postgres` driver, no runtime dependency):

```toml
[dependencies]
almostsql = { version = "0.1", default-features = false, features = ["postgres"] }
```

Postgres via `tokio-postgres` (each pooled connection runs on its own
current-thread Tokio runtime, so the public API is unchanged and callers do
not need to be inside a Tokio runtime; takes precedence over `postgres` when
both are enabled):

```toml
[dependencies]
almostsql = { version = "0.1", default-features = false, features = ["postgres-tokio"] }
```

## Example

```rust
use almostsql::{ConnectionPool, migrations};
use uuid::Uuid;

migrations! {
    namespace app {
        v1 {
            table users {
                id: Uuid [PrimaryKey],
                name: String,
                age: i64,
            }
        }
    }
}

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let db = ConnectionPool::new("sqlite::memory:")?;
db.migrator().apply(schema()).run().await?;

let id = Uuid::new_v4();
users::insert()
    .id(id)
    .name("Ada")
    .age(36_i64)
    .execute(&db)
    .await?;

let user = users::select()
    .where_(users::id.eq(id))
    .one(&db)
    .await?;

assert_eq!(user.name, "Ada");
# Ok(())
# }
```

The `migrations!` macro supports tables, primary and unique keys, foreign keys,
raw SQL, table alterations, custom column names, nullable fields, and vector
columns. See the integration tests for complete examples.

## Performance

Statements are prepared once per pooled connection and cached, so repeated
queries with the same SQL text — including everything the typed builders
generate — skip re-compilation automatically. On top of that:

**Precompiled queries.** `prepare` validates a statement eagerly and returns a
reusable handle; executing it ships only the parameter values:

```rust,ignore
let by_id = db.prepare("SELECT name FROM users WHERE id = ?").await?;
let user = by_id.query(vec![Value::Uuid(id)]).await?;
```

The typed select builder can be precompiled too: `*_param()` comparisons
become bind parameters supplied at execution time (in order of appearance),
and results decode into the table's typed rows:

```rust,ignore
let adults_by_age = users::select()
    .where_(users::age.ge_param())
    .prepare(&db)
    .await?;
let rows = adults_by_age.all(almostsql::params![18_i64]).await?;
```

**Batched inserts.** `insert_batch` writes many rows per statement (chunked to
respect bind-parameter limits, atomic across chunks):

```rust,ignore
let mut batch = users::insert_batch();
for user in new_users {
    batch = batch.add(users::insert().id(user.id).name(user.name).age(user.age));
}
batch.execute(&db).await?;
```

**Streaming results.** `query_stream` yields rows in bounded chunks instead of
materializing the whole result set:

```rust,ignore
use futures::TryStreamExt;
let mut rows = db.query_stream("SELECT * FROM users;", Vec::new()).await?;
while let Some(row) = rows.try_next().await? {
    /* ... */
}
```

**Connection pooling.** File-backed SQLite (in WAL mode) and Postgres pools
hold several connections, so independent queries run concurrently. In-memory
SQLite keeps a single connection, since each `:memory:` connection would be a
separate database.

## Transactions

A transaction checks a dedicated connection out of the pool. Issue its
statements through the `Transaction` handle — the typed builders accept either
a pool or a transaction:

```rust,ignore
let tx = db.transaction().await?;
users::insert().id(id).name("Ada").age(36_i64).execute(&tx).await?;
tx.commit().await?; // dropping the handle without commit rolls back
```

Queries made on the pool while a transaction is open run on other connections
and do not join it.

## Database URLs

- In-memory SQLite: `sqlite::memory:`
- SQLite file: `sqlite:path/to/database.sqlite`
- Postgres: `postgres://user:password@host/database`

## Minimum supported Rust version

Rust 1.88 or newer.

## Publishing

The repository has a manual-only **Publish release** GitHub Actions workflow.
Run it from the default branch with the exact version found in both manifests.
Its `dry_run` input defaults to `true`.

Before the first real release, create a GitHub `release` environment and add a
`CARGO_REGISTRY_TOKEN` secret to it. The workflow validates and packages both
crates, creates a draft GitHub release, publishes `almostsql-macros` before
`almostsql`, then publishes the GitHub release.

## License

MIT
