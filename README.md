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

## Installation

SQLite:

```toml
[dependencies]
almostsql = "0.1"
```

Postgres:

```toml
[dependencies]
almostsql = { version = "0.1", default-features = false, features = ["postgres"] }
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
