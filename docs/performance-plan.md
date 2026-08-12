# almostsql performance improvement plan

> **Status: implemented.** All four phases landed (plus phase 0). Measured on
> the shared micro-benchmark (in-memory SQLite, release build): 10k-row scans
> ~1.75× faster, batched inserts ~21× faster per row than single-row inserts,
> UPDATE/DELETE affected-row counts fixed, and `cargo bench` now tracks the
> baselines. One deviation from the plan: the Postgres backend keeps the
> synchronous `postgres` crate (now with per-connection statement caching and
> four pooled connections) instead of migrating to `tokio-postgres`; the
> migration remains a possible follow-up for pipelining.

This document describes the current performance problems in almostsql and a
phased plan to fix them. The headline gap — no way to precompile a query — is
real, but it is a symptom of several deeper issues in the execution path. The
plan is ordered so that each phase delivers a measurable win on its own and
lays the groundwork for the next one.

## Where the time goes today

Tracing one `users::select().where_(...).one(&db)` call end to end:

1. The builder regenerates the SQL string from scratch (`Select::to_sql` in
   `src/dsl.rs`), allocating a fresh `String` and `Vec<Value>` per call.
2. `ConnectionPool::query_with_params` clones the SQL into a `String` again to
   ship it over an unbounded channel to a single worker thread
   (`src/sqlite.rs`, `src/postgres.rs`).
3. The worker **prepares the statement from scratch on every call**. SQLite
   calls `connection.prepare(query)` per request; the synchronous `postgres`
   client re-prepares per `query()` call as well. Nothing is cached, and there
   is no API to prepare once and execute many times.
4. On Postgres, `postgres_placeholders` rewrites `?` → `$n` character by
   character on every call, and `returns_rows` uppercases the entire query
   string just to sniff the verb.
5. Every result row is materialized as a `HashMap<String, Value>`
   (`src/query.rs`), which means one heap-allocated `String` **per column per
   row** for names the caller already knows, plus hashing on every access.
   `String`/`FloatVec` decoding then clones the values out of the map again.
6. All rows are buffered eagerly into a `Vec<Row>` — there is no streaming and
   no way to bound memory for large result sets.
7. Every error path allocates: errors travel as `String` /
   `Box<dyn Error + Send + Sync>` throughout.

On top of the per-query costs, the concurrency story caps throughput:

- `ConnectionPool` is not a pool. Both backends hold exactly **one**
  connection, serviced by **one** OS thread, so all queries serialize.
  A transaction "begins" by sending `BEGIN` down the shared connection, which
  means an in-flight transaction blocks (and can interleave with) every other
  caller — a correctness hazard as well as a throughput ceiling.
- The Postgres backend uses the synchronous `postgres` crate, which internally
  drives its own Tokio runtime — so each query pays two channel hops, a thread
  context switch, and a nested-runtime `block_on`.

Minor but free wins spotted along the way:

- `sqlparser` is declared in `Cargo.toml` but never used by either crate —
  dead compile-time weight.
- The SQLite worker never enables WAL mode or tunes synchronous/cache
  pragmas, leaving significant file-backed write throughput on the table.
- Inserts are single-row only; there is no multi-row `VALUES` batching.

## Phase 1 — Prepared statements and statement caching

Goal: prepare once, execute many. This is the largest single win for hot
query paths and directly addresses the "no way to precompile" complaint.

1. **Worker-side statement cache.** Inside each backend worker, add an LRU
   cache keyed by SQL text (`HashMap<String, CachedStatement>` + LRU list,
   default capacity ~256, configurable). `execute_query` looks up the cache,
   resets and re-binds on hit, prepares and inserts on miss. Because
   statements live on the worker thread with the connection, no `Send`
   gymnastics are needed. This transparently speeds up *existing* callers —
   the DSL emits identical SQL text for identical query shapes, so repeat
   queries hit the cache without any API change.
   - SQLite: keep the `sqlite::Statement` (reset between uses). Invalidate
     the cache on schema-changing statements (any DDL) or on
     `SQLITE_SCHEMA` errors.
   - Postgres: cache the prepared `Statement` handle; do the `?` → `$n`
     rewrite and the `returns_rows` check once at prepare time and store the
     results alongside the handle.
2. **Public `prepare` API.** Add an explicit handle for callers who want
   guaranteed precompilation:

   ```rust
   let stmt: PreparedQuery = db.prepare("SELECT * FROM users WHERE id = ?").await?;
   let result = stmt.query(vec![Value::Uuid(id)]).await?;
   ```

   `PreparedQuery` holds the pool handle plus a cache key/statement id; it
   pins the entry in the worker cache for its lifetime and unpins on drop.
3. **DSL integration.** Give the typed builders a `.prepare(&db)` step that
   returns a typed prepared handle (e.g. `PreparedSelect<Tab, C>`) whose
   `bind(...)` accepts only the parameter values, so per-call work is just
   binding — no SQL generation at all. A `Placeholder<T>` expression variant
   (`users::age.gt(param::<i64>())`) lets a query be built once with holes
   and executed with different values.
4. **Stop rebuilding SQL per call in the DSL.** For the common no-placeholder
   path, cache the generated SQL string in the builder (build lazily, store
   in the struct) so `to_sql` never runs twice for one builder.

Deliverables: statement cache in both backends, `ConnectionPool::prepare`,
`PreparedQuery`, typed prepared builders in the macro crate, cache
metrics (hits/misses) exposed for testing.

## Phase 2 — Cheap rows and less copying

Goal: cut per-row and per-value allocations, which dominate result-heavy
workloads.

1. **Replace `HashMap<String, Value>` rows.** A `QueryResult` gets one shared
   `Arc<[String]>` of column names (built once per statement execution — and
   cacheable per prepared statement) plus a `Arc<HashMap<&str, usize>>` index.
   Each `Row` becomes a `Vec<Value>` and a clone of the shared header. Name
   lookups become one hash probe into a shared map, and per-row per-column
   name allocation disappears entirely. Positional access
   (`row.get_index(i)`) becomes free.
2. **Decode without cloning.** `decode`/`FromValue` should be able to take
   ownership: add a consuming `Row::take::<T>(column)` / `into_decode` path so
   `String`, `Vec<u8>`, and vector values move out instead of being cloned.
   The macro-generated `from_row` for typed selects should use it.
3. **Ship fewer bytes across the channel.** `send_query` currently clones the
   SQL string per call; with Phase 1, hot paths send a statement id + params
   only. Errors become a lightweight `enum Error` (`thiserror`-style) instead
   of allocated strings.
4. **Bind by reference where possible** (SQLite `bind` accepts slices — avoid
   the intermediate `Vec<u8>` for UUID/vector encodings by using stack
   buffers).

Deliverables: new row representation behind the same public accessors, a
consuming decode path used by the macros, typed error enum.

## Phase 3 — Real concurrency

Goal: stop serializing every query through one connection, and make
transactions safe.

1. **Actual connection pooling.** Grow each backend to N worker
   threads/connections (default: min(4, cores) for Postgres; for SQLite, one
   writer + N readers using WAL). Route queries to idle workers.
2. **Dedicated transaction connections.** `pool.transaction()` checks out a
   connection for the transaction's lifetime and returns a handle whose
   queries are pinned to it. This fixes the current hazard where unrelated
   queries interleave into an open transaction, and lets other traffic
   proceed concurrently. `Transaction` gains `query`/`query_with_params`
   methods and the builders accept `&Transaction` as an executor (via an
   `Executor` trait implemented by both `ConnectionPool` and `Transaction`).
3. **Switch Postgres to `tokio-postgres`.** Eliminates the nested runtime and
   one thread hop; prepared-statement handles map directly onto its native
   statement cache. (Keep the worker-thread architecture for SQLite, which is
   inherently synchronous.)
4. **SQLite tuning on connect:** `PRAGMA journal_mode=WAL`,
   `PRAGMA synchronous=NORMAL`, busy timeout, and a documented way to pass
   custom pragmas through the URL (`sqlite:file.db?wal=off`).

Deliverables: multi-connection backends, `Executor` trait, transaction
checkout semantics, tokio-postgres migration.

## Phase 4 — Throughput features

1. **Batched inserts:** `users::insert_many(iter)` generating a multi-row
   `VALUES` statement (chunked to respect placeholder limits: 32k for
   SQLite, 65k for Postgres), executed inside one transaction.
2. **Streaming results:** `fetch()` returning a `Stream<Item = Result<Row>>`
   with worker-side backpressure (bounded channel), so large scans don't
   buffer the whole result set.
3. **`execute` fast path:** for statements that return no rows, skip row
   materialization entirely and return only the affected count (SQLite:
   `sqlite3_changes` instead of counting stepped rows — note today's
   `affected_rows` is actually wrong for UPDATE/DELETE, it counts *returned*
   rows, which is 0).

## Phase 0 (immediate, alongside Phase 1) — Measure first

- Add a `benches/` suite (criterion): single-row select by PK, 10k-row scan,
  1k single-row inserts, 1k-row batched insert, mixed concurrent workload —
  against in-memory SQLite, file SQLite, and Postgres (behind an env var).
  Every phase above must show its win here before merging.
- Remove the unused `sqlparser` dependency (compile-time win, zero risk).

## Sequencing and compatibility

| Phase | Expected impact | API breakage |
|-------|-----------------|--------------|
| 0     | baseline + faster builds | none |
| 1     | big win on hot repeated queries; enables precompilation | additive only |
| 2     | big win on row-heavy reads | additive; `Row` internals change but accessors stay |
| 3     | throughput under concurrency; transaction correctness | additive (`Executor` trait); `transaction()` semantics documented |
| 4     | bulk-write and large-scan workloads | additive |

The crate is pre-1.0, so minor breakage is acceptable where noted, but every
phase above can land without breaking the README example.
