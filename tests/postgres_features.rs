//! Live Postgres coverage for the performance features (prepared queries,
//! transactions, batch inserts, streaming). Skipped unless
//! ALMOSTSQL_PG_TEST_URL points at a reachable Postgres instance. Runs under
//! either postgres driver feature.
#![cfg(any(feature = "postgres", feature = "postgres-tokio"))]

use almostsql::{ConnectionPool, Value};
use futures::TryStreamExt;

fn pg_url() -> Option<String> {
    std::env::var("ALMOSTSQL_PG_TEST_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

async fn fresh_table(db: &ConnectionPool, table: &str) {
    db.query(&format!("DROP TABLE IF EXISTS {table};"))
        .await
        .expect("drop");
    db.query(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT);"
    ))
    .await
    .expect("create");
}

#[tokio::test]
async fn postgres_prepared_transactions_batch_and_streaming() {
    let Some(url) = pg_url() else {
        eprintln!("skipping: ALMOSTSQL_PG_TEST_URL not set");
        return;
    };
    let db = ConnectionPool::new(&url).expect("pool");
    fresh_table(&db, "pg_perf_items").await;

    // Prepared statement, reused across executions.
    let insert = db
        .prepare("INSERT INTO pg_perf_items (id, name) VALUES (?, ?)")
        .await
        .expect("prepare");
    for i in 0..10_i64 {
        let affected = insert
            .execute(vec![Value::Integer(i), Value::Text(format!("row-{i}"))])
            .await
            .expect("prepared insert");
        assert_eq!(affected, 1);
    }
    assert!(db.prepare("SELECT FROM WHERE").await.is_err());

    // Batch insert in chunks.
    let mut batch = almostsql::BatchInsert::new("pg_perf_items");
    for i in 10..1010_i64 {
        batch.push(
            &["id", "name"],
            vec![Value::Integer(i), Value::Text("batch".into())],
        );
    }
    let inserted = batch.execute(&db).await.expect("batch");
    assert_eq!(inserted, 1000);

    // Affected rows from UPDATE.
    let updated = db
        .query_with_params(
            "UPDATE pg_perf_items SET name = ? WHERE id < ?",
            vec![Value::Text("renamed".into()), Value::Integer(5)],
        )
        .await
        .expect("update")
        .affected_rows();
    assert_eq!(updated, 5);

    // Streaming matches materialized results.
    let rows: Vec<_> = db
        .query_stream("SELECT id FROM pg_perf_items ORDER BY id;", Vec::new())
        .await
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    assert_eq!(rows.len(), 1010);
    assert_eq!(rows[0].get_int("id"), Some(0));
    assert_eq!(rows[1009].get_int("id"), Some(1009));

    // Transaction on a checked-out connection: committed work persists...
    let tx = db.transaction().await.expect("begin");
    tx.query_with_params(
        "INSERT INTO pg_perf_items (id, name) VALUES (?, ?)",
        vec![Value::Integer(5000), Value::Text("tx".into())],
    )
    .await
    .expect("insert in tx");
    tx.commit().await.expect("commit");

    // ...and dropped transactions roll back.
    let tx = db.transaction().await.expect("begin");
    tx.query_with_params(
        "INSERT INTO pg_perf_items (id, name) VALUES (?, ?)",
        vec![Value::Integer(6000), Value::Text("gone".into())],
    )
    .await
    .expect("insert in tx");
    drop(tx);

    let count = db
        .query("SELECT COUNT(*) AS n FROM pg_perf_items;")
        .await
        .expect("count");
    assert_eq!(count.rows()[0].get_int("n"), Some(1011));

    db.query("DROP TABLE pg_perf_items;")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn postgres_concurrent_queries_share_the_pool() {
    let Some(url) = pg_url() else {
        eprintln!("skipping: ALMOSTSQL_PG_TEST_URL not set");
        return;
    };
    let db = ConnectionPool::new(&url).expect("pool");
    fresh_table(&db, "pg_conc_items").await;
    for i in 0..50_i64 {
        db.query_with_params(
            "INSERT INTO pg_conc_items (id, name) VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text("c".into())],
        )
        .await
        .expect("seed");
    }

    let queries = (0..16).map(|i| {
        let db = db.clone();
        async move {
            db.query_with_params(
                "SELECT COUNT(*) AS n FROM pg_conc_items WHERE id >= ?",
                vec![Value::Integer(i)],
            )
            .await
            .expect("concurrent select")
            .rows()[0]
                .get_int("n")
                .unwrap()
        }
    });
    let counts = futures::future::join_all(queries).await;
    for (i, count) in counts.into_iter().enumerate() {
        assert_eq!(count, 50 - i as i64);
    }

    db.query("DROP TABLE pg_conc_items;")
        .await
        .expect("cleanup");
}
