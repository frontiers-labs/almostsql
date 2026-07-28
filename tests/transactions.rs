#![cfg(feature = "sqlite")]

use almostsql::{ConnectionPool, Migration};

#[tokio::test]
async fn committed_transaction_persists_changes() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.query("CREATE TABLE events (id INTEGER PRIMARY KEY);")
        .await
        .expect("create table");

    let transaction = db.transaction().await.expect("begin");
    db.query("INSERT INTO events (id) VALUES (1);")
        .await
        .expect("insert");
    transaction.commit().await.expect("commit");

    let result = db.query("SELECT id FROM events;").await.expect("select");
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn dropped_transaction_rolls_back_changes() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.query("CREATE TABLE events (id INTEGER PRIMARY KEY);")
        .await
        .expect("create table");

    let transaction = db.transaction().await.expect("begin");
    db.query("INSERT INTO events (id) VALUES (1);")
        .await
        .expect("insert");
    drop(transaction);

    let result = db.query("SELECT id FROM events;").await.expect("select");
    assert!(result.is_empty());
}

#[tokio::test]
async fn failed_migration_rolls_back_schema_and_ledger_entry() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    let migration = Migration::new()
        .raw("CREATE TABLE partial (id INTEGER PRIMARY KEY);")
        .raw("THIS IS NOT VALID SQL;");

    let error = db
        .initialize_database(vec![migration])
        .await
        .expect_err("migration must fail");
    assert!(!error.to_string().is_empty());
    assert!(db.query("SELECT * FROM partial;").await.is_err());

    let ledger = db
        .query("SELECT id FROM __migrations;")
        .await
        .expect("ledger");
    assert!(ledger.is_empty());
}

#[tokio::test]
async fn legacy_migration_ledger_is_upgraded_without_losing_history() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.query(
        "CREATE TABLE __migrations (
            id BIGINT NOT NULL PRIMARY KEY,
            hash BIGINT NOT NULL
        );",
    )
    .await
    .expect("legacy ledger");
    db.query("INSERT INTO __migrations (id, hash) VALUES (0, 123);")
        .await
        .expect("legacy row");

    db.ensure_migrations_table().await.expect("upgrade");

    let result = db
        .query("SELECT namespace, id, hash FROM __migrations;")
        .await
        .expect("upgraded ledger");
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0].get_text("namespace"), Some(""));
    assert_eq!(result.rows()[0].get_int("id"), Some(0));
    assert_eq!(result.rows()[0].get_int("hash"), Some(123));
}
