#![cfg(feature = "sqlite")]

use almostsql::Value;
use almostsql::{ConnectionPool, Migration, SqlTag, Table};

#[tokio::test]
async fn insert_and_select_raw_sql() {
    // Use in-memory SQLite by default
    let pool = ConnectionPool::new("sqlite::memory:").expect("create pool");

    // Schema: users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)
    let migration = Migration::new().table(
        Table::from_name("users")
            .add::<i64, _>("id", &[SqlTag::PrimaryKey])
            .add::<String, _>("name", &[])
            .add::<i64, _>("age", &[]),
    );

    pool.initialize_database(vec![migration])
        .await
        .expect("migrate");

    // Insert a row
    pool.query_with_params(
        "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
        vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Integer(30),
        ],
    )
    .await
    .expect("insert row");

    // Select the row back
    let res = pool
        .query_with_params(
            "SELECT id, name, age FROM users WHERE id = ?",
            vec![Value::Integer(1)],
        )
        .await
        .expect("select row");

    assert_eq!(res.row_count(), 1);
    let row = &res.rows()[0];

    assert_eq!(row.get_int("id"), Some(1));
    assert_eq!(row.get_text("name"), Some("Alice"));
    assert_eq!(row.get_int("age"), Some(30));
}
