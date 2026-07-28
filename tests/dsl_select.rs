#![cfg(feature = "sqlite")]

use almostsql::Value;
use almostsql::{Column, Select};
use almostsql::{ConnectionPool, Migration, SqlTag, Table};

// Marker type for typed columns
struct Users;

const NAME: Column<String, Users> = Column::new("name");
const AGE: Column<i64, Users> = Column::new("age");

#[tokio::test]
async fn select_with_where_and_limit_via_dsl() {
    let pool = ConnectionPool::new("sqlite::memory:").expect("create pool");

    // Create schema and seed data
    let migration = Migration::new().table(
        Table::from_name("users")
            .add::<i64, _>("id", &[SqlTag::PrimaryKey])
            .add::<String, _>("name", &[])
            .add::<i64, _>("age", &[]),
    );
    pool.initialize_database(vec![migration])
        .await
        .expect("migrate");

    // Seed multiple rows
    for (id, name, age) in [(1, "Alice", 30), (2, "Bob", 28), (3, "Alice", 31)] {
        pool.query_with_params(
            "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
            vec![
                Value::Integer(id),
                Value::Text(name.to_string()),
                Value::Integer(age),
            ],
        )
        .await
        .expect("insert row");
    }

    // Build a typed SELECT using the DSL: WHERE name = 'Alice' ORDER BY age ASC LIMIT 1
    let mut query = Select::<Users>::new("users").where_(NAME.eq("Alice"));
    query.order_by(AGE, true);
    let (sql, params) = query.limit(1).to_sql();

    let res = pool
        .query_with_params(&sql, params)
        .await
        .expect("dsl select");

    assert_eq!(res.row_count(), 1);
    let row = &res.rows()[0];
    assert_eq!(row.get_text("name"), Some("Alice"));
    // Youngest Alice due to ORDER BY age ASC
    assert_eq!(row.get_int("age"), Some(30));
}
