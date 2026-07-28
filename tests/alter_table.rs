#![cfg(feature = "sqlite")]
#![allow(non_upper_case_globals)]

use almostsql::{AlterTable, ConnectionPool, Migration, migrations};
use uuid::Uuid;

migrations! {
    v1 {
        table accounts {
            id: Uuid [PrimaryKey],
            name: String,
            obsolete: String,
        }
    }
    v2 {
        alter table accounts {
            add age: i64;
            drop obsolete;
            rename name -> full_name;
        }
    }
}

#[tokio::test]
async fn macro_alter_table_applies_and_updates_typed_module() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.initialize_database(get_migrations())
        .await
        .expect("migrate");

    let id = Uuid::new_v4();
    // Typed module reflects the post-v2 schema: full_name + age exist, obsolete gone.
    accounts::insert()
        .id(id)
        .full_name("Alice")
        .age(30i64)
        .execute(&db)
        .await
        .expect("insert");

    let rows = accounts::select().limit(10).all(&db).await.expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].full_name, "Alice");
    assert_eq!(rows[0].age, 30);
}

#[tokio::test]
async fn builder_alter_table_runs_as_second_migration() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");

    let v1 = Migration::new().table(
        almostsql::Table::from_name("items")
            .add::<Uuid, _>("id", &[almostsql::SqlTag::PrimaryKey])
            .add::<String, _>("label", &[]),
    );
    let v2 =
        Migration::new().alter_table(AlterTable::from_name("items").add_column::<i64, _>("qty"));

    db.initialize_database(vec![v1, v2]).await.expect("migrate");

    let id = Uuid::new_v4();
    db.query_with_params(
        "INSERT INTO items (id, label, qty) VALUES (?, ?, ?)",
        vec![
            almostsql::Value::Uuid(id),
            almostsql::Value::Text("widget".into()),
            almostsql::Value::Integer(7),
        ],
    )
    .await
    .expect("insert");

    let res = db.query("SELECT qty FROM items;").await.expect("select");
    assert_eq!(res.rows().len(), 1);
    assert_eq!(res.rows()[0].get_int("qty").unwrap(), 7);
}
