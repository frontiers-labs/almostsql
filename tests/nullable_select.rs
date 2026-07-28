#![cfg(feature = "sqlite")]
#![allow(non_upper_case_globals)]

use almostsql::{ConnectionPool, Migration, migrations};
use uuid::Uuid;

migrations! {
    test_schema {
        table messages {
            id: Uuid [PrimaryKey],
            content: Option<String>,
            model_name: Option<String>,
        }
    }
}

#[tokio::test]
async fn typed_select_handles_null_text_columns() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    let migrations: Vec<Migration> = get_migrations();
    db.initialize_database(migrations).await.expect("migrate");

    let id = Uuid::new_v4();
    messages::insert()
        .id(id)
        .content(Option::<&str>::None)
        .model_name(Option::<&str>::None)
        .execute(&db)
        .await
        .expect("insert");

    let rows = messages::select().limit(10).all(&db).await.expect("select");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.id, id);
    assert!(row.content.is_none());
    assert!(row.model_name.is_none());
}
