#![cfg(feature = "sqlite")]
#![allow(non_upper_case_globals)]

use almostsql::{ConnectionPool, Migration, migrations};
use uuid::Uuid;

migrations! {
    schema {
        table users {
            id: Uuid [PrimaryKey],
            name: String,
            age: i64,
        }
    }
}

async fn seed() -> (ConnectionPool, Uuid, Uuid) {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    let migrations: Vec<Migration> = get_migrations();
    db.initialize_database(migrations).await.expect("migrate");

    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    users::insert()
        .id(alice)
        .name("Alice")
        .age(30i64)
        .execute(&db)
        .await
        .expect("insert alice");
    users::insert()
        .id(bob)
        .name("Bob")
        .age(28i64)
        .execute(&db)
        .await
        .expect("insert bob");
    (db, alice, bob)
}

#[tokio::test]
async fn update_with_where_touches_only_matching_rows() {
    let (db, alice, bob) = seed().await;

    users::update()
        .name("Alicia")
        .age(31i64)
        .where_(users::id.eq(alice))
        .execute(&db)
        .await
        .expect("update");

    let updated = users::select()
        .where_(users::id.eq(alice))
        .one(&db)
        .await
        .expect("select alice");
    assert_eq!(updated.name, "Alicia");
    assert_eq!(updated.age, 31);

    let untouched = users::select()
        .where_(users::id.eq(bob))
        .one(&db)
        .await
        .expect("select bob");
    assert_eq!(untouched.name, "Bob");
    assert_eq!(untouched.age, 28);
}

#[tokio::test]
async fn delete_with_where_removes_only_matching_rows() {
    let (db, alice, bob) = seed().await;

    users::delete()
        .where_(users::id.eq(alice))
        .execute(&db)
        .await
        .expect("delete");

    let rows = users::select().all(&db).await.expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, bob);
}

#[tokio::test]
async fn update_without_where_is_rejected() {
    let (db, _, _) = seed().await;

    let err = users::update()
        .age(0i64)
        .execute(&db)
        .await
        .expect_err("missing WHERE must error");
    assert!(err.to_string().contains("WHERE"));

    // Nothing changed.
    let rows = users::select().all(&db).await.expect("select");
    assert!(rows.iter().all(|r| r.age != 0));
}

#[tokio::test]
async fn delete_without_where_is_rejected() {
    let (db, _, _) = seed().await;

    let err = users::delete()
        .execute(&db)
        .await
        .expect_err("missing WHERE must error");
    assert!(err.to_string().contains("WHERE"));

    let rows = users::select().all(&db).await.expect("select");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn all_escape_hatch_affects_every_row() {
    let (db, _, _) = seed().await;

    users::update()
        .age(99i64)
        .all()
        .execute(&db)
        .await
        .expect("update all");

    let rows = users::select().all(&db).await.expect("select");
    assert!(rows.iter().all(|r| r.age == 99));

    users::delete()
        .all()
        .execute(&db)
        .await
        .expect("delete all");
    let rows = users::select().all(&db).await.expect("select empty");
    assert!(rows.is_empty());
}
