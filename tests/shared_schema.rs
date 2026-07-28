#![cfg(feature = "sqlite")]
#![allow(non_upper_case_globals)]

use almostsql::ConnectionPool;
use uuid::Uuid;

// A fragment that could live in a shared library crate.
mod core_schema {
    use almostsql::migrations;
    use uuid::Uuid;

    migrations! {
        namespace core {
            v1 {
                table accounts {
                    id: Uuid [PrimaryKey],
                    name: String,
                }
            }
        }
    }
}

// A second, independently-defined fragment that references the first via a
// foreign key. In real use this would be an app-specific crate.
mod app_schema {
    use almostsql::migrations;
    use uuid::Uuid;

    migrations! {
        namespace app {
            v1 {
                table sessions {
                    id: Uuid [PrimaryKey],
                    account_id: Uuid,

                    foreign_key(account_id -> accounts.id);
                }
            }
        }
    }
}

#[tokio::test]
async fn composes_named_fragments_on_one_pool() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");

    db.migrator()
        .apply(core_schema::schema())
        .apply(app_schema::schema())
        .run()
        .await
        .expect("migrate");

    let account_id = Uuid::new_v4();
    core_schema::accounts::insert()
        .id(account_id)
        .name("Alice")
        .execute(&db)
        .await
        .expect("insert account");

    app_schema::sessions::insert()
        .id(Uuid::new_v4())
        .account_id(account_id)
        .execute(&db)
        .await
        .expect("insert session");

    // Each fragment tracks its own append-only history under its own namespace.
    let ledger = db
        .query("SELECT namespace, id FROM __migrations ORDER BY namespace, id;")
        .await
        .expect("ledger");
    let namespaces: Vec<String> = ledger
        .rows()
        .iter()
        .map(|r| r.get_text("namespace").unwrap().to_string())
        .collect();
    assert_eq!(namespaces, vec!["app".to_string(), "core".to_string()]);
}

#[tokio::test]
async fn rerunning_is_idempotent() {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");

    db.migrator()
        .apply(core_schema::schema())
        .apply(app_schema::schema())
        .run()
        .await
        .expect("first run");

    // Running the same fragments again must be a no-op, not re-create tables.
    db.migrator()
        .apply(core_schema::schema())
        .apply(app_schema::schema())
        .run()
        .await
        .expect("second run");

    let ledger = db
        .query("SELECT namespace, id FROM __migrations;")
        .await
        .expect("ledger");
    assert_eq!(ledger.rows().len(), 2);
}
