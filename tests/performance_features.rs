#![cfg(feature = "sqlite")]
#![allow(non_upper_case_globals)]

use almostsql::{ConnectionPool, Value, migrations};
use futures::TryStreamExt;
use uuid::Uuid;

migrations! {
    namespace perf {
        v1 {
            table items {
                id: Uuid [PrimaryKey],
                name: String,
                rank: i64,
            }
        }
    }
}

async fn pool_with_schema() -> ConnectionPool {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.migrator().apply(schema()).run().await.expect("migrate");
    db
}

#[tokio::test]
async fn prepared_query_reuses_statement_across_executions() {
    let db = pool_with_schema().await;
    let id = Uuid::new_v4();
    items::insert()
        .id(id)
        .name("first")
        .rank(1_i64)
        .execute(&db)
        .await
        .expect("insert");

    let by_rank = db
        .prepare("SELECT name FROM items WHERE rank = ?")
        .await
        .expect("prepare");
    for _ in 0..10 {
        let result = by_rank
            .query(vec![Value::Integer(1)])
            .await
            .expect("execute prepared");
        assert_eq!(result.rows()[0].get_text("name"), Some("first"));
    }

    let miss = by_rank
        .query(vec![Value::Integer(99)])
        .await
        .expect("execute prepared");
    assert!(miss.is_empty());
}

#[tokio::test]
async fn prepare_rejects_invalid_sql_eagerly() {
    let db = pool_with_schema().await;
    assert!(db.prepare("SELECT FROM WHERE").await.is_err());
}

#[tokio::test]
async fn affected_rows_reports_updates_and_deletes() {
    let db = pool_with_schema().await;
    for i in 0..5_i64 {
        items::insert()
            .id(Uuid::new_v4())
            .name("bulk")
            .rank(i)
            .execute(&db)
            .await
            .expect("insert");
    }

    let updated = items::update()
        .name("renamed")
        .where_(items::rank.lt(3_i64))
        .execute(&db)
        .await
        .expect("update");
    assert_eq!(updated, 3);

    let deleted = items::delete()
        .where_(items::rank.ge(3_i64))
        .execute(&db)
        .await
        .expect("delete");
    assert_eq!(deleted, 2);
}

#[tokio::test]
async fn batch_insert_writes_all_rows_in_chunks() {
    let db = pool_with_schema().await;

    let mut batch = items::insert_batch();
    for i in 0..1000_i64 {
        batch = batch.add(items::insert().id(Uuid::new_v4()).name("batch").rank(i));
    }
    let inserted = batch.execute(&db).await.expect("batch insert");
    assert_eq!(inserted, 1000);

    let count = db
        .query("SELECT COUNT(*) AS n FROM items;")
        .await
        .expect("count");
    assert_eq!(count.rows()[0].get_int("n"), Some(1000));
}

#[tokio::test]
async fn batch_insert_rejects_mismatched_rows() {
    let db = pool_with_schema().await;
    let batch = items::insert_batch()
        .add(items::insert().id(Uuid::new_v4()).name("a").rank(1_i64))
        .add(items::insert().id(Uuid::new_v4()).name("b"));
    assert!(batch.execute(&db).await.is_err());
}

#[tokio::test]
async fn streamed_rows_match_materialized_rows() {
    let db = pool_with_schema().await;
    let mut batch = items::insert_batch();
    for i in 0..999_i64 {
        batch = batch.add(items::insert().id(Uuid::new_v4()).name("s").rank(i));
    }
    batch.execute(&db).await.expect("seed");

    let rows: Vec<_> = db
        .query_stream("SELECT rank FROM items ORDER BY rank;", Vec::new())
        .await
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    assert_eq!(rows.len(), 999);
    assert_eq!(rows[0].get_int("rank"), Some(0));
    assert_eq!(rows[998].get_int("rank"), Some(998));
}

#[tokio::test]
async fn typed_builders_run_on_transactions() {
    let db = pool_with_schema().await;

    let transaction = db.transaction().await.expect("begin");
    items::insert()
        .id(Uuid::new_v4())
        .name("tx")
        .rank(1_i64)
        .execute(&transaction)
        .await
        .expect("insert in tx");
    let seen = items::select()
        .all(&transaction)
        .await
        .expect("select in tx");
    assert_eq!(seen.len(), 1);
    transaction.commit().await.expect("commit");

    let rows = items::select().all(&db).await.expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "tx");
}

#[tokio::test]
async fn dropped_transaction_discards_typed_writes() {
    let db = pool_with_schema().await;

    let transaction = db.transaction().await.expect("begin");
    items::insert()
        .id(Uuid::new_v4())
        .name("gone")
        .rank(1_i64)
        .execute(&transaction)
        .await
        .expect("insert in tx");
    drop(transaction);

    let rows = items::select().all(&db).await.expect("select");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn typed_prepared_select_binds_params_at_execution() {
    let db = pool_with_schema().await;
    let mut batch = items::insert_batch();
    for i in 0..50_i64 {
        batch = batch.add(
            items::insert()
                .id(Uuid::new_v4())
                .name(format!("row-{i}"))
                .rank(i),
        );
    }
    batch.execute(&db).await.expect("seed");

    let by_rank = items::select()
        .where_(items::rank.eq_param())
        .prepare(&db)
        .await
        .expect("prepare");

    for i in [0_i64, 7, 42] {
        let row = by_rank.one(almostsql::params![i]).await.expect("execute");
        assert_eq!(row.rank, i);
        assert_eq!(row.name, format!("row-{i}"));
    }

    // Wrong arity is rejected before reaching the database.
    assert!(by_rank.all(almostsql::params![1_i64, 2_i64]).await.is_err());
    assert!(by_rank.all(almostsql::params![]).await.is_err());

    // Fixed and hole parameters mix; holes bind in order of appearance.
    let range = items::select()
        .where_(items::rank.ge_param().and(items::rank.lt(40_i64)))
        .prepare(&db)
        .await
        .expect("prepare range");
    let rows = range.all(almostsql::params![35_i64]).await.expect("range");
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn unprepared_execution_of_param_holes_is_rejected() {
    let db = pool_with_schema().await;
    let error = match items::select()
        .where_(items::rank.eq_param())
        .all(&db)
        .await
    {
        Ok(_) => panic!("must not run with unbound parameters"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("unbound"));
}

#[tokio::test]
async fn concurrent_queries_share_the_pool() {
    let db = pool_with_schema().await;
    let mut batch = items::insert_batch();
    for i in 0..100_i64 {
        batch = batch.add(items::insert().id(Uuid::new_v4()).name("c").rank(i));
    }
    batch.execute(&db).await.expect("seed");

    let queries = (0..16).map(|i| {
        let db = db.clone();
        async move {
            items::select()
                .where_(items::rank.ge(i as i64))
                .all(&db)
                .await
                .expect("concurrent select")
                .len()
        }
    });
    let counts = futures::future::join_all(queries).await;
    for (i, count) in counts.into_iter().enumerate() {
        assert_eq!(count, 100 - i);
    }
}
