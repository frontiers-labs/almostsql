#![allow(non_upper_case_globals)]
//! Baseline performance suite. Run with `cargo bench`.
//!
//! Covers the hot paths the performance plan targets: repeated point queries
//! (statement cache + prepared handles), single-row vs batched inserts, and
//! full-table scans (materialized vs streamed).

use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use tokio::runtime::Runtime;

use almostsql::{ConnectionPool, Value, migrations};
use uuid::Uuid;

migrations! {
    namespace bench {
        v1 {
            table entries {
                id: Uuid [PrimaryKey],
                name: String,
                rank: i64,
            }
        }
    }
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

async fn seeded_pool(rows: i64) -> ConnectionPool {
    let db = ConnectionPool::new("sqlite::memory:").expect("pool");
    db.migrator().apply(schema()).run().await.expect("migrate");
    let mut batch = entries::insert_batch();
    for i in 0..rows {
        batch = batch.add(
            entries::insert()
                .id(Uuid::new_v4())
                .name(format!("entry-{i}"))
                .rank(i),
        );
    }
    batch.execute(&db).await.expect("seed");
    db
}

fn point_queries(c: &mut Criterion) {
    let rt = runtime();
    let db = rt.block_on(seeded_pool(10_000));

    c.bench_function("select_point/query_with_params", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.query_with_params(
                    "SELECT name FROM entries WHERE rank = ?",
                    vec![Value::Integer(5000)],
                )
                .await
                .expect("query")
            })
        })
    });

    let prepared = rt
        .block_on(db.prepare("SELECT name FROM entries WHERE rank = ?"))
        .expect("prepare");
    c.bench_function("select_point/prepared", |b| {
        b.iter(|| {
            rt.block_on(async {
                prepared
                    .query(vec![Value::Integer(5000)])
                    .await
                    .expect("query")
            })
        })
    });

    c.bench_function("select_point/typed_dsl", |b| {
        b.iter(|| {
            rt.block_on(async {
                entries::select()
                    .where_(entries::rank.eq(5000_i64))
                    .one(&db)
                    .await
                    .expect("query")
            })
        })
    });
}

fn inserts(c: &mut Criterion) {
    let rt = runtime();

    c.bench_function("insert/single_row_x100", |b| {
        b.iter_batched(
            || rt.block_on(seeded_pool(0)),
            |db| {
                rt.block_on(async {
                    for i in 0..100_i64 {
                        entries::insert()
                            .id(Uuid::new_v4())
                            .name("bench")
                            .rank(i)
                            .execute(&db)
                            .await
                            .expect("insert");
                    }
                })
            },
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("insert/batched_x1000", |b| {
        b.iter_batched(
            || rt.block_on(seeded_pool(0)),
            |db| {
                rt.block_on(async {
                    let mut batch = entries::insert_batch();
                    for i in 0..1000_i64 {
                        batch =
                            batch.add(entries::insert().id(Uuid::new_v4()).name("bench").rank(i));
                    }
                    batch.execute(&db).await.expect("batch");
                })
            },
            criterion::BatchSize::PerIteration,
        )
    });
}

fn scans(c: &mut Criterion) {
    let rt = runtime();
    let db = rt.block_on(seeded_pool(10_000));

    c.bench_function("scan_10k/materialized", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = db
                    .query("SELECT id, name, rank FROM entries;")
                    .await
                    .expect("scan");
                assert_eq!(result.row_count(), 10_000);
            })
        })
    });

    c.bench_function("scan_10k/streamed", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows: Vec<_> = db
                    .query_stream("SELECT id, name, rank FROM entries;", Vec::new())
                    .await
                    .expect("stream")
                    .try_collect()
                    .await
                    .expect("collect");
                assert_eq!(rows.len(), 10_000);
            })
        })
    });

    c.bench_function("scan_10k/typed_dsl", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = entries::select().all(&db).await.expect("scan");
                assert_eq!(rows.len(), 10_000);
            })
        })
    });
}

criterion_group!(benches, point_queries, inserts, scans);
criterion_main!(benches);
