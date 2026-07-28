use std::collections::HashMap;

use almostsql::{
    BitVec, Column, ConnectionPool, Delete, FloatVec, FromValue, Int8Vec, QueryResult, Row, Select,
    Update, Value,
};
use uuid::Uuid;

struct Users;

#[test]
fn connection_pool_rejects_unsupported_urls() {
    let error = match ConnectionPool::new("mysql://localhost/database") {
        Ok(_) => panic!("unsupported URL must fail"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("Unsupported database URL"));
}

#[test]
fn query_result_reports_rows_and_affected_count() {
    let row = Row::new(HashMap::from([
        ("id".to_string(), Value::Integer(7)),
        ("name".to_string(), Value::Text("Ada".to_string())),
    ]));
    let result = QueryResult::new(vec![row], 3);

    assert_eq!(result.row_count(), 1);
    assert!(!result.is_empty());
    assert_eq!(result.affected_rows(), 3);
    assert_eq!(result.rows()[0].get_int("id"), Some(7));
    assert_eq!(result.rows()[0].get_text("name"), Some("Ada"));
}

#[test]
fn value_decoding_handles_boundaries_and_nullability() {
    assert_eq!(i32::from_value(&Value::Integer(42)).unwrap(), 42);
    assert!(i32::from_value(&Value::Integer(i64::MAX)).is_err());
    assert!(u64::from_value(&Value::Integer(-1)).is_err());
    assert_eq!(Option::<String>::from_value(&Value::Null).unwrap(), None);

    let id = Uuid::new_v4();
    assert_eq!(Uuid::from_value(&Value::Text(id.to_string())).unwrap(), id);
    assert!(Uuid::from_value(&Value::Blob(vec![0; 15])).is_err());
}

#[test]
fn vector_decoding_validates_float_blob_shape() {
    let bytes = [1.25_f32.to_ne_bytes(), (-2.5_f32).to_ne_bytes()].concat();
    let vector = FloatVec::<2>::from_value(&Value::Blob(bytes)).unwrap();
    assert_eq!(vector.0, vec![1.25, -2.5]);

    assert!(FloatVec::<2>::from_value(&Value::Blob(vec![0; 3])).is_err());
    assert_eq!(
        BitVec::<8>::from_value(&Value::Blob(vec![0b1010_0001]))
            .unwrap()
            .0,
        vec![0b1010_0001]
    );
    assert_eq!(
        Int8Vec::<2>::from_value(&Value::Blob(vec![255, 1]))
            .unwrap()
            .0,
        vec![-1, 1]
    );
}

#[test]
fn builders_preserve_parameter_order_and_require_explicit_bulk_writes() {
    let id = Column::<i64, Users>::new("id");
    let name = Column::<String, Users>::new("name");
    let predicate = id.gt(10).and(name.ne("root"));

    let (sql, params) = Select::<Users>::new("users")
        .where_(predicate)
        .limit(5)
        .offset(2)
        .to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE ((id > ?) AND (name != ?)) LIMIT ? OFFSET ?"
    );
    assert!(matches!(params.as_slice(), [
        Value::Integer(10),
        Value::Text(value),
        Value::Integer(5),
        Value::Integer(2)
    ] if value == "root"));

    assert!(
        Update::<Users>::new("users")
            .set("name", Value::Null)
            .to_sql()
            .is_err()
    );
    assert!(Delete::<Users>::new("users").to_sql().is_err());
    assert!(
        Update::<Users>::new("users")
            .set("name", Value::Null)
            .all()
            .to_sql()
            .is_ok()
    );
    assert!(Delete::<Users>::new("users").all().to_sql().is_ok());
}
