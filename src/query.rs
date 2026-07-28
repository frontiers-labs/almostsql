use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

use crate::connection_pool::Backend;

/// Represents a SQL value of any supported type
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    FloatVector(Vec<f32>),
    Uuid(Uuid),
}

/// Represents a row in a query result
#[derive(Debug, Clone)]
pub struct Row {
    values: HashMap<String, Value>,
}

/// Represents the result of a SQL query
#[derive(Debug, Clone)]
pub struct QueryResult {
    rows: Vec<Row>,
    affected_rows: usize,
}

pub struct Transaction {
    backend: Arc<Backend>,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "\"{}\"", s),
            Value::Blob(b) => write!(f, "{:?}", b),
            Value::FloatVector(v) => write!(f, "{:?}", v),
            Value::Uuid(u) => write!(f, "{}", u),
        }
    }
}

impl Row {
    pub fn new(values: HashMap<String, Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    pub fn get_int(&self, column: &str) -> Option<i64> {
        match self.get(column) {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub fn get_real(&self, column: &str) -> Option<f64> {
        match self.get(column) {
            Some(Value::Real(r)) => Some(*r),
            _ => None,
        }
    }

    pub fn get_text(&self, column: &str) -> Option<&str> {
        match self.get(column) {
            Some(Value::Text(s)) => Some(s),
            _ => None,
        }
    }

    pub fn get_blob(&self, column: &str) -> Option<&[u8]> {
        match self.get(column) {
            Some(Value::Blob(b)) => Some(b),
            _ => None,
        }
    }

    pub fn get_float_vector(&self, column: &str) -> Option<&[f32]> {
        match self.get(column) {
            Some(Value::FloatVector(v)) => Some(v),
            _ => None,
        }
    }

    pub fn column_names(&self) -> Vec<&String> {
        self.values.keys().collect()
    }
}

impl QueryResult {
    pub fn new(rows: Vec<Row>, affected_rows: usize) -> Self {
        Self {
            rows,
            affected_rows,
        }
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Transaction {
    pub(crate) fn new(backend: Arc<Backend>) -> Self {
        Self { backend }
    }

    pub async fn commit(&self) -> Result<(), Box<dyn Error + Sync + Send>> {
        match &*self.backend {
            #[cfg(feature = "postgres")]
            Backend::Postgres(postgres) => postgres.commit_transaction().await,
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.commit_transaction().await,
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        match &*self.backend {
            #[cfg(feature = "postgres")]
            Backend::Postgres(postgres) => postgres.rollback_transaction_fire_and_forget(),
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(sqlite) => sqlite.rollback_transaction_fire_and_forget(),
        }
    }
}

// -----------------------
// Decoding helpers
// -----------------------

#[derive(Debug)]
pub struct DecodeError(pub String);

pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self, DecodeError>;
}

impl FromValue for i64 {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Integer(i) => Ok(*i),
            other => Err(DecodeError(format!("expected INTEGER, got {}", other))),
        }
    }
}

impl FromValue for i32 {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        i64::from_value(v)
            .and_then(|i| i32::try_from(i).map_err(|_| DecodeError("out of range".into())))
    }
}

impl FromValue for u64 {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        i64::from_value(v)
            .and_then(|i| u64::try_from(i).map_err(|_| DecodeError("negative to u64".into())))
    }
}

impl FromValue for u32 {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        i64::from_value(v)
            .and_then(|i| u32::try_from(i).map_err(|_| DecodeError("negative to u32".into())))
    }
}

impl FromValue for f64 {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Real(r) => Ok(*r),
            other => Err(DecodeError(format!("expected REAL, got {}", other))),
        }
    }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Integer(i) => Ok(*i != 0),
            other => Err(DecodeError(format!(
                "expected INTEGER(bool), got {}",
                other
            ))),
        }
    }
}

impl FromValue for Uuid {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Uuid(u) => Ok(*u),
            Value::Blob(b) if b.len() == 16 => {
                Ok(Uuid::from_slice(b).map_err(|e| DecodeError(e.to_string()))?)
            }
            Value::Text(s) => Uuid::parse_str(s).map_err(|e| DecodeError(e.to_string())),
            other => Err(DecodeError(format!(
                "expected UUID blob/text, got {}",
                other
            ))),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DecodeError {}

impl Row {
    pub fn decode<T: FromValue>(&self, column: &str) -> Result<T, DecodeError> {
        self.get(column)
            .ok_or_else(|| DecodeError(format!("missing column '{}'", column)))
            .and_then(T::from_value)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Null => Ok(None),
            _ => T::from_value(v).map(Some),
        }
    }
}

impl FromValue for String {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Text(s) => Ok(s.clone()),
            other => Err(DecodeError(format!("expected TEXT, got {}", other))),
        }
    }
}

impl<const DIM: u32> FromValue for crate::migration::FloatVec<DIM> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::FloatVector(v) => Ok(crate::migration::FloatVec::<DIM>(v.clone())),
            Value::Blob(b) => {
                if b.len() % 4 != 0 {
                    return Err(DecodeError("FloatVec blob len not multiple of 4".into()));
                }
                let mut out = Vec::with_capacity(b.len() / 4);
                for chunk in b.chunks_exact(4) {
                    out.push(f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                }
                Ok(crate::migration::FloatVec::<DIM>(out))
            }
            other => Err(DecodeError(format!(
                "expected BLOB for FloatVec, got {}",
                other
            ))),
        }
    }
}

impl<const DIM: u32> FromValue for crate::migration::BitVec<DIM> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Blob(b) => Ok(crate::migration::BitVec::<DIM>(b.clone())),
            other => Err(DecodeError(format!(
                "expected BLOB for BitVec, got {}",
                other
            ))),
        }
    }
}

impl<const DIM: u32> FromValue for crate::migration::Int8Vec<DIM> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Blob(b) => {
                let out: Vec<i8> = b.iter().map(|x| *x as i8).collect();
                Ok(crate::migration::Int8Vec::<DIM>(out))
            }
            other => Err(DecodeError(format!(
                "expected BLOB for Int8Vec, got {}",
                other
            ))),
        }
    }
}
