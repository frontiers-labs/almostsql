use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

use crate::error::Error;
use crate::pool::{Request, RequestQueue};

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

/// The column header of a result set: names plus a name → position index.
/// Built once per statement execution and shared by every [`Row`] via `Arc`,
/// so rows carry no per-row name allocations.
#[derive(Debug)]
pub struct Columns {
    names: Vec<String>,
    index: HashMap<String, usize>,
}

impl Columns {
    pub fn new(names: Vec<String>) -> Self {
        let index = names
            .iter()
            .enumerate()
            .map(|(position, name)| (name.clone(), position))
            .collect();
        Self { names, index }
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }
}

/// Represents a row in a query result. Values are stored positionally; the
/// column header is shared across all rows of a result set.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Arc<Columns>,
    values: Vec<Value>,
}

/// Represents the result of a SQL query
#[derive(Debug, Clone)]
pub struct QueryResult {
    rows: Vec<Row>,
    affected_rows: usize,
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
    pub fn new(columns: Arc<Columns>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Build a standalone row from name/value pairs. Intended for tests and
    /// fixtures; real result rows share one [`Columns`] header per result set.
    pub fn from_pairs<I: IntoIterator<Item = (String, Value)>>(pairs: I) -> Self {
        let (names, values): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
        Self {
            columns: Arc::new(Columns::new(names)),
            values,
        }
    }

    pub fn columns(&self) -> &Arc<Columns> {
        &self.columns
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .position(column)
            .and_then(|position| self.values.get(position))
    }

    /// Positional access to a value.
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Move a value out of the row, leaving `Null` behind. Lets callers
    /// decode owned values (strings, blobs, vectors) without cloning.
    pub fn take(&mut self, column: &str) -> Option<Value> {
        self.columns
            .position(column)
            .and_then(|position| self.values.get_mut(position))
            .map(|value| std::mem::replace(value, Value::Null))
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
        self.columns.names().iter().collect()
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

    /// Consume the result, yielding owned rows so values can be decoded
    /// without cloning.
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
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

/// A database transaction pinned to one checked-out connection.
///
/// Queries issued through the transaction run on that connection; queries
/// issued through the [`ConnectionPool`](crate::ConnectionPool) while a
/// transaction is open run on *other* connections and do not see uncommitted
/// changes. Dropping the transaction without calling [`commit`](Self::commit)
/// rolls it back.
pub struct Transaction {
    queue: RequestQueue,
    finished: bool,
}

impl Transaction {
    pub(crate) fn new(queue: RequestQueue) -> Self {
        Self {
            queue,
            finished: false,
        }
    }

    /// Execute a raw SQL query on the transaction's connection.
    pub async fn query(&self, query: &str) -> Result<QueryResult, Error> {
        self.queue.query(Arc::from(query), Vec::new()).await
    }

    /// Execute a parameterized statement on the transaction's connection.
    pub async fn query_with_params(
        &self,
        query: &str,
        params: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        self.queue.query(Arc::from(query), params).await
    }

    pub async fn commit(mut self) -> Result<(), Error> {
        self.query("COMMIT;").await?;
        self.finish();
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        self.query("ROLLBACK;").await?;
        self.finish();
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
        let _ = self.queue.sender().try_send(Request::Release);
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            let (response, _) = futures::channel::oneshot::channel();
            let _ = self.queue.sender().try_send(Request::Query {
                sql: Arc::from("ROLLBACK;"),
                params: Vec::new(),
                response,
            });
            let _ = self.queue.sender().try_send(Request::Release);
        }
    }
}

// -----------------------
// Decoding helpers
// -----------------------

#[derive(Debug, Clone)]
pub struct DecodeError(pub String);

pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self, DecodeError>;

    /// Decode from an owned value. Types with owned storage (strings, blobs,
    /// vectors) override this to move the data instead of cloning it.
    fn from_owned_value(v: Value) -> Result<Self, DecodeError> {
        Self::from_value(&v)
    }
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

    /// Decode a column by moving its value out of the row, avoiding a clone
    /// for owned types. The column's slot is left as `Null`.
    pub fn take_decode<T: FromValue>(&mut self, column: &str) -> Result<T, DecodeError> {
        self.take(column)
            .ok_or_else(|| DecodeError(format!("missing column '{}'", column)))
            .and_then(T::from_owned_value)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::Null => Ok(None),
            _ => T::from_value(v).map(Some),
        }
    }

    fn from_owned_value(v: Value) -> Result<Self, DecodeError> {
        match v {
            Value::Null => Ok(None),
            _ => T::from_owned_value(v).map(Some),
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

    fn from_owned_value(v: Value) -> Result<Self, DecodeError> {
        match v {
            Value::Text(s) => Ok(s),
            other => Err(DecodeError(format!("expected TEXT, got {}", other))),
        }
    }
}

impl<const DIM: u32> FromValue for crate::migration::FloatVec<DIM> {
    fn from_value(v: &Value) -> Result<Self, DecodeError> {
        match v {
            Value::FloatVector(v) => Ok(crate::migration::FloatVec::<DIM>(v.clone())),
            Value::Blob(b) => float_vec_from_blob(b),
            other => Err(DecodeError(format!(
                "expected BLOB for FloatVec, got {}",
                other
            ))),
        }
    }

    fn from_owned_value(v: Value) -> Result<Self, DecodeError> {
        match v {
            Value::FloatVector(v) => Ok(crate::migration::FloatVec::<DIM>(v)),
            Value::Blob(b) => float_vec_from_blob(&b),
            other => Err(DecodeError(format!(
                "expected BLOB for FloatVec, got {}",
                other
            ))),
        }
    }
}

fn float_vec_from_blob<const DIM: u32>(
    b: &[u8],
) -> Result<crate::migration::FloatVec<DIM>, DecodeError> {
    if !b.len().is_multiple_of(4) {
        return Err(DecodeError("FloatVec blob len not multiple of 4".into()));
    }
    let mut out = Vec::with_capacity(b.len() / 4);
    for chunk in b.chunks_exact(4) {
        out.push(f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(crate::migration::FloatVec::<DIM>(out))
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

    fn from_owned_value(v: Value) -> Result<Self, DecodeError> {
        match v {
            Value::Blob(b) => Ok(crate::migration::BitVec::<DIM>(b)),
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
