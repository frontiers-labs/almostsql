use std::marker::PhantomData;

use crate::query::Value;

/// Converts Rust values into SQL bindable `Value`s
pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::Integer(self)
    }
}
impl IntoValue for i32 {
    fn into_value(self) -> Value {
        Value::Integer(self as i64)
    }
}
impl IntoValue for u64 {
    fn into_value(self) -> Value {
        Value::Integer(self as i64)
    }
}
impl IntoValue for u32 {
    fn into_value(self) -> Value {
        Value::Integer(self as i64)
    }
}
impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Integer(if self { 1 } else { 0 })
    }
}
impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::Real(self)
    }
}
impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value::Real(self as f64)
    }
}
impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::Text(self)
    }
}
impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl IntoValue for uuid::Uuid {
    fn into_value(self) -> Value {
        Value::Uuid(self)
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        match self {
            Some(v) => v.into_value(),
            None => Value::Null,
        }
    }
}

impl<const DIM: u32> IntoValue for crate::migration::FloatVec<DIM> {
    fn into_value(self) -> Value {
        Value::FloatVector(self.0)
    }
}

impl<const DIM: u32> IntoValue for crate::migration::Int8Vec<DIM> {
    fn into_value(self) -> Value {
        let bytes: Vec<u8> = self.0.into_iter().map(|x| x as u8).collect();
        Value::Blob(bytes)
    }
}

impl<const DIM: u32> IntoValue for crate::migration::BitVec<DIM> {
    fn into_value(self) -> Value {
        Value::Blob(self.0)
    }
}

// Column-input typing: allows accepting &str for String columns, Option<&str> for Option<String>
pub trait ColumnInput<Target> {
    fn into_value(self) -> Value;
}

impl<T: IntoValue> ColumnInput<T> for T {
    fn into_value(self) -> Value {
        IntoValue::into_value(self)
    }
}

impl ColumnInput<String> for &str {
    fn into_value(self) -> Value {
        IntoValue::into_value(self)
    }
}

impl ColumnInput<Option<String>> for Option<&str> {
    fn into_value(self) -> Value {
        match self {
            Some(s) => IntoValue::into_value(s),
            None => Value::Null,
        }
    }
}

impl<T: IntoValue> ColumnInput<Option<T>> for T {
    fn into_value(self) -> Value {
        IntoValue::into_value(self)
    }
}

/// A typed column handle bound to a specific table marker type.
pub struct Column<T, Tab> {
    pub(crate) name: &'static str,
    _phantom: PhantomData<(T, Tab)>,
}

impl<T, Tab> Copy for Column<T, Tab> {}
impl<T, Tab> Clone for Column<T, Tab> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, Tab> Column<T, Tab> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub fn eq<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: "=",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    pub fn ne<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: "!=",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    pub fn gt<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: ">",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    pub fn ge<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: ">=",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    pub fn lt<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: "<",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    pub fn le<U: ColumnInput<T>>(&self, v: U) -> Expr<Tab> {
        Expr::<Tab>::Compare {
            column: self.name,
            op: "<=",
            value: ColumnInput::into_value(v),
            _p: PhantomData,
        }
    }

    /// Compare against a bind parameter supplied when the prepared query
    /// executes, instead of a fixed value.
    pub fn eq_param(&self) -> Expr<Tab> {
        self.compare_param("=")
    }

    pub fn ne_param(&self) -> Expr<Tab> {
        self.compare_param("!=")
    }

    pub fn gt_param(&self) -> Expr<Tab> {
        self.compare_param(">")
    }

    pub fn ge_param(&self) -> Expr<Tab> {
        self.compare_param(">=")
    }

    pub fn lt_param(&self) -> Expr<Tab> {
        self.compare_param("<")
    }

    pub fn le_param(&self) -> Expr<Tab> {
        self.compare_param("<=")
    }

    fn compare_param(&self, op: &'static str) -> Expr<Tab> {
        Expr::CompareParam {
            column: self.name,
            op,
            _p: PhantomData,
        }
    }

    pub fn is_null(&self) -> Expr<Tab> {
        Expr::<Tab>::IsNull {
            column: self.name,
            _p: PhantomData,
        }
    }

    pub fn is_not_null(&self) -> Expr<Tab> {
        Expr::<Tab>::IsNotNull {
            column: self.name,
            _p: PhantomData,
        }
    }
}

pub struct VectorSearch<Tab> {
    table: &'static str,
    id_column: &'static str,
    vector_column: &'static str,
    query: Value,
    where_clause: Option<Expr<Tab>>,
    limit: Option<u64>,
    _p: PhantomData<Tab>,
}

pub fn vector_search<Tab, Id, VecTy, Query>(
    table: &'static str,
    id_column: Column<Id, Tab>,
    vector_column: Column<VecTy, Tab>,
    query: Query,
) -> VectorSearch<Tab>
where
    Query: IntoValue,
{
    VectorSearch {
        table,
        id_column: id_column.name,
        vector_column: vector_column.name,
        query: query.into_value(),
        where_clause: None,
        limit: None,
        _p: PhantomData,
    }
}

impl<Tab> VectorSearch<Tab> {
    pub fn where_(mut self, predicate: Expr<Tab>) -> Self {
        self.where_clause = Some(predicate);
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    pub(crate) fn table(&self) -> &'static str {
        self.table
    }

    pub(crate) fn id_column(&self) -> &'static str {
        self.id_column
    }

    pub(crate) fn vector_column(&self) -> &'static str {
        self.vector_column
    }

    pub(crate) fn query(&self) -> Value {
        self.query.clone()
    }

    pub(crate) fn append_where(
        &self,
        sql: &mut String,
        params: &mut Vec<Value>,
    ) -> Result<(), String> {
        if let Some(expr) = &self.where_clause {
            sql.push_str(" WHERE ");
            expr.to_sql_values(sql, params)?;
            sql.push_str(" AND ");
        } else {
            sql.push_str(" WHERE ");
        }
        sql.push_str(self.vector_column);
        sql.push_str(" IS NOT NULL");
        Ok(())
    }

    pub(crate) fn append_limit(&self, sql: &mut String, params: &mut Vec<Value>) {
        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT ?");
            params.push(Value::Integer(limit as i64));
        }
    }
}

/// One position in a statement's parameter list: either a value fixed when
/// the builder was constructed, or a hole (from a `*_param()` comparison) to
/// be filled at execution time.
#[derive(Clone)]
pub(crate) enum ParamSlot {
    Fixed(Value),
    Hole,
}

/// A typed boolean/compare expression bound to a single table
pub enum Expr<Tab> {
    Compare {
        column: &'static str,
        op: &'static str,
        value: Value,
        _p: PhantomData<Tab>,
    },
    /// A comparison whose right-hand side is a bind parameter supplied at
    /// execution time (see [`Column::eq_param`] and friends).
    CompareParam {
        column: &'static str,
        op: &'static str,
        _p: PhantomData<Tab>,
    },
    IsNull {
        column: &'static str,
        _p: PhantomData<Tab>,
    },
    IsNotNull {
        column: &'static str,
        _p: PhantomData<Tab>,
    },
    And(Box<Expr<Tab>>, Box<Expr<Tab>>, PhantomData<Tab>),
    Or(Box<Expr<Tab>>, Box<Expr<Tab>>, PhantomData<Tab>),
    Not(Box<Expr<Tab>>, PhantomData<Tab>),
    True(PhantomData<Tab>),
}

impl<Tab> Expr<Tab> {
    pub fn and(self, other: Expr<Tab>) -> Expr<Tab> {
        Expr::And(Box::new(self), Box::new(other), PhantomData)
    }

    pub fn or(self, other: Expr<Tab>) -> Expr<Tab> {
        Expr::Or(Box::new(self), Box::new(other), PhantomData)
    }

    fn to_sql(&self, out_sql: &mut String, out_params: &mut Vec<ParamSlot>) {
        match self {
            Expr::Compare {
                column, op, value, ..
            } => {
                out_sql.push('(');
                out_sql.push_str(column);
                out_sql.push(' ');
                out_sql.push_str(op);
                out_sql.push_str(" ?)");
                out_params.push(ParamSlot::Fixed(value.clone()));
            }
            Expr::CompareParam { column, op, .. } => {
                out_sql.push('(');
                out_sql.push_str(column);
                out_sql.push(' ');
                out_sql.push_str(op);
                out_sql.push_str(" ?)");
                out_params.push(ParamSlot::Hole);
            }
            Expr::And(a, b, _) => {
                out_sql.push('(');
                a.to_sql(out_sql, out_params);
                out_sql.push_str(" AND ");
                b.to_sql(out_sql, out_params);
                out_sql.push(')');
            }
            Expr::Or(a, b, _) => {
                out_sql.push('(');
                a.to_sql(out_sql, out_params);
                out_sql.push_str(" OR ");
                b.to_sql(out_sql, out_params);
                out_sql.push(')');
            }
            Expr::Not(e, _) => {
                out_sql.push_str("NOT (");
                e.to_sql(out_sql, out_params);
                out_sql.push(')');
            }
            Expr::True(_) => {
                out_sql.push('1');
            }
            Expr::IsNull { column, .. } => {
                out_sql.push('(');
                out_sql.push_str(column);
                out_sql.push_str(" IS NULL)");
            }
            Expr::IsNotNull { column, .. } => {
                out_sql.push('(');
                out_sql.push_str(column);
                out_sql.push_str(" IS NOT NULL)");
            }
        }
    }

    /// Like `to_sql`, but requires every parameter to be fixed. Errors when
    /// the expression contains `*_param()` holes, which only a prepared
    /// statement can bind.
    fn to_sql_values(
        &self,
        out_sql: &mut String,
        out_params: &mut Vec<Value>,
    ) -> Result<(), String> {
        let mut slots = Vec::new();
        self.to_sql(out_sql, &mut slots);
        for slot in slots {
            match slot {
                ParamSlot::Fixed(value) => out_params.push(value),
                ParamSlot::Hole => {
                    return Err(
                        "query has unbound parameters from *_param(); use prepare() to bind them"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }
}

impl<Tab> std::ops::Not for Expr<Tab> {
    type Output = Expr<Tab>;
    fn not(self) -> Expr<Tab> {
        Expr::Not(Box::new(self), PhantomData)
    }
}

/// A single-table SELECT builder that emits SQL and parameters
pub struct Select<Tab> {
    table: &'static str,
    where_clause: Option<Expr<Tab>>,
    limit: Option<u64>,
    offset: Option<u64>,
    order_by: Option<(&'static str, bool)>, // (column, asc)
    _p: PhantomData<Tab>,
}

impl<Tab> Select<Tab> {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            where_clause: None,
            limit: None,
            offset: None,
            order_by: None,
            _p: PhantomData,
        }
    }

    pub fn where_(mut self, predicate: Expr<Tab>) -> Self {
        self.where_clause = Some(predicate);
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    pub fn order_by<T>(&mut self, column: Column<T, Tab>, asc: bool) -> &mut Self {
        self.order_by = Some((column.name, asc));
        self
    }

    /// Build SQL string with placeholders and ordered params. Errors when the
    /// query contains `*_param()` holes; those require [`Select::prepare`].
    pub fn to_sql(&self) -> Result<(String, Vec<Value>), String> {
        let (sql, slots) = self.to_sql_slots();
        Ok((sql, fixed_values(slots)?))
    }

    pub(crate) fn to_sql_slots(&self) -> (String, Vec<ParamSlot>) {
        let mut sql = String::new();
        sql.push_str("SELECT * FROM ");
        sql.push_str(self.table);
        let mut params = Vec::new();
        if let Some(expr) = &self.where_clause {
            sql.push_str(" WHERE ");
            expr.to_sql(&mut sql, &mut params);
        }
        if let Some((col, asc)) = &self.order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(col);
            sql.push_str(if *asc { " ASC" } else { " DESC" });
        }
        if let Some(n) = self.limit {
            sql.push_str(" LIMIT ?");
            params.push(ParamSlot::Fixed(Value::Integer(n as i64)));
        }
        if let Some(n) = self.offset {
            sql.push_str(" OFFSET ?");
            params.push(ParamSlot::Fixed(Value::Integer(n as i64)));
        }
        (sql, params)
    }

    /// Precompile this query. `*_param()` comparisons become bind parameters
    /// supplied at execution time, in the order they appear in the query.
    pub async fn prepare(
        &self,
        db: &crate::ConnectionPool,
    ) -> Result<PreparedSelect<Tab>, crate::Error> {
        let (sql, slots) = self.to_sql_slots();
        let prepared = db.prepare(&sql).await?;
        Ok(PreparedSelect {
            inner: crate::connection_pool::PreparedStatement::new(prepared, slots),
            _p: PhantomData,
        })
    }
}

/// A precompiled, typed SELECT. Execute with the values for the query's
/// `*_param()` holes, in order of appearance.
pub struct PreparedSelect<Tab> {
    inner: crate::connection_pool::PreparedStatement,
    _p: PhantomData<Tab>,
}

impl<Tab> PreparedSelect<Tab> {
    pub fn sql(&self) -> &str {
        self.inner.sql()
    }

    pub async fn query(&self, args: Vec<Value>) -> Result<crate::QueryResult, crate::Error> {
        self.inner.query(args).await
    }
}

/// A single-table INSERT builder that emits SQL and parameters. Columns are
/// set either to fixed values or to bind parameters filled in when a
/// prepared statement executes.
pub struct Insert<Tab> {
    table: &'static str,
    cols: Vec<&'static str>,
    slots: Vec<ParamSlot>,
    _p: PhantomData<Tab>,
}

impl<Tab> Insert<Tab> {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            cols: Vec::new(),
            slots: Vec::new(),
            _p: PhantomData,
        }
    }

    pub fn set(mut self, column: &'static str, value: Value) -> Self {
        self.cols.push(column);
        self.slots.push(ParamSlot::Fixed(value));
        self
    }

    /// Insert a column from a bind parameter supplied when the prepared
    /// statement executes.
    pub fn set_param(mut self, column: &'static str) -> Self {
        self.cols.push(column);
        self.slots.push(ParamSlot::Hole);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn to_sql(&self) -> Result<(String, Vec<Value>), String> {
        let (sql, slots) = self.to_sql_slots();
        Ok((sql, fixed_values(slots)?))
    }

    pub(crate) fn to_sql_slots(&self) -> (String, Vec<ParamSlot>) {
        let placeholders = std::iter::repeat_n("?", self.cols.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            self.table,
            self.cols.join(", "),
            placeholders
        );
        (sql, self.slots.clone())
    }

    /// The column names and fixed values of this row, for batch insertion.
    /// Errors when the row contains `set_param` holes.
    pub(crate) fn into_row(self) -> Result<(Vec<&'static str>, Vec<Value>), String> {
        let values = fixed_values(self.slots)?;
        Ok((self.cols, values))
    }

    /// Precompile this statement. Columns added with
    /// [`set_param`](Self::set_param) become bind parameters supplied at
    /// execution time, in the order the columns were set.
    pub async fn prepare(
        &self,
        db: &crate::ConnectionPool,
    ) -> Result<PreparedExec<Tab>, crate::Error> {
        if self.is_empty() {
            return Err(crate::Error::InvalidQuery(
                "INSERT with no columns cannot be prepared".into(),
            ));
        }
        let (sql, slots) = self.to_sql_slots();
        PreparedExec::prepare(db, &sql, slots).await
    }
}

/// A precompiled, typed INSERT/UPDATE/DELETE. Execute with the values for
/// the statement's parameter holes, in order of appearance; returns the
/// number of affected rows.
pub struct PreparedExec<Tab> {
    inner: crate::connection_pool::PreparedStatement,
    _p: PhantomData<Tab>,
}

impl<Tab> PreparedExec<Tab> {
    pub(crate) async fn prepare(
        db: &crate::ConnectionPool,
        sql: &str,
        slots: Vec<ParamSlot>,
    ) -> Result<Self, crate::Error> {
        let prepared = db.prepare(sql).await?;
        Ok(Self {
            inner: crate::connection_pool::PreparedStatement::new(prepared, slots),
            _p: PhantomData,
        })
    }

    pub fn sql(&self) -> &str {
        self.inner.sql()
    }

    pub async fn execute(&self, args: Vec<Value>) -> Result<usize, crate::Error> {
        Ok(self.inner.query(args).await?.affected_rows())
    }
}

pub(crate) fn fixed_values(slots: Vec<ParamSlot>) -> Result<Vec<Value>, String> {
    let mut values = Vec::with_capacity(slots.len());
    for slot in slots {
        match slot {
            ParamSlot::Fixed(value) => values.push(value),
            ParamSlot::Hole => {
                return Err(
                    "query has unbound parameters from *_param(); use prepare() to bind them"
                        .into(),
                );
            }
        }
    }
    Ok(values)
}

/// A single-table UPDATE builder that emits SQL and parameters.
///
/// Refuses to build without a WHERE clause unless `all()` is called, so a
/// forgotten predicate can't silently rewrite the whole table.
pub struct Update<Tab> {
    table: &'static str,
    set_cols: Vec<&'static str>,
    set_slots: Vec<ParamSlot>,
    where_clause: Option<Expr<Tab>>,
    all: bool,
    _p: PhantomData<Tab>,
}

impl<Tab> Update<Tab> {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            set_cols: Vec::new(),
            set_slots: Vec::new(),
            where_clause: None,
            all: false,
            _p: PhantomData,
        }
    }

    pub fn set(mut self, column: &'static str, value: Value) -> Self {
        self.set_cols.push(column);
        self.set_slots.push(ParamSlot::Fixed(value));
        self
    }

    /// SET a column from a bind parameter supplied when the prepared
    /// statement executes.
    pub fn set_param(mut self, column: &'static str) -> Self {
        self.set_cols.push(column);
        self.set_slots.push(ParamSlot::Hole);
        self
    }

    pub fn where_(mut self, predicate: Expr<Tab>) -> Self {
        self.where_clause = Some(predicate);
        self
    }

    pub fn all(mut self) -> Self {
        self.all = true;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.set_cols.is_empty()
    }

    pub fn to_sql(&self) -> Result<(String, Vec<Value>), String> {
        let (sql, slots) = self.to_sql_slots()?;
        Ok((sql, fixed_values(slots)?))
    }

    pub(crate) fn to_sql_slots(&self) -> Result<(String, Vec<ParamSlot>), String> {
        if self.where_clause.is_none() && !self.all {
            return Err("UPDATE without WHERE: call .where_(...) or .all() to confirm".into());
        }
        let mut sql = String::new();
        sql.push_str("UPDATE ");
        sql.push_str(self.table);
        sql.push_str(" SET ");
        let assignments = self
            .set_cols
            .iter()
            .map(|c| format!("{} = ?", c))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&assignments);
        let mut slots = self.set_slots.clone();
        if let Some(expr) = &self.where_clause {
            sql.push_str(" WHERE ");
            expr.to_sql(&mut sql, &mut slots);
        }
        Ok((sql, slots))
    }

    /// Precompile this statement. SET values added with
    /// [`set_param`](Self::set_param) and `*_param()` comparisons become bind
    /// parameters supplied at execution time, in order of appearance (SET
    /// values first, then the WHERE clause).
    pub async fn prepare(
        &self,
        db: &crate::ConnectionPool,
    ) -> Result<PreparedExec<Tab>, crate::Error> {
        let (sql, slots) = self.to_sql_slots().map_err(crate::Error::InvalidQuery)?;
        PreparedExec::prepare(db, &sql, slots).await
    }
}

/// A single-table DELETE builder that emits SQL and parameters.
///
/// Like `Update`, it refuses to build without a WHERE clause unless `all()`
/// is called.
pub struct Delete<Tab> {
    table: &'static str,
    where_clause: Option<Expr<Tab>>,
    all: bool,
    _p: PhantomData<Tab>,
}

impl<Tab> Delete<Tab> {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            where_clause: None,
            all: false,
            _p: PhantomData,
        }
    }

    pub fn where_(mut self, predicate: Expr<Tab>) -> Self {
        self.where_clause = Some(predicate);
        self
    }

    pub fn all(mut self) -> Self {
        self.all = true;
        self
    }

    pub fn to_sql(&self) -> Result<(String, Vec<Value>), String> {
        let (sql, slots) = self.to_sql_slots()?;
        Ok((sql, fixed_values(slots)?))
    }

    pub(crate) fn to_sql_slots(&self) -> Result<(String, Vec<ParamSlot>), String> {
        if self.where_clause.is_none() && !self.all {
            return Err("DELETE without WHERE: call .where_(...) or .all() to confirm".into());
        }
        let mut sql = String::new();
        sql.push_str("DELETE FROM ");
        sql.push_str(self.table);
        let mut slots = Vec::new();
        if let Some(expr) = &self.where_clause {
            sql.push_str(" WHERE ");
            expr.to_sql(&mut sql, &mut slots);
        }
        Ok((sql, slots))
    }

    /// Precompile this statement; `*_param()` comparisons become bind
    /// parameters supplied at execution time.
    pub async fn prepare(
        &self,
        db: &crate::ConnectionPool,
    ) -> Result<PreparedExec<Tab>, crate::Error> {
        let (sql, slots) = self.to_sql_slots().map_err(crate::Error::InvalidQuery)?;
        PreparedExec::prepare(db, &sql, slots).await
    }
}

// -----------------------
// Projection support
// -----------------------

use crate::query::{DecodeError, FromValue, Row as DbRow};

pub trait SelectList<Tab> {
    type Out;
    fn names(&self, out: &mut Vec<&'static str>);
    /// Decode a projection from a row. Takes the row mutably so owned values
    /// (strings, blobs, vectors) can be moved out instead of cloned.
    fn decode_row(row: &mut DbRow, names: &[&'static str]) -> Result<Self::Out, DecodeError>;
}

pub struct SelectCols<Tab, C: SelectList<Tab>> {
    base: Select<Tab>,
    names: Vec<&'static str>,
    _pc: PhantomData<C>,
}

impl<Tab> Select<Tab> {
    pub fn select<C: SelectList<Tab>>(self, cols: C) -> SelectCols<Tab, C> {
        let mut names = Vec::new();
        cols.names(&mut names);
        SelectCols {
            base: self,
            names,
            _pc: PhantomData,
        }
    }
}

impl<Tab, C: SelectList<Tab>> SelectCols<Tab, C> {
    pub fn where_(mut self, predicate: Expr<Tab>) -> Self {
        self.base = self.base.where_(predicate);
        self
    }
    pub fn limit(mut self, n: u64) -> Self {
        self.base = self.base.limit(n);
        self
    }
    pub fn offset(mut self, n: u64) -> Self {
        self.base = self.base.offset(n);
        self
    }
    pub fn order_by<T>(mut self, column: Column<T, Tab>, asc: bool) -> Self {
        self.base.order_by(column, asc);
        self
    }

    pub fn to_sql(&self) -> Result<(String, Vec<Value>), String> {
        let (sql, slots) = self.to_sql_slots();
        Ok((sql, fixed_values(slots)?))
    }

    pub(crate) fn to_sql_slots(&self) -> (String, Vec<ParamSlot>) {
        let mut sql = String::new();
        sql.push_str("SELECT ");
        sql.push_str(&self.names.join(", "));
        sql.push_str(" FROM ");
        sql.push_str(self.base.table);
        let mut slots = Vec::new();
        if let Some(expr) = &self.base.where_clause {
            sql.push_str(" WHERE ");
            expr.to_sql(&mut sql, &mut slots);
        }
        if let Some((col, asc)) = &self.base.order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(col);
            sql.push_str(if *asc { " ASC" } else { " DESC" });
        }
        if let Some(n) = self.base.limit {
            sql.push_str(" LIMIT ?");
            slots.push(ParamSlot::Fixed(Value::Integer(n as i64)));
        }
        if let Some(n) = self.base.offset {
            sql.push_str(" OFFSET ?");
            slots.push(ParamSlot::Fixed(Value::Integer(n as i64)));
        }
        (sql, slots)
    }

    pub fn names_slice(&self) -> &[&'static str] {
        &self.names
    }

    /// Precompile this projection. `*_param()` comparisons become bind
    /// parameters supplied at execution time, in order of appearance.
    pub async fn prepare(
        &self,
        db: &crate::ConnectionPool,
    ) -> Result<PreparedSelectCols<Tab, C>, crate::Error> {
        let (sql, slots) = self.to_sql_slots();
        let prepared = db.prepare(&sql).await?;
        Ok(PreparedSelectCols {
            inner: crate::connection_pool::PreparedStatement::new(prepared, slots),
            names: self.names.clone(),
            _p: PhantomData,
        })
    }
}

/// A precompiled, typed projection SELECT. Execute with the values for the
/// query's `*_param()` holes; rows decode into the projection's tuple type.
pub struct PreparedSelectCols<Tab, C: SelectList<Tab>> {
    inner: crate::connection_pool::PreparedStatement,
    names: Vec<&'static str>,
    _p: PhantomData<(Tab, C)>,
}

impl<Tab, C: SelectList<Tab>> PreparedSelectCols<Tab, C> {
    pub fn sql(&self) -> &str {
        self.inner.sql()
    }

    pub async fn all(&self, args: Vec<Value>) -> Result<Vec<C::Out>, crate::Error> {
        let result = self.inner.query(args).await?;
        let mut out = Vec::with_capacity(result.row_count());
        for mut row in result.into_rows() {
            out.push(C::decode_row(&mut row, &self.names)?);
        }
        Ok(out)
    }

    pub async fn one(&self, args: Vec<Value>) -> Result<C::Out, crate::Error> {
        let mut rows = self.all(args).await?;
        if rows.is_empty() {
            return Err(crate::Error::InvalidQuery("query returned 0 rows".into()));
        }
        Ok(rows.swap_remove(0))
    }
}

// We can't refer to tuple fields in a const context easily. Provide direct impls for 1..=4
impl<Tab, T1> SelectList<Tab> for (Column<T1, Tab>,)
where
    T1: FromValue,
{
    type Out = (T1,);
    fn names(&self, out: &mut Vec<&'static str>) {
        out.push(self.0.name)
    }
    fn decode_row(row: &mut DbRow, names: &[&'static str]) -> Result<Self::Out, DecodeError> {
        Ok((row.take_decode::<T1>(names[0])?,))
    }
}

impl<Tab, T1, T2> SelectList<Tab> for (Column<T1, Tab>, Column<T2, Tab>)
where
    T1: FromValue,
    T2: FromValue,
{
    type Out = (T1, T2);
    fn names(&self, out: &mut Vec<&'static str>) {
        out.push(self.0.name);
        out.push(self.1.name);
    }
    fn decode_row(row: &mut DbRow, names: &[&'static str]) -> Result<Self::Out, DecodeError> {
        Ok((
            row.take_decode::<T1>(names[0])?,
            row.take_decode::<T2>(names[1])?,
        ))
    }
}

impl<Tab, T1, T2, T3> SelectList<Tab> for (Column<T1, Tab>, Column<T2, Tab>, Column<T3, Tab>)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
{
    type Out = (T1, T2, T3);
    fn names(&self, out: &mut Vec<&'static str>) {
        out.push(self.0.name);
        out.push(self.1.name);
        out.push(self.2.name);
    }
    fn decode_row(row: &mut DbRow, names: &[&'static str]) -> Result<Self::Out, DecodeError> {
        Ok((
            row.take_decode::<T1>(names[0])?,
            row.take_decode::<T2>(names[1])?,
            row.take_decode::<T3>(names[2])?,
        ))
    }
}

impl<Tab, T1, T2, T3, T4> SelectList<Tab>
    for (
        Column<T1, Tab>,
        Column<T2, Tab>,
        Column<T3, Tab>,
        Column<T4, Tab>,
    )
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
{
    type Out = (T1, T2, T3, T4);
    fn names(&self, out: &mut Vec<&'static str>) {
        out.push(self.0.name);
        out.push(self.1.name);
        out.push(self.2.name);
        out.push(self.3.name);
    }
    fn decode_row(row: &mut DbRow, names: &[&'static str]) -> Result<Self::Out, DecodeError> {
        Ok((
            row.take_decode::<T1>(names[0])?,
            row.take_decode::<T2>(names[1])?,
            row.take_decode::<T3>(names[2])?,
            row.take_decode::<T4>(names[3])?,
        ))
    }
}
