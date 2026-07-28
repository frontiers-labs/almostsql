use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{
    LitStr, Result, Token, Type, braced, bracketed,
    parse::{Parse, ParseStream},
    parse_macro_input, token,
};

#[proc_macro]
pub fn migrations(input: TokenStream) -> TokenStream {
    let migrations = parse_macro_input!(input as Migrations);
    let expanded = migrations.expand();
    TokenStream::from(expanded)
}

struct Migrations {
    namespace: Option<String>,
    migrations: Vec<Migration>,
}

struct Migration {
    tables: Vec<Table>,
    alters: Vec<AlterTable>,
    raw_sql: Vec<LitStr>,
}

#[derive(Clone)]
struct Table {
    name: Ident,
    columns: Vec<Column>,
    foreign_keys: Vec<ForeignKey>,
    enable_vectors: bool,
}

#[derive(Clone)]
struct Column {
    name: Ident,
    column_name: Option<String>,
    ty: Type,
    tags: Vec<SqlTag>,
}

#[derive(Clone)]
struct ForeignKey {
    fields: Vec<Ident>,
    table: Ident,
    foreign_fields: Vec<Ident>,
}

struct AlterTable {
    name: Ident,
    actions: Vec<AlterAction>,
}

enum AlterAction {
    AddColumn { name: Ident, ty: Type },
    DropColumn { name: Ident },
    RenameColumn { from: Ident, to: Ident },
    RenameTable { to: Ident },
}

#[derive(Clone)]
enum SqlTag {
    PrimaryKey,
    Unique,
}

impl Parse for Migrations {
    fn parse(input: ParseStream) -> Result<Self> {
        // Optional `namespace <name> { ... }` wrapper. The version blocks live
        // inside it. Disambiguated from a legacy block literally named
        // `namespace` by requiring an identifier (the name) after the keyword.
        if input.peek(syn::Ident) {
            let fork = input.fork();
            let kw: Ident = fork.parse()?;
            if kw == "namespace" && fork.peek(syn::Ident) {
                input.parse::<Ident>()?; // consume `namespace`
                let name: Ident = input.parse()?;
                let content;
                braced!(content in input);

                let mut migrations = Vec::new();
                while !content.is_empty() {
                    migrations.push(content.parse()?);
                }

                return Ok(Migrations {
                    namespace: Some(name.to_string()),
                    migrations,
                });
            }
        }

        let mut migrations = Vec::new();
        while !input.is_empty() {
            migrations.push(input.parse()?);
        }

        Ok(Migrations {
            namespace: None,
            migrations,
        })
    }
}

impl Parse for Migration {
    fn parse(input: ParseStream) -> Result<Self> {
        let _name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut tables = Vec::new();
        let mut alters = Vec::new();
        let mut raw_sql = Vec::new();

        while !content.is_empty() {
            let lookahead = content.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: Ident = content.fork().parse()?;
                if ident == "table" {
                    tables.push(content.parse()?);
                } else if ident == "alter" {
                    alters.push(content.parse()?);
                } else if ident == "raw" {
                    content.parse::<Ident>()?; // consume "raw"
                    let sql: LitStr = content.parse()?;
                    content.parse::<Token![;]>()?;
                    raw_sql.push(sql);
                } else {
                    return Err(lookahead.error());
                }
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(Migration {
            tables,
            alters,
            raw_sql,
        })
    }
}

impl Parse for Table {
    fn parse(input: ParseStream) -> Result<Self> {
        let table_keyword: Ident = input.parse()?;
        if table_keyword != "table" {
            return Err(syn::Error::new(table_keyword.span(), "Expected 'table'"));
        }
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut columns = Vec::new();
        let mut foreign_keys = Vec::new();
        let mut enable_vectors = false;

        while !content.is_empty() {
            let lookahead = content.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: Ident = content.fork().parse()?;
                if ident == "foreign_key" {
                    foreign_keys.push(content.parse()?);
                } else if ident == "enable_vectors" {
                    content.parse::<Ident>()?; // consume "enable_vectors"
                    content.parse::<Token![;]>()?;
                    enable_vectors = true;
                } else if content.peek2(Token![:]) {
                    columns.push(content.parse()?);
                } else {
                    return Err(lookahead.error());
                }
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(Table {
            name,
            columns,
            foreign_keys,
            enable_vectors,
        })
    }
}

impl Parse for Column {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;

        let mut tags = Vec::new();
        let mut column_name = None;

        // Parse attributes (tags and column name)
        if input.peek(token::Bracket) {
            let content;
            bracketed!(content in input);

            while !content.is_empty() {
                if content.peek(syn::Ident) && content.peek2(Token![=]) {
                    // Parse column_name = "actual_name"
                    let attr_name: Ident = content.parse()?;
                    content.parse::<Token![=]>()?;
                    let attr_value: LitStr = content.parse()?;

                    if attr_name == "column" {
                        column_name = Some(attr_value.value());
                    }

                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    }
                } else {
                    // Parse regular tags
                    let tag_ident: Ident = content.parse()?;
                    let tag = match tag_ident.to_string().as_str() {
                        "PrimaryKey" => SqlTag::PrimaryKey,
                        "Unique" => SqlTag::Unique,
                        _ => return Err(syn::Error::new(tag_ident.span(), "Unknown SQL tag")),
                    };
                    tags.push(tag);

                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    }
                }
            }
        }

        input.parse::<Token![,]>()?;

        Ok(Column {
            name,
            column_name,
            ty,
            tags,
        })
    }
}

impl Parse for ForeignKey {
    fn parse(input: ParseStream) -> Result<Self> {
        let foreign_key_keyword: Ident = input.parse()?;
        if foreign_key_keyword != "foreign_key" {
            return Err(syn::Error::new(
                foreign_key_keyword.span(),
                "Expected 'foreign_key'",
            ));
        }
        let content;
        syn::parenthesized!(content in input);

        // Parse single field or field list
        let first_field: Ident = content.parse()?;
        let mut fields = vec![first_field];

        // Check for additional fields (comma-separated)
        while content.peek(Token![,]) && !content.peek2(Token![-]) {
            content.parse::<Token![,]>()?;
            fields.push(content.parse()?);
        }

        content.parse::<Token![-]>()?;
        content.parse::<Token![>]>()?;

        let table: Ident = content.parse()?;
        content.parse::<Token![.]>()?;

        // Parse single foreign field or field list
        let first_foreign_field: Ident = content.parse()?;
        let mut foreign_fields = vec![first_foreign_field];

        // Check for additional foreign fields (comma-separated)
        while content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
            foreign_fields.push(content.parse()?);
        }

        input.parse::<Token![;]>()?;

        Ok(ForeignKey {
            fields,
            table,
            foreign_fields,
        })
    }
}

impl Parse for AlterTable {
    fn parse(input: ParseStream) -> Result<Self> {
        let alter_keyword: Ident = input.parse()?;
        if alter_keyword != "alter" {
            return Err(syn::Error::new(alter_keyword.span(), "Expected 'alter'"));
        }
        let table_keyword: Ident = input.parse()?;
        if table_keyword != "table" {
            return Err(syn::Error::new(table_keyword.span(), "Expected 'table'"));
        }
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut actions = Vec::new();

        while !content.is_empty() {
            let kw: Ident = content.parse()?;
            if kw == "add" {
                let col_name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let ty: Type = content.parse()?;
                content.parse::<Token![;]>()?;
                actions.push(AlterAction::AddColumn { name: col_name, ty });
            } else if kw == "drop" {
                let col_name: Ident = content.parse()?;
                content.parse::<Token![;]>()?;
                actions.push(AlterAction::DropColumn { name: col_name });
            } else if kw == "rename" {
                // `rename table <new>;` renames the table; `rename <a> -> <b>;`
                // renames a column. "table" is reserved as a column name here.
                if content.peek(syn::Ident) && content.fork().parse::<Ident>()? == "table" {
                    content.parse::<Ident>()?; // consume "table"
                    let to: Ident = content.parse()?;
                    content.parse::<Token![;]>()?;
                    actions.push(AlterAction::RenameTable { to });
                } else {
                    let from: Ident = content.parse()?;
                    content.parse::<Token![-]>()?;
                    content.parse::<Token![>]>()?;
                    let to: Ident = content.parse()?;
                    content.parse::<Token![;]>()?;
                    actions.push(AlterAction::RenameColumn { from, to });
                }
            } else {
                return Err(syn::Error::new(
                    kw.span(),
                    "Expected 'add', 'drop', or 'rename'",
                ));
            }
        }

        Ok(AlterTable { name, actions })
    }
}

impl AlterTable {
    fn expand(&self) -> proc_macro2::TokenStream {
        let name = self.name.to_string();
        let action_calls: Vec<proc_macro2::TokenStream> =
            self.actions.iter().map(|a| a.expand()).collect();

        quote! {
            ::almostsql::AlterTable::from_name(#name)
                #(#action_calls)*
        }
    }

    // Fold this alter into the accumulated schema so generated typed modules
    // reflect the final, post-migration table shape.
    fn apply_to(&self, table: &mut Table) {
        for action in &self.actions {
            match action {
                AlterAction::AddColumn { name, ty } => table.columns.push(Column {
                    name: name.clone(),
                    column_name: None,
                    ty: ty.clone(),
                    tags: vec![],
                }),
                AlterAction::DropColumn { name } => {
                    table.columns.retain(|c| &c.name != name);
                }
                AlterAction::RenameColumn { from, to } => {
                    if let Some(c) = table.columns.iter_mut().find(|c| &c.name == from) {
                        c.name = to.clone();
                        c.column_name = None;
                    }
                }
                AlterAction::RenameTable { to } => table.name = to.clone(),
            }
        }
    }
}

impl AlterAction {
    fn expand(&self) -> proc_macro2::TokenStream {
        match self {
            AlterAction::AddColumn { name, ty } => {
                let n = name.to_string();
                quote! { .add_column::<#ty, _>(#n) }
            }
            AlterAction::DropColumn { name } => {
                let n = name.to_string();
                quote! { .drop_column(#n) }
            }
            AlterAction::RenameColumn { from, to } => {
                let f = from.to_string();
                let t = to.to_string();
                quote! { .rename_column(#f, #t) }
            }
            AlterAction::RenameTable { to } => {
                let t = to.to_string();
                quote! { .rename_to(#t) }
            }
        }
    }
}

impl Migrations {
    fn expand(&self) -> proc_macro2::TokenStream {
        let migration_exprs: Vec<proc_macro2::TokenStream> = self
            .migrations
            .iter()
            .map(|migration| migration.expand())
            .collect();

        // Build the final schema by replaying creates and alters in order, so
        // generated typed modules match the table shape after all migrations.
        let mut final_tables: Vec<Table> = Vec::new();
        for migration in &self.migrations {
            for t in &migration.tables {
                final_tables.push(t.clone());
            }
            for a in &migration.alters {
                if let Some(t) = final_tables.iter_mut().find(|t| t.name == a.name) {
                    a.apply_to(t);
                }
            }
        }

        let table_modules: Vec<proc_macro2::TokenStream> =
            final_tables.iter().map(|t| t.expand_module()).collect();

        // Namespaced form exposes a composable `schema()` fragment; the legacy
        // form keeps the bare `get_migrations()` entry point.
        let entry = match &self.namespace {
            Some(name) => quote! {
                pub fn schema() -> ::almostsql::SchemaSet {
                    ::almostsql::SchemaSet::new(#name, vec![
                        #(#migration_exprs),*
                    ])
                }
            },
            None => quote! {
                pub fn get_migrations() -> Vec<::almostsql::Migration> {
                    vec![
                        #(#migration_exprs),*
                    ]
                }
            },
        };

        quote! {
            #entry

            #(#table_modules)*
        }
    }
}

impl Migration {
    fn expand(&self) -> proc_macro2::TokenStream {
        let table_exprs: Vec<proc_macro2::TokenStream> =
            self.tables.iter().map(|table| table.expand()).collect();

        let alter_exprs: Vec<proc_macro2::TokenStream> =
            self.alters.iter().map(|alter| alter.expand()).collect();

        let raw_sql_exprs: Vec<proc_macro2::TokenStream> = self
            .raw_sql
            .iter()
            .map(|sql| {
                quote! { .raw(#sql) }
            })
            .collect();

        quote! {
            ::almostsql::Migration::new()
                #(.table(#table_exprs))*
                #(.alter_table(#alter_exprs))*
                #(#raw_sql_exprs)*
        }
    }
}

impl Table {
    fn expand(&self) -> proc_macro2::TokenStream {
        let table_name = self.name.to_string();

        let column_exprs: Vec<proc_macro2::TokenStream> =
            self.columns.iter().map(|column| column.expand()).collect();

        let foreign_key_exprs: Vec<proc_macro2::TokenStream> =
            self.foreign_keys.iter().map(|fk| fk.expand()).collect();

        let enable_vectors = if self.enable_vectors {
            quote! { .enable_vectors() }
        } else {
            quote! {}
        };

        quote! {
            ::almostsql::Table::from_name(#table_name)
                #(#column_exprs)*
                #(#foreign_key_exprs)*
                #enable_vectors
        }
    }

    // Emit a table module with typed Row, Column consts, and select helpers
    fn expand_module(&self) -> proc_macro2::TokenStream {
        let mod_ident = &self.name;
        let table_name_str = self.name.to_string();

        // fields for Row
        let row_fields: Vec<proc_macro2::TokenStream> = self
            .columns
            .iter()
            .map(|c| {
                let name_ident = &c.name;
                let ty = &c.ty;
                quote! { pub #name_ident: #ty }
            })
            .collect();

        // Column consts (DB column name may differ via [column = "..."])
        let column_consts: Vec<proc_macro2::TokenStream> = self
            .columns
            .iter()
            .map(|c| {
                let name_ident = &c.name;
                let ty = &c.ty;
                let db_name = c
                    .column_name
                    .clone()
                    .unwrap_or_else(|| c.name.to_string());
                quote! { pub const #name_ident: ::almostsql::Column<#ty, Table> = ::almostsql::Column::new(#db_name); }
            })
            .collect();

        // Row construction from generic Row
        let row_inits: Vec<proc_macro2::TokenStream> = self
            .columns
            .iter()
            .map(|c| {
                let name_ident = &c.name;
                let ty = &c.ty;
                let db_name = c.column_name.clone().unwrap_or_else(|| c.name.to_string());
                quote! { #name_ident: r.decode::<#ty>(#db_name)? }
            })
            .collect();

        // Insert builder setters per column
        let insert_setters: Vec<proc_macro2::TokenStream> = self
            .columns
            .iter()
            .map(|c| {
                let name_ident = &c.name;
                let ty = &c.ty;
                let db_name = c.column_name.clone().unwrap_or_else(|| c.name.to_string());
                quote! {
                    pub fn #name_ident<U: ::almostsql::ColumnInput<#ty>>(mut self, v: U) -> Self {
                        self.cols.push(#db_name);
                        let val = <U as ::almostsql::ColumnInput<#ty>>::into_value(v);
                        self.params.push(val);
                        self
                    }
                }
            })
            .collect();

        // Update builder setters per column (SET column = value)
        let update_setters: Vec<proc_macro2::TokenStream> = self
            .columns
            .iter()
            .map(|c| {
                let name_ident = &c.name;
                let ty = &c.ty;
                let db_name = c.column_name.clone().unwrap_or_else(|| c.name.to_string());
                quote! {
                    pub fn #name_ident<U: ::almostsql::ColumnInput<#ty>>(mut self, v: U) -> Self {
                        let val = <U as ::almostsql::ColumnInput<#ty>>::into_value(v);
                        self.0 = self.0.set(#db_name, val);
                        self
                    }
                }
            })
            .collect();

        quote! {
            pub mod #mod_ident {
                use super::*;
                /// Marker type for this table
                pub struct Table;

                pub const TABLE_NAME: &str = #table_name_str;

                /// Strongly-typed row for this table
                pub struct Row { #( #row_fields, )* }

                #( #column_consts )*

                /// Local wrapper over almostsql::Select to allow inherent impls
                pub struct SelectQuery(pub(crate) ::almostsql::Select<Table>);

                /// Start a SELECT * query for this table
                pub fn select() -> SelectQuery {
                    SelectQuery(::almostsql::Select::new(TABLE_NAME))
                }

                impl SelectQuery {
                    pub fn where_(self, predicate: ::almostsql::Expr<Table>) -> Self {
                        SelectQuery(self.0.where_(predicate))
                    }

                    pub fn limit(self, n: u64) -> Self {
                        SelectQuery(self.0.limit(n))
                    }

                    pub fn offset(self, n: u64) -> Self {
                        SelectQuery(self.0.offset(n))
                    }

                    pub fn order_by_asc<T>(mut self, column: ::almostsql::Column<T, Table>) -> Self {
                        self.0.order_by(column, true);
                        self
                    }

                    pub fn order_by_desc<T>(mut self, column: ::almostsql::Column<T, Table>) -> Self {
                        self.0.order_by(column, false);
                        self
                    }

                    /// Fetch all rows for this select
                    pub async fn all(self, db: &::almostsql::ConnectionPool) -> Result<Vec<Row>, Box<dyn std::error::Error + Send + Sync>> {
                        let (sql, params) = self.0.to_sql();
                        let res = db.query_with_params(&sql, params).await?;
                        let mut out = Vec::with_capacity(res.rows().len());
                        for r in res.rows() {
                            let row = Row { #( #row_inits, )* };
                            out.push(row);
                        }
                        Ok(out)
                    }

                    /// Fetch exactly one row or return an error
                    pub async fn one(self, db: &::almostsql::ConnectionPool) -> Result<Row, Box<dyn std::error::Error + Send + Sync>> {
                        let mut rows = self.limit(1).all(db).await?;
                        rows.pop().ok_or_else(|| "query returned 0 rows".into())
                    }
                }

                /// Local wrapper for projection selects
                pub struct SelectColsQuery<C: ::almostsql::SelectList<Table>>(pub(crate) ::almostsql::SelectCols<Table, C>);

                /// Start a SELECT of specific columns
                pub fn select_cols<C: ::almostsql::SelectList<Table>>(cols: C) -> SelectColsQuery<C> {
                    SelectColsQuery(::almostsql::Select::new(TABLE_NAME).select(cols))
                }

                impl<C: ::almostsql::SelectList<Table>> SelectColsQuery<C> {
                    pub fn where_(self, predicate: ::almostsql::Expr<Table>) -> Self {
                        SelectColsQuery(self.0.where_(predicate))
                    }
                    pub fn limit(self, n: u64) -> Self {
                        SelectColsQuery(self.0.limit(n))
                    }
                    pub fn offset(self, n: u64) -> Self {
                        SelectColsQuery(self.0.offset(n))
                    }
                    pub fn order_by_asc<T>(self, column: ::almostsql::Column<T, Table>) -> Self {
                        SelectColsQuery(self.0.order_by(column, true))
                    }
                    pub fn order_by_desc<T>(self, column: ::almostsql::Column<T, Table>) -> Self {
                        SelectColsQuery(self.0.order_by(column, false))
                    }

                    pub async fn all(self, db: &::almostsql::ConnectionPool) -> Result<Vec<<C as ::almostsql::SelectList<Table>>::Out>, Box<dyn std::error::Error + Send + Sync>> {
                        let (sql, params) = self.0.to_sql();
                        let res = db.query_with_params(&sql, params).await?;
                        let mut out = Vec::with_capacity(res.rows().len());
                        let colnames = self.0.names_slice().to_vec();
                        for r in res.rows() {
                            let row = <C as ::almostsql::SelectList<Table>>::decode_row(r, &colnames)?;
                            out.push(row);
                        }
                        Ok(out)
                    }

                    pub async fn one(self, db: &::almostsql::ConnectionPool) -> Result<<C as ::almostsql::SelectList<Table>>::Out, Box<dyn std::error::Error + Send + Sync>> {
                        let mut rows = self.limit(1).all(db).await?;
                        rows.pop().ok_or_else(|| "query returned 0 rows".into())
                    }
                }

                /// Typed insert builder for this table
                pub struct InsertBuilder {
                    cols: Vec<&'static str>,
                    params: Vec<::almostsql::Value>,
                }

                /// Start an INSERT builder for this table
                pub fn insert() -> InsertBuilder {
                    InsertBuilder { cols: Vec::new(), params: Vec::new() }
                }

                impl InsertBuilder {
                    #(#insert_setters)*

                    pub async fn execute(self, db: &::almostsql::ConnectionPool) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
                        if self.cols.is_empty() {
                            return Ok(0);
                        }
                        let placeholders = std::iter::repeat("?")
                            .take(self.cols.len())
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "INSERT INTO {} ({}) VALUES ({});",
                            TABLE_NAME,
                            self.cols.join(", "),
                            placeholders
                        );
                        let res = db.query_with_params(&sql, self.params).await?;
                        Ok(res.affected_rows())
                    }
                }

                /// Typed UPDATE builder for this table
                pub struct UpdateBuilder(pub(crate) ::almostsql::Update<Table>);

                /// Start an UPDATE builder for this table
                pub fn update() -> UpdateBuilder {
                    UpdateBuilder(::almostsql::Update::new(TABLE_NAME))
                }

                impl UpdateBuilder {
                    #(#update_setters)*

                    /// Restrict the rows to update. Required unless `all()` is called.
                    pub fn where_(mut self, predicate: ::almostsql::Expr<Table>) -> Self {
                        self.0 = self.0.where_(predicate);
                        self
                    }

                    /// Opt in to updating every row (no WHERE clause).
                    pub fn all(mut self) -> Self {
                        self.0 = self.0.all();
                        self
                    }

                    pub async fn execute(self, db: &::almostsql::ConnectionPool) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
                        if self.0.is_empty() {
                            return Ok(0);
                        }
                        let (sql, params) = self.0.to_sql()?;
                        let res = db.query_with_params(&sql, params).await?;
                        Ok(res.affected_rows())
                    }
                }

                /// Typed DELETE builder for this table
                pub struct DeleteBuilder(pub(crate) ::almostsql::Delete<Table>);

                /// Start a DELETE builder for this table
                pub fn delete() -> DeleteBuilder {
                    DeleteBuilder(::almostsql::Delete::new(TABLE_NAME))
                }

                impl DeleteBuilder {
                    /// Restrict the rows to delete. Required unless `all()` is called.
                    pub fn where_(mut self, predicate: ::almostsql::Expr<Table>) -> Self {
                        self.0 = self.0.where_(predicate);
                        self
                    }

                    /// Opt in to deleting every row (no WHERE clause).
                    pub fn all(mut self) -> Self {
                        self.0 = self.0.all();
                        self
                    }

                    pub async fn execute(self, db: &::almostsql::ConnectionPool) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
                        let (sql, params) = self.0.to_sql()?;
                        let res = db.query_with_params(&sql, params).await?;
                        Ok(res.affected_rows())
                    }
                }
            }
        }
    }
}

impl Column {
    fn expand(&self) -> proc_macro2::TokenStream {
        let name_string = self.name.to_string();
        let column_name = self.column_name.as_ref().unwrap_or(&name_string);
        let column_type = &self.ty;

        let tags: Vec<proc_macro2::TokenStream> = self
            .tags
            .iter()
            .map(|tag| match tag {
                SqlTag::PrimaryKey => quote! { ::almostsql::SqlTag::PrimaryKey },
                SqlTag::Unique => quote! { ::almostsql::SqlTag::Unique },
            })
            .collect();

        quote! {
            .add::<#column_type, _>(#column_name, &[#(#tags),*])
        }
    }
}

impl ForeignKey {
    fn expand(&self) -> proc_macro2::TokenStream {
        let fields: Vec<String> = self.fields.iter().map(|f| f.to_string()).collect();
        let table_name = self.table.to_string();
        let foreign_fields: Vec<String> =
            self.foreign_fields.iter().map(|f| f.to_string()).collect();

        quote! {
            .foreign_key(&[#(#fields),*], #table_name, &[#(#foreign_fields),*])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_macro_simple() {
        let input = quote::quote! {
            test_migration {
                table users {
                    id: String,
                    name: String,
                }
            }
        };

        let result = std::panic::catch_unwind(|| {
            let parsed: Migrations = syn::parse2(input).unwrap();
            parsed.expand()
        });

        assert!(result.is_ok());
    }
}
