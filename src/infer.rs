//! Our type inference subsystem.

// This is work in progress.
#![allow(dead_code)]

use crate::{
    ast::{self, SelectList},
    errors::{format_err, Result},
    scope::{CaseInsensitiveIdent, Scope, ScopeHandle},
    tokenizer::{Ident, Literal, LiteralValue, Spanned},
    types::{ArgumentType, ColumnType, SimpleType, TableType, Type, ValueType},
};

// TODO: Remember this rather scary example. Verify BigQuery supports it
// and that we need it.
//
// ```txt
// COUNT(DISTINCT x % 9)
// FROM SELECT 18 AS x, 'a' AS z
// GROUP BY z
//
// % => Fn(INT64, INT64) -> INT64
// x => Agg<INT64>
// 9 => INT64
//
// x % 9 => Agg<INT64>
//
// -- Auto lifting ick.
// Agg(%, [0]) => Fn(Agg<INT64>, INT64) -> Agg<INT64>
// Agg(%, [1]) => Fn(INT64, Agg<INT64>) -> Agg<INT64>
// Agg(%, [0, 1]) => Fn(Agg<INT64>, Agg<INT64>) -> Agg<INT64>
// ```

/// Types which support inference.
pub trait InferTypes {
    /// The type of AST node itself, if it has one.
    type Type;

    /// Infer types for this value. Returns the scope after inference in case
    /// the caller needs to use it for a later sibling node.
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)>;
}

impl InferTypes for ast::SqlProgram {
    type Type = Option<TableType>;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let mut ty = None;
        let mut scope = scope.clone();

        // Example:
        //
        // Start with default root scope.
        //
        // CREATE TABLE foo (x INT64, y STRING);
        //   - Add `foo` to the scope.
        //
        // CREATE TABLE bar AS SELECT * FROM foo;
        //   - Add `bar` to the scope.
        //
        // DROP TABLE foo;
        //   - Remove `foo` from the scope.

        for statement in self.statements.node_iter_mut() {
            (ty, scope) = statement.infer_types(&scope)?;
        }

        Ok((ty, scope))
    }
}

impl InferTypes for ast::Statement {
    type Type = Option<TableType>;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::Statement::Query(stmt) => {
                let (ty, _scope) = stmt.infer_types(scope)?;
                Ok((Some(ty), scope.clone()))
            }
            ast::Statement::DeleteFrom(_) => todo!("DELETE FROM"),
            ast::Statement::InsertInto(_) => todo!("INSERT INTO"),
            ast::Statement::CreateTable(stmt) => {
                let ((), scope) = stmt.infer_types(scope)?;
                Ok((None, scope))
            }
            ast::Statement::CreateView(_) => todo!("CREATE VIEW"),
            ast::Statement::DropTable(stmt) => {
                let ((), scope) = stmt.infer_types(scope)?;
                Ok((None, scope))
            }
            ast::Statement::DropView(_) => todo!("DROP VIEW"),
        }
    }
}

/// Convert a table name to an identifier.
fn ident_from_table_name(table_name: &ast::TableName) -> Result<CaseInsensitiveIdent> {
    match table_name {
        ast::TableName::Table { table, .. } => Ok(table.clone().into()),
        _ => Err(format_err!(
            "type inference doesn't yet support dotted name: {:?}",
            table_name
        )),
    }
}

impl InferTypes for ast::CreateTableStatement {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::CreateTableStatement {
                table_name,
                definition: ast::CreateTableDefinition::Columns { columns, .. },
                ..
            } => {
                let mut scope = Scope::new(scope);
                let column_decls = columns
                    .node_iter()
                    .map(|column| {
                        let ty = ValueType::try_from(&column.data_type)?;
                        Ok(ColumnType {
                            name: column.name.clone(),
                            ty,
                            // TODO: We don't support this in the main grammar yet.
                            not_null: false,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                scope.add(
                    ident_from_table_name(table_name)?,
                    Type::Table(TableType {
                        columns: column_decls,
                    }),
                )?;
                Ok(((), scope.into_handle()))
            }
            ast::CreateTableStatement {
                table_name,
                definition:
                    ast::CreateTableDefinition::As {
                        query_statement, ..
                    },
                ..
            } => {
                let (ty, _scope) = query_statement.infer_types(scope)?;
                let mut scope = Scope::new(scope);
                scope.add(ident_from_table_name(table_name)?, Type::Table(ty))?;
                Ok(((), scope.into_handle()))
            }
        }
    }
}

impl InferTypes for ast::DropTableStatement {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::DropTableStatement { table_name, .. } = self;
        let mut scope = Scope::new(scope);
        let table = ident_from_table_name(table_name)?;
        scope.hide(&table)?;
        Ok(((), scope.into_handle()))
    }
}

impl InferTypes for ast::QueryStatement {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::QueryStatement { query_expression } = self;
        query_expression.infer_types(scope)
    }
}

impl InferTypes for ast::QueryExpression {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::QueryExpression::SelectExpression(expr) => expr.infer_types(scope),
            ast::QueryExpression::Nested { query, .. } => query.infer_types(scope),
            ast::QueryExpression::With { ctes, query, .. } => {
                // Non-recursive CTEs, so each will create a new namespace.
                let mut scope = scope.to_owned();
                for cte in ctes.node_iter_mut() {
                    let ((), new_scope) = cte.infer_types(&scope)?;
                    scope = new_scope;
                }
                query.infer_types(&scope)
            }
            ast::QueryExpression::SetOperation { .. } => todo!("set operation"),
        }
    }
}

impl InferTypes for ast::CommonTableExpression {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::CommonTableExpression { name, query, .. } = self;
        let (ty, scope) = query.infer_types(scope)?;
        let mut scope = Scope::new(&scope);
        scope.add(name.to_owned().into(), Type::Table(ty))?;
        Ok(((), scope.into_handle()))
    }
}

impl InferTypes for ast::SelectExpression {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        // In order of type inference:
        //
        // - FROM clause (including JOIN). Introduces both tables and columns.
        // - WHERE clause. Only uses types.
        // - GROUP BY. Does something complicated, splitting columns into ones
        //   that can be used in SELECT, and ones that can only be used in
        //   aggregates. Cannot see columns from SELECT (we checked).
        // - SELECT list. Introduces columns.
        // - HAVING. Same as WHERE, but uses output of GROUP BY. BigQuery allows
        //   using SELECT aliases, too, even though this happens _before_
        //   SELECT.
        // - QUALIFY. Same as WHERE, but can access columns from SELECT.
        // - ORDER BY. Only uses types.
        // - LIMIT. Only uses types.

        let ast::SelectExpression {
            //select_options,
            select_list: SelectList {
                items: select_list, ..
            },
            from_clause,
            //where_clause,
            //group_by,
            //having,
            //qualify,
            //order_by,
            //limit,
            ..
        } = self;

        let mut scope = scope.to_owned();
        if let Some(from_clause) = from_clause {
            ((), scope) = from_clause.infer_types(&scope)?;
        }

        let mut next_anon_col_id: u64 = 0;
        let mut cols = vec![];
        for item in select_list.node_iter_mut() {
            match item {
                ast::SelectListItem::Expression { expression, alias } => {
                    // BigQuery does not allow select list items to see names
                    // bound by other select list items.
                    let (ty, _scope) = expression.infer_types(&scope)?;
                    let name = alias
                        .infer_column_name()
                        .or_else(|| expression.infer_column_name())
                        .unwrap_or_else(|| {
                            // We try to predict how BigQuery will name these.
                            let ident =
                                Ident::new(&format!("_f{}", next_anon_col_id), expression.span());
                            next_anon_col_id += 1;
                            ident
                        });

                    cols.push(ColumnType {
                        name,
                        ty,
                        not_null: false,
                    });
                }
                _ => todo!("select list item"),
            }
        }
        let table_type = TableType { columns: cols };
        Ok((table_type, scope.clone()))
    }
}

impl InferTypes for ast::FromClause {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::FromClause {
            from_item,
            join_operations,
            ..
        } = self;
        let ((), scope) = from_item.infer_types(scope)?;
        if !join_operations.is_empty() {
            todo!("join operations")
        }
        Ok(((), scope))
    }
}

impl InferTypes for ast::FromItem {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::FromItem::TableName { table_name, alias } => {
                let table = ident_from_table_name(table_name)?;
                let table_type = scope
                    .get(&table)
                    .ok_or_else(|| format_err!("table {:?} not found in scope", table_name))?;
                let table_type = match table_type {
                    Type::Table(table_type) => table_type,
                    _ => Err(format_err!(
                        "table {:?} is not a table: {:?}",
                        table_name,
                        table_type
                    ))?,
                };

                if alias.is_some() {
                    todo!("from with alias");
                }

                let mut scope = Scope::new(scope);
                for column in &table_type.columns {
                    scope.add(
                        column.name.clone().into(),
                        Type::Argument(ArgumentType::Value(column.ty.clone())),
                    )?;
                }
                Ok(((), scope.into_handle()))
            }
            ast::FromItem::Subquery { .. } => todo!("from subquery"),
            ast::FromItem::Unnest { .. } => todo!("from unnest"),
        }
    }
}

impl InferTypes for ast::Expression {
    type Type = ValueType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::Expression::BoolValue(_) => Ok((
                ValueType::Simple(crate::types::SimpleType::Bool),
                scope.clone(),
            )),
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(scope),
            ast::Expression::ColumnName(ident) => {
                let ident = ident.to_owned().into();
                let ty = scope
                    .get(&ident)
                    .ok_or_else(|| format_err!("column {:?} not found in scope", ident))?;
                let ty = match ty {
                    Type::Argument(ArgumentType::Value(ty)) => ty,
                    _ => Err(format_err!("column {:?} is not a value: {:?}", ident, ty))?,
                };
                Ok((ty.to_owned(), scope.clone()))
            }
            ast::Expression::TableAndColumnName(ast::TableAndColumnName {
                table_name,
                column_name,
                ..
            }) => {
                let table = ident_from_table_name(table_name)?;
                let table_type = scope
                    .get(&table)
                    .ok_or_else(|| format_err!("table {:?} not found in scope", table_name))?;
                let table_type = match table_type {
                    Type::Table(table_type) => table_type,
                    _ => Err(format_err!(
                        "table {:?} is not a table: {:?}",
                        table_name,
                        table_type
                    ))?,
                };
                let column_type = table_type
                    .columns
                    .iter()
                    .find(|column_type| column_type.name == *column_name)
                    .ok_or_else(|| {
                        format_err!("column {:?} not found in table {:?}", column_name, table)
                    })?;
                Ok((column_type.ty.to_owned(), scope.clone()))
            }
            _ => todo!("expression: {:?}", self),
        }
    }
}

impl InferTypes for LiteralValue {
    type Type = ValueType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let simple_ty = match self {
            LiteralValue::Int64(_) => SimpleType::Int64,
            LiteralValue::Float64(_) => SimpleType::Float64,
            LiteralValue::String(_) => SimpleType::String,
        };
        Ok((ValueType::Simple(simple_ty), scope.clone()))
    }
}

/// Figure out whether an expression defines an implicit column name.
pub trait InferColumnName {
    /// Infer the column name, if any.
    fn infer_column_name(&mut self) -> Option<Ident>;
}

impl<T: InferColumnName> InferColumnName for Option<T> {
    fn infer_column_name(&mut self) -> Option<Ident> {
        match self {
            Some(expr) => expr.infer_column_name(),
            None => None,
        }
    }
}

impl InferColumnName for ast::Expression {
    fn infer_column_name(&mut self) -> Option<Ident> {
        match self {
            ast::Expression::ColumnName(ident) => Some(ident.clone()),
            ast::Expression::TableAndColumnName(ast::TableAndColumnName {
                column_name, ..
            }) => Some(column_name.clone()),
            _ => None,
        }
    }
}

impl InferColumnName for ast::Alias {
    fn infer_column_name(&mut self) -> Option<Ident> {
        Some(self.ident.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use pretty_assertions::assert_eq;

    use crate::{
        ast::parse_sql,
        scope::{CaseInsensitiveIdent, Scope, ScopeValue},
        tokenizer::Span,
        types::tests::ty,
    };

    use super::*;

    fn infer(sql: &str) -> Result<(Option<TableType>, ScopeHandle)> {
        let mut program = match parse_sql(Path::new("test.sql"), sql) {
            Ok(program) => program,
            Err(e) => {
                e.emit();
                panic!("parse error");
            }
        };
        let scope = Scope::root();
        program.infer_types(&scope)
    }

    fn lookup(scope: &ScopeHandle, name: &str) -> Option<ScopeValue> {
        scope
            .get(&CaseInsensitiveIdent::new(name, Span::Unknown))
            .cloned()
    }

    macro_rules! assert_defines {
        ($scope:expr, $name:expr, $ty:expr) => {
            assert_eq!(lookup(&$scope, $name), Some(ty($ty)))
        };
    }

    macro_rules! assert_not_defines {
        ($scope:expr, $name:expr) => {
            assert_eq!(lookup(&$scope, $name), None)
        };
    }

    #[test]
    fn root_scope_defines_functions() {
        let (_, scope) = infer("SELECT 1 AS x").unwrap();
        assert_defines!(scope, "LOWER", "Fn(STRING) -> STRING");
        assert_defines!(scope, "lower", "Fn(STRING) -> STRING");
        assert_not_defines!(scope, "NO_SUCH_FUNCTION");
    }

    #[test]
    fn create_table_adds_table_to_scope() {
        let (_, scope) = infer("CREATE TABLE foo (x INT64, y STRING)").unwrap();
        assert_defines!(scope, "foo", "TABLE<x INT64, y STRING>");
    }

    #[test]
    fn drop_table_removes_table_from_scope() {
        let (_, scope) = infer("CREATE TABLE foo (x INT64, y STRING); DROP TABLE foo").unwrap();
        assert_not_defines!(scope, "foo");
    }

    #[test]
    fn create_table_as_infers_column_types() {
        let (_, scope) = infer("CREATE TABLE foo AS SELECT 'a' AS x, TRUE AS y").unwrap();
        assert_defines!(scope, "foo", "TABLE<x STRING, y BOOL>");
    }

    #[test]
    fn ctes_are_added_to_scope() {
        let sql = "
CREATE TABLE foo AS
WITH
    t1 AS (SELECT 'a' AS x),
    t2 AS (SELECT x FROM t1)
SELECT x FROM t2";
        let (_, scope) = infer(sql).unwrap();
        assert_defines!(scope, "foo", "TABLE<x STRING>");
    }

    #[test]
    fn anon_and_aliased_columns() {
        let (_, scope) = infer("CREATE TABLE foo AS SELECT 1, 2 AS x, 3").unwrap();
        assert_defines!(scope, "foo", "TABLE<_f0 INT64, x INT64, _f1 INT64>");
    }

    #[test]
    fn columns_scoped_by_table() {
        let sql = "
CREATE TABLE foo AS
WITH t AS (SELECT 'a' AS x)
SELECT t.x FROM t";
        let (_, scope) = infer(sql).unwrap();
        assert_defines!(scope, "foo", "TABLE<x STRING>");
    }
}
