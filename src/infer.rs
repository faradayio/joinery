//! Our type inference subsystem.

// This is work in progress.
#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    ast::{self, SelectList},
    errors::{format_err, Result},
    scope::{CaseInsensitiveIdent, Scope, ScopeHandle},
    tokenizer::{Literal, LiteralValue},
    types::{ArgumentType, ColumnType, SimpleType, TableType, Type, TypeVar, ValueType},
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
    /// Infer types for this value. Returns the scope after inference in case
    /// the caller needs to use it for a later sibling node.
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)>;
}

impl InferTypes for ast::SqlProgram {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
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
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        match self {
            // TODO: This can't bind anything into our scope, but we should
            // check types anyway.
            ast::Statement::Query(_) => Ok((None, scope.clone())),
            ast::Statement::DeleteFrom(_) => todo!(),
            ast::Statement::InsertInto(_) => todo!(),
            ast::Statement::CreateTable(stmt) => stmt.infer_types(scope),
            ast::Statement::CreateView(_) => todo!(),
            ast::Statement::DropTable(stmt) => stmt.infer_types(scope),
            ast::Statement::DropView(_) => todo!(),
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
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
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
                        let ty = ValueType::<TypeVar>::try_from(&column.data_type)?;
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
                    Arc::new(Type::Table(TableType {
                        columns: column_decls,
                    })),
                )?;
                Ok((None, scope.into_handle()))
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
                scope.add(ident_from_table_name(table_name)?, ty.unwrap())?;
                Ok((None, scope.into_handle()))
            }
        }
    }
}

impl InferTypes for ast::DropTableStatement {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        let ast::DropTableStatement { table_name, .. } = self;
        let mut scope = Scope::new(scope);
        let table = ident_from_table_name(table_name)?;
        scope.hide(&table)?;
        Ok((None, scope.into_handle()))
    }
}

impl InferTypes for ast::QueryStatement {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        let ast::QueryStatement { query_expression } = self;
        query_expression.infer_types(scope)
    }
}

impl InferTypes for ast::QueryExpression {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        match self {
            ast::QueryExpression::SelectExpression(expr) => expr.infer_types(scope),
            ast::QueryExpression::Nested { query, .. } => query.infer_types(scope),
            ast::QueryExpression::With {
                ctes: _, query: _, ..
            } => todo!(),
            ast::QueryExpression::SetOperation { .. } => todo!(),
        }
    }
}

impl InferTypes for ast::SelectExpression {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
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
            //from_clause,
            //where_clause,
            //group_by,
            //having,
            //qualify,
            //order_by,
            //limit,
            ..
        } = self;

        let mut cols = vec![];
        for item in select_list.node_iter_mut() {
            match item {
                ast::SelectListItem::Expression {
                    expression,
                    alias: Some(ast::Alias { ident, .. }),
                } => {
                    // BigQuery does not allow select list items to see names
                    // bound by other select list items.
                    let (ty, _scope) = expression.infer_types(scope)?;
                    // TODO: Something has gone seriously wrong with the
                    // `InferType` API and the choice to wrap namespace entries
                    // in `Arc` here. This is ugly and we'll be doing it often.
                    let Type::Argument(ArgumentType::Value(ty)) = ty
                        .expect("expression should have a type")
                        .as_ref()
                        .to_owned()
                    else {
                        panic!("expression should have a value type");
                    };
                    cols.push(ColumnType {
                        name: ident.clone(),
                        ty,
                        not_null: false,
                    });
                }
                _ => todo!(),
            }
        }
        let table_type = Arc::new(Type::Table(TableType { columns: cols }));
        Ok((Some(table_type), scope.clone()))
    }
}

impl InferTypes for ast::Expression {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        match self {
            ast::Expression::BoolValue(_) => Ok((
                // TODO: This could be shorter.
                Some(Arc::new(Type::Argument(crate::types::ArgumentType::Value(
                    ValueType::Simple(crate::types::SimpleType::Bool),
                )))),
                scope.clone(),
            )),
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(scope),
            _ => todo!(),
        }
    }
}

impl InferTypes for LiteralValue {
    fn infer_types(
        &mut self,
        scope: &ScopeHandle,
    ) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
        let simple_ty = match self {
            LiteralValue::Int64(_) => SimpleType::Int64,
            LiteralValue::Float64(_) => SimpleType::Float64,
            LiteralValue::String(_) => SimpleType::String,
        };
        Ok((
            Some(Arc::new(Type::Argument(crate::types::ArgumentType::Value(
                ValueType::Simple(simple_ty),
            )))),
            scope.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use pretty_assertions::assert_eq;

    use crate::{
        ast::parse_sql,
        scope::{CaseInsensitiveIdent, Scope},
        tokenizer::Span,
        types::{tests::ty, Type, TypeVar},
    };

    use super::*;

    fn infer(sql: &str) -> Result<(Option<Arc<Type<TypeVar>>>, ScopeHandle)> {
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

    fn lookup(scope: &ScopeHandle, name: &str) -> Option<Arc<Type<TypeVar>>> {
        scope.get(&CaseInsensitiveIdent::new(name, Span::Unknown))
    }

    macro_rules! assert_defines {
        ($scope:expr, $name:expr, $ty:expr) => {
            assert_eq!(lookup(&$scope, $name), Some(Arc::new(ty($ty))))
        };
    }

    macro_rules! assert_not_defines {
        ($scope:expr, $name:expr) => {
            assert_eq!(lookup(&$scope, $name), None)
        };
    }

    #[test]
    fn root_scope_defines_functions() {
        let (_, scope) = infer("SELECT 1").unwrap();
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
}
