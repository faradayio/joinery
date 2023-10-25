//! Our type inference subsystem.

// This is work in progress.
#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    ast,
    errors::Result,
    scope::{Scope, ScopeHandle},
    types::{ColumnType, TableType, Type, TypeVar, ValueType},
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
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<ScopeHandle>;
}

impl InferTypes for ast::SqlProgram {
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<ScopeHandle> {
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
            scope = statement.infer_types(&scope)?;
        }

        Ok(scope)
    }
}

impl InferTypes for ast::Statement {
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<ScopeHandle> {
        match self {
            // TODO: This can't bind anything into our scope, but we should
            // check types anyway.
            ast::Statement::Query(_) => Ok(scope.clone()),
            ast::Statement::DeleteFrom(_) => todo!(),
            ast::Statement::InsertInto(_) => todo!(),
            ast::Statement::CreateTable(stmt) => stmt.infer_types(scope),
            ast::Statement::CreateView(_) => todo!(),
            ast::Statement::DropTable(stmt) => stmt.infer_types(scope),
            ast::Statement::DropView(_) => todo!(),
        }
    }
}

impl InferTypes for ast::CreateTableStatement {
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<ScopeHandle> {
        match self {
            ast::CreateTableStatement {
                // TODO: Allow dotted names in scopes.
                table_name: ast::TableName::Table { table, .. },
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
                    table.clone().into(),
                    Arc::new(Type::Table(TableType {
                        columns: column_decls,
                    })),
                )?;
                Ok(scope.into_handle())
            }
            _ => Ok(scope.clone()),
        }
    }
}

impl InferTypes for ast::DropTableStatement {
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<ScopeHandle> {
        match self {
            ast::DropTableStatement {
                // TODO: Allow dotted names in scopes.
                table_name: ast::TableName::Table { table, .. },
                ..
            } => {
                let mut scope = Scope::new(scope);
                scope.hide(&table.clone().into())?;
                Ok(scope.into_handle())
            }
            _ => Ok(scope.clone()),
        }
    }
}

impl InferTypes for ast::SelectExpression {
    fn infer_types(&mut self, _scope: &ScopeHandle) -> Result<ScopeHandle> {
        // let SelectExpression {
        //     select_options,
        //     select_list,
        //     from_clause,
        //     where_clause,
        //     group_by,
        //     having,
        //     qualify,
        //     order_by,
        //     limit,
        // } = self;

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

        unimplemented!("infer_types")
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

    fn infer(sql: &str) -> Result<ScopeHandle> {
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
        let scope = infer("SELECT 1").unwrap();
        assert_defines!(scope, "LOWER", "Fn(STRING) -> STRING");
        assert_defines!(scope, "lower", "Fn(STRING) -> STRING");
        assert_not_defines!(scope, "NO_SUCH_FUNCTION");
    }

    #[test]
    fn create_table_adds_table_to_scope() {
        let scope = infer("CREATE TABLE foo (x INT64, y STRING)").unwrap();
        assert_defines!(scope, "foo", "TABLE<x INT64, y STRING>");
    }

    #[test]
    fn drop_table_removes_table_from_scope() {
        let scope = infer("CREATE TABLE foo (x INT64, y STRING); DROP TABLE foo").unwrap();
        assert_not_defines!(scope, "foo");
    }
}
