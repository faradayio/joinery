//! Our type inference subsystem.

// This is work in progress.
#![allow(dead_code)]

use crate::{
    ast,
    errors::{Error, Result},
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
                let (ty, scope) = stmt.infer_types(scope)?;
                Ok((Some(ty), scope))
            }
            ast::Statement::DeleteFrom(_) => Err(nyi(self, "DELETE FROM")),
            ast::Statement::InsertInto(stmt) => {
                let ((), scope) = stmt.infer_types(scope)?;
                Ok((None, scope))
            }
            ast::Statement::CreateTable(stmt) => {
                let ((), scope) = stmt.infer_types(scope)?;
                Ok((None, scope))
            }
            ast::Statement::CreateView(_) => Err(nyi(self, "CREATE VIEW")),
            ast::Statement::DropTable(stmt) => {
                let ((), scope) = stmt.infer_types(scope)?;
                Ok((None, scope))
            }
            ast::Statement::DropView(_) => Err(nyi(self, "DROP VIEW")),
        }
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
                        let col_ty = ColumnType {
                            name: Some(column.name.clone()),
                            ty: ArgumentType::Value(ty),
                            // TODO: We don't support this in the main grammar yet.
                            not_null: false,
                        };
                        col_ty.expect_creatable(column)?;
                        Ok(col_ty)
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
                for col_ty in &ty.columns {
                    col_ty.expect_creatable(query_statement)?;
                }
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

impl InferTypes for ast::InsertIntoStatement {
    type Type = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::InsertIntoStatement {
            table_name,
            inserted_data,
            ..
        } = self;
        let table = ident_from_table_name(table_name)?;
        let table_type = scope.get_or_err(&table)?.try_as_table_type(&table)?;

        match inserted_data {
            ast::InsertedData::Values { rows, .. } => {
                for row in rows.node_iter_mut() {
                    let (ty, _scope) = row.infer_types(scope)?;
                    ty.expect_subtype_ignoring_nullability_of(table_type, row)?;
                }
                Ok(((), scope.clone()))
            }
            ast::InsertedData::Select { query, .. } => Err(nyi(query, "INSERT INTO .. AS")),
        }
    }
}

impl InferTypes for ast::Row {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::Row { expressions, .. } = self;
        let mut cols = vec![];
        for expr in expressions.node_iter_mut() {
            let (ty, _scope) = expr.infer_types(scope)?;
            let ty = ty.expect_value_type(expr)?.to_owned();
            cols.push(ColumnType {
                name: None,
                ty: ArgumentType::Value(ty),
                not_null: false,
            });
        }
        Ok((TableType { columns: cols }, scope.clone()))
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
            ast::QueryExpression::SetOperation { .. } => Err(nyi(self, "set operation")),
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
            select_list: ast::SelectList {
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
                    //
                    // TODO: Delay assigning anonymous names until we actually
                    // create a table?
                    let (ty, _scope) = expression.infer_types(&scope)?;
                    // Make sure any aggregates have been turned into values.
                    let ty = ty.expect_value_type(expression)?.to_owned();
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
                        name: Some(name),
                        ty: ArgumentType::Value(ty),
                        not_null: false,
                    });
                }
                _ => return Err(nyi(item, "select list item")),
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
            return Err(nyi(self, "join operations"));
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
                let table_type = scope.get_or_err(&table)?.try_as_table_type(&table)?;

                if alias.is_some() {
                    return Err(nyi(alias, "from with alias"));
                }

                let mut scope = Scope::new(scope);
                for column in &table_type.columns {
                    if let Some(column_name) = &column.name {
                        scope.add(
                            column_name.clone().into(),
                            Type::Argument(column.ty.clone()),
                        )?;
                    }
                }
                Ok(((), scope.into_handle()))
            }
            ast::FromItem::Subquery { .. } => Err(nyi(self, "from subquery")),
            ast::FromItem::Unnest { .. } => Err(nyi(self, "from unnest")),
        }
    }
}

impl InferTypes for ast::Expression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let arg = ArgumentType::Value;
        let arg_simple = |ty| arg(ValueType::Simple(ty));
        match self {
            ast::Expression::BoolValue(_) => Ok((arg_simple(SimpleType::Bool), scope.clone())),
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(scope),
            ast::Expression::Null { .. } => Ok((arg_simple(SimpleType::Null), scope.clone())),
            ast::Expression::ColumnName(ident) => ident.infer_types(scope),
            ast::Expression::TableAndColumnName(name) => name.infer_types(scope),
            ast::Expression::Cast(cast) => cast.infer_types(scope),
            ast::Expression::FunctionCall(fcall) => fcall.infer_types(scope),
            _ => Err(nyi(self, "expression")),
        }
    }
}

impl InferTypes for LiteralValue {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let simple_ty = match self {
            LiteralValue::Int64(_) => SimpleType::Int64,
            LiteralValue::Float64(_) => SimpleType::Float64,
            LiteralValue::String(_) => SimpleType::String,
        };
        Ok((
            ArgumentType::Value(ValueType::Simple(simple_ty)),
            scope.clone(),
        ))
    }
}

impl InferTypes for Ident {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ident = self.to_owned().into();
        let ty = scope.get_or_err(&ident)?.try_as_argument_type(&ident)?;
        Ok((ty.to_owned(), scope.clone()))
    }
}

impl InferTypes for ast::TableAndColumnName {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::TableAndColumnName {
            table_name,
            column_name,
            ..
        } = self;

        let table = ident_from_table_name(table_name)?;
        let table_type = scope.get_or_err(&table)?.try_as_table_type(&table)?;
        let column_type = table_type.column_by_name_or_err(column_name)?;
        // TODO: Actually, we ought to be able store `ArgumentType` in a
        // column. See the test case `scoped_aggregates.sql`.
        Ok((column_type.ty.to_owned(), scope.clone()))
    }
}

impl InferTypes for ast::Cast {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::Cast {
            expression,
            data_type,
            ..
        } = self;
        // TODO: Pass through aggregate status.
        expression.infer_types(scope)?;
        let ty = ValueType::try_from(&*data_type)?;
        Ok((ArgumentType::Value(ty), scope.clone()))
    }
}

impl InferTypes for ast::FunctionCall {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::FunctionCall {
            name,
            args,
            over_clause,
            ..
        } = self;

        let name = ident_from_function_name(name)?;
        let func_ty = scope.get_or_err(&name)?.try_as_function_type(&name)?;

        if over_clause.is_some() {
            return Err(nyi(over_clause, "over clause"));
        }

        let mut arg_types = vec![];
        for arg in args.node_iter_mut() {
            let (ty, _scope) = arg.infer_types(scope)?;
            arg_types.push(ty);
        }
        let ret_ty = func_ty.return_type_for(&arg_types, &name)?;
        Ok((ret_ty, scope.clone()))
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

/// Convert a table name to an identifier.
fn ident_from_table_name(table_name: &ast::TableName) -> Result<CaseInsensitiveIdent> {
    match table_name {
        ast::TableName::Table { table, .. } => Ok(table.clone().into()),
        _ => Err(nyi(table_name, "dotted name")),
    }
}

/// Convert a function name to an identifier.
fn ident_from_function_name(function_name: &ast::FunctionName) -> Result<CaseInsensitiveIdent> {
    match function_name {
        ast::FunctionName::Function { function, .. } => Ok(function.clone().into()),
        _ => Err(nyi(function_name, "dotted name")),
    }
}

/// Print a pretty error message when we haven't implemented something.
fn nyi(spanned: &dyn Spanned, name: &str) -> Error {
    Error::annotated(
        "could not infer types",
        spanned.span(),
        format!("not yet implemented: {}", name),
    )
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{
        ast::parse_sql,
        known_files::KnownFiles,
        scope::{CaseInsensitiveIdent, Scope, ScopeValue},
        tokenizer::Span,
        types::tests::ty,
    };

    use super::*;

    fn infer(sql: &str) -> Result<(KnownFiles, Option<TableType>, ScopeHandle)> {
        let mut files = KnownFiles::new();
        let file_id = files.add_string("test.sql", sql);
        let mut program = match parse_sql(&files, file_id) {
            Ok(program) => program,
            Err(e) => {
                e.emit(&files);
                panic!("parse error");
            }
        };
        let scope = Scope::root();
        match program.infer_types(&scope) {
            Ok((ty, scope)) => Ok((files, ty, scope)),
            Err(e) => {
                e.emit(&files);
                panic!("type inference error");
            }
        }
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
        let (_, _, scope) = infer("SELECT 1 AS x").unwrap();
        assert_defines!(scope, "LOWER", "Fn(STRING) -> STRING");
        assert_defines!(scope, "lower", "Fn(STRING) -> STRING");
        assert_not_defines!(scope, "NO_SUCH_FUNCTION");
    }

    #[test]
    fn create_table_adds_table_to_scope() {
        let (_, _, scope) = infer("CREATE TABLE foo (x INT64, y STRING)").unwrap();
        assert_defines!(scope, "foo", "TABLE<x INT64, y STRING>");
    }

    #[test]
    fn drop_table_removes_table_from_scope() {
        let (_, _, scope) = infer("CREATE TABLE foo (x INT64, y STRING); DROP TABLE foo").unwrap();
        assert_not_defines!(scope, "foo");
    }

    #[test]
    fn create_table_as_infers_column_types() {
        let (_, _, scope) = infer("CREATE TABLE foo AS SELECT 'a' AS x, TRUE AS y").unwrap();
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
        let (_, _, scope) = infer(sql).unwrap();
        assert_defines!(scope, "foo", "TABLE<x STRING>");
    }

    #[test]
    fn anon_and_aliased_columns() {
        let (_, _, scope) = infer("CREATE TABLE foo AS SELECT 1, 2 AS x, 3").unwrap();
        assert_defines!(scope, "foo", "TABLE<_f0 INT64, x INT64, _f1 INT64>");
    }

    #[test]
    fn table_dot_column() {
        let sql = "
CREATE TABLE foo AS
WITH t AS (SELECT 'a' AS x)
SELECT t.x FROM t";
        let (_, _, scope) = infer(sql).unwrap();
        assert_defines!(scope, "foo", "TABLE<x STRING>");
    }

    //     #[test]
    //     fn table_dot_star() {
    //         let sql = "
    // CREATE TABLE foo AS
    // WITH t AS (SELECT 'a' AS x)
    // SELECT t.* FROM t";
    //         let (_, _, scope) = infer(sql).unwrap();
    //         assert_defines!(scope, "foo", "TABLE<x STRING>");
    //     }

    #[test]
    fn insert_into() {
        let sql = "
CREATE TABLE foo (x FLOAT64);
INSERT INTO foo VALUES (3.0), (2), (NULL)";
        let (_, _, scope) = infer(sql).unwrap();
        assert_defines!(scope, "foo", "TABLE<x FLOAT64>");
    }

    #[test]
    fn function_call_with_type_vars() {
        let sql = "SELECT GREATEST(1, NULL, 3.0) AS x";
        let (_, rty, _) = infer(sql).unwrap();
        assert_eq!(Type::Table(rty.unwrap()), ty("TABLE<x FLOAT64>"));
    }
}
