//! Our type inference subsystem.

use std::collections::HashSet;

use crate::{
    ast,
    errors::{Error, Result},
    scope::{CaseInsensitiveIdent, Scope, ScopeHandle},
    tokenizer::{Ident, Literal, LiteralValue, Spanned},
    types::{ArgumentType, ColumnType, SimpleType, TableType, Type, ValueType},
    unification::{UnificationTable, Unify},
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
                        Ok(col_ty)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let table_type = TableType {
                    columns: column_decls,
                }
                .name_anonymous_columns(table_name.span());
                table_type.expect_creatable(table_name)?;
                scope.add(ident_from_table_name(table_name)?, Type::Table(table_type))?;
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
                let ty = ty.name_anonymous_columns(table_name.span());
                ty.expect_creatable(table_name)?;
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
            ast::InsertedData::Select { query, .. } => {
                let (ty, _scope) = query.infer_types(scope)?;
                ty.expect_subtype_ignoring_nullability_of(table_type, query)?;
                Ok(((), scope.clone()))
            }
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
            ast::QueryExpression::SetOperation {
                left,
                set_operator,
                right,
            } => {
                let left_ty = left.infer_types(scope)?.0;
                let right_ty = right.infer_types(scope)?.0;
                let result_ty = left_ty.common_supertype(&right_ty).ok_or_else(|| {
                    Error::annotated(
                        format!("cannot combine {} and {}", left_ty, right_ty),
                        set_operator.span(),
                        "incompatible types",
                    )
                })?;
                Ok((result_ty, scope.clone()))
            }
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

        // See if we have a FROM clause.
        let mut from_type = None;
        let mut scope = scope.to_owned();
        if let Some(from_clause) = from_clause {
            let (new_from_type, new_scope) = from_clause.infer_types(&scope)?;
            from_type = Some(new_from_type);
            scope = new_scope;
        }

        // Helper function to add columns from a table type to a list of columns.
        let add_table_cols =
            |cols: &mut Vec<_>, table_type: &TableType, except: &Option<ast::Except>| {
                let except = except_set(except);
                for column in &table_type.columns {
                    if let Some(column_name) = &column.name {
                        let column_name = CaseInsensitiveIdent::from(column_name.clone());
                        if !except.contains(&column_name) {
                            cols.push(ColumnType {
                                name: column.name.clone(),
                                ty: column.ty.to_owned(),
                                not_null: false,
                            });
                        }
                    }
                }
            };

        // Iterate over the select list, adding columns to the scope.
        let mut cols = vec![];
        for item in select_list.node_iter_mut() {
            match item {
                ast::SelectListItem::Expression { expression, alias } => {
                    // BigQuery does not allow select list items to see names
                    // bound by other select list items.
                    let (ty, _scope) = expression.infer_types(&scope)?;
                    // Make sure any aggregates have been turned into values.
                    let ty = ty.expect_value_type(expression)?.to_owned();
                    let name = alias
                        .infer_column_name()
                        .or_else(|| expression.infer_column_name());
                    cols.push(ColumnType {
                        name,
                        ty: ArgumentType::Value(ty),
                        not_null: false,
                    });
                }
                ast::SelectListItem::Wildcard { star, except } => {
                    if let Some(from_type) = &from_type {
                        add_table_cols(&mut cols, from_type, except);
                    } else {
                        return Err(Error::annotated(
                            "cannot use * in SELECT without a FROM clause",
                            star.span(),
                            "no FROM clause",
                        ));
                    }
                }
                ast::SelectListItem::TableNameWildcard {
                    table_name, except, ..
                } => {
                    let table = ident_from_table_name(table_name)?;
                    let table_type = scope.get_or_err(&table)?.try_as_table_type(&table)?;
                    add_table_cols(&mut cols, table_type, except);
                }
            }
        }
        let table_type = TableType { columns: cols };
        Ok((table_type, scope.clone()))
    }
}

impl InferTypes for ast::FromClause {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let ast::FromClause {
            from_item,
            join_operations,
            ..
        } = self;
        let (table_type, scope) = from_item.infer_types(scope)?;
        if !join_operations.is_empty() {
            return Err(nyi(self, "join operations"));
        }
        Ok((table_type, scope))
    }
}

impl InferTypes for ast::FromItem {
    /// We return a table type for use by `SELECT *`.
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::FromItem::TableName { table_name, alias } => {
                let table = ident_from_table_name(table_name)?;
                let table_type = scope.get_or_err(&table)?.try_as_table_type(&table)?;
                let name = match alias {
                    Some(alias) => CaseInsensitiveIdent::from(alias.ident.clone()),
                    None => table,
                };

                let mut scope = Scope::new(scope);
                scope.add(name, Type::Table(table_type.clone()))?;
                for column in &table_type.columns {
                    if let Some(column_name) = &column.name {
                        scope.add(
                            column_name.clone().into(),
                            Type::Argument(column.ty.clone()),
                        )?;
                    }
                }
                Ok((table_type.clone(), scope.into_handle()))
            }
            ast::FromItem::Subquery { .. } => Err(nyi(self, "from subquery")),
            ast::FromItem::Unnest { .. } => Err(nyi(self, "from unnest")),
        }
    }
}

impl InferTypes for ast::Expression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(scope),
            ast::Expression::BoolValue(_) => Ok((ArgumentType::bool(), scope.clone())),
            ast::Expression::Null { .. } => Ok((ArgumentType::null(), scope.clone())),
            ast::Expression::ColumnName(ident) => ident.infer_types(scope),
            ast::Expression::TableAndColumnName(name) => name.infer_types(scope),
            ast::Expression::Cast(cast) => cast.infer_types(scope),
            ast::Expression::Is(is) => is.infer_types(scope),
            ast::Expression::In(in_expr) => in_expr.infer_types(scope),
            ast::Expression::Between(between) => between.infer_types(scope),
            ast::Expression::KeywordBinop(binop) => binop.infer_types(scope),
            ast::Expression::Not(not) => not.infer_types(scope),
            ast::Expression::If(if_expr) => if_expr.infer_types(scope),
            ast::Expression::Case(case) => case.infer_types(scope),
            ast::Expression::Binop(binop) => binop.infer_types(scope),
            ast::Expression::Array(array) => array.infer_types(scope),
            ast::Expression::FunctionCall(fcall) => fcall.infer_types(scope),
            ast::Expression::Index(index) => index.infer_types(scope),
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

impl InferTypes for ast::IsExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        // We need to do this manually because our second argument isn't an
        // expression.
        let func_name = &CaseInsensitiveIdent::new("%IS", self.is_token.span());
        let func_ty = scope
            .get_or_err(func_name)?
            .try_as_function_type(func_name)?;
        let arg_types = [
            self.left.infer_types(scope)?.0,
            self.predicate.infer_types(scope)?.0,
        ];
        let ret_ty = func_ty.return_type_for(&arg_types, func_name)?;
        Ok((ret_ty, scope.clone()))
    }
}

impl InferTypes for ast::IsExpressionPredicate {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        // Return either BOOL or NULL type, depending on the predicate. The
        // types for the %IS primitive will use this to verify that the left
        // argument and predicate are compatible.
        match self {
            ast::IsExpressionPredicate::Null(_) | ast::IsExpressionPredicate::Unknown(_) => {
                Ok((ArgumentType::null(), scope.clone()))
            }
            ast::IsExpressionPredicate::True(_) | ast::IsExpressionPredicate::False(_) => {
                Ok((ArgumentType::bool(), scope.clone()))
            }
        }
    }
}

impl InferTypes for ast::InExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        // We need to do this manually because our second argument isn't an
        // expression.
        let func_name = &CaseInsensitiveIdent::new("%IN", self.in_token.span());
        let func_ty = scope
            .get_or_err(func_name)?
            .try_as_function_type(func_name)?;
        let left_ty = self.left.infer_types(scope)?.0;
        let value_set_ty = self.value_set.infer_types(scope)?.0;
        let elem_ty = value_set_ty.expect_one_column(&self.value_set)?.ty.clone();
        let ret_ty = func_ty.return_type_for(&[left_ty, elem_ty], func_name)?;
        Ok((ret_ty, scope.clone()))
    }
}

impl InferTypes for ast::InValueSet {
    type Type = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::InValueSet::QueryExpression { query, .. } => query.infer_types(scope),
            ast::InValueSet::ExpressionList {
                paren1,
                expressions,
                ..
            } => {
                // Create a 1-column table type.
                let mut table = UnificationTable::default();
                let col_ty = table.type_var("T", paren1)?;
                for e in expressions.node_iter_mut() {
                    col_ty.unify(&e.infer_types(scope)?.0, &mut table, e)?;
                }
                let table_type = TableType {
                    columns: vec![ColumnType {
                        name: None,
                        ty: col_ty.resolve(&table, self)?,
                        not_null: false,
                    }],
                };
                Ok((table_type, scope.clone()))
            }
            ast::InValueSet::Unnest { expression, .. } => {
                let array_ty = expression.infer_types(scope)?.0;
                let array_ty = array_ty.expect_array_type(expression)?;
                let table_ty = array_ty.unnest(expression)?;
                Ok((table_ty, scope.clone()))
            }
        }
    }
}

impl InferTypes for ast::BetweenExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let func_name = &CaseInsensitiveIdent::new("%BETWEEN", self.between_token.span());
        let args = [
            self.left.as_mut(),
            self.middle.as_mut(),
            self.right.as_mut(),
        ];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::KeywordBinopExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let func_name = &CaseInsensitiveIdent::new(
            &format!("%{}", self.op_keyword.ident.token.as_str()),
            self.op_keyword.span(),
        );
        let args = [self.left.as_mut(), self.right.as_mut()];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::NotExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let func_name = &CaseInsensitiveIdent::new("%NOT", self.not_token.span());
        let args = [self.expression.as_mut()];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::IfExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let args = [
            self.condition.as_mut(),
            self.then_expression.as_mut(),
            self.else_expression.as_mut(),
        ];
        infer_call(
            &CaseInsensitiveIdent::new("%IF", self.if_token.span()),
            args,
            scope,
        )
    }
}

impl InferTypes for ast::CaseExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        // CASE is basically two different constructs, depending on whether
        // there is a CASE expression or not. We handle them separately.
        let mut table = UnificationTable::default();
        if let Some(case_expr) = &mut self.case_expr {
            let match_tv = table.type_var("M", &self.case_token)?;
            match_tv.unify(&case_expr.infer_types(scope)?.0, &mut table, case_expr)?;
            let result_tv = table.type_var("R", &self.case_token)?;
            for c in &mut self.when_clauses {
                match_tv.unify(&c.condition.infer_types(scope)?.0, &mut table, &c.condition)?;
                result_tv.unify(&c.result.infer_types(scope)?.0, &mut table, &c.result)?;
            }
            if let Some(else_clause) = &mut self.else_clause {
                let else_expr = &mut else_clause.result;
                result_tv.unify(&else_expr.infer_types(scope)?.0, &mut table, else_expr)?;
            } else {
                result_tv.unify(&ArgumentType::null(), &mut table, self)?;
            }
            Ok((match_tv.resolve(&table, self)?, scope.clone()))
        } else {
            let bool_ty = ArgumentType::bool();
            let result_tv = table.type_var("R", &self.case_token)?;
            for c in &mut self.when_clauses {
                c.condition
                    .infer_types(scope)?
                    .0
                    .expect_subtype_of(&bool_ty, c)?;
                result_tv.unify(&c.result.infer_types(scope)?.0, &mut table, &c.result)?;
            }
            if let Some(else_clause) = &mut self.else_clause {
                let else_expr = &mut else_clause.result;
                result_tv.unify(&else_expr.infer_types(scope)?.0, &mut table, else_expr)?;
            } else {
                result_tv.unify(&ArgumentType::null(), &mut table, self)?;
            }
            Ok((result_tv.resolve(&table, self)?, scope.clone()))
        }
    }
}

impl InferTypes for ast::BinopExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let prim_name = CaseInsensitiveIdent::new(
            &format!("%{}", self.op_token.token.as_str()),
            self.op_token.span(),
        );
        infer_call(&prim_name, [self.left.as_mut(), self.right.as_mut()], scope)
    }
}

impl InferTypes for ast::ArrayExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let return_ty = self.definition.infer_types(scope)?.0;
        let return_ty = if let Some(element_ty) = &self.element_type {
            let element_ty = ArgumentType::Value(ValueType::try_from(&element_ty.elem_type)?);
            return_ty.expect_subtype_of(&element_ty, self)?;
            element_ty
        } else {
            return_ty
        };
        let return_ty = return_ty.expect_simple_type(self)?;
        Ok((
            ArgumentType::Value(ValueType::Array(return_ty.clone())),
            scope.clone(),
        ))
    }
}

impl InferTypes for ast::ArrayDefinition {
    /// The **element type** of the array.
    type Type = ArgumentType;

    /// Infer the **element type** of the array.
    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        match self {
            ast::ArrayDefinition::Query(query) => {
                let table_ty = query.infer_types(scope)?.0;
                let elem_ty = table_ty.expect_one_column(query)?.ty.clone();
                Ok((elem_ty, scope.clone()))
            }
            ast::ArrayDefinition::Elements(exprs) => {
                // We can use infer_call if we're careful.
                let span = exprs.items.span();
                let func_name = &CaseInsensitiveIdent::new("%ARRAY", span);
                let (elem_ty, _) = infer_call(func_name, exprs.node_iter_mut(), scope)?;
                let elem_ty = elem_ty.expect_array_type_returning_elem_type(self)?;
                let elem_ty = ArgumentType::Value(ValueType::Simple(elem_ty.clone()));
                Ok((elem_ty, scope.clone()))
            }
        }
    }
}

impl InferTypes for ast::FunctionCall {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let name = ident_from_function_name(&self.name)?;
        if self.over_clause.is_some() {
            return Err(nyi(&self.over_clause, "over clause"));
        }
        infer_call(&name, self.args.node_iter_mut(), scope)
    }
}

impl InferTypes for ast::IndexExpression {
    type Type = ArgumentType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<(Self::Type, ScopeHandle)> {
        let func_name = &CaseInsensitiveIdent::new("%[]", self.bracket1.span());
        let index_expr = match &mut self.index {
            ast::IndexOffset::Simple(expression)
            | ast::IndexOffset::Offset { expression, .. }
            | ast::IndexOffset::Ordinal { expression, .. } => expression,
        };
        let args = [self.expression.as_mut(), index_expr];
        infer_call(func_name, args, scope)
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

/// Build a set from an optional [`ast::Except`] clause.
fn except_set(except: &Option<ast::Except>) -> HashSet<CaseInsensitiveIdent> {
    let mut set = HashSet::new();
    if let Some(except) = except {
        for ident in except.columns.node_iter() {
            set.insert(ident.clone().into());
        }
    }
    set
}

/// Infer types a function-like expression (including primitives).
fn infer_call<'args, ArgExprs>(
    func_name: &CaseInsensitiveIdent,
    args: ArgExprs,
    scope: &ScopeHandle,
) -> Result<(ArgumentType, ScopeHandle)>
where
    ArgExprs: IntoIterator<Item = &'args mut ast::Expression>,
{
    let func_ty = scope
        .get_or_err(func_name)?
        .try_as_function_type(func_name)?;
    let mut arg_types = vec![];
    for arg in args {
        arg_types.push(arg.infer_types(scope)?.0);
    }
    let ret_ty = func_ty.return_type_for(&arg_types, func_name)?;
    Ok((ret_ty, scope.clone()))
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
