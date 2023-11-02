//! Our type inference subsystem.

use std::collections::HashSet;

use crate::{
    ast::{self, ConditionJoinOperator, Name},
    errors::{Error, Result},
    scope::{ColumnSet, ColumnSetScope, Scope, ScopeGet, ScopeHandle},
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
    type Scope;

    /// The result of type inference. For expressions, this will be a type.
    /// For top-level statements, this will be a new scope.
    type Output;

    /// Infer types for this value. Returns the scope after inference in case
    /// the caller needs to use it for a later sibling node.
    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output>;
}

impl InferTypes for ast::SqlProgram {
    type Scope = ScopeHandle;
    type Output = (Option<TableType>, ScopeHandle);

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
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
    type Scope = ScopeHandle;
    type Output = (Option<TableType>, ScopeHandle);

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        match self {
            ast::Statement::Query(stmt) => Ok((Some(stmt.infer_types(scope)?), scope.clone())),
            ast::Statement::DeleteFrom(_) => Err(nyi(self, "DELETE FROM")),
            ast::Statement::InsertInto(stmt) => {
                stmt.infer_types(scope)?;
                Ok((None, scope.clone()))
            }
            ast::Statement::CreateTable(stmt) => Ok((None, stmt.infer_types(scope)?)),
            ast::Statement::CreateView(_) => Err(nyi(self, "CREATE VIEW")),
            ast::Statement::DropTable(stmt) => Ok((None, stmt.infer_types(scope)?)),
            ast::Statement::DropView(_) => Err(nyi(self, "DROP VIEW")),
        }
    }
}

impl InferTypes for ast::CreateTableStatement {
    type Scope = ScopeHandle;
    type Output = ScopeHandle;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
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
                scope.add(table_name.clone(), Type::Table(table_type))?;
                Ok(scope.into_handle())
            }
            ast::CreateTableStatement {
                table_name,
                definition:
                    ast::CreateTableDefinition::As {
                        query_statement, ..
                    },
                ..
            } => {
                let ty = query_statement.infer_types(scope)?;
                let ty = ty.name_anonymous_columns(table_name.span());
                ty.expect_creatable(table_name)?;
                let mut scope = Scope::new(scope);
                scope.add(table_name.clone(), Type::Table(ty))?;
                Ok(scope.into_handle())
            }
        }
    }
}

impl InferTypes for ast::DropTableStatement {
    type Scope = ScopeHandle;
    type Output = ScopeHandle;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        let mut scope = Scope::new(scope);
        scope.hide(&self.table_name)?;
        Ok(scope.into_handle())
    }
}

impl InferTypes for ast::InsertIntoStatement {
    type Scope = ScopeHandle;
    type Output = ();

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        let table_type = scope.get_table_type(&self.table_name)?;

        match &mut self.inserted_data {
            ast::InsertedData::Values { rows, .. } => {
                for row in rows.node_iter_mut() {
                    let ty = row.infer_types(scope)?;
                    ty.expect_subtype_ignoring_nullability_of(&table_type, row)?;
                }
                Ok(())
            }
            ast::InsertedData::Select { query, .. } => {
                let ty = query.infer_types(scope)?;
                ty.expect_subtype_ignoring_nullability_of(&table_type, query)?;
                Ok(())
            }
        }
    }
}

impl InferTypes for ast::ValuesRow {
    type Scope = ScopeHandle;
    type Output = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        let mut cols = vec![];
        for expr in self.expressions.node_iter_mut() {
            // We have an expression outside of a SELECT, so we need a
            // ColumnSetScope but without any columns. So aggregates and window
            // functions can't be used here.
            let scope = ColumnSetScope::new_empty(scope);
            let ty = expr.infer_types(&scope)?;
            let ty = ty.expect_value_type(expr)?.to_owned();
            cols.push(ColumnType {
                name: None,
                ty: ArgumentType::Value(ty),
                not_null: false,
            });
        }
        Ok(TableType { columns: cols })
    }
}

impl InferTypes for ast::QueryStatement {
    type Scope = ScopeHandle;
    type Output = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        self.query_expression.infer_types(scope)
    }
}

impl InferTypes for ast::QueryExpression {
    type Scope = ScopeHandle;
    type Output = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        match self {
            ast::QueryExpression::SelectExpression(expr) => expr.infer_types(scope),
            ast::QueryExpression::Nested { query, .. } => query.infer_types(scope),
            ast::QueryExpression::With { ctes, query, .. } => {
                // Non-recursive CTEs, so each will create a new namespace.
                let mut scope = scope.to_owned();
                for cte in ctes.node_iter_mut() {
                    scope = cte.infer_types(&scope)?;
                }
                query.infer_types(&scope)
            }
            ast::QueryExpression::SetOperation {
                left,
                set_operator,
                right,
            } => {
                let left_ty = left.infer_types(scope)?;
                let right_ty = right.infer_types(scope)?;
                let result_ty = left_ty.common_supertype(&right_ty).ok_or_else(|| {
                    Error::annotated(
                        format!("cannot combine {} and {}", left_ty, right_ty),
                        set_operator.span(),
                        "incompatible types",
                    )
                })?;
                Ok(result_ty)
            }
        }
    }
}

impl InferTypes for ast::CommonTableExpression {
    type Scope = ScopeHandle;
    type Output = ScopeHandle;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        let table_ty = self.query.infer_types(scope)?;
        let mut scope = Scope::new(scope);
        scope.add(self.name.to_owned().into(), Type::Table(table_ty))?;
        Ok(scope.into_handle())
    }
}

impl InferTypes for ast::SelectExpression {
    type Scope = ScopeHandle;
    type Output = TableType;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
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
        let column_set_scope = if let Some(from_clause) = from_clause {
            from_clause.infer_types(scope)
        } else {
            Ok(ColumnSetScope::new_empty(scope))
        }?;

        // Helper function to add columns from a table type to a list of columns.
        let add_table_cols =
            |cols: &mut Vec<_>, table_type: &TableType, except: &Option<ast::Except>| {
                let except = except_set(except);
                for column in &table_type.columns {
                    if let Some(column_name) = &column.name {
                        let name = Name::from(column_name.clone());
                        if !except.contains(&name) {
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
                    let ty = expression.infer_types(&column_set_scope)?;
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
                    let table_type = column_set_scope.column_set().star(star)?;
                    add_table_cols(&mut cols, &table_type, except);
                }
                ast::SelectListItem::TableNameWildcard {
                    table_name,
                    star,
                    except,
                    ..
                } => {
                    let table_type = column_set_scope.column_set().star_for(table_name, star)?;
                    add_table_cols(&mut cols, &table_type, except);
                }
            }
        }
        Ok(TableType { columns: cols })
    }
}

impl InferTypes for ast::FromClause {
    type Scope = ScopeHandle;
    type Output = ColumnSetScope;

    fn infer_types(&mut self, outer_scope: &Self::Scope) -> Result<Self::Output> {
        let mut scope = ColumnSetScope::new_empty(outer_scope);
        scope = self.from_item.infer_types(&(outer_scope.clone(), scope))?;
        for op in &mut self.join_operations {
            scope = op.infer_types(&(outer_scope.clone(), scope))?;
        }
        Ok(scope)
    }
}

impl InferTypes for ast::FromItem {
    type Scope = (ScopeHandle, ColumnSetScope);
    type Output = ColumnSetScope;

    fn infer_types(&mut self, (outer_scope, scope): &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::FromItem::TableName { table_name, alias } => {
                let table_type = outer_scope.get_table_type(table_name)?;
                let name = match alias {
                    Some(alias) => alias.ident.clone().into(),
                    None => table_name.clone(),
                };

                let column_set = ColumnSet::from_table(Some(name), table_type);
                Ok(ColumnSetScope::new(outer_scope, column_set))
            }
            ast::FromItem::Subquery { query, alias, .. } => {
                let table_type = query.infer_types(outer_scope)?;
                let name = alias.clone().map(|alias| alias.ident.into());
                let column_set = ColumnSet::from_table(name, table_type);
                Ok(ColumnSetScope::new(outer_scope, column_set))
            }
            ast::FromItem::Unnest {
                expression, alias, ..
            } => {
                let array_ty = expression.infer_types(scope)?;
                let array_ty = array_ty.expect_array_type(expression)?;
                let mut table_ty = array_ty.unnest(expression)?;
                if array_ty.unnests_to_anonymous_column() {
                    // If we have an alias, use it as our single column's name. Leave the table anonymous.
                    table_ty.columns[0].name = alias.as_ref().map(|alias| alias.ident.clone());
                    let column_set = ColumnSet::from_table(None, table_ty);
                    Ok(ColumnSetScope::new(outer_scope, column_set))
                } else {
                    // We have a multi-column output table. Use the alias as the table name.
                    let name = alias.clone().map(|alias| alias.ident.into());
                    let column_set = ColumnSet::from_table(name, table_ty);
                    Ok(ColumnSetScope::new(outer_scope, column_set))
                }
            }
        }
    }
}

impl InferTypes for ast::JoinOperation {
    type Scope = (ScopeHandle, ColumnSetScope);
    type Output = ColumnSetScope;

    fn infer_types(&mut self, scopes @ (_, scope): &Self::Scope) -> Result<Self::Output> {
        let from_type = self.from_item_mut().infer_types(scopes)?;
        scope.clone().try_transform(|column_set| match self {
            ast::JoinOperation::ConditionJoin {
                operator: Some(ConditionJoinOperator::Using { column_names, .. }, ..),
                ..
            } => {
                let column_names = column_names
                    .node_iter()
                    .map(|ident| ident.clone().into())
                    .collect::<Vec<_>>();
                column_set.join_using(from_type.column_set(), &column_names)
            }
            ast::JoinOperation::ConditionJoin {
                operator: Some(ConditionJoinOperator::On { expression, .. }, ..),
                ..
            } => {
                let expr_ty = expression.infer_types(scope)?;
                expr_ty.expect_subtype_of(&ArgumentType::bool(), expression)?;
                Ok(column_set.join(from_type.column_set()))
            }
            _ => Ok(column_set.join(from_type.column_set())),
        })
    }
}

impl InferTypes for ast::Expression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(scope),
            ast::Expression::BoolValue(_) => Ok(ArgumentType::bool()),
            ast::Expression::Null { .. } => Ok(ArgumentType::null()),
            ast::Expression::ColumnName(name) => name.infer_types(scope),
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
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, _scope: &Self::Scope) -> Result<Self::Output> {
        let simple_ty = match self {
            LiteralValue::Int64(_) => SimpleType::Int64,
            LiteralValue::Float64(_) => SimpleType::Float64,
            LiteralValue::String(_) => SimpleType::String,
        };
        Ok(ArgumentType::Value(ValueType::Simple(simple_ty)))
    }
}

impl InferTypes for Ident {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ident = self.to_owned().into();
        scope.get_argument_type(&ident)
    }
}

impl InferTypes for ast::Name {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let (table_name, column_name) = self.split_table_and_column();
        if let Some(table_name) = table_name {
            let table_type = scope.get_table_type(&table_name)?;
            let column_type = table_type.column_by_name_or_err(&column_name)?;
            Ok(column_type.ty.to_owned())
        } else {
            scope.get_argument_type(self)
        }
    }
}

impl InferTypes for ast::Cast {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ast::Cast {
            expression,
            data_type,
            ..
        } = self;
        // TODO: Pass through aggregate status.
        expression.infer_types(scope)?;
        let ty = ValueType::try_from(&*data_type)?;
        Ok(ArgumentType::Value(ty))
    }
}

impl InferTypes for ast::IsExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // We need to do this manually because our second argument isn't an
        // expression.
        let func_name = &Name::new("%IS", self.is_token.span());
        let func_ty = scope.get_function_type(func_name)?;
        let arg_types = [
            self.left.infer_types(scope)?,
            self.predicate.infer_types(scope)?,
        ];
        func_ty.return_type_for(&arg_types, func_name)
    }
}

impl InferTypes for ast::IsExpressionPredicate {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, _scope: &Self::Scope) -> Result<Self::Output> {
        // Return either BOOL or NULL type, depending on the predicate. The
        // types for the %IS primitive will use this to verify that the left
        // argument and predicate are compatible.
        match self {
            ast::IsExpressionPredicate::Null(_) | ast::IsExpressionPredicate::Unknown(_) => {
                Ok(ArgumentType::null())
            }
            ast::IsExpressionPredicate::True(_) | ast::IsExpressionPredicate::False(_) => {
                Ok(ArgumentType::bool())
            }
        }
    }
}

impl InferTypes for ast::InExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // We need to do this manually because our second argument isn't an
        // expression.
        let func_name = &Name::new("%IN", self.in_token.span());
        let func_ty = scope.get_function_type(func_name)?;
        let left_ty = self.left.infer_types(scope)?;
        let value_set_ty = self.value_set.infer_types(scope)?;
        let elem_ty = value_set_ty.expect_one_column(&self.value_set)?.ty.clone();
        func_ty.return_type_for(&[left_ty, elem_ty], func_name)
    }
}

impl InferTypes for ast::InValueSet {
    type Scope = ColumnSetScope;
    type Output = TableType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::InValueSet::QueryExpression { query, .. } => {
                query.infer_types(&scope.clone().try_into_handle_for_subquery()?)
            }
            ast::InValueSet::ExpressionList {
                paren1,
                expressions,
                ..
            } => {
                // Create a 1-column table type.
                let mut table = UnificationTable::default();
                let col_ty = table.type_var("T", paren1)?;
                for e in expressions.node_iter_mut() {
                    col_ty.unify(&e.infer_types(scope)?, &mut table, e)?;
                }
                let table_type = TableType {
                    columns: vec![ColumnType {
                        name: None,
                        ty: col_ty.resolve(&table, self)?,
                        not_null: false,
                    }],
                };
                Ok(table_type)
            }
            ast::InValueSet::Unnest { expression, .. } => {
                let array_ty = expression.infer_types(scope)?;
                let array_ty = array_ty.expect_array_type(expression)?;
                let table_ty = array_ty.unnest(expression)?;
                Ok(table_ty)
            }
        }
    }
}

impl InferTypes for ast::BetweenExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new("%BETWEEN", self.between_token.span());
        let args = [
            self.left.as_mut(),
            self.middle.as_mut(),
            self.right.as_mut(),
        ];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::KeywordBinopExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new(
            &format!("%{}", self.op_keyword.ident.token.as_str()),
            self.op_keyword.span(),
        );
        let args = [self.left.as_mut(), self.right.as_mut()];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::NotExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new("%NOT", self.not_token.span());
        let args = [self.expression.as_mut()];
        infer_call(func_name, args, scope)
    }
}

impl InferTypes for ast::IfExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let args = [
            self.condition.as_mut(),
            self.then_expression.as_mut(),
            self.else_expression.as_mut(),
        ];
        infer_call(&Name::new("%IF", self.if_token.span()), args, scope)
    }
}

impl InferTypes for ast::CaseExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // CASE is basically two different constructs, depending on whether
        // there is a CASE expression or not. We handle them separately.
        let mut table = UnificationTable::default();
        if let Some(case_expr) = &mut self.case_expr {
            let match_tv = table.type_var("M", &self.case_token)?;
            match_tv.unify(&case_expr.infer_types(scope)?, &mut table, case_expr)?;
            let result_tv = table.type_var("R", &self.case_token)?;
            for c in &mut self.when_clauses {
                match_tv.unify(&c.condition.infer_types(scope)?, &mut table, &c.condition)?;
                result_tv.unify(&c.result.infer_types(scope)?, &mut table, &c.result)?;
            }
            if let Some(else_clause) = &mut self.else_clause {
                let else_expr = &mut else_clause.result;
                result_tv.unify(&else_expr.infer_types(scope)?, &mut table, else_expr)?;
            } else {
                result_tv.unify(&ArgumentType::null(), &mut table, self)?;
            }
            Ok(match_tv.resolve(&table, self)?)
        } else {
            let bool_ty = ArgumentType::bool();
            let result_tv = table.type_var("R", &self.case_token)?;
            for c in &mut self.when_clauses {
                c.condition
                    .infer_types(scope)?
                    .expect_subtype_of(&bool_ty, c)?;
                result_tv.unify(&c.result.infer_types(scope)?, &mut table, &c.result)?;
            }
            if let Some(else_clause) = &mut self.else_clause {
                let else_expr = &mut else_clause.result;
                result_tv.unify(&else_expr.infer_types(scope)?, &mut table, else_expr)?;
            } else {
                result_tv.unify(&ArgumentType::null(), &mut table, self)?;
            }
            Ok(result_tv.resolve(&table, self)?)
        }
    }
}

impl InferTypes for ast::BinopExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let prim_name = Name::new(
            &format!("%{}", self.op_token.token.as_str()),
            self.op_token.span(),
        );
        infer_call(&prim_name, [self.left.as_mut(), self.right.as_mut()], scope)
    }
}

impl InferTypes for ast::ArrayExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let return_ty = self.definition.infer_types(scope)?;
        let return_ty = if let Some(element_ty) = &self.element_type {
            let element_ty = ArgumentType::Value(ValueType::try_from(&element_ty.elem_type)?);
            return_ty.expect_subtype_of(&element_ty, self)?;
            element_ty
        } else {
            return_ty
        };
        let return_ty = return_ty.expect_simple_type(self)?;
        Ok(ArgumentType::Value(ValueType::Array(return_ty.clone())))
    }
}

impl InferTypes for ast::ArrayDefinition {
    type Scope = ColumnSetScope;
    /// The **element type** of the array.
    type Output = ArgumentType;

    /// Infer the **element type** of the array.
    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::ArrayDefinition::Query(query) => {
                let table_ty = query.infer_types(&scope.clone().try_into_handle_for_subquery()?)?;
                let elem_ty = table_ty.expect_one_column(query)?.ty.clone();
                Ok(elem_ty)
            }
            ast::ArrayDefinition::Elements(exprs) => {
                // We can use infer_call if we're careful.
                let span = exprs.items.span();
                let func_name = &Name::new("%ARRAY", span);
                let elem_ty = infer_call(func_name, exprs.node_iter_mut(), scope)?;
                let elem_ty = elem_ty.expect_array_type_returning_elem_type(self)?;
                let elem_ty = ArgumentType::Value(ValueType::Simple(elem_ty.clone()));
                Ok(elem_ty)
            }
        }
    }
}

impl InferTypes for ast::FunctionCall {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        if self.over_clause.is_some() {
            return Err(nyi(&self.over_clause, "over clause"));
        }
        infer_call(&self.name, self.args.node_iter_mut(), scope)
    }
}

impl InferTypes for ast::IndexExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new("%[]", self.bracket1.span());
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
            ast::Expression::ColumnName(name) => {
                let (_table, col) = name.split_table_and_column();
                Some(col)
            }
            _ => None,
        }
    }
}

impl InferColumnName for ast::Alias {
    fn infer_column_name(&mut self) -> Option<Ident> {
        Some(self.ident.clone())
    }
}

/// Build a set from an optional [`ast::Except`] clause.
fn except_set(except: &Option<ast::Except>) -> HashSet<Name> {
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
    func_name: &Name,
    args: ArgExprs,
    scope: &ColumnSetScope,
) -> Result<ArgumentType>
where
    ArgExprs: IntoIterator<Item = &'args mut ast::Expression>,
{
    let func_ty = scope.get_function_type(func_name)?;
    let mut arg_types = vec![];
    for arg in args {
        arg_types.push(arg.infer_types(scope)?);
    }
    let ret_ty = func_ty.return_type_for(&arg_types, func_name)?;
    Ok(ret_ty)
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
        scope::{Scope, ScopeValue},
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
        scope.get(&Name::new(name, Span::Unknown)).unwrap()
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
