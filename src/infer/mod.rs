//! Our type inference subsystem.

use std::collections::HashSet;

use tracing::trace;

use crate::{
    ast::{self, ConditionJoinOperator, Emit, Expression, Name},
    errors::{Error, Result},
    scope::{ColumnSet, ColumnSetScope, Scope, ScopeGet, ScopeHandle},
    tokenizer::{Ident, Keyword, Literal, LiteralValue, Punct, Spanned},
    types::{
        ArgumentType, ColumnType, ResolvedTypeVarsOnly, SimpleType, StructElementType, StructType,
        TableType, Type, Unnested, ValueType,
    },
    unification::{UnificationTable, Unify},
};

use self::contains_aggregate::ContainsAggregate;

mod contains_aggregate;

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
        let ast::QueryExpression {
            with_clause,
            query,
            order_by,
            limit,
        } = self;

        let mut scope = scope.clone();
        if let Some(with_clause) = with_clause {
            scope = with_clause.infer_types(&scope)?;
        }
        let (ty, column_set_scope) = query.infer_types(&scope)?;
        if let Some(order_by) = order_by {
            // This is a bit complicated because `ORDER BY` can see columns from
            // an immediately adjacent `SELECT` list, but not from a nested
            // query or a set operation. So we'll use the `column_set_scope` if
            // we have it, but fall back to creating a new one from the table
            // type. Both BigQuery and Trino agree on this. See
            // `order_and_limit.sql` for an example.
            let column_set_scope = column_set_scope
                .unwrap_or_else(|| ColumnSetScope::new_from_table_type(&scope, &ty));
            order_by.infer_types(&column_set_scope)?;
        }
        if let Some(limit) = limit {
            limit.infer_types(&())?;
        }
        Ok(ty)
    }
}

impl InferTypes for ast::QueryExpressionWithClause {
    type Scope = ScopeHandle;
    type Output = ScopeHandle;

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        let mut scope = scope.clone();
        for cte in self.ctes.node_iter_mut() {
            scope = cte.infer_types(&scope)?;
        }
        Ok(scope)
    }
}

impl InferTypes for ast::QueryExpressionQuery {
    type Scope = ScopeHandle;

    /// We return both a `TableType` and _possibly_ a `ColumnSetScope` because
    /// `ORDER BY` may need a full `ColumnSetScope` to support things like
    /// `ORDER BY table1.col1, table2.col2`. But the `ColumnSetScope` is easily
    /// lost in more complicated cases. See the test `order_and_limit.sql` for
    /// example code.
    type Output = (TableType, Option<ColumnSetScope>);

    fn infer_types(&mut self, scope: &ScopeHandle) -> Result<Self::Output> {
        match self {
            ast::QueryExpressionQuery::Select(expr) => {
                let (ty, column_set_scope) = expr.infer_types(scope)?;
                Ok((ty, Some(column_set_scope)))
            }
            ast::QueryExpressionQuery::Nested { query, .. } => {
                Ok((query.infer_types(scope)?, None))
            }
            ast::QueryExpressionQuery::SetOperation {
                left,
                set_operator,
                right,
            } => {
                let (left_ty, _) = left.infer_types(scope)?;
                let (right_ty, _) = right.infer_types(scope)?;
                let result_ty = left_ty.common_supertype(&right_ty).ok_or_else(|| {
                    Error::annotated(
                        format!("cannot combine {} and {}", left_ty, right_ty),
                        set_operator.span(),
                        "incompatible types",
                    )
                })?;
                Ok((result_ty, None))
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

    /// We return both a `TableType` and a `ColumnSetScope` because `ORDER BY`
    /// may need a full `ColumnSetScope` to support things like `ORDER BY
    /// table1.col1, table2.col2`, which is allowed in certain contexts.
    type Output = (TableType, ColumnSetScope);

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

        trace!(sql = %self.emit_to_string(ast::Target::BigQuery), "inferring types");
        let ast::SelectExpression {
            ty,
            select_options: _,
            select_list: ast::SelectList {
                items: select_list, ..
            },
            from_clause,
            where_clause,
            group_by,
            having,
            qualify,
            order_by,
            limit,
        } = self;

        // See if we have a FROM clause.
        let mut column_set_scope = if let Some(from_clause) = from_clause {
            from_clause.infer_types(scope)
        } else {
            Ok(ColumnSetScope::new_empty(scope))
        }?;
        trace!(columns = %column_set_scope.column_set(), "columns after FROM clause");

        // Check our WHERE clause.
        if let Some(where_clause) = where_clause {
            where_clause.infer_types(&column_set_scope)?;
        }

        // See if we have a GROUP BY clause.
        if let Some(group_by) = group_by {
            let group_by_names = group_by.infer_types(&column_set_scope)?;
            column_set_scope = column_set_scope
                .try_transform(|column_set| column_set.group_by(&group_by_names))?;
            trace!(columns = %column_set_scope.column_set(), "columns after GROUP BY");
        } else if select_list.contains_aggregate(&column_set_scope) {
            // If we have aggregates but no GROUP BY, we need to add a synthetic
            // GROUP BY of the empty set of columns.
            column_set_scope =
                column_set_scope.try_transform(|column_set| column_set.group_by(&[]))?;
            trace!(columns = %column_set_scope.column_set(), "columns after implicit GROUP BY");
        }

        // Check our HAVING clause.
        if let Some(having) = having {
            having.infer_types(&column_set_scope)?;
        }

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
                ast::SelectListItem::Wildcard { ty, star, except } => {
                    let table_type = column_set_scope.column_set().star(star)?;
                    // TODO: Should we pass the `column_set` here instead of a
                    // table type? I need to think about this.
                    add_table_cols(&mut cols, &table_type, except);
                    // Table type built from `column_set_scope`.
                    *ty = Some(table_type);
                }
                ast::SelectListItem::TableNameWildcard {
                    ty,
                    table_name,
                    star,
                    except,
                    ..
                } => {
                    let table_type = column_set_scope.column_set().star_for(table_name, star)?;
                    add_table_cols(&mut cols, &table_type, except);
                    // Modified version of `table_name`'s type, built from
                    // `column_set_scope`.
                    *ty = Some(table_type);
                }
                ast::SelectListItem::ExpressionWildcard { ty, expression, .. } => {
                    let struct_type = expression.infer_types(&column_set_scope)?;
                    let struct_type = struct_type.expect_struct_type(expression)?;
                    let table_type = struct_type.to_table_type();
                    add_table_cols(&mut cols, &table_type, &None);
                    *ty = Some(struct_type.to_owned());
                }
            }
        }
        let table_type = TableType { columns: cols };
        trace!(table_type = %table_type, "select list type");

        // Handle our QUALIFY clause.
        if let Some(qualify) = qualify {
            // BigQuery also allows us to see SELECT columns, but that's not
            // easy to do using our portable rewrite.
            qualify.infer_types(&column_set_scope)?;
        }

        // Handle our ORDER BY clause.
        if let Some(order_by) = order_by {
            // Make a new scope for ORDER BY, which can see columns from the
            // FROM clause and SELECT list.
            let scope = column_set_scope.clone().try_into_handle_for_subquery()?;
            let mut scope = Scope::new(&scope);
            for column in &table_type.columns {
                if let Some(name) = &column.name {
                    scope.add(
                        Name::from(name.clone()),
                        Type::Argument(column.ty.to_owned()),
                    )?;
                }
            }
            let scope = scope.into_handle();
            order_by.infer_types(&ColumnSetScope::new_empty(&scope))?;
        }

        // Handle our LIMIT clause.
        if let Some(limit) = limit {
            limit.infer_types(&())?;
        }

        *ty = Some(table_type.clone());
        Ok((table_type, column_set_scope))
    }
}

impl InferTypes for ast::ArraySelectExpression {
    type Scope = ScopeHandle;
    // Return the element type of the array.
    type Output = SimpleType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ast::ArraySelectExpression {
            select_options: _,
            expression,
            from_token: _,
            unnest,
            alias,
            where_clause,
        } = self;

        let alias = alias.ident.clone();
        let column_set = unnest.infer_types(&(ColumnSetScope::new_empty(scope), Some(alias)))?;
        let scope = ColumnSetScope::new(scope, column_set);
        let elem_type = expression.infer_types(&scope)?;
        let elem_type = elem_type.expect_simple_type(expression)?;
        if let Some(where_clause) = where_clause {
            where_clause.infer_types(&scope)?;
        }
        Ok(elem_type.clone())
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
        // We need to handle `UNNEST(..)` specially because it interacts with
        // our alias.
        match &mut self.table_expression {
            ast::FromTableExpression::Unnest(unnest) => {
                let unnest_scope = (scope.clone(), self.alias.clone().map(|a| a.ident));
                let column_set = unnest.infer_types(&unnest_scope)?;
                Ok(ColumnSetScope::new(outer_scope, column_set))
            }
            _ => {
                // TODO: Do we want `outer_scope` here?
                let (name, table_type) = self.table_expression.infer_types(outer_scope)?;
                let alias = self.alias.as_ref().map(ast::Alias::name);
                let name = alias.or(name);
                let column_set = ColumnSet::from_table(name, table_type);
                Ok(ColumnSetScope::new(outer_scope, column_set))
            }
        }
    }
}

impl InferTypes for ast::FromTableExpression {
    type Scope = ScopeHandle;
    type Output = (Option<Name>, TableType);

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::FromTableExpression::TableName(table_name) => {
                let table_type = scope.get_table_type(table_name)?;
                Ok((Some(table_name.clone()), table_type))
            }
            ast::FromTableExpression::Subquery { query, .. } => {
                let table_type = query.infer_types(scope)?;
                Ok((None, table_type))
            }
            ast::FromTableExpression::Unnest(_) => {
                // This interracts with the alias handling in `FromItem`.
                panic!("UnnestExpression should be handled by FromItem")
            }
        }
    }
}

impl InferTypes for ast::UnnestExpression {
    type Scope = (ColumnSetScope, Option<Ident>);
    type Output = ColumnSet;

    fn infer_types(&mut self, (scope, name): &Self::Scope) -> Result<Self::Output> {
        let array_ty = self.expression.infer_types(scope)?;
        let array_ty = array_ty.expect_array_type(&self.expression)?;
        match array_ty.unnest(&self.expression)? {
            Unnested::AnonymousColumn(mut table_ty) => {
                table_ty.columns[0].name = name.clone();
                Ok(ColumnSet::from_table(None, table_ty))
            }
            Unnested::NamedColumns(table_ty) => Ok(ColumnSet::from_table(
                name.clone().map(Name::from),
                table_ty,
            )),
        }
    }
}

impl InferTypes for ast::JoinOperation {
    type Scope = (ScopeHandle, ColumnSetScope);
    type Output = ColumnSetScope;

    fn infer_types(&mut self, scopes @ (_, scope): &Self::Scope) -> Result<Self::Output> {
        let from_type = self.from_item_mut().infer_types(scopes)?;
        let new_scope = scope.clone().try_transform(|column_set| match self {
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
            _ => Ok(column_set.join(from_type.column_set())),
        })?;

        // Check `ON` clause, which needs the JOINed scope.
        if let ast::JoinOperation::ConditionJoin {
            operator: Some(ConditionJoinOperator::On { expression, .. }),
            ..
        } = self
        {
            let expr_ty = expression.infer_types(&new_scope)?;
            expr_ty.expect_subtype_of(&ArgumentType::bool(), expression)?;
        }
        Ok(new_scope)
    }
}

impl InferTypes for ast::WhereClause {
    type Scope = ColumnSetScope;
    type Output = ();

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ty = self.expression.infer_types(scope)?;
        ty.expect_subtype_of(&ArgumentType::bool(), &self.expression)?;
        Ok(())
    }
}

impl InferTypes for ast::GroupBy {
    type Scope = ColumnSetScope;
    /// We output a list of names that can be used normally in the SELECT list.
    ///
    /// TODO: This is actually insufficient, given the weirdness of how GROUP
    /// BY, HAVING and SELECT interact. See
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_grouping_item
    /// for details. But we'll keep this simple for now.
    type Output = Vec<Name>;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // Simplified GROUP BY rules:
        //   1. We type check everything.
        //   2. We group by simple identifiers seen in the FROM and JOIN
        //      clauses, which are all in our namespace.
        //   3. We don't try to handle names from the SELECT list, or integer
        //      ordinals, because those will require a third type of special
        //      namespace.
        let mut group_by_names = vec![];
        for expr in self.expressions.node_iter_mut() {
            let _ty = expr.infer_types(scope)?;
            match expr {
                Expression::Name(name) => {
                    group_by_names.push(name.clone());
                }
                _ => {
                    return Err(nyi(expr, "group by expression"));
                }
            }
        }
        Ok(group_by_names)
    }
}

impl InferTypes for ast::Having {
    type Scope = ColumnSetScope;
    type Output = ();

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ty = self.expression.infer_types(scope)?;
        ty.expect_subtype_of(&ArgumentType::bool(), &self.expression)?;
        Ok(())
    }
}

impl InferTypes for ast::Qualify {
    type Scope = ColumnSetScope;
    type Output = ();

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ty = self.expression.infer_types(scope)?;
        ty.expect_subtype_of(&ArgumentType::bool(), &self.expression)?;
        Ok(())
    }
}

impl InferTypes for ast::Expression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::Expression::Literal(Literal { value, .. }) => value.infer_types(&()),
            ast::Expression::BoolValue(_) => Ok(ArgumentType::bool()),
            ast::Expression::Null { .. } => Ok(ArgumentType::null()),
            ast::Expression::Name(name) => name.infer_types(scope),
            ast::Expression::Cast(cast) => cast.infer_types(scope),
            ast::Expression::Is(is) => is.infer_types(scope),
            ast::Expression::In(in_expr) => in_expr.infer_types(scope),
            ast::Expression::Between(between) => between.infer_types(scope),
            ast::Expression::KeywordBinop(binop) => binop.infer_types(scope),
            ast::Expression::Not(not) => not.infer_types(scope),
            ast::Expression::If(if_expr) => if_expr.infer_types(scope),
            ast::Expression::Case(case) => case.infer_types(scope),
            ast::Expression::Unary(unary) => unary.infer_types(scope),
            ast::Expression::Binop(binop) => binop.infer_types(scope),
            ast::Expression::Query { query, .. } => {
                let table_ty = query.infer_types(&scope.clone().try_into_handle_for_subquery()?)?;
                Ok(table_ty.expect_one_column(query)?.ty.clone())
            }
            ast::Expression::Parens { expression, .. } => expression.infer_types(scope),
            ast::Expression::Array(array) => array.infer_types(scope),
            ast::Expression::Struct(struct_expr) => struct_expr.infer_types(scope),
            ast::Expression::Count(count) => count.infer_types(scope),
            ast::Expression::CurrentTimeUnit(current) => current.infer_types(scope),
            ast::Expression::ArrayAgg(array_agg) => array_agg.infer_types(scope),
            ast::Expression::SpecialDateFunctionCall(fcall) => fcall.infer_types(scope),
            ast::Expression::FunctionCall(fcall) => fcall.infer_types(scope),
            ast::Expression::Index(index) => index.infer_types(scope),
            ast::Expression::FieldAccess(field_access) => field_access.infer_types(scope),
            ast::Expression::Load(load_expr) => load_expr.infer_types(scope),
            ast::Expression::Store(store_expr) => store_expr.infer_types(scope),
        }
    }
}

impl InferTypes for LiteralValue {
    type Scope = ();
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
        // `scope` contains the columns we can see. But this might be something
        // like `[my_table.]my_struct_column.field1.field2`, so we need to be
        // prepared to split this.

        // First, find the split between our "base name" (presumably a column)
        // and any field accesses.
        let mut field_names_with_base_names = vec![];
        let mut candidate = self.clone();
        let mut base_type = loop {
            match scope.get_argument_type(&candidate) {
                Ok(base_type) => break base_type,
                Err(_) => {
                    let (next, field_name) = candidate.split_table_and_column();
                    if let Some(next) = next {
                        field_names_with_base_names.push((field_name, next.clone()));
                        candidate = next;
                    } else {
                        // Report an error containing the original name.
                        return Err(Error::annotated(
                            format!("unknown name: {}", self.unescaped_bigquery()),
                            self.span(),
                            "not found",
                        ));
                    }
                }
            }
        };

        // Now that we have a base type, look up each field.
        for (field_name, base_name) in field_names_with_base_names.into_iter().rev() {
            // TODO: These errors here _might_ not always be optimal for the
            // user if we're not actually dealing with structs? But maybe
            // they're OK in most cases.
            base_type = ArgumentType::Value(
                base_type
                    .expect_struct_type(&base_name)?
                    .expect_field(&Name::from(field_name))?
                    .clone(),
            );
        }
        Ok(base_type)
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
        func_ty.return_type_for(&arg_types, false, func_name)
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
        func_ty.return_type_for(&[left_ty, elem_ty], false, func_name)
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
                Ok(table_ty.into())
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
        infer_call(func_name, args, false, scope)
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
        infer_call(func_name, args, false, scope)
    }
}

impl InferTypes for ast::NotExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new("%NOT", self.not_token.span());
        let args = [self.expression.as_mut()];
        infer_call(func_name, args, false, scope)
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
        infer_call(&Name::new("%IF", self.if_token.span()), args, false, scope)
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

impl InferTypes for ast::UnaryExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = &Name::new(
            &format!("%UNARY{}", self.op_token.token.as_str()),
            self.op_token.span(),
        );
        let func_ty = scope.get_function_type(func_name)?;
        let arg_ty = self.expression.infer_types(scope)?;
        func_ty.return_type_for(&[arg_ty], false, func_name)
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
        infer_call(
            &prim_name,
            [self.left.as_mut(), self.right.as_mut()],
            false,
            scope,
        )
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

        // Record our type inference data for use elsewhere.
        let simple_ty = return_ty.expect_simple_type(self)?;
        let array_type = ValueType::Array(simple_ty.to_owned());
        self.ty = Some(array_type.clone());

        // `simple_ty` is the type of the elements, so build the array type around the element type.
        Ok(ArgumentType::Value(array_type))
    }
}

impl InferTypes for ast::ArrayDefinition {
    type Scope = ColumnSetScope;
    /// The **element type** of the array.
    type Output = ArgumentType;

    /// Infer the **element type** of the array.
    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::ArrayDefinition::Query { select } => {
                let elem_ty = select.infer_types(&scope.clone().try_into_handle_for_subquery()?)?;
                Ok(ArgumentType::Value(ValueType::Simple(elem_ty)))
            }
            ast::ArrayDefinition::Elements(exprs) => {
                if exprs.is_empty() {
                    // The type of `ARRAY[]` appears to always be
                    // `ARRAY<INT64>`. An expression like `SELECT
                    // ARRAY[STRUCT([]), STRUCT(["hello"])]` fails with the
                    // error message "Array elements of types
                    // {STRUCT<ARRAY<INT64>>, STRUCT<ARRAY<STRING>>} do not have
                    // a common supertype", so BigQuery isn't doing any complex
                    // inference to find a consistent element type.
                    //
                    // And Trino does _very_ weird thing with `ARRAY(unknown)`
                    // types for empty arrays if we don't infer this.
                    Ok(ArgumentType::Value(ValueType::Simple(SimpleType::Int64)))
                } else {
                    // We can use infer_call if we're careful.
                    let span = exprs.items.span();
                    let func_name = &Name::new("%ARRAY", span);
                    let elem_ty = infer_call(func_name, exprs.node_iter_mut(), false, scope)?;
                    let elem_ty = elem_ty.expect_array_type_returning_elem_type(self)?;
                    let elem_ty = ArgumentType::Value(ValueType::Simple(elem_ty.clone()));
                    Ok(elem_ty)
                }
            }
        }
    }
}

impl InferTypes for ast::StructExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let ast::StructExpression {
            ty,
            struct_token,
            field_decls,
            fields,
            ..
        } = self;

        // Infer our struct type from the field expressions.
        let mut field_types = vec![];
        for field in fields.items.node_iter_mut() {
            if let ast::SelectListItem::Expression { expression, alias } = field {
                let field_ty = expression.infer_types(scope)?;
                let field_ty = field_ty.expect_value_type(expression)?;
                let ident = alias
                    .clone()
                    .map(|a| a.ident)
                    .or_else(|| expression.infer_column_name());
                field_types.push(StructElementType {
                    name: ident,
                    ty: field_ty.clone(),
                });
            } else {
                // We could forbid this in the grammar if we were less lazy.
                return Err(Error::annotated(
                    "struct field must be expression, not wildcard",
                    field.span(),
                    "expression required",
                ));
            }
        }
        let actual_ty = StructType {
            fields: field_types,
        };

        // If we have `field_decls`, use those to build our official type.
        let return_ty = if let Some(field_decls) = field_decls {
            let expected_ty =
                ValueType::<ResolvedTypeVarsOnly>::try_from(&ast::DataType::Struct {
                    struct_token: Keyword::new("STRUCT", struct_token.span()),
                    lt: Punct::new("<", field_decls.lt.span()),
                    fields: field_decls.fields.clone(),
                    gt: Punct::new(">", field_decls.gt.span()),
                })?;
            let expected_ty = expected_ty.expect_struct_type(field_decls)?;
            actual_ty.expect_subtype_of(expected_ty, field_decls)?;
            expected_ty.clone()
        } else {
            actual_ty
        };

        *ty = Some(return_ty.clone());
        if return_ty
            .fields
            .iter()
            .any(|f| f.ty == ValueType::Simple(SimpleType::Null))
        {
            return Err(Error::annotated(
                format!(
                    "NULL column in {}, try STRUCT<col1 type1, ..>(..)",
                    return_ty
                ),
                struct_token.span(),
                "contains a NULL field",
            ));
        }
        let return_ty = ValueType::Simple(SimpleType::Struct(return_ty));
        return_ty.expect_inhabited(field_decls)?;
        Ok(ArgumentType::Value(return_ty))
    }
}

impl InferTypes for ast::CountExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::CountExpression::CountStar { .. } => {
                Ok(ArgumentType::Value(ValueType::Simple(SimpleType::Int64)))
            }
            ast::CountExpression::CountExpression {
                count_token,
                expression,
                ..
            } => {
                // TODO: COUNT OVER
                let func_name = &Name::new("COUNT", count_token.span());
                let args = [expression.as_mut()];
                infer_call(func_name, args, false, scope)
            }
        }
    }
}

impl InferTypes for ast::CurrentTimeUnit {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_name = Name::from(self.current_time_unit_token.ident.clone());
        let func_ty = scope.get_function_type(&func_name)?;
        func_ty.return_type_for(&[], false, &func_name)
    }
}

impl InferTypes for ast::ArrayAggExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // TODO: ARRAY_AGG OVER
        if let Some(order_by) = &mut self.order_by {
            // TODO: Should this always return an aggregate type?
            order_by.infer_types(scope)?;
        }
        let func_name = &Name::new("ARRAY_AGG", self.array_agg_token.span());
        let args = [self.expression.as_mut()];
        infer_call(func_name, args, false, scope)
    }
}

impl InferTypes for ast::OrderBy {
    type Scope = ColumnSetScope;
    type Output = ();

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        for item in self.items.node_iter_mut() {
            item.expression.infer_types(scope)?;
        }
        Ok(())
    }
}

impl InferTypes for ast::Limit {
    type Scope = ();
    type Output = ();

    fn infer_types(&mut self, _scope: &Self::Scope) -> Result<Self::Output> {
        let ty = self.value.value.infer_types(&())?;
        ty.expect_subtype_of(&ArgumentType::int64(), &self.value)?;
        Ok(())
    }
}

impl InferTypes for ast::SpecialDateFunctionCall {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let func_ty = scope.get_function_type(&Name::from(self.function_name.ident.clone()))?;
        let mut arg_types = vec![];
        for arg in &mut self.args.node_iter_mut() {
            arg_types.push(arg.infer_types(scope)?);
        }
        func_ty.return_type_for(&arg_types, false, &self.function_name)
    }
}

impl InferTypes for ast::SpecialDateExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        match self {
            ast::SpecialDateExpression::Expression(expression) => expression.infer_types(scope),
            ast::SpecialDateExpression::Interval(_) => {
                Ok(ArgumentType::Value(ValueType::Simple(SimpleType::Interval)))
            }
            ast::SpecialDateExpression::DatePart(_) => {
                Ok(ArgumentType::Value(ValueType::Simple(SimpleType::Datepart)))
            }
        }
    }
}

impl InferTypes for ast::FunctionCall {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        if let Some(over_clause) = self.over_clause.as_mut() {
            let new_scope = over_clause.infer_types(scope)?;
            infer_call(&self.name, self.args.node_iter_mut(), true, &new_scope)
        } else {
            infer_call(&self.name, self.args.node_iter_mut(), false, scope)
        }
    }
}

impl InferTypes for ast::OverClause {
    type Scope = ColumnSetScope;
    type Output = ColumnSetScope;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        if let Some(order_by) = &mut self.order_by {
            order_by.infer_types(scope)?;
        }
        let partition_by_names = if let Some(partition_by) = &mut self.partition_by {
            partition_by.infer_types(scope)?
        } else {
            vec![]
        };
        scope
            .clone()
            .try_transform(|column_set| column_set.group_by(&partition_by_names))
    }
}

impl InferTypes for ast::PartitionBy {
    type Scope = ColumnSetScope;
    type Output = Vec<Name>;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let mut partition_by_names = vec![];
        for expr in self.expressions.node_iter_mut() {
            match expr {
                ast::Expression::Name(name) => {
                    scope.get_argument_type(name)?;
                    partition_by_names.push(name.clone());
                }
                _ => {
                    return Err(nyi(expr, "partition by expression"));
                }
            }
        }
        Ok(partition_by_names)
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
        infer_call(func_name, args, false, scope)
    }
}

impl InferTypes for ast::FieldAccessExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        let struct_ty = self.expression.infer_types(scope)?;
        let struct_ty = struct_ty.expect_struct_type(&self.expression)?;
        let field_ty = struct_ty.expect_field(&Name::from(self.field_name.clone()))?;
        Ok(ArgumentType::Value(field_ty.clone()))
    }
}

impl InferTypes for ast::LoadExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // TODO: More here.
        self.expression.infer_types(scope)
    }
}

impl InferTypes for ast::StoreExpression {
    type Scope = ColumnSetScope;
    type Output = ArgumentType;

    fn infer_types(&mut self, scope: &Self::Scope) -> Result<Self::Output> {
        // TODO: More here.
        self.expression.infer_types(scope)
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
            ast::Expression::Name(name) => {
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
    is_window: bool,
    scope: &ColumnSetScope,
) -> Result<ArgumentType>
where
    ArgExprs: IntoIterator<Item = &'args mut ast::Expression>,
{
    trace!(scope = %scope.column_set(), "inferring function call");
    let func_ty = scope.get_function_type(func_name)?;
    let mut arg_types = vec![];
    for arg in args {
        arg_types.push(arg.infer_types(scope)?);
    }
    trace!(name = &func_name.unescaped_bigquery(), args = ?arg_types, "inferring function call");
    let ret_ty = func_ty.return_type_for(&arg_types, is_window, func_name)?;
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
