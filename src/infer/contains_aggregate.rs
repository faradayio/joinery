use crate::{
    ast,
    scope::{ColumnSetScope, ScopeGet},
};

/// Interface used to search an AST node for aggregate functions.
pub trait ContainsAggregate {
    /// Does this AST node contain an aggregate function?
    ///
    /// Does not recurse into sub-queries.
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool;
}

impl<T: ContainsAggregate + ast::Node> ContainsAggregate for Box<T> {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.as_ref().contains_aggregate(scope)
    }
}

impl<T: ContainsAggregate + ast::Node> ContainsAggregate for Vec<T> {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.iter().any(|item| item.contains_aggregate(scope))
    }
}

impl<T: ContainsAggregate + ast::Node> ContainsAggregate for Option<T> {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.as_ref()
            .map_or(false, |item| item.contains_aggregate(scope))
    }
}

impl<T: ContainsAggregate + ast::Node> ContainsAggregate for ast::NodeVec<T> {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.node_iter().any(|item| item.contains_aggregate(scope))
    }
}

impl ContainsAggregate for ast::SelectList {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.items.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::SelectListItem {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            ast::SelectListItem::Expression {
                expression,
                alias: _,
            } => expression.contains_aggregate(scope),
            ast::SelectListItem::Wildcard { .. }
            | ast::SelectListItem::TableNameWildcard { .. } => false,
            ast::SelectListItem::ExpressionWildcard { expression, .. } => {
                expression.contains_aggregate(scope)
            }
        }
    }
}

impl ContainsAggregate for ast::Expression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            ast::Expression::Literal(_) => false,
            ast::Expression::BoolValue(_) => false,
            ast::Expression::Null(_) => false,
            ast::Expression::Name(_) => false,
            ast::Expression::Cast(cast) => cast.contains_aggregate(scope),
            ast::Expression::Is(is) => is.contains_aggregate(scope),
            ast::Expression::In(in_expr) => in_expr.contains_aggregate(scope),
            ast::Expression::Between(between) => between.contains_aggregate(scope),
            ast::Expression::KeywordBinop(binop) => binop.contains_aggregate(scope),
            ast::Expression::Not(not) => not.contains_aggregate(scope),
            ast::Expression::If(if_expr) => if_expr.contains_aggregate(scope),
            ast::Expression::Case(case) => case.contains_aggregate(scope),
            ast::Expression::Unary(unary) => unary.contains_aggregate(scope),
            ast::Expression::Binop(binop) => binop.contains_aggregate(scope),
            // We never look into sub-queries.
            ast::Expression::Query { .. } => false,
            ast::Expression::Parens { expression, .. } => expression.contains_aggregate(scope),
            ast::Expression::Array(arr) => arr.contains_aggregate(scope),
            ast::Expression::Struct(st) => st.contains_aggregate(scope),
            // TODO: false if we add `OVER`.
            ast::Expression::Count(_) => true,
            ast::Expression::CurrentTimeUnit(_) => false,
            // TODO: false if we add `OVER`.
            ast::Expression::ArrayAgg(_) => true,
            ast::Expression::SpecialDateFunctionCall(fcall) => fcall.contains_aggregate(scope),
            ast::Expression::FunctionCall(fcall) => fcall.contains_aggregate(scope),
            ast::Expression::Index(idx) => idx.contains_aggregate(scope),
            // Putting an aggregate here would be very weird. Do not allow it
            // until forced to do so.
            ast::Expression::FieldAccess(_) => false,
            ast::Expression::Load(load_expr) => load_expr.contains_aggregate(scope),
            ast::Expression::Store(store_expr) => store_expr.contains_aggregate(scope),
        }
    }
}

impl ContainsAggregate for ast::IntervalExpression {
    fn contains_aggregate(&self, _scope: &ColumnSetScope) -> bool {
        // These are currently all literals, so they can't contain aggregates.
        false
    }
}

impl ContainsAggregate for ast::Cast {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::IsExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.left.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::InExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.left.contains_aggregate(scope) || self.value_set.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::InValueSet {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            // We never look into sub-queries.
            ast::InValueSet::QueryExpression { .. } => false,
            ast::InValueSet::ExpressionList { expressions, .. } => {
                expressions.contains_aggregate(scope)
            }
            // I am doubtful that anything good is happening if we hit this
            // case.
            ast::InValueSet::Unnest { expression, .. } => expression.contains_aggregate(scope),
        }
    }
}

impl ContainsAggregate for ast::BetweenExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.left.contains_aggregate(scope)
            || self.middle.contains_aggregate(scope)
            || self.right.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::KeywordBinopExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.left.contains_aggregate(scope) || self.right.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::NotExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::IfExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.condition.contains_aggregate(scope)
            || self.then_expression.contains_aggregate(scope)
            || self.else_expression.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::CaseExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.case_expr.contains_aggregate(scope)
            || self.when_clauses.contains_aggregate(scope)
            || self.else_clause.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::CaseWhenClause {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.condition.contains_aggregate(scope) || self.result.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::CaseElseClause {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.result.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::UnaryExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::BinopExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.left.contains_aggregate(scope) || self.right.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::ArrayExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.definition.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::ArrayDefinition {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            // We never look into sub-queries.
            ast::ArrayDefinition::Query { .. } => false,
            ast::ArrayDefinition::Elements(expressions) => expressions.contains_aggregate(scope),
        }
    }
}

impl ContainsAggregate for ast::StructExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.fields.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::SpecialDateFunctionCall {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        // We don't need to check the function name here.
        self.args.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::SpecialDateExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            ast::SpecialDateExpression::Expression(expr) => expr.contains_aggregate(scope),
            ast::SpecialDateExpression::Interval(_) => false,
            ast::SpecialDateExpression::DatePart(_) => false,
        }
    }
}

impl ContainsAggregate for ast::FunctionCall {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        // Check the function we're calling.
        if scope
            .get_function_type(&self.name)
            .map_or(false, |func_ty| func_ty.is_aggregate())
            // If we have an OVER clause, we're not a normal aggregate.
            && self.over_clause.is_none()
        {
            return true;
        }

        // Check our arguments.
        self.args.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::IndexExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope) || self.index.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::IndexOffset {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        match self {
            ast::IndexOffset::Simple(expression) => expression.contains_aggregate(scope),
            ast::IndexOffset::Offset { expression, .. } => expression.contains_aggregate(scope),
            ast::IndexOffset::Ordinal { expression, .. } => expression.contains_aggregate(scope),
        }
    }
}

impl ContainsAggregate for ast::LoadExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope)
    }
}

impl ContainsAggregate for ast::StoreExpression {
    fn contains_aggregate(&self, scope: &ColumnSetScope) -> bool {
        self.expression.contains_aggregate(scope)
    }
}
