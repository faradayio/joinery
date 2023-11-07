use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, IsExpression, IsExpressionPredicate},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `expr IS [NOT] (TRUE|FALSE)` into a portable
/// `CASE` expression.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct IsBoolToCase;

impl IsBoolToCase {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::Is(IsExpression {
            left,
            not_token,
            predicate: IsExpressionPredicate::True(pred) | IsExpressionPredicate::False(pred),
            ..
        }) = expr
        {
            let replacement = sql_quote! {
                CASE #not_token #left WHEN #pred THEN TRUE ELSE FALSE END
            }
            .try_into_expression()
            .expect("generated SQL should always parse");
            *expr = replacement;
        }
    }
}

impl Transform for IsBoolToCase {
    fn name(&self) -> &'static str {
        "IsBoolToCase"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
