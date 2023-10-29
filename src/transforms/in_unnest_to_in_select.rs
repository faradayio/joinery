use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, InExpression, InValueSet},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `val IN UNNEST(expr)` into `val IN (SELECT * FROM UNNEST(expr))`.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct InUnnestToInSelect;

impl InUnnestToInSelect {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::In(InExpression {
            left,
            not_token,
            in_token,
            value_set: InValueSet::Unnest { expression, .. },
        }) = expr
        {
            let replacement = sql_quote! {
                #left #not_token #in_token (SELECT * FROM UNNEST(#expression))
            }
            .try_into_expression()
            .expect("generated SQL should always parse");
            *expr = replacement;
        }
    }
}

impl Transform for InUnnestToInSelect {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
