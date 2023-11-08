use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, InExpression, InValueSet},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `val IN UNNEST(expr)` into `CONTAINS(expr, val)`.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct InUnnestToContains;

impl InUnnestToContains {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::In(InExpression {
            left,
            not_token,
            value_set: InValueSet::Unnest { expression, .. },
            ..
        }) = expr
        {
            let replacement = sql_quote! {
                (#not_token r#CONTAINS(#expression, #left))
            }
            .try_into_expression()
            .expect("generated SQL should always parse");
            *expr = replacement;
        }
    }
}

impl Transform for InUnnestToContains {
    fn name(&self) -> &'static str {
        "InUnnestToInSelect"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
