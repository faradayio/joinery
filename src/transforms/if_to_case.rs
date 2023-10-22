use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `IF(condition, then_expression, else_expression)` into a portable
/// `CASE` expression.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct IfToCase;

impl IfToCase {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::If {
            condition,
            then_expression,
            else_expression,
            ..
        } = expr
        {
            let replacement = sql_quote! {
                CASE WHEN #condition THEN #then_expression ELSE #else_expression END
            }
            .try_into_expression()
            .expect("generated SQL should always parse");
            *expr = replacement;
        }
    }
}

impl Transform for IfToCase {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
