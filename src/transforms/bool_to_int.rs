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
pub struct BoolToInt;

impl BoolToInt {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::BoolValue(keyword) = expr {
            let name = keyword.ident.name.to_ascii_uppercase();
            let int_val: i64 = match name.as_str() {
                "TRUE" => 1,
                "FALSE" => 0,
                _ => unreachable!("the parser only allows TRUE or FALSE"),
            };
            *expr = sql_quote! { #int_val }
                .try_into_expression()
                .expect("generated SQL should always parse");
        }
    }
}

impl Transform for BoolToInt {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
