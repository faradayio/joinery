use derive_visitor::{DriveMut, VisitorMut};

use crate::{
    ast::{self, Expression},
    errors::Result,
    tokenizer::{Literal, Spanned},
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
            *expr = Expression::Literal(Literal::int(int_val, keyword.span()))
        }
    }
}

impl Transform for BoolToInt {
    fn name(&self) -> &'static str {
        "BoolToInt"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
