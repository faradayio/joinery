use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, IndexExpression, IndexOffset},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `val[OFFSET(i)]` and `val[ORDINAL(i)]` into appropriate `val[..]`
/// expressions.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct IndexFromZero;

impl IndexFromZero {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::Index(IndexExpression { index, .. }) = expr {
            match index {
                IndexOffset::Simple(_) => {}
                IndexOffset::Offset { expression, .. } => {
                    *index = IndexOffset::Simple(expression.clone());
                }
                IndexOffset::Ordinal { expression, .. } => {
                    let replacement = sql_quote! { (#expression) - 1 }
                        .try_into_expression()
                        .expect("generated SQL should always parse");
                    *index = IndexOffset::Simple(Box::new(replacement));
                }
            }
        }
    }
}

impl Transform for IndexFromZero {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
