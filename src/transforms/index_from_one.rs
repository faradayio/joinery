use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, IndexExpression, IndexOffset},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `v[i]`, `val[OFFSET(i)]` and `val[ORDINAL(i)]` into appropriate `val[..]`
/// expressions that assume 1-based indexing.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct IndexFromOne;

impl IndexFromOne {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::Index(IndexExpression { index, .. }) = expr {
            match index {
                IndexOffset::Simple(expression) | IndexOffset::Offset { expression, .. } => {
                    let replacement = sql_quote! { (#expression) + 1 }
                        .try_into_expression()
                        .expect("generated SQL should always parse");
                    *index = IndexOffset::Simple(Box::new(replacement));
                }
                IndexOffset::Ordinal { expression, .. } => {
                    *index = IndexOffset::Simple(expression.clone());
                }
            }
        }
    }
}

impl Transform for IndexFromOne {
    fn name(&self) -> &'static str {
        "IndexFromOne"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
