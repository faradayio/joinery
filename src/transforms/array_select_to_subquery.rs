use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, ArrayDefinition, ArrayExpression, Expression},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `ARRAY(SELECT ..)` into `(SELECT ARRAY_AGG(..) FROM (SELECT ..))`.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct ArraySelectToSubquery;

impl ArraySelectToSubquery {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::Array(ArrayExpression {
            definition: ArrayDefinition::Query { ty, query },
            ..
        }) = expr
        {
            // First we need to make sure we have 1 column, and it has a name.
            // All of this should have been enforced by type inference, so use
            // asserts to panic.
            let table_ty = ty
                .clone()
                .expect("inference should have added a table type");
            assert!(
                table_ty.columns.len() == 1,
                "array subquery must have 1 column"
            );
            let column_name = table_ty.columns[0]
                .name
                .clone()
                .expect("array subquery column must have a name");

            // The actual rewrite isn't too bad.
            *expr = sql_quote! {
                (
                    SELECT ARRAY_AGG(#column_name)
                    FROM (#query)
                )
            }
            .try_into_expression()
            .expect("should be valid SQL");
        }
    }
}

impl Transform for ArraySelectToSubquery {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
