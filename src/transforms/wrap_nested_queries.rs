use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, QueryExpression},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `(query_expression)` into `SELECT * FROM (query_expression)`.
/// Needed for SQLite, which doesn't allow using parentheses when working with
/// `UNION` or `EXCEPT`.
#[derive(VisitorMut)]
#[visitor(QueryExpression(enter))]
pub struct WrapNestedQueries;

impl WrapNestedQueries {
    fn enter_query_expression(&mut self, query_expression: &mut QueryExpression) {
        if let QueryExpression::Nested {
            paren1,
            query,
            paren2,
        } = query_expression
        {
            let replacement = sql_quote! { SELECT * FROM #paren1 #query #paren2 }
                .try_into_query_expression()
                .expect("generated SQL should always parse");
            *query_expression = replacement;
        }
    }
}

impl Transform for WrapNestedQueries {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
