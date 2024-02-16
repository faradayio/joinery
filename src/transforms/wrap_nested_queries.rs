use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, QueryExpressionQuery},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `(query_expression)` into `SELECT * FROM (query_expression)`.
/// Needed for SQLite, which doesn't allow using parentheses when working with
/// `UNION` or `EXCEPT`.
#[derive(VisitorMut)]
#[visitor(QueryExpressionQuery(enter))]
pub struct WrapNestedQueries;

impl WrapNestedQueries {
    fn enter_query_expression_query(&mut self, query_expression_query: &mut QueryExpressionQuery) {
        if let QueryExpressionQuery::Nested {
            paren1,
            query,
            paren2,
        } = query_expression_query
        {
            let replacement = sql_quote! { SELECT * FROM #paren1 #query #paren2 }
                .try_into_query_expression_query()
                .expect("generated SQL should always parse");
            *query_expression_query = replacement;
        }
    }
}

impl Transform for WrapNestedQueries {
    fn name(&self) -> &'static str {
        "WrapNestedQueries"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
