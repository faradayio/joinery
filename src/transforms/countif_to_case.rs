use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression, FunctionCall, FunctionName},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `COUNTIF(condition)` into a portable `CASE` expression.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct CountifToCase;

impl CountifToCase {
    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::FunctionCall(FunctionCall {
            name: FunctionName::Function { function },
            args,
            over_clause: None,
            ..
        }) = expr
        {
            if function.name.eq_ignore_ascii_case("COUNTIF") && args.node_iter().count() == 1 {
                let condition = args.node_iter().next().expect("has 1 arg");
                let replacement = sql_quote! {
                    COUNT(CASE WHEN #condition THEN 1 END)
                }
                .try_into_expression()
                .expect("generated SQL should always parse");
                *expr = replacement;
            }
        }
    }
}

impl Transform for CountifToCase {
    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
