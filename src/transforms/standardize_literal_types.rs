use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Expression},
    errors::Result,
    tokenizer::{Literal, LiteralValue},
};

use super::{Transform, TransformExtra};

/// Transform things like `1` to `CAST(1 AS INT64)`
#[derive(VisitorMut)]
#[visitor(Expression(exit))]
pub struct StandardizeLiteralTypes;

impl StandardizeLiteralTypes {
    fn exit_expression(&mut self, expr: &mut Expression) {
        if let ref int_lit @ Expression::Literal(Literal {
            value: LiteralValue::Int64(_),
            ..
        }) = expr
        {
            let new_expr = sql_quote! { CAST(#int_lit AS INT64) }
                .try_into_expression()
                .expect("generated SQL should always parse");
            *expr = new_expr;
        }
    }
}

impl Transform for StandardizeLiteralTypes {
    fn name(&self) -> &'static str {
        "StandardizeLiteralTypes"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
