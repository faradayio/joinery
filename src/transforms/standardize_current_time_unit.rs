use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, CurrentTimeUnit, Expression},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Ensure `CURRENT_DATE` either has parens, or doesn't.
#[derive(VisitorMut)]
#[visitor(Expression(enter))]
pub struct StandardizeCurrentTimeUnit {
    want_parens: bool,
}

impl StandardizeCurrentTimeUnit {
    /// Ensure `CURRENT_DATE` always has parens.
    #[allow(dead_code)]
    pub fn parens() -> Self {
        Self { want_parens: true }
    }

    /// Ensure `CURRENT_DATE` never has parens.
    pub fn no_parens() -> Self {
        Self { want_parens: false }
    }

    fn enter_expression(&mut self, expr: &mut Expression) {
        if let Expression::CurrentTimeUnit(CurrentTimeUnit {
            current_time_unit_token,
            empty_parens,
        }) = expr
        {
            match (self.want_parens, empty_parens.is_some()) {
                (true, false) => {
                    let replacement = sql_quote! { #current_time_unit_token () }
                        .try_into_expression()
                        .expect("generated SQL should always parse");
                    *expr = replacement;
                }
                (false, true) => *empty_parens = None,
                _ => { /* nothing to change */ }
            }
        }
    }
}

impl Transform for StandardizeCurrentTimeUnit {
    fn name(&self) -> &'static str {
        "StandardizeCurrentTimeUnit"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
