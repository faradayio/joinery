use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Qualify, SelectExpression},
    errors::Result,
    tokenizer::{Ident, Spanned},
    unique_names::unique_name,
};

use super::{Transform, TransformExtra};

/// Transform `QUALIFY` into a subquery.
#[derive(VisitorMut)]
#[visitor(SelectExpression(enter))]
pub struct QualifyToSubquery;

impl QualifyToSubquery {
    fn enter_select_expression(&mut self, expr: &mut SelectExpression) {
        if let SelectExpression {
            select_list,
            qualify:
                Some(Qualify {
                    expression: qualify_expr,
                    ..
                }),
            ..
        } = expr
        {
            // Add a new SELECT list item for the QUALIFY expression, and
            // remove the QUALIFY from the original SELECT.
            let new_name = Ident::new(&unique_name(), qualify_expr.span());
            let select_list_item = sql_quote! {
                #qualify_expr AS #new_name
            }
            .try_into_select_list_item()
            .expect("should be valid SQL");
            select_list.items.push(select_list_item);

            // Remove the QUALIFY from the original SELECT. This has to happen after
            // the manipulate above, because we need to drop a bunch of `&mut` before
            // we can change `expr`.
            expr.qualify = None;

            // Make a copy of our SELECT expression.
            let nested_expr = expr.clone();

            // Rewrite the original SELECT expression to be a subquery. The
            // `#nested_expr` used here is a placeholder; we'll patch it up
            // later with one that retains type information.
            let new_expr = sql_quote! {
                SELECT * EXCEPT (#new_name)
                FROM (#nested_expr)
                WHERE #new_name
            }
            .try_into_select_expression()
            .expect("should be valid SQL");

            // Install our new SELECT expression.
            *expr = new_expr;
        }
    }
}

impl Transform for QualifyToSubquery {
    fn name(&self) -> &'static str {
        "QualifyToSubquery"
    }

    fn requires_types(&self) -> bool {
        true
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
