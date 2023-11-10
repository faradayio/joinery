use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, SpecialDateFunctionCall},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Deal with add the `DATETIME_DIFF` functions and convert them to Trino
/// equivalents.
#[derive(VisitorMut)]
#[visitor(SpecialDateFunctionCall(enter))]
pub struct SpecialDateFunctionsToTrino;

impl SpecialDateFunctionsToTrino {
    fn enter_special_date_function_call(&mut self, expr: &mut SpecialDateFunctionCall) {
        let ast::SpecialDateFunctionCall {
            function_name,
            paren1,
            args,
            paren2,
        } = expr;
        let ident = &function_name.ident;
        let func_name = ident.name.to_uppercase();
        let args = args.node_iter_mut().collect::<Vec<_>>();
        match &func_name[..] {
            // - BigQuery order: DATE_DIFF(date1, date2, date_part)
            // - Trino order: DATE_DIFF(date_part, date2, date1)
            "DATE_DIFF" | "DATETIME_DIFF" => {
                let ident = ident.with_str("DATE_DIFF");
                let date1 = args[0]
                    .try_as_expression()
                    .expect("arg type should be enforced by type checker");
                let date2 = args[1]
                    .try_as_expression()
                    .expect("arg type should be enforced by type checker");
                let date_part = args[2]
                    .try_as_date_part()
                    .expect("arg type should be enforced by type checker")
                    .to_literal();
                *expr = sql_quote! {
                    #ident #paren1 #date_part, #date2, #date1 #paren2
                }
                .try_into_special_date_function_call()
                .expect("generated SQL should always parse");
            }
            // - BigQuery order: DATE_TRUNC(date, date_part)
            // - Trino order: DATE_TRUNC(date_part, date)
            "DATE_TRUNC" | "DATETIME_TRUNC" => {
                let ident = ident.with_str("DATE_TRUNC");
                let date = args[0]
                    .try_as_expression()
                    .expect("arg type should be enforced by type checker");
                let date_part = args[1]
                    .try_as_date_part()
                    .expect("arg type should be enforced by type checker")
                    .to_literal();
                *expr = sql_quote! {
                    #ident #paren1 #date_part, #date #paren2
                }
                .try_into_special_date_function_call()
                .expect("generated SQL should always parse");
            }
            // - BigQuery order: DATE_ADD(date, INTERVAL literal date_part)
            // - Trino order: DATE_ADD(date_part, literal, date)
            "DATE_ADD" | "DATETIME_ADD" => {
                let ident = ident.with_str("DATE_ADD");
                let date = args[0]
                    .try_as_expression()
                    .expect("arg type should be enforced by type checker");
                let interval = args[1]
                    .try_as_interval()
                    .expect("arg type should be enforced by type checker");
                let literal = &interval.number;
                let date_part = interval.date_part.to_literal();
                *expr = sql_quote! {
                    #ident #paren1 #date_part, #literal, #date #paren2
                }
                .try_into_special_date_function_call()
                .expect("generated SQL should always parse");
            }
            // Like `DATE_ADD`, but we negate the interval.
            "DATE_SUB" | "DATETIME_SUB" => {
                let ident = ident.with_str("DATE_ADD");
                let date = args[0]
                    .try_as_expression()
                    .expect("arg type should be enforced by type checker");
                let interval = args[1]
                    .try_as_interval()
                    .expect("arg type should be enforced by type checker");
                let literal = &interval.number;
                let date_part = interval.date_part.to_literal();
                *expr = sql_quote! {
                    #ident #paren1 #date_part, -#literal, #date #paren2
                }
                .try_into_special_date_function_call()
                .expect("generated SQL should always parse");
            }
            _ => {}
        }
    }
}

impl Transform for SpecialDateFunctionsToTrino {
    fn name(&self) -> &'static str {
        "SpecialDateFunctionsToTrino"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        Ok(TransformExtra::default())
    }
}
