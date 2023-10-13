//! Various routines to analyze an [`SqlProgram`]. We rely heavily on
//! [`derive_visitor`] to traverse the AST.

use std::collections::HashMap;

use derive_visitor::{Drive, Visitor};

use crate::ast::{FunctionCall, SpecialDateFunctionCall, SqlProgram};

/// A `phf` set of functions that are known to take any number of arguments.
static KNOWN_VARARG_FUNCTIONS: phf::Set<&'static str> = phf::phf_set! {
    "COALESCE", "CONCAT",
};

/// Count all the function calls in a [`SqlProgram`].
#[derive(Debug, Default, Visitor)]
#[visitor(FunctionCall(enter), SpecialDateFunctionCall(enter))]
pub struct FunctionCallCounts {
    counts: HashMap<String, usize>,
}

impl FunctionCallCounts {
    /// Find all the function calls in a [`SqlProgram`].
    pub fn visit(&mut self, sql_program: &SqlProgram) {
        sql_program.drive(self)
    }

    fn record_call(&mut self, name: String) {
        let count = self.counts.entry(name).or_default();
        *count += 1;
    }

    fn enter_function_call(&mut self, function_call: &FunctionCall) {
        let base_name = function_call.name.unescaped_bigquery().to_ascii_uppercase();
        let mut name = format!("{}(", base_name);
        if KNOWN_VARARG_FUNCTIONS.contains(base_name.as_str()) {
            name.push('*');
        } else {
            // Push '_' separated by ','.
            for i in 0..function_call.args.nodes.len() {
                if i > 0 {
                    name.push(',');
                }
                name.push('_');
            }
        }
        name.push(')');
        if function_call.over_clause.is_some() {
            name.push_str(" OVER(..)");
        }
        self.record_call(name);
    }

    fn enter_special_date_function_call(
        &mut self,
        special_date_function_call: &SpecialDateFunctionCall,
    ) {
        let mut name = format!(
            "{}(",
            special_date_function_call
                .function_name
                .unescaped_bigquery()
                .to_ascii_uppercase(),
        );
        for i in 0..special_date_function_call.args.nodes.len() {
            if i > 0 {
                name.push(',');
            }
            name.push('_');
        }
        name.push_str(") (special)");
        self.record_call(name);
    }

    /// Get a list of functions and how often they were called, sorted by
    /// decreasing frequency.
    pub fn counts(&self) -> Vec<(&str, usize)> {
        let mut counts: Vec<_> = self.counts.iter().map(|(f, c)| (f.as_str(), *c)).collect();
        counts.sort_by(|(_, c1), (_, c2)| c2.cmp(c1));
        counts
    }
}
