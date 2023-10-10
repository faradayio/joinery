//! Various routines to analyze an [`SqlProgram`]. We rely heavily on
//! [`derive_visitor`] to traverse the AST.

use std::collections::HashMap;

use derive_visitor::{Drive, Visitor};

use crate::ast::{FunctionName, SpecialDateFunctionCall, SqlProgram};

/// Count all the function calls in a [`SqlProgram`].
#[derive(Debug, Default, Visitor)]
#[visitor(FunctionName(enter), SpecialDateFunctionCall(enter))]
pub struct FunctionCallCounts {
    counts: HashMap<String, usize>,
}

impl FunctionCallCounts {
    /// Find all the function calls in a [`SqlProgram`].
    pub fn visit(&mut self, sql_program: &SqlProgram) {
        sql_program.drive(self)
    }

    fn record_call(&mut self, name: &str) {
        let count = self.counts.entry(name.to_ascii_uppercase()).or_default();
        *count += 1;
    }

    fn enter_function_name(&mut self, function_name: &FunctionName) {
        self.record_call(&function_name.unescaped_bigquery());
    }

    fn enter_special_date_function_call(
        &mut self,
        special_date_function_call: &SpecialDateFunctionCall,
    ) {
        self.record_call(
            special_date_function_call
                .function_name
                .unescaped_bigquery(),
        );
    }

    /// Get a list of functions and how often they were called, sorted by
    /// decreasing frequency.
    pub fn counts(&self) -> Vec<(&str, usize)> {
        let mut counts: Vec<_> = self.counts.iter().map(|(f, c)| (f.as_str(), *c)).collect();
        counts.sort_by(|(_, c1), (_, c2)| c2.cmp(c1));
        counts
    }
}
