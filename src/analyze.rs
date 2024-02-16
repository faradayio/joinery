//! Various routines to analyze an [`SqlProgram`]. We rely heavily on
//! [`derive_visitor`] to traverse the AST.

use std::collections::HashMap;

use derive_visitor::{Drive, Visitor};

use crate::{
    ast::{FunctionCall, Name, Node, NodeVec, SpecialDateFunctionCall, SqlProgram},
    scope::{Scope, ScopeGet, ScopeHandle},
    tokenizer::Span,
};

/// A `phf` set of functions that are known to take any number of arguments.
static KNOWN_VARARG_FUNCTIONS: phf::Set<&'static str> = phf::phf_set! {
    "COALESCE", "CONCAT", "GREATEST", "LEAST",
};

/// Count all the function calls in a [`SqlProgram`].
#[derive(Debug, Visitor)]
#[visitor(FunctionCall(enter), SpecialDateFunctionCall(enter))]
pub struct FunctionCallCounts {
    root_scope: ScopeHandle,
    counts: HashMap<String, usize>,
}

impl FunctionCallCounts {
    /// Find all the function calls in a [`SqlProgram`].
    pub fn visit(&mut self, sql_program: &SqlProgram) {
        sql_program.drive(self)
    }

    /// Return true if we have at least one signature for a function which
    /// could be called with the given number of arguments.
    fn is_known_function_and_airty<T: Node>(&self, name: &str, args: &NodeVec<T>) -> bool {
        let name = Name::new(name, Span::Unknown);
        let ftype = match self.root_scope.get_function_type(&name) {
            Ok(ftype) => ftype,
            Err(_) => return false,
        };
        let arg_count = args.node_iter().count();
        ftype.could_be_called_with_arg_count(arg_count)
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
            for (i, _) in function_call.args.node_iter().enumerate() {
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
        if !self.is_known_function_and_airty(&base_name, &function_call.args) {
            name.push_str(" (UNKNOWN)");
        }
        self.record_call(name);
    }

    fn enter_special_date_function_call(
        &mut self,
        special_date_function_call: &SpecialDateFunctionCall,
    ) {
        let base_name = special_date_function_call
            .function_name
            .ident
            .name
            .to_ascii_uppercase();
        let mut name = format!("{}(", base_name);
        for (i, _) in special_date_function_call.args.node_iter().enumerate() {
            if i > 0 {
                name.push(',');
            }
            name.push('_');
        }
        name.push_str(") (special)");
        if !self.is_known_function_and_airty(&base_name, &special_date_function_call.args) {
            name.push_str(" (UNKNOWN)");
        }
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

impl Default for FunctionCallCounts {
    fn default() -> Self {
        Self {
            root_scope: Scope::root(),
            counts: HashMap::new(),
        }
    }
}
