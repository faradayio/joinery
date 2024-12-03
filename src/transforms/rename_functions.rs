//! A simple tree-walker that renames functions to their target equivalents.

use std::collections::HashMap;

use derive_visitor::{DriveMut, VisitorMut};

use crate::{
    ast::{self, CurrentTimeUnit, FunctionCall, Name},
    errors::Result,
    tokenizer::{Ident, Spanned, TokenStream},
};

use super::{Transform, TransformExtra};

/// Given a `FunctionCall`, return a rewritten `TokenStream` that implements the
/// function in the target dialect. This is basically a proc macro for SQL
/// function calls.
pub type FunctionCallRewriter = &'static dyn Fn(&FunctionCall) -> TokenStream;

/// A builder type for [`RenameFunctions`].
pub struct RenameFunctionsBuilder {
    function_table: &'static phf::Map<&'static str, &'static str>,
    function_call_rewriters: HashMap<String, FunctionCallRewriter>,
}

impl RenameFunctionsBuilder {
    /// Create a new `RenameFunctionsBuilder`.
    pub fn new(function_table: &'static phf::Map<&'static str, &'static str>) -> Self {
        Self {
            function_table,
            function_call_rewriters: HashMap::new(),
        }
    }

    /// Add a function call rewriter.
    ///
    /// WARNING: This should be a last resort, after trying [`Self::udf_table`],
    /// or defining an SQL UDF and passing it to [`Self::new`] as part of the
    /// `function_table`. Mostly this should be reserved for rewriting aggregate
    /// functions, or for databases that don't support SQL UDFs at all.
    pub fn rewrite_function_call(
        mut self,
        name: &'static str,
        rewriter: FunctionCallRewriter,
    ) -> Self {
        self.function_call_rewriters
            .insert(name.to_ascii_uppercase(), rewriter);
        self
    }

    /// Build a `RenameFunctions` transform.
    pub fn build(self) -> RenameFunctions {
        RenameFunctions {
            function_table: self.function_table,
            function_call_rewriters: self.function_call_rewriters,
        }
    }
}

#[derive(VisitorMut)]
#[visitor(CurrentTimeUnit(enter), FunctionCall(enter))]
pub struct RenameFunctions {
    // Lookup table containing function replacements.
    function_table: &'static phf::Map<&'static str, &'static str>,

    // Function call rewriters.
    function_call_rewriters: HashMap<String, FunctionCallRewriter>,
}

impl RenameFunctions {
    /// Allow renaming CURRENT_DATETIME, etc.
    fn enter_current_time_unit(&mut self, current_time_unit: &mut ast::CurrentTimeUnit) {
        let ident = &current_time_unit.current_time_unit_token.ident;
        let name = ident.token.as_str().to_ascii_uppercase();
        if let Some(&new_name) = self.function_table.get(&name) {
            // Rename the function.
            current_time_unit.current_time_unit_token.ident = Ident::new(new_name, ident.span());
        }
    }

    fn enter_function_call(&mut self, function_call: &mut FunctionCall) {
        let name = function_call.name.unescaped_bigquery().to_ascii_uppercase();
        if let Some(&new_name) = self.function_table.get(&name) {
            // Rename the function.
            //
            // TODO: Preserve whitespace and source location.
            function_call.name = Name::new_from_dotted(new_name, function_call.name.span());
        } else if let Some(rewriter) = self.function_call_rewriters.get(&name) {
            // Rewrite the function call.
            let token_stream = rewriter(function_call);
            let new_function_call = token_stream
                .try_into_function_call()
                .expect("could not parse rewritten function call");
            *function_call = new_function_call;
        }
    }
}

impl Transform for RenameFunctions {
    fn name(&self) -> &'static str {
        "RenameFunctions"
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        // Walk the AST, renaming functions and collecting UDFs.
        sql_program.drive_mut(self.as_mut());

        // Create any UDFs that we need.
        Ok(TransformExtra {
            native_setup_sql: vec![],
            native_teardown_sql: vec![],
        })
    }
}
