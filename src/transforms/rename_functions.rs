//! A simple tree-walker that renames functions to their Snowflake equivalents.

use std::collections::HashMap;

use derive_visitor::{DriveMut, VisitorMut};

use crate::{
    ast::{self, CurrentTimeUnit, FunctionCall, Name},
    errors::Result,
    tokenizer::{Ident, Spanned, TokenStream},
};

use super::{Transform, TransformExtra};

/// A custom UDF (user-defined function).
pub struct Udf {
    pub decl: &'static str,
    pub sql: &'static str,
}

/// Given a `FunctionCall`, return a rewritten `TokenStream` that implements the
/// function in the target dialect. This is basically a proc macro for SQL
/// function calls.
pub type FunctionCallRewriter = &'static dyn Fn(&FunctionCall) -> TokenStream;

/// A builder type for [`RenameFunctions`].
pub struct RenameFunctionsBuilder {
    function_table: &'static phf::Map<&'static str, &'static str>,
    udf_table: &'static phf::Map<&'static str, &'static Udf>,
    format_udf: &'static dyn Fn(&Udf) -> String,
    function_call_rewriters: HashMap<String, FunctionCallRewriter>,
}

impl RenameFunctionsBuilder {
    /// Create a new `RenameFunctionsBuilder`.
    pub fn new(function_table: &'static phf::Map<&'static str, &'static str>) -> Self {
        static EMPTY_UDFS: phf::Map<&'static str, &'static Udf> = phf::phf_map! {};
        Self {
            function_table,
            udf_table: &EMPTY_UDFS,
            format_udf: &|_| "".to_string(),
            function_call_rewriters: HashMap::new(),
        }
    }

    /// Set the UDF table and formatter, for databases that support
    /// `WITH FUNCTION`-like syntax.
    pub fn udf_table(
        mut self,
        udf_table: &'static phf::Map<&'static str, &'static Udf>,
        format_udf: &'static dyn Fn(&Udf) -> String,
    ) -> Self {
        self.udf_table = udf_table;
        self.format_udf = format_udf;
        self
    }

    /// Add a function call rewriter.
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
            udf_table: self.udf_table,
            format_udf: self.format_udf,
            udfs: HashMap::new(),
            function_call_rewriters: self.function_call_rewriters,
        }
    }
}

#[derive(VisitorMut)]
#[visitor(CurrentTimeUnit(enter), FunctionCall(enter))]
pub struct RenameFunctions {
    // Lookup table containing function replacements.
    function_table: &'static phf::Map<&'static str, &'static str>,

    // Lookup table containing UDF replacements.
    udf_table: &'static phf::Map<&'static str, &'static Udf>,

    // Format a UDF.
    format_udf: &'static dyn Fn(&Udf) -> String,

    // UDFs that we need to create, if we haven't already.
    udfs: HashMap<String, &'static Udf>,

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
        } else if let Some(udf) = self.udf_table.get(&name) {
            // We'll need a UDF, so add it to our list it if isn't already
            // there.
            self.udfs.insert(name, udf);
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
        let mut extra_sql = vec![];
        for udf in self.udfs.values() {
            extra_sql.push((self.format_udf)(udf));
        }
        Ok(TransformExtra {
            native_setup_sql: extra_sql,
            native_teardown_sql: vec![],
        })
    }
}
