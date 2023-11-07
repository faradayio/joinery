//! A simple tree-walker that renames functions to their Snowflake equivalents.

use std::collections::HashMap;

use derive_visitor::{DriveMut, VisitorMut};

use crate::{
    ast::{self, FunctionCall, Name},
    errors::Result,
    tokenizer::Spanned,
};

use super::{Transform, TransformExtra};

/// A Snowflake UDF (user-defined function).
pub struct Udf {
    pub decl: &'static str,
    pub sql: &'static str,
}

#[derive(VisitorMut)]
#[visitor(FunctionCall(enter))]
pub struct RenameFunctions {
    // Lookup table containing function replacements.
    function_table: &'static phf::Map<&'static str, &'static str>,

    // Lookup table containing UDF replacements.
    udf_table: &'static phf::Map<&'static str, &'static Udf>,

    // Format a UDF.
    format_udf: &'static dyn Fn(&Udf) -> String,

    // UDFs that we need to create, if we haven't already.
    udfs: HashMap<String, &'static Udf>,
}

impl RenameFunctions {
    /// Create a new `RenameFunctions` visitor.
    pub fn new(
        function_table: &'static phf::Map<&'static str, &'static str>,
        udf_table: &'static phf::Map<&'static str, &'static Udf>,
        format_udf: &'static dyn Fn(&Udf) -> String,
    ) -> Self {
        Self {
            function_table,
            udf_table,
            format_udf,
            udfs: HashMap::new(),
        }
    }

    fn enter_function_call(&mut self, function_call: &mut FunctionCall) {
        let name = function_call.name.unescaped_bigquery().to_ascii_uppercase();
        if let Some(&new_name) = self.function_table.get(&name) {
            // Rename the function.
            //
            // TODO: Preserve whitespace and source location.
            function_call.name = Name::new(new_name, function_call.name.span());
        } else if let Some(udf) = self.udf_table.get(&name) {
            // We'll need a UDF, so add it to our list it if isn't already
            // there.
            self.udfs.insert(name, udf);
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
