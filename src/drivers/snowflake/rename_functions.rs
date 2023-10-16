//! A simple tree-walker that renames functions to their Snowflake equivalents.

use std::collections::HashMap;

use derive_visitor::VisitorMut;

use crate::ast::{FunctionName, Identifier};

// A `phf_map!` of BigQuery function names to Snowflake function names. Use
// this for simple renaming.
static FUNCTION_NAMES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "REGEXP_EXTRACT" => "REGEXP_SUBSTR",
    "SHA256" => "SHA2_BINARY", // Second argument defaults to SHA256.
};

/// A Snowflake UDF (user-defined function).
pub struct Udf {
    pub decl: &'static str,
    pub sql: &'static str,
}

impl Udf {
    /// Generate the SQL to create this UDF.
    pub fn to_sql(&self) -> String {
        format!(
            "CREATE OR REPLACE TEMP FUNCTION {} AS $$\n{}\n$$\n",
            self.decl, self.sql
        )
    }
}

/// A `phf_map!` of BigQuery UDF names to Snowflake UDFs. Use this when we
/// actually need to create a UDF as a helper function.
static UDFS: phf::Map<&'static str, &'static Udf> = phf::phf_map! {
    "TO_HEX" => &Udf { decl: "TO_HEX(b BINARY) RETURNS STRING", sql: "HEX_ENCODE(b, 0)" },
};

#[derive(Default, VisitorMut)]
#[visitor(FunctionName(enter))]
pub struct RenameFunctions {
    // UDFs that we need to create, if we haven't already.
    pub udfs: HashMap<String, &'static Udf>,
}

impl RenameFunctions {
    fn enter_function_name(&mut self, function_name: &mut FunctionName) {
        if let FunctionName::Function { function } = function_name {
            let name = function.unescaped_bigquery().to_ascii_uppercase();
            if let Some(snowflake_name) = FUNCTION_NAMES.get(&name) {
                // Rename the function.
                let orig_ident = function_name.function_identifier();
                *function_name = FunctionName::Function {
                    function: Identifier {
                        token: orig_ident.token.with_token_str(snowflake_name),
                        text: snowflake_name.to_string(),
                    },
                };
            } else if let Some(udf) = UDFS.get(&name) {
                // We'll need a UDF, so add it to our list it if isn't already
                // there.
                self.udfs.insert(name, udf);
            }
        }
    }
}
