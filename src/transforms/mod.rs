//! Ways to transform an [`ast::SqlProgram`] into another `ast::SqlProgram`.
//!
//! These work entirely in terms of parsed BigQuery SQL. Normally, the ideal
//! transform should take a `SqlProgram` containing BigQuery-specific features,
//! and return one that uses the equivalent standard SQL features. This allows
//! us to use the same transforms for as many databases as possible.

use crate::{ast, errors::Result};

pub use self::{
    bool_to_int::BoolToInt,
    clean_up_temp_manually::CleanUpTempManually,
    countif_to_case::CountifToCase,
    if_to_case::IfToCase,
    in_unnest_to_in_select::InUnnestToInSelect,
    index_from_one::IndexFromOne,
    index_from_zero::IndexFromZero,
    is_bool_to_case::IsBoolToCase,
    or_replace_to_drop_if_exists::OrReplaceToDropIfExists,
    rename_functions::{RenameFunctions, Udf},
    standardize_current_date::StandardizeCurrentDate,
    wrap_nested_queries::WrapNestedQueries,
};

mod bool_to_int;
mod clean_up_temp_manually;
mod countif_to_case;
mod if_to_case;
mod in_unnest_to_in_select;
mod index_from_one;
mod index_from_zero;
mod is_bool_to_case;
mod or_replace_to_drop_if_exists;
mod rename_functions;
mod standardize_current_date;
mod wrap_nested_queries;

/// A transform that modifies an [`SqlProgram`].
pub trait Transform {
    /// Apply this transform to an [`SqlProgram`].
    ///
    /// Returns a list of extra SQL statements that need to be executed before
    /// the transformed program. These statements must be in the target dialect
    /// of SQL, and typically include custom UDFs or similar temporary
    /// definitions.
    ///
    /// A transform should only be used once, as it may modify itself in the
    /// process of transforming the AST. To enforce this, the transform takes
    /// `self: Box<Self>` rather than `&mut self`.
    fn transform(self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra>;
}

/// Extra SQL returned by a [`Transform`].
#[derive(Debug, Default)]
pub struct TransformExtra {
    /// Individual statements that should be run before the transformed program.
    pub native_setup_sql: Vec<String>,

    /// Individual statements that should be run after the transformed program,
    /// even if it fails. These may individually fail.
    pub native_teardown_sql: Vec<String>,
}

impl TransformExtra {
    /// Merge in another `TransformExtra`.
    pub fn extend(&mut self, other: TransformExtra) {
        self.native_setup_sql.extend(other.native_setup_sql);
        self.native_teardown_sql.extend(other.native_teardown_sql);
    }
}
