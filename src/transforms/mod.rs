//! Ways to transform an [`ast::SqlProgram`] into another `ast::SqlProgram`.
//!
//! These work entirely in terms of parsed BigQuery SQL. Normally, the ideal
//! transform should take a `SqlProgram` containing BigQuery-specific features,
//! and return one that uses the equivalent standard SQL features. This allows
//! us to use the same transforms for as many databases as possible.

use crate::{ast, errors::Result};

pub use self::{
    clean_up_temp_manually::CleanUpTempManually,
    or_replace_to_drop_if_exists::OrReplaceToDropIfExists,
    rename_functions::{RenameFunctions, Udf},
};

mod clean_up_temp_manually;
mod or_replace_to_drop_if_exists;
mod rename_functions;

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
