//! Database drivers.

use crate::{
    ast::{parse_sql, Emit, Target},
    errors::{format_err, Error, Result},
};

use self::sqlite3::SQLite3Driver;

mod sqlite3;

/// Get the driver for the given target.
pub fn driver_for_target(target: Target) -> Result<Box<dyn Driver>> {
    match target {
        Target::SQLite3 => Ok(Box::new(SQLite3Driver::memory()?)),
        _ => Err(format_err!("no driver for target: {:?}", target)),
    }
}

/// A database driver. This is mostly used for running SQL tests.
pub trait Driver {
    /// The target for this database.
    fn target(&self) -> Target;

    /// Execute a SQL query, using native SQL for this database. This may
    /// contain multiple statements separated by semicolons.
    fn execute_native_sql(&self, sql: &str) -> Result<()>;

    /// Execute BigQuery SQL.
    fn execute_bigquery_sql(&self, sql: &str) -> Result<()> {
        let ast = parse_sql(sql)?;
        self.execute_ast(&ast)
    }

    /// Execute a query represented as an AST.
    fn execute_ast(&self, ast: &crate::ast::SqlProgram) -> Result<()> {
        let sql = ast.emit_to_string(self.target());
        self.execute_native_sql(&sql)
    }
}
