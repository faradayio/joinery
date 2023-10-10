//! Database drivers.

use std::{collections::VecDeque, fmt};

use crate::{
    ast::{Emit, Target},
    errors::{format_err, Error, Result},
};

use self::sqlite3::SQLite3Driver;

mod sqlite3;

/// A type that supports basic equality and display, used for comparing
/// test results.
pub trait Comparable: fmt::Debug + fmt::Display + PartialEq {}
impl<T> Comparable for T where T: fmt::Debug + fmt::Display + PartialEq {}

/// Get the driver for the given target.
pub fn driver_for_target(target: Target) -> Result<Box<dyn Driver>> {
    match target {
        Target::SQLite3 => Ok(Box::new(SQLite3Driver::memory()?)),
        _ => Err(format_err!("no driver for target: {:?}", target)),
    }
}

/// A database driver. This is mostly used for running SQL tests.
///
/// This trait is ["object safe"][safe], so it can be used as `Box<dyn Driver>`
/// without knowing the concrete type. This means it can't have associated types
/// like `Self::Type` or `Self::Value`, so we have [`DriverImpl`] for that.
///
/// [safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
pub trait Driver {
    /// The target for this database.
    fn target(&self) -> Target;

    /// Execute a SQL query, using native SQL for this database. This may
    /// contain multiple statements separated by semicolons.
    fn execute_native_sql(&self, sql: &str) -> Result<()>;

    /// Execute a query represented as an AST.
    fn execute_ast(&self, ast: &crate::ast::SqlProgram) -> Result<()> {
        let sql = ast.emit_to_string(self.target());
        self.execute_native_sql(&sql)
    }

    /// Compare two tables for equality. This is done by querying all rows
    /// from both tables, sorted by all columns, and comparing the results.
    ///
    /// This is implemented by [`DriverImpl::compare_tables_impl`].
    fn compare_tables(&self, result_table: &str, expected_table: &str) -> Result<()>;
}

/// Extensions to [`Driver`] that are not ["object safe"][safe].
///
/// [safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
pub trait DriverImpl {
    /// A native data type for this database.
    type Type: Comparable;

    /// A native value for this database.
    type Value: Comparable;

    /// An iterator over the rows of a table.
    type Rows: Iterator<Item = Result<Vec<Self::Value>>>;

    /// Columns of a table.
    fn table_columns(&self, table_name: &str) -> Result<Vec<Column<Self::Type>>>;

    /// Query all rows from a table.
    fn query_table_sorted(
        &self,
        table_name: &str,
        columns: &[Column<Self::Type>],
    ) -> Result<Self::Rows>;

    /// Implement [`Driver::compare_tables`] for this database.
    fn compare_tables_impl(&self, result_table: &str, expected_table: &str) -> Result<()> {
        // Get our columns.
        let result_columns = self.table_columns(result_table)?;
        let expected_columns = self.table_columns(expected_table)?;

        // Compare our columns by name, but not type, because the types of
        // columns built from `SELECT` expressions may be less strict than
        // the types of columns in the expected table.
        let result_column_names = result_columns.iter().map(|c| &c.name).collect::<Vec<_>>();
        let expected_column_names = expected_columns.iter().map(|c| &c.name).collect::<Vec<_>>();
        if result_column_names != expected_column_names {
            return Err(format_err!(
                "result table has different columns from expected table: {} vs {}",
                vec_to_string(&result_columns),
                vec_to_string(&expected_columns)
            ));
        }

        // Keep a history of recent rows, so we can report errors with context.
        const HISTORY_SIZE: usize = 3;
        let mut result_history: VecDeque<Vec<Self::Value>> = VecDeque::with_capacity(HISTORY_SIZE);

        // Query rows from both tables, sorted by all columns.
        let mut result_rows = self.query_table_sorted(result_table, &result_columns)?;
        let mut expected_rows = self.query_table_sorted(expected_table, &expected_columns)?;
        let mut row_count = 0usize;
        loop {
            match (result_rows.next(), expected_rows.next()) {
                (Some(Ok(result_row)), Some(Ok(expected_row))) => {
                    if result_row != expected_row {
                        // Build a short diff of recent rows.
                        let mut diff = String::new();
                        for row in result_history.iter() {
                            diff.push_str(&format!("  {}\n", vec_to_string(row)));
                        }
                        diff.push_str(&format!("- {}\n", vec_to_string(&expected_row)));
                        diff.push_str(&format!("+ {}\n", vec_to_string(&result_row)));

                        return Err(Error::tables_not_equal(format!(
                            "row from {} does not match row from {}:\n{}",
                            result_table, expected_table, diff
                        )));
                    }
                    row_count += 1;

                    // Update our history.
                    if result_history.len() == HISTORY_SIZE {
                        result_history.pop_front();
                    }
                    result_history.push_back(result_row);
                }
                (Some(Err(e)), _) => return Err(e),
                (_, Some(Err(e))) => return Err(e),
                (Some(Ok(_)), None) => {
                    return Err(Error::tables_not_equal(format!(
                        "{} has more rows than {} ({}+ vs {})",
                        result_table,
                        expected_table,
                        row_count + 1,
                        row_count
                    )))
                }
                (None, Some(Ok(_))) => {
                    return Err(Error::tables_not_equal(format!(
                        "{} has fewer rows than {} ({} vs {}+)",
                        result_table,
                        expected_table,
                        row_count,
                        row_count + 1
                    )))
                }
                (None, None) => break,
            }
        }

        Ok(())
    }
}

/// A database column.
#[derive(Debug, PartialEq)]
pub struct Column<Type: Comparable> {
    /// The name of the column.
    pub name: String,

    /// The type of the column.
    pub ty: Type,
}

impl<Type: Comparable> fmt::Display for Column<Type> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.ty)
    }
}

/// Display as a list of comma-separated values.
pub fn vec_to_string(row: &[impl fmt::Display]) -> String {
    let mut s = String::new();
    for (i, value) in row.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("{}", value));
    }
    s
}
