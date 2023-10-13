//! Database drivers.

use std::{collections::VecDeque, fmt, str::FromStr};

use async_trait::async_trait;

use crate::{
    ast::{Emit, Target},
    errors::{format_err, Error, Result},
};

use self::snowflake::{SnowflakeLocator, SNOWFLAKE_LOCATOR_PREFIX};
use self::sqlite3::{SQLite3Locator, SQLITE3_LOCATOR_PREFIX};

pub mod bigquery;
pub mod snowflake;
pub mod sqlite3;

/// A URL-like locator for a database.
#[async_trait]
pub trait Locator: fmt::Display + fmt::Debug + Send + Sync + 'static {
    /// Get the target for this locator.
    fn target(&self) -> Target;

    /// Get the driver for this locator.
    async fn driver(&self) -> Result<Box<dyn Driver>>;
}

impl FromStr for Box<dyn Locator> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let colon_pos = s
            .find(':')
            .ok_or_else(|| format_err!("could not find scheme for locator: {}", s))?;
        let prefix = &s[..colon_pos + 1];
        match prefix {
            SQLITE3_LOCATOR_PREFIX => Ok(Box::new(s.parse::<SQLite3Locator>()?)),
            SNOWFLAKE_LOCATOR_PREFIX => Ok(Box::new(s.parse::<SnowflakeLocator>()?)),
            _ => Err(format_err!("unsupported database type: {}", s)),
        }
    }
}

/// A type that supports basic equality and display, used for comparing
/// test results.
pub trait Comparable: fmt::Debug + fmt::Display + PartialEq + Send + Sync {}
impl<T> Comparable for T where T: fmt::Debug + fmt::Display + PartialEq + Send + Sync {}

/// A database driver. This is mostly used for running SQL tests.
///
/// This trait is ["object safe"][safe], so it can be used as `Box<dyn Driver>`
/// without knowing the concrete type. This means it can't have associated types
/// like `Self::Type` or `Self::Value`, so we have [`DriverImpl`] for that.
///
/// [safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
#[async_trait]
pub trait Driver: Send + Sync + 'static {
    /// The target for this database.
    fn target(&self) -> Target;

    /// Execute a single SQL statement, using native SQL for this database. This
    /// is only guaranteed to work if passed a single statement, although some
    /// drivers may support multiple statements. Resources created using `CREATE
    /// TEMP TABLE`, etc., may not persist across calls.
    async fn execute_native_sql_statement(&mut self, sql: &str) -> Result<()>;

    /// Execute a query represented as an AST. This can execute multiple
    /// statements.
    async fn execute_ast(&mut self, ast: &crate::ast::SqlProgram) -> Result<()> {
        for statement in &ast.statements {
            let sql = statement.emit_to_string(self.target());
            self.execute_native_sql_statement(&sql).await?;
        }
        Ok(())
    }

    /// Drop a table if it exists.
    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()>;

    /// Compare two tables for equality. This is done by querying all rows
    /// from both tables, sorted by all columns, and comparing the results.
    ///
    /// This is implemented by [`DriverImpl::compare_tables_impl`].
    async fn compare_tables(&mut self, result_table: &str, expected_table: &str) -> Result<()>;
}

/// Extensions to [`Driver`] that are not ["object safe"][safe].
///
/// [safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
#[async_trait]
pub trait DriverImpl {
    /// A native data type for this database.
    type Type: Comparable;

    /// A native value for this database.
    type Value: Comparable;

    /// An iterator over the rows of a table.
    type Rows: Iterator<Item = Result<Vec<Self::Value>>> + Send + Sync;

    /// Columns of a table.
    async fn table_columns(&mut self, table_name: &str) -> Result<Vec<Column<Self::Type>>>;

    /// Query all rows from a table.
    async fn query_table_sorted(
        &mut self,
        table_name: &str,
        columns: &[Column<Self::Type>],
    ) -> Result<Self::Rows>;

    /// Implement [`Driver::compare_tables`] for this database.
    async fn compare_tables_impl(
        &mut self,
        result_table: &str,
        expected_table: &str,
    ) -> Result<()> {
        // Get our columns.
        let result_columns = self.table_columns(result_table).await?;
        let expected_columns = self.table_columns(expected_table).await?;

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
        let mut result_rows = self
            .query_table_sorted(result_table, &result_columns)
            .await?;
        let mut expected_rows = self
            .query_table_sorted(expected_table, &expected_columns)
            .await?;
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
                            expected_table, result_table, diff
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
#[derive(Clone, Debug, PartialEq)]
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