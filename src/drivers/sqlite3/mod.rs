//! Our SQLite3 driver.

use std::{fmt, vec};

use rusqlite::{functions::FunctionFlags, types};

use crate::{
    ast::Target,
    errors::{Context, Error, Result},
};

use self::unnest::register_unnest;

use super::{Column, Driver, DriverImpl};

mod unnest;

/// Our SQLite3 driver.
pub struct SQLite3Driver {
    conn: rusqlite::Connection,
}

impl SQLite3Driver {
    /// Create an in-memory SQLite3 database.
    pub fn memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()
            .context("could not open SQLite3 memory database")?;

        // Install our dummy `UNNEST` virtual table. This does nothing useful
        // yet, but it allows us to parse and execute queries that use `UNNEST`.
        register_unnest(&conn).expect("failed to register UNNEST");

        // Install some dummy functions that always return NULL.
        let dummy_fns = &[
            ("array", -1),
            ("array_index", 2),
            ("current_datetime", 0),
            ("date_diff", 3),
            ("date_trunc", 2),
            ("datetime_diff", 3),
            ("datetime_trunc", 2),
            ("format_datetime", 2),
            ("generate_uuid", 0),
            ("interval", 2),
            ("struct", -1),
        ];
        for &(fn_name, n_arg) in dummy_fns {
            conn.create_scalar_function(fn_name, n_arg, FunctionFlags::SQLITE_UTF8, move |_ctx| {
                Ok(types::Value::Null)
            })
            .expect("failed to create dummy function");
        }

        Ok(Self { conn })
    }
}

impl Driver for SQLite3Driver {
    fn target(&self) -> Target {
        Target::SQLite3
    }

    fn execute_native_sql(&self, sql: &str) -> Result<()> {
        self.conn
            .execute_batch(sql)
            .context("failed to execute SQL")
    }

    fn drop_table_if_exists(&self, table_name: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", SQLite3Ident(table_name));
        self.execute_native_sql(&sql)
    }

    fn compare_tables(&self, result_table: &str, expected_table: &str) -> Result<()> {
        self.compare_tables_impl(result_table, expected_table)
    }
}

impl DriverImpl for SQLite3Driver {
    type Type = String;
    type Value = Value;
    type Rows = Box<dyn Iterator<Item = Result<Vec<Self::Value>>>>;

    fn table_columns(&self, table_name: &str) -> Result<Vec<Column<Self::Type>>> {
        let sql = format!("PRAGMA table_info({})", table_name);
        let mut stmt = self.conn.prepare(&sql).context("failed to prepare SQL")?;
        let rows = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let ty: String = row.get(2)?;
                Ok(Column { name, ty })
            })
            .map_err(Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to list table columns")?;
        Ok(rows)
    }

    fn query_table_sorted(
        &self,
        table_name: &str,
        columns: &[Column<Self::Type>],
    ) -> Result<Self::Rows> {
        let column_list = columns
            .iter()
            .map(|c| sqlite3_quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            column_list, table_name, column_list
        );
        let mut stmt = self.conn.prepare(&sql).context("failed to prepare SQL")?;
        let rows = stmt
            .query_map([], |row| {
                let mut values = vec![];
                for i in 0..columns.len() {
                    let value = row.get(i)?;
                    values.push(Value(value));
                }
                Ok(values)
            })
            .map_err(Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to query table")?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

/// A displayable SQLite3 value. We use a wrapper so that we can implement
/// `std::fmt::Display` for someone else's type.
///
/// If we ever start encoding arrays and structs as SQLite3 values, we'll also
/// be able to implement `std::fmt::Display` for them.
#[derive(Debug, PartialEq)]
pub struct Value(pub rusqlite::types::Value);

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            rusqlite::types::Value::Null => write!(f, "NULL"),
            rusqlite::types::Value::Integer(i) => write!(f, "{}", i),
            rusqlite::types::Value::Real(r) => write!(f, "{}", r),
            rusqlite::types::Value::Text(s) => write!(f, "{}", SQLite3String(s)),
            rusqlite::types::Value::Blob(b) => write!(f, "{:?}", b),
        }
    }
}

/// Escape an identifier for use in a SQLite3 query.
fn sqlite3_quote_ident(s: &str) -> String {
    format!("{}", SQLite3Ident(s))
}

/// Format a single- or double-quoted string for use in a SQLite3 query. SQLite3
/// does not support backslash escapes.
fn sqlite3_quote_fmt(s: &str, quote_char: char, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", quote_char)?;
    for c in s.chars() {
        if c == quote_char {
            write!(f, "{}", quote_char)?;
        }
        write!(f, "{}", c)?;
    }
    write!(f, "{}", quote_char)
}

/// Formatting wrapper for single-quoted strings.
pub struct SQLite3String<'a>(pub &'a str);

impl fmt::Display for SQLite3String<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        sqlite3_quote_fmt(self.0, '\'', f)
    }
}

/// Formatting wrapper for double-quoted identifiers.
pub struct SQLite3Ident<'a>(pub &'a str);

impl fmt::Display for SQLite3Ident<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        sqlite3_quote_fmt(self.0, '"', f)
    }
}
