//! Our SQLite3 driver.

use std::{fmt, str::FromStr, vec};

use async_rusqlite::Connection;
use async_trait::async_trait;
use rusqlite::{functions::FunctionFlags, types};

use crate::{
    ast::Target,
    errors::{format_err, Context, Error, Result},
};

use self::unnest::register_unnest;

use super::{Column, Driver, DriverImpl, Locator};

mod unnest;

/// Our locator prefix.
pub const SQLITE3_LOCATOR_PREFIX: &str = "sqlite3:";

/// A locator for an SQLite3 database.
#[derive(Debug)]
pub struct SQLite3Locator {
    /// The path to the SQLite3 database.
    path: String,
}

impl SQLite3Locator {
    /// Create an in-memory SQLite3 database.
    pub fn memory() -> Self {
        Self {
            path: ":memory:".to_string(),
        }
    }
}

impl fmt::Display for SQLite3Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", SQLITE3_LOCATOR_PREFIX, self.path)
    }
}

#[async_trait]
impl Locator for SQLite3Locator {
    fn target(&self) -> Target {
        Target::SQLite3
    }

    async fn driver(&self) -> Result<Box<dyn Driver>> {
        Ok(Box::new(SQLite3Driver::from_locator(self).await?))
    }
}

impl FromStr for SQLite3Locator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with(SQLITE3_LOCATOR_PREFIX) {
            return Err(format_err!(
                "SQLite3 locator must start with {}",
                SQLITE3_LOCATOR_PREFIX
            ));
        }
        Ok(Self {
            path: s[SQLITE3_LOCATOR_PREFIX.len()..].to_string(),
        })
    }
}

/// Our SQLite3 driver.
pub struct SQLite3Driver {
    /// Our database connection.
    ///
    /// We use a [`async_rusqlite::Connection`] instead of a
    /// [`rusqlite::Connection`], because the basic [`rusqlite::Connection`]
    /// does not implement [`Send`] or [`Sync`]. Which means it can only be used
    /// from within a single thread, and can't be used in async  code. The
    /// [`async_rusqlite::Connection`] wrapper actually spawns a single worker
    /// thread to own a given SQLite3 connection, so that it never needs to move
    /// between threads. SQLite3 is great, but it's only thread-safe under a
    /// very specific set of assumptions.
    conn: Connection,
}

impl SQLite3Driver {
    /// Create an in-memory SQLite3 database.
    #[allow(dead_code)]
    pub async fn memory() -> Result<Self> {
        Self::from_locator(&SQLite3Locator::memory()).await
    }

    /// Create an SQLite3 database from a locator.
    pub async fn from_locator(locator: &SQLite3Locator) -> Result<Self> {
        let conn = Connection::open(&locator.path)
            .await
            .with_context(|| format!("failed to open SQLite3 database: {}", locator.path))?;

        conn.call(|conn| -> Result<()> {
            // Install our dummy `UNNEST` virtual table. This does nothing useful
            // yet, but it allows us to parse and execute queries that use `UNNEST`.
            register_unnest(conn).expect("failed to register UNNEST");

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
                ("function.custom", 0),
            ];
            for &(fn_name, n_arg) in dummy_fns {
                conn.create_scalar_function(
                    fn_name,
                    n_arg,
                    FunctionFlags::SQLITE_UTF8,
                    move |_ctx| Ok(types::Value::Null),
                )
                .expect("failed to create dummy function");
            }
            Ok(())
        })
        .await?;

        Ok(Self { conn })
    }
}

#[async_trait]
impl Driver for SQLite3Driver {
    fn target(&self) -> Target {
        Target::SQLite3
    }

    async fn execute_native_sql_statement(&mut self, sql: &str) -> Result<()> {
        let sql = sql.to_owned();
        self.conn
            .call(move |conn| conn.execute_batch(&sql).context("failed to execute SQL"))
            .await
    }

    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", SQLite3Ident(table_name));
        self.execute_native_sql_statement(&sql).await
    }

    async fn compare_tables(&mut self, result_table: &str, expected_table: &str) -> Result<()> {
        self.compare_tables_impl(result_table, expected_table).await
    }
}

#[async_trait]
impl DriverImpl for SQLite3Driver {
    type Type = String;
    type Value = Value;
    type Rows = Box<dyn Iterator<Item = Result<Vec<Self::Value>>> + Send + Sync>;

    async fn table_columns(&mut self, table_name: &str) -> Result<Vec<Column<Self::Type>>> {
        let sql = format!("PRAGMA table_info({})", SQLite3Ident(table_name));
        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql).context("failed to prepare SQL")?;
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
            })
            .await
    }

    async fn query_table_sorted(
        &mut self,
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
            column_list,
            SQLite3Ident(table_name),
            column_list
        );
        let columns = columns.to_vec();
        Ok(self
            .conn
            .call(move |conn| -> Result<_> {
                let mut stmt = conn.prepare(&sql).context("failed to prepare SQL")?;
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
            })
            .await?)
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
