//! Our SQLite3 driver.

use rusqlite::{functions::FunctionFlags, types};

use crate::{
    ast::Target,
    errors::{Context, Result},
};

use self::unnest::register_unnest;

use super::Driver;

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

        // Install our dummy `UNNEST` virtual table.
        register_unnest(&conn).expect("failed to register UNNEST");

        // Install some dummy functions that always return NULL.
        let dummy_fns = &[
            ("array", -1),
            ("current_datetime", 0),
            ("date_diff", 3),
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
}
