//! A Snowflake driver.

use std::{borrow::Cow, env, fmt, str::FromStr};

use arrow_json::writer::record_batches_to_json_rows;
use async_trait::async_trait;
use derive_visitor::DriveMut;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use snowflake_api::{QueryResult, SnowflakeApi};
use tracing::{debug, instrument};

use crate::{
    ast::{self, Emit, Target},
    errors::{format_err, Context, Error, Result},
};

use super::{sqlite3::SQLite3Ident, Column, Driver, DriverImpl, Locator, RewrittenAst};

mod rename_functions;

/// Locator prefix for Snowflake.
pub const SNOWFLAKE_LOCATOR_PREFIX: &str = "snowflake:";

/// A locator for a Snowflake database.
///
/// For now, we will format these as:
///
/// ```text
/// snowflake://<user>@<organization>-<account>[.privatelink]/<warehouse>/<database>
/// ```
///
/// Credentials must be provided using key pair authentication, as described
/// in the [`snowflake-connector` documentation][doc].
///
/// [doc]: https://crates.io/crates/snowflake-connector
#[derive(Debug)]
pub struct SnowflakeLocator {
    /// The organization for the Snowflake database.
    organization: String,

    /// The account for the Snowflake database.
    account: String,

    /// Is this a private link?
    privatelink: bool,

    /// The user for the Snowflake database.
    user: String,

    /// The warehouse for the Snowflake database.
    warehouse: String,

    /// The database for the Snowflake database.
    database: String,
}

impl SnowflakeLocator {
    /// Get the bare host for this Snowflake database. This will have
    /// `.snowflakecomputing.com` appended to it to access the REST API.
    fn account_identifier(&self) -> String {
        let mut base = format!("{}-{}", self.organization, self.account);
        if self.privatelink {
            base.push_str(".privatelink");
        }
        base
    }
}

impl fmt::Display for SnowflakeLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "snowflake://{}@{}-{}{}/{}/{}",
            self.user,
            self.organization,
            self.account,
            if self.privatelink { ".privatelink" } else { "" },
            self.warehouse,
            self.database,
        )
    }
}

// Use `once_cell` and `regex` to parse our locator.
impl FromStr for SnowflakeLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with(SNOWFLAKE_LOCATOR_PREFIX) {
            return Err(format_err!(
                "Snowflake locator must start with {}",
                SNOWFLAKE_LOCATOR_PREFIX
            ));
        }

        // Parse locator using regex.
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r"^snowflake://(?P<user>[^@/]+)@(?P<organization>[^-/]+)-(?P<account>[^/]+)(?P<privatelink>\.privatelink)?/(?P<warehouse>[^/]+)/(?P<database>[^/]+)$",
            )
            .unwrap()
        });
        let captures = RE
            .captures(s)
            .ok_or_else(|| format_err!("invalid Snowflake locator: {}", s))?;

        // Extract captures. These should never fail.
        let user = captures
            .name("user")
            .expect("missing user")
            .as_str()
            .to_string();
        let organization = captures
            .name("organization")
            .expect("missing organization")
            .as_str()
            .to_string();
        let account = captures
            .name("account")
            .expect("missing account")
            .as_str()
            .to_string();
        let privatelink = captures.name("privatelink").is_some();
        let warehouse = captures
            .name("warehouse")
            .expect("missing warehouse")
            .as_str()
            .to_string();
        let database = captures
            .name("database")
            .expect("missing database")
            .as_str()
            .to_string();

        Ok(Self {
            organization,
            account,
            privatelink,
            user,
            warehouse,
            database,
        })
    }
}

#[async_trait]
impl Locator for SnowflakeLocator {
    fn target(&self) -> Target {
        Target::Snowflake
    }

    async fn driver(&self) -> Result<Box<dyn super::Driver>> {
        Ok(Box::new(SnowflakeDriver::from_locator(self)?))
    }
}

/// A Snowflake driver.
///
/// We use the Rust [`snowflake-connector`][snowflake-connector] crate to connect
/// to Snowflake.
///
/// [snowflake-connector]: https://crates.io/crates/snowflake-connector
pub struct SnowflakeDriver {
    connection: SnowflakeApi,
}

impl SnowflakeDriver {
    /// Create a new Snowflake driver from a locator.
    pub fn from_locator(locator: &SnowflakeLocator) -> Result<Self> {
        let password = env::var("SNOWFLAKE_PASSWORD")
            .map_err(|_| format_err!("missing SNOWFLAKE_PASSWORD"))?;
        let connection = SnowflakeApi::with_password_auth(
            &locator.account_identifier(),
            &locator.warehouse,
            Some(&locator.database),
            Some("public"),
            &locator.user,
            None,
            &password,
        )
        .with_context(|| format!("failed to connect to Snowflake database: {}", locator))?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl Driver for SnowflakeDriver {
    fn target(&self) -> Target {
        Target::Snowflake
    }

    #[instrument(skip(self, sql), err)]
    async fn execute_native_sql_statement(&mut self, sql: &str) -> Result<()> {
        debug!(%sql, "executing SQL");
        self.connection
            .exec(sql)
            .await
            .with_context(|| format!("error running SQL: {}", sql))?;
        Ok(())
    }

    async fn execute_ast(&mut self, ast: &ast::SqlProgram) -> Result<()> {
        let rewritten = self.rewrite_ast(ast)?;
        for sql in rewritten.extra_native_sql {
            self.execute_native_sql_statement(&sql).await?;
        }

        // We can only execute one statement at a time.
        for statement in &rewritten.ast.statements {
            let sql = statement.emit_to_string(self.target());
            self.execute_native_sql_statement(&sql).await?;
        }

        // Reset session to drop `TEMP` tables and UDFs.
        self.connection
            .close_session()
            .await
            .context("could not end Snowflake session")
    }

    fn rewrite_ast<'ast>(&self, ast: &'ast ast::SqlProgram) -> Result<RewrittenAst<'ast>> {
        let mut ast = ast.clone();
        let mut renamer = rename_functions::RenameFunctions::default();
        ast.drive_mut(&mut renamer);
        let extra_native_sql = renamer
            .udfs
            .values()
            .map(|udf| udf.to_sql())
            .collect::<Vec<_>>();
        Ok(RewrittenAst {
            extra_native_sql,
            ast: Cow::Owned(ast),
        })
    }

    #[instrument(skip(self))]
    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()> {
        self.execute_native_sql_statement(&format!(
            "DROP TABLE IF EXISTS {}",
            SQLite3Ident(table_name)
        ))
        .await
    }

    #[instrument(skip(self))]
    async fn compare_tables(&mut self, result_table: &str, expected_table: &str) -> Result<()> {
        self.compare_tables_impl(result_table, expected_table).await
    }
}

#[async_trait]
impl DriverImpl for SnowflakeDriver {
    type Type = String;
    /// Apparently, Snowflake sends everything as a string, according to
    /// `snowflake-connector`.
    type Value = Value;
    type Rows = Box<dyn Iterator<Item = Result<Vec<Self::Value>>> + Send + Sync>;

    #[instrument(skip(self))]
    async fn table_columns(&mut self, table_name: &str) -> Result<Vec<Column<Self::Type>>> {
        // TODO: Escaping the table name seems to fail.
        let sql = format!("DESCRIBE TABLE {}", table_name);
        let query_result = self
            .connection
            .exec(&sql)
            .await
            .context("could not execute SQL to list columns")?;
        let rows = deserialize_query_result::<Vec<Value>>(query_result, &["name", "type"])?;

        rows.into_iter()
            .map(|row| {
                let name = row[0]
                    .as_str()
                    .ok_or_else(|| format_err!("expected column name string"))?
                    .to_string();
                let ty = row[1]
                    .as_str()
                    .ok_or_else(|| format_err!("expected column type string"))?
                    .to_string();
                Ok(Column { name, ty })
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn query_table_sorted(
        &mut self,
        table_name: &str,
        columns: &[Column<Self::Type>],
    ) -> Result<Self::Rows> {
        let column_list = columns
            .iter()
            .map(|c| SQLite3Ident(&c.name).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        // TODO: Again, quoting the table name fails.
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            column_list, table_name, column_list,
        );
        let query_result = self
            .connection
            .exec(&sql)
            .await
            .context("could not execute SQL to list columns")?;
        let rows = deserialize_query_result::<Vec<Value>>(
            query_result,
            &columns.iter().map(|c| &c.name[..]).collect::<Vec<_>>(),
        )
        .context("failed to deserialize query result")?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

/// Deserialize query results from Snowflake to our row type.
fn deserialize_query_result<T: serde::de::DeserializeOwned>(
    query_result: QueryResult,
    columns: &[&str],
) -> Result<Vec<T>> {
    let json = query_result_json(query_result, columns)?;
    serde_json::from_value(json).context("failed to deserialize query result")
}

/// Get the JSON associated with a [`QueryResult`].
fn query_result_json(query_result: QueryResult, columns: &[&str]) -> Result<Value> {
    match query_result {
        QueryResult::Arrow(record_batches) => {
            let rows = record_batches_to_json_rows(&record_batches.iter().collect::<Vec<_>>())
                .context("failed to convert Arrow record batches to JSON rows")?
                .into_iter()
                // Convert row map to a JSON array.
                .map(|row_map| {
                    let mut row = Vec::with_capacity(columns.len());
                    for &column in columns {
                        row.push(row_map.get(column).cloned().unwrap_or(Value::Null));
                    }
                    Value::Array(row)
                })
                .collect::<Vec<_>>();
            Ok(Value::Array(rows))
        }
        QueryResult::Json(json) => Ok(json.clone()),
        QueryResult::Empty => Ok(Value::Array(vec![])),
    }
}

/// Quote `s` for Snowflake, surrounding it with `'` and escaping special
/// characters.
///
/// This kind of escaping only works for strings, not for identifiers, which
/// don't allow backslashes, and escape double quotes using `""`.
fn snowflake_quote_fmt(s: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "'")?;
    for c in s.chars() {
        match c {
            '\\' => write!(f, "\\\\")?,
            '"' => write!(f, "\\\"")?,
            '\'' => write!(f, "\\\'")?,
            '\n' => write!(f, "\\n")?,
            '\r' => write!(f, "\\r")?,
            '\t' => write!(f, "\\t")?,
            '\0' => write!(f, "\\0")?,
            _ if c.is_ascii_graphic() || c == ' ' => write!(f, "{}", c)?,
            _ if c as u32 <= 0xFF => write!(f, "\\x{:02x}", c as u32)?,
            _ if c as u32 <= 0xFFFF => write!(f, "\\u{:04x}", c as u32)?,
            // We can't escape these using `\U`, but we seem to be able to
            // include them literally in the cases I tested. Hope for the best.
            _ => write!(f, "{}", c)?,
        }
    }
    write!(f, "'")
}

/// Formatting wrapper for strings quoted with single quotes.
pub struct SnowflakeString<'a>(pub &'a str);

impl fmt::Display for SnowflakeString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        snowflake_quote_fmt(self.0, f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_round_trip_locator() {
        let examples = &[
            "snowflake://user@organization-account/warehouse/database",
            "snowflake://user@organization-account.privatelink/warehouse/database",
        ];
        for example in examples {
            let locator = example.parse::<SnowflakeLocator>().unwrap();
            assert_eq!(example, &locator.to_string());
        }
    }
}
