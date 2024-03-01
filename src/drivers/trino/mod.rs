//! Trino and maybe Presto driver.

use std::{fmt, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use codespan_reporting::{diagnostic::Diagnostic, files::Files};
use joinery_macros::sql_quote;
use once_cell::sync::Lazy;
use prusto::{error::Error as PrustoError, Client, ClientBuilder, Presto, QueryError, Row};
use regex::Regex;
use tokio::time::sleep;
use tracing::debug;

use crate::{
    ast::{FunctionCall, Target},
    errors::{format_err, Context, Error, Result, SourceError},
    known_files::KnownFiles,
    tokenizer::TokenStream,
    transforms::{self, RenameFunctionsBuilder, Transform},
    util::AnsiIdent,
};

use super::{Column, Driver, DriverImpl, Locator};

/// Our locator prefix.
pub const TRINO_LOCATOR_PREFIX: &str = "trino:";

/// SQLite3 reserved keywords that should be quoted in specific circumstances.
pub static KEYWORDS: phf::Set<&'static str> = phf::phf_set! {
    "ALTER", "AND", "AS", "BETWEEN", "BY", "CASE", "CAST", "CONSTRAINT",
    "CREATE", "CROSS", "CUBE", "CURRENT_CATALOG", "CURRENT_DATE",
    "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
    "CURRENT_TIMESTAMP", "CURRENT_USER", "DEALLOCATE", "DELETE", "DESCRIBE",
    "DISTINCT", "DROP", "ELSE", "END", "ESCAPE", "EXCEPT", "EXECUTE", "EXISTS",
    "EXTRACT", "FALSE", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "HAVING",
    "IN", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "JSON_ARRAY",
    "JSON_EXISTS", "JSON_OBJECT", "JSON_QUERY", "JSON_TABLE", "JSON_VALUE",
    "LEFT", "LIKE", "LISTAGG", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL",
    "NORMALIZE", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "PREPARE",
    "RECURSIVE", "RIGHT", "ROLLUP", "SELECT", "SKIP", "TABLE", "THEN", "TRIM",
    "TRUE", "UESCAPE", "UNION", "UNNEST", "USING", "VALUES", "WHEN", "WHERE",
    "WITH",
};

// A `phf_map!` of BigQuery function names to native function names. Use
// this for simple renaming.
static FUNCTION_NAMES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "APPROX_COUNT_DISTINCT" => "APPROX_DISTINCT",
    "ARRAY_LENGTH" => "CARDINALITY",
    "ARRAY_TO_STRING" => "ARRAY_JOIN",
    "CURRENT_DATETIME" => "CURRENT_TIMESTAMP",
    "DATETIME" => "memory.joinery_compat.DATETIME",
    "GENERATE_UUID" => "memory.joinery_compat.GENERATE_UUID",
    "REGEXP_CONTAINS" => "REGEXP_LIKE",
    "SHA256" => "memory.joinery_compat.SHA256_COMPAT",
    "SUBSTR" => "memory.joinery_compat.SUBSTR_COMPAT",
    "TO_HEX" => "memory.joinery_compat.TO_HEX_COMPAT",
};

/// Rewrite `APPROX_QUANTILES` to `APPROX_PERCENTILE`. We need to implement this
/// as a function call rewriter, because it's an aggregate function, and we
/// can't define those as an SQL UDF.
fn rewrite_approx_quantiles(call: &FunctionCall) -> TokenStream {
    let mut args = call.args.node_iter();
    let value_arg = args.next().expect("should be enforced by type checker");
    let quantiles_arg = args.next().expect("should be enforced by type checker");
    sql_quote! {
        APPROX_PERCENTILE(
            #value_arg,
            PERCENTILE_ARRAY_FOR_QUANTILES(#quantiles_arg)
        )
    }
}

/// Quick and dirty retry loop to deal with transient Trino errors. We will
/// probably ultimately want exponential backoff, etc.
macro_rules! retry_trino_error {
    ($e:expr) => {{
        let mut max_tries = 3;
        let mut sleep_duration = Duration::from_millis(500);
        loop {
            match $e {
                Ok(val) => break Ok(val),
                Err(e) if should_retry(&e) && max_tries > 0 => {
                    eprintln!("Retrying Trino query after error");
                    sleep(sleep_duration).await;
                    max_tries -= 1;
                    sleep_duration *= 2;
                    continue;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}

/// A locator for a Trino database. May or may not also work for Presto.
#[derive(Debug)]
pub struct TrinoLocator {
    user: String,
    host: String,
    port: Option<u16>,
    catalog: String,
    schema: String,
}

impl fmt::Display for TrinoLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "trino://{}@{}", self.user, self.host,)?;
        if let Some(port) = self.port {
            write!(f, ":{}", port)?;
        }
        write!(f, "/{}/{}", self.catalog, self.schema)
    }
}

// Use `once_cell` and `regex` to parse our locator.
impl FromStr for TrinoLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static LOCATOR_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r"(?x)
                ^trino://
                (?P<user>[^@]+)@
                (?P<host>[^:/]+)
                (?::(?P<port>\d+))?
                /(?P<catalog>[^/]+)
                /(?P<schema>[^/]+)
                $",
            )
            .unwrap()
        });

        let captures = LOCATOR_RE
            .captures(s)
            .ok_or_else(|| format_err!("Invalid Trino locator: {}", s))?;

        Ok(Self {
            user: captures.name("user").unwrap().as_str().to_owned(),
            host: captures.name("host").unwrap().as_str().to_owned(),
            port: captures
                .name("port")
                .map(|m| m.as_str().parse::<u16>().unwrap()),
            catalog: captures.name("catalog").unwrap().as_str().to_owned(),
            schema: captures.name("schema").unwrap().as_str().to_owned(),
        })
    }
}

#[async_trait]
impl Locator for TrinoLocator {
    fn target(&self) -> Target {
        Target::Trino
    }

    async fn driver(&self) -> Result<Box<dyn Driver>> {
        Ok(Box::new(TrinoDriver::from_locator(self)?))
    }
}

/// A Trino driver.
pub struct TrinoDriver {
    catalog: String,
    schema: String,
    client: Client,
}

impl TrinoDriver {
    /// Create a new Trino driver from a locator.
    pub fn from_locator(locator: &TrinoLocator) -> Result<Self> {
        let client = ClientBuilder::new(&locator.user, &locator.host)
            .port(locator.port.unwrap_or(8080))
            .catalog(&locator.catalog)
            .schema(&locator.schema)
            .build()
            .with_context(|| format!("Failed to connect to Trino: {}", locator))?;
        Ok(Self {
            client,
            catalog: locator.catalog.clone(),
            schema: locator.schema.clone(),
        })
    }
}

#[async_trait]
impl Driver for TrinoDriver {
    fn target(&self) -> Target {
        Target::Trino
    }

    #[tracing::instrument(skip_all)]
    async fn execute_native_sql_statement(&mut self, sql: &str) -> Result<()> {
        debug!(%sql, "Executing native SQL statement");
        retry_trino_error! {
            self.client.execute(sql.to_owned()).await
        }
        .map_err(|err| abbreviate_trino_error(sql, err))?;
        Ok(())
    }

    fn supports_multiple_statements(&self) -> bool {
        false
    }

    fn transforms(&self) -> Vec<Box<dyn Transform>> {
        let rename_functions = RenameFunctionsBuilder::new(&FUNCTION_NAMES)
            .rewrite_function_call("APPROX_QUANTILES", &rewrite_approx_quantiles)
            .build();
        vec![
            Box::new(transforms::QualifyToSubquery),
            Box::<transforms::ExpandExcept>::default(),
            Box::new(transforms::InUnnestToContains),
            Box::new(transforms::CountifToCase),
            Box::new(transforms::IndexFromOne),
            Box::new(transforms::IsBoolToCase),
            Box::new(transforms::OrReplaceToDropIfExists),
            Box::new(rename_functions),
            Box::new(transforms::SpecialDateFunctionsToTrino),
            Box::new(transforms::StandardizeCurrentTimeUnit::no_parens()),
            Box::new(transforms::CleanUpTempManually {
                format_name: &|table_name| AnsiIdent(&table_name.unescaped_bigquery()).to_string(),
            }),
        ]
    }

    #[tracing::instrument(skip(self))]
    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", AnsiIdent(table_name));
        retry_trino_error! {
            self.client.execute(sql.clone()).await
        }
        .map_err(|err| abbreviate_trino_error(&sql, err))
        .with_context(|| format!("Failed to drop table: {}", table_name))?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn compare_tables(&mut self, result_table: &str, expected_table: &str) -> Result<()> {
        self.compare_tables_impl(result_table, expected_table).await
    }
}

#[async_trait]
impl DriverImpl for TrinoDriver {
    type Type = String;
    type Value = serde_json::Value;
    type Rows = Box<dyn Iterator<Item = Result<Vec<Self::Value>>> + Send + Sync>;

    #[tracing::instrument(skip(self))]
    async fn table_columns(&mut self, table_name: &str) -> Result<Vec<Column<Self::Type>>> {
        #[derive(Debug, Presto)]
        #[allow(non_snake_case)]
        struct Col {
            col: String,
            ty: String,
        }

        let sql = format!(
            "SELECT column_name AS col, data_type AS ty
            FROM information_schema.columns
            WHERE table_catalog = {} AND table_schema = {} AND table_name = {}",
            // TODO: Replace with real string escapes.
            TrinoString(&self.catalog),
            TrinoString(&self.schema),
            TrinoString(table_name)
        );
        let dataset = retry_trino_error! {
            self.client.get_all::<Col>(sql.clone()).await
        }
        .map_err(|err| abbreviate_trino_error(&sql, err))
        .with_context(|| format!("Failed to get columns for table: {}", table_name))?;
        Ok(dataset
            .into_vec()
            .into_iter()
            .map(|c| Column {
                name: c.col,
                ty: c.ty,
            })
            .collect())
    }

    #[tracing::instrument(skip(self))]
    async fn query_table_sorted(
        &mut self,
        table_name: &str,
        columns: &[Column<Self::Type>],
    ) -> Result<Self::Rows> {
        let cols_sql = columns
            .iter()
            .map(|c| AnsiIdent(&c.name).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            cols_sql,
            AnsiIdent(table_name),
            cols_sql
        );
        let dataset = retry_trino_error! {
            self.client.get_all::<Row>(sql.clone()).await
        }
        .map_err(|err| abbreviate_trino_error(&sql, err))
        .with_context(|| format!("Failed to query table: {}", table_name))?;
        let rows = dataset.into_vec().into_iter().map(|r| Ok(r.into_json()));
        Ok(Box::new(rows))
    }
}

/// Quote `s` for Trino, surrounding it with `'` and escaping special
/// characters as needed.
fn trino_quote_fmt(s: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
        write!(f, "'")?;
        for c in s.chars() {
            match c {
                '\'' => write!(f, "''")?,
                _ => write!(f, "{}", c)?,
            }
        }
        write!(f, "'")
    } else {
        write!(f, "U&'")?;
        for c in s.chars() {
            match c {
                '\'' => write!(f, "''")?,
                '\\' => write!(f, "\\\\")?,
                _ if c.is_ascii_graphic() || c == ' ' => write!(f, "{}", c)?,
                _ if c as u32 <= 0xFFFF => write!(f, "\\{:04x}", c as u32)?,
                _ => write!(f, "\\+{:06x}", c as u32)?,
            }
        }
        write!(f, "'")
    }
}

/// Formatting wrapper for strings quoted with single quotes.
pub struct TrinoString<'a>(pub &'a str);

impl fmt::Display for TrinoString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        trino_quote_fmt(self.0, f)
    }
}

/// Should an error be retried?
///
/// Note that the `rusto` crate has internal support for retrying connection
/// and network errors, so we don't need to worry about that. But we do need
/// to look out for `QueryError`s that might need to be retried.
fn should_retry(e: &PrustoError) -> bool {
    matches!(e, PrustoError::QueryError(QueryError { error_name, .. }) if error_name == "NO_NODES_AVAILABLE")
}

/// These errors are pages long.
fn abbreviate_trino_error(sql: &str, e: PrustoError) -> Error {
    if let PrustoError::QueryError(e) = &e {
        // We can make these look pretty.
        let QueryError {
            message,
            error_code,
            error_location,
            ..
        } = e;
        let mut files = KnownFiles::default();
        let file_id = files.add_string("trino.sql", sql);

        let offset = if let Some(loc) = error_location {
            // We don't want to panic, because we're already processing an
            // error, and the error comes from an external source. So just
            // muddle through and return Span::Unknown or a bogus location
            // if our input data is too odd.
            //
            // Convert from u32, defaulting negative values to 1. (Although
            // lines count from 1.)
            let line_number = usize::try_from(loc.line_number).unwrap_or(0);
            let column_number = usize::try_from(loc.column_number).unwrap_or(0);
            files
                .line_range(file_id, line_number.saturating_sub(1))
                .ok()
                .map(|r| r.start + column_number.saturating_sub(1))
        } else {
            None
        };

        if let Some(offset) = offset {
            let diagnostic = Diagnostic::error()
                .with_message(message.clone())
                .with_code(format!("TRINO {}", error_code))
                .with_labels(vec![codespan_reporting::diagnostic::Label::primary(
                    file_id,
                    offset..offset,
                )
                .with_message("Trino error")]);

            return Error::Source(Box::new(SourceError {
                alternate_summary: message.clone(),
                diagnostic,
                files_override: Some(Arc::new(files)),
            }));
        }
    }

    let msg = e
        .to_string()
        .lines()
        .take(10)
        .collect::<Vec<_>>()
        .join("\n");
    format_err!("Trino error: {}", msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trino_locator() {
        let locator: TrinoLocator = "trino://user@host:1234/catalog/schema"
            .parse()
            .expect("parse failed");
        assert_eq!(locator.user, "user");
        assert_eq!(locator.host, "host");
        assert_eq!(locator.port, Some(1234));
        assert_eq!(locator.catalog, "catalog");
        assert_eq!(locator.schema, "schema");

        let locator: TrinoLocator = "trino://user@host/catalog/schema"
            .parse()
            .expect("parse failed");
        assert_eq!(locator.user, "user");
        assert_eq!(locator.host, "host");
        assert_eq!(locator.port, None);
        assert_eq!(locator.catalog, "catalog");
        assert_eq!(locator.schema, "schema");
    }
}
