//! Trino and maybe Presto driver.

use std::{fmt, str::FromStr};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use prusto::{Client, ClientBuilder, Presto, Row};
use regex::Regex;
use tracing::debug;

use crate::{
    ast::Target,
    errors::{format_err, Context, Error, Result},
    transforms::{self, Transform, Udf},
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
    "ARRAY_LENGTH" => "CARDINALITY",
    "ARRAY_TO_STRING" => "ARRAY_JOIN",
    "GENERATE_UUID" => "UUID",
};

/// A `phf_map!` of BigQuery function names to UDFs.
///
/// TODO: I'm not even sure there's a way to define SQL UDFs in Trino.
static UDFS: phf::Map<&'static str, &'static Udf> = phf::phf_map! {};

/// Format a UDF.
///
/// TODO: I'm not even sure there's a way to define SQL UDFs in Trino.
fn format_udf(udf: &Udf) -> String {
    format!(
        "CREATE OR REPLACE TEMP FUNCTION {} AS $$\n{}\n$$\n",
        udf.decl, udf.sql
    )
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
        self.client
            .execute(sql.to_owned())
            .await
            .map_err(abbreviate_trino_error)
            .with_context(|| format!("Failed to execute SQL: {}", sql))?;
        Ok(())
    }

    fn supports_multiple_statements(&self) -> bool {
        false
    }

    fn transforms(&self) -> Vec<Box<dyn Transform>> {
        vec![
            Box::new(transforms::ArraySelectToSubquery),
            Box::new(transforms::QualifyToSubquery),
            Box::<transforms::ExpandExcept>::default(),
            Box::new(transforms::InUnnestToInSelect),
            Box::new(transforms::CountifToCase),
            Box::new(transforms::IndexFromOne),
            Box::new(transforms::IsBoolToCase),
            Box::new(transforms::OrReplaceToDropIfExists),
            Box::new(transforms::RenameFunctions::new(
                &FUNCTION_NAMES,
                &UDFS,
                &format_udf,
            )),
            Box::new(transforms::CleanUpTempManually {
                format_name: &|table_name| AnsiIdent(&table_name.unescaped_bigquery()).to_string(),
            }),
        ]
    }

    #[tracing::instrument(skip(self))]
    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()> {
        self.client
            .execute(format!("DROP TABLE IF EXISTS {}", AnsiIdent(table_name)))
            .await
            .map_err(abbreviate_trino_error)
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
        Ok(self
            .client
            .get_all::<Col>(sql)
            .await
            .map_err(abbreviate_trino_error)
            .with_context(|| format!("Failed to get columns for table: {}", table_name))?
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
        let rows = self
            .client
            .get_all::<Row>(sql)
            .await
            .map_err(abbreviate_trino_error)
            .with_context(|| format!("Failed to query table: {}", table_name))?
            .into_vec()
            .into_iter()
            .map(|r| Ok(r.into_json()));
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

/// These errors are pages long.
fn abbreviate_trino_error(e: prusto::error::Error) -> Error {
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
