//! Trino and maybe Presto driver.

use std::{fmt, str::FromStr};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use prusto::{Client, ClientBuilder, Presto, Row};
use regex::Regex;
use tracing::debug;

use crate::{
    ast::{self, Emit, Target},
    drivers::sqlite3::SQLite3String,
    errors::{format_err, Context, Error, Result},
    transforms::{self, Transform, Udf},
};

use super::{sqlite3::SQLite3Ident, Column, Driver, DriverImpl, Locator};

/// Our locator prefix.
pub const TRINO_LOCATOR_PREFIX: &str = "trino:";

// A `phf_map!` of BigQuery function names to native function names. Use
// this for simple renaming.
static FUNCTION_NAMES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "ARRAY_LENGTH" => "CARDINALITY",
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
            .with_context(|| format!("Failed to execute SQL: {}", sql))?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn execute_ast(&mut self, ast: &ast::SqlProgram) -> Result<()> {
        let rewritten = self.rewrite_ast(ast)?;
        for sql in rewritten.extra_native_sql {
            self.execute_native_sql_statement(&sql).await?;
        }

        // We can only execute one statement at a time.
        for statement in rewritten.ast.statements.node_iter() {
            let sql = statement.emit_to_string(self.target());
            self.execute_native_sql_statement(&sql).await?;
        }
        Ok(())
    }

    fn transforms(&self) -> Vec<Box<dyn Transform>> {
        vec![
            Box::new(transforms::OrReplaceToDropIfExists),
            Box::new(transforms::RenameFunctions::new(
                &FUNCTION_NAMES,
                &UDFS,
                &format_udf,
            )),
        ]
    }

    #[tracing::instrument(skip(self))]
    async fn drop_table_if_exists(&mut self, table_name: &str) -> Result<()> {
        self.client
            .execute(format!("DROP TABLE IF EXISTS {}", SQLite3Ident(table_name)))
            .await
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
            SQLite3String(&self.catalog),
            SQLite3String(&self.schema),
            SQLite3String(table_name)
        );
        Ok(self
            .client
            .get_all::<Col>(sql)
            .await
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
            .map(|c| SQLite3Ident(&c.name).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            cols_sql,
            SQLite3Ident(table_name),
            cols_sql
        );
        let rows = self
            .client
            .get_all::<Row>(sql)
            .await
            .with_context(|| format!("Failed to query table: {}", table_name))?
            .into_vec()
            .into_iter()
            .map(|r| Ok(r.into_json()));
        Ok(Box::new(rows))
    }
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
