//! Trino and maybe Presto driver.

use std::{fmt, str::FromStr, time::Duration};

use async_trait::async_trait;
use dbcrossbar_trino::{
    client::{Client, ClientBuilder, ClientError, QueryError},
    ConnectorType as TrinoConnectorType, DataType as TrinoDataType, Field as TrinoField,
    Ident as TrinoIdent, Value,
};
use joinery_macros::sql_quote;
use once_cell::sync::Lazy;
use regex::Regex;
use tokio::time::sleep;
use tracing::debug;

use crate::{
    ast::{FunctionCall, Target},
    errors::{format_err, Context, Error, Result},
    tokenizer::TokenStream,
    transforms::{self, RenameFunctionsBuilder, Transform},
    types::{SimpleType, StructElementType, ValueType},
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

impl TrinoLocator {
    /// Create a client for this locator.
    fn client(&self) -> Client {
        ClientBuilder::new(
            self.user.clone(),
            self.host.clone(),
            self.port.unwrap_or(8080),
        )
        .catalog_and_schema(self.catalog.clone(), self.schema.clone())
        .build()
    }

    /// Get our Trino connector type.
    async fn connector_type(&self) -> Result<TrinoConnectorType> {
        let client = self.client();
        let catalog = TrinoIdent::new(&self.catalog).map_err(Error::other)?;
        client
            .catalog_connector_type(&catalog)
            .await
            .map_err(Error::other)
    }
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
    async fn target(&self) -> Result<Target> {
        let connector_type = self.connector_type().await?;
        Ok(Target::Trino(connector_type))
    }

    async fn driver(&self) -> Result<Box<dyn Driver>> {
        Ok(Box::new(TrinoDriver::from_locator(self).await?))
    }
}

/// A Trino driver.
pub struct TrinoDriver {
    connector_type: TrinoConnectorType,
    catalog: String,
    schema: String,
    client: Client,
}

impl TrinoDriver {
    /// Create a new Trino driver from a locator.
    pub async fn from_locator(locator: &TrinoLocator) -> Result<Self> {
        let connector_type = locator.connector_type().await?;
        let client = locator.client();
        Ok(Self {
            connector_type,
            catalog: locator.catalog.clone(),
            schema: locator.schema.clone(),
            client,
        })
    }
}

#[async_trait]
impl Driver for TrinoDriver {
    fn target(&self) -> Target {
        Target::Trino(self.connector_type)
    }

    #[tracing::instrument(skip_all)]
    async fn execute_native_sql_statement(&mut self, sql: &str) -> Result<()> {
        debug!(%sql, "Executing native SQL statement");
        retry_trino_error! {
            self.client.run_statement(sql).await
        }
        .map_err(Error::other)?;
        //.map_err(|err| abbreviate_trino_error(sql, err))?;
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
            Box::new(transforms::StandardizeLiteralTypes),
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
            self.client.run_statement(&sql).await
        }
        //.map_err(|err| abbreviate_trino_error(&sql, err))
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
    type Type = TrinoDataType;
    type Value = Value;
    type Rows = Box<dyn Iterator<Item = Result<Vec<Self::Value>>> + Send + Sync>;

    #[tracing::instrument(skip(self))]
    async fn table_columns(&mut self, table_name: &str) -> Result<Vec<Column<Self::Type>>> {
        let catalog = TrinoIdent::new(&self.catalog).map_err(Error::other)?;
        let schema = TrinoIdent::new(&self.schema).map_err(Error::other)?;
        let table_name = TrinoIdent::new(table_name).map_err(Error::other)?;

        let column_infos = retry_trino_error! {
            self.client.get_table_column_info(&catalog, &schema, &table_name).await
        }
        .with_context(|| format!("Failed to get columns for table: {}", table_name))?;
        column_infos
            .into_iter()
            .map(|c| {
                Ok(Column {
                    name: c.column_name.as_unquoted_str().to_owned(),
                    ty: TrinoDataType::try_from(c.data_type).map_err(Error::other)?,
                })
            })
            .collect::<Result<Vec<_>>>()
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
            self.client.get_all::<Vec<Value>>(&sql).await
        }
        //.map_err(|err| abbreviate_trino_error(&sql, err))
        .with_context(|| format!("Failed to query table: {}", table_name))?;
        let rows = dataset.into_iter().map(|r| Ok(r));
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
fn should_retry(e: &ClientError) -> bool {
    matches!(e, ClientError::QueryError(QueryError { error_name, .. }) if error_name == "NO_NODES_AVAILABLE")
}

// /// These errors are pages long.
// fn abbreviate_trino_error(sql: &str, e: PrustoError) -> Error {
//     if let PrustoError::QueryError(e) = &e {
//         // We can make these look pretty.
//         let QueryError {
//             message,
//             error_code,
//             error_location,
//             ..
//         } = e;
//         let mut files = KnownFiles::default();
//         let file_id = files.add_string("trino.sql", sql);

//         let offset = if let Some(loc) = error_location {
//             // We don't want to panic, because we're already processing an
//             // error, and the error comes from an external source. So just
//             // muddle through and return Span::Unknown or a bogus location
//             // if our input data is too odd.
//             //
//             // Convert from u32, defaulting negative values to 1. (Although
//             // lines count from 1.)
//             let line_number = usize::try_from(loc.line_number).unwrap_or(0);
//             let column_number = usize::try_from(loc.column_number).unwrap_or(0);
//             files
//                 .line_range(file_id, line_number.saturating_sub(1))
//                 .ok()
//                 .map(|r| r.start + column_number.saturating_sub(1))
//         } else {
//             None
//         };

//         if let Some(offset) = offset {
//             let diagnostic = Diagnostic::error()
//                 .with_message(message.clone())
//                 .with_code(format!("TRINO {}", error_code))
//                 .with_labels(vec![codespan_reporting::diagnostic::Label::primary(
//                     file_id,
//                     offset..offset,
//                 )
//                 .with_message("Trino error")]);

//             return Error::Source(Box::new(SourceError {
//                 alternate_summary: message.clone(),
//                 diagnostic,
//                 files_override: Some(Arc::new(files)),
//             }));
//         }
//     }

//     let msg = e
//         .to_string()
//         .lines()
//         .take(10)
//         .collect::<Vec<_>>()
//         .join("\n");
//     format_err!("Trino error: {}", msg)
// }

impl TryFrom<&'_ ValueType> for TrinoDataType {
    type Error = Error;

    fn try_from(value_type: &ValueType) -> std::result::Result<Self, Self::Error> {
        match value_type {
            ValueType::Simple(simple_type) => TrinoDataType::try_from(simple_type),
            ValueType::Array(simple_type) => Ok(TrinoDataType::Array(Box::new(
                TrinoDataType::try_from(simple_type)?,
            ))),
        }
    }
}

impl TryFrom<&'_ SimpleType> for TrinoDataType {
    type Error = Error;

    fn try_from(simple_type: &SimpleType) -> std::result::Result<Self, Self::Error> {
        match simple_type {
            SimpleType::Bool => Ok(TrinoDataType::Boolean),
            SimpleType::Bytes => Ok(TrinoDataType::Varbinary),
            SimpleType::Date => Ok(TrinoDataType::Date),
            SimpleType::Datetime => Ok(TrinoDataType::timestamp()),
            SimpleType::Float64 => Ok(TrinoDataType::Double),
            SimpleType::Geography => Ok(TrinoDataType::SphericalGeography),
            SimpleType::Int64 => Ok(TrinoDataType::BigInt),
            SimpleType::Numeric => Ok(TrinoDataType::bigquery_sized_decimal()),
            SimpleType::String => Ok(TrinoDataType::varchar()),
            SimpleType::Time => Ok(TrinoDataType::time()),
            SimpleType::Timestamp => Ok(TrinoDataType::timestamp_with_time_zone()),
            SimpleType::Struct(struct_type) => {
                let fields = struct_type
                    .fields
                    .iter()
                    .map(|f| TrinoField::try_from(f))
                    .collect::<Result<Vec<_>>>()?;
                Ok(TrinoDataType::Row(fields))
            }

            // These shouldn't make it this far, either because they're not a concrete type,
            // or because they're types we only allow as constant arguments to functions that
            // should have been transformed away by now.
            SimpleType::Bottom | SimpleType::Datepart | SimpleType::Interval | SimpleType::Null => {
                Err(format_err!(
                    "Cannot represent {} as concrete Trino type",
                    simple_type
                ))
            }

            // This shouldn't be a constructable value for
            // `SimpleType<ResolvedTypeVarsOnly>`.
            SimpleType::Parameter(_) => unreachable!("parameter types should be resolved"),
        }
    }
}

impl TryFrom<&'_ StructElementType> for TrinoField {
    type Error = Error;

    fn try_from(field: &StructElementType) -> std::result::Result<Self, Self::Error> {
        let name = match &field.name {
            Some(name) => Some(TrinoIdent::new(&name.name).map_err(Error::other)?),
            None => None,
        };
        Ok(TrinoField {
            name,
            data_type: TrinoDataType::try_from(&field.ty)?,
        })
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
