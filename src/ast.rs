// Don't bother with `Box`-ing everything for now. Allow huge enum values.
#![allow(clippy::large_enum_variant)]

use std::{borrow::Cow, error, fmt, ops::Range};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFile,
    term::{
        self,
        termcolor::{ColorChoice, StandardStream},
    },
};

/// None of these keywords should ever be matched as a bare identifier. We use
/// [`phf`](https://github.com/rust-phf/rust-phf), which generates "perfect hash
/// functions." These allow us to create highly optimized, read-only sets/maps
/// generated at compile time.
static KEYWORDS: phf::Set<&'static str> = phf::phf_set! {
    "ABORT", "ACCOUNT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS",
    "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT",
    "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE",
    "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT",
    "CONNECTION", "CONSTRAINT", "CONTAINS", "CREATE", "CROSS", "CUBE",
    "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE",
    "DEFAULT", "DEFERRABLE", "DEFERRED", "DEFINE", "DELETE", "DESC", "DETACH",
    "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT",
    "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "EXTRACT", "FAIL", "FALSE",
    "FETCH", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL",
    "GENERATED", "GLOB", "GROUP", "GROUPING", "GROUPS", "GSCLUSTER", "HASH",
    "HAVING", "IF", "IGNORE", "ILIKE", "IMMEDIATE", "IN", "INCREMENT", "INDEX",
    "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT",
    "INTERVAL", "INTO", "IS", "ISNULL", "ISSUE", "JOIN", "KEY", "LAST",
    "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MATCH", "MATERIALIZED",
    "MERGE", "MINUS", "NATURAL", "NEW", "NO", "NOT", "NOTHING", "NOTNULL",
    "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "ORGANIZATION",
    "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING",
    "PRIMARY", "PROTO", "QUALIFY", "QUERY", "RAISE", "RANGE", "RECURSIVE",
    "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE",
    "RESPECT", "RESTRICT", "RETURNING", "RIGHT", "RLIKE", "ROLLBACK", "ROLLUP",
    "ROW", "ROWS", "SAVEPOINT", "SCHEMA", "SELECT", "SET", "SOME", "STRUCT",
    "TABLE", "TABLESAMPLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO",
    "TRANSACTION", "TREAT", "TRIGGER", "TRUE", "TRY_CAST", "UNBOUNDED", "UNION",
    "UNIQUE", "UNNEST", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW",
    "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN", "WITHOUT",

    // Types. These aren't really keywords but we treat them as such?
    //
    // TODO: Figure this out.
    "BOOL", "BOOLEAN", "INT64", "FLOAT64", "STRING", "DATETIME",

    // Magic functions with parser integration.
    "COUNT",
};

/// We represent a span in our source code using a Rust range. These are easy
/// to construct from within our parser.
type Span = Range<usize>;

/// A function that compares two strings for equality.
type StrCmp = dyn Fn(&str, &str) -> bool;

/// The target language we're transpiling to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum Target {
    BigQuery,
    SQLite3,
}

impl Target {
    /// Format this node for the given target.
    fn f<T: DisplayForTarget>(self, node: &T) -> FormatForTarget<'_, T> {
        FormatForTarget { target: self, node }
    }
}

/// Wrapper for formatting a node for a given target.
struct FormatForTarget<'a, T: DisplayForTarget> {
    target: Target,
    node: &'a T,
}

impl<'a, T: DisplayForTarget> fmt::Display for FormatForTarget<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.fmt(self.target, f)
    }
}

/// A version of [`fmt::Display`] that can be customized for different
/// [`Target`]s.
pub trait DisplayForTarget: Sized {
    // The source span corresponding to this node.
    // fn span(&self) -> Span;

    /// Format this node as its SQLite3 equivalent. This would need to be replaced
    /// with something a bit more compiler-like to support full transpilation.
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    /// Convert this node to a string for the given target.
    fn to_string_for_target(&self, t: Target) -> String {
        format!("{}", t.f(self))
    }
}

impl<T: DisplayForTarget> DisplayForTarget for Box<T> {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(t, f)
    }
}

impl<T: DisplayForTarget> DisplayForTarget for Option<T> {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(node) = self {
            node.fmt(t, f)
        } else {
            Ok(())
        }
    }
}

impl<T: DisplayForTarget> DisplayForTarget for Vec<T> {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for node in self.iter() {
            node.fmt(t, f)?;
        }
        Ok(())
    }
}

/// Our basic token type. This is used for all punctuation and keywords.
#[derive(Clone, Debug)]
pub struct Token {
    pub span: Span,
    pub ws_offset: usize,
    pub text: String,
}

// We have a few functions that aren't used yet.
#[allow(dead_code)]
impl Token {
    /// Is this token a keyword?
    pub fn is_keyword(&self) -> bool {
        KEYWORDS.contains(self.text.to_ascii_uppercase().as_str())
    }

    /// Get the token's string.
    pub fn token_str(&self) -> &str {
        &self.text[0..self.ws_offset]
    }

    /// Get the token's span.
    pub fn token_span(&self) -> Span {
        self.span.start..self.span.start + self.ws_offset
    }

    /// Create a new token, changing the token string. This is useful where
    /// tokens need to be normalized. Does not fix `span`, which always
    /// corresponds to the original source code.
    pub fn with_token_str(&self, token_str: &str) -> Token {
        Token {
            span: self.span.clone(),
            ws_offset: token_str.len(),
            text: format!("{}{}", token_str, &self.text[self.ws_offset..]),
        }
    }

    /// "Erase" a token that we do not want to appear. This is useful for when
    /// we want to remove a token from the transpiled output for a specific
    /// database. However, we need to be careful not to accidentally connect the
    /// tokens before and after the erased token. We will insert whitespace if
    /// needed. Does not fix `span`, which always corresponds to the original
    /// source code.
    pub fn with_erased_token_str(&self) -> Token {
        if self.has_ws() {
            // We can just replace the token and rely on the existing
            // whitespace to separate any surrounding tokens.
            self.with_token_str("")
        } else {
            // We need to insert trailing whitespace.
            Token {
                span: self.span.clone(),
                ws_offset: 0,
                text: " ".to_owned(),
            }
        }
    }

    /// Does this token have trailing whitespace?
    pub fn has_ws(&self) -> bool {
        self.ws_offset < self.text.len()
    }

    /// Get the token's whitespace.
    pub fn ws_str(&self) -> &str {
        &self.text[self.ws_offset..]
    }

    /// Get the token's whitespace span.
    pub fn ws_span(&self) -> Span {
        self.span.start + self.ws_offset..self.span.end
    }

    /// Return only the whitespace portion of this token.
    pub fn ws_only(&self) -> Token {
        Token {
            span: self.ws_span(),
            ws_offset: 0,
            text: self.ws_str().to_owned(),
        }
    }

    /// Make sure this token has at least some trailing whitespace. This is
    /// used when replacing a punctuation token with an identifier token, to
    /// make sure we don't accidentally concatenate with the next token.
    pub fn ensure_ws(&self) -> Cow<Token> {
        if self.has_ws() {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(Token {
                span: self.span.clone(),
                ws_offset: self.text.len(),
                text: format!("{} ", self.text),
            })
        }
    }
}

impl DisplayForTarget for Token {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => write!(f, "{}", self.text),
            Target::SQLite3 => {
                // Write out the token itself.
                write!(f, "{}", self.token_str())?;

                // We need to fix `#` comments, which are not supported by SQLite3.
                let mut in_hash_comment = false;
                for c in self.ws_str().chars() {
                    if in_hash_comment {
                        write!(f, "{}", c)?;
                        if c == '\n' {
                            in_hash_comment = false;
                        }
                    } else if c == '#' {
                        write!(f, "--")?;
                        in_hash_comment = true;
                    } else {
                        write!(f, "{}", c)?;
                    }
                }
                Ok(())
            }
        }
    }
}

/// A vector which contains a list of nodes, along with any whitespace that
/// appears after a node but before any separating punctuation. This can be
/// used either with or without a final trailing separator.
#[derive(Debug)]
pub struct NodeVec<T: fmt::Debug> {
    pub nodes: Vec<T>,
    pub separators: Vec<Token>,
}

impl<T: fmt::Debug> Default for NodeVec<T> {
    fn default() -> Self {
        NodeVec {
            nodes: Vec::new(),
            separators: Vec::new(),
        }
    }
}

impl<T: fmt::Debug + DisplayForTarget> DisplayForTarget for NodeVec<T> {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, node) in self.nodes.iter().enumerate() {
            write!(f, "{}", t.f(node))?;
            if i < self.separators.len() {
                let sep = &self.separators[i];
                if i + 1 < self.nodes.len() {
                    write!(f, "{}", t.f(sep))?;
                } else {
                    // Trailing separator.
                    match t {
                        Target::BigQuery => write!(f, "{}", t.f(sep))?,
                        Target::SQLite3 => write!(f, "{}", t.f(&sep.with_erased_token_str()))?,
                    }
                }
            }
        }
        Ok(())
    }
}

/// An identifier, such as a column name.
#[derive(Debug)]
pub struct Identifier {
    /// Our original token.
    pub token: Token,

    /// Our unescaped text.
    pub text: String,
}

impl Identifier {
    /// Is this identifier a keyword?
    fn is_keyword(&self) -> bool {
        KEYWORDS.contains(self.text.to_ascii_uppercase().as_str())
    }

    /// Is this a valid C-style identifier?
    fn is_c_ident(&self) -> bool {
        let mut chars = self.text.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    // Does this identifier need to be quoted?
    fn needs_quotes(&self) -> bool {
        self.is_keyword() || !self.is_c_ident()
    }
}

impl DisplayForTarget for Identifier {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.needs_quotes() {
            match t {
                Target::BigQuery => {
                    write!(f, "`")?;
                    escape_for_bigquery(&self.text, f)?;
                    write!(f, "`{}", t.f(&self.token.ws_only()))
                }
                Target::SQLite3 => {
                    write!(f, "\"")?;
                    escape_for_sqlite3(&self.text, f)?;
                    write!(f, "\"{}", t.f(&self.token.ws_only()))
                }
            }
        } else {
            write!(f, "{}{}", self.text, t.f(&self.token.ws_only()))
        }
    }
}

/// Format `s` as a string literal for BigQuery. Does not include any
/// surrounding quotes, so you can use it with identifiers or strings.
fn escape_for_bigquery(s: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for c in s.chars() {
        match c {
            '\\' => write!(f, "\\\\")?,
            '"' => write!(f, "\\\"")?,
            '\'' => write!(f, "\\\'")?,
            '`' => write!(f, "\\`")?,
            '\n' => write!(f, "\\n")?,
            '\r' => write!(f, "\\r")?,
            _ if c.is_ascii_graphic() || c == ' ' => write!(f, "{}", c)?,
            _ if c as u32 <= 0xFF => write!(f, "\\x{:02x}", c as u32)?,
            _ if c as u32 <= 0xFFFF => write!(f, "\\u{:04x}", c as u32)?,
            _ => write!(f, "\\U{:08x}", c as u32)?,
        }
    }
    Ok(())
}

/// Escape for SQLite3.
fn escape_for_sqlite3(s: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // TODO: Implement this for real.
    escape_for_bigquery(s, f)
}

/// A table name.
#[derive(Debug)]
pub enum TableName {
    ProjectDatasetTable {
        project: Identifier,
        dot1: Token,
        dataset: Identifier,
        dot2: Token,
        table: Identifier,
    },
    DatasetTable {
        dataset: Identifier,
        dot: Token,
        table: Identifier,
    },
    Table {
        table: Identifier,
    },
}

impl DisplayForTarget for TableName {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => match self {
                TableName::ProjectDatasetTable {
                    project,
                    dot1,
                    dataset,
                    dot2,
                    table,
                } => {
                    write!(
                        f,
                        "{}{}{}{}{}",
                        t.f(project),
                        t.f(dot1),
                        t.f(dataset),
                        t.f(dot2),
                        t.f(table)
                    )
                }
                TableName::DatasetTable {
                    dataset,
                    dot,
                    table,
                } => {
                    write!(f, "{}{}{}", t.f(dataset), t.f(dot), t.f(table))
                }
                TableName::Table { table } => write!(f, "{}", t.f(table)),
            },
            Target::SQLite3 => match self {
                TableName::ProjectDatasetTable {
                    project,
                    dataset,
                    table,
                    ..
                } => {
                    write!(f, "\"")?;
                    escape_for_sqlite3(&project.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&dataset.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&table.text, f)?;
                    write!(f, "\"{}", t.f(&table.token.ws_only()))
                }
                TableName::DatasetTable { dataset, table, .. } => {
                    write!(f, "\"")?;
                    escape_for_sqlite3(&dataset.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&table.text, f)?;
                    write!(f, "\"{}", t.f(&table.token.ws_only()))
                }
                TableName::Table { table } => write!(f, "{}", t.f(table)),
            },
        }
    }
}

/// A table and a column name.
#[derive(Debug)]
pub struct TableAndColumnName {
    pub table_name: TableName,
    pub dot: Token,
    pub column_name: Identifier,
}

impl DisplayForTarget for TableAndColumnName {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.table_name),
            t.f(&self.dot),
            t.f(&self.column_name)
        )
    }
}

/// An entire SQL program.
#[derive(Debug)]
pub struct SqlProgram {
    /// Any whitespace that appears before the first statement. This is represented
    /// as a token with an empty `token_str()`.
    pub leading_ws: Token,

    /// For now, just handle single statements; BigQuery DDL is messy and maybe
    /// out of scope.
    pub statement: Statement,
}

impl DisplayForTarget for SqlProgram {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.leading_ws), t.f(&self.statement))
    }
}

/// A statement in our abstract syntax tree.
#[derive(Debug)]
pub enum Statement {
    Query(QueryStatement),
    DeleteFrom(DeleteFromStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
}

impl DisplayForTarget for Statement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Query(s) => write!(f, "{}", t.f(s)),
            Statement::DeleteFrom(s) => write!(f, "{}", t.f(s)),
            Statement::CreateTable(s) => write!(f, "{}", t.f(s)),
            Statement::CreateView(s) => write!(f, "{}", t.f(s)),
            Statement::DropView(s) => write!(f, "{}", t.f(s)),
        }
    }
}

/// A query statement. This exists mainly because it's in the official grammar.
#[derive(Debug)]
pub struct QueryStatement {
    pub query_expression: QueryExpression,
}

impl DisplayForTarget for QueryStatement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.query_expression))
    }
}

/// A query expression is a `SELECT` statement, plus some other optional
/// surrounding things. See the [official grammar][]. This is where CTEs and
/// similar things hook into the grammar.
///
/// [official grammar]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#sql_syntax.
#[derive(Debug)]
pub enum QueryExpression {
    SelectExpression(SelectExpression),
    Nested {
        paren1: Token,
        query: Box<QueryStatement>,
        paren2: Token,
    },
    With {
        with_token: Token,
        ctes: NodeVec<CommonTableExpression>,
        query: Box<QueryStatement>,
    },
    SetOperation {
        left: Box<QueryExpression>,
        set_operator: SetOperator,
        right: Box<QueryExpression>,
    },
}

impl DisplayForTarget for QueryExpression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryExpression::SelectExpression(s) => write!(f, "{}", t.f(s)),
            QueryExpression::Nested {
                paren1,
                query,
                paren2,
            } if t == Target::SQLite3 => {
                // Throw in extra spaces for the erased parens.
                write!(
                    f,
                    "{}{}{}",
                    t.f(&paren1.with_erased_token_str()),
                    t.f(query),
                    t.f(&paren2.with_erased_token_str())
                )
            }
            QueryExpression::Nested {
                paren1,
                query,
                paren2,
            } => write!(f, "{}{}{}", t.f(paren1), t.f(query), t.f(paren2)),
            QueryExpression::With {
                with_token,
                ctes,
                query,
            } => {
                write!(f, "{}{}{}", t.f(with_token), t.f(ctes), t.f(query))
            }
            QueryExpression::SetOperation {
                left: query1,
                set_operator,
                right: query2,
            } => {
                write!(f, "{}{}{}", t.f(query1), t.f(set_operator), t.f(query2))
            }
        }
    }
}

/// Common table expressions (CTEs).
#[derive(Debug)]
pub struct CommonTableExpression {
    pub name: Identifier,
    pub as_token: Token,
    pub paren1: Token,
    pub query: Box<QueryStatement>,
    pub paren2: Token,
}

impl DisplayForTarget for CommonTableExpression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}{}",
            t.f(&self.name),
            t.f(&self.as_token),
            t.f(&self.paren1),
            t.f(&self.query),
            t.f(&self.paren2)
        )
    }
}

/// Set operators.
#[derive(Debug)]
pub enum SetOperator {
    UnionAll {
        union_token: Token,
        all_token: Token,
    },
    UnionDistinct {
        union_token: Token,
        distinct_token: Token,
    },
    IntersectDistinct {
        intersect_token: Token,
        distinct_token: Token,
    },
    ExceptDistinct {
        except_token: Token,
        distinct_token: Token,
    },
}

impl DisplayForTarget for SetOperator {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => match self {
                SetOperator::UnionAll {
                    union_token,
                    all_token,
                } => write!(f, "{}{}", t.f(union_token), t.f(all_token)),
                SetOperator::UnionDistinct {
                    union_token,
                    distinct_token,
                } => write!(f, "{}{}", t.f(union_token), t.f(distinct_token)),
                SetOperator::IntersectDistinct {
                    intersect_token,
                    distinct_token,
                } => write!(f, "{}{}", t.f(intersect_token), t.f(distinct_token)),
                SetOperator::ExceptDistinct {
                    except_token,
                    distinct_token,
                } => write!(f, "{}{}", t.f(except_token), t.f(distinct_token)),
            },
            // SQLite3 only supports `UNION` and `INTERSECT`. We'll keep the
            // whitespace from the first token in those cases. In other cases,
            // we'll substitute `UNION` with a comment saying what it really
            // should be.
            Target::SQLite3 => match self {
                SetOperator::UnionAll { union_token, .. } => write!(f, "{}", t.f(union_token)),
                SetOperator::UnionDistinct { union_token, .. } => {
                    write!(
                        f,
                        "{}",
                        t.f(&union_token.with_token_str("UNION/*DISTINCT*/"))
                    )
                }
                SetOperator::IntersectDistinct {
                    intersect_token, ..
                } => write!(f, "{}", t.f(intersect_token)),
                SetOperator::ExceptDistinct { except_token, .. } => {
                    write!(
                        f,
                        "{}",
                        t.f(&except_token.with_token_str("UNION/*EXCEPT DISTINCT*/"))
                    )
                }
            },
        }
    }
}

/// A `SELECT` expression.
#[derive(Debug)]
pub struct SelectExpression {
    pub select_options: SelectOptions,
    pub select_list: SelectList,
    pub from_clause: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupBy>,
    // pub having: Option<Having>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

impl DisplayForTarget for SelectExpression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}{}{}{}",
            t.f(&self.select_options),
            t.f(&self.select_list),
            t.f(&self.from_clause),
            t.f(&self.where_clause),
            t.f(&self.group_by),
            t.f(&self.order_by),
            t.f(&self.limit),
        )
    }
}

/// The head of a `SELECT`, including any modifiers.
#[derive(Debug)]
pub struct SelectOptions {
    pub select_token: Token,
    pub distinct: Option<Distinct>,
}

impl DisplayForTarget for SelectOptions {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.select_token), t.f(&self.distinct))
    }
}

/// The `DISTINCT` modifier.
#[derive(Debug)]
pub struct Distinct {
    pub distinct_token: Token,
}

impl DisplayForTarget for Distinct {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.distinct_token))
    }
}

/// The list of columns in a `SELECT` statement.
///
/// # Official grammar
///
/// We'll implement this as needed.
///
/// ```text
/// select_list:
///   { select_all | select_expression } [, ...]
///
///   select_all:
///     [ expression. ]*
///     [ EXCEPT ( column_name [, ...] ) ]
///     [ REPLACE ( expression [ AS ] column_name [, ...] ) ]
///   
///   select_expression:
///     expression [ [ AS ] alias ]
/// ```
#[derive(Debug)]
pub struct SelectList {
    pub items: NodeVec<SelectListItem>,
}

impl DisplayForTarget for SelectList {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.items))
    }
}

/// A single item in a `SELECT` list.
#[derive(Debug)]
pub enum SelectListItem {
    /// An expression, optionally with an alias.
    Expression {
        expression: Expression,
        alias: Option<Alias>,
    },
    /// A `*` wildcard.
    Wildcard { star: Token, except: Option<Except> },
    /// A `table.*` wildcard.
    TableNameWildcard {
        table_name: TableName,
        dot: Token,
        star: Token,
        except: Option<Except>,
    },
}

impl DisplayForTarget for SelectListItem {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectListItem::Expression {
                expression, alias, ..
            } => {
                write!(f, "{}{}", t.f(expression), t.f(alias))
            }
            SelectListItem::Wildcard { star, except } => write!(f, "{}{}", &t.f(star), t.f(except)),
            SelectListItem::TableNameWildcard {
                table_name,
                dot,
                star,
                except,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(table_name),
                    t.f(dot),
                    t.f(star),
                    t.f(except),
                )
            }
        }
    }
}

/// An `EXCEPT` clause.
#[derive(Debug)]
pub struct Except {
    pub except_token: Token,
    pub paren1: Token,
    pub columns: NodeVec<Identifier>,
    pub paren2: Token,
}

impl DisplayForTarget for Except {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(&self.except_token),
                    t.f(&self.paren1),
                    t.f(&self.columns),
                    t.f(&self.paren2)
                )
            }
            Target::SQLite3 => {
                // TODO: Implement `EXCEPT` by inspecting the database schema.
                // For now, erase it.
                Ok(())
            }
        }
    }
}

/// An SQL expression.
#[derive(Debug)]
pub enum Expression {
    Literal {
        token: Token,
        value: LiteralValue,
    },
    Null {
        null_token: Token,
    },
    ColumnName(Identifier),
    TableAndColumnName(TableAndColumnName),
    Cast {
        cast_token: Token,
        paren1: Token,
        expression: Box<Expression>,
        as_token: Token,
        data_type: DataType,
        paren2: Token,
    },
    Is {
        left: Box<Expression>,
        is_token: Token,
        right: Box<Expression>,
    },
    IsNot {
        left: Box<Expression>,
        is_token: Token,
        not_token: Token, // TODO: Merge into `Is`?
        right: Box<Expression>,
    },
    In {
        left: Box<Expression>,
        not_token: Option<Token>,
        in_token: Token,
        paren1: Token,
        value_set: InValueSet,
        paren2: Token,
    },
    And {
        left: Box<Expression>,
        and_token: Token,
        right: Box<Expression>,
    },
    Or {
        left: Box<Expression>,
        or_token: Token,
        right: Box<Expression>,
    },
    Not {
        not_token: Token,
        expression: Box<Expression>,
    },
    If {
        if_token: Token,
        paren1: Token,
        condition: Box<Expression>,
        comma1: Token,
        then_expression: Box<Expression>,
        comma2: Token,
        else_expression: Box<Expression>,
        paren2: Token,
    },
    Case {
        case_token: Token,
        when_clauses: Vec<CaseWhenClause>,
        else_clause: Option<CaseElseClause>,
        end_token: Token,
    },
    Binop {
        left: Box<Expression>,
        op_token: Token,
        right: Box<Expression>,
    },
    Parens {
        paren1: Token,
        expression: Box<Expression>,
        paren2: Token,
    },
    Array(ArrayExpression),
    Count(CountExpression),
    FunctionCall(FunctionCall),
}

impl Expression {
    /// Create a new binary operator expression.
    fn binop(left: Expression, op_token: Token, right: Expression) -> Expression {
        Expression::Binop {
            left: Box::new(left),
            op_token,
            right: Box::new(right),
        }
    }
}

impl DisplayForTarget for Expression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // SQLite3 does not support `TRUE` or `FALSE`.
            Expression::Literal {
                token,
                value: LiteralValue::Bool(b),
            } if t == Target::SQLite3 => {
                write!(
                    f,
                    "{}",
                    t.f(token
                        .with_token_str(if *b { "1" } else { "0" })
                        .ensure_ws()
                        .as_ref())
                )
            }
            Expression::Literal { token, .. } => write!(f, "{}", t.f(token)),
            Expression::Null { null_token } => write!(f, "{}", t.f(null_token)),
            Expression::ColumnName(ident) => write!(f, "{}", t.f(ident)),
            Expression::TableAndColumnName(table_and_column_name) => {
                write!(f, "{}", t.f(table_and_column_name))
            }
            Expression::Cast {
                cast_token,
                paren1,
                expression,
                as_token,
                data_type,
                paren2,
            } => {
                write!(
                    f,
                    "{}{}{}{}{}{}",
                    t.f(cast_token),
                    t.f(paren1),
                    t.f(expression),
                    t.f(as_token),
                    t.f(data_type),
                    t.f(paren2)
                )
            }
            Expression::Is {
                left,
                is_token,
                right,
            } => {
                write!(f, "{}{}{}", t.f(left), t.f(is_token), t.f(right))
            }
            Expression::IsNot {
                left,
                is_token,
                not_token,
                right,
            } => write!(
                f,
                "{}{}{}{}",
                t.f(left),
                t.f(is_token),
                t.f(not_token),
                t.f(right)
            ),
            Expression::In {
                left,
                not_token,
                in_token,
                paren1,
                value_set,
                paren2,
            } => {
                write!(
                    f,
                    "{}{}{}{}{}{}",
                    t.f(left),
                    t.f(not_token),
                    t.f(in_token),
                    t.f(paren1),
                    t.f(value_set),
                    t.f(paren2)
                )
            }
            Expression::And {
                left,
                and_token,
                right,
            } => write!(f, "{}{}{}", t.f(left), t.f(and_token), t.f(right)),
            Expression::Or {
                left,
                or_token,
                right,
            } => write!(f, "{}{}{}", t.f(left), t.f(or_token), t.f(right)),
            Expression::Not {
                not_token,
                expression,
            } => write!(f, "{}{}", t.f(not_token), t.f(expression)),
            Expression::If {
                if_token,
                paren1,
                condition,
                comma1,
                then_expression,
                comma2,
                else_expression,
                paren2,
            } => match t {
                Target::BigQuery => write!(
                    f,
                    "{}{}{}{}{}{}{}{}",
                    t.f(if_token),
                    t.f(paren1),
                    t.f(condition),
                    t.f(comma1),
                    t.f(then_expression),
                    t.f(comma2),
                    t.f(else_expression),
                    t.f(paren2)
                ),
                // SQLite3 only supports `CASE` expressions, not `IF`.
                Target::SQLite3 => write!(
                    f,
                    "{}WHEN {} THEN {} ELSE {} {}",
                    t.f(if_token.with_token_str("CASE").ensure_ws().as_ref()),
                    t.f(condition),
                    t.f(then_expression),
                    t.f(else_expression),
                    t.f(paren2.with_token_str("END").ensure_ws().as_ref()),
                ),
            },
            Expression::Case {
                case_token,
                when_clauses,
                else_clause,
                end_token,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(case_token),
                    t.f(when_clauses),
                    t.f(else_clause),
                    t.f(end_token),
                )
            }
            Expression::Binop {
                left,
                op_token,
                right,
            } => write!(f, "{}{}{}", t.f(left), t.f(op_token), t.f(right)),
            Expression::Parens {
                paren1,
                expression,
                paren2,
            } => {
                write!(f, "{}{}{}", t.f(paren1), t.f(expression), t.f(paren2))
            }
            Expression::Array(array_expression) => {
                write!(f, "{}", t.f(array_expression))
            }
            Expression::Count(count_expression) => write!(f, "{}", t.f(count_expression)),
            Expression::FunctionCall(function_call) => {
                write!(f, "{}", t.f(function_call))
            }
        }
    }
}

/// A literal value.
#[derive(Debug)]
pub enum LiteralValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
}

/// A value set for an `IN` expression.
#[derive(Debug)]
pub enum InValueSet {
    QueryExpression(Box<QueryExpression>),
    ExpressionList(NodeVec<Expression>),
}

impl DisplayForTarget for InValueSet {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InValueSet::QueryExpression(query_expression) => {
                write!(f, "{}", t.f(query_expression))
            }
            InValueSet::ExpressionList(expressions) => {
                write!(f, "{}", t.f(expressions))
            }
        }
    }
}

/// An `ARRAY` expression. This takes a bunch of different forms in BigQuery.
///
/// Not all combinations of our fields are valid. For example, we can't have
/// a missing `ARRAY` and a `delim1` of `(`. We'll let the parser handle that.
#[derive(Debug)]
pub struct ArrayExpression {
    pub array_token: Option<Token>,
    pub element_type: Option<ArrayElementType>,
    pub delim1: Token,
    pub elements: NodeVec<Expression>,
    pub delim2: Token,
}

impl DisplayForTarget for ArrayExpression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => {
                write!(
                    f,
                    "{}{}{}{}{}",
                    t.f(&self.array_token),
                    t.f(&self.element_type),
                    t.f(&self.delim1),
                    t.f(&self.elements),
                    t.f(&self.delim2),
                )
            }
            Target::SQLite3 => {
                if let Some(array_token) = &self.array_token {
                    write!(f, "{}", t.f(array_token))?;
                } else {
                    write!(f, "ARRAY")?;
                }
                write!(
                    f,
                    "{}{}{}",
                    t.f(&self.delim1.with_token_str("(")),
                    t.f(&self.elements),
                    t.f(&self.delim2.with_token_str(")"))
                )
            }
        }
    }
}

/// The type of the elements in an `ARRAY` expression.
#[derive(Debug)]
pub struct ArrayElementType {
    pub lt: Token,
    pub elem_type: DataType,
    pub gt: Token,
}

impl DisplayForTarget for ArrayElementType {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.lt),
            t.f(&self.elem_type),
            t.f(&self.gt)
        )
    }
}

/// A `COUNT` expression.
#[derive(Debug)]
pub enum CountExpression {
    CountStar {
        count_token: Token,
        paren1: Token,
        star: Token,
        paren2: Token,
    },
    CountExpression {
        count_token: Token,
        paren1: Token,
        distinct: Option<Distinct>,
        expression: Box<Expression>,
        paren2: Token,
    },
}

impl DisplayForTarget for CountExpression {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CountExpression::CountStar {
                count_token,
                paren1,
                star,
                paren2,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(count_token),
                    t.f(paren1),
                    t.f(star),
                    t.f(paren2)
                )
            }
            CountExpression::CountExpression {
                count_token,
                paren1,
                distinct,
                expression,
                paren2,
            } => {
                write!(
                    f,
                    "{}{}{}{}{}",
                    t.f(count_token),
                    t.f(paren1),
                    t.f(distinct),
                    t.f(expression),
                    t.f(paren2)
                )
            }
        }
    }
}

/// A `CASE WHEN` clause.
#[derive(Debug)]
pub struct CaseWhenClause {
    pub when_token: Token,
    pub condition: Box<Expression>,
    pub then_token: Token,
    pub result: Box<Expression>,
}

impl DisplayForTarget for CaseWhenClause {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}",
            t.f(&self.when_token),
            t.f(&self.condition),
            t.f(&self.then_token),
            t.f(&self.result)
        )
    }
}

/// A `CASE ELSE` clause.
#[derive(Debug)]
pub struct CaseElseClause {
    pub else_token: Token,
    pub result: Box<Expression>,
}

impl DisplayForTarget for CaseElseClause {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.else_token), t.f(&self.result))
    }
}

/// A function call.
#[derive(Debug)]
pub struct FunctionCall {
    pub name: Identifier,
    pub paren1: Token,
    pub args: NodeVec<Expression>,
    pub paren2: Token,
    pub over_clause: Option<OverClause>,
}

impl DisplayForTarget for FunctionCall {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}{}",
            t.f(&self.name),
            t.f(&self.paren1),
            t.f(&self.args),
            t.f(&self.paren2),
            t.f(&self.over_clause),
        )
    }
}

/// An `OVER` clause for a window function.
///
/// See the [official grammar][]. We only implement part of this.
///
/// [official grammar]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#syntax
#[derive(Debug)]
pub struct OverClause {
    pub over_token: Token,
    pub paren1: Token,
    pub partition_by: Option<PartitionBy>,
    pub order_by: Option<OrderBy>,
    pub paren2: Token,
}

impl DisplayForTarget for OverClause {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}{}",
            t.f(&self.over_token),
            t.f(&self.paren1),
            t.f(&self.partition_by),
            t.f(&self.order_by),
            t.f(&self.paren2),
        )
    }
}

/// A `PARTITION BY` clause for a window function.
#[derive(Debug)]
pub struct PartitionBy {
    pub partition_token: Token,
    pub by_token: Token,
    pub expressions: NodeVec<Expression>,
}

impl DisplayForTarget for PartitionBy {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.partition_token),
            t.f(&self.by_token),
            t.f(&self.expressions),
        )
    }
}

/// An `ORDER BY` clause.
#[derive(Debug)]
pub struct OrderBy {
    pub order_token: Token,
    pub by_token: Token,
    pub items: NodeVec<OrderByItem>,
}

impl DisplayForTarget for OrderBy {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.order_token),
            t.f(&self.by_token),
            t.f(&self.items),
        )
    }
}

/// An item in an `ORDER BY` clause.
#[derive(Debug)]
pub struct OrderByItem {
    pub expression: Expression,
    pub asc_desc: Option<AscDesc>,
}

impl DisplayForTarget for OrderByItem {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.expression), t.f(&self.asc_desc))
    }
}

/// An `ASC` or `DESC` modifier.
#[derive(Debug)]
pub struct AscDesc(Token);

impl DisplayForTarget for AscDesc {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.0))
    }
}

/// A `LIMIT` clause.
#[derive(Debug)]
pub struct Limit {
    pub limit_token: Token,
    pub value: Box<Expression>,
}

impl DisplayForTarget for Limit {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.limit_token), t.f(&self.value))
    }
}

/// Data types.
#[derive(Debug)]
pub enum DataType {
    Bool(Token),
    Int64(Token),
    Float64(Token),
    String(Token),
    Datetime(Token),
    Array {
        array_token: Token,
        lt: Token,
        data_type: Box<DataType>,
        gt: Token,
    },
}

impl DisplayForTarget for DataType {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => match self {
                DataType::Bool(token)
                | DataType::Int64(token)
                | DataType::Float64(token)
                | DataType::String(token)
                | DataType::Datetime(token) => {
                    write!(f, "{}", t.f(token))
                }
                DataType::Array {
                    array_token,
                    lt,
                    data_type,
                    gt,
                } => write!(
                    f,
                    "{}{}{}{}",
                    t.f(array_token),
                    t.f(lt),
                    t.f(data_type),
                    t.f(gt)
                ),
            },
            Target::SQLite3 => match self {
                DataType::Bool(token) | DataType::Int64(token) => {
                    write!(f, "{}", t.f(&token.with_token_str("INTEGER")))
                }
                DataType::Float64(token) => write!(f, "{}", t.f(&token.with_token_str("REAL"))),
                DataType::String(token) | DataType::Datetime(token) => {
                    write!(f, "{}", t.f(&token.with_token_str("TEXT")))
                }
                DataType::Array { gt, .. } => {
                    write!(
                        f,
                        "{}",
                        // Force whitespace because we're replacing a
                        // punctuation token with an identifier token.
                        t.f(gt.with_token_str("/*JSON*/TEXT").ensure_ws().as_ref())
                    )
                }
            },
        }
    }
}

/// An `AS` alias.
#[derive(Debug)]
pub struct Alias {
    pub as_token: Option<Token>,
    pub ident: Identifier,
}

impl DisplayForTarget for Alias {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.as_token), t.f(&self.ident))
    }
}

/// The `FROM` clause.
///
/// TODO: We're keeping this simple for now.
#[derive(Debug)]
pub struct FromClause {
    pub from_token: Token,
    pub from_item: FromItem,
    pub join_operations: Vec<JoinOperation>,
}

impl DisplayForTarget for FromClause {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.from_token), t.f(&self.from_item))?;
        for join_operation in &self.join_operations {
            write!(f, "{}", t.f(join_operation))?;
        }
        Ok(())
    }
}

/// Items which may appear in a `FROM` clause.
#[derive(Debug)]
pub enum FromItem {
    /// A table name, optionally with an alias.
    TableName {
        table_name: TableName,
        alias: Option<Alias>,
    },
    /// A subquery, optionally with an alias. These parens belong here in the
    /// grammar; they're different from the ones in [`QueryExpression`].
    Subquery {
        paren1: Token,
        query: Box<QueryStatement>,
        paren2: Token,
        alias: Option<Alias>,
    },
    /// A `UNNEST` clause.
    Unnest {
        unnest_token: Token,
        paren1: Token,
        expression: Box<Expression>,
        paren2: Token,
        alias: Option<Alias>,
    },
}

impl DisplayForTarget for FromItem {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FromItem::TableName { table_name, alias } => {
                write!(f, "{}{}", t.f(table_name), t.f(alias))
            }
            FromItem::Subquery {
                paren1,
                query,
                paren2,
                alias,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(paren1),
                    t.f(query),
                    t.f(paren2),
                    t.f(alias)
                )
            }
            FromItem::Unnest {
                unnest_token,
                paren1,
                expression,
                paren2,
                alias,
            } => {
                write!(
                    f,
                    "{}{}{}{}{}",
                    t.f(unnest_token),
                    t.f(paren1),
                    t.f(expression),
                    t.f(paren2),
                    t.f(alias)
                )
            }
        }
    }
}

/// A join operation.
#[derive(Debug)]
pub enum JoinOperation {
    /// A `JOIN` clause.
    ConditionJoin {
        join_type: JoinType,
        join_token: Token,
        from_item: FromItem,
        operator: ConditionJoinOperator,
    },
    /// A `CROSS JOIN` clause.
    CrossJoin {
        cross_token: Token,
        join_token: Token,
        from_item: FromItem,
    },
}

impl DisplayForTarget for JoinOperation {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinOperation::ConditionJoin {
                join_type,
                join_token,
                from_item,
                operator,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(join_type),
                    t.f(join_token),
                    t.f(from_item),
                    t.f(operator)
                )
            }
            JoinOperation::CrossJoin {
                cross_token,
                join_token,
                from_item,
            } => {
                write!(
                    f,
                    "{}{}{}",
                    t.f(cross_token),
                    t.f(join_token),
                    t.f(from_item)
                )
            }
        }
    }
}

/// The type of a join.
#[derive(Debug)]
pub enum JoinType {
    Inner {
        inner_token: Option<Token>,
    },
    Left {
        left_token: Token,
        outer_token: Option<Token>,
    },
    Right {
        right_token: Token,
        outer_token: Option<Token>,
    },
    Full {
        full_token: Token,
        outer_token: Option<Token>,
    },
}

impl DisplayForTarget for JoinType {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Clean this up with support for printing `Option<T>`.
        match self {
            JoinType::Inner { inner_token } => {
                write!(f, "{}", t.f(inner_token))
            }
            JoinType::Left {
                left_token,
                outer_token,
            } => {
                write!(f, "{}{}", t.f(left_token), t.f(outer_token))
            }
            JoinType::Right {
                right_token,
                outer_token,
            } => {
                write!(f, "{}{}", t.f(right_token), t.f(outer_token))
            }
            JoinType::Full {
                full_token,
                outer_token,
            } => {
                write!(f, "{}{}", t.f(full_token), t.f(outer_token))
            }
        }
    }
}

/// The condition used for a `JOIN`.
#[derive(Debug)]
pub enum ConditionJoinOperator {
    Using {
        using_token: Token,
        paren1: Token,
        column_names: NodeVec<Identifier>,
        paren2: Token,
    },
    On {
        on_token: Token,
        expression: Expression,
    },
}

impl DisplayForTarget for ConditionJoinOperator {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConditionJoinOperator::Using {
                using_token,
                paren1,
                column_names,
                paren2,
            } => {
                write!(
                    f,
                    "{}{}{}{}",
                    t.f(using_token),
                    t.f(paren1),
                    t.f(column_names),
                    t.f(paren2),
                )
            }
            ConditionJoinOperator::On {
                on_token,
                expression,
            } => {
                write!(f, "{}{}", t.f(on_token), t.f(expression))
            }
        }
    }
}

/// A `WHERE` clause.
#[derive(Debug)]
pub struct WhereClause {
    pub where_token: Token,
    pub expression: Expression,
}

impl DisplayForTarget for WhereClause {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.where_token), t.f(&self.expression))
    }
}

/// A `GROUP BY` clause.
#[derive(Debug)]
pub struct GroupBy {
    pub group_token: Token,
    pub by_token: Token,
    pub expressions: NodeVec<Expression>,
}

impl DisplayForTarget for GroupBy {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.group_token),
            t.f(&self.by_token),
            t.f(&self.expressions)
        )
    }
}

/// A `DELETE FROM` statement.
#[derive(Debug)]
pub struct DeleteFromStatement {
    pub delete_token: Token,
    pub from_token: Token,
    pub table_name: TableName,
    pub alias: Option<Alias>,
    pub where_clause: Option<WhereClause>,
}

impl DisplayForTarget for DeleteFromStatement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}{}{}",
            t.f(&self.delete_token),
            t.f(&self.from_token),
            t.f(&self.table_name),
            t.f(&self.alias),
            t.f(&self.where_clause)
        )
    }
}

/// A `CREATE TABLE` statement.
#[derive(Debug)]
pub struct CreateTableStatement {
    pub create_token: Token,
    pub or_replace: Option<OrReplace>,
    pub table_token: Token,
    pub table_name: TableName,
    pub definition: CreateTableDefinition,
}

impl DisplayForTarget for CreateTableStatement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 if self.or_replace.is_some() => {
                // We need to convert this to a `DROP TABLE IF EXISTS` statement.
                write!(f, "DROP TABLE IF EXISTS {};", t.f(&self.table_name))?;
            }
            _ => {}
        }
        write!(
            f,
            "{}{}{}{}{}",
            t.f(&self.create_token),
            t.f(&self.or_replace),
            t.f(&self.table_token),
            t.f(&self.table_name),
            t.f(&self.definition),
        )
    }
}

/// A `CREATE VIEW` statement.
#[derive(Debug)]
pub struct CreateViewStatement {
    pub create_token: Token,
    pub or_replace: Option<OrReplace>,
    pub view_token: Token,
    pub view_name: TableName,
    // TODO: Factor out shared `AS` code from [`CreateTableDefinition`].
    pub as_token: Token,
    pub query: QueryStatement,
}

impl DisplayForTarget for CreateViewStatement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 if self.or_replace.is_some() => {
                // We need to convert add a `DROP VIEW IF EXISTS` statement.
                write!(f, "DROP VIEW IF EXISTS {};", t.f(&self.view_name))?;
            }
            _ => {}
        }

        write!(
            f,
            "{}{}{}{}{}{}",
            t.f(&self.create_token),
            t.f(&self.or_replace),
            t.f(&self.view_token),
            t.f(&self.view_name),
            t.f(&self.as_token),
            t.f(&self.query),
        )
    }
}

/// The `OR REPLACE` modifier.
#[derive(Debug)]
pub struct OrReplace {
    pub or_token: Token,
    pub replace_token: Token,
}

impl DisplayForTarget for OrReplace {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => write!(f, "{}{}", t.f(&self.or_token), t.f(&self.replace_token)),
            Target::SQLite3 => Ok(()),
        }
    }
}

/// The part of a `CREATE TABLE` statement that defines the columns.
#[derive(Debug)]
pub enum CreateTableDefinition {
    /// ( column_definition [, ...] )
    Columns {
        paren1: Token,
        columns: NodeVec<ColumnDefinition>,
        paren2: Token,
    },
    /// AS select_statement
    As {
        as_token: Token,
        query_statement: QueryStatement,
    },
}

impl DisplayForTarget for CreateTableDefinition {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateTableDefinition::Columns {
                paren1,
                columns,
                paren2,
            } => {
                write!(f, "{}{}{}", t.f(paren1), t.f(columns), t.f(paren2))
            }
            CreateTableDefinition::As {
                as_token,
                query_statement,
            } => {
                write!(f, "{}{}", t.f(as_token), t.f(query_statement))
            }
        }
    }
}

/// A column definition.
#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: DataType,
}

impl DisplayForTarget for ColumnDefinition {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.name), t.f(&self.data_type))
    }
}

/// A `DROP VIEW` statement.
#[derive(Debug)]
pub struct DropViewStatement {
    pub drop_token: Token,
    pub view_token: Token,
    pub view_name: TableName,
}

impl DisplayForTarget for DropViewStatement {
    fn fmt(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.drop_token),
            t.f(&self.view_token),
            t.f(&self.view_name)
        )
    }
}

#[derive(Debug)]
pub struct ParseError {
    pub source: peg::error::ParseError<peg::str::LineCol>,
    pub files: SimpleFile<&'static str, String>,
    pub diagnostic: Diagnostic<()>,
}

impl ParseError {
    pub fn emit(&self) -> Result<(), codespan_reporting::files::Error> {
        let writer = StandardStream::stderr(ColorChoice::Auto);
        let config = term::Config::default();
        let result = term::emit(&mut writer.lock(), &config, &self.files, &self.diagnostic);
        result
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parser error: expected {}", self.source)
    }
}

impl error::Error for ParseError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Parse BigQuery SQL.
pub fn parse_sql(sql: &str) -> Result<SqlProgram, Box<ParseError>> {
    // Parse with or without tracing, as appropriate. The tracing code throws
    // off error positions, so we don't want to use it unless we're going to
    // use `pegviz` to visualize the parse.
    #[cfg(feature = "trace")]
    let result = sql_program::sql_program_traced(sql);
    #[cfg(not(feature = "trace"))]
    let result = sql_program::sql_program(sql);

    match result {
        Ok(sql_program) => Ok(sql_program),
        // Prepare a user-friendly error message.
        Err(e) => {
            let files = SimpleFile::new("input.sql", sql.to_string());
            let diagnostic = Diagnostic::error()
                .with_message("Failed to parse query")
                .with_labels(vec![Label::primary(
                    (),
                    e.location.offset..e.location.offset + 1,
                )
                .with_message(format!("expected {}", e.expected))]);
            Err(Box::new(ParseError {
                source: e,
                files,
                diagnostic,
            }))
        }
    }
}

// We use `rust-peg` to parse BigQuery SQL. `rustpeg` uses a [Parsing Expression
// Grammar](https://en.wikipedia.org/wiki/Parsing_expression_grammar), which is
// conceptually sort of like a recursive regex with named rules.
//
// PEG grammars have certain limitations, but `rust-peg` is rather nice and has
// macros that help with precedence and associativity, which are the main things
// that PEGs don't handle well. Also, I've used `rust-peg` for quite a few other
// projects.
//
// I also considered
// [`rust-sitter`](https://github.com/hydro-project/rust-sitter) which is a Rust
// port of the [Tree-sitter](https://tree-sitter.github.io/tree-sitter/)
// library. This is extremely good for writing "language servers" for IDEs, but
// it has a learning curve. And in many ways, it's less mature than `rust-peg`.
//
// Grammar notes:
//   - Most parsing ultimately goes through `Token`, which contains a single
//     source token and any trailing whitespace, plus a source position
//   - Keywords are matched using `k` (eg `k("SELECT")`) and other tokens using
//     `t` (eg `t("(")`). There is also a `token` rule which can be used to
//     wrap a more complex lexical rule.
//   - Tokens are saved into the AST, including whitespace. This allows us to
//     preserve the original formatting when manipulating the AST.
peg::parser! {
    /// We parse as much of BigQuery's "Standard SQL" as we can.
    pub grammar sql_program() for str {
        /// Alternate entry point for tracing the parse with `pegviz`.
        pub rule sql_program_traced() -> SqlProgram = traced(<sql_program()>)

        /// Main entry point.
        pub rule sql_program() -> SqlProgram
            = leading_ws:t("") statement:statement()
              { SqlProgram { leading_ws, statement } }

        rule statement() -> Statement
            = s:query_statement() { Statement::Query(s) }
            / d:delete_from_statement() { Statement::DeleteFrom(d) }
            / c:create_table_statement() { Statement::CreateTable(c) }
            / c:create_view_statement() { Statement::CreateView(c) }
            / d:drop_view_statement() { Statement::DropView(d) }

        rule query_statement() -> QueryStatement
            = query_expression:query_expression() { QueryStatement { query_expression } }

        rule delete_from_statement() -> DeleteFromStatement
            = delete_token:k("DELETE") from_token:k("FROM") table_name:table_name() alias:alias()? where_clause:where_clause()? {
                DeleteFromStatement {
                    delete_token,
                    from_token,
                    table_name,
                    alias,
                    where_clause,
                }
            }

        rule query_expression() -> QueryExpression = precedence! {
            left:(@) set_operator:set_operator() right:@ {
                QueryExpression::SetOperation {
                    left: Box::new(left), set_operator, right: Box::new(right)
                }
            }
            --
            select_expression:select_expression() { QueryExpression::SelectExpression(select_expression) }
            paren1:t("(") query:query_statement() paren2:t(")") {
                QueryExpression::Nested {
                    paren1,
                    query: Box::new(query),
                    paren2,
                }
            }
            with_token:k("WITH") ctes:sep_opt_trailing(<common_table_expression()>, ",") query:query_statement() {
                QueryExpression::With {
                    with_token,
                    ctes,
                    query: Box::new(query),
                }
            }
        }

        rule set_operator() -> SetOperator
            = union_token:k("UNION") all_token:k("ALL") { SetOperator::UnionAll { union_token, all_token } }
            / union_token:k("UNION") distinct_token:k("DISTINCT") { SetOperator::UnionDistinct { union_token, distinct_token } }
            / except_token:k("EXCEPT") distinct_token:k("DISTINCT") { SetOperator::ExceptDistinct { except_token, distinct_token } }
            / intersect_token:k("INTERSECT") distinct_token:k("DISTINCT") { SetOperator::IntersectDistinct { intersect_token, distinct_token } }

        rule common_table_expression() -> CommonTableExpression
            = name:ident() as_token:k("AS") paren1:t("(") query:query_statement() paren2:t(")") {
                CommonTableExpression {
                    name,
                    as_token,
                    paren1,
                    query: Box::new(query),
                    paren2,
                }
            }

        rule select_expression() -> SelectExpression
            = select_options:select_options()
              select_list:select_list()
              from_clause:from_clause()?
              where_clause:where_clause()?
              group_by:group_by()?
              order_by:order_by()?
              limit:limit()?
            {
                SelectExpression {
                    select_options,
                    select_list,
                    from_clause,
                    where_clause,
                    group_by,
                    order_by,
                    limit,
                }
            }

        rule select_options() -> SelectOptions
            = s:position!()
              select_token:k("SELECT")
              distinct:distinct()?
              e:position!()
            {
                SelectOptions {
                    select_token,
                    distinct,
                }
            }

        rule distinct() -> Distinct
            = distinct_token:k("DISTINCT") {
                Distinct { distinct_token }
            }

        rule select_list() -> SelectList
            = items:sep_opt_trailing(<select_list_item()>, ",") {
                SelectList { items }
            }

        rule select_list_item() -> SelectListItem
            = star:t("*") except:except()? {
                SelectListItem::Wildcard { star, except }
            }
            / table_name:table_name() dot:t(".") star:t("*") except:except()? {
                SelectListItem::TableNameWildcard { table_name, dot, star, except }
            }
            / s:position!() expression:expression() alias:alias()? e:position!() {
                SelectListItem::Expression { expression, alias }
            }

        rule except() -> Except
            = except_token:k("EXCEPT") paren1:t("(") columns:sep_opt_trailing(<ident()>, ",") paren2:t(")") {
                Except {
                    except_token,
                    paren1,
                    columns,
                    paren2,
                }
            }

        /// Expressions. See the [precedence table][] for details.
        ///
        /// [precedence table]: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#operator_precedence
        rule expression() -> Expression = precedence! {
            left:(@) or_token:k("OR") right:@ { Expression::Or { left: Box::new(left), or_token, right: Box::new(right) } }
            --
            left:(@) and_token:k("AND") right:@ { Expression::And { left: Box::new(left), and_token, right: Box::new(right) } }
            --
            not_token:k("NOT") expression:@ { Expression::Not { not_token, expression: Box::new(expression) } }
            --
            left:(@) is_token:k("IS") right:@ { Expression::Is { left: Box::new(left), is_token, right: Box::new(right) } }
            left:(@) is_token:k("IS") not_token:k("NOT") right:@ { Expression::IsNot { left: Box::new(left), is_token, not_token, right: Box::new(right) } }
            left:(@) not_token:k("NOT")? in_token:k("IN") paren1:t("(") value_set:in_value_set() paren2:t(")") {
                Expression::In {
                    left: Box::new(left),
                    not_token,
                    in_token,
                    paren1,
                    value_set,
                    paren2,
                }
            }
            left:(@) op_token:t("=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t("!=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t("<") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t("<=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t(">") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t(">=") right:@ { Expression::binop(left, op_token, right) }
            --
            left:(@) op_token:t("+") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t("-") right:@ { Expression::binop(left, op_token, right) }
            --
            left:(@) op_token:t("*") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:t("/") right:@ { Expression::binop(left, op_token, right) }
            --
            case_token:k("CASE") when_clauses:(case_when_clause()*) else_clause:case_else_clause()? end_token:k("END") {
                Expression::Case {
                    case_token,
                    when_clauses,
                    else_clause,
                    end_token,
                }
            }
            if_token:k("IF") paren1:t("(") condition:expression() comma1:t(",") then_expression:expression() comma2:t(",") else_expression:expression() paren2:t(")") {
                Expression::If {
                    if_token,
                    paren1,
                    condition: Box::new(condition),
                    comma1,
                    then_expression: Box::new(then_expression),
                    comma2,
                    else_expression: Box::new(else_expression),
                    paren2,
                }
            }
            array_expression:array_expression() { Expression::Array(array_expression) }
            count_expression:count_expression() { Expression::Count(count_expression) }
            function_call:function_call() { Expression::FunctionCall(function_call) }
            paren1:t("(") expression:expression() paren2:t(")") { Expression::Parens { paren1, expression: Box::new(expression), paren2 } }
            literal:literal() { literal }
            null_token:k("NULL") { Expression::Null { null_token } }
            table_and_column_name:table_and_column_name() {
                Expression::TableAndColumnName(table_and_column_name)
            }
            column_name:ident() { Expression::ColumnName(column_name) }
            cast_token:k("CAST") paren1:t("(") expression:expression() as_token:k("AS") data_type:data_type() paren2:t(")") {
                Expression::Cast {
                    cast_token,
                    paren1,
                    expression: Box::new(expression),
                    as_token,
                    data_type,
                    paren2,
                }
            }
        }

        rule literal() -> Expression
            // TODO: Check for other floating point notations.
            = quiet! { token:(k("TRUE") / k("FALSE")) {
                let value = LiteralValue::Bool(token.token_str() == "TRUE");
                Expression::Literal { token, value }
            } }
            / quiet! { token:token("float", <"-"? ['0'..='9']+ "." ['0'..='9']*>) {
                let value = LiteralValue::Float64(token.token_str().parse().unwrap());
                Expression::Literal { token, value }
            } }
            / quiet! { token:token("integer", <"-"? ['0'..='9']+>) {
                let value = LiteralValue::Int64(token.token_str().parse().unwrap());
                Expression::Literal { token, value }
            } }
            / quiet! { token:token("string", <"'" ([^ '\\' | '\''] / escape())* "'">) {
                // TODO: Unescape.
                let unescaped = token.token_str()[1..token.token_str().len() - 1].to_string();
                let value = LiteralValue::String(unescaped);
                Expression::Literal { token, value }
            } }
            / expected!("literal")

        rule in_value_set() -> InValueSet
            = query_expression:query_expression() { InValueSet::QueryExpression(Box::new(query_expression)) }
            / expressions:sep(<expression()>, ",") { InValueSet::ExpressionList(expressions) }

        rule case_when_clause() -> CaseWhenClause
            = when_token:k("WHEN") condition:expression() then_token:k("THEN") result:expression() {
                CaseWhenClause {
                    when_token,
                    condition: Box::new(condition),
                    then_token,
                    result: Box::new(result),
                }
            }

        rule case_else_clause() -> CaseElseClause
            = else_token:k("ELSE") result:expression() {
                CaseElseClause {
                    else_token,
                    result: Box::new(result),
                }
            }

        rule array_expression() -> ArrayExpression
            = delim1:t("[") elements:sep_opt_trailing(<expression()>, ",")? delim2:t("]") {
                ArrayExpression {
                    array_token: None,
                    element_type: None,
                    delim1,
                    elements: elements.unwrap_or_default(),
                    delim2,
                }
            }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:t("[") elements:sep_opt_trailing(<expression()>, ",")? delim2:t("]") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    elements: elements.unwrap_or_default(),
                    delim2,
                }
              }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:t("(") elements:sep_opt_trailing(<expression()>, ",")? delim2:t(")") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    elements: elements.unwrap_or_default(),
                    delim2,
                }
              }

        rule array_element_type() -> ArrayElementType
            = lt:t("<") elem_type:data_type() gt:t(">") {
                ArrayElementType { lt, elem_type, gt }
            }

        rule count_expression() -> CountExpression
            = count_token:k("COUNT") paren1:t("(") star:t("*") paren2:t(")") {
                CountExpression::CountStar {
                    count_token,
                    paren1,
                    star,
                    paren2,
                }
            }
            / count_token:k("COUNT") paren1:t("(") distinct:distinct()? expression:expression() paren2:t(")") {
                CountExpression::CountExpression {
                    count_token,
                    paren1,
                    distinct,
                    expression: Box::new(expression),
                    paren2,
                }
            }

        rule function_call() -> FunctionCall
            = name:ident() paren1:t("(") args:sep_opt_trailing(<expression()>, ",")? paren2:t(")") over_clause:over_clause()? {
                FunctionCall {
                    name,
                    paren1,
                    args: args.unwrap_or_default(),
                    paren2,
                    over_clause,
                }
            }

        rule over_clause() -> OverClause
            = over_token:k("OVER") paren1:t("(") partition_by:partition_by()? order_by:order_by()? paren2:t(")") {
                OverClause {
                    over_token,
                    paren1,
                    partition_by,
                    order_by,
                    paren2,
                }
            }

        rule partition_by() -> PartitionBy
            = partition_token:k("PARTITION") by_token:k("BY") expressions:sep(<expression()>, ",") {
                PartitionBy {
                    partition_token,
                    by_token,
                    expressions,
                }
            }

        rule order_by() -> OrderBy
            = order_token:k("ORDER") by_token:k("BY") items:sep(<order_by_item()>, ",") {
                OrderBy {
                    order_token,
                    by_token,
                    items,
                }
            }

        rule order_by_item() -> OrderByItem
            = expression:expression() asc_desc:asc_desc()? {
                OrderByItem {
                    expression,
                    asc_desc,
                }
            }

        rule asc_desc() -> AscDesc
            = token:(k("ASC") / k("DESC")) { AscDesc(token) }

        rule limit() -> Limit
            = limit_token:k("LIMIT") value:expression() {
                Limit {
                    limit_token,
                    value: Box::new(value),
                }
            }

        rule data_type() -> DataType
            = token:(k("BOOLEAN") / k("BOOL")) { DataType::Bool(token) }
            / token:k("INT64") { DataType::Int64(token) }
            / token:k("FLOAT64") { DataType::Float64(token) }
            / token:k("STRING") { DataType::String(token) }
            / token:k("DATETIME") { DataType::Datetime(token) }
            / array_token:k("ARRAY") lt:t("<") data_type:data_type() gt:t(">") {
                DataType::Array {
                    array_token,
                    lt,
                    data_type: Box::new(data_type),
                    gt,
                }
            }

        rule alias() -> Alias
            = as_token:(k("AS")?) ident:ident() {
                Alias { as_token, ident }
            }

        rule from_clause() -> FromClause
            = from_token:k("FROM") from_item:from_item()
              join_operations:join_operations()
            {
                FromClause {
                    from_token,
                    from_item,
                    join_operations,
                }
            }

        rule from_item() -> FromItem
            = table_name:table_name() alias:alias()? {
                FromItem::TableName { table_name, alias }
            }
            / paren1:t("(") query:query_statement() paren2:t(")") alias:alias()? {
                FromItem::Subquery {
                    paren1,
                    query: Box::new(query),
                    paren2,
                    alias,
                }
            }
            / unnest_token:k("UNNEST") paren1:t("(") expression:expression() paren2:t(")") alias:alias()? {
                FromItem::Unnest {
                    unnest_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                    alias,
                }
            }

        rule join_operations() -> Vec<JoinOperation>
            = join_operations:join_operation()* { join_operations }

        rule join_operation() -> JoinOperation
            = join_type:join_type() join_token:k("JOIN") from_item:from_item() operator:condition_join_operator() {
                JoinOperation::ConditionJoin {
                    join_type,
                    join_token,
                    from_item,
                    operator,
                }
            }
            / cross_token:k("CROSS") join_token:k("JOIN") from_item:from_item() {
                JoinOperation::CrossJoin {
                    cross_token,
                    join_token,
                    from_item,
                }
            }

        rule join_type() -> JoinType
            = left_token:k("LEFT") outer_token:(k("OUTER")?) {
                JoinType::Left { left_token, outer_token }
            }
            / right_token:k("RIGHT") outer_token:(k("OUTER")?) {
                JoinType::Right { right_token, outer_token }
            }
            / full_token:k("FULL") outer_token:(k("OUTER")?) {
                JoinType::Full { full_token, outer_token }
            }
            / inner_token:(k("INNER")?) e:position!() {
                JoinType::Inner { inner_token }
            }

        rule condition_join_operator() -> ConditionJoinOperator
            = using_token:k("USING") paren1:t("(") column_names:sep(<ident()>, ",") paren2:t(")") {
                ConditionJoinOperator::Using {
                    using_token,
                    paren1,
                    column_names,
                    paren2,
                }
            }
            / on_token:k("ON") expression:expression() {
                ConditionJoinOperator::On {
                    on_token,
                    expression,
                }
            }

        rule where_clause() -> WhereClause
            = where_token:k("WHERE") expression:expression() {
                WhereClause {
                    where_token,
                    expression,
                }
            }

        rule group_by() -> GroupBy
            = group_token:k("GROUP") by_token:k("BY") expressions:sep(<expression()>, ",") {
                GroupBy {
                    group_token,
                    by_token,
                    expressions,
                }
            }

        rule create_table_statement() -> CreateTableStatement
            = create_token:k("CREATE") or_replace:or_replace()?
              table_token:k("TABLE") table_name:table_name()
              definition:create_table_definition()
              e:position!()
            {
                CreateTableStatement {
                    create_token,
                    or_replace,
                    table_token,
                    table_name,
                    definition,
                }
            }

        rule create_view_statement() -> CreateViewStatement
            = create_token:k("CREATE") or_replace:or_replace()?
              view_token:k("VIEW") view_name:table_name()
              as_token:k("AS") query:query_statement()
            {
                CreateViewStatement {
                    create_token,
                    or_replace,
                    view_token,
                    view_name,
                    as_token,
                    query,
                }
            }

        rule or_replace() -> OrReplace
            = or_token:k("OR") replace_token:k("REPLACE") {
                OrReplace { or_token, replace_token }
            }

        rule create_table_definition() -> CreateTableDefinition
            = paren1:t("(") columns:sep(<column_definition()>, ",") paren2:t(")") {
                CreateTableDefinition::Columns {
                    paren1,
                    columns,
                    paren2,
                }
            }
            / as_token:k("AS") query_statement:query_statement() {
                CreateTableDefinition::As {
                    as_token,
                    query_statement,
                }
            }

        rule column_definition() -> ColumnDefinition
            = name:ident() data_type:data_type() {
                ColumnDefinition {
                    name,
                    data_type,
                }
            }

        rule drop_view_statement() -> DropViewStatement
            // Oddly, BigQuery accepts `DELETE VIEW`.
            = drop_token:(k("DROP") / k("DELETE")) view_token:k("VIEW") view_name:table_name() {
                DropViewStatement {
                    // Fix this at parse time. Nobody wants `DELETE VIEW`.
                    drop_token: drop_token.with_token_str("DROP"),
                    view_token,
                    view_name,
                }
            }

        /// A table name, such as `t1` or `project-123.dataset1.table2`.
        rule table_name() -> TableName
            // We handle this manually because of PEG backtracking limitations.
            = dotted:dotted_name() {?
                let len = dotted.nodes.len();
                let mut nodes = dotted.nodes.into_iter();
                let mut dots = dotted.separators.into_iter();
                if len == 1 {
                    Ok(TableName::Table { table: nodes.next().unwrap() })
                } else if len == 2 {
                    Ok(TableName::DatasetTable {
                        dataset: nodes.next().unwrap(),
                        dot: dots.next().unwrap(),
                        table: nodes.next().unwrap(),
                    })
                } else if len == 3 {
                    Ok(TableName::ProjectDatasetTable {
                        project: nodes.next().unwrap(),
                        dot1: dots.next().unwrap(),
                        dataset: nodes.next().unwrap(),
                        dot2: dots.next().unwrap(),
                        table: nodes.next().unwrap(),
                    })
                } else {
                    Err("table name")
                }
            }

        /// A table name and a column name.
        rule table_and_column_name() -> TableAndColumnName
            // We handle this manually because of PEG backtracking limitations.
            = dotted:dotted_name() {?
                let len = dotted.nodes.len();
                let mut nodes = dotted.nodes.into_iter();
                let mut dots = dotted.separators.into_iter();
                if len == 2 {
                    Ok(TableAndColumnName {
                        table_name: TableName::Table { table: nodes.next().unwrap() },
                        dot: dots.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 3 {
                    Ok(TableAndColumnName {
                        table_name: TableName::DatasetTable {
                            dataset: nodes.next().unwrap(),
                            dot: dots.next().unwrap(),
                            table: nodes.next().unwrap(),
                        },
                        dot: dots.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 4 {
                    Ok(TableAndColumnName {
                        table_name: TableName::ProjectDatasetTable {
                            project: nodes.next().unwrap(),
                            dot1: dots.next().unwrap(),
                            dataset: nodes.next().unwrap(),
                            dot2: dots.next().unwrap(),
                            table: nodes.next().unwrap(),
                        },
                        dot: dots.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else {
                    Err("table and column name")
                }
            }

        /// A table or column name with internal dots. This requires special
        /// handling with a PEG parser, because PEG parsers are greedy
        /// and backtracking to try alternatives can sometimes be strange.
        rule dotted_name() -> NodeVec<Identifier> = sep(<ident()>, ".")

        /// An identifier, such as a column name.
        rule ident() -> Identifier
            = s:position!() id:c_ident() ws:$(_) e:position!() {?
                if KEYWORDS.contains(id.to_ascii_uppercase().as_str()) {
                    // Wanted an identifier, but got a bare keyword.
                    Err("identifier")
                } else {
                    Ok(Identifier {
                        token: Token {
                            span: s..e,
                            ws_offset: id.len(),
                            text: format!("{}{}", id, ws),
                        },
                        // TODO: Unescape.
                        text: id.to_string(),
                    })
                }
            }
            / s:position!() "`" id:$(([^ '\\' | '`'] / escape())*) "`" ws:$(_) e:position!() {
                Identifier {
                    token: Token {
                        span: s..e,
                        ws_offset: id.len() + 2,
                        text: format!("`{}`{}", id, ws),
                    },
                    // TODO: Unescape.
                    text: id.to_string(),
                }
            }
            / expected!("identifier")

        /// Low-level rule for matching a C-style identifier.
        rule c_ident() -> String
            = quiet! { id:$(c_ident_start() c_ident_cont()*)
              // The next character cannot be a valid ident character.
              !c_ident_cont()
              { id.to_string() } }
            / expected!("identifier")
        rule c_ident_start() = ['a'..='z' | 'A'..='Z' | '_']
        rule c_ident_cont() = ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']

        /// Escape sequences.
        rule escape() = "\\" (octal_escape() / hex_escape() / unicode_escape() / [_])
        rule octal_escape() = (['0'..='7'] * <3,3>)
        rule hex_escape() = ("x" / "X") hex_digit() * <2,2>
        rule unicode_escape() = "u" hex_digit() * <4,4> / "U" hex_digit() * <8,8>
        rule hex_digit() = ['0'..='9' | 'a'..='f' | 'A'..='F']

        /// Punctuation separated list. Does not allow a trailing separator.
        rule sep<T: fmt::Debug>(item: rule<T>, separator: &'static str) -> NodeVec<T>
            = first:item() items:(sep:t(separator) item:item() { (sep, item) })* {
                let mut nodes = Vec::new();
                let mut separators = Vec::new();
                nodes.push(first);
                for (sep, item) in items {
                    separators.push(sep);
                    nodes.push(item);
                }
                NodeVec { nodes, separators }
            }

        /// Punctuation separated list. Allows a trailing separator.
        rule sep_opt_trailing<T: fmt::Debug>(item: rule<T>, separator: &'static str) -> NodeVec<T>
            = list:sep(item, separator) trailing_sep:t(separator)? {
                let mut list = list;
                if let Some(trailing_sep) = trailing_sep {
                    list.separators.push(trailing_sep);
                }
                list
            }

        /// Keywords. These use case-insensitive matching, and may not be
        /// followed by a valid identifier character. See
        /// https://github.com/kevinmehall/rust-peg/issues/216#issuecomment-564390313
        rule k(kw: &'static str) -> Token
            = want(kw, &str::eq_ignore_ascii_case)
              s:position!() input:$([_]*<{kw.len()}>)
              !['a'..='z' | 'A'..='Z' | '0'..='9' | '_']
              ws:$(_) e:position!()

            {
                if !KEYWORDS.contains(kw) {
                    panic!("BUG: {:?} is not in KEYWORDS", kw);
                }
                Token {
                    span: s..e,
                    ws_offset: input.len(),
                    text: format!("{}{}", input, ws),
                }
            }

        /// Simple tokens.
        rule t(token: &'static str) -> Token
            = want(token, &str::eq)
              s:position!() input:$([_]*<{token.len()}>) ws:$(_) e:position!()
            {
                if KEYWORDS.contains(token) {
                    panic!("BUG: {:?} is in KEYWORDS, so parse it with k()", token);
                }
                Token {
                    span: s..e,
                    ws_offset: input.len(),
                    text: format!("{}{}", input, ws),
                }
            }

        /// Complex tokens matching a grammar rule.
        rule token<T>(label: &'static str, r: rule<T>) -> Token
            = s:position!() text:$(r()) ws:$(_) e:position!() {
                let ws_offset = text.len();
                let text = format!("{}{}", text, ws);
                Token { span: s..e, ws_offset, text }
            }

        /// Tricky zero-length rule for asserting the next token without
        /// advancing the parse location.
        ///
        /// TODO: See https://github.com/kevinmehall/rust-peg/issues/361
        rule want(s: &'static str, cmp: &StrCmp)
            = &(found:$([_]*<{s.len()}>) {?
                if cmp(found, s) {
                    Ok(())
                } else {
                    Err(s)
                }
            })
            / {? Err(s) }
            // / expected!("token/keyword")

        // Whitespace, including comments. We don't normally want whitespace to
        // show up as an "expected" token in error messages, so we carefully
        // enclose _most_ of this in `quiet!`. The exception is the closing "*/"
        // in a block comment, which we want to mention explicitly.

        /// Optional whitespace.
        rule _ = whitespace()?

        /// Mandatory whitespace.
        rule whitespace()
            = (whitespace_char() / line_comment() / block_comment())+

        rule whitespace_char() = quiet! { [' ' | '\t' | '\r' | '\n'] }
        rule line_comment() = quiet! { ("#" / "--") (!['\n'][_])* ( "\n" / ![_] ) }
        rule block_comment() = quiet! { "/*"(!"*/"[_])* } "*/"

        /// Tracing rule for `pegviz`. See
        /// https://github.com/fasterthanlime/pegviz.
        rule traced<T>(e: rule<T>) -> T =
            &(input:$([_]*) {
                #[cfg(feature = "trace")]
                println!("[PEG_INPUT_START]\n{}\n[PEG_TRACE_START]", input);
            })
            e:e()? {?
                #[cfg(feature = "trace")]
                println!("[PEG_TRACE_STOP]");
                e.ok_or("")
            }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{functions::FunctionFlags, types, Connection};

    use crate::unnest::register_unnest;

    use super::*;

    #[test]
    fn test_parser_and_run_with_sqlite3() {
        let sql_examples = &[
            // Basic test cases of gradually increasing complexity.
            (r#"SELECT * FROM t"#, None),
            (r#"SELECT * FROM t # comment"#, None),
            (r#"SELECT DISTINCT * FROM t"#, None),
            (r#"SELECT 1 a"#, None),
            (r#"SELECT * FROM `t`"#, Some(r#"SELECT * FROM t"#)),
            (r#"select * from t"#, None),
            (
                r#"select /* hi */ * from `t`"#,
                Some(r#"select /* hi */ * from t"#),
            ),
            (r#"SELECT a,b FROM t"#, None),
            (r#"select a, b /* hi */, from t"#, None),
            ("select a, b, /* hi */ from t", None),
            ("select a, b,from t", None),
            (r#"select p.*, p.a AS c from t as p"#, None),
            (r#"SELECT * EXCEPT(a) FROM t"#, None),
            (r"SELECT a, COUNT(*) AS c FROM t GROUP BY a", None),
            (r"SELECT * FROM t ORDER BY a LIMIT 10", None),
            (r"SELECT * FROM t UNION ALL SELECT * FROM t", None),
            (r"SELECT * FROM t UNION DISTINCT SELECT * FROM t", None),
            (r"SELECT * FROM t WHERE a IS NULL", None),
            (r"SELECT * FROM t WHERE a IS NOT NULL", None),
            (r"SELECT * FROM t WHERE a < 0", None),
            (r"SELECT * FROM t WHERE a >= 0", None),
            (r"SELECT * FROM t WHERE a != 0", None),
            (r"SELECT * FROM t WHERE a = 0 AND b = 0", None),
            (r"SELECT * FROM t WHERE a = 0 OR b = 0", None),
            (r"SELECT * FROM t WHERE a < 0.0", None),
            (r"SELECT * FROM t WHERE a IN (1,2)", None),
            (r"SELECT * FROM t WHERE a NOT IN (1,2)", None),
            (r"SELECT * FROM t WHERE a IN (SELECT b FROM t)", None),
            (r"SELECT IF(a = 0, 1, 2) c FROM t", None),
            (r"SELECT CASE WHEN a = 0 THEN 1 ELSE 2 END c FROM t", None),
            (r"SELECT TRUE AND FALSE", None),
            (r"SELECT TRUE OR FALSE", None),
            (r"SELECT NOT TRUE", None),
            (r"SELECT COUNT(*) FROM t", None),
            (r"SELECT COUNT(a) FROM t", None),
            (r"SELECT COUNT(DISTINCT a) FROM t", None),
            (r"SELECT COUNT(DISTINCT(a)) FROM t", None),
            (r"SELECT generate_uuid()", None),
            (
                r"SELECT format_datetime('%Y-%Q', current_datetime()) AS uuid",
                None,
            ),
            (
                r"SELECT a, RANK() OVER (PARTITION BY a ORDER BY b ASC) AS rank FROM t",
                None,
            ),
            (r#"SELECT * FROM (SELECT a FROM t) AS p"#, None),
            (r#"SELECT * FROM (SELECT a FROM t) p"#, None),
            (
                r#"select * from `p-123`.`d`.`t`"#,
                Some(r#"select * from `p-123`.d.t"#),
            ),
            (r#"SELECT * FROM t AS t1 JOIN t AS t2 USING (a)"#, None),
            (r#"SELECT * FROM t AS t1 JOIN t AS t2 ON t1.a = t2.a"#, None),
            (r#"SELECT * FROM t AS t1 CROSS JOIN t AS t2"#, None),
            (r#"SELECT * FROM t CROSS JOIN UNNEST(a) AS t2"#, None),
            (r#"select * from `d`.`t`"#, Some(r#"select * from d.t"#)),
            (r#"SELECT a, CAST(NULL AS BOOL) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS ARRAY<INT64>) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS FLOAT64) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS STRING) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS DATETIME) AS c FROM t"#, None),
            (r#"SELECT ARRAY(1, 2)"#, None),
            (r#"SELECT ARRAY[1, 2]"#, None),
            (r#"SELECT [1, 2]"#, None),
            (r#"SELECT ARRAY<INT64>[]"#, None),
            (r#"WITH t2 AS (SELECT * FROM t) SELECT * FROM t2"#, None),
            // TODO: INSERT INTO
            (r#"DELETE FROM t WHERE a = 0"#, None),
            (r#"DELETE FROM t AS t2 WHERE a = 0"#, None),
            (r#"CREATE OR REPLACE TABLE t2 (a INT64, b INT64)"#, None),
            (r#"CREATE OR REPLACE TABLE t2 AS (SELECT * FROM t)"#, None),
            //(r#"DROP TABLE t2"#, None),
            (r#"CREATE OR REPLACE VIEW v AS (SELECT * FROM t)"#, None),
            (r#"DROP VIEW v"#, None),
            (
                r#"# Here's a challenge!
CREATE OR REPLACE TABLE `project-123`.`proxies`.`t2` AS (
    SELECT
        s1.*,
        CAST(`s1`.`random_id` AS STRING) AS `key`,
        proxy.first_name,
        proxy.last_name,
        # this is a comment
        CAST(NULL AS BOOL) AS id_placeholder,
        CAST(NULL AS ARRAY<INT64>) as more_ids
    FROM `project-123`.`sources`.`s1` AS s1
    INNER JOIN
        (
            SELECT DISTINCT first_name, last_name, `join_id`
            FROM `project-123`.`proxies`.`t1`
        ) AS proxy
        USING (`join_id`)
    )
                "#,
                Some(
                    r#"# Here's a challenge!
CREATE OR REPLACE TABLE `project-123`.proxies.t2 AS (
    SELECT
        s1.*,
        CAST(s1.random_id AS STRING) AS `key`,
        proxy.first_name,
        proxy.last_name,
        # this is a comment
        CAST(NULL AS BOOL) AS id_placeholder,
        CAST(NULL AS ARRAY<INT64>) as more_ids
    FROM `project-123`.sources.s1 AS s1
    INNER JOIN
        (
            SELECT DISTINCT first_name, last_name, join_id
            FROM `project-123`.proxies.t1
        ) AS proxy
        USING (join_id)
    )
                "#,
                ),
            ),
        ];

        // Set up SQLite3 database for testing transpiled SQL.
        let conn = Connection::open_in_memory().expect("failed to open SQLite3 database");

        // Install our dummy `UNNEST` virtual table.
        register_unnest(&conn).expect("failed to register UNNEST");

        // Install some dummy functions that always return NULL.
        let dummy_fns = &[
            ("array", -1),
            ("current_datetime", 0),
            ("format_datetime", 2),
            ("generate_uuid", 0),
        ];
        for &(fn_name, n_arg) in dummy_fns {
            conn.create_scalar_function(fn_name, n_arg, FunctionFlags::SQLITE_UTF8, move |_ctx| {
                Ok(types::Value::Null)
            })
            .expect("failed to create dummy function");
        }

        // Create some fixtures.
        let fixtures = r#"
            CREATE TABLE t (a INT, b INT);
            CREATE TABLE "p-123.d.t" (a INT, b INT);
            CREATE TABLE "d.t" (a INT, b INT);

            -- For our big query.
            CREATE TABLE "project-123.sources.s1" (
                random_id INT,
                join_id INT
            );
            CREATE TABLE "project-123.proxies.t1" (
                first_name TEXT,
                last_name TEXT,
                join_id INT
            );
        "#;
        conn.execute_batch(fixtures)
            .expect("failed to create SQLite3 fixtures");

        for &(sql, normalized) in sql_examples {
            println!("parsing:   {}", sql);
            let normalized = normalized.unwrap_or(sql);
            let parsed = match parse_sql(sql) {
                Ok(parsed) => parsed,
                Err(err) => {
                    err.emit().unwrap();
                    panic!("{}", err);
                }
            };
            assert_eq!(normalized, &parsed.to_string_for_target(Target::BigQuery));

            let sql = parsed.to_string_for_target(Target::SQLite3);
            println!("  SQLite3: {}", sql);
            if let Err(err) = conn.execute_batch(&sql) {
                panic!("failed to execute with SQLite3:\n{}\n{}", sql, err);
            }
        }
    }
}
