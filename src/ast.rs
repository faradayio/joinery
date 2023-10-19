//! Our abstract syntax tree and parser.
//!
//! Many nodes in this tree implement the following types:
//!
//! - [`Emit`]: Emit the AST as code for a specific database. This will normally
//!   call [`EmitDefault::emit_default`], but it can be customized.
//! - [`EmitDefault`]: Emit the AST as BigQuery SQL, extremely close to the
//!   original input. This is optional, and only required if that type's
//!   [`Emit`] implementation wants to use it.
//! - [`Drive`] and [`DriveMut`]: Provided by the [`derive-visitor`
//!   crate](https://github.com/nikis05/derive-visitor). This provides an API to
//!   traverse the AST generically, using the [`derive_visitor::Visitor`] trait.
//!   This is honestly deep Rust magic, but it prevents us from needing to
//!   implement custom traversal code from scratch every time we want to walk
//!   the AST for some useful information.

// Don't bother with `Box`-ing everything for now. Allow huge enum values.
#![allow(clippy::large_enum_variant)]

use std::{
    borrow::Cow,
    fmt::{self, Display as _},
    mem::take,
    ops::Range,
};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFile,
};
use derive_visitor::{Drive, DriveMut};
use joinery_macros::{Emit, EmitDefault};

use crate::{
    drivers::{
        bigquery::BigQueryName,
        snowflake::SnowflakeString,
        sqlite3::{SQLite3Ident, SQLite3String},
        trino::TrinoString,
    },
    errors::{Result, SourceError},
    util::is_c_ident,
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

    // Magic functions with parser integration.
    "COUNT", "SAFE_CAST", "ORDINAL",

    // Interval units.
    "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
};

/// We represent a span in our source code using a Rust range. These are easy
/// to construct from within our parser.
type Span = Range<usize>;

/// Used to represent a missing span.
pub fn span_none() -> Span {
    usize::MAX..usize::MAX
}

/// A function that compares two strings for equality.
type StrCmp = dyn Fn(&str, &str) -> bool;

/// The target language we're transpiling to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum Target {
    BigQuery,
    Snowflake,
    SQLite3,
    Trino,
}

impl Target {
    /// Format this node for the given target.
    fn f<T: Emit>(self, node: &T) -> FormatForTarget<'_, T> {
        FormatForTarget { target: self, node }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::BigQuery => write!(f, "bigquery"),
            Target::Snowflake => write!(f, "snowflake"),
            Target::SQLite3 => write!(f, "sqlite3"),
            Target::Trino => write!(f, "trino"),
        }
    }
}

/// Wrapper for formatting a node for a given target.
struct FormatForTarget<'a, T: Emit> {
    target: Target,
    node: &'a T,
}

impl<'a, T: Emit> fmt::Display for FormatForTarget<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.emit(self.target, f)
    }
}

/// A default version of [`Emit`] which attemps to write the AST back out as
/// BigQuery SQL, extremely close to the original input.
///
/// For most types, you will start by using `#[derive(Emit, EmitDefault)]`. This
/// will generate:
///
/// - An implementation of [`Emit`] which calls [`EmitDefault::emit_default`].
/// - An implementation of [`EmitDefault`] which tries to output the AST as
///   close to the original input as possible.
///
/// Note that [`EmitDefault`] is an implementation detail, and not all types
/// will need to implement it. For example, [`Token`] implements [`Emit`]
/// directly, so it has no need for [`EmitDefault`].
///
/// If you need to override the default output for a particular database, you
/// should use `#[derive(EmitDefault)]` and implement [`Emit::emit`] to handle
/// the special cases, and to pass through to [`EmitDefault::emit_default`] when
/// the default behavior is desired.
pub trait EmitDefault {
    /// Emit the AST as BigQuery SQL.
    fn emit_default(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

/// Emit the AST as code for a specific database.
///
/// If you use `#[derive(Emit, EmitDefault)]` on a type, then [`Emit::emit`]
/// will be generated to call [`EmitDefault::emit_default`].
pub trait Emit: Sized {
    /// Format this node as its SQLite3 equivalent. This would need to be replaced
    /// with something a bit more compiler-like to support full transpilation.
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    /// Convert this node to a string for the given target.
    fn emit_to_string(&self, t: Target) -> String {
        format!("{}", t.f(self))
    }
}

impl<T: Emit> Emit for Box<T> {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().emit(t, f)
    }
}

impl<T: Emit> Emit for Option<T> {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(node) = self {
            node.emit(t, f)
        } else {
            Ok(())
        }
    }
}

impl<T: Emit> Emit for Vec<T> {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for node in self.iter() {
            node.emit(t, f)?;
        }
        Ok(())
    }
}

/// Our basic token type. This is used for all punctuation and keywords.
#[derive(Clone, Debug, Drive, DriveMut)]
pub struct Token {
    #[drive(skip)]
    pub span: Span,
    #[drive(skip)]
    pub ws_offset: usize,
    #[drive(skip)]
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

impl Emit for Token {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => write!(f, "{}", self.text),
            Target::Snowflake | Target::SQLite3 | Target::Trino => {
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

/// A node type, for use with [`NodeVec`].
pub trait Node: Clone + fmt::Debug + Drive + DriveMut + Emit + 'static {}

impl<T: Clone + fmt::Debug + Drive + DriveMut + Emit + 'static> Node for T {}

/// Either a node or a separator token.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum NodeOrSep<T: Node> {
    Node(T),
    Sep(Token),
}

/// A vector which contains a list of nodes, along with any whitespace that
/// appears after a node but before any separating punctuation. This can be
/// used either with or without a final trailing separator.
///
/// ## Note for [`Drive`]
///
/// Because of how [`Drive`] is defined, it uses `IntoIterator` to iterate
/// over any collection type, and we can't customize that. So currently we
/// do not send [`Vistor`] events for the separators. If we wanted to fix
/// this, we would need to modify [`IntoIterator`] to return interleaved
/// nodes and separators, and define custom node-only and separator-only
/// iterators.
#[derive(Debug)]
pub struct NodeVec<T: Node> {
    /// The separator to use when adding items.
    pub separator: &'static str,
    /// The nodes and separators in this vector.
    pub items: Vec<NodeOrSep<T>>,
}

impl<T: Node> NodeVec<T> {
    /// Create a new [`NodeVec`] with the given separator.
    pub fn new(separator: &'static str) -> NodeVec<T> {
        NodeVec {
            separator,
            items: vec![],
        }
    }

    /// Take the elements from this [`NodeVec`], leaving it empty, and return a new
    /// [`NodeVec`] containing the taken elements.
    pub fn take(&mut self) -> NodeVec<T> {
        NodeVec {
            separator: self.separator,
            items: take(&mut self.items),
        }
    }

    /// Add a node to this [`NodeVec`].
    pub fn push(&mut self, node: T) {
        if let Some(NodeOrSep::Node(_)) = self.items.last() {
            self.items.push(NodeOrSep::Sep(Token {
                span: span_none(),
                ws_offset: self.separator.len(),
                text: self.separator.to_owned(),
            }));
        }
        self.items.push(NodeOrSep::Node(node));
    }

    /// Add a node or a separator to this [`NodeVec`], inserting or removing
    /// separators as needed to ensure that the vector is well-formed.
    pub fn push_node_or_sep(&mut self, node_or_sep: NodeOrSep<T>) {
        match node_or_sep {
            NodeOrSep::Node(_) => {
                if let Some(NodeOrSep::Node(_)) = self.items.last() {
                    self.items.push(NodeOrSep::Sep(Token {
                        span: span_none(),
                        ws_offset: self.separator.len(),
                        text: self.separator.to_owned(),
                    }));
                }
            }
            NodeOrSep::Sep(_) => {
                if let Some(NodeOrSep::Sep(_)) = self.items.last() {
                    self.items.pop();
                }
            }
        }
        self.items.push(node_or_sep);
    }

    /// Iterate over the node and separators in this [`NodeVec`].
    pub fn iter(&self) -> impl Iterator<Item = &NodeOrSep<T>> {
        self.items.iter()
    }

    /// Iterate over just the nodes in this [`NodeVec`].
    pub fn node_iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter().filter_map(|item| match item {
            NodeOrSep::Node(node) => Some(node),
            NodeOrSep::Sep(_) => None,
        })
    }

    /// Iterate over just the nodes in this [`NodeVec`], mutably.
    pub fn node_iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.items.iter_mut().filter_map(|item| match item {
            NodeOrSep::Node(node) => Some(node),
            NodeOrSep::Sep(_) => None,
        })
    }

    /// Iterate over nodes and separators separately. Used internally for
    /// parsing dotted names.
    fn into_node_and_sep_iters(self) -> (impl Iterator<Item = T>, impl Iterator<Item = Token>) {
        let mut nodes = vec![];
        let mut seps = vec![];
        for item in self.items {
            match item {
                NodeOrSep::Node(node) => nodes.push(node),
                NodeOrSep::Sep(token) => seps.push(token),
            }
        }
        (nodes.into_iter(), seps.into_iter())
    }
}

impl<T: Node> Clone for NodeVec<T> {
    fn clone(&self) -> Self {
        NodeVec {
            separator: self.separator,
            items: self.items.clone(),
        }
    }
}

impl<T: Node> IntoIterator for NodeVec<T> {
    type Item = NodeOrSep<T>;
    type IntoIter = std::vec::IntoIter<NodeOrSep<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T: Node> IntoIterator for &'a NodeVec<T> {
    type Item = &'a NodeOrSep<T>;
    type IntoIter = std::slice::Iter<'a, NodeOrSep<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

// Mutable iterator for `DriveMut`.
impl<'a, T: Node> IntoIterator for &'a mut NodeVec<T> {
    type Item = &'a mut NodeOrSep<T>;
    type IntoIter = std::slice::IterMut<'a, NodeOrSep<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter_mut()
    }
}

impl<T: Node> Emit for NodeVec<T> {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, node_or_sep) in self.items.iter().enumerate() {
            let is_last = i + 1 == self.items.len();
            match node_or_sep {
                NodeOrSep::Node(node) => node.emit(t, f)?,
                NodeOrSep::Sep(sep) if is_last && t != Target::BigQuery => {
                    sep.with_erased_token_str().emit(t, f)?
                }
                NodeOrSep::Sep(sep) => sep.emit(t, f)?,
            }
        }
        Ok(())
    }
}

/// An identifier, such as a column name.
#[derive(Clone, Debug, Drive, DriveMut)]
pub struct Identifier {
    /// Our original token.
    pub token: Token,

    /// Our unescaped text.
    #[drive(skip)]
    pub text: String,
}

impl Identifier {
    /// Build an identifier from a "simple" token, one which does not need to
    /// be quoted.
    pub fn from_simple_token(token: Token) -> Identifier {
        let text = token.token_str().to_owned();
        Identifier { token, text }
    }

    /// Is this identifier a keyword?
    fn is_keyword(&self) -> bool {
        KEYWORDS.contains(self.text.to_ascii_uppercase().as_str())
    }

    /// Is this a valid C-style identifier?
    fn is_c_ident(&self) -> bool {
        is_c_ident(&self.text)
    }

    // Does this identifier need to be quoted?
    fn needs_quotes(&self) -> bool {
        self.is_keyword() || !self.is_c_ident()
    }

    // Get the unescaped identifier name.
    pub fn unescaped_bigquery(&self) -> &str {
        &self.text
    }
}

impl Emit for Identifier {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.needs_quotes() {
            match t {
                Target::BigQuery => write!(
                    f,
                    "{}{}",
                    BigQueryName(&self.text),
                    t.f(&self.token.ws_only())
                ),
                // Snowflake and SQLite3 use double quoted identifiers and
                // escape quotes by doubling them. Neither allows backslash
                // escapes here, though Snowflake does in strings.
                Target::Snowflake | Target::SQLite3 | Target::Trino => write!(
                    f,
                    "{}{}",
                    SQLite3Ident(&self.text),
                    t.f(&self.token.ws_only())
                ),
            }
        } else {
            write!(f, "{}{}", self.text, t.f(&self.token.ws_only()))
        }
    }
}

/// A table name.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
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

impl TableName {
    /// Get the unescaped table name, in the original BigQuery form.
    pub fn unescaped_bigquery(&self) -> String {
        match self {
            TableName::ProjectDatasetTable {
                project,
                dataset,
                table,
                ..
            } => format!("{}.{}.{}", project.text, dataset.text, table.text,),
            TableName::DatasetTable { dataset, table, .. } => {
                format!("{}.{}", dataset.text, table.text,)
            }
            TableName::Table { table } => table.text.clone(),
        }
    }
}

impl Emit for TableName {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 => {
                let name = self.unescaped_bigquery();
                let ws = match self {
                    TableName::ProjectDatasetTable { table, .. }
                    | TableName::DatasetTable { table, .. }
                    | TableName::Table { table } => table.token.ws_only(),
                };
                write!(f, "{}{}", SQLite3Ident(&name), t.f(&ws))
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A table and a column name.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct TableAndColumnName {
    pub table_name: TableName,
    pub dot: Token,
    pub column_name: Identifier,
}

/// An entire SQL program.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct SqlProgram {
    /// Any whitespace that appears before the first statement. This is represented
    /// as a token with an empty `token_str()`.
    pub leading_ws: Token,

    /// For now, just handle single statements; BigQuery DDL is messy and maybe
    /// out of scope.
    pub statements: NodeVec<Statement>,
}

/// A statement in our abstract syntax tree.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum Statement {
    Query(QueryStatement),
    DeleteFrom(DeleteFromStatement),
    InsertInto(InsertIntoStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
}

/// A query statement. This exists mainly because it's in the official grammar.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct QueryStatement {
    pub query_expression: QueryExpression,
}

/// A query expression is a `SELECT` statement, plus some other optional
/// surrounding things. See the [official grammar][]. This is where CTEs and
/// similar things hook into the grammar.
///
/// [official grammar]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#sql_syntax.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
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

impl Emit for QueryExpression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Nested needs special handling on SQLite3.
            QueryExpression::Nested {
                paren1,
                query,
                paren2,
            } if t == Target::SQLite3 => {
                // Add a leading space in case the previous token is an
                // identifier.
                paren1.with_token_str(" SELECT * FROM (").emit(t, f)?;
                query.emit(t, f)?;
                paren2.emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// Common table expressions (CTEs).
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct CommonTableExpression {
    pub name: Identifier,
    pub as_token: Token,
    pub paren1: Token,
    pub query: Box<QueryStatement>,
    pub paren2: Token,
}

/// Set operators.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
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

impl Emit for SetOperator {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // SQLite3 only supports `UNION` and `INTERSECT`. We'll keep the
            // whitespace from the first token in those cases. In other cases,
            // we'll substitute `UNION` with a comment saying what it really
            // should be.
            Target::Snowflake | Target::SQLite3 => match self {
                SetOperator::UnionAll {
                    union_token,
                    all_token,
                } => {
                    union_token.emit(t, f)?;
                    all_token.emit(t, f)
                }
                SetOperator::UnionDistinct { union_token, .. } => union_token.emit(t, f),
                SetOperator::IntersectDistinct {
                    intersect_token, ..
                } => intersect_token.emit(t, f),
                SetOperator::ExceptDistinct { except_token, .. } => except_token.emit(t, f),
            },
            _ => self.emit_default(t, f),
        }
    }
}

/// A `SELECT` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct SelectExpression {
    pub select_options: SelectOptions,
    pub select_list: SelectList,
    pub from_clause: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,

    // TODO: Actually these belong in `QueryExpression`?
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

/// The head of a `SELECT`, including any modifiers.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct SelectOptions {
    pub select_token: Token,
    pub distinct: Option<Distinct>,
}

/// The `DISTINCT` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Distinct {
    pub distinct_token: Token,
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
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct SelectList {
    pub items: NodeVec<SelectListItem>,
}

/// A single item in a `SELECT` list.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
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

/// An `EXCEPT` clause.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct Except {
    pub except_token: Token,
    pub paren1: Token,
    pub columns: NodeVec<Identifier>,
    pub paren2: Token,
}

impl Emit for Except {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::Snowflake => {
                self.except_token.with_token_str("EXCLUDE").emit(t, f)?;
                self.paren1.ws_only().ensure_ws().emit(t, f)?;
                self.columns.emit(t, f)?;
                self.paren2.ws_only().ensure_ws().emit(t, f)
            }
            Target::SQLite3 => {
                // TODO: Implement `EXCEPT` by inspecting the database schema.
                // For now, erase it.
                write!(f, "/* ")?;
                self.emit_default(t, f)?;
                write!(f, " */")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// An SQL expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum Expression {
    Literal {
        token: Token,
        #[emit(skip)]
        value: LiteralValue,
    },
    Null {
        null_token: Token,
    },
    Interval(IntervalExpression),
    ColumnName(Identifier),
    TableAndColumnName(TableAndColumnName),
    Cast(Cast),
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
        value_set: InValueSet,
    },
    Between {
        left: Box<Expression>,
        not_token: Option<Token>,
        between_token: Token,
        middle: Box<Expression>,
        and_token: Token,
        right: Box<Expression>,
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
    Struct(StructExpression),
    Count(CountExpression),
    CurrentDate(CurrentDate),
    SpecialDateFunctionCall(SpecialDateFunctionCall),
    FunctionCall(FunctionCall),
    Index(IndexExpression),
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

impl Emit for Expression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            // Snowflake quotes strings differently.
            Expression::Literal {
                token,
                value: LiteralValue::String(s),
            } if t == Target::Snowflake => {
                SnowflakeString(s).fmt(f)?;
                token.ws_only().emit(t, f)
            }
            // SQLite3 quotes strings differently.
            Expression::Literal {
                token,
                value: LiteralValue::String(s),
            } if t == Target::SQLite3 => {
                SQLite3String(s).fmt(f)?;
                token.ws_only().emit(t, f)
            }
            // SQLite3 quotes strings differently.
            Expression::Literal {
                token,
                value: LiteralValue::String(s),
            } if t == Target::Trino => {
                TrinoString(s).fmt(f)?;
                token.ws_only().emit(t, f)
            }
            Expression::If {
                if_token,
                condition,
                then_expression,
                else_expression,
                paren2,
                ..
            } if t == Target::Snowflake || t == Target::SQLite3 => write!(
                f,
                "{}WHEN {} THEN {} ELSE {} {}",
                t.f(if_token.with_token_str("CASE").ensure_ws().as_ref()),
                t.f(condition),
                t.f(then_expression),
                t.f(else_expression),
                t.f(paren2.with_token_str("END").ensure_ws().as_ref()),
            ),
            _ => self.emit_default(t, f),
        }
    }
}

/// A literal value.
#[derive(Clone, Debug, Drive, DriveMut)]
pub enum LiteralValue {
    Bool(#[drive(skip)] bool),
    Int64(#[drive(skip)] i64),
    Float64(#[drive(skip)] f64),
    String(#[drive(skip)] String),
}

/// An `INTERVAL` expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct IntervalExpression {
    pub interval_token: Token,
    pub number: Token, // Not even bothering to parse this for now.
    pub date_part: DatePart,
}

impl Emit for IntervalExpression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // Treat these as strings for now.
            Target::SQLite3 => write!(
                f,
                "INTERVAL({}, {})",
                t.f(&self.number),
                t.f(&self.date_part)
            ),
            _ => self.emit_default(t, f),
        }
    }
}

/// A date part in an `INTERVAL` expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct DatePart {
    pub date_part_token: Token,
}

impl Emit for DatePart {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // Treat these as strings for now.
            Target::SQLite3 => {
                write!(f, "'{}'", t.f(&self.date_part_token))
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A cast expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct Cast {
    cast_type: CastType,
    paren1: Token,
    expression: Box<Expression>,
    as_token: Token,
    data_type: DataType,
    paren2: Token,
}

impl Emit for Cast {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // For SQLite2, convert `SAFE_CAST` to `CAST`.
            Target::SQLite3 => write!(
                f,
                "{}{}{}{}{}{}",
                t.f(&self.cast_type),
                t.f(&self.paren1),
                t.f(&self.expression),
                t.f(&self.as_token),
                t.f(&self.data_type),
                t.f(&self.paren2),
            ),
            _ => self.emit_default(t, f),
        }
    }
}

/// What type of cast do we want to perform?
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum CastType {
    Cast { cast_token: Token },
    SafeCast { safe_cast_token: Token },
}

impl Emit for CastType {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CastType::SafeCast { safe_cast_token }
                if t == Target::Snowflake || t == Target::Trino =>
            {
                safe_cast_token.with_token_str("TRY_CAST").emit(t, f)
            }
            // TODO: This isn't strictly right, but it's as close as I know how to
            // get with SQLite3.
            CastType::SafeCast { safe_cast_token } if t == Target::SQLite3 => {
                safe_cast_token.with_token_str("CAST").emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A value set for an `IN` expression. See the [official grammar][].
///
/// [official grammar]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum InValueSet {
    QueryExpression {
        paren1: Token,
        query: Box<QueryExpression>,
        paren2: Token,
    },
    ExpressionList {
        paren1: Token,
        expressions: NodeVec<Expression>,
        paren2: Token,
    },
    /// `IN UNNEST` is handled using a special grammar rule.
    Unnest {
        unnest_token: Token,
        paren1: Token,
        expression: Box<Expression>,
        paren2: Token,
    },
}

/// An `ARRAY` expression. This takes a bunch of different forms in BigQuery.
///
/// Not all combinations of our fields are valid. For example, we can't have
/// a missing `ARRAY` and a `delim1` of `(`. We'll let the parser handle that.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct ArrayExpression {
    pub array_token: Option<Token>,
    pub element_type: Option<ArrayElementType>,
    pub delim1: Token,
    pub definition: Option<ArrayDefinition>,
    pub delim2: Token,
}

impl Emit for ArrayExpression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::Snowflake => {
                self.delim1.with_token_str("[").emit(t, f)?;
                self.definition.emit(t, f)?;
                self.delim2.with_token_str("]").emit(t, f)?;
            }
            Target::SQLite3 => {
                if let Some(array_token) = &self.array_token {
                    array_token.emit(t, f)?;
                } else {
                    write!(f, "ARRAY")?;
                }
                self.delim1.with_token_str("(").emit(t, f)?;
                self.definition.emit(t, f)?;
                self.delim2.with_token_str(")").emit(t, f)?;
            }
            Target::Trino => {
                if let Some(array_token) = &self.array_token {
                    array_token.emit(t, f)?;
                } else {
                    write!(f, "ARRAY")?;
                }
                self.delim1.with_token_str("[").emit(t, f)?;
                self.definition.emit(t, f)?;
                self.delim2.with_token_str("]").emit(t, f)?;
            }
            _ => self.emit_default(t, f)?,
        }
        Ok(())
    }
}

/// An `ARRAY` definition. Either a `SELECT` expression or a list of
/// expressions.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum ArrayDefinition {
    Query(Box<QueryExpression>),
    Elements(NodeVec<Expression>),
}

impl Emit for ArrayDefinition {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // No query expressions in SQLite3.
        match self {
            ArrayDefinition::Query(_) if t == Target::SQLite3 => {
                write!(f, "/*")?;
                self.emit_default(t, f)?;
                write!(f, "*/")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A struct expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct StructExpression {
    pub struct_token: Token,
    pub paren1: Token,
    pub fields: SelectList,
    pub paren2: Token,
}

impl Emit for StructExpression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 => {
                self.struct_token.emit(t, f)?;
                self.paren1.emit(t, f)?;
                // TODO: Output without AS. Do we want to add `map`?
                write!(f, "/* TODO STRUCT FIELDS */")?;
                self.paren2.emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// The type of the elements in an `ARRAY` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct ArrayElementType {
    pub lt: Token,
    pub elem_type: DataType,
    pub gt: Token,
}

/// A `COUNT` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
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

/// A `CASE WHEN` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct CaseWhenClause {
    pub when_token: Token,
    pub condition: Box<Expression>,
    pub then_token: Token,
    pub result: Box<Expression>,
}

/// A `CASE ELSE` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct CaseElseClause {
    pub else_token: Token,
    pub result: Box<Expression>,
}

/// `CURRENT_DATE` may appear as either `CURRENT_DATE` or `CURRENT_DATE()`.
/// And different databases seem to support one or the other or both.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct CurrentDate {
    pub current_date_token: Token,
    pub empty_parens: Option<EmptyParens>,
}

impl Emit for CurrentDate {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // SQLite3 only supports the keyword form.
            Target::SQLite3 => self.current_date_token.ensure_ws().emit(t, f),
            _ => self.emit_default(t, f),
        }
    }
}

/// An empty `()` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct EmptyParens {
    pub paren1: Token,
    pub paren2: Token,
}

/// Special "functions" that manipulate dates. These all take a [`DatePart`]
/// as a final argument. So in Lisp sense, these are special forms or macros,
/// not ordinary function calls.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct SpecialDateFunctionCall {
    pub function_name: Identifier,
    pub paren1: Token,
    pub args: NodeVec<ExpressionOrDatePart>,
    pub paren2: Token,
}

/// An expression or a date part.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum ExpressionOrDatePart {
    Expression(Expression),
    DatePart(DatePart),
}

/// A function call.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct FunctionCall {
    pub name: FunctionName,
    pub paren1: Token,
    pub args: NodeVec<Expression>,
    pub paren2: Token,
    pub over_clause: Option<OverClause>,
}

/// A function name.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum FunctionName {
    ProjectDatasetFunction {
        project: Identifier,
        dot1: Token,
        dataset: Identifier,
        dot2: Token,
        function: Identifier,
    },
    DatasetFunction {
        dataset: Identifier,
        dot: Token,
        function: Identifier,
    },
    Function {
        function: Identifier,
    },
}

impl FunctionName {
    pub fn function_identifier(&self) -> &Identifier {
        match self {
            FunctionName::ProjectDatasetFunction { function, .. }
            | FunctionName::DatasetFunction { function, .. }
            | FunctionName::Function { function } => function,
        }
    }

    /// Get the unescaped function name, in the original BigQuery form.
    pub fn unescaped_bigquery(&self) -> String {
        match self {
            FunctionName::ProjectDatasetFunction {
                project,
                dataset,
                function,
                ..
            } => format!("{}.{}.{}", project.text, dataset.text, function.text),
            FunctionName::DatasetFunction {
                dataset, function, ..
            } => {
                format!("{}.{}", dataset.text, function.text)
            }
            FunctionName::Function { function } => function.text.clone(),
        }
    }
}

impl Emit for FunctionName {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 => {
                let name = self.unescaped_bigquery();
                let ws = self.function_identifier().token.ws_only();
                write!(f, "{}{}", SQLite3Ident(&name), t.f(&ws))
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// An `OVER` clause for a window function.
///
/// See the [official grammar][]. We only implement part of this.
///
/// [official grammar]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#syntax
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct OverClause {
    pub over_token: Token,
    pub paren1: Token,
    pub partition_by: Option<PartitionBy>,
    pub order_by: Option<OrderBy>,
    pub window_frame: Option<WindowFrame>,
    pub paren2: Token,
}

/// A `PARTITION BY` clause for a window function.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct PartitionBy {
    pub partition_token: Token,
    pub by_token: Token,
    pub expressions: NodeVec<Expression>,
}

/// An `ORDER BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct OrderBy {
    pub order_token: Token,
    pub by_token: Token,
    pub items: NodeVec<OrderByItem>,
}

/// An item in an `ORDER BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct OrderByItem {
    pub expression: Expression,
    pub asc_desc: Option<AscDesc>,
}

/// An `ASC` or `DESC` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct AscDesc {
    direction: Token,
    nulls_clause: Option<NullsClause>,
}

/// A `NULLS FIRST` or `NULLS LAST` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct NullsClause {
    nulls_token: Token,
    first_last_token: Token,
}

/// A `LIMIT` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Limit {
    pub limit_token: Token,
    pub value: Box<Expression>,
}

/// A window frame clause.
///
/// See the [official grammar][]. We only implement part of this.
///
/// [official grammar]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct WindowFrame {
    pub rows_token: Token,
    pub definition: WindowFrameDefinition,
}

/// A window frame definition.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum WindowFrameDefinition {
    Start(WindowFrameStart),
    Between {
        between_token: Token,
        start: WindowFrameStart,
        and_token: Token,
        end: WindowFrameEnd,
    },
}

/// A window frame start. Keep this simple for now.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum WindowFrameStart {
    UnboundedPreceding {
        unbounded_token: Token,
        preceding_token: Token,
    },
}

/// A window frame end. Keep this simple for now.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum WindowFrameEnd {
    CurrentRow {
        current_token: Token,
        row_token: Token,
    },
}

/// Data types.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum DataType {
    Bool(Token),
    Bytes(Token),
    Date(Token),
    Datetime(Token),
    Float64(Token),
    Geography(Token), // WGS84
    Int64(Token),
    Numeric(Token),
    String(Token),
    Time(Token),
    Timestamp(Token),
    Array {
        array_token: Token,
        lt: Token,
        data_type: Box<DataType>,
        gt: Token,
    },
    Struct {
        struct_token: Token,
        lt: Token,
        fields: NodeVec<StructField>,
        gt: Token,
    },
}

impl Emit for DataType {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::Snowflake => match self {
                DataType::Bool(token) => token.with_token_str("BOOLEAN").emit(t, f),
                DataType::Bytes(token) => token.with_token_str("BINARY").emit(t, f),
                DataType::Int64(token) => token.with_token_str("INTEGER").emit(t, f),
                DataType::Date(token) => token.emit(t, f),
                // "Wall clock" time with no timezone.
                DataType::Datetime(token) => token.with_token_str("TIMESTAMP_NTZ").emit(t, f),
                DataType::Float64(token) => token.with_token_str("FLOAT8").emit(t, f),
                DataType::Geography(token) => token.emit(t, f),
                DataType::Numeric(token) => token.emit(t, f),
                DataType::String(token) => token.with_token_str("TEXT").emit(t, f),
                DataType::Time(token) => token.emit(t, f),
                // `TIMESTAMP_TZ` will need very careful timezone handling.
                DataType::Timestamp(token) => token.with_token_str("TIMESTAMP_TZ").emit(t, f),
                DataType::Array { gt, .. } => gt.with_token_str("ARRAY").ensure_ws().emit(t, f),
                DataType::Struct { gt, .. } => gt.with_token_str("OBJECT").ensure_ws().emit(t, f),
            },
            Target::SQLite3 => match self {
                DataType::Bool(token) | DataType::Int64(token) => {
                    token.with_token_str("INTEGER").emit(t, f)
                }
                // NUMERIC is used when people want accurate math, so we want
                // either BLOB or TEXT, whatever makes math easier.
                DataType::Bytes(token) | DataType::Numeric(token) => {
                    token.with_token_str("BLOB").emit(t, f)
                }
                DataType::Float64(token) => write!(f, "{}", t.f(&token.with_token_str("REAL"))),
                DataType::String(token)
                | DataType::Date(token)      // All date types should be strings
                | DataType::Datetime(token)
                | DataType::Geography(token) // Use GeoJSON
                | DataType::Time(token)
                | DataType::Timestamp(token) => {
                    write!(f, "{}", t.f(&token.with_token_str("TEXT")))
                }
                DataType::Array { gt, .. } | DataType::Struct { gt, .. } => {
                    write!(
                        f,
                        "{}",
                        // Force whitespace because we're replacing a
                        // punctuation token with an identifier token.
                        t.f(gt.with_token_str("/*JSON*/TEXT").ensure_ws().as_ref())
                    )
                }
            },
            Target::Trino => match self {
                DataType::Bool(token) => token.with_token_str("BOOLEAN").emit(t, f),
                DataType::Bytes(token) => token.with_token_str("VARBINARY").emit(t, f),
                DataType::Date(token) => token.emit(t, f),
                DataType::Datetime(token) => token.with_token_str("TIMESTAMP").emit(t, f),
                DataType::Float64(token) => token.with_token_str("DOUBLE").emit(t, f),
                DataType::Geography(token) => token.with_token_str("JSON").emit(t, f),
                DataType::Int64(token) => token.with_token_str("BIGINT").emit(t, f),
                // TODO: This cannot be done safely in Trino, because you always
                // need to specify the precision and where to put the decimal
                // place.
                DataType::Numeric(token) => token.with_token_str("DECIMAL(?,?)").emit(t, f),
                DataType::String(token) => token.with_token_str("VARCHAR").emit(t, f),
                DataType::Time(token) => token.emit(t, f),
                DataType::Timestamp(token) => {
                    token.with_token_str("TIMESTAMP WITH TIME ZONE").emit(t, f)
                }
                DataType::Array {
                    array_token,
                    lt,
                    data_type,
                    gt,
                } => {
                    array_token.emit(t, f)?;
                    lt.with_token_str("(").emit(t, f)?;
                    data_type.emit(t, f)?;
                    gt.with_token_str(")").emit(t, f)
                }
                // TODO: I think we can translate the column types?
                DataType::Struct {
                    struct_token,
                    lt,
                    fields,
                    gt,
                } => {
                    struct_token.with_token_str("ROW").emit(t, f)?;
                    lt.with_token_str("(").emit(t, f)?;
                    fields.emit(t, f)?;
                    gt.with_token_str(")").emit(t, f)
                }
            },
            _ => self.emit_default(t, f),
        }
    }
}

/// A field in a `STRUCT` type.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct StructField {
    pub name: Option<Identifier>,
    pub data_type: DataType,
}

/// An array index expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct IndexExpression {
    pub expression: Box<Expression>,
    pub bracket1: Token,
    pub index: IndexOffset,
    pub bracket2: Token,
}

impl Emit for IndexExpression {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            // SQLite3 doesn't support array indexing.
            Target::SQLite3 => {
                write!(f, "ARRAY_INDEX(")?;
                self.expression.emit(t, f)?;
                write!(f, ", ")?;
                self.index.emit(t, f)?;
                self.bracket2.with_token_str(")").emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// Different ways to index arrays.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub enum IndexOffset {
    Simple(Box<Expression>),
    Offset {
        offset_token: Token,
        paren1: Token,
        expression: Box<Expression>,
        paren2: Token,
    },
    Ordinal {
        ordinal_token: Token,
        paren1: Token,
        expression: Box<Expression>,
        paren2: Token,
    },
}

impl Emit for IndexOffset {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Many databases use 0-based indexing, but Trireme uses 1-based
        // indexing. BigQuery supports both, so translate as needed.
        match self {
            IndexOffset::Simple(expression) if t == Target::Trino => {
                write!(f, "(")?;
                expression.emit(t, f)?;
                write!(f, ") + 1")
            }
            IndexOffset::Offset { expression, .. }
                if t == Target::Snowflake || t == Target::SQLite3 =>
            {
                expression.emit(t, f)
            }
            IndexOffset::Offset {
                paren1,
                expression,
                paren2,
                ..
            } if t == Target::Trino => {
                paren1.emit(t, f)?;
                expression.emit(t, f)?;
                paren2.emit(t, f)?;
                write!(f, " + 1")
            }
            IndexOffset::Ordinal {
                paren1,
                expression,
                paren2,
                ..
            } if t == Target::Snowflake || t == Target::SQLite3 => {
                paren1.emit(t, f)?;
                expression.emit(t, f)?;
                paren2.emit(t, f)?;
                write!(f, " - 1")
            }
            IndexOffset::Ordinal { expression, .. } if t == Target::Trino => expression.emit(t, f),
            _ => self.emit_default(t, f),
        }
    }
}

/// An `AS` alias.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Alias {
    pub as_token: Option<Token>,
    pub ident: Identifier,
}

/// The `FROM` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct FromClause {
    pub from_token: Token,
    pub from_item: FromItem,
    pub join_operations: Vec<JoinOperation>,
}

/// Items which may appear in a `FROM` clause.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
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

impl Emit for FromItem {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FromItem::Unnest {
                unnest_token,
                paren1,
                expression,
                paren2,
                alias: Some(Alias { as_token, ident }),
            } if t == Target::Trino => {
                unnest_token.emit(t, f)?;
                paren1.emit(t, f)?;
                expression.emit(t, f)?;
                paren2.emit(t, f)?;
                if let Some(as_token) = as_token {
                    as_token.ensure_ws().emit(t, f)?;
                }
                // UNNEST aliases aren't like other aliases, and Trino treats
                // them specially.
                write!(f, "U(")?;
                ident.emit(t, f)?;
                write!(f, ")")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A join operation.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum JoinOperation {
    /// A `JOIN` clause.
    ConditionJoin {
        join_type: JoinType,
        join_token: Token,
        from_item: FromItem,
        operator: Option<ConditionJoinOperator>,
    },
    /// A `CROSS JOIN` clause.
    CrossJoin {
        cross_token: Token,
        join_token: Token,
        from_item: FromItem,
    },
}

/// The type of a join.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
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

/// The condition used for a `JOIN`.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
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

/// A `WHERE` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct WhereClause {
    pub where_token: Token,
    pub expression: Expression,
}

/// A `GROUP BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct GroupBy {
    pub group_token: Token,
    pub by_token: Token,
    pub expressions: NodeVec<Expression>,
}

/// A `HAVING` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Having {
    pub having_token: Token,
    pub expression: Expression,
}

/// A `QUALIFY` clause.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct Qualify {
    pub qualify_token: Token,
    pub expression: Expression,
}

impl Emit for Qualify {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 => {
                write!(f, "/* ")?;
                self.emit_default(t, f)?;
                write!(f, " */")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A `DELETE FROM` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct DeleteFromStatement {
    pub delete_token: Token,
    pub from_token: Token,
    pub table_name: TableName,
    pub alias: Option<Alias>,
    pub where_clause: Option<WhereClause>,
}

/// A `INSERT INTO` statement. We only support the `SELECT` version.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct InsertIntoStatement {
    pub insert_token: Token,
    pub into_token: Token,
    pub table_name: TableName,
    pub inserted_data: InsertedData,
}

/// The data to be inserted into a table.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub enum InsertedData {
    /// A `SELECT` statement.
    Select { query: QueryExpression },
    /// A `VALUES` clause.
    Values {
        values_token: Token,
        rows: NodeVec<Row>,
    },
}

/// A row in a `VALUES` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Row {
    pub paren1: Token,
    pub expressions: NodeVec<Expression>,
    pub paren2: Token,
}

/// A `CREATE TABLE` statement.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct CreateTableStatement {
    pub create_token: Token,
    pub or_replace: Option<OrReplace>,
    pub temporary: Option<Temporary>,
    pub table_token: Token,
    pub table_name: TableName,
    pub definition: CreateTableDefinition,
}

impl Emit for CreateTableStatement {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 | Target::Trino if self.or_replace.is_some() => {
                // We need to convert this to a `DROP TABLE IF EXISTS` statement.
                write!(f, "DROP TABLE IF EXISTS {};", t.f(&self.table_name))?;
            }
            _ => {}
        }
        self.emit_default(t, f)
    }
}

/// A `CREATE VIEW` statement.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct CreateViewStatement {
    pub create_token: Token,
    pub or_replace: Option<OrReplace>,
    pub view_token: Token,
    pub view_name: TableName,
    // TODO: Factor out shared `AS` code from [`CreateTableDefinition`].
    pub as_token: Token,
    pub query: QueryStatement,
}

impl Emit for CreateViewStatement {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 if self.or_replace.is_some() => {
                // We need to convert add a `DROP VIEW IF EXISTS` statement.
                write!(f, "DROP VIEW IF EXISTS {};", t.f(&self.view_name))?;
            }
            _ => {}
        }
        self.emit_default(t, f)
    }
}

/// The `OR REPLACE` modifier.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault)]
pub struct OrReplace {
    pub or_token: Token,
    pub replace_token: Token,
}

impl Emit for OrReplace {
    fn emit(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::SQLite3 => Ok(()),
            _ => self.emit_default(t, f),
        }
    }
}

/// The `TEMPORARY` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct Temporary {
    pub temporary_token: Token,
}

/// The part of a `CREATE TABLE` statement that defines the columns.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
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

/// A column definition.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: DataType,
}

/// A `DROP TABLE` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct DropTableStatement {
    pub drop_token: Token,
    pub table_token: Token,
    pub if_exists: Option<IfExists>,
    pub table_name: TableName,
}

/// A `DROP VIEW` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct DropViewStatement {
    pub drop_token: Token,
    pub view_token: Token,
    pub if_exists: Option<IfExists>,
    pub view_name: TableName,
}

/// An `IF EXISTS` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault)]
pub struct IfExists {
    pub if_token: Token,
    pub exists_token: Token,
}

/// Parse BigQuery SQL.
pub fn parse_sql(filename: &str, sql: &str) -> Result<SqlProgram> {
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
            let files = SimpleFile::new(filename.to_owned(), sql.to_string());
            let diagnostic = Diagnostic::error()
                .with_message("Failed to parse query")
                .with_labels(vec![Label::primary(
                    (),
                    e.location.offset..e.location.offset + 1,
                )
                .with_message(format!("expected {}", e.expected))]);
            Err(SourceError {
                source: e,
                files,
                diagnostic,
            }
            .into())
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
            = leading_ws:t("") statements:sep_opt_trailing(<statement()>, ";")
              { SqlProgram { leading_ws, statements } }

        rule statement() -> Statement
            = s:query_statement() { Statement::Query(s) }
            / i:insert_into_statement() { Statement::InsertInto(i) }
            / d:delete_from_statement() { Statement::DeleteFrom(d) }
            / c:create_table_statement() { Statement::CreateTable(c) }
            / c:create_view_statement() { Statement::CreateView(c) }
            / d:drop_table_statement() { Statement::DropTable(d) }
            / d:drop_view_statement() { Statement::DropView(d) }

        rule query_statement() -> QueryStatement
            = query_expression:query_expression() { QueryStatement { query_expression } }

        rule insert_into_statement() -> InsertIntoStatement
            = insert_token:k("INSERT") into_token:k("INTO") table_name:table_name() inserted_data:inserted_data() {
                InsertIntoStatement {
                    insert_token,
                    into_token,
                    table_name,
                    inserted_data,
                }
            }

        rule inserted_data() -> InsertedData
            = query:query_expression() { InsertedData::Select { query } }
            / values_token:k("VALUES") rows:sep_opt_trailing(<row()>, ",") {
                InsertedData::Values {
                    values_token,
                    rows,
                }
            }

        rule row() -> Row
            = paren1:t("(") expressions:sep_opt_trailing(<expression()>, ",") paren2:t(")") {
                Row {
                    paren1,
                    expressions,
                    paren2,
                }
            }

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
              having:having()?
              qualify:qualify()?
              order_by:order_by()?
              limit:limit()?
            {
                SelectExpression {
                    select_options,
                    select_list,
                    from_clause,
                    where_clause,
                    group_by,
                    having,
                    qualify,
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
        /// We split `expression` and `expression_no_and` because `AND` also
        /// appears in `x BETWEEN y AND z`, and we don't want to parse that as
        /// `x BETWEEN (y AND z)`. But `peg` is greedy and will do that if we
        /// let it.
        ///
        /// [precedence table]:
        ///     https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#operator_precedence
        rule expression() -> Expression = precedence! {
            left:(@) or_token:k("OR") right:@ { Expression::Or { left: Box::new(left), or_token, right: Box::new(right) } }
            --
            left:(@) and_token:k("AND") right:@ { Expression::And { left: Box::new(left), and_token, right: Box::new(right) } }
            --
            expr:expression_no_and() { expr }
        }

        rule expression_no_and() -> Expression = precedence! {
            not_token:k("NOT") expression:@ { Expression::Not { not_token, expression: Box::new(expression) } }
            --
            left:(@) is_token:k("IS") right:@ { Expression::Is { left: Box::new(left), is_token, right: Box::new(right) } }
            left:(@) is_token:k("IS") not_token:k("NOT") right:@ { Expression::IsNot { left: Box::new(left), is_token, not_token, right: Box::new(right) } }
            left:(@) not_token:k("NOT")? in_token:k("IN") value_set:in_value_set() {
                Expression::In {
                    left: Box::new(left),
                    not_token,
                    in_token,
                    value_set,
                }
            }
            left:(@) not_token:k("NOT")? between_token:k("BETWEEN") middle:expression_no_and() and_token:k("AND") right:@ {
                Expression::Between {
                    left: Box::new(left),
                    not_token,
                    between_token,
                    middle: Box::new(middle),
                    and_token,
                    right: Box::new(right),
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
            arr:(@) bracket1:t("[") index:index_offset() bracket2:t("]") {
                Expression::Index(IndexExpression {
                    expression: Box::new(arr),
                    bracket1,
                    index,
                    bracket2,
                })
            }
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
            struct_expression:struct_expression() { Expression::Struct(struct_expression) }
            count_expression:count_expression() { Expression::Count(count_expression) }
            current_date:current_date() { Expression::CurrentDate(current_date) }
            special_date_function_call:special_date_function_call() { Expression::SpecialDateFunctionCall(special_date_function_call) }
            function_call:function_call() { Expression::FunctionCall(function_call) }
            paren1:t("(") expression:expression() paren2:t(")") { Expression::Parens { paren1, expression: Box::new(expression), paren2 } }
            literal:literal() { literal }
            null_token:k("NULL") { Expression::Null { null_token } }
            interval_expression:interval_expression() { Expression::Interval(interval_expression) }
            table_and_column_name:table_and_column_name() {
                Expression::TableAndColumnName(table_and_column_name)
            }
            column_name:ident() { Expression::ColumnName(column_name) }
            cast:cast() { Expression::Cast(cast) }
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
            / quiet! {
                s:position!()
                chars_and_raw:with_slice(<"'" chars:([^ '\\' | '\''] / escape())* "'" { chars }>)
                ws:$(_)
                e:position!()
            {
                let (chars, raw) = chars_and_raw;
                let token = Token {
                    span: s..e,
                    ws_offset: raw.len(),
                    text: format!("{}{}", raw, ws),
                };
                Expression::Literal {
                    token,
                    value: LiteralValue::String(String::from_iter(chars)),
                }
            } }
            / quiet! { token:token("string", <"r'" [^ '\'']* "'">) {
                let trimmed = &token.token_str()[2..token.token_str().len() - 1];
                let value = LiteralValue::String(trimmed.to_string());
                Expression::Literal { token, value }
            } }
            / quiet! { token:token("string", <"r\"" [^ '"']* "\"">) {
                let trimmed = &token.token_str()[2..token.token_str().len() - 1];
                let value = LiteralValue::String(trimmed.to_string());
                Expression::Literal { token, value }
            } }
            / expected!("literal")

        rule interval_expression() -> IntervalExpression
            = interval_token:k("INTERVAL") number:token("integer", <"-"? ['0'..='9']+>) date_part:date_part() {
                IntervalExpression {
                    interval_token,
                    number,
                    date_part,
                }
            }

        rule date_part() -> DatePart
            = date_part_token:(k("YEAR") / k("QUARTER") / k("MONTH") / k("WEEK") / k("DAY") / k("HOUR") / k("MINUTE") / k("SECOND")) {
                DatePart { date_part_token }
            }

        rule in_value_set() -> InValueSet
            = paren1:t("(") query_expression:query_expression() paren2:t(")") {
                InValueSet::QueryExpression {
                    paren1,
                    query: Box::new(query_expression),
                    paren2,
                }
            }
            / paren1:t("(") expressions:sep(<expression()>, ",") paren2:t(")") {
                InValueSet::ExpressionList {
                    paren1,
                    expressions,
                    paren2,
                }
            }
            / unnest_token:k("UNNEST") paren1:t("(") expression:expression() paren2:t(")") {
                InValueSet::Unnest {
                    unnest_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                }
            }

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

        rule index_offset() -> IndexOffset
            = offset_token:k("OFFSET") paren1:t("(") expression:expression() paren2:t(")") {
                IndexOffset::Offset {
                    offset_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                }
            }
            / ordinal_token:k("ORDINAL") paren1:t("(") expression:expression() paren2:t(")") {
                IndexOffset::Ordinal {
                    ordinal_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                }
            }
            / expression:expression() { IndexOffset::Simple(Box::new(expression)) }

        rule array_expression() -> ArrayExpression
            = delim1:t("[") definition:array_definition()? delim2:t("]") {
                ArrayExpression {
                    array_token: None,
                    element_type: None,
                    delim1,
                    definition,
                    delim2,
                }
            }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:t("[") definition:array_definition()? delim2:t("]") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    definition,
                    delim2,
                }
              }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:t("(") definition:array_definition()? delim2:t(")") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    definition,
                    delim2,
                }
              }

        rule array_definition() -> ArrayDefinition
            = query:query_expression() { ArrayDefinition::Query(Box::new(query)) }
            / expressions:sep(<expression()>, ",") { ArrayDefinition::Elements(expressions) }

        rule struct_expression() -> StructExpression
            = struct_token:k("STRUCT") paren1:t("(") fields:select_list() paren2:t(")") {
                StructExpression {
                    struct_token,
                    paren1,
                    fields,
                    paren2,
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

        rule current_date() -> CurrentDate
            = current_date_token:k("CURRENT_DATE") empty_parens:empty_parens()? {
                CurrentDate {
                    current_date_token,
                    empty_parens,
                }
            }

        rule empty_parens() -> EmptyParens
            = paren1:t("(") paren2:t(")") {
                EmptyParens { paren1, paren2 }
            }

        rule special_date_function_call() -> SpecialDateFunctionCall
            = function_name:special_date_function_name() paren1:t("(")
              args:sep(<expression_or_date_part()>, ",") paren2:t(")") {
                SpecialDateFunctionCall {
                    function_name,
                    paren1,
                    args,
                    paren2,
                }
            }

        rule special_date_function_name() -> Identifier
            = token:(t("DATE_DIFF") / t("DATE_TRUNC") / t("DATETIME_DIFF") / t("DATETIME_TRUNC")) {
                Identifier::from_simple_token(token)
            }

        rule expression_or_date_part() -> ExpressionOrDatePart
            = expression:expression() { ExpressionOrDatePart::Expression(expression) }
            / date_part:date_part() { ExpressionOrDatePart::DatePart(date_part) }

        rule function_call() -> FunctionCall
            = name:function_name() paren1:t("(")
              args:sep_opt_trailing(<expression()>, ",")? paren2:t(")")
              over_clause:over_clause()?
            {
                FunctionCall {
                    name,
                    paren1,
                    args: args.unwrap_or_else(|| NodeVec::new(",")),
                    paren2,
                    over_clause,
                }
            }

        rule function_name() -> FunctionName
            = dotted:dotted_function_name()

        rule over_clause() -> OverClause
            = over_token:k("OVER") paren1:t("(")
            partition_by:partition_by()?
            order_by:order_by()?
            window_frame:window_frame()?
            paren2:t(")")
            {
                OverClause {
                    over_token,
                    paren1,
                    partition_by,
                    order_by,
                    window_frame,
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
            = direction:(k("ASC") / k("DESC")) nulls_clause:nulls_clause()? {
                AscDesc { direction, nulls_clause }
            }

        rule nulls_clause() -> NullsClause
            = nulls_token:k("NULLS") first_last_token:(k("FIRST") / k("LAST")) {
                NullsClause { nulls_token, first_last_token }
            }

        rule limit() -> Limit
            = limit_token:k("LIMIT") value:expression() {
                Limit {
                    limit_token,
                    value: Box::new(value),
                }
            }

        rule window_frame() -> WindowFrame
            = rows_token:(k("ROWS") / k("RANGE")) definition:window_frame_definition() {
                WindowFrame {
                    rows_token,
                    definition,
                }
            }

        rule window_frame_definition() -> WindowFrameDefinition
            = between_token:k("BETWEEN") start:window_frame_start() and_token:k("AND") end:window_frame_end() {
                WindowFrameDefinition::Between {
                    between_token,
                    start,
                    and_token,
                    end,
                }
            }
            / start:window_frame_start() {
                WindowFrameDefinition::Start(start)
            }

        rule window_frame_start() -> WindowFrameStart
            = unbounded_token:k("UNBOUNDED") preceding_token:k("PRECEDING") {
                WindowFrameStart::UnboundedPreceding {
                    unbounded_token,
                    preceding_token,
                }
            }

        rule window_frame_end() -> WindowFrameEnd
            = current_token:k("CURRENT") row_token:k("ROW") {
                WindowFrameEnd::CurrentRow {
                    current_token,
                    row_token,
                }
            }

        rule cast() -> Cast
            = cast_type:cast_type() paren1:t("(") expression:expression() as_token:k("AS") data_type:data_type() paren2:t(")") {
                Cast {
                    cast_type,
                    paren1,
                    expression: Box::new(expression),
                    as_token,
                    data_type,
                    paren2,
                }
            }

        rule cast_type() -> CastType
            = cast_token:k("CAST") { CastType::Cast { cast_token } }
            / safe_cast_token:k("SAFE_CAST") { CastType::SafeCast { safe_cast_token } }

        rule data_type() -> DataType
            = token:(ti("BOOLEAN") / ti("BOOL")) { DataType::Bool(token) }
            / token:ti("BYTES") { DataType::Bytes(token) }
            / token:ti("DATETIME") { DataType::Datetime(token) }
            / token:ti("DATE") { DataType::Date(token) }
            / token:ti("FLOAT64") { DataType::Float64(token) }
            / token:ti("GEOGRAPHY") { DataType::Geography(token) }
            / token:ti("INT64") { DataType::Int64(token) }
            / token:ti("NUMERIC") { DataType::Numeric(token) }
            / token:ti("STRING") { DataType::String(token) }
            / token:ti("TIMESTAMP") { DataType::Timestamp(token) }
            / token:ti("TIME") { DataType::Time(token) }
            / array_token:k("ARRAY") lt:t("<") data_type:data_type() gt:t(">") {
                DataType::Array {
                    array_token,
                    lt,
                    data_type: Box::new(data_type),
                    gt,
                }
            }
            / struct_token:k("STRUCT") lt:t("<") fields:sep_opt_trailing(<struct_field()>, ",") gt:t(">") {
                DataType::Struct {
                    struct_token,
                    lt,
                    fields,
                    gt,
                }
            }

        rule struct_field() -> StructField
            // We need to be careful how we parse this because of how `peg`
            // handles greedy matches. Longest ambigous match must come first.
            = name:ident() data_type:data_type() {
                StructField {
                    name: Some(name),
                    data_type,
                }
            }
            / data_type:data_type() {
                StructField {
                    name: None,
                    data_type,
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
            = join_type:join_type() join_token:k("JOIN") from_item:from_item() operator:condition_join_operator()? {
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

        rule having() -> Having
            = having_token:k("HAVING") expression:expression() {
                Having {
                    having_token,
                    expression,
                }
            }

        rule qualify() -> Qualify
            = qualify_token:k("QUALIFY") expression:expression() {
                Qualify {
                    qualify_token,
                    expression,
                }
            }

        rule create_table_statement() -> CreateTableStatement
            = create_token:k("CREATE")
              or_replace:or_replace()?
              temporary:temporary()?
              table_token:k("TABLE") table_name:table_name()
              definition:create_table_definition()
              e:position!()
            {
                CreateTableStatement {
                    create_token,
                    or_replace,
                    temporary,
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

        rule temporary() -> Temporary
            = temporary_token:(k("TEMPORARY") / k("TEMP")) {
                Temporary { temporary_token }
            }

        rule create_table_definition() -> CreateTableDefinition
            = paren1:t("(") columns:sep_opt_trailing(<column_definition()>, ",") paren2:t(")") {
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

        rule drop_table_statement() -> DropTableStatement
            = drop_token:k("DROP") table_token:k("TABLE") if_exists:if_exists()? table_name:table_name() {
                DropTableStatement {
                    drop_token,
                    table_token,
                    if_exists,
                    table_name,
                }
            }

        rule drop_view_statement() -> DropViewStatement
            // Oddly, BigQuery accepts `DELETE VIEW`.
            = drop_token:(k("DROP") / k("DELETE")) view_token:k("VIEW") if_exists:if_exists()? view_name:table_name() {
                DropViewStatement {
                    // Fix this at parse time. Nobody wants `DELETE VIEW`.
                    drop_token: drop_token.with_token_str("DROP"),
                    view_token,
                    if_exists,
                    view_name,
                }
            }

        rule if_exists() -> IfExists
            = if_token:k("IF") exists_token:k("EXISTS") {
                IfExists {
                    if_token,
                    exists_token,
                }
            }

        /// A table name, such as `t1` or `project-123.dataset1.table2`.
        rule table_name() -> TableName
            // We handle this manually because of PEG backtracking limitations.
            = dotted:dotted_name() {?
                let len = dotted.items.len();
                let (mut nodes, mut dots) = dotted.into_node_and_sep_iters();
                if len == 1 {
                    Ok(TableName::Table { table: nodes.next().unwrap() })
                } else if len == 3 {
                    Ok(TableName::DatasetTable {
                        dataset: nodes.next().unwrap(),
                        dot: dots.next().unwrap(),
                        table: nodes.next().unwrap(),
                    })
                } else if len == 5 {
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
                let len = dotted.items.len();
                let (mut nodes, mut dots) = dotted.into_node_and_sep_iters();
                if len == 3 {
                    Ok(TableAndColumnName {
                        table_name: TableName::Table { table: nodes.next().unwrap() },
                        dot: dots.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 5 {
                    Ok(TableAndColumnName {
                        table_name: TableName::DatasetTable {
                            dataset: nodes.next().unwrap(),
                            dot: dots.next().unwrap(),
                            table: nodes.next().unwrap(),
                        },
                        dot: dots.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 7 {
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

        rule dotted_function_name() -> FunctionName
            // We handle this manually because of PEG backtracking limitations.
            = dotted:dotted_name() {?
                let len = dotted.items.len();
                let (mut nodes, mut dots) = dotted.into_node_and_sep_iters();
                if len == 1 {
                    Ok(FunctionName::Function {
                        function: nodes.next().unwrap()
                    })
                } else if len == 3 {
                    Ok(FunctionName::DatasetFunction {
                        dataset: nodes.next().unwrap(),
                        dot: dots.next().unwrap(),
                        function: nodes.next().unwrap(),
                    })
                } else if len == 5 {
                    Ok(FunctionName::ProjectDatasetFunction {
                        project: nodes.next().unwrap(),
                        dot1: dots.next().unwrap(),
                        dataset: nodes.next().unwrap(),
                        dot2: dots.next().unwrap(),
                        function: nodes.next().unwrap(),
                    })
                } else {
                    Err("function name")
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
                        text: id.to_string(),
                    })
                }
            }
            / s:position!() id_and_raw:with_slice(<"`" id:(([^ '\\' | '`'] / escape())*) "`" { id }>) ws:$(_) e:position!() {
                let (id, raw) = id_and_raw;
                Identifier {
                    token: Token {
                        span: s..e,
                        ws_offset: raw.len(),
                        text: format!("{}{}", raw, ws),
                    },
                    text: String::from_iter(id),
                }
            }
            / expected!("identifier")

        /// Return both the value and slice matched by the rule. See
        /// https://github.com/kevinmehall/rust-peg/issues/283.
        rule with_slice<T>(r: rule<T>) -> (T, &'input str)
            = value:&r() input:$(r()) { (value, input) }

        /// Low-level rule for matching a C-style identifier.
        rule c_ident() -> String
            = quiet! { id:$(c_ident_start() c_ident_cont()*)
              // The next character cannot be a valid ident character.
              !c_ident_cont()
              { id.to_string() } }
            / expected!("identifier")
        rule c_ident_start() = ['a'..='z' | 'A'..='Z' | '_']
        rule c_ident_cont() = ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']

        /// Escape sequences. The unwrap calls below should never fail because
        /// the grammar should have already validated the escape sequence.
        rule escape() -> char =
            "\\" c:(octal_escape() / hex_escape() / unicode_escape_4() /
                    unicode_escape_8() / simple_escape()) { c }
        rule octal_escape() -> char = s:$(['0'..='7'] * <3,3>) {
            char::try_from(u32::from_str_radix(s, 8).unwrap()).unwrap()
        }
        rule hex_escape() -> char = ("x" / "X") s:$(hex_digit() * <2,2>) {
            char::try_from(u32::from_str_radix(s, 16).unwrap()).unwrap()
        }
        rule unicode_escape_4() -> char = "u" s:$(hex_digit() * <4,4>) {?
            // Not all u32 values are valid Unicode code points.
            char::try_from(u32::from_str_radix(s, 16).unwrap())
                .or(Err("valid Unicode code point"))
        }
        rule unicode_escape_8() -> char = "U" s:$(hex_digit() * <8,8>) {?
            // Not all u32 values are valid Unicode code points.
            char::try_from(u32::from_str_radix(s, 16).unwrap())
                .or(Err("valid Unicode code point"))
        }
        rule simple_escape() -> char
            = "\\" { '\\' }
            / "'" { '\'' }
            / "\"" { '"' }
            / "`" { '`' }
            / "a" { '\x07' }
            / "b" { '\x08' }
            / "f" { '\x0C' }
            / "n" { '\n' }
            / "r" { '\r' }
            / "t" { '\t' }
            / "v" { '\x0B' }
            / "?" { '?' }
        rule hex_digit() = ['0'..='9' | 'a'..='f' | 'A'..='F']

        /// Punctuation separated list. Does not allow a trailing separator.
        rule sep<T: Node>(node: rule<T>, separator: &'static str) -> NodeVec<T>
            = first:node() rest:(sep:t(separator) node:node() { (sep, node) })* {
                let mut items = Vec::new();
                items.push(NodeOrSep::Node(first));
                for (sep, node) in rest {
                    items.push(NodeOrSep::Sep(sep));
                    items.push(NodeOrSep::Node(node));
                }
                NodeVec { separator, items }
            }

        /// Punctuation separated list. Allows a trailing separator.
        rule sep_opt_trailing<T: Node>(item: rule<T>, separator: &'static str) -> NodeVec<T>
            = list:sep(item, separator) trailing_sep:t(separator)? {
                let mut list = list;
                if let Some(trailing_sep) = trailing_sep {
                    list.items.push(NodeOrSep::Sep(trailing_sep));
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

        /// Case-insensitive tokens. These are sort of like keywords, but don't
        /// appear in the `KEYWORDS` set and don't need to be quoted if used
        /// as a column name.
        rule ti(token: &'static str) -> Token
            = want(token, &str::eq_ignore_ascii_case)
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
    use crate::drivers::{sqlite3::SQLite3Locator, Locator};

    use super::*;

    #[tokio::test]
    async fn test_parser_and_run_with_sqlite3() {
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
            (
                r"SELECT a, COUNT(*) AS c FROM t GROUP BY a HAVING c > 5",
                None,
            ),
            (r"SELECT * FROM t ORDER BY a LIMIT 10", None),
            (r"SELECT * FROM t ORDER BY a DESC NULLS LAST", None),
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
            (r"SELECT * FROM t WHERE a BETWEEN 1 AND 10", None),
            (r"SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10", None),
            (r"SELECT INTERVAL -3 DAY", None),
            (r"SELECT * FROM t WHERE a IN (1,2)", None),
            (r"SELECT * FROM t WHERE a NOT IN (1,2)", None),
            (r"SELECT * FROM t WHERE a IN (SELECT b FROM t)", None),
            (r"SELECT * FROM t WHERE a IN UNNEST([1])", None),
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
            (r"SELECT function.custom()", None),
            (
                r"SELECT format_datetime('%Y-%Q', current_datetime()) AS uuid",
                None,
            ),
            (r"SELECT CURRENT_DATE()", None),
            (r"SELECT CURRENT_DATE", None), // Why not both? Sigh.
            (r"SELECT DATETIME(2008, 12, 25, 05, 30, 00)", None),
            (
                r"SELECT DATE_DIFF(CURRENT_DATE(), CURRENT_DATE(), DAY)",
                None,
            ),
            (
                r"SELECT DATETIME_DIFF(CURRENT_DATETIME(), CURRENT_DATETIME(), DAY)",
                None,
            ),
            (r"SELECT DATE_TRUNC(CURRENT_DATE(), DAY)", None),
            (r"SELECT DATETIME_TRUNC(CURRENT_DATETIME(), DAY)", None),
            (
                r"SELECT a, RANK() OVER (PARTITION BY a ORDER BY b ASC) AS rank FROM t QUALIFY rank = 1",
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
            (r#"SELECT * FROM t AS t1 JOIN t AS t2"#, None),
            (r#"SELECT * FROM t AS t1 CROSS JOIN t AS t2"#, None),
            (r#"SELECT * FROM t CROSS JOIN UNNEST(a) AS t2"#, None),
            (r#"select * from `d`.`t`"#, Some(r#"select * from d.t"#)),
            (r#"SELECT a, CAST(NULL AS BOOL) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS ARRAY<INT64>) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS FLOAT64) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS STRING) AS c FROM t"#, None),
            (r#"SELECT a, CAST(NULL AS DATETIME) AS c FROM t"#, None),
            (r#"SELECT a, SAFE_CAST(NULL AS BOOL) AS c FROM t"#, None),
            (r#"SELECT ARRAY(SELECT 1)"#, None),
            (r#"SELECT ARRAY(1, 2)"#, None),
            (r#"SELECT ARRAY[1, 2]"#, None),
            (r#"SELECT [1, 2]"#, None),
            (r#"SELECT [1, 2][0]"#, None),
            (r#"SELECT [1, 2][OFFSET(0)]"#, None),
            (r#"SELECT [1, 2][ORDINAL(0)]"#, None),
            (r#"SELECT ARRAY<INT64>[]"#, None),
            (r#"SELECT ARRAY<STRUCT<INT64, INT64>>[]"#, None),
            (r#"SELECT ARRAY<STRUCT<a INT64, b INT64>>[]"#, None),
            (r#"SELECT STRUCT(a, b) FROM t"#, None),
            (r#"SELECT STRUCT(a AS c) FROM t"#, None),
            (r#"WITH t2 AS (SELECT * FROM t) SELECT * FROM t2"#, None),
            (r#"INSERT INTO t VALUES (1, 2), (3,4)"#, None),
            (r#"INSERT INTO t (SELECT * FROM t)"#, None),
            (r#"DELETE FROM t WHERE a = 0"#, None),
            (r#"DELETE FROM t AS t2 WHERE a = 0"#, None),
            (r#"CREATE OR REPLACE TABLE t2 (a INT64, b INT64)"#, None),
            (r#"CREATE OR REPLACE TABLE t2 AS (SELECT * FROM t)"#, None),
            (r#"DROP TABLE t2"#, None),
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
        let mut conn = SQLite3Locator::memory().driver().await.unwrap();

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
        conn.execute_native_sql_statement(fixtures)
            .await
            .expect("failed to create SQLite3 fixtures");

        for &(sql, normalized) in sql_examples {
            println!("parsing:   {}", sql);
            let normalized = normalized.unwrap_or(sql);
            let parsed = match parse_sql("test.sq", sql) {
                Ok(parsed) => parsed,
                Err(err) => {
                    err.emit();
                    panic!("{}", err);
                }
            };
            assert_eq!(normalized, &parsed.emit_to_string(Target::BigQuery));

            let sql = parsed.emit_to_string(Target::SQLite3);
            println!("  SQLite3: {}", sql);
            if let Err(err) = conn.execute_native_sql_statement(&sql).await {
                panic!("failed to execute with SQLite3:\n{}\n{}", sql, err);
            }
        }
    }
}
