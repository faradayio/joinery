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
    fmt::{self},
    hash,
    io::{self, Write as _},
    mem::take,
};

use derive_visitor::{Drive, DriveMut};
use joinery_macros::{Emit, EmitDefault, Spanned, ToTokens};

use crate::{
    drivers::{
        bigquery::{BigQueryName, BigQueryString},
        snowflake::{SnowflakeString, KEYWORDS as SNOWFLAKE_KEYWORDS},
        sqlite3::KEYWORDS as SQLITE3_KEYWORDS,
        trino::{TrinoString, KEYWORDS as TRINO_KEYWORDS},
    },
    errors::{format_err, Error, Result},
    known_files::{FileId, KnownFiles},
    tokenizer::{
        tokenize_sql, EmptyFile, Ident, Keyword, Literal, LiteralValue, PseudoKeyword, Punct,
        RawToken, Span, Spanned, ToTokens, Token, TokenStream, TokenWriter,
    },
    types::TableType,
    util::{is_c_ident, AnsiIdent, AnsiString},
};

/// None of these keywords should ever be matched as a bare Ident. We use
/// [`phf`](https://github.com/rust-phf/rust-phf), which generates "perfect hash
/// functions." These allow us to create highly optimized, read-only sets/maps
/// generated at compile time.
static KEYWORDS: phf::Set<&'static str> = phf::phf_set! {
    "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT",
    "BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS",
    "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END",
    "ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE",
    "FETCH", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS",
    "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL",
    "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE",
    "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER",
    "OUTER", "OVER", "PARTITION", "PRECEDING", "PROTO", "QUALIFY", "RANGE",
    "RECURSIVE", "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME",
    "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE", "UNBOUNDED",
    "UNION", "UNNEST", "USING", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN",
};

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
    /// Is the specified string a keyword?
    pub fn is_keyword(self, s: &str) -> bool {
        let keywords = match self {
            Target::BigQuery => &KEYWORDS,
            Target::Snowflake => &SNOWFLAKE_KEYWORDS,
            Target::SQLite3 => &SQLITE3_KEYWORDS,
            Target::Trino => &TRINO_KEYWORDS,
        };
        keywords.contains(s.to_ascii_uppercase().as_str())
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

/// A default version of [`Emit`] which attemps to write the AST back out as
/// BigQuery SQL, extremely close to the original input.
///
/// For most types, you will start by using `#[derive(Emit, EmitDefault, Spanned, ToTokens)]`. This
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
    fn emit_default(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()>;
}

/// Emit the AST as code for a specific database.
///
/// If you use `#[derive(Emit, EmitDefault, Spanned, ToTokens)]` on a type, then [`Emit::emit`]
/// will be generated to call [`EmitDefault::emit_default`].
pub trait Emit: Sized {
    /// Format this node for the specified database.
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()>;

    /// Emit this node to a string.
    fn emit_to_string(&self, t: Target) -> String {
        let mut buf = vec![];
        self.emit(t, &mut TokenWriter::from_wtr(&mut buf))
            .expect("Writing to a Vec should never fail");
        String::from_utf8(buf).expect("Emitting to a Vec should always produce valid UTF-8")
    }
}

impl<T: Emit> Emit for Box<T> {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        self.as_ref().emit(t, f)
    }
}

impl<T: Emit> Emit for Option<T> {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        if let Some(node) = self {
            node.emit(t, f)
        } else {
            Ok(())
        }
    }
}

impl<T: Emit> Emit for Vec<T> {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        for node in self.iter() {
            node.emit(t, f)?;
        }
        Ok(())
    }
}

impl Emit for RawToken {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        emit_whitespace(self.leading_whitespace(), t, f)?;
        f.write_token_start(self.as_str())?;
        emit_whitespace(self.trailing_whitespace(), t, f)
    }
}

impl Emit for Token {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match self {
            Token::EmptyFile(empty_file) => empty_file.emit(t, f),
            Token::Ident(ident) => ident.emit(t, f),
            Token::Literal(literal) => literal.emit(t, f),
            Token::Punct(punct) => punct.emit(t, f),
        }
    }
}

impl Emit for EmptyFile {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        // Nothing but whitespace.
        emit_whitespace(self.token.as_raw_str(), t, f)
    }
}

impl Emit for Ident {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        emit_whitespace(self.token.leading_whitespace(), t, f)?;
        f.mark_token_start();
        if t.is_keyword(&self.name) || !is_c_ident(&self.name) {
            match t {
                Target::BigQuery => write!(f, "{}", BigQueryName(&self.name))?,
                Target::Snowflake | Target::SQLite3 | Target::Trino => {
                    write!(f, "{}", AnsiIdent(&self.name))?;
                }
            }
        } else {
            f.write_token_start(self.name.as_str())?;
        }
        emit_whitespace(self.token.trailing_whitespace(), t, f)
    }
}

impl Emit for PseudoKeyword {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        // TODO: Treat these as pseudo-keywords for now. We need to think about
        // how this works across databases.
        emit_whitespace(self.ident.token.leading_whitespace(), t, f)?;
        f.write_token_start(&self.ident.name)?;
        emit_whitespace(self.ident.token.trailing_whitespace(), t, f)
    }
}

impl Emit for Keyword {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        emit_whitespace(self.ident.token.leading_whitespace(), t, f)?;
        f.write_token_start(&self.ident.name)?;
        emit_whitespace(self.ident.token.trailing_whitespace(), t, f)
    }
}

impl Emit for Literal {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        emit_whitespace(self.token.leading_whitespace(), t, f)?;
        self.value.emit(t, f)?;
        emit_whitespace(self.token.trailing_whitespace(), t, f)
    }
}

impl Emit for LiteralValue {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        f.mark_token_start();
        match self {
            LiteralValue::Int64(i) => write!(f, "{}", i),
            LiteralValue::Float64(fl) => write!(f, "{}", fl),
            LiteralValue::String(s) => match t {
                Target::BigQuery => write!(f, "{}", BigQueryString(s)),
                Target::Snowflake => write!(f, "{}", SnowflakeString(s)),
                Target::SQLite3 => write!(f, "{}", AnsiString(s)),
                Target::Trino => write!(f, "{}", TrinoString(s)),
            },
        }
    }
}

impl Emit for Punct {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        self.token.emit(t, f)
    }
}

/// Emit whitespace, converting comments to one of the portable comment
/// formats.
fn emit_whitespace(ws: &str, t: Target, f: &mut dyn io::Write) -> io::Result<()> {
    if t != Target::BigQuery {
        let mut in_hash_comment = false;
        for c in ws.chars() {
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
    } else {
        write!(f, "{}", ws)?;
    }
    Ok(())
}

/// A node type, for use with [`NodeVec`].
pub trait Node:
    Clone + fmt::Debug + Drive + DriveMut + Emit + Spanned + ToTokens + 'static
{
}

impl<T: Clone + fmt::Debug + Drive + DriveMut + Emit + Spanned + ToTokens + 'static> Node for T {}

/// Either a node or a separator token.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum NodeOrSep<T: Node> {
    Node(T),
    Sep(Punct),
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
#[derive(Debug, ToTokens)]
pub struct NodeVec<T: Node> {
    /// The separator to use when adding items.
    #[to_tokens(skip)]
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
            self.items
                .push(NodeOrSep::Sep(Punct::new(self.separator, node.span())));
        }
        self.items.push(NodeOrSep::Node(node));
    }

    /// Add a node or a separator to this [`NodeVec`], inserting or removing
    /// separators as needed to ensure that the vector is well-formed.
    pub fn push_node_or_sep(&mut self, node_or_sep: NodeOrSep<T>) {
        match &node_or_sep {
            NodeOrSep::Node(node) => {
                if let Some(NodeOrSep::Node(_)) = self.items.last() {
                    self.items
                        .push(NodeOrSep::Sep(Punct::new(self.separator, node.span())));
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
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        for (i, node_or_sep) in self.items.iter().enumerate() {
            let is_last = i + 1 == self.items.len();
            match node_or_sep {
                NodeOrSep::Node(node) => node.emit(t, f)?,
                NodeOrSep::Sep(sep) if is_last && t != Target::BigQuery => {
                    sep.token.with_ws_only().emit(t, f)?
                }
                NodeOrSep::Sep(sep) => sep.emit(t, f)?,
            }
        }
        Ok(())
    }
}

/// A name, consisting of one or more identifiers separated by dots.
///
/// This may represent a number of things in different contexts, including:
///
/// - A table.
/// - A column.
/// - A function.
///
/// All of these are ASCII case insensitive in BigQuery, but with [certain
/// complications][case]:
///
/// - The BigQuery docs claim that user-defined functions are case-sensitive,
///   but testing reveals this is not the case.
/// - Datasets may supposedly be configured to enable case-sensitivite table
///   names, but we do not currently attempt to support this.
///
/// [case]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, ToTokens)]
pub struct Name {
    /// Our name, as it appeared in the source code.
    items: NodeVec<Ident>,

    /// A version of our name that we can use as a key in a map.
    #[emit(skip)]
    #[to_tokens(skip)]
    #[drive(skip)]
    key: Vec<String>,
}

impl Name {
    /// Create a new [`Name`] from a string and a span.
    pub fn new(s: &str, span: Span) -> Name {
        let ident = Ident::new(s, span);
        ident.into()
    }

    /// Create a new name of the form `table.column`.
    pub fn new_table_column(table: &str, column: &str, span: Span) -> Name {
        let mut node_vec = NodeVec::new(".");
        node_vec.push(Ident::new(table, span.clone()));
        node_vec.push(Ident::new(column, span));
        node_vec.into()
    }

    /// Create from a dotted string.
    pub fn new_from_dotted(s: &str, span: Span) -> Name {
        let mut node_vec = NodeVec::new(".");
        for part in s.split('.') {
            node_vec.push(Ident::new(part, span.clone()));
        }
        node_vec.into()
    }

    /// Combine this name with another name, using a dot as a separator.
    pub fn combine(&self, other: &Name) -> Name {
        let mut node_vec = self.items.clone();
        node_vec.push_node_or_sep(NodeOrSep::Sep(Punct::new(".", other.span())));
        node_vec.items.extend(other.items.iter().cloned());
        node_vec.into()
    }

    /// Split this name into a table name and a column name.
    pub fn split_table_and_column(&self) -> (Option<Name>, Ident) {
        // No table part.
        if self.items.items.len() == 1 {
            return (
                None,
                self.items
                    .node_iter()
                    .next()
                    .expect("Name should start with ident")
                    .clone(),
            );
        }

        // Split off table part.
        let mut items = self.items.clone();
        match (items.items.pop(), items.items.pop()) {
            (Some(NodeOrSep::Node(ident)), Some(NodeOrSep::Sep(_))) => (Some(items.into()), ident),
            _ => panic!("Invalid name (impossible number of parts): {:?}", self),
        }
    }

    /// Get an unquoted BigQuery version of this name.
    pub fn unescaped_bigquery(&self) -> String {
        self.items
            .iter()
            .map(|item| match item {
                NodeOrSep::Node(ident) => ident.name.clone(),
                NodeOrSep::Sep(_) => ".".to_string(),
            })
            .collect()
    }
}

impl Spanned for Name {
    fn span(&self) -> Span {
        // Write a reasonably efficient implementation of `Spanned::span`,
        // because we'll use it a lot.
        let items = &self.items.items;
        let start = items
            .first()
            .expect("Names must contain at least one element");
        let end = items
            .last()
            .expect("Names must contain at least one element");
        start.span().combined_with(&end.span())
    }
}

impl From<Ident> for Name {
    fn from(ident: Ident) -> Self {
        let mut node_vec = NodeVec::new(".");
        node_vec.push(ident);
        node_vec.into()
    }
}

impl TryFrom<Name> for Ident {
    type Error = Error;

    fn try_from(value: Name) -> std::result::Result<Self, Self::Error> {
        if value.items.items.len() != 1 {
            return Err(format_err!(
                "Expected name to contain exactly one ident: {:?}",
                value
            ));
        }
        match &value.items.items[0] {
            NodeOrSep::Node(ident) => Ok(ident.clone()),
            _ => panic!("Names should always begin with an ident"),
        }
    }
}

impl From<NodeVec<Ident>> for Name {
    fn from(value: NodeVec<Ident>) -> Self {
        assert!(
            !value.items.is_empty(),
            "Names must contain at least one element"
        );
        let key = value
            .node_iter()
            .map(|ident| ident.name.to_ascii_uppercase())
            .collect();
        Name { items: value, key }
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Name {}

impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for Name {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl hash::Hash for Name {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

/// An entire SQL program.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct SqlProgram {
    /// For now, just handle single statements; BigQuery DDL is messy and maybe
    /// out of scope.
    pub statements: NodeVec<Statement>,
}

/// A statement in our abstract syntax tree.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
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
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct QueryStatement {
    pub query_expression: QueryExpression,
}

/// A query expression is a `SELECT` statement, plus some other optional
/// surrounding things. See the [official grammar][]. This is where CTEs and
/// similar things hook into the grammar.
///
/// [official grammar]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#sql_syntax.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum QueryExpression {
    SelectExpression(SelectExpression),
    Nested {
        paren1: Punct,
        query: Box<QueryStatement>,
        paren2: Punct,
    },
    With {
        with_token: Keyword,
        ctes: NodeVec<CommonTableExpression>,
        query: Box<QueryStatement>,
    },
    SetOperation {
        left: Box<QueryExpression>,
        set_operator: SetOperator,
        right: Box<QueryExpression>,
    },
}

/// Common table expressions (CTEs).
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CommonTableExpression {
    pub name: Ident,
    pub as_token: Keyword,
    pub paren1: Punct,
    pub query: Box<QueryStatement>,
    pub paren2: Punct,
}

/// Set operators.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub enum SetOperator {
    UnionAll {
        union_token: Keyword,
        all_token: Keyword,
    },
    UnionDistinct {
        union_token: Keyword,
        distinct_token: Keyword,
    },
    IntersectDistinct {
        intersect_token: Keyword,
        distinct_token: Keyword,
    },
    ExceptDistinct {
        except_token: Keyword,
        distinct_token: Keyword,
    },
}

impl Emit for SetOperator {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
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
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct SelectExpression {
    /// Type information added later by inference.
    #[emit(skip)]
    #[to_tokens(skip)]
    #[drive(skip)]
    pub ty: Option<TableType>,

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
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct SelectOptions {
    pub select_token: Keyword,
    pub distinct: Option<Distinct>,
}

/// The `DISTINCT` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Distinct {
    pub distinct_token: Keyword,
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
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct SelectList {
    pub items: NodeVec<SelectListItem>,
}

/// A single item in a `SELECT` list.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum SelectListItem {
    /// An expression, optionally with an alias.
    Expression {
        expression: Expression,
        alias: Option<Alias>,
    },
    /// A `*` wildcard.
    Wildcard {
        /// Type information added later by inference.
        #[emit(skip)]
        #[to_tokens(skip)]
        #[drive(skip)]
        ty: Option<TableType>,
        star: Punct,
        except: Option<Except>,
    },
    /// A `table.*` wildcard.
    TableNameWildcard {
        /// Type information added later by inference. Note that this does
        /// not necessarily match the original type of `table_name`, because
        /// `USING` may remove columns.
        #[emit(skip)]
        #[to_tokens(skip)]
        #[drive(skip)]
        ty: Option<TableType>,
        table_name: Name,
        dot: Punct,
        star: Punct,
        except: Option<Except>,
    },
}

/// An `EXCEPT` clause.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub struct Except {
    pub except_token: Keyword,
    pub paren1: Punct,
    pub columns: NodeVec<Ident>,
    pub paren2: Punct,
}

impl Emit for Except {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match t {
            Target::Snowflake => {
                self.except_token
                    .ident
                    .token
                    .with_str("EXCLUDE")
                    .emit(t, f)?;
                self.paren1.token.with_ws_only().emit(t, f)?;
                self.columns.emit(t, f)?;
                self.paren2.token.with_ws_only().emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// An SQL expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum Expression {
    Literal(Literal),
    BoolValue(Keyword),
    Null(Keyword),
    Interval(IntervalExpression),
    ColumnName(Name),
    Cast(Cast),
    Is(IsExpression),
    In(InExpression),
    Between(BetweenExpression),
    KeywordBinop(KeywordBinopExpression),
    Not(NotExpression),
    If(IfExpression),
    Case(CaseExpression),
    Binop(BinopExpression),
    Query {
        paren1: Punct,
        query: Box<QueryExpression>,
        paren2: Punct,
    },
    Parens {
        paren1: Punct,
        expression: Box<Expression>,
        paren2: Punct,
    },
    Array(ArrayExpression),
    Struct(StructExpression),
    Count(CountExpression),
    CurrentDate(CurrentDate),
    ArrayAgg(ArrayAggExpression),
    SpecialDateFunctionCall(SpecialDateFunctionCall),
    FunctionCall(FunctionCall),
    Index(IndexExpression),
}

impl Expression {
    /// Create a new keyword binary operator expression.
    fn keyword_binop(left: Expression, op_keyword: Keyword, right: Expression) -> Expression {
        Expression::KeywordBinop(KeywordBinopExpression {
            left: Box::new(left),
            op_keyword,
            right: Box::new(right),
        })
    }

    /// Create a new binary operator expression.
    fn binop(left: Expression, op_token: Punct, right: Expression) -> Expression {
        Expression::Binop(BinopExpression {
            left: Box::new(left),
            op_token,
            right: Box::new(right),
        })
    }
}

/// An `INTERVAL` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct IntervalExpression {
    pub interval_token: Keyword,
    pub number: Literal,
    pub date_part: DatePart,
}

/// A date part in an `INTERVAL` expression, or in the special date functions.S
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct DatePart {
    pub date_part_token: PseudoKeyword,
}

/// A cast expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Cast {
    pub cast_type: CastType,
    pub paren1: Punct,
    pub expression: Box<Expression>,
    pub as_token: Keyword,
    pub data_type: DataType,
    pub paren2: Punct,
}

/// What type of cast do we want to perform?
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub enum CastType {
    Cast { cast_token: Keyword },
    SafeCast { safe_cast_token: PseudoKeyword },
}

impl Emit for CastType {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match self {
            CastType::SafeCast { safe_cast_token }
                if t == Target::Snowflake || t == Target::Trino =>
            {
                safe_cast_token.ident.token.with_str("TRY_CAST").emit(t, f)
            }
            // TODO: This isn't strictly right, but it's as close as I know how to
            // get with SQLite3.
            CastType::SafeCast { safe_cast_token } if t == Target::SQLite3 => {
                safe_cast_token.ident.token.with_str("CAST").emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// An `IS` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct IsExpression {
    pub left: Box<Expression>,
    pub is_token: Keyword,
    pub not_token: Option<Keyword>,
    pub predicate: IsExpressionPredicate,
}

/// An `IS` predicate.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub enum IsExpressionPredicate {
    Null(Keyword),
    True(Keyword),
    False(Keyword),
    Unknown(PseudoKeyword),
}

impl Emit for IsExpressionPredicate {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        // Remap UNKNOWN to NULL for everyone but BigQuery.
        match (self, t) {
            (IsExpressionPredicate::Unknown(_), Target::BigQuery) => self.emit_default(t, f),
            (IsExpressionPredicate::Unknown(unknown_token), _) => {
                unknown_token.ident.token.with_str("NULL").emit(t, f)
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// An `IN` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct InExpression {
    pub left: Box<Expression>,
    pub not_token: Option<Keyword>,
    pub in_token: Keyword,
    pub value_set: InValueSet,
}

/// A value set for an `IN` expression. See the [official grammar][].
///
/// [official grammar]:
///     https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum InValueSet {
    QueryExpression {
        paren1: Punct,
        query: Box<QueryExpression>,
        paren2: Punct,
    },
    ExpressionList {
        paren1: Punct,
        expressions: NodeVec<Expression>,
        paren2: Punct,
    },
    /// `IN UNNEST` is handled using a special grammar rule.
    Unnest {
        unnest_token: Keyword,
        paren1: Punct,
        expression: Box<Expression>,
        paren2: Punct,
    },
}

/// A `BETWEEN` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct BetweenExpression {
    pub left: Box<Expression>,
    pub not_token: Option<Keyword>,
    pub between_token: Keyword,
    pub middle: Box<Expression>,
    pub and_token: Keyword,
    pub right: Box<Expression>,
}

/// A binary operator represented as a keyword.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct KeywordBinopExpression {
    pub left: Box<Expression>,
    pub op_keyword: Keyword,
    pub right: Box<Expression>,
}

/// A `NOT` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct NotExpression {
    pub not_token: Keyword,
    pub expression: Box<Expression>,
}

/// An `IF` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct IfExpression {
    pub if_token: Keyword,
    pub paren1: Punct,
    pub condition: Box<Expression>,
    pub comma1: Punct,
    pub then_expression: Box<Expression>,
    pub comma2: Punct,
    pub else_expression: Box<Expression>,
    pub paren2: Punct,
}

/// A `CASE` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CaseExpression {
    pub case_token: Keyword,
    pub case_expr: Option<Box<Expression>>,
    pub when_clauses: Vec<CaseWhenClause>,
    pub else_clause: Option<CaseElseClause>,
    pub end_token: Keyword,
}

/// A binary operator.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct BinopExpression {
    pub left: Box<Expression>,
    pub op_token: Punct,
    pub right: Box<Expression>,
}

/// An `ARRAY` expression. This takes a bunch of different forms in BigQuery.
///
/// Not all combinations of our fields are valid. For example, we can't have
/// a missing `ARRAY` and a `delim1` of `(`. We'll let the parser handle that.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub struct ArrayExpression {
    pub array_token: Option<Keyword>,
    pub element_type: Option<ArrayElementType>,
    pub delim1: Punct,
    pub definition: ArrayDefinition,
    pub delim2: Punct,
}

impl Emit for ArrayExpression {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match self {
            // ARRAY(SELECT ..) needs a very heavy transformation for Trino.
            ArrayExpression {
                array_token,
                delim1,
                definition: ArrayDefinition::Query { select },
                delim2,
                ..
            } if t == Target::Trino => {
                // We can't do this with a transform and sql_quote because it
                // outputs Trino-specific closure syntax.
                let ArraySelectExpression {
                    select_options: SelectOptions { distinct, .. },
                    expression,
                    from_token: _,
                    unnest,
                    alias,
                    where_clause,
                } = select.as_ref();
                let first_token = array_token
                    .as_ref()
                    .map(|t| &t.ident.token)
                    .unwrap_or_else(|| &delim1.token);
                let last_token = &delim2.token;

                first_token.with_ws_only().emit(t, f)?;

                if distinct.is_some() {
                    f.write_token_start("ARRAY_DISTINCT(")?;
                }

                f.write_token_start("TRANSFORM(")?;

                if where_clause.is_some() {
                    f.write_token_start("FILTER(")?;
                }
                unnest.expression.emit(t, f)?;
                if let Some(where_clause) = where_clause {
                    f.write_token_start(", ")?;
                    alias.ident.emit(t, f)?;
                    f.write_token_start(" -> ")?;
                    where_clause.expression.emit(t, f)?;
                    f.write_token_start(")")?;
                }

                f.write_token_start(", ")?;
                alias.ident.emit(t, f)?;
                f.write_token_start(" -> ")?;
                expression.emit(t, f)?;
                f.write_token_start(")")?;

                if distinct.is_some() {
                    f.write_token_start(")")?;
                }

                last_token.with_ws_only().emit(t, f)?;
            }
            _ => match t {
                Target::Snowflake => {
                    self.delim1.token.with_str("[").emit(t, f)?;
                    self.definition.emit(t, f)?;
                    self.delim2.token.with_str("]").emit(t, f)?;
                }
                Target::SQLite3 => {
                    if let Some(array_token) = &self.array_token {
                        array_token.emit(t, f)?;
                    } else {
                        f.write_token_start("ARRAY")?;
                    }
                    self.delim1.token.with_str("(").emit(t, f)?;
                    self.definition.emit(t, f)?;
                    self.delim2.token.with_str(")").emit(t, f)?;
                }
                Target::Trino => {
                    if let Some(array_token) = &self.array_token {
                        array_token.emit(t, f)?;
                    } else {
                        f.write_token_start("ARRAY")?;
                    }
                    self.delim1.token.with_str("[").emit(t, f)?;
                    self.definition.emit(t, f)?;
                    self.delim2.token.with_str("]").emit(t, f)?;
                }
                _ => self.emit_default(t, f)?,
            },
        }
        Ok(())
    }
}

/// An `ARRAY` definition. Either a `SELECT` expression or a list of
/// expressions.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum ArrayDefinition {
    Query {
        /// The `SELECT` expression. BigQuery allows a [`QueryExpression`] here,
        /// but translating `ARRAY(SELECT ...)` to other databases involves some
        /// very complicated transformations, so we want to restrict things
        /// here.
        ///
        /// BigQuery effectively uses this as a MAP/FILTER/DISTINCT operation on
        /// the input array, and we have the best chance of translating it if we
        /// recognize that.
        select: Box<ArraySelectExpression>,
    },
    Elements(NodeVec<Expression>),
}

/// A very restricted version of [`SelectExpression`] that we allow in an
/// `ARRAY(SELECT ...)`, because we handle this as a special case to avoid
/// hitting correlated subquery restrictions in other databases.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct ArraySelectExpression {
    pub select_options: SelectOptions,
    pub expression: Expression,
    pub from_token: Keyword,
    pub unnest: UnnestExpression,
    pub alias: Alias,
    pub where_clause: Option<WhereClause>,
}

/// A struct expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct StructExpression {
    pub struct_token: Keyword,
    pub paren1: Punct,
    pub fields: SelectList,
    pub paren2: Punct,
}

/// The type of the elements in an `ARRAY` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct ArrayElementType {
    pub lt: Punct,
    pub elem_type: DataType,
    pub gt: Punct,
}

/// A `COUNT` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum CountExpression {
    CountStar {
        count_token: PseudoKeyword,
        paren1: Punct,
        star: Punct,
        paren2: Punct,
    },
    CountExpression {
        count_token: PseudoKeyword,
        paren1: Punct,
        distinct: Option<Distinct>,
        expression: Box<Expression>,
        paren2: Punct,
    },
}

/// A `CASE WHEN` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CaseWhenClause {
    pub when_token: Keyword,
    pub condition: Box<Expression>,
    pub then_token: Keyword,
    pub result: Box<Expression>,
}

/// A `CASE ELSE` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CaseElseClause {
    pub else_token: Keyword,
    pub result: Box<Expression>,
}

/// `CURRENT_DATE` may appear as either `CURRENT_DATE` or `CURRENT_DATE()`.
/// And different databases seem to support one or the other or both.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CurrentDate {
    pub current_date_token: PseudoKeyword,
    pub empty_parens: Option<EmptyParens>,
}

/// An empty `()` expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct EmptyParens {
    pub paren1: Punct,
    pub paren2: Punct,
}

/// Special "functions" that manipulate dates. These all take a [`DatePart`]
/// as a final argument. So in Lisp sense, these are special forms or macros,
/// not ordinary function calls.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct SpecialDateFunctionCall {
    pub function_name: PseudoKeyword,
    pub paren1: Punct,
    pub args: NodeVec<ExpressionOrDatePart>,
    pub paren2: Punct,
}

/// An expression or a date part.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum ExpressionOrDatePart {
    Expression(Expression),
    DatePart(DatePart),
}

/// An `ARRAY_AGG` expression.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub struct ArrayAggExpression {
    pub array_agg_token: PseudoKeyword,
    pub paren1: Punct,
    pub distinct: Option<Distinct>,
    pub expression: Box<Expression>,
    pub order_by: Option<OrderBy>,
    pub paren2: Punct,
}

impl Emit for ArrayAggExpression {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match self {
            // Snowflake formats ORDER BY as `ARRAY_AGG(expression) WITHIN GROUP
            // (ORDER BY ...)`.
            ArrayAggExpression {
                array_agg_token,
                paren1,
                distinct,
                expression,
                order_by: Some(order_by),
                paren2,
            } if t == Target::Snowflake => {
                array_agg_token.emit(t, f)?;
                paren1.emit(t, f)?;
                distinct.emit(t, f)?;
                expression.emit(t, f)?;
                paren2.emit(t, f)?;
                f.write_token_start("WITHIN GROUP(")?;
                order_by.emit(t, f)?;
                f.write_token_start(")")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A function call.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct FunctionCall {
    pub name: Name,
    pub paren1: Punct,
    pub args: NodeVec<Expression>,
    pub paren2: Punct,
    pub over_clause: Option<OverClause>,
}

/// An `OVER` clause for a window function.
///
/// See the [official grammar][]. We only implement part of this.
///
/// [official grammar]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#syntax
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct OverClause {
    pub over_token: Keyword,
    pub paren1: Punct,
    pub partition_by: Option<PartitionBy>,
    pub order_by: Option<OrderBy>,
    pub window_frame: Option<WindowFrame>,
    pub paren2: Punct,
}

/// A `PARTITION BY` clause for a window function.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct PartitionBy {
    pub partition_token: Keyword,
    pub by_token: Keyword,
    pub expressions: NodeVec<Expression>,
}

/// An `ORDER BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct OrderBy {
    pub order_token: Keyword,
    pub by_token: Keyword,
    pub items: NodeVec<OrderByItem>,
}

/// An item in an `ORDER BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct OrderByItem {
    pub expression: Expression,
    pub asc_desc: Option<AscDesc>,
}

/// An `ASC` or `DESC` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct AscDesc {
    direction: Keyword,
    nulls_clause: Option<NullsClause>,
}

/// A `NULLS FIRST` or `NULLS LAST` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct NullsClause {
    nulls_token: Keyword,
    first_last_token: PseudoKeyword,
}

/// A `LIMIT` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Limit {
    pub limit_token: Keyword,
    pub value: Literal,
}

/// A window frame clause.
///
/// See the [official grammar][]. We only implement part of this.
///
/// [official grammar]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct WindowFrame {
    pub rows_token: Keyword,
    pub definition: WindowFrameDefinition,
}

/// A window frame definition.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum WindowFrameDefinition {
    Start(WindowFrameStart),
    Between {
        between_token: Keyword,
        start: WindowFrameStart,
        and_token: Keyword,
        end: WindowFrameEnd,
    },
}

/// A window frame start. Keep this simple for now.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum WindowFrameStart {
    UnboundedPreceding {
        unbounded_token: Keyword,
        preceding_token: Keyword,
    },
    CurrentRow {
        current_token: Keyword,
        row_token: PseudoKeyword,
    },
}

/// A window frame end. Keep this simple for now.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum WindowFrameEnd {
    CurrentRow {
        current_token: Keyword,
        row_token: PseudoKeyword,
    },
    UnboundedFollowing {
        unbounded_token: Keyword,
        following_token: Keyword,
    },
}

/// Data types.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub enum DataType {
    Bool(PseudoKeyword),
    Bytes(PseudoKeyword),
    Date(PseudoKeyword),
    Datetime(PseudoKeyword),
    Float64(PseudoKeyword),
    Geography(PseudoKeyword), // WGS84
    Int64(PseudoKeyword),
    Numeric(PseudoKeyword),
    String(PseudoKeyword),
    Time(PseudoKeyword),
    Timestamp(PseudoKeyword),
    Array {
        array_token: Keyword,
        lt: Punct,
        data_type: Box<DataType>,
        gt: Punct,
    },
    Struct {
        struct_token: Keyword,
        lt: Punct,
        fields: NodeVec<StructField>,
        gt: Punct,
    },
}

impl Emit for DataType {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match t {
            Target::Snowflake => match self {
                DataType::Bool(token) => token.ident.token.with_str("BOOLEAN").emit(t, f),
                DataType::Bytes(token) => token.ident.token.with_str("BINARY").emit(t, f),
                DataType::Int64(token) => token.ident.token.with_str("INTEGER").emit(t, f),
                DataType::Date(token) => token.emit(t, f),
                // "Wall clock" time with no timezone.
                DataType::Datetime(token) => token.ident.token.with_str("TIMESTAMP_NTZ").emit(t, f),
                DataType::Float64(token) => token.ident.token.with_str("FLOAT8").emit(t, f),
                DataType::Geography(token) => token.emit(t, f),
                DataType::Numeric(token) => token.emit(t, f),
                DataType::String(token) => token.ident.token.with_str("TEXT").emit(t, f),
                DataType::Time(token) => token.emit(t, f),
                // `TIMESTAMP_TZ` will need very careful timezone handling.
                DataType::Timestamp(token) => token.ident.token.with_str("TIMESTAMP_TZ").emit(t, f),
                DataType::Array { array_token, .. } => {
                    array_token.ident.token.with_str("ARRAY").emit(t, f)
                }
                DataType::Struct { struct_token, .. } => {
                    struct_token.ident.token.with_str("OBJECT").emit(t, f)
                }
            },
            Target::SQLite3 => match self {
                DataType::Bool(token) | DataType::Int64(token) => {
                    token.ident.token.with_str("INTEGER").emit(t, f)
                }
                // NUMERIC is used when people want accurate math, so we want
                // either BLOB or TEXT, whatever makes math easier.
                DataType::Bytes(token) | DataType::Numeric(token) => {
                    token.ident.token.with_str("BLOB").emit(t, f)
                }
                DataType::Float64(token) => token.ident.token.with_str("REAL").emit(t, f),
                DataType::String(token)
                | DataType::Date(token)      // All date types should be strings
                | DataType::Datetime(token)
                | DataType::Geography(token) // Use GeoJSON
                | DataType::Time(token)
                | DataType::Timestamp(token) =>
                    token.ident.token.with_str("TEXT").emit(t, f),
                DataType::Array { array_token: token, .. } | DataType::Struct { struct_token: token, .. } => {
                    token.ident.token.with_str("/*JSON*/TEXT").emit(t, f)
                }
            },
            Target::Trino => match self {
                DataType::Bool(token) => token.ident.token.with_str("BOOLEAN").emit(t, f),
                DataType::Bytes(token) => token.ident.token.with_str("VARBINARY").emit(t, f),
                DataType::Date(token) => token.emit(t, f),
                DataType::Datetime(token) => token.ident.token.with_str("TIMESTAMP").emit(t, f),
                DataType::Float64(token) => token.ident.token.with_str("DOUBLE").emit(t, f),
                DataType::Geography(token) => token.ident.token.with_str("JSON").emit(t, f),
                DataType::Int64(token) => token.ident.token.with_str("BIGINT").emit(t, f),
                // BigQuery's precision is fixed, and we can't change it.
                DataType::Numeric(token) => token.ident.token.with_str("DECIMAL(38,9)").emit(t, f),
                DataType::String(token) => token.ident.token.with_str("VARCHAR").emit(t, f),
                DataType::Time(token) => token.emit(t, f),
                DataType::Timestamp(token) => token
                    .ident
                    .token
                    .with_str("TIMESTAMP WITH TIME ZONE")
                    .emit(t, f),
                DataType::Array {
                    array_token,
                    lt,
                    data_type,
                    gt,
                } => {
                    array_token.emit(t, f)?;
                    lt.token.with_str("(").emit(t, f)?;
                    data_type.emit(t, f)?;
                    gt.token.with_str(")").emit(t, f)
                }
                // TODO: I think we can translate the column types?
                DataType::Struct {
                    struct_token,
                    lt,
                    fields,
                    gt,
                } => {
                    struct_token.ident.token.with_str("ROW").emit(t, f)?;
                    lt.token.with_str("(").emit(t, f)?;
                    fields.emit(t, f)?;
                    gt.token.with_str(")").emit(t, f)
                }
            },
            _ => self.emit_default(t, f),
        }
    }
}

/// A field in a `STRUCT` type.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct StructField {
    pub name: Option<Ident>,
    pub data_type: DataType,
}

/// An array index expression.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct IndexExpression {
    pub expression: Box<Expression>,
    pub bracket1: Punct,
    pub index: IndexOffset,
    pub bracket2: Punct,
}

/// Different ways to index arrays.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum IndexOffset {
    Simple(Box<Expression>),
    Offset {
        offset_token: PseudoKeyword,
        paren1: Punct,
        expression: Box<Expression>,
        paren2: Punct,
    },
    Ordinal {
        ordinal_token: PseudoKeyword,
        paren1: Punct,
        expression: Box<Expression>,
        paren2: Punct,
    },
}

/// An `AS` alias.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Alias {
    pub as_token: Option<Keyword>,
    pub ident: Ident,
}

impl Alias {
    /// Get the [`Name`] of this alias.
    pub fn name(&self) -> Name {
        self.ident.clone().into()
    }
}

/// The `FROM` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct FromClause {
    pub from_token: Keyword,
    pub from_item: FromItem,
    pub join_operations: Vec<JoinOperation>,
}

/// Items which may appear in a `FROM` clause.
#[derive(Clone, Debug, Drive, DriveMut, EmitDefault, Spanned, ToTokens)]
pub struct FromItem {
    pub table_expression: FromTableExpression,
    pub alias: Option<Alias>,
}

impl Emit for FromItem {
    fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> io::Result<()> {
        match self {
            FromItem {
                table_expression: table_expression @ FromTableExpression::Unnest(..),
                alias: Some(Alias { as_token, ident }),
            } if t == Target::Trino => {
                table_expression.emit(t, f)?;
                as_token.emit(t, f)?;
                // UNNEST aliases aren't like other aliases, and Trino treats
                // them specially.
                f.write_token_start("U(")?;
                ident.emit(t, f)?;
                f.write_token_start(")")
            }
            _ => self.emit_default(t, f),
        }
    }
}

/// A table-valued expression which can occur in a `FROM` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum FromTableExpression {
    /// A table name.
    TableName(Name),
    /// A subquery. These parens belong here in the grammar; they're different
    /// from the ones in [`QueryExpression`].
    Subquery {
        paren1: Punct,
        query: Box<QueryStatement>,
        paren2: Punct,
    },
    /// A `UNNEST` clause.
    Unnest(UnnestExpression),
}

/// A `UNNEST` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct UnnestExpression {
    pub unnest_token: Keyword,
    pub paren1: Punct,
    pub expression: Box<Expression>,
    pub paren2: Punct,
}

/// A join operation.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum JoinOperation {
    /// A `JOIN` clause.
    ConditionJoin {
        join_type: JoinType,
        join_token: Keyword,
        from_item: FromItem,
        // The fact that this is optional is really dubious. But it appears in
        // some production queries.
        operator: Option<ConditionJoinOperator>,
    },
    /// A `CROSS JOIN` clause.
    CrossJoin {
        cross_token: Keyword,
        join_token: Keyword,
        from_item: FromItem,
    },
}

impl JoinOperation {
    /// The FROM item on the right-hand side of this join.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_item_mut(&mut self) -> &mut FromItem {
        match self {
            JoinOperation::ConditionJoin { from_item, .. } => from_item,
            JoinOperation::CrossJoin { from_item, .. } => from_item,
        }
    }

    /// Will all columns on the left side of this join become nullable?
    pub fn left_nullable(&self) -> bool {
        match self {
            JoinOperation::ConditionJoin { join_type, .. } => match join_type {
                JoinType::Inner { .. } => false,
                JoinType::Left { .. } => true,
                JoinType::Right { .. } => false,
                JoinType::Full { .. } => true,
            },
            JoinOperation::CrossJoin { .. } => false,
        }
    }

    /// Will all columns on the right side of this join become nullable?
    pub fn right_nullable(&self) -> bool {
        match self {
            JoinOperation::ConditionJoin { join_type, .. } => match join_type {
                JoinType::Inner { .. } => false,
                JoinType::Left { .. } => false,
                JoinType::Right { .. } => true,
                JoinType::Full { .. } => true,
            },
            JoinOperation::CrossJoin { .. } => false,
        }
    }
}

/// The type of a join.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum JoinType {
    Inner {
        inner_token: Option<Keyword>,
    },
    Left {
        left_token: Keyword,
        outer_token: Option<Keyword>,
    },
    Right {
        right_token: Keyword,
        outer_token: Option<Keyword>,
    },
    Full {
        full_token: Keyword,
        outer_token: Option<Keyword>,
    },
}

/// The condition used for a `JOIN`.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum ConditionJoinOperator {
    Using {
        using_token: Keyword,
        paren1: Punct,
        column_names: NodeVec<Ident>,
        paren2: Punct,
    },
    On {
        on_token: Keyword,
        expression: Expression,
    },
}

/// A `WHERE` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct WhereClause {
    pub where_token: Keyword,
    pub expression: Expression,
}

/// A `GROUP BY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct GroupBy {
    pub group_token: Keyword,
    pub by_token: Keyword,
    pub expressions: NodeVec<Expression>,
}

/// A `HAVING` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Having {
    pub having_token: Keyword,
    pub expression: Expression,
}

/// A `QUALIFY` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Qualify {
    pub qualify_token: Keyword,
    pub expression: Expression,
}

/// A `DELETE FROM` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct DeleteFromStatement {
    // DDL "keywords" are not actually treated as such by BigQuery.
    pub delete_token: PseudoKeyword,
    pub from_token: Keyword,
    pub table_name: Name,
    pub alias: Option<Alias>,
    pub where_clause: Option<WhereClause>,
}

/// A `INSERT INTO` statement. We only support the `SELECT` version.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct InsertIntoStatement {
    pub insert_token: PseudoKeyword,
    pub into_token: Keyword,
    pub table_name: Name,
    pub inserted_data: InsertedData,
}

/// The data to be inserted into a table.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum InsertedData {
    /// A `SELECT` statement.
    Select { query: QueryExpression },
    /// A `VALUES` clause.
    Values {
        values_token: PseudoKeyword,
        rows: NodeVec<ValuesRow>,
    },
}

/// A row in a `VALUES` clause.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct ValuesRow {
    pub paren1: Punct,
    pub expressions: NodeVec<Expression>,
    pub paren2: Punct,
}

/// A `CREATE TABLE` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CreateTableStatement {
    pub create_token: Keyword,
    pub or_replace: Option<OrReplace>,
    pub temporary: Option<Temporary>,
    pub table_token: PseudoKeyword,
    pub table_name: Name,
    pub definition: CreateTableDefinition,
}

/// A `CREATE VIEW` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct CreateViewStatement {
    pub create_token: Keyword,
    pub or_replace: Option<OrReplace>,
    pub view_token: PseudoKeyword,
    pub view_name: Name,
    pub as_token: Keyword,
    pub query: QueryStatement,
}

/// The `OR REPLACE` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct OrReplace {
    pub or_token: Keyword,
    pub replace_token: PseudoKeyword,
}

/// The `TEMPORARY` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct Temporary {
    pub temporary_token: PseudoKeyword,
}

/// The part of a `CREATE TABLE` statement that defines the columns.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub enum CreateTableDefinition {
    /// ( column_definition [, ...] )
    Columns {
        paren1: Punct,
        columns: NodeVec<ColumnDefinition>,
        paren2: Punct,
    },
    /// AS select_statement
    As {
        as_token: Keyword,
        query_statement: QueryStatement,
    },
}

/// A column definition.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct ColumnDefinition {
    pub name: Ident,
    pub data_type: DataType,
}

/// A `DROP TABLE` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct DropTableStatement {
    pub drop_token: PseudoKeyword,
    pub table_token: PseudoKeyword,
    pub if_exists: Option<IfExists>,
    pub table_name: Name,
}

/// A `DROP VIEW` statement.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct DropViewStatement {
    pub drop_token: PseudoKeyword,
    pub view_token: PseudoKeyword,
    pub if_exists: Option<IfExists>,
    pub view_name: Name,
}

/// An `IF EXISTS` modifier.
#[derive(Clone, Debug, Drive, DriveMut, Emit, EmitDefault, Spanned, ToTokens)]
pub struct IfExists {
    pub if_token: Keyword,
    pub exists_token: Keyword,
}

/// Parse BigQuery SQL.
pub fn parse_sql(files: &KnownFiles, file_id: FileId) -> Result<SqlProgram> {
    let token_stream = tokenize_sql(files, file_id)?;
    //println!("token_stream = {:?}", token_stream);

    // Parse with or without tracing, as appropriate. The tracing code throws
    // off error positions, so we don't want to use it unless we're going to
    // use `pegviz` to visualize the parse.
    //
    // #[cfg(feature = "trace")]
    // let result = sql_program::sql_program_traced(sql);
    // #[cfg(not(feature = "trace"))]

    let result = sql_program::sql_program(&token_stream);

    match result {
        Ok(sql_program) => Ok(sql_program),
        // Prepare a user-friendly error message.
        Err(e) => Err(Error::annotated(
            "Failed to parse query",
            e.location.clone(),
            format!("expected {}", e.expected),
        )),
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
    pub grammar sql_program() for TokenStream {
        // /// Alternate entry point for tracing the parse with `pegviz`.
        // pub rule sql_program_traced() -> SqlProgram = traced(<sql_program()>)

        /// Main entry point.
        pub rule sql_program() -> SqlProgram
            = statements:sep_opt_trailing(<statement()>, ";")
              { SqlProgram { statements } }

        pub rule statement() -> Statement
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
            = insert_token:pk("INSERT") into_token:k("INTO") table_name:name() inserted_data:inserted_data() {
                InsertIntoStatement {
                    insert_token,
                    into_token,
                    table_name,
                    inserted_data,
                }
            }

        rule inserted_data() -> InsertedData
            = query:query_expression() { InsertedData::Select { query } }
            / values_token:pk("VALUES") rows:sep_opt_trailing(<row()>, ",") {
                InsertedData::Values {
                    values_token,
                    rows,
                }
            }

        rule row() -> ValuesRow
            = paren1:p("(") expressions:sep_opt_trailing(<expression()>, ",") paren2:p(")") {
                ValuesRow {
                    paren1,
                    expressions,
                    paren2,
                }
            }

        rule delete_from_statement() -> DeleteFromStatement
            = delete_token:pk("DELETE") from_token:k("FROM") table_name:name() alias:alias()? where_clause:where_clause()? {
                DeleteFromStatement {
                    delete_token,
                    from_token,
                    table_name,
                    alias,
                    where_clause,
                }
            }

        pub rule query_expression() -> QueryExpression = precedence! {
            left:(@) set_operator:set_operator() right:@ {
                QueryExpression::SetOperation {
                    left: Box::new(left), set_operator, right: Box::new(right)
                }
            }
            --
            select_expression:select_expression() { QueryExpression::SelectExpression(select_expression) }
            paren1:p("(") query:query_statement() paren2:p(")") {
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
            = name:ident() as_token:k("AS") paren1:p("(") query:query_statement() paren2:p(")") {
                CommonTableExpression {
                    name,
                    as_token,
                    paren1,
                    query: Box::new(query),
                    paren2,
                }
            }

        pub rule select_expression() -> SelectExpression
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
                    ty: None,
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

        pub rule select_list_item() -> SelectListItem
            = star:p("*") except:except()? {
                SelectListItem::Wildcard { ty: None, star, except }
            }
            / table_name:name() dot:p(".") star:p("*") except:except()? {
                SelectListItem::TableNameWildcard { ty: None, table_name, dot, star, except }
            }
            / s:position!() expression:expression() alias:alias()? e:position!() {
                SelectListItem::Expression { expression, alias }
            }

        rule except() -> Except
            = except_token:k("EXCEPT") paren1:p("(") columns:sep_opt_trailing(<ident()>, ",") paren2:p(")") {
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
        pub rule expression() -> Expression = precedence! {
            left:(@) or_token:k("OR") right:@ { Expression::keyword_binop(left, or_token, right) }
            --
            left:(@) and_token:k("AND") right:@ { Expression::keyword_binop(left, and_token, right) }
            --
            expr:expression_no_and() { expr }
        }

        rule expression_no_and() -> Expression = precedence! {
            not_token:k("NOT") expression:@ { Expression::Not(NotExpression { not_token, expression: Box::new(expression) }) }
            --
            left:(@) is_token:k("IS") not_token:k("NOT")? predicate:is_expression_predicate() {
                Expression::Is(IsExpression { left: Box::new(left), is_token, not_token, predicate })
            }
            left:(@) not_token:k("NOT")? in_token:k("IN") value_set:in_value_set() {
                Expression::In(InExpression {
                    left: Box::new(left),
                    not_token,
                    in_token,
                    value_set,
                })
            }
            left:(@) not_token:k("NOT")? between_token:k("BETWEEN") middle:expression_no_and() and_token:k("AND") right:@ {
                Expression::Between(BetweenExpression {
                    left: Box::new(left),
                    not_token,
                    between_token,
                    middle: Box::new(middle),
                    and_token,
                    right: Box::new(right),
                })
            }
            left:(@) op_token:p("=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p("!=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p("<") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p("<=") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p(">") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p(">=") right:@ { Expression::binop(left, op_token, right) }
            --
            left:(@) op_token:p("+") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p("-") right:@ { Expression::binop(left, op_token, right) }
            --
            left:(@) op_token:p("*") right:@ { Expression::binop(left, op_token, right) }
            left:(@) op_token:p("/") right:@ { Expression::binop(left, op_token, right) }
            --
            arr:(@) bracket1:p("[") index:index_offset() bracket2:p("]") {
                Expression::Index(IndexExpression {
                    expression: Box::new(arr),
                    bracket1,
                    index,
                    bracket2,
                })
            }
            --
            case_token:k("CASE")
            case_expr:expression()?
            when_clauses:(case_when_clause()*)
            else_clause:case_else_clause()? end_token:k("END") {
                Expression::Case(CaseExpression {
                    case_token,
                    case_expr: case_expr.map(Box::new),
                    when_clauses,
                    else_clause,
                    end_token,
                })
            }
            if_token:k("IF") paren1:p("(") condition:expression() comma1:p(",") then_expression:expression() comma2:p(",") else_expression:expression() paren2:p(")") {
                Expression::If(IfExpression {
                    if_token,
                    paren1,
                    condition: Box::new(condition),
                    comma1,
                    then_expression: Box::new(then_expression),
                    comma2,
                    else_expression: Box::new(else_expression),
                    paren2,
                })
            }
            array_expression:array_expression() { Expression::Array(array_expression) }
            struct_expression:struct_expression() { Expression::Struct(struct_expression) }
            count_expression:count_expression() { Expression::Count(count_expression) }
            current_date:current_date() { Expression::CurrentDate(current_date) }
            paren1:p("(") query:query_expression() paren2:p(")") { Expression::Query { paren1, query: Box::new(query), paren2 } }
            paren1:p("(") expression:expression() paren2:p(")") { Expression::Parens { paren1, expression: Box::new(expression), paren2 } }
            literal:literal() { Expression::Literal(literal) }
            bool_token:(k("TRUE") / k("FALSE")) { Expression::BoolValue(bool_token) }
            null_token:k("NULL") { Expression::Null(null_token) }
            interval_expression:interval_expression() { Expression::Interval(interval_expression) }
            cast:cast() { Expression::Cast(cast) }
            array_agg:array_agg() { Expression::ArrayAgg(array_agg) }
            special_date_function_call:special_date_function_call() { Expression::SpecialDateFunctionCall(special_date_function_call) }
            // Things from here down might start with arbitrary identifiers, so
            // we need to be careful about the order.
            function_call:function_call() { Expression::FunctionCall(function_call) }
            column_name:name() { Expression::ColumnName(column_name) }
        }

        rule interval_expression() -> IntervalExpression
            = interval_token:k("INTERVAL") number:literal() date_part:date_part() {
                IntervalExpression {
                    interval_token,
                    number,
                    date_part,
                }
            }

        rule date_part() -> DatePart
            = date_part_token:(pk("YEAR") / pk("QUARTER") / pk("MONTH") / pk("WEEK") / pk("DAY") / pk("HOUR") / pk("MINUTE") / pk("SECOND")) {
                DatePart { date_part_token }
            }

        rule is_expression_predicate() -> IsExpressionPredicate
            = null_token:k("NULL") { IsExpressionPredicate::Null(null_token) }
            / true_token:k("TRUE") { IsExpressionPredicate::True(true_token) }
            / false_token:k("FALSE") { IsExpressionPredicate::False(false_token) }
            / unknown_token:pk("UNKNOWN") { IsExpressionPredicate::Unknown(unknown_token) }

        rule in_value_set() -> InValueSet
            = paren1:p("(") query_expression:query_expression() paren2:p(")") {
                InValueSet::QueryExpression {
                    paren1,
                    query: Box::new(query_expression),
                    paren2,
                }
            }
            / paren1:p("(") expressions:sep(<expression()>, ",") paren2:p(")") {
                InValueSet::ExpressionList {
                    paren1,
                    expressions,
                    paren2,
                }
            }
            / unnest_token:k("UNNEST") paren1:p("(") expression:expression() paren2:p(")") {
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
            = offset_token:pk("OFFSET") paren1:p("(") expression:expression() paren2:p(")") {
                IndexOffset::Offset {
                    offset_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                }
            }
            / ordinal_token:pk("ORDINAL") paren1:p("(") expression:expression() paren2:p(")") {
                IndexOffset::Ordinal {
                    ordinal_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
                }
            }
            / expression:expression() { IndexOffset::Simple(Box::new(expression)) }

        rule array_expression() -> ArrayExpression
            = delim1:p("[") definition:array_definition() delim2:p("]") {
                ArrayExpression {
                    array_token: None,
                    element_type: None,
                    delim1,
                    definition,
                    delim2,
                }
            }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:p("[") definition:array_definition() delim2:p("]") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    definition,
                    delim2,
                }
              }
            / array_token:k("ARRAY") element_type:array_element_type()?
              delim1:p("(") definition:array_definition() delim2:p(")") {
                ArrayExpression {
                    array_token: Some(array_token),
                    element_type,
                    delim1,
                    definition,
                    delim2,
                }
              }

        rule array_definition() -> ArrayDefinition
            = select:array_select_expression() { ArrayDefinition::Query { select: Box::new(select) } }
            / expressions:sep(<expression()>, ",") { ArrayDefinition::Elements(expressions) }
            / { ArrayDefinition::Elements(NodeVec::new(",")) }

        rule array_select_expression() -> ArraySelectExpression
            = select_options:select_options()
              expression:expression()
              comma:p(",")?
              from_token:k("FROM")
              unnest:unnest_expression()
              alias:alias()
              where_clause:where_clause()?
            {
                ArraySelectExpression {
                    select_options,
                    expression,
                    from_token,
                    unnest,
                    alias,
                    where_clause,
                }
            }

        rule struct_expression() -> StructExpression
            = struct_token:k("STRUCT") paren1:p("(") fields:select_list() paren2:p(")") {
                StructExpression {
                    struct_token,
                    paren1,
                    fields,
                    paren2,
                }
            }

        rule array_element_type() -> ArrayElementType
            = lt:p("<") elem_type:data_type() gt:p(">") {
                ArrayElementType { lt, elem_type, gt }
            }

        rule count_expression() -> CountExpression
            = count_token:pk("COUNT") paren1:p("(") star:p("*") paren2:p(")") {
                CountExpression::CountStar {
                    count_token,
                    paren1,
                    star,
                    paren2,
                }
            }
            / count_token:pk("COUNT") paren1:p("(") distinct:distinct()? expression:expression() paren2:p(")") {
                CountExpression::CountExpression {
                    count_token,
                    paren1,
                    distinct,
                    expression: Box::new(expression),
                    paren2,
                }
            }

        rule current_date() -> CurrentDate
            = current_date_token:pk("CURRENT_DATE") empty_parens:empty_parens()? {
                CurrentDate {
                    current_date_token,
                    empty_parens,
                }
            }

        rule empty_parens() -> EmptyParens
            = paren1:p("(") paren2:p(")") {
                EmptyParens { paren1, paren2 }
            }

        rule array_agg() -> ArrayAggExpression
            = array_agg_token:pk("ARRAY_AGG") paren1:p("(") distinct:distinct()?
              expression:expression()
              order_by:order_by()?
              paren2:p(")")
            {
                ArrayAggExpression {
                    array_agg_token,
                    paren1,
                    distinct,
                    expression: Box::new(expression),
                    order_by,
                    paren2,
                }
            }

        rule special_date_function_call() -> SpecialDateFunctionCall
            = function_name:special_date_function_name() paren1:p("(")
              args:sep(<expression_or_date_part()>, ",") paren2:p(")") {
                SpecialDateFunctionCall {
                    function_name,
                    paren1,
                    args,
                    paren2,
                }
            }

        rule special_date_function_name() -> PseudoKeyword
            = pk("DATE_DIFF") / pk("DATE_TRUNC") / pk("DATETIME_DIFF") / pk("DATETIME_TRUNC")

        rule expression_or_date_part() -> ExpressionOrDatePart
            = date_part:date_part() { ExpressionOrDatePart::DatePart(date_part) }
            / expression:expression() { ExpressionOrDatePart::Expression(expression) }

        pub rule function_call() -> FunctionCall
            = name:name() paren1:p("(")
              args:sep_opt_trailing(<expression()>, ",")? paren2:p(")")
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

        rule over_clause() -> OverClause
            = over_token:k("OVER") paren1:p("(")
            partition_by:partition_by()?
            order_by:order_by()?
            window_frame:window_frame()?
            paren2:p(")")
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
            = nulls_token:k("NULLS") first_last_token:(pk("FIRST") / pk("LAST")) {
                NullsClause { nulls_token, first_last_token }
            }

        rule limit() -> Limit
            = limit_token:k("LIMIT") value:literal() {
                Limit {
                    limit_token,
                    value,
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
            / current_token:k("CURRENT") row_token:pk("ROW") {
                WindowFrameStart::CurrentRow {
                    current_token,
                    row_token,
                }
            }

        rule window_frame_end() -> WindowFrameEnd
            = current_token:k("CURRENT") row_token:pk("ROW") {
                WindowFrameEnd::CurrentRow {
                    current_token,
                    row_token,
                }
            }
            / unbounded_token:k("UNBOUNDED") following_token:k("FOLLOWING") {
                WindowFrameEnd::UnboundedFollowing {
                    unbounded_token,
                    following_token,
                }
            }

        rule cast() -> Cast
            = cast_type:cast_type() paren1:p("(") expression:expression() as_token:k("AS") data_type:data_type() paren2:p(")") {
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
            = safe_cast_token:pk("SAFE_CAST") { CastType::SafeCast { safe_cast_token } }
            / cast_token:k("CAST") { CastType::Cast { cast_token } }

        rule data_type() -> DataType
            = token:(pk("BOOLEAN") / pk("BOOL")) { DataType::Bool(token) }
            / token:pk("BYTES") { DataType::Bytes(token) }
            / token:pk("DATETIME") { DataType::Datetime(token) }
            / token:pk("DATE") { DataType::Date(token) }
            / token:pk("FLOAT64") { DataType::Float64(token) }
            / token:pk("GEOGRAPHY") { DataType::Geography(token) }
            / token:pk("INT64") { DataType::Int64(token) }
            / token:pk("NUMERIC") { DataType::Numeric(token) }
            / token:pk("STRING") { DataType::String(token) }
            / token:pk("TIMESTAMP") { DataType::Timestamp(token) }
            / token:pk("TIME") { DataType::Time(token) }
            / array_token:k("ARRAY") lt:p("<") data_type:data_type() gt:p(">") {
                DataType::Array {
                    array_token,
                    lt,
                    data_type: Box::new(data_type),
                    gt,
                }
            }
            / struct_token:k("STRUCT") lt:p("<") fields:sep_opt_trailing(<struct_field()>, ",") gt:p(">") {
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
            = table_expression:from_table_expression() alias:alias()? {
                FromItem {
                    table_expression,
                    alias,
                }
            }

        rule from_table_expression() -> FromTableExpression
            = table_name:name() { FromTableExpression::TableName(table_name) }
            / paren1:p("(") query:query_statement() paren2:p(")") {
                FromTableExpression::Subquery {
                    paren1,
                    query: Box::new(query),
                    paren2,
                }
            }
            / unnest:unnest_expression() { FromTableExpression::Unnest(unnest) }

        rule unnest_expression() -> UnnestExpression
            = unnest_token:k("UNNEST") paren1:p("(") expression:expression() paren2:p(")") {
                UnnestExpression {
                    unnest_token,
                    paren1,
                    expression: Box::new(expression),
                    paren2,
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
            = using_token:k("USING") paren1:p("(") column_names:sep(<ident()>, ",") paren2:p(")") {
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
              table_token:pk("TABLE") table_name:name()
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
              view_token:pk("VIEW") view_name:name()
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
            = or_token:k("OR") replace_token:pk("REPLACE") {
                OrReplace { or_token, replace_token }
            }

        rule temporary() -> Temporary
            = temporary_token:(pk("TEMPORARY") / pk("TEMP")) {
                Temporary { temporary_token }
            }

        rule create_table_definition() -> CreateTableDefinition
            = paren1:p("(") columns:sep_opt_trailing(<column_definition()>, ",") paren2:p(")") {
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
            = drop_token:pk("DROP") table_token:pk("TABLE") if_exists:if_exists()? table_name:name() {
                DropTableStatement {
                    drop_token,
                    table_token,
                    if_exists,
                    table_name,
                }
            }

        rule drop_view_statement() -> DropViewStatement
            = drop_token:pk("DROP") view_token:pk("VIEW") if_exists:if_exists()? view_name:name() {
                DropViewStatement {
                    drop_token,
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


        /// A table or column name with internal dots.
        rule name() -> Name = items:sep(<ident()>, ".") {
            items.into()
        }

        /// Punctuation separated list. Does not allow a trailing separator.
        rule sep<T: Node>(node: rule<T>, separator: &'static str) -> NodeVec<T>
            = first:node() rest:(sep:p(separator) node:node() { (sep, node) })* {
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
            = list:sep(item, separator) trailing_sep:p(separator)? {
                let mut list = list;
                if let Some(trailing_sep) = trailing_sep {
                    list.items.push(NodeOrSep::Sep(trailing_sep));
                }
                list
            }

        // Broken by switch to TokenStream.
        //
        // /// Tracing rule for `pegviz`. See
        // /// https://github.com/fasterthanlime/pegviz.
        // rule traced<T>(e: rule<T>) -> T =
        //     &(input:$([_]*) {
        //         #[cfg(feature = "trace")]
        //         println!("[PEG_INPUT_START]\n{}\n[PEG_TRACE_START]", input);
        //     })
        //     e:e()? {?
        //         #[cfg(feature = "trace")]
        //         println!("[PEG_TRACE_STOP]");
        //         e.ok_or("")
        //     }

        //================================================================
        // Parser integration
        //
        // The following grammar rules use the undocumented `##` syntax in `peg`
        // to hook into `TokenStream`.

        // Match a literal value.
        rule literal() -> Literal
            = literal:##literal()
            / expected!("literal")

        // Match an identifier.
        rule ident() -> Ident
            = ident:##ident() {?
                let upper = ident.token.as_str().to_ascii_uppercase();
                if KEYWORDS.contains(&upper) {
                    Err("identifier")
                } else {
                    Ok(ident)
                }
            }

        // Match a pseudo-.
        rule pk(s: &'static str) -> PseudoKeyword
            = ident:##pseudo_keyword(s) {?
                let upper = ident.ident.token.as_str().to_ascii_uppercase();
                if KEYWORDS.contains(&upper) {
                    Err("identifier")
                } else {
                    Ok(ident)
                }
            }

        // Get the next keyword from the stream.
        rule k(s: &'static str) -> Keyword
            = keyword:##keyword(s) {?
                let upper = keyword.ident.token.as_str().to_ascii_uppercase();
                if KEYWORDS.contains(&upper) {
                    Ok(keyword)
                } else {
                    Err(s)
                }
            }

        // Get the next punctuation from the stream.
        rule p(s: &'static str) -> Punct
            = token:##punct_eq(s) { token }
            / expected!(s)

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parser_and_run_with_sqlite3() {
        let sql_examples = &[
            // Basic test cases of gradually increasing complexity.
            (r#"SELECT * FROM t"#, None),
            (r#"SELECT * FROM t -- comment"#, None),
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
            (r"SELECT * FROM t WHERE a < 0.5", None),
            (r"SELECT * FROM t WHERE a BETWEEN 1 AND 10", None),
            (r"SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10", None),
            (r"SELECT INTERVAL -3 DAY", None),
            (r"SELECT * FROM t WHERE a IN (1,2)", None),
            (r"SELECT * FROM t WHERE a NOT IN (1,2)", None),
            (r"SELECT * FROM t WHERE a IN (SELECT b FROM t)", None),
            (r"SELECT * FROM t WHERE a IN UNNEST([1])", None),
            (r"SELECT IF(a = 0, 1, 2) c FROM t", None),
            (r"SELECT CASE WHEN a = 0 THEN 1 ELSE 2 END c FROM t", None),
            (r"SELECT CASE WHEN a = 0 THEN 1 END c FROM t", None),
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
            (r"SELECT DATETIME(2008, 12, 25, 5, 30, 0)", None),
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
            (
                r#"SELECT ARRAY(SELECT DISTINCT i*2 FROM UNNEST([1,2]) AS i WHERE MOD(i, 2) = 0)"#,
                None,
            ),
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
        CAST(s1.random_id AS STRING) AS key,
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

        // Make sure we can parse and re-emit all the examples.
        let mut files = KnownFiles::new();
        for &(sql, normalized) in sql_examples {
            println!("parsing:   {}", sql);
            let file_id = files.add_string("test.sql", sql);
            let normalized = normalized.unwrap_or(sql);
            let parsed = match parse_sql(&files, file_id) {
                Ok(parsed) => parsed,
                Err(err) => {
                    err.emit(&files);
                    panic!("{}", err);
                }
            };
            assert_eq!(normalized, &parsed.emit_to_string(Target::BigQuery));
        }
    }
}
