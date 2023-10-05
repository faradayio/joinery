// Don't bother with `Box`-ing everything for now. Allow huge enum values.
#![allow(clippy::large_enum_variant)]

use std::{fmt, ops::Range};

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
};

/// We represent a span in our source code using a Rust range. These are easy
/// to construct from within our parser.
type Span = Range<usize>;

/// The target language we're transpiling to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum Target {
    BigQuery,
    SQLite3,
}

impl Target {
    /// Format this node for the given target.
    fn f<T: Node>(self, node: &T) -> FormatForTarget<'_, T> {
        FormatForTarget { target: self, node }
    }
}

/// Wrapper for formatting a node for a given target.
struct FormatForTarget<'a, T: Node> {
    target: Target,
    node: &'a T,
}

impl<'a, T: Node> fmt::Display for FormatForTarget<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.fmt_for_target(self.target, f)
    }
}

/// A node in our abstract syntax tree.
pub trait Node: fmt::Debug + Sized {
    // The source span corresponding to this node.
    // fn span(&self) -> Span;

    /// Format this node as its SQLite3 equivalent. This would need to be replaced
    /// with something a bit more compiler-like to support full transpilation.
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    /// Convert this node to a string for the given target.
    fn to_string_for_target(&self, t: Target) -> String {
        format!("{}", t.f(self))
    }
}

/// Because we're implementing a transpiler, we want to be able to keep track of
/// the original whitespace and comments used in the source code. We'll use this
/// to generate output that looks as close to the original source as possible.
///
/// Normally, each AST node is responsible for storing any leading or interior
/// whitespace. This is because whitespace contains comments, and comments tend to
/// mostly come before nodes. Trailing whitespace for the final token
/// is stored in the [`SqlProgram`] struct.
#[derive(Debug)]
pub struct Whitespace {
    pub span: Span,
    pub text: String,
}

impl Node for Whitespace {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => write!(f, "{}", self.text),
            Target::SQLite3 => {
                // We need to fix `#` comments, which are not supported by SQLite3.
                let mut in_hash_comment = false;
                for c in self.text.chars() {
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
    pub trailing_ws: Vec<Whitespace>,
}

impl<T: Node> NodeVec<T> {
    pub fn display_with_sep(
        &self,
        t: Target,
        f: &mut fmt::Formatter<'_>,
        sep: &str,
    ) -> fmt::Result {
        for (i, node) in self.nodes.iter().enumerate() {
            write!(f, "{}", t.f(node))?;
            if i < self.trailing_ws.len() {
                write!(f, "{}", t.f(&self.trailing_ws[i]))?;
                match t {
                    _ if i + 1 < self.nodes.len() => write!(f, "{}", sep)?,
                    Target::BigQuery => write!(f, "{}", sep)?,
                    Target::SQLite3 => write!(f, " ")?,
                }
            }
        }
        Ok(())
    }
}

/// An identifier, such as a column name.
#[derive(Debug)]
pub struct Identifier {
    pub ws: Whitespace,
    pub span: Span,
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

    // Does this need to be quoted?
    fn needs_quotes(&self) -> bool {
        self.is_keyword() || !self.is_c_ident()
    }
}

impl Node for Identifier {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.needs_quotes() {
            match t {
                Target::BigQuery => {
                    write!(f, "{}`", t.f(&self.ws))?;
                    escape_for_bigquery(&self.text, f)?;
                    write!(f, "`")
                }
                Target::SQLite3 => {
                    write!(f, "{}\"", t.f(&self.ws))?;
                    escape_for_sqlite3(&self.text, f)?;
                    write!(f, "\"")
                }
            }
        } else {
            write!(f, "{}{}", t.f(&self.ws), self.text)
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
        ws: Whitespace,
        dataset: Identifier,
        ws2: Whitespace,
        table: Identifier,
    },
    DatasetTable {
        dataset: Identifier,
        ws: Whitespace,
        table: Identifier,
    },
    Table {
        table: Identifier,
    },
}

impl Node for TableName {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => match self {
                TableName::ProjectDatasetTable {
                    project,
                    ws,
                    dataset,
                    ws2,
                    table,
                } => {
                    write!(
                        f,
                        "{}{}.{}{}.{}",
                        t.f(project),
                        t.f(ws),
                        t.f(dataset),
                        t.f(ws2),
                        t.f(table)
                    )
                }
                TableName::DatasetTable { dataset, ws, table } => {
                    write!(f, "{}{}.{}", t.f(dataset), t.f(ws), t.f(table))
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
                    write!(f, "{}\"", t.f(&project.ws))?;
                    escape_for_sqlite3(&project.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&dataset.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&table.text, f)?;
                    write!(f, "\"")
                }
                TableName::DatasetTable { dataset, table, .. } => {
                    write!(f, "{}\"", t.f(&dataset.ws))?;
                    escape_for_sqlite3(&dataset.text, f)?;
                    write!(f, ".")?;
                    escape_for_sqlite3(&table.text, f)?;
                    write!(f, "\"")
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
    pub ws: Whitespace,
    pub column_name: Identifier,
}

impl Node for TableAndColumnName {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}.{}",
            t.f(&self.table_name),
            t.f(&self.ws),
            t.f(&self.column_name)
        )
    }
}

/// An entire SQL program.
#[derive(Debug)]
pub struct SqlProgram {
    /// For now, just handle single statements; BigQuery DDL is messy and maybe
    /// out of scope.
    pub statement: Statement,
    /// Any trailing whitespace after the final token in the program.
    pub trailing_ws: Whitespace,
}

impl Node for SqlProgram {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.statement), t.f(&self.trailing_ws))
    }
}

/// A statement in our abstract syntax tree.
#[derive(Debug)]
pub enum Statement {
    Query(QueryStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
}

impl Node for Statement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Query(s) => write!(f, "{}", t.f(s)),
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

impl Node for QueryStatement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        paren1_ws: Whitespace,
        query: Box<QueryStatement>,
        paren2_ws: Whitespace,
    },
}

impl Node for QueryExpression {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryExpression::SelectExpression(s) => write!(f, "{}", t.f(s)),
            QueryExpression::Nested {
                paren1_ws,
                query,
                paren2_ws,
            } if t == Target::SQLite3 => {
                // Throw in extra spaces for the erased parens.
                write!(
                    f,
                    "{} {}{} ",
                    t.f(paren1_ws),
                    t.f(query.as_ref()),
                    t.f(paren2_ws)
                )
            }
            QueryExpression::Nested {
                paren1_ws,
                query,
                paren2_ws,
            } => write!(
                f,
                "{}({}{})",
                t.f(paren1_ws),
                t.f(query.as_ref()),
                t.f(paren2_ws)
            ),
        }
    }
}

/// A `SELECT` expression.
#[derive(Debug)]
pub struct SelectExpression {
    pub select_options: SelectOptions,
    pub select_list: SelectList,
    pub from_clause: FromClause,
    pub where_clause: Option<WhereClause>,
    // pub group_by: Option<GroupBy>,
    // pub having: Option<Having>,
    // pub order_by: Option<OrderBy>,
    // pub limit: Option<Limit>,
}

impl Node for SelectExpression {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.select_options),
            t.f(&self.select_list),
            t.f(&self.from_clause)
        )?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, "{}", t.f(where_clause))?;
        }
        Ok(())
    }
}

/// The head of a `SELECT`, including any modifiers.
#[derive(Debug)]
pub struct SelectOptions {
    pub ws: Whitespace,
    pub span: Span,
    pub distinct: Option<Distinct>,
}

impl Node for SelectOptions {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}SELECT", t.f(&self.ws))?;
        if let Some(distinct) = &self.distinct {
            write!(f, "{}", t.f(distinct))?;
        }
        Ok(())
    }
}

/// The `DISTINCT` modifier.
#[derive(Debug)]
pub struct Distinct {
    pub ws: Whitespace,
    pub span: Span,
}

impl Node for Distinct {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}DISTINCT", t.f(&self.ws))
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

impl Node for SelectList {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.items.display_with_sep(t, f, ",")
    }
}

/// A single item in a `SELECT` list.
#[derive(Debug)]
pub enum SelectListItem {
    /// An expression, optionally with an alias.
    Expression {
        span: Span,
        expression: Expression,
        alias: Option<Alias>,
    },
    /// A `*` wildcard.
    Wildcard { span: Span, ws: Whitespace },
    /// A `table.*` wildcard.
    TableNameWildcard {
        span: Span,
        table_name: TableName,
        dot_ws: Whitespace,
        star_ws: Whitespace,
    },
}

impl Node for SelectListItem {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectListItem::Expression {
                expression, alias, ..
            } => {
                write!(f, "{}", t.f(expression))?;
                if let Some(alias) = alias {
                    write!(f, "{}", t.f(alias))?;
                }
                Ok(())
            }
            SelectListItem::Wildcard { ws, .. } => write!(f, "{}*", &t.f(ws)),
            SelectListItem::TableNameWildcard {
                table_name,
                dot_ws,
                star_ws,
                ..
            } => {
                write!(f, "{}{}.{}*", t.f(table_name), t.f(dot_ws), t.f(star_ws))
            }
        }
    }
}

/// An SQL expression.
#[derive(Debug)]
pub enum Expression {
    Literal {
        ws: Whitespace,
        span: Span,
        literal: Literal,
    },
    Null {
        ws: Whitespace,
        span: Span,
    },
    ColumnName(Identifier),
    TableAndColumnName(TableAndColumnName),
    Cast {
        ws: Whitespace,
        span: Span,
        paren_ws: Whitespace,
        expression: Box<Expression>,
        as_ws: Whitespace,
        data_type: DataType,
        paren2_ws: Whitespace,
    },
    Binop {
        left: Box<Expression>,
        op_ws: Whitespace,
        op: Binop,
        right: Box<Expression>,
    },
}

impl Node for Expression {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal { ws, literal, .. } => write!(f, "{}{}", t.f(ws), t.f(literal)),
            Expression::Null { ws, .. } => write!(f, "{}NULL", t.f(ws)),
            Expression::ColumnName(ident) => write!(f, "{}", t.f(ident)),
            Expression::TableAndColumnName(table_and_column_name) => {
                write!(f, "{}", t.f(table_and_column_name))
            }
            Expression::Cast {
                ws,
                paren_ws,
                expression,
                as_ws,
                data_type,
                paren2_ws,
                ..
            } => {
                write!(
                    f,
                    "{}CAST{}({}{}AS{}{})",
                    t.f(ws),
                    t.f(paren_ws),
                    t.f(expression.as_ref()),
                    t.f(as_ws),
                    t.f(data_type),
                    t.f(paren2_ws)
                )
            }
            Expression::Binop {
                left,
                op_ws,
                op,
                right,
            } => write!(
                f,
                "{}{}{}{}",
                t.f(left.as_ref()),
                t.f(op_ws),
                t.f(op),
                t.f(right.as_ref())
            ),
        }
    }
}

/// A literal value.
#[derive(Debug)]
pub enum Literal {
    Int64(i64),
}

impl Node for Literal {
    fn fmt_for_target(&self, _t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Int64(i) => write!(f, "{}", i),
        }
    }
}

/// A binary operator.
#[derive(Debug)]
pub enum Binop {
    Gte,
}

impl Node for Binop {
    fn fmt_for_target(&self, _t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Binop::Gte => write!(f, ">="),
        }
    }
}

/// Data types.
#[derive(Debug)]
pub enum DataType {
    Bool {
        ws: Whitespace,
        span: Span,
    },
    Int64 {
        ws: Whitespace,
        span: Span,
    },
    String {
        ws: Whitespace,
        span: Span,
    },
    Array {
        ws: Whitespace,
        span: Span,
        lt_ws: Whitespace,
        data_type: Box<DataType>,
        gt_ws: Whitespace,
    },
}

impl Node for DataType {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => match self {
                DataType::Bool { ws, .. } => write!(f, "{}BOOL", t.f(ws)),
                DataType::Int64 { ws, .. } => write!(f, "{}INT64", t.f(ws)),
                DataType::String { ws, .. } => write!(f, "{}STRING", t.f(ws)),
                DataType::Array {
                    ws,
                    lt_ws,
                    data_type,
                    gt_ws,
                    ..
                } => write!(
                    f,
                    "{}ARRAY{}<{}{}>",
                    t.f(ws),
                    t.f(lt_ws),
                    t.f(data_type.as_ref()),
                    t.f(gt_ws)
                ),
            },
            Target::SQLite3 => match self {
                DataType::Bool { ws, .. } | DataType::Int64 { ws, .. } => {
                    write!(f, "{}INTEGER", t.f(ws))
                }
                DataType::String { ws, .. } => write!(f, "{}TEXT", t.f(ws)),
                DataType::Array { ws, .. } => write!(f, "{}/*JSON*/TEXT", t.f(ws)),
            },
        }
    }
}

/// An `AS` alias.
#[derive(Debug)]
pub struct Alias {
    pub ws: Whitespace,
    pub span: Span,
    pub ident: Identifier,
}

impl Node for Alias {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}AS{}", t.f(&self.ws), t.f(&self.ident))
    }
}

/// The `FROM` clause.
///
/// TODO: We're keeping this simple for now.
#[derive(Debug)]
pub struct FromClause {
    pub ws: Whitespace,
    pub span: Span,
    pub from_item: FromItem,
    pub join_operations: Vec<JoinOperation>,
}

impl Node for FromClause {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}FROM{}", t.f(&self.ws), t.f(&self.from_item))?;
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
    /// A subquery, optionally with an alias.
    Subquery {
        paren1_ws: Whitespace,
        query: Box<QueryStatement>,
        paren2_ws: Whitespace,
        alias: Option<Alias>,
    },
}

impl Node for FromItem {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FromItem::TableName { table_name, alias } => {
                write!(f, "{}", t.f(table_name))?;
                if let Some(alias) = alias {
                    write!(f, "{}", t.f(alias))?;
                }
                Ok(())
            }
            FromItem::Subquery {
                paren1_ws,
                query,
                paren2_ws,
                alias,
            } => {
                write!(
                    f,
                    "{}({}{})",
                    t.f(paren1_ws),
                    t.f(query.as_ref()),
                    t.f(paren2_ws)
                )?;
                if let Some(alias) = alias {
                    write!(f, "{}", t.f(alias))?;
                }
                Ok(())
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
        join_ws: Whitespace,
        from_item: FromItem,
        operator: ConditionJoinOperator,
    },
}

impl Node for JoinOperation {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinOperation::ConditionJoin {
                join_type,
                join_ws,
                from_item,
                operator,
            } => {
                write!(
                    f,
                    "{}{}JOIN{}{}",
                    t.f(join_type),
                    t.f(join_ws),
                    t.f(from_item),
                    t.f(operator)
                )
            }
        }
    }
}

/// The type of a join.
#[derive(Debug)]
pub enum JoinType {
    Inner { ws: Whitespace, span: Span },
    Left { ws: Whitespace, span: Span },
    Right { ws: Whitespace, span: Span },
    Full { ws: Whitespace, span: Span },
}

impl Node for JoinType {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner { ws, .. } => write!(f, "{}INNER", t.f(ws)),
            JoinType::Left { ws, .. } => write!(f, "{}LEFT", t.f(ws)),
            JoinType::Right { ws, .. } => write!(f, "{}RIGHT", t.f(ws)),
            JoinType::Full { ws, .. } => write!(f, "{}FULL", t.f(ws)),
        }
    }
}

/// The condition used for a `JOIN`.
#[derive(Debug)]
pub enum ConditionJoinOperator {
    Using {
        ws: Whitespace,
        span: Span,
        paren1_ws: Whitespace,
        column_names: NodeVec<Identifier>,
        paren2_ws: Whitespace,
    },
}

impl Node for ConditionJoinOperator {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConditionJoinOperator::Using {
                ws,
                paren1_ws,
                column_names,
                paren2_ws,
                ..
            } => {
                write!(f, "{}USING{}(", t.f(ws), t.f(paren1_ws))?;
                column_names.display_with_sep(t, f, ",")?;
                write!(f, "{})", t.f(paren2_ws))
            }
        }
    }
}

/// A `WHERE` clause.
#[derive(Debug)]
pub struct WhereClause {
    pub ws: Whitespace,
    pub span: Span,
    pub expression: Expression,
}

impl Node for WhereClause {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}WHERE{}", t.f(&self.ws), t.f(&self.expression))
    }
}

/// A `CREATE TABLE` statement.
#[derive(Debug)]
pub struct CreateTableStatement {
    pub create_ws: Whitespace,
    pub span: Span,
    pub or_replace: Option<OrReplace>,
    pub table_ws: Whitespace,
    pub table_name: TableName,
    pub definition: CreateTableDefinition,
}

impl Node for CreateTableStatement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.create_ws))?;
        match t {
            Target::SQLite3 if self.or_replace.is_some() => {
                // We need to convert this to a `DROP TABLE IF EXISTS` statement.
                write!(f, "DROP TABLE IF EXISTS{};", t.f(&self.table_name))?;
            }
            _ => {}
        }

        write!(f, "CREATE")?;
        if let Some(or_replace) = &self.or_replace {
            write!(f, "{}", t.f(or_replace))?;
        }
        write!(
            f,
            "{}TABLE{}{}",
            t.f(&self.table_ws),
            t.f(&self.table_name),
            t.f(&self.definition),
        )
    }
}

/// A `CREATE VIEW` statement.
#[derive(Debug)]
pub struct CreateViewStatement {
    pub create_ws: Whitespace,
    pub span: Span,
    pub or_replace: Option<OrReplace>,
    pub view_ws: Whitespace,
    pub view_name: TableName,
    // TODO: Factor out shared code from
    pub as_ws: Whitespace,
    pub query: QueryStatement,
}

impl Node for CreateViewStatement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", t.f(&self.create_ws))?;
        match t {
            Target::SQLite3 if self.or_replace.is_some() => {
                // We need to convert this to a `DROP VIEW IF EXISTS` statement.
                write!(f, "DROP VIEW IF EXISTS{};", t.f(&self.view_name))?;
            }
            _ => {}
        }

        write!(f, "CREATE")?;
        if let Some(or_replace) = &self.or_replace {
            write!(f, "{}", t.f(or_replace))?;
        }
        write!(
            f,
            "{}VIEW{}{}AS{}",
            t.f(&self.view_ws),
            t.f(&self.view_name),
            t.f(&self.as_ws),
            t.f(&self.query),
        )
    }
}

/// The `OR REPLACE` modifier.
#[derive(Debug)]
pub struct OrReplace {
    pub or_ws: Whitespace,
    pub span: Span,
    pub replace_ws: Whitespace,
}

impl Node for OrReplace {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            Target::BigQuery => write!(f, "{}OR{}REPLACE", t.f(&self.or_ws), t.f(&self.replace_ws)),
            Target::SQLite3 => Ok(()),
        }
    }
}

/// The part of a `CREATE TABLE` statement that defines the columns.
#[derive(Debug)]
pub enum CreateTableDefinition {
    /// ( column_definition [, ...] )
    Columns {
        paren1_ws: Whitespace,
        columns: NodeVec<ColumnDefinition>,
        paren2_ws: Whitespace,
    },
    /// AS select_statement
    As {
        as_ws: Whitespace,
        query_statement: QueryStatement,
    },
}

impl Node for CreateTableDefinition {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateTableDefinition::Columns {
                paren1_ws,
                columns,
                paren2_ws,
            } => {
                write!(f, "{}(", t.f(paren1_ws))?;
                columns.display_with_sep(t, f, ",")?;
                write!(f, "{})", t.f(paren2_ws))
            }
            CreateTableDefinition::As {
                as_ws,
                query_statement: select_statement,
            } => write!(f, "{}AS{}", t.f(as_ws), t.f(select_statement)),
        }
    }
}

/// A column definition.
#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: DataType,
}

impl Node for ColumnDefinition {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", t.f(&self.name), t.f(&self.data_type))
    }
}

/// A `DROP VIEW` statement.
#[derive(Debug)]
pub struct DropViewStatement {
    pub drop_ws: Whitespace,
    pub span: Span,
    pub view_ws: Whitespace,
    pub view_name: TableName,
}

impl Node for DropViewStatement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}DROP{}VIEW{}",
            t.f(&self.drop_ws),
            t.f(&self.view_ws),
            t.f(&self.view_name)
        )
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
//   - `_` matches optional whitespace.
//   - `__` matches mandatory whitespace.
//   - Normally, leading whitespace should be captured by the rule that includes
//     the actual following token. For example, we should have a rule with `ws:_
//     "SELECT"`, but not a rule like `ws:_ select:select()`, because the latter
//     would capture the whitespace before the `SELECT` keyword instead of
//     leaving it for the `ws:_ "SELECT"` rule.
peg::parser! {
    /// We parse as much of BigQuery's "Standard SQL" as we can.
    pub grammar sql_program() for str {
        pub rule sql_program() -> SqlProgram
            = statement:statement()
              trailing_ws:_ { SqlProgram {
                  statement,
                  trailing_ws,
              } }

        rule statement() -> Statement
            = s:query_statement() { Statement::Query(s) }
            / c:create_table_statement() { Statement::CreateTable(c) }
            / c:create_view_statement() { Statement::CreateView(c) }
            / d:drop_view_statement() { Statement::DropView(d) }

        rule query_statement() -> QueryStatement
            = query_expression:query_expression() { QueryStatement { query_expression } }

        rule query_expression() -> QueryExpression
            = select_expression:select_expression() { QueryExpression::SelectExpression(select_expression) }
            / paren1_ws:_ "(" query:query_statement() paren2_ws:_ ")" {
                QueryExpression::Nested {
                    paren1_ws,
                    query: Box::new(query),
                    paren2_ws,
                }
            }

        rule select_expression() -> SelectExpression
            = select_options:select_options()
              select_list:select_list()
              from_clause:from_clause()
              where_clause:where_clause()?
            {
                SelectExpression {
                    select_options,
                    select_list,
                    from_clause,
                    where_clause,
                }
            }

        rule select_options() -> SelectOptions
            = s:position!()
              ws:_
              k("SELECT")
              distinct:distinct()?
              e:position!()
            {
                SelectOptions {
                    span: s..e,
                    ws,
                    distinct,
                }
            }

        rule distinct() -> Distinct
            = ws:_ s:position!() k("DISTINCT") e:position!() {
                Distinct { span: s..e, ws }
            }

        rule select_list() -> SelectList
            = items:sep_opt_trailing(<select_list_item()>, <",">) {
                SelectList { items }
            }

        rule select_list_item() -> SelectListItem
            = ws:_ s:position!() "*" e:position!() {
                SelectListItem::Wildcard { span: s..e, ws }
            }
            / s:position!() table_name:table_name() dot_ws:_ "." star_ws:_ "*" e:position!() {
                SelectListItem::TableNameWildcard {
                    span: s..e,
                    table_name,
                    dot_ws,
                    star_ws,
                }
            }
            / s:position!() expression:expression() alias:alias()? e:position!() {
                SelectListItem::Expression { span: s..e, expression, alias }
            }

        rule expression() -> Expression = precedence! {
            left:(@) op_ws:_ ">=" right:@ {
                Expression::Binop {
                    left: Box::new(left),
                    op_ws,
                    op: Binop::Gte,
                    right: Box::new(right),
                }
            }
            --
            ws:_ s:position!() literal:literal() e:position!() {
                Expression::Literal { ws, span: s..e, literal }
            }
            ws:_ s:position!() k("NULL") e:position!() {
                Expression::Null { ws, span: s..e, }
            }
            table_and_column_name:table_and_column_name() {
                Expression::TableAndColumnName(table_and_column_name)
            }
            column_name:ident() { Expression::ColumnName(column_name) }
            ws:_ s:position!() k("CAST") paren_ws:_ "(" expression:expression() as_ws:_ k("AS") data_type:data_type() paren2_ws:_ ")" e:position!() {
                Expression::Cast {
                    ws,
                    span: s..e,
                    paren_ws,
                    expression: Box::new(expression),
                    as_ws,
                    data_type,
                    paren2_ws,
                }
            }
        }

        rule literal() -> Literal
            = value_str:$("-"? ['0'..='9']+) { Literal::Int64(value_str.parse().unwrap()) }

        rule data_type() -> DataType
            = ws:_ s:position!() k("BOOL") e:position!() {
                DataType::Bool { ws, span: s..e }
            }
            / ws:_ s:position!() k("INT64") e:position!() {
                DataType::Int64 { ws, span: s..e }
            }
            / ws:_ s:position!() k("STRING") e:position!() {
                DataType::String { ws, span: s..e }
            }
            / ws:_ s:position!() k("ARRAY") lt_ws:_ "<" data_type:data_type() gt_ws:_ ">" e:position!() {
                DataType::Array {
                    ws,
                    span: s..e,
                    lt_ws,
                    data_type: Box::new(data_type),
                    gt_ws,
                }
            }

        rule alias() -> Alias
            = ws:_ s:position!() k("AS") ident:ident() e:position!() {
                Alias {
                    span: s..e,
                    ws,
                    ident,
                }
            }

        rule from_clause() -> FromClause
            = ws:_ s:position!() k("FROM") from_item:from_item()
              join_operations:join_operations() e:position!()
            {
                FromClause {
                    span: s..e,
                    ws,
                    from_item,
                    join_operations,
                }
            }

        rule from_item() -> FromItem
            = table_name:table_name() alias:alias()? {
                FromItem::TableName { table_name, alias }
            }
            / paren1_ws:_ "(" query:query_statement() paren2_ws:_ ")" alias:alias()? {
                FromItem::Subquery {
                    paren1_ws,
                    query: Box::new(query),
                    paren2_ws,
                    alias,
                }
            }

        rule join_operations() -> Vec<JoinOperation>
            = join_operations:join_operation()* { join_operations }

        rule join_operation() -> JoinOperation
            = join_type:join_type() join_ws:_ k("JOIN") from_item:from_item() operator:condition_join_operator() {
                JoinOperation::ConditionJoin {
                    join_type,
                    join_ws,
                    from_item,
                    operator,
                }
            }

        rule join_type() -> JoinType
            = ws:_ s:position!() k("INNER") e:position!() {
                JoinType::Inner { ws, span: s..e }
            }
            / ws:_ s:position!() k("LEFT") e:position!() {
                JoinType::Left { ws, span: s..e }
            }
            / ws:_ s:position!() k("RIGHT") e:position!() {
                JoinType::Right { ws, span: s..e }
            }
            / ws:_ s:position!() k("FULL") e:position!() {
                JoinType::Full { ws, span: s..e }
            }

        rule condition_join_operator() -> ConditionJoinOperator
            = ws:_ s:position!() k("USING") paren1_ws:_ "(" column_names:sep(<ident()>, <",">) paren2_ws:_ ")" e:position!() {
                ConditionJoinOperator::Using {
                    span: s..e,
                    ws,
                    paren1_ws,
                    column_names,
                    paren2_ws,
                }
            }

        rule where_clause() -> WhereClause
            = ws:_ s:position!() k("WHERE") expression:expression() e:position!() {
                WhereClause {
                    span: s..e,
                    ws,
                    expression,
                }
            }

        rule create_table_statement() -> CreateTableStatement
            = create_ws:_ s:position!() k("CREATE") or_replace:or_replace()?
              table_ws:_ k("TABLE") table_name:table_name()
              definition:create_table_definition()
              e:position!()
            {
                CreateTableStatement {
                    create_ws,
                    span: s..e,
                    or_replace,
                    table_ws,
                    table_name,
                    definition,
                }
            }

        rule create_view_statement() -> CreateViewStatement
            = create_ws:_ s:position!() k("CREATE") or_replace:or_replace()?
              view_ws:_ k("VIEW") view_name:table_name()
              as_ws:_ k("AS") query:query_statement()
              e:position!()
            {
                CreateViewStatement {
                    create_ws,
                    span: s..e,
                    or_replace,
                    view_ws,
                    view_name,
                    as_ws,
                    query,
                }
            }

        rule or_replace() -> OrReplace
            = or_ws:_ s:position!() k("OR") replace_ws:_ k("REPLACE") e:position!() {
                OrReplace {
                    or_ws,
                    span: s..e,
                    replace_ws,
                }
            }

        rule create_table_definition() -> CreateTableDefinition
            = paren1_ws:_ "(" columns:sep(<column_definition()>, <",">) paren2_ws:_ ")" {
                CreateTableDefinition::Columns {
                    paren1_ws,
                    columns,
                    paren2_ws,
                }
            }
            / as_ws:_ k("AS") query_statement:query_statement() {
                CreateTableDefinition::As {
                    as_ws,
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
            = drop_ws:_ s:position!() (k("DROP") / k("DELETE")) view_ws:_ k("VIEW") view_name:table_name() e:position!() {
                DropViewStatement {
                    drop_ws,
                    span: s..e,
                    view_ws,
                    view_name,
                }
            }

        /// A table name, such as `t1` or `project-123.dataset1.table2`.
        rule table_name() -> TableName
            // We handle this manually because of PEG backtracking limitations.
            = dotted:dotted_name() {?
                let len = dotted.nodes.len();
                let mut nodes = dotted.nodes.into_iter();
                let mut trailing_ws = dotted.trailing_ws.into_iter();
                if len == 1 {
                    Ok(TableName::Table { table: nodes.next().unwrap() })
                } else if len == 2 {
                    Ok(TableName::DatasetTable {
                        dataset: nodes.next().unwrap(),
                        ws: trailing_ws.next().unwrap(),
                        table: nodes.next().unwrap(),
                    })
                } else if len == 3 {
                    Ok(TableName::ProjectDatasetTable {
                        project: nodes.next().unwrap(),
                        ws: trailing_ws.next().unwrap(),
                        dataset: nodes.next().unwrap(),
                        ws2: trailing_ws.next().unwrap(),
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
                let mut trailing_ws = dotted.trailing_ws.into_iter();
                if len == 2 {
                    Ok(TableAndColumnName {
                        table_name: TableName::Table { table: nodes.next().unwrap() },
                        ws: trailing_ws.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 3 {
                    Ok(TableAndColumnName {
                        table_name: TableName::DatasetTable {
                            dataset: nodes.next().unwrap(),
                            ws: trailing_ws.next().unwrap(),
                            table: nodes.next().unwrap(),
                        },
                        ws: trailing_ws.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else if len == 4 {
                    Ok(TableAndColumnName {
                        table_name: TableName::ProjectDatasetTable {
                            project: nodes.next().unwrap(),
                            ws: trailing_ws.next().unwrap(),
                            dataset: nodes.next().unwrap(),
                            ws2: trailing_ws.next().unwrap(),
                            table: nodes.next().unwrap(),
                        },
                        ws: trailing_ws.next().unwrap(),
                        column_name: nodes.next().unwrap(),
                    })
                } else {
                    Err("table and column name")
                }
            }

        /// A table or column name with internal dots. This requires special
        /// handling with a PEG parser, because PEG parsers are greedy
        /// and backtracking to try alternatives can sometimes be strange.
        rule dotted_name() -> NodeVec<Identifier> = sep(<ident()>, <".">)

        /// An identifier, such as a column name.
        rule ident() -> Identifier
            = ws:_ s:position!() id:c_ident() e:position!() {?
                if KEYWORDS.contains(id.to_ascii_uppercase().as_str()) {
                    // Wanted an identifier, but got a bare keyword.
                    Err("identifier")
                } else {
                    Ok(Identifier {
                        span: s..e,
                        ws,
                        // TODO: Unescape.
                        text: id.to_string(),
                    })
                }
            }
            / ws:_ s:position!() "`" id:$(([^ '\\' | '`'] / escape())*) "`" e:position!() {
                Identifier {
                    span: s..e,
                    ws,
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

        /// Keywords. These use case-insensitive matching, and may not be
        /// followed by a valid identifier character. See
        /// https://github.com/kevinmehall/rust-peg/issues/216#issuecomment-564390313
        rule k(kw: &'static str)
            = input:$([_]*<{kw.len()}>) !['a'..='z' | 'A'..='Z' | '0'..='9' | '_']
              {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) } }

        /// Punctuation separated list. Does not allow a trailing separator.
        rule sep<T: fmt::Debug, S>(item: rule<T>, separator: rule<S>) -> NodeVec<T>
            = first:item()
              items:(ws:_ separator() item:item() { (ws, item) })*
            {
                let mut nodes = Vec::new();
                let mut trailing_ws = Vec::new();
                nodes.push(first);
                for (ws, item) in items {
                    trailing_ws.push(ws);
                    nodes.push(item);
                }
                NodeVec { nodes, trailing_ws }
            }

        /// Punctuation separated list. Allows a trailing separator.
        rule sep_opt_trailing<T: fmt::Debug, S>(item: rule<T>, separator: rule<S>) -> NodeVec<T>
            = first:item()
              items:(ws:_ separator() item:item() { (ws, item) })*
              ws2:(ws2:_ separator() { ws2 })?
            {
                let mut nodes = Vec::new();
                let mut trailing_ws = Vec::new();
                nodes.push(first);
                for (ws, item) in items {
                    trailing_ws.push(ws);
                    nodes.push(item);
                }
                if let Some(ws) = ws2 {
                    trailing_ws.push(ws);
                }
                NodeVec { nodes, trailing_ws }
            }

        // Whitespace, including comments. We don't normally want whitespace to
        // show up as an "expected" token in error messages, so we carefully
        // enclose _most_ of this in `quiet!`. The exception is the closing "*/"
        // in a block comment, which we want to mention explicitly.

        /// Optional whitespace.
        rule _ -> Whitespace
            = w:__ { w }
            / s:position!() e:position!() { Whitespace {
                span: s..e,
                text: "".to_string(),
            } }

        /// Mandatory whitespace.
        rule __ -> Whitespace
            = s:position!()
              text:$((whitespace() / line_comment() / block_comment())+)
              e:position!()
            {
                Whitespace {
                    span: s..e,
                    text: text.to_string(),
                }
            }

        rule whitespace() = quiet! { [' ' | '\t' | '\r' | '\n'] }
        rule line_comment() = quiet! { ("#" / "--") (!['\n'][_])* ( "\n" / ![_] ) }
        rule block_comment() = quiet! { "/*"(!"*/"[_])* } "*/"
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    #[test]
    fn test_parser_and_run_with_sqlite3() {
        let sql_examples = &[
            // Basic test cases of gradually increasing complexity.
            (
                r#"SELECT * FROM t # comment"#,
                r#"SELECT * FROM t # comment"#,
            ),
            (r#"SELECT DISTINCT * FROM t"#, r#"SELECT DISTINCT * FROM t"#),
            (r#"SELECT * FROM `t`"#, r#"SELECT * FROM t"#),
            (r#"select * from t"#, r#"SELECT * FROM t"#),
            (
                r#"select /* hi */ * from `t`"#,
                r#"SELECT /* hi */ * FROM t"#,
            ),
            (r#"SELECT a,b FROM t"#, r#"SELECT a,b FROM t"#),
            (
                r#"select a, b /* hi */, from t"#,
                r#"SELECT a, b /* hi */, FROM t"#,
            ),
            (
                "select a, b, /* hi */ from t",
                "SELECT a, b, /* hi */ FROM t",
            ),
            ("select a, b,from t", "SELECT a, b,FROM t"),
            (
                r#"select p.*, p.a AS c from t as p"#,
                r#"SELECT p.*, p.a AS c FROM t AS p"#,
            ),
            (
                r"SELECT * FROM t WHERE a >= 0",
                r"SELECT * FROM t WHERE a >= 0",
            ),
            (
                r#"SELECT * FROM (SELECT a FROM t) AS p"#,
                r#"SELECT * FROM (SELECT a FROM t) AS p"#,
            ),
            (
                r#"select * from `p-123`.`d`.`t`"#,
                r#"SELECT * FROM `p-123`.d.t"#,
            ),
            (r#"select * from `d`.`t`"#, r#"SELECT * FROM d.t"#),
            (
                r#"SELECT a, CAST(NULL AS BOOL) AS placeholder FROM t"#,
                r#"SELECT a, CAST(NULL AS BOOL) AS placeholder FROM t"#,
            ),
            (
                r#"SELECT a, CAST(NULL AS ARRAY<INT64>) AS placeholder FROM t"#,
                r#"SELECT a, CAST(NULL AS ARRAY<INT64>) AS placeholder FROM t"#,
            ),
            (
                r#"CREATE OR REPLACE TABLE t2 (a INT64, b INT64)"#,
                r#"CREATE OR REPLACE TABLE t2 (a INT64, b INT64)"#,
            ),
            (
                r#"CREATE OR REPLACE TABLE t2 AS (SELECT * FROM t)"#,
                r#"CREATE OR REPLACE TABLE t2 AS (SELECT * FROM t)"#,
            ),
            (
                r#"CREATE OR REPLACE VIEW v AS (SELECT * FROM t)"#,
                r#"CREATE OR REPLACE VIEW v AS (SELECT * FROM t)"#,
            ),
            (r#"DROP VIEW v"#, r#"DROP VIEW v"#),
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
                r#"# Here's a challenge!
CREATE OR REPLACE TABLE `project-123`.proxies.t2 AS (
    SELECT
        s1.*,
        CAST(s1.random_id AS STRING) AS `key`,
        proxy.first_name,
        proxy.last_name,
        # this is a comment
        CAST(NULL AS BOOL) AS id_placeholder,
        CAST(NULL AS ARRAY<INT64>) AS more_ids
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
        ];

        // Set up SQLite3 database for testing transpiled SQL.
        let conn = Connection::open_in_memory().expect("failed to open SQLite3 database");
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

        for (sql, normalized) in sql_examples {
            println!("parsing:   {}", sql);
            let parsed = sql_program::sql_program(sql).unwrap();
            assert_eq!(normalized, &parsed.to_string_for_target(Target::BigQuery));

            let sql = parsed.to_string_for_target(Target::SQLite3);
            println!("  SQLite3: {}", sql);
            if let Err(err) = conn.execute_batch(&sql) {
                panic!("failed to execute with SQLite3:\n{}\n{}", sql, err);
            }
        }
    }
}
