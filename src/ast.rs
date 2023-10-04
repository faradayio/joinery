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
#[derive(Clone, Copy, Debug)]
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
    Select(SelectStatement),
}

impl Node for Statement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(s) => write!(f, "{}", t.f(s)),
        }
    }
}

/// A `SELECT` statement.
#[derive(Debug)]
pub struct SelectStatement {
    pub select: Select,
    pub select_list: SelectList,
    pub from_clause: FromClause,
    // pub where_clause: Option<WhereClause>,
    // pub group_by: Option<GroupBy>,
    // pub having: Option<Having>,
    // pub order_by: Option<OrderBy>,
    // pub limit: Option<Limit>,
}

impl Node for SelectStatement {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            t.f(&self.select),
            t.f(&self.select_list),
            t.f(&self.from_clause)
        )
    }
}

/// The head of a `SELECT`, including any modifiers.
#[derive(Debug)]
pub struct Select {
    pub ws: Whitespace,
    pub span: Span,
    pub distinct: Option<Distinct>,
}

impl Node for Select {
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
    ColumnName(Identifier),
    TableAndColumnName(TableAndColumnName),
}

impl Node for Expression {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::ColumnName(ident) => write!(f, "{}", t.f(ident)),
            Expression::TableAndColumnName(table_and_column_name) => {
                write!(f, "{}", t.f(table_and_column_name))
            }
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
    pub table_name: TableName,
    pub alias: Option<Alias>,
}

impl Node for FromClause {
    fn fmt_for_target(&self, t: Target, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}FROM{}", t.f(&self.ws), t.f(&self.table_name))?;
        if let Some(alias) = &self.alias {
            write!(f, "{}", t.f(alias))?;
        }
        Ok(())
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
    grammar sql_program() for str {
        pub rule sql_program() -> SqlProgram
            = statement:statement()
              trailing_ws:_ { SqlProgram {
                  statement,
                  trailing_ws,
              } }

        rule statement() -> Statement
            = s:select_statement() { Statement::Select(s) }

        rule select_statement() -> SelectStatement
            = select:select()
              select_list:select_list()
              from_clause:from_clause()
            {
                SelectStatement {
                    select,
                    select_list,
                    from_clause,
                }
            }

        rule select() -> Select
            = s:position!()
              ws:_
              k("SELECT")
              distinct:distinct()?
              e:position!()
            {
                Select {
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

        rule expression() -> Expression
            = table_and_column_name:table_and_column_name() {
                Expression::TableAndColumnName(table_and_column_name)
            }
            / column_name:ident() { Expression::ColumnName(column_name) }

        rule alias() -> Alias
            = ws:_ s:position!() k("AS") ident:ident() e:position!() {
                Alias {
                    span: s..e,
                    ws,
                    ident,
                }
            }

        rule from_clause() -> FromClause
            = ws:_ s:position!() k("FROM") table_name:table_name() alias:(alias()?) e:position!()
            {
                FromClause {
                    span: s..e,
                    ws,
                    table_name,
                    alias,
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
                        text: id.to_string(),
                    })
                }
            }
            / ws:_ s:position!() "`" id:$(([^ '\\' | '`'] / escape())*) "`" e:position!() {
                Identifier {
                    span: s..e,
                    ws,
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
                r#"select * from `p-123`.`d`.`t`"#,
                r#"SELECT * FROM `p-123`.d.t"#,
            ),
            (r#"select * from `d`.`t`"#, r#"SELECT * FROM d.t"#),
            // We're working up to being able to parse this.
            //
            // r#"
            // CREATE OR REPLACE TABLE `project-123`.`proxies`.`t2`  AS (
            //     SELECT
            //         s1.*,
            //         CAST(`s1`.`random_id` AS STRING) AS key,
            //         proxy.first_name,
            //         proxy.last_name,
            //         -- this is a comment
            //         CAST(NULL AS BOOL) AS id_placeholder,
            //         CAST(NULL AS ARRAY<INT64>) as more_ids
            //     FROM `project-123`.`sources`.`s1` AS s1
            //     INNER JOIN
            //         (
            //             SELECT DISTINCT first_name, last_name, `join_id`
            //             FROM `project-123`.`proxies`.`t1`
            //         ) AS proxy
            //         USING (`join_id`)
            //     )
            // "#,
        ];

        // Set up SQLite3 database for testing transpiled SQL.
        let conn = Connection::open_in_memory().expect("failed to open SQLite3 database");
        let fixtures = r#"
            CREATE TABLE t (a INT, b INT);
            CREATE TABLE "p-123.d.t" (a INT, b INT);
            CREATE TABLE "d.t" (a INT, b INT);
        "#;
        conn.execute_batch(fixtures)
            .expect("failed to create SQLite3 fixtures");

        for (sql, normalized) in sql_examples {
            println!("parsing:   {}", sql);
            let parsed = sql_program::sql_program(sql).unwrap();
            assert_eq!(normalized, &parsed.to_string_for_target(Target::BigQuery));

            let sql = parsed.to_string_for_target(Target::SQLite3);
            println!("  SQLite3: {}", sql);
            if let Err(err) = conn.prepare(&sql) {
                panic!("failed to prepare SQL:\n{}\n{}", sql, err);
            }
        }
    }
}
