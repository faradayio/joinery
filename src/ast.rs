use std::{ops::Range, fmt};

/// None of these keywords should ever be matched as a bare identifier. We use
/// [`phf`](https://github.com/rust-phf/rust-phf), which generates "perfect hash
/// functions." These allow us to create highly optimized, read-only sets/maps
/// generated at compile time.
static KEYWORDS: phf::Set<&'static str> = phf::phf_set! {
    "AS", "DISTINCT", "FROM", "SELECT",
};

/// We represent a span in our source code using a Rust range. These are easy
/// to construct from within our parser.
type Span = Range<usize>;

/// A node in our abstract syntax tree.
pub trait Node: fmt::Debug + fmt::Display {
     /// The source span corresponding to this node.
     fn span(&self) -> Span;
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

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.text)
    }    
}

impl Node for Whitespace {
    fn span(&self) -> Span {
        self.span.clone()
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

impl<T: fmt::Debug + fmt::Display> NodeVec<T> {
    pub fn display_with_sep(&self, f: &mut fmt::Formatter<'_>, sep: &str) -> fmt::Result {
        for (i, node) in self.nodes.iter().enumerate() {
            write!(f, "{}", node)?;
            if i < self.trailing_ws.len() {
                write!(f, "{}", self.trailing_ws[i])?;
                write!(f, "{}", sep)?;
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

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.ws, self.text)
    }    
}

/// A table name.
#[derive(Debug)]
pub struct TableName {
    pub ident: Identifier,
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.ident.fmt(f)
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

impl fmt::Display for SqlProgram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.statement, self.trailing_ws)
    }    
}

/// A statement in our abstract syntax tree.
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(s) => write!(f, "{}", s),
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

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.select, self.select_list, self.from_clause)
    }    
}

/// The head of a `SELECT`, including any modifiers.
#[derive(Debug)]
pub struct Select {
    pub ws: Whitespace,
    pub span: Span,
    pub distinct: Option<Distinct>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}SELECT", self.ws)?;
        if let Some(distinct) = &self.distinct {
            write!(f, "{}", distinct)?;
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

impl fmt::Display for Distinct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}DISTINCT", self.ws)
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

impl fmt::Display for SelectList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.items.display_with_sep(f, ",")
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
    Wildcard {
        span: Span,
        ws: Whitespace,
    },
    /// A `table.*` wildcard.
    TableNameWildcard {
        span: Span,
        table_name: TableName,
        dot_ws: Whitespace,
        star_ws: Whitespace,
    },
}

impl fmt::Display for SelectListItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectListItem::Expression { expression, alias, .. } => {
                write!(f, "{}", expression)?;
                if let Some(alias) = alias {
                    write!(f, "{}", alias)?;
                }
                Ok(()) 
            }
            SelectListItem::Wildcard { ws, .. } => write!(f, "{}*", ws),
            SelectListItem::TableNameWildcard { table_name, dot_ws, star_ws, .. } => {
                write!(f, "{}{}.{}*", table_name, dot_ws, star_ws)
            }   
        }
    }    
}

/// An SQL expression.
#[derive(Debug)]
pub enum Expression {
    ColumnName(Identifier),
    TableAndColumnName {
        span: Span,
        table_name: TableName,
        /// Whitespace before `.`.
        ws: Whitespace,
        column_name: Identifier,
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::ColumnName(ident) => write!(f, "{}", ident),
            Expression::TableAndColumnName { table_name, ws, column_name, .. } => {
                write!(f, "{}{}.{}", table_name, ws, column_name)
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

impl fmt::Display for Alias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}AS{}", self.ws, self.ident)
    }    
}

/// The `FROM` clause.
/// 
/// TODO: We're keeping this simple for now.
#[derive(Debug)]
pub struct FromClause {
    pub ws: Whitespace,
    pub span: Span,
    pub table_name: Identifier,
    pub alias: Option<Alias>,
}

impl fmt::Display for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}FROM{}", self.ws, self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, "{}", alias)?;
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
            = items:sep(<select_list_item()>, <",">) {
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
            = s:position!() table_name:table_name() ws:_ "." column_name:ident() e:position!() {
                Expression::TableAndColumnName {
                    span: s..e,
                    table_name,
                    ws,
                    column_name,
                }
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
            = ws:_ s:position!() k("FROM") table_name:ident() alias:(alias()?) e:position!()
            {
                FromClause {
                    span: s..e,
                    ws,
                    table_name,
                    alias,
                }
            }

        /// A table name, such as `t1` or `project-123.dataset1.table2`.
        ///
        /// TODO: Support more possibilities.
        rule table_name() -> TableName = ident:ident() { TableName { ident } }

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
            / ws:_ s:position!() "`" id:c_ident() "`" e:position!() {
                Identifier {
                    span: s..e,
                    ws,
                    text: id.to_string(),
                }
            }
            / expected!("identifier")

        /// Low-level rule for matching a C-style identifier.
        rule c_ident() -> String
            = id:$(['a'..='z' | 'A'..='Z' | '_'] ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*)
              // The next character cannot be a valid ident character.
              !['a'..='z' | 'A'..='Z' | '0'..='9' | '_']
              { id.to_string() }
            / expected!("identifier")

        /// Keywords. These use case-insensitive matching, and may not be
        /// followed by a valid identifier character. See
        /// https://github.com/kevinmehall/rust-peg/issues/216#issuecomment-564390313
        rule k(kw: &'static str)
            = input:$([_]*<{kw.len()}>) !['a'..='z' | 'A'..='Z' | '0'..='9' | '_']
              {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) } }

        /// Punctuation separated list. Allows a trailing separator.
        rule sep<T: fmt::Debug, S>(item: rule<T>, separator: rule<S>) -> NodeVec<T>
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
    use super::*;

    #[test]
    fn test_parser() {
        let sql_examples = &[
            // Basic test cases of gradually increasing complexity.
            (r#"SELECT DISTINCT * FROM t1"#, r#"SELECT DISTINCT * FROM t1"#),
            (r#"SELECT * FROM `t1`"#, r#"SELECT * FROM t1"#),
            (r#"select * from t1"#, r#"SELECT * FROM t1"#),
            (r#"select /* hi */ * from `t1`"#, r#"SELECT /* hi */ * FROM t1"#),
            (r#"SELECT a,b FROM t"#, r#"SELECT a,b FROM t"#),
            (r#"select a, b /* hi */, from t"#, r#"SELECT a, b /* hi */, FROM t"#),
            (r#"select p.*, p.a AS b from t as p"#, r#"SELECT p.*, p.a AS b FROM t AS p"#),

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
        for (sql, normalized) in sql_examples {
            println!("parsing: {}", sql);
            let parsed = sql_program::sql_program(sql).unwrap();
            assert_eq!(normalized, &parsed.to_string());
        }
    }
}
