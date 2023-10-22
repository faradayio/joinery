//! Convert raw strings into a series of tokens.
//!
//! This is essentially a lexer/tokenizer, separate from our actual SQL grammar.
//! Compare and contrast Rust's [`proc_macro2::TokenTree`][TokenTree], and
//! [`peg`'s own `Token` type][Token]. We split this from the main grammar so
//! that we can implement [quasiquoting][], similar to the Rust [`quote`
//! crate][quote].
//!
//! Essentially, we want to be able to write:
//!
//! ```rust,no_compile
//! let expr = some_expression_from_parse_tree();
//! sql_quote! { CAST((#expr) AS STRING) }
//! ```
//!
//! Here, `expr` will be flattened back into tokens, and combined with tokens
//! from the `sql_quote!` macro to produce a new stream of tokens. This stream
//! will then be _reparsed_ into a new expression AST. In other words, we want
//! to implement "Rust-style macros for SQL", so that we can rewrite SQL
//! parse trees in a clear and powerful way.
//!
//! [TokenTree]: https://doc.rust-lang.org/proc_macro/enum.TokenTree.html
//! [Token]:
//!     https://github.com/kevinmehall/rust-peg/blob/master/peg-macros/tokens.rs
//! [quasiquoting]: https://docs.racket-lang.org/reference/quasiquote.html
//! [quote]: https://docs.rs/quote/latest/quote/

use std::{
    fmt::{self},
    io::{self, Write as _},
    ops::Range,
    path::Path,
    str::from_utf8,
};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
};
use derive_visitor::{Drive, DriveMut};
use joinery_macros::ToTokens;
use peg::{error::ParseError, Parse, ParseElem, RuleResult};

use crate::{
    ast,
    drivers::bigquery::BigQueryString,
    errors::{Result, SourceError},
};

/// A source location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Loc {
    /// The file ID used to identify this file. The actual source lives in
    /// a `codespan_reporting` data structure.
    pub file_id: usize,

    /// The byte offset of this location.
    pub offset: usize,
}

// We implement `PartialOrd`, because positions in the same file are comparable.
// But we don't implement `Ord`, because positions in different files are not
// comparable.
impl PartialOrd for Loc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.file_id == other.file_id {
            Some(self.offset.cmp(&other.offset))
        } else {
            None
        }
    }
}

impl fmt::Display for Loc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "file_id={} offset={}", self.file_id, self.offset)
    }
}

/// A span of source code.
pub type Span = Range<Loc>;

/// A token.
#[derive(Clone, Drive, DriveMut, PartialEq)]
pub enum Token {
    /// Whitespace in an empty file.
    EmptyFile(EmptyFile),

    /// An identifier.
    Ident(Ident),

    /// A literal.
    Literal(Literal),

    /// A punctuation token.
    Punct(Punct),
}

impl Token {
    /// Construct a new identifier token.
    pub fn ident(ident: &str) -> Self {
        Self::Ident(Ident::new(ident))
    }

    /// Construct a new punctuation token.
    pub fn punct(punct: &str) -> Self {
        Self::Punct(Punct::new(punct))
    }

    /// Get the raw token for this token.
    pub fn raw(&self) -> &RawToken {
        match self {
            Token::EmptyFile(empty_file) => &empty_file.token,
            Token::Ident(ident) => &ident.token,
            Token::Literal(literal) => &literal.token,
            Token::Punct(punct) => &punct.token,
        }
    }

    /// Get the mutable raw token for this token.
    pub fn raw_mut(&mut self) -> &mut RawToken {
        match self {
            Token::EmptyFile(empty_file) => &mut empty_file.token,
            Token::Ident(ident) => &mut ident.token,
            Token::Literal(literal) => &mut literal.token,
            Token::Punct(punct) => &mut punct.token,
        }
    }
}

// We want to simplify the debug output of `Token`, because it will normally
// appear as part of a vector of many tokens, or as part of a larger parse tree.
// The default output would result in screens full of information almost nobody
// will ever read.
//
// Feel free to comment this out and `#[derive(Debug)]` if you actually need more
// details to debug a problem.
impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::EmptyFile(_) => write!(f, "$EMPTY_FILE"),
            Token::Ident(ident) => write!(f, "I:{}", ident.name),
            Token::Literal(literal) => write!(f, "L:{}", literal.value),
            Token::Punct(punct) => write!(f, "P:{}", punct.token.as_str()),
        }
    }
}

/// A raw token. This contains both the text of the token itself, and possibly some
/// surrounding whitespace.
#[derive(Debug, Drive, DriveMut, Eq, Clone)]
pub struct RawToken {
    /// The raw text of the token, including any surrounding whitespace.
    #[drive(skip)]
    raw: String,

    /// Indices into `raw` pointing to the real text.
    #[drive(skip)]
    token_range: Range<usize>,

    /// The span from which we parsed this token. May not exist if we generated
    /// this token internally. The span refers to the original location, because
    /// the token may have been modified.
    #[drive(skip)]
    source_span: Option<Span>,
}

impl RawToken {
    /// Create a new token.
    pub fn new(s: &str) -> Self {
        Self {
            raw: s.to_string(),
            token_range: 0..s.len(),
            source_span: None,
        }
    }

    /// Leading whitespace.
    pub fn leading_whitespace(&self) -> &str {
        &self.raw[..self.token_range.start]
    }

    /// Get the token, including any whitespace.
    pub fn as_raw_str(&self) -> &str {
        &self.raw
    }

    /// Get the token without any whitespace.
    pub fn as_str(&self) -> &str {
        &self.raw[self.token_range.clone()]
    }

    /// Trainling whitespace.
    pub fn trailing_whitespace(&self) -> &str {
        &self.raw[self.token_range.end..]
    }

    /// Create a new token, changing the string.
    pub fn with_str(&self, s: &str) -> Self {
        let mut raw = String::with_capacity(
            self.leading_whitespace().len() + s.len() + self.trailing_whitespace().len(),
        );
        raw.push_str(self.leading_whitespace());
        raw.push_str(s);
        raw.push_str(self.trailing_whitespace());

        Self {
            raw,
            token_range: self.leading_whitespace().len()..self.leading_whitespace().len() + s.len(),
            source_span: self.source_span.clone(),
        }
    }

    /// Create a new token, without any whitespace.
    pub fn without_ws(&self) -> Self {
        let raw = self.as_str().to_string();
        let raw_len = raw.len();
        Self {
            raw,
            token_range: 0..raw_len,
            source_span: self.source_span.clone(),
        }
    }

    /// Create a new token, with the string erased.
    pub fn with_ws_only(&self) -> Self {
        self.with_str("")
    }

    /// Prepend whitespace to this token.
    fn prepend_whitespace(&mut self, ws: &str) {
        let mut raw = String::with_capacity(ws.len() + self.raw.len());
        raw.push_str(ws);
        raw.push_str(&self.raw);
        self.raw = raw;
        self.token_range.start += ws.len();
        self.token_range.end += ws.len();
    }
}

impl PartialEq for RawToken {
    /// Two tokens are equal if they have the same text.
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

/// The start of a file.
#[derive(Clone, Drive, DriveMut, Debug, PartialEq)]
pub struct EmptyFile {
    /// Our token, which should contain nothing but (maybe) whitespace.
    pub token: RawToken,
}

/// An identifier token
#[derive(Clone, Debug, Drive, DriveMut, Eq)]
pub struct Ident {
    /// Our token.
    pub token: RawToken,

    /// The actual identifier, normalized to uppercase if it wasn't quoted.
    #[drive(skip)]
    pub name: String,
}

impl Ident {
    /// Create a new `Ident` with no source location.
    pub fn new(name: &str) -> Self {
        Self {
            token: RawToken::new(name),
            name: name.to_owned(),
        }
    }
}

impl PartialEq for Ident {
    fn eq(&self, other: &Self) -> bool {
        // TODO: Case sensitivity?
        self.name == other.name
    }
}

/// A keyword. This is just a thin wrapper over an `Ident` to change the
/// equality semantics.
#[derive(Debug, Drive, DriveMut, Clone, Eq, ToTokens)]
pub struct Keyword {
    /// Our keyword.
    pub ident: Ident,
}

impl Keyword {
    /// Create a new `Keyword` with no source location.
    pub fn new(name: &str) -> Self {
        Self {
            ident: Ident::new(name),
        }
    }
}

impl PartialEq for Keyword {
    fn eq(&self, other: &Self) -> bool {
        self.ident.name.eq_ignore_ascii_case(&other.ident.name)
    }
}

/// A case-insensitive identifier. This is pretty much identical to a keyword,
/// but it appears in different places in the grammar. These words are only
/// reserved in specific contexts and they don't normally need to be quoted
/// when used as column names, etc.
#[derive(Debug, Drive, DriveMut, Clone, Eq, ToTokens)]
pub struct CaseInsensitiveIdent {
    /// Our identifier.
    pub ident: Ident,
}

impl CaseInsensitiveIdent {
    /// Create a new `CaseInsensitiveIdent` with no source location.
    pub fn new(name: &str) -> Self {
        Self {
            ident: Ident::new(name),
        }
    }
}

impl PartialEq for CaseInsensitiveIdent {
    fn eq(&self, other: &Self) -> bool {
        self.ident.name.eq_ignore_ascii_case(&other.ident.name)
    }
}

/// A literal token.
#[derive(Debug, Drive, DriveMut, Clone, PartialEq)]
pub struct Literal {
    /// Our token.
    pub token: RawToken,

    /// The actual literal value.
    #[drive(skip)]
    pub value: LiteralValue,
}

impl Literal {
    /// Construct a literal containing an integer.
    pub fn int(i: i64) -> Self {
        Self {
            token: RawToken::new(&i.to_string()),
            value: LiteralValue::Int64(i),
        }
    }

    /// Construct a literal containing a floating-point number.
    pub fn float(d: f64) -> Self {
        Self {
            token: RawToken::new(&d.to_string()),
            value: LiteralValue::Float64(d),
        }
    }

    /// Construct a literal containing a string.
    pub fn string(s: &str) -> Self {
        Self {
            token: RawToken::new(&BigQueryString(s).to_string()),
            value: LiteralValue::String(s.to_owned()),
        }
    }
}

/// A literal value.
///
/// Does not include literals like `TRUE`, `FALSE` or `NULL`, which are parsed
/// as identifiers at this level.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    /// An integer.
    Int64(i64),

    /// A floating-point number.
    Float64(f64),

    /// A string.
    String(String),
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Int64(i) => write!(f, "{}", i),
            LiteralValue::Float64(d) => write!(f, "{}", d),
            LiteralValue::String(s) => write!(f, "{}", BigQueryString(s)),
        }
    }
}

/// A punctuation token.
#[derive(Debug, Drive, DriveMut, Clone, PartialEq)]
pub struct Punct {
    /// Our token.
    pub token: RawToken,
}

impl Punct {
    /// Create a new `Punct` with no source location.
    pub fn new(s: &str) -> Self {
        Self {
            token: RawToken::new(s),
        }
    }
}

/// A token stream.
#[derive(Debug, Clone, PartialEq)]
pub struct TokenStream {
    /// The tokens.
    tokens: Vec<Token>,
}

impl TokenStream {
    /// Create from tokens.
    pub fn from_tokens<Tokens: Into<Vec<Token>>>(tokens: Tokens) -> Self {
        Self {
            tokens: tokens.into(),
        }
    }

    /// Try to parse this stream using a grammar rule. This is generally called
    /// to re-parse the token stream created by [`sql_quote!`].
    fn try_into_parsed<T, R>(self, grammar_rule: R) -> Result<T>
    where
        R: FnOnce(&TokenStream) -> Result<T, ParseError<Loc>>,
    {
        match grammar_rule(&self) {
            Ok(t) => Ok(t),
            Err(err) => {
                let diagnostic = Diagnostic::error().with_message("Failed to parse token stream");
                Err(SourceError {
                    expected: err.to_string(),
                    files: SimpleFiles::new(),
                    diagnostic,
                }
                .into())
            }
        }
    }

    /// Try to parse this stream as a [`ast::Statement`].
    pub fn try_into_statement(self) -> Result<ast::Statement> {
        self.try_into_parsed(ast::sql_program::statement)
    }

    /// Try to parse this stream as a [`ast::QueryExpression`].
    pub fn try_into_query_expression(self) -> Result<ast::QueryExpression> {
        self.try_into_parsed(ast::sql_program::query_expression)
    }

    /// Try to parse this stream as a [`ast::Expression`].
    pub fn try_into_expression(self) -> Result<ast::Expression> {
        self.try_into_parsed(ast::sql_program::expression)
    }

    /// Parse a literal.
    pub fn literal(&self, pos: usize) -> RuleResult<Literal> {
        match self.tokens.get(pos) {
            Some(Token::Literal(literal)) => RuleResult::Matched(pos + 1, literal.clone()),
            _ => RuleResult::Failed,
        }
    }

    /// Parse an identifier. This preserves case, because BigQuery has
    /// [complicated rules][case] about which identifiers are case-sensitive.
    ///
    /// [case]: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    pub fn ident(&self, pos: usize) -> RuleResult<Ident> {
        match self.tokens.get(pos) {
            Some(Token::Ident(ident)) => RuleResult::Matched(pos + 1, ident.clone()),
            _ => RuleResult::Failed,
        }
    }

    /// Parse an identifier matching a specific string, ignoring case. This is a
    /// bit of an edge case, because we parse most identifiers using either
    /// [`TokenStream::ident`] above (which preserves case) or as
    /// [`TokenStream::keyword`] (which preserves case but ignores it when
    /// comparing).
    ///
    /// But there are some tokens which aren't keywords, but which need to be
    /// case-insensitive. For example, many of the date functions take arguments
    /// like `DAY`, but they aren't actually keywords. Column type declarations
    /// are similar.
    ///
    /// For a list of current case-sensitivity rules, see [the BigQuery
    /// docs][case].
    ///
    /// [case]: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    pub fn ident_eq_ignore_ascii_case(
        &self,
        pos: usize,
        s: &'static str,
    ) -> RuleResult<CaseInsensitiveIdent> {
        match self.tokens.get(pos) {
            Some(Token::Ident(ident)) if ident.name.eq_ignore_ascii_case(s) => RuleResult::Matched(
                pos + 1,
                CaseInsensitiveIdent {
                    ident: ident.clone(),
                },
            ),
            _ => RuleResult::Failed,
        }
    }

    /// Parse a keyword matching a specific string.
    pub fn keyword(&self, pos: usize, s: &'static str) -> RuleResult<Keyword> {
        match self.tokens.get(pos) {
            Some(Token::Ident(ident)) if ident.name.eq_ignore_ascii_case(s) => RuleResult::Matched(
                pos + 1,
                Keyword {
                    ident: ident.clone(),
                },
            ),
            _ => RuleResult::Failed,
        }
    }

    /// Parse a punctuation token matching a specific string.
    pub fn punct_eq(&self, pos: usize, s: &'static str) -> RuleResult<Punct> {
        match self.tokens.get(pos) {
            Some(Token::Punct(punct)) if punct.token.as_str() == s => {
                RuleResult::Matched(pos + 1, punct.clone())
            }
            _ => RuleResult::Failed,
        }
    }
}

impl Parse for TokenStream {
    type PositionRepr = Loc;

    fn start(&self) -> usize {
        0
    }

    fn is_eof(&self, pos: usize) -> bool {
        pos >= self.tokens.len()
    }

    fn position_repr(&self, pos: usize) -> Self::PositionRepr {
        if pos < self.tokens.len() {
            self.tokens[pos]
                .raw()
                .source_span
                .as_ref()
                .expect("tokens from tokenizer should always have a source location")
                .start
        } else if pos > 0 {
            self.tokens[pos - 1]
                .raw()
                .source_span
                .as_ref()
                .expect("tokens from tokenizer should always have a source location")
                .end
        } else {
            // We have absolutely no tokens, and we're being asked for the
            // position of the first token. So make something up and hope it
            // doesn't break `codespan_reporting`.
            //
            // This shouldn't happen, anyway, because we should always return
            // a single `EmptyFile` token for an empty file.
            Loc {
                file_id: 0,
                offset: 0,
            }
        }
    }
}

impl<'input> ParseElem<'input> for TokenStream {
    type Element = &'input Token;

    fn parse_elem(&'input self, pos: usize) -> peg::RuleResult<Self::Element> {
        match self.tokens.get(pos) {
            Some(c) => RuleResult::Matched(pos + 1, c),
            None => RuleResult::Failed,
        }
    }
}

// We don't implement `ParseLiteral` for `TokenStream` because all our
// literal-matching rules need to return a token, and `ParseLiteral` returns
// `()`.

/// Convert a value into tokens, and append them to a [`Vec`].
///
/// This is used as part of the implementation of [`sql_quote!`].
pub trait ToTokens {
    /// Convert `self` into tokens, and append them to `tokens`.
    fn to_tokens(&self, tokens: &mut Vec<Token>);
}

impl ToTokens for i64 {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.push(Token::Literal(Literal::int(*self)));
    }
}

impl ToTokens for Token {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.push(self.clone());
    }
}

impl ToTokens for Ident {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.push(Token::Ident(self.clone()));
    }
}

impl ToTokens for Literal {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.push(Token::Literal(self.clone()));
    }
}

impl ToTokens for Punct {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.push(Token::Punct(self.clone()));
    }
}

impl<T: ToTokens> ToTokens for Option<T> {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        if let Some(t) = self {
            t.to_tokens(tokens);
        }
    }
}

impl<T: ToTokens> ToTokens for Vec<T> {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        for t in self {
            t.to_tokens(tokens);
        }
    }
}

impl<T: ToTokens> ToTokens for &T {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        (*self).to_tokens(tokens);
    }
}

impl<T: ToTokens> ToTokens for Box<T> {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        (**self).to_tokens(tokens);
    }
}

impl ToTokens for TokenStream {
    /// This allows composing `TokenStream`s before re-parsing them.
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        tokens.extend(self.tokens.clone());
    }
}

/// A smart `Token` writer that knows when to insert whitespace to prevent
/// two adjact tokens from being combined into a single token. This can also
/// be used to write raw strings, which we do when emitting non-BigQuery SQL.
pub struct TokenWriter<'wtr> {
    wtr: &'wtr mut dyn io::Write,
    last_char: Option<char>,
    treat_next_write_as_safe: bool,
}

impl<'wtr> TokenWriter<'wtr> {
    /// Create a new `TokenWriter`.
    pub fn from_wtr(wtr: &'wtr mut dyn io::Write) -> Self {
        Self {
            wtr,
            last_char: None,
            treat_next_write_as_safe: false,
        }
    }

    /// Mark the start of a tokens.
    pub fn mark_token_start(&mut self) {
        self.treat_next_write_as_safe = true;
    }

    /// Write a token, being careful not to let it merge with the previous
    /// token.
    pub fn write_token(&mut self, t: &Token) -> io::Result<()> {
        self.write_raw_token(t.raw())
    }

    /// Write a raw token, being careful not to let it merge with the previous
    /// token.
    fn write_raw_token(&mut self, t: &RawToken) -> io::Result<()> {
        // Write leading whitespace. Any whitespace automatically clears
        // `last_char`.
        let leading_whitespace = t.leading_whitespace();
        if leading_whitespace.is_empty() {
            self.treat_next_write_as_safe = true;
        } else {
            self.write_raw_str(leading_whitespace)?;
            self.last_char = None;
            self.treat_next_write_as_safe = false;
        }

        // Write a string, being careful not to let any tokens it contains
        // merge with any previous token.
        self.write_raw_str(t.as_str())?;

        // Write trailing whitespace. Any whitespace automatically clears
        // `last_char`.
        let trailing_whitespace = t.trailing_whitespace();
        if !trailing_whitespace.is_empty() {
            self.write_raw_str(trailing_whitespace)?;
            self.last_char = None;
            self.treat_next_write_as_safe = false;
        }

        Ok(())
    }

    /// Write a string, being careful not to let any tokens it contains merge
    /// with any previous token. This is a bit tricky, because we need to
    /// support any dialect of SQL that we known how to emit, not just BigQuery.
    /// So we're conservative, and we insert whitespace unless we're pretty
    /// sure it's not needed.
    pub fn write_token_start(&mut self, s: &str) -> io::Result<()> {
        self.treat_next_write_as_safe = true;
        self.write_raw_str(s)
    }

    /// Write a string without checking for token merging.
    fn write_raw_str(&mut self, s: &str) -> io::Result<()> {
        self.write_all(s.as_bytes())
    }
}

impl<'wtr> io::Write for TokenWriter<'wtr> {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        // We're abusing the `Write` trait here, because we're imposing extra
        // restrictions on the `write` method.
        //
        // This is because we don't want to try to keep track of `last_char` in
        // the presence of partially-written UTF-8 characters.
        panic!("all writes must go through `write_all` for now");
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // Get the string we're trying to write.
        let str_to_write = from_utf8(buf)
            .map_err(|_| io::ErrorKind::InvalidData)?
            .to_string();

        // Insert a space to separate tokens if needed.
        if self.treat_next_write_as_safe {
            if let Some(last_char) = self.last_char {
                if let Some(first_char) = str_to_write.chars().next() {
                    self.treat_next_write_as_safe = false;
                    if tokens_might_merge(last_char, first_char) {
                        self.wtr.write_all(b" ")?;
                    }
                }
            }
        }

        // Do the actual write.
        self.wtr.write_all(buf)?;
        if let Some(last_char) = str_to_write.chars().last() {
            self.last_char = Some(last_char);
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.wtr.flush()
    }
}

// Returns true if the two characters might merge into a single token.
fn tokens_might_merge(c1: char, c2: char) -> bool {
    if never_merges(c1) || never_merges(c2) {
        return false;
    }
    let c1_is_ident = c1.is_ascii_alphanumeric() || c1 == '_';
    let c2_is_ident = c2.is_ascii_alphanumeric() || c2 == '_';
    match (c1_is_ident, c2_is_ident) {
        (true, true) => return true,
        (true, false) => return false,
        (false, true) => return false,
        (false, false) => {}
    }

    // Special cases.
    #[allow(clippy::match_like_matches_macro)]
    match (c1, c2) {
        // There's an unfortunate lexing ambiguity between `['>', '>']` and
        // `['>>']`. The latter is the bitwize right shift operator, while the
        // former appears in types like `ARRAY<STRUCT<...>>`. We want to
        // represent this as two tokens, and then allow the parser to
        // reconstruct `'>>'`. But we need to be careful not to add whitespace
        // here, and not just for aesthetic reasons.
        ('>', '>') => false,
        _ => true,
    }
}

/// Returns true if the character never merges with any other character.
fn never_merges(c: char) -> bool {
    // Be very conservative here.
    c.is_ascii_whitespace() || matches!(c, '(' | ')' | '[' | ']' | ',' | '.' | ';')
}

/// Convert `sql` into a series of tokens.
pub fn tokenize_sql(filename: &Path, sql: &str) -> Result<TokenStream> {
    let mut files = SimpleFiles::new();
    let file_id = files.add(filename.to_string_lossy().into_owned(), sql.to_string());
    match lexer::tokens(sql, file_id) {
        Ok(tokens) => Ok(TokenStream { tokens }),
        Err(err) => {
            let diagnostic = Diagnostic::error()
                .with_message("Failed to tokenize query")
                .with_labels(vec![Label::primary(
                    file_id,
                    err.location.offset..err.location.offset + 1,
                )
                .with_message(format!("expected {}", err.expected))]);
            Err(SourceError {
                expected: err.to_string(),
                files,
                diagnostic,
            }
            .into())
        }
    }
}

peg::parser! {
    grammar lexer(file_id: usize) for str {
        pub rule tokens() -> Vec<Token>
            = leading_ws:whitespace_only() tokens:(token()*) {
                if tokens.is_empty() {
                    vec![Token::EmptyFile(EmptyFile { token: leading_ws })]
                } else {
                    // Merge leading whitespace with the first token.
                    let mut tokens = tokens;
                    tokens[0].raw_mut().prepend_whitespace(leading_ws.as_raw_str());
                    tokens
                }
            }

        pub rule token() -> Token
            = literal:literal() { Token::Literal(literal) }
            / ident:ident() { Token::Ident(ident) }
            / punct:punct() { Token::Punct(punct) }

        rule whitespace_only() -> RawToken
            = s:position!() ws:$(_) e:position!() {
                RawToken {
                    raw: ws.to_string(),
                    token_range: 0..0,
                    source_span: Some(Loc { file_id, offset: s }..Loc { file_id, offset: e }),
                }
            }

        rule ident() -> Ident
            = name_and_token:t(<c_ident()>) {
                let (name, token) = name_and_token;
                Ident { token, name }
            }
            / name_and_token:t(<"`" name:(([^ '\\' | '`'] / escape())*) "`" { name }>) {
                let (name, token) = name_and_token;
                Ident { token, name: name.into_iter().collect() }
            }

        /// Low-level rule for matching a C-style identifier.
        rule c_ident() -> String
            = quiet! { id:$(c_ident_start() c_ident_cont()*)
              // The next character cannot be a valid ident character.
              !c_ident_cont()
              { id.to_string() } }
            / expected!("identifier")
        rule c_ident_start() = ['a'..='z' | 'A'..='Z' | '_']
        rule c_ident_cont() = ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']

        rule literal() -> Literal
            = quiet! { nothing_and_token:t(<"-"? ['0'..='9']+ "." ['0'..='9']*>) {
                let (_, token) = nothing_and_token;
                let value = LiteralValue::Float64(token.as_str().parse().unwrap());
                Literal { token, value }
            } }
            / quiet! { nothing_and_token:t(<"-"? ['0'..='9']+>) {
                let (_, token) = nothing_and_token;
                let value = LiteralValue::Int64(token.as_str().parse().unwrap());
                Literal { token, value }
            } }
            / quiet! { s_and_token:t(<"'" s:(([^ '\\' | '\''] / escape())*) "'" { s }>) {
                let (s, token) = s_and_token;
                let value = LiteralValue::String(s.into_iter().collect());
                Literal { token, value }
            } }
            / quiet! { s_and_token:t(<"r'" s:[^ '\'']* "'" { s }>) {
                let (s, token) = s_and_token;
                let value = LiteralValue::String(s.into_iter().collect());
                Literal { token, value }
            } }
            / quiet! { s_and_token:t(<"r\"" s:[^ '"']* "\""{ s }>) {
                let (s, token) = s_and_token;
                let value = LiteralValue::String(s.into_iter().collect());
                Literal { token, value }
            } }

        rule punct() -> Punct
            // The most basic punctuation tokens.
            = p:t(<"(">) { Punct { token: p.1 } }
            / p:t(<")">) { Punct { token: p.1 } }
            / p:t(<"[">) { Punct { token: p.1 } }
            / p:t(<"]">) { Punct { token: p.1 } }
            / p:t(<",">) { Punct { token: p.1 } }
            / p:t(<".">) { Punct { token: p.1 } }
            / p:t(<";">) { Punct { token: p.1 } }
            // Other punctuation tokens. When multiple tokens start with the
            // same first character(s), we need to list them in order of longest
            // to shortest.
            / p:t(<"=">) { Punct { token: p.1 } }
            / p:t(<"!=">) { Punct { token: p.1 } }
            / p:t(<"<=">) { Punct { token: p.1 } }
            / p:t(<"<">) { Punct { token: p.1 } }
            / p:t(<">=">) { Punct { token: p.1 } }
            / p:t(<">">) { Punct { token: p.1 } }
            / p:t(<"+">) { Punct { token: p.1 } }
            / p:t(<"-">) { Punct { token: p.1 } }
            / p:t(<"*">) { Punct { token: p.1 } }
            / p:t(<"/">) { Punct { token: p.1 } }

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

        /// Complex tokens matching a grammar rule.
        rule t<T>(r: rule<T>) -> (T, RawToken)
            = s:position!() parsed_and_slice:with_slice(<r()>) ws:$(_) e:position!() {
                let (parsed, slice) = parsed_and_slice;
                let ws_offset = slice.len();
                (parsed, RawToken {
                    raw: format!("{}{}", slice, ws),
                    token_range: 0..slice.len(),
                    source_span: Some(Loc { file_id, offset: s }..Loc { file_id, offset: e }),
                })
            }

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

        /// Return both the value and slice matched by the rule. See
        /// https://github.com/kevinmehall/rust-peg/issues/283.
        rule with_slice<T>(r: rule<T>) -> (T, &'input str)
            = value:&r() input:$(r()) { (value, input) }
    }
}

#[cfg(test)]
mod test {
    use joinery_macros::sql_quote;

    use super::*;

    #[test]
    fn tokenize_sql_tests() {
        // Get the path to `tests/sql`.
        let tests_sql_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sql");
        let tests_sql_str = tests_sql_path
            .to_str()
            .expect("tests/sql path is not valid UTF-8");

        // Use `glob` to find all the `.sql` files in `tests/sql`.
        for entry in
            glob::glob(&format!("{}/**/*.sql", tests_sql_str)).expect("failed to read glob pattern")
        {
            let path = entry.expect("failed to read glob entry");
            println!("Tokenizing {:?}", path);

            // Read the file.
            let sql = std::fs::read_to_string(&path).expect("failed to read SQL test file");

            // Tokenize it.
            let tokens = match tokenize_sql(&path, &sql) {
                Ok(tokens) => tokens,
                Err(err) => {
                    err.emit();
                    panic!("failed to tokenize SQL test file");
                }
            };

            // Reconstruct the original text from the tokens.
            let mut reconstructed = String::new();
            for token in &tokens.tokens {
                reconstructed.push_str(token.raw().as_raw_str());
            }

            // Compare the reconstructed text to the original text.
            assert_eq!(sql, reconstructed, "failed to reproduce input text");

            // Now test TokenWriter by stripping all the whitespace from each
            // token, outputting them, tokenizing them again, and comparing the
            // result to the original tokens.
            let mut buf = vec![];
            let mut wtr = TokenWriter::from_wtr(&mut buf);
            for token in &tokens.tokens {
                let stripped = token.raw().without_ws();
                wtr.write_raw_token(&stripped)
                    .expect("failed to write token");
            }
            let stripped_sql = from_utf8(&buf).unwrap();
            let stripped_tokens = match tokenize_sql(&path, stripped_sql) {
                Ok(tokens) => tokens,
                Err(err) => {
                    err.emit();
                    panic!("failed to tokenize stripped SQL test file");
                }
            };
            assert_eq!(
                tokens, stripped_tokens,
                "stripping whitespace and reparising did not produce the same tokens"
            );
        }
    }

    #[derive(Debug, PartialEq)]
    pub enum SelectItem {
        All(Punct),
        Col(Ident),
    }

    #[derive(Debug, PartialEq)]
    pub struct Query {
        select_token: Keyword,
        item: SelectItem,
        from_token: Keyword,
        table: Ident,
    }

    // An extremely simplified SQL grammar for testing purposes.
    peg::parser! {
        grammar test_parser() for TokenStream {
            pub rule query() -> Query
                = select_token:k("SELECT") item:select_item() from_token:k("FROM") table:ident() {
                    Query { select_token, item, from_token, table }
                }

            rule select_item() -> SelectItem
                = all_token:p("*") { SelectItem::All(all_token) }
                / ident:ident() { SelectItem::Col(ident) }

            // An identifier. We use the undocumented `##` to call a method on
            // `TokenStream`.
            rule ident() -> Ident
                = ident:##ident() { ident }
                / expected!("identifier")

            // Get the next keyword from the stream.
            rule k(s: &'static str) -> Keyword
                = token:##keyword(s) { token }
                / expected!(s)

            // Get the next punctuation from the stream.
            rule p(s: &'static str) -> Punct
                = token:##punct_eq(s) { token }
                / expected!(s)
        }
    }

    #[test]
    fn toy_parser_operates_on_token_stream() {
        let sql = "select * from t";
        let tokens = tokenize_sql(Path::new("test.sql"), sql).unwrap();
        println!("==== PARSING ====\n{:?}\n====", tokens);
        let parsed = test_parser::query(&tokens).unwrap();
        let expected = Query {
            select_token: Keyword::new("SELECT"),
            item: SelectItem::All(Punct::new("*")),
            from_token: Keyword::new("FROM"),
            table: Ident::new("t"),
        };
        assert_eq!(parsed, expected);
    }

    #[test]
    fn sql_quote_and_try_into_statement() {
        let optional_distinct = Some(sql_quote! { DISTINCT });
        let statement = sql_quote! {
            SELECT #optional_distinct
                generate_uuid() AS id,
                "hello" AS message,
                1 AS n,
                1.0 AS x,
                true AS t,
                false AS f,
        }
        .try_into_statement()
        .unwrap();
        assert!(matches!(statement, ast::Statement::Query(_)));
    }

    #[test]
    fn expression_rewriting() {
        let if_expr = sql_quote! { IF(TRUE, 2.0, 1.0) }
            .try_into_expression()
            .unwrap();
        if let ast::Expression::If {
            condition,
            then_expression,
            else_expression,
            ..
        } = &if_expr
        {
            let case_expr =
                sql_quote! { CASE WHEN #condition THEN #then_expression ELSE #else_expression END }
                    .try_into_expression()
                    .unwrap();
            assert!(matches!(case_expr, ast::Expression::Case { .. }));
        } else {
            panic!("expected IF expression");
        }
    }
}
