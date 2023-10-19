//! The inevitable grab-bag of utility functions.

use std::fmt;

/// Is `s` a valid C identifier?
pub fn is_c_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        None => false,
        Some(c) if !c.is_ascii_alphabetic() && c != '_' => false,
        _ => chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
    }
}

/// Format a single- or double-quoted string for use in a SQLite3 query. SQLite3
/// does not support backslash escapes.
fn ansi_quote_fmt(s: &str, quote_char: char, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", quote_char)?;
    for c in s.chars() {
        if c == quote_char {
            write!(f, "{}", quote_char)?;
        }
        write!(f, "{}", c)?;
    }
    write!(f, "{}", quote_char)
}

/// Formatting wrapper for single-quoted strings. It's actually pretty rare for
/// databases to support just this format with no backslash escapes, so please
/// double-check things like `'\\'`` and `'\''` before using this.
pub struct AnsiString<'a>(pub &'a str);

impl fmt::Display for AnsiString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ansi_quote_fmt(self.0, '\'', f)
    }
}

/// Formatting wrapper for double-quoted identifiers.
pub struct AnsiIdent<'a>(pub &'a str);

impl fmt::Display for AnsiIdent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ansi_quote_fmt(self.0, '"', f)
    }
}
