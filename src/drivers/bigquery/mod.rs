//! We will eventually put a BigQuery driver here, but for now we only have
//! supporting functions.

use std::fmt;

/// Quote `s` for BigQuery, surrounding it with `quote_char` and escaping
/// special characters.
fn bigquery_quote_fmt(s: &str, quote_char: char, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", quote_char)?;
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
    write!(f, "{}", quote_char)
}

/// Formatting wrapper for strings quoted with single quotes.
pub struct BigQueryString<'a>(pub &'a str);

impl fmt::Display for BigQueryString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        bigquery_quote_fmt(self.0, '\'', f)
    }
}

/// Formatting wrapper for identifiers, table names, etc., quoted with
/// backticks.
pub struct BigQueryName<'a>(pub &'a str);

impl fmt::Display for BigQueryName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        bigquery_quote_fmt(self.0, '`', f)
    }
}
