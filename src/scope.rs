//! Namespace for SQL.

// This is work in progress.
#![allow(dead_code)]

use std::{collections::BTreeMap, fmt, hash, sync::Arc};

use crate::{
    drivers::bigquery::BigQueryName,
    errors::{format_err, Error, Result},
    known_files::KnownFiles,
    tokenizer::{Ident, Span, Spanned, ToTokens, Token},
    types::{parse_function_decls, Type},
    util::is_c_ident,
};

/// A case-insensitive identifier.
#[derive(Clone, Eq)]
pub struct CaseInsensitiveIdent {
    ident: Ident,
    cmp_key: String,
}

impl CaseInsensitiveIdent {
    /// Create a new case-insensitive identifier.
    pub fn new(name: &str, span: Span) -> Self {
        Ident::new(name, span).into()
    }

    /// Get the underlying identifier.
    pub fn ident(&self) -> &Ident {
        &self.ident
    }
}

impl From<Ident> for CaseInsensitiveIdent {
    fn from(ident: Ident) -> Self {
        Self {
            // We actually want ASCII lowercase, not Unicode lowercase, because
            // BigQuery appears to consider `é` and `É` to be different
            // characters. I checked using the UI.
            cmp_key: ident.name.to_ascii_lowercase(),
            ident,
        }
    }
}

impl PartialEq for CaseInsensitiveIdent {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_key == other.cmp_key
    }
}

impl Ord for CaseInsensitiveIdent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_key.cmp(&other.cmp_key)
    }
}

impl PartialOrd for CaseInsensitiveIdent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl hash::Hash for CaseInsensitiveIdent {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.cmp_key.hash(state);
    }
}

impl fmt::Debug for CaseInsensitiveIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.ident)
    }
}

impl fmt::Display for CaseInsensitiveIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if is_c_ident(&self.ident.name) {
            write!(f, "{}", self.ident.name)
        } else {
            write!(f, "{}", BigQueryName(&self.ident.name))
        }
    }
}

impl ToTokens for CaseInsensitiveIdent {
    fn to_tokens(&self, tokens: &mut Vec<Token>) {
        self.ident.to_tokens(tokens)
    }
}

impl Spanned for CaseInsensitiveIdent {
    fn span(&self) -> Span {
        self.ident.span()
    }
}

/// A value we can store in a scope. Details may change.
pub type ScopeValue = Type;

/// We need to both define and hide names in a scope.
#[derive(Clone, Debug)]
enum ScopeEntry {
    /// A name that is defined in this scope.
    Defined(ScopeValue),
    /// A name that is hidden in this scope.
    Hidden,
}

/// A handle to a scope.
pub type ScopeHandle = Arc<Scope>;

/// A scope is a namespace for SQL code. We use it to look up names,
/// and associate them with types and other information.
#[derive(Clone, Debug)]
pub struct Scope {
    /// The parent scope.
    parent: Option<ScopeHandle>,

    /// The names in this scope.
    names: BTreeMap<CaseInsensitiveIdent, ScopeEntry>,
}

impl Scope {
    /// Create a new scope with no parent.
    pub fn root() -> ScopeHandle {
        let mut scope = Self {
            parent: None,
            names: BTreeMap::new(),
        };

        // Add built-in functions. We use a local `known_files` here since we
        // never return any parse errors, and our caller doesn't need to know we
        // parse at all.
        let mut files = KnownFiles::new();
        let file_id = files.add_string("built-in functions", BUILT_IN_FUNCTIONS);
        let built_ins = match parse_function_decls(&files, file_id) {
            Ok(built_ins) => built_ins,
            Err(err) => {
                err.emit(&files);
                panic!("built-in function parse error");
            }
        };
        for (name, ty) in built_ins {
            scope
                .add(CaseInsensitiveIdent::from(name), Type::Function(ty))
                .expect("duplicate built-in function");
        }

        Arc::new(scope)
    }

    /// Create a new scope.
    pub fn new(parent: &ScopeHandle) -> Scope {
        Self {
            parent: Some(parent.clone()),
            names: BTreeMap::new(),
        }
    }

    /// Freeze this scope and get a handle.
    ///
    /// You cannot do this once you have called [`Self::into_handle()`].
    pub fn into_handle(self) -> ScopeHandle {
        Arc::new(self)
    }

    /// Add a new value to the scope.
    pub fn add(&mut self, name: CaseInsensitiveIdent, value: ScopeValue) -> Result<()> {
        if self.names.contains_key(&name) {
            // We don't try to do anything fancy like replacing a hidden name
            // with a defined name, because we probably don't need that.
            return Err(format_err!("duplicate name: {}", name));
        }
        self.names.insert(name, ScopeEntry::Defined(value));
        Ok(())
    }

    /// "Drop" a value from the scope by hiding it. This is used to implement
    /// `DROP TABLE`, etc.
    ///
    /// You cannot do this once you have called [`Self::into_handle()`].
    pub fn hide(&mut self, name: &CaseInsensitiveIdent) -> Result<()> {
        if self.names.contains_key(name) {
            // We do not allow hiding a name that was defined in this scope.
            // Make a new scope for that.
            return Err(format_err!(
                "cannot hide name {} because it was defined in this scope",
                name
            ));
        }
        self.names.insert(name.clone(), ScopeEntry::Hidden);
        Ok(())
    }

    /// Get a value from the scope.
    pub fn get<'scope>(&'scope self, name: &CaseInsensitiveIdent) -> Option<&'scope ScopeValue> {
        match self.names.get(name) {
            Some(ScopeEntry::Defined(value)) => Some(value),
            Some(ScopeEntry::Hidden) => None,
            None => {
                if let Some(parent) = self.parent.as_ref() {
                    parent.get(name)
                } else {
                    None
                }
            }
        }
    }

    /// Get a value, or return an error if it is not defined.
    pub fn get_or_err(&self, name: &CaseInsensitiveIdent) -> Result<&ScopeValue> {
        self.get(name).ok_or_else(|| {
            Error::annotated(
                format!("unknown name: {}", name),
                name.ident.span(),
                "not defined",
            )
        })
    }
}

/// Built-in function declarations in the default scope.
static BUILT_IN_FUNCTIONS: &str = "
ANY_VALUE = FnAgg<?T>(Agg<?T>) -> ?T;
ARRAY_LENGTH = Fn<?T>(ARRAY<?T>) -> INT64;
ARRAY_TO_STRING = Fn<?T>(ARRAY<?T>, STRING) -> STRING;
COALESCE = Fn<?T>(?T, ..?T) -> ?T;
CONCAT = Fn(STRING, ..STRING) -> STRING | Fn(BYTES, ..BYTES) -> BYTES;
COUNTIF = FnAgg(Agg<BOOL>) -> INT64 | FnOver(Agg<BOOL>) -> INT64;
CURRENT_DATETIME = Fn() -> DATETIME;
DATE = Fn(STRING) -> DATE;
DATE_ADD = Fn(DATE, INTERVAL) -> DATE;
DATE_DIFF = Fn(DATE, DATE, DATEPART) -> INT64;
DATE_SUB = Fn(DATE, INTERVAL) -> DATE;
DATE_TRUNC = Fn(DATE, DATEPART) -> DATE;
DATETIME = Fn(DATE) -> DATETIME;
DATETIME_DIFF = Fn(DATETIME, DATETIME, DATEPART) -> INT64;
DATETIME_SUB = Fn(DATETIME, INTERVAL) -> DATETIME;
DATETIME_TRUNC = Fn(DATETIME, DATEPART) -> DATETIME;
EXP = Fn(FLOAT64) -> FLOAT64;
FARM_FINGERPRINT = Fn(STRING) -> INT64 | Fn(BYTES) -> INT64;
FIRST_VALUE = FnOver<?T>(Agg<?T>) -> ?T;
FORMAT_DATETIME = Fn(DATETIME, STRING) -> STRING;
GENERATE_DATE_ARRAY = Fn(DATE, DATE, INTERVAL) -> ARRAY<DATE>;
GENERATE_UUID = Fn() -> STRING;
GREATEST = Fn<?T>(?T, ..?T) -> ?T;
LAG = FnOver<?T>(Agg<?T>) -> ?T;
LEAST = Fn<?T>(?T, ..?T) -> ?T;
LENGTH = Fn(STRING) -> INT64 | Fn(BYTES) -> INT64;
LOWER = Fn(STRING) -> STRING;
MAX = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
MIN = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
RAND = Fn() -> FLOAT64;
RANK = FnOver() -> INT64;
REGEXP_EXTRACT = Fn(STRING, STRING) -> STRING;
REGEXP_REPLACE = Fn(STRING, STRING, STRING) -> STRING;
ROW_NUMBER = FnOver() -> INT64;
SHA256 = Fn(STRING) -> BYTES;
SUM = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64
    | FnOver(Agg<INT64>) -> INT64 | FnOver(Agg<FLOAT64>) -> FLOAT64;
TO_HEX = Fn(BYTES) -> STRING;
TRIM = Fn(STRING) -> STRING;
UPPER = Fn(STRING) -> STRING;
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_built_in_functions() {
        Scope::root();
    }
}
