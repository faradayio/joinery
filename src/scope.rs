//! Namespace for SQL.

// This is work in progress.
#![allow(dead_code)]

use std::{collections::BTreeMap, fmt, hash, sync::Arc};

use crate::{
    drivers::bigquery::BigQueryName,
    errors::{format_err, Result},
    tokenizer::Ident,
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
    pub fn new(ident: Ident) -> Self {
        Self {
            cmp_key: ident.name.to_ascii_lowercase(),
            ident,
        }
    }

    /// Get the underlying identifier.
    pub fn ident(&self) -> &Ident {
        &self.ident
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

/// A value we can store in a scope. Details may change.
type ScopeValue = Type;

/// A scope is a namespace for SQL code. We use it to look up names,
/// and associate them with types and other information.
#[derive(Clone, Debug)]
pub struct Scope {
    /// The parent scope.
    parent: Option<Arc<Scope>>,

    /// The names in this scope.
    names: BTreeMap<CaseInsensitiveIdent, ScopeValue>,
}

impl Scope {
    /// Create a new scope with no parent.
    pub fn root() -> Self {
        let mut scope = Self::new(None);

        // Add built-in functions.
        let built_ins = match parse_function_decls(BUILT_IN_FUNCTIONS) {
            Ok(built_ins) => built_ins,
            Err(err) => {
                err.emit();
                panic!("built-in function parse error");
            }
        };
        for (name, ty) in built_ins {
            scope
                .add(CaseInsensitiveIdent::new(name), Type::Function(ty))
                .expect("duplicate built-in function");
        }

        scope
    }

    /// Create a new scope.
    pub fn new(parent: Option<Arc<Scope>>) -> Self {
        Self {
            parent,
            names: BTreeMap::new(),
        }
    }

    /// Add a new value to the scope.
    pub fn add(&mut self, name: CaseInsensitiveIdent, value: ScopeValue) -> Result<()> {
        if self.names.contains_key(&name) {
            return Err(format_err!("duplicate name: {}", name));
        }
        self.names.insert(name, value);
        Ok(())
    }

    /// Get a value from the scope.
    pub fn get(&self, name: &CaseInsensitiveIdent) -> Option<&ScopeValue> {
        self.names
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|parent| parent.get(name)))
    }
}

/// Built-in function declarations in the default scope.
static BUILT_IN_FUNCTIONS: &str = "
ANY_VALUE = FnAgg<?T>(?T) -> ?T;
ARRAY_LENGTH = Fn<?T>(ARRAY<?T>) -> INT64;
ARRAY_TO_STRING = Fn<?T>(ARRAY<?T>, STRING) -> STRING;
COALESCE = Fn<?T>(?T, ..?T) -> ?T;
CONCAT = Fn(STRING, ..STRING) -> STRING | Fn(BYTES, ..BYTES) -> BYTES;
COUNTIF = FnAgg(BOOL) -> INT64 | FnOver(BOOL) -> INT64;
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
FIRST_VALUE = FnOver<?T>(?T) -> ?T;
FORMAT_DATETIME = Fn(DATETIME, STRING) -> STRING;
GENERATE_DATE_ARRAY = Fn(DATE, DATE, INTERVAL) -> ARRAY<DATE>;
GENERATE_UUID = Fn() -> STRING;
LAG = FnOver<?T>(?T) -> ?T;
LEAST = Fn<?T>(?T, ..?T) -> ?T;
LENGTH = Fn(STRING) -> INT64 | Fn(BYTES) -> INT64;
LOWER = Fn(STRING) -> STRING;
MAX = FnAgg(INT64) -> INT64 | FnAgg(FLOAT64) -> FLOAT64;
MIN = FnAgg(INT64) -> INT64 | FnAgg(FLOAT64) -> FLOAT64;
RAND = Fn() -> FLOAT64;
RANK = FnOver() -> INT64;
REGEXP_EXTRACT = Fn(STRING, STRING) -> STRING;
REGEXP_REPLACE = Fn(STRING, STRING, STRING) -> STRING;
ROW_NUMBER = FnOver() -> INT64;
SHA256 = Fn(STRING) -> BYTES;
SUM = FnAgg(INT64) -> INT64 | FnAgg(FLOAT64) -> FLOAT64 | FnOver(INT64) -> INT64 | FnOver(FLOAT64) -> FLOAT64;
TO_HEX = Fn(BYTES) -> STRING;
TRIM = Fn(STRING) -> STRING;
UPPER = Fn(STRING) -> STRING;
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_built_in_functions() {
        if let Err(err) = parse_function_decls(BUILT_IN_FUNCTIONS) {
            err.emit();
            panic!("parse error");
        }
    }
}
