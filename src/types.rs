//! A basic type system for BigQuery-compatible SQL.
//!
//! We support most basic BigQuery types, including arrays and structs. We also
//! have representations for table types and function types. Function types may
//! have type variables, which we use to represent things like `Fn<T>(ARRAY<T>,
//! INT64) -> T`.
//!
//! Function types may be overloaded, to support things like addition:
//! `Fn(INT64, INT64) -> INT64 | Fn(FLOAT64, FLOAT64) -> FLOAT64`.
//!
//! We try to get some of the fiddly details right, like the fact that BigQuery
//! does not support `ARRAY<ARRAY<T>>`, only `ARRAY<STRUCT<ARRAY<T>>>`, and that
//! `STRUCT` fields have optional names.

// Work in progress.
#![allow(dead_code)]

use std::fmt;

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
};

use crate::{
    drivers::bigquery::BigQueryName,
    errors::{format_err, Result, SourceError},
    tokenizer::{Ident, Span},
    util::is_c_ident,
};

/// Sometimes we want concrete types, and sometimes we want types with type
/// variables. This trait convers both those cases.
pub trait TypeVarSupport: fmt::Display {}

/// This type can never be instantiated. We use this to represent a type variable with
/// all type variables resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedTypeVarsOnly {}

impl TypeVarSupport for ResolvedTypeVarsOnly {}

impl fmt::Display for ResolvedTypeVarsOnly {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unreachable!()
    }
}

/// A type variable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeVar {
    name: String,
}

impl TypeVar {
    /// Create a new type variable.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Self::new_helper(name.into())
    }

    /// Helper for `new` that has no type parameter, and therefore only gets
    /// compiled once.
    fn new_helper(name: String) -> Result<Self> {
        if !is_c_ident(&name) {
            return Err(format_err!("invalid type variable name: {}", name));
        }
        if !name.chars().next().unwrap().is_ascii_uppercase() {
            return Err(format_err!(
                "type variable name must start with an uppercase letter: {}",
                name
            ));
        }
        Ok(Self { name })
    }
}

impl TypeVarSupport for TypeVar {}

impl fmt::Display for TypeVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.name)?;
        Ok(())
    }
}

/// Basic types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    /// A value which can be stored in a column.
    Value(ValueType<TV>),
    /// The type of a table, as seen in `CREATE TABLE` statements, or
    /// as returned from a sub-`SELECT`, or as passed to `UNNEST`.
    Table(TableType<TV>),
    /// A function type.
    Function(FunctionType),
}

impl<TV: TypeVarSupport> fmt::Display for Type<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Value(v) => write!(f, "{}", v),
            Type::Table(t) => write!(f, "{}", t),
            Type::Function(func) => write!(f, "{}", func),
        }
    }
}

/// A value type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    /// Any value type which may appear in an array.
    Simple(SimpleType<TV>),

    /// An array of values. BigQuery does not support nesting arrays, although
    /// you can have `ARRAY<STRUCT<ARRAY<T>>>` if you wish.
    Array(Box<SimpleType<TV>>),
}

impl<TV: TypeVarSupport> fmt::Display for ValueType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Simple(s) => write!(f, "{}", s),
            ValueType::Array(t) => write!(f, "ARRAY<{}>", t),
        }
    }
}

/// A simple type, which can appear in an array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimpleType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    Bool,
    Bytes,
    Date,
    /// This type can be used a function argument, but does not exist as an
    /// actual value, as far as I can tell. It's part of the grammar for
    /// `DATE_TRUNC`, `DATE_DIFF`, etc. It takes "values" like `DAY` and `YEAR`.
    Datepart,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Numeric,
    String,
    Time,
    Timestamp,
    Struct(StructType<TV>),
    Parameter(TV),
}

impl<TV: TypeVarSupport> fmt::Display for SimpleType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimpleType::Bool => write!(f, "BOOL"),
            SimpleType::Bytes => write!(f, "BYTES"),
            SimpleType::Date => write!(f, "DATE"),
            SimpleType::Datepart => write!(f, "DATEPART"),
            SimpleType::Datetime => write!(f, "DATETIME"),
            SimpleType::Float64 => write!(f, "FLOAT64"),
            SimpleType::Geography => write!(f, "GEOGRAPHY"),
            SimpleType::Int64 => write!(f, "INT64"),
            SimpleType::Interval => write!(f, "INTERVAL"),
            SimpleType::Numeric => write!(f, "NUMERIC"),
            SimpleType::String => write!(f, "STRING"),
            SimpleType::Time => write!(f, "TIME"),
            SimpleType::Timestamp => write!(f, "TIMESTAMP"),
            SimpleType::Struct(s) => write!(f, "{}", s),
            SimpleType::Parameter(t) => write!(f, "{}", t),
        }
    }
}

/// A struct type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    pub fields: Vec<StructElementType<TV>>,
}

impl<TV: TypeVarSupport> fmt::Display for StructType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "STRUCT<")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, ">")?;
        Ok(())
    }
}

/// A struct element type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructElementType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    pub name: Option<Ident>,
    pub ty: ValueType<TV>,
}

impl<TV: TypeVarSupport> fmt::Display for StructElementType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", BigQueryName(&name.name))?;
        }
        write!(f, "{}", self.ty)?;
        Ok(())
    }
}

/// A table type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    pub columns: Vec<ColumnType<TV>>,
}

impl<TV: TypeVarSupport> fmt::Display for TableType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TABLE<")?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", column)?;
        }
        write!(f, ">")?;
        Ok(())
    }
}

/// A column type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    pub name: Ident,
    pub ty: ValueType<TV>,
    pub not_null: bool,
}

impl<TV: TypeVarSupport> fmt::Display for ColumnType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", BigQueryName(&self.name.name), self.ty)?;
        if self.not_null {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

/// A function type. Note that functions may have multiple signatures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionType {
    pub signatures: Vec<FunctionSignature>,
}

impl fmt::Display for FunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, sig) in self.signatures.iter().enumerate() {
            if i > 0 {
                write!(f, " | ")?;
            }
            write!(f, "{}", sig)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionSignatureType {
    Scalar,
    Aggregate,
    Window,
}

impl fmt::Display for FunctionSignatureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionSignatureType::Scalar => write!(f, "Fn"),
            FunctionSignatureType::Aggregate => write!(f, "FnAgg"),
            FunctionSignatureType::Window => write!(f, "FnOver"),
        }
    }
}

/// A function signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionSignature {
    pub sig_type: FunctionSignatureType,
    pub type_vars: Vec<TypeVar>,
    pub params: Vec<ValueType<TypeVar>>,
    pub rest_params: Option<ValueType<TypeVar>>,
    pub return_type: ValueType<TypeVar>,
}

impl fmt::Display for FunctionSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sig_type)?;
        if !self.type_vars.is_empty() {
            write!(f, "<")?;
            for (i, type_var) in self.type_vars.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", type_var)?;
            }
            write!(f, ">")?;
        }
        write!(f, "(")?;
        for (i, param) in self.params.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", param)?;
        }
        if let Some(rest_params) = &self.rest_params {
            if !self.params.is_empty() {
                write!(f, ", ")?;
            }
            write!(f, "..{}", rest_params)?;
        }
        write!(f, ") -> {}", self.return_type)
    }
}

pub fn parse_function_decls(s: &str) -> Result<Vec<(Ident, FunctionType)>> {
    match type_grammar::function_decls(s) {
        Ok(decls) => Ok(decls),
        Err(e) => {
            let mut files = SimpleFiles::new();
            let file_id = files.add("function_decls".to_owned(), s.to_owned());
            let diagnostic = Diagnostic::error()
                .with_message("parse error")
                .with_labels(vec![Label::primary(
                    file_id,
                    e.location.offset..e.location.offset,
                )
                .with_message(format!("expected {}", e.expected))]);
            Err(SourceError {
                expected: e.expected.to_string(),
                files,
                diagnostic,
            }
            .into())
        }
    }
}

// A `peg` grammar for internal use only, used to define our built-in functions.
//
// This is much more of a "classic" parser than our main SQL parser. It throws
// away whitespace, and doesn't try to record source spans.
peg::parser! {
    grammar type_grammar() for str {

        pub rule function_decls() -> Vec<(Ident, FunctionType)>
            = _? decls:(function_decl() ** (_? ";" _?)) _? (";" _?)? {
                decls
            }

        rule function_decl() -> (Ident, FunctionType)
            = name:ident() _? "=" _? ty:function_type() {
                (name, ty)
            }

        rule function_type() -> FunctionType
            = signatures:(function_signature() ** (_? "|" _?)) {
                FunctionType { signatures }
            }

        rule function_signature() -> FunctionSignature
            = sig_type:function_signature_type()
              type_vars:type_vars()
              "(" _? params:function_params() _? ")" _?
              "->" _? return_type:value_type()
            {
                let (params, rest_params) = params;
                FunctionSignature {
                    sig_type,
                    type_vars,
                    params,
                    rest_params,
                    return_type,
                }
            }

        rule function_signature_type() -> FunctionSignatureType
            // Longest match first.
            = "FnAgg" { FunctionSignatureType::Aggregate }
            / "FnOver" { FunctionSignatureType::Window }
            / "Fn" { FunctionSignatureType::Scalar }

        rule function_params() -> (Vec<ValueType<TypeVar>>, Option<ValueType<TypeVar>>)
            = params:(value_type() ++ (_? "," _?))
              rest_params:(("," _? ".." _? rest_params:value_type() { rest_params })?)
            {
                (params, rest_params)
            }
            / ".." _? rest_params:value_type() { (Vec::new(), Some(rest_params)) }
            / { (Vec::new(), None) }

        rule type_vars() -> Vec<TypeVar>
            = "<" _? vars:(type_var() ** (_? "," _?)) _? ">" _? { vars }
            / { Vec::new() }

        pub rule value_type() -> ValueType<TypeVar>
            = t:simple_type() { ValueType::Simple(t) }
            / "ARRAY" _? "<" _? t:simple_type() _? ">" { ValueType::Array(Box::new(t)) }

        // Longest match first.
        rule simple_type() -> SimpleType<TypeVar>
            = "BOOL" { SimpleType::Bool }
            / "BYTES" { SimpleType::Bytes }
            / "DATETIME" { SimpleType::Datetime }
            / "DATEPART" { SimpleType::Datepart }
            / "DATE" { SimpleType::Date }
            / "FLOAT64" { SimpleType::Float64 }
            / "GEOGRAPHY" { SimpleType::Geography }
            / "INT64" { SimpleType::Int64 }
            / "INTERVAL" { SimpleType::Interval }
            / "NUMERIC" { SimpleType::Numeric }
            / "STRING" { SimpleType::String }
            / "TIMESTAMP" { SimpleType::Timestamp }
            / "TIME" { SimpleType::Time }
            / "STRUCT" _? "<" _? fields:(struct_field() ** (_? "," _?)) _? ">" {
                SimpleType::Struct(StructType { fields }) }
            / type_var:type_var() { SimpleType::Parameter(type_var) }

        rule struct_field() -> StructElementType<TypeVar>
            = t:value_type() { StructElementType { name: None, ty: t } }
            / name:ident() _ t:value_type() { StructElementType { name: Some(name), ty: t } }

        rule ident() -> Ident
            = name:$(['A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) {
                Ident::new(name, Span::Unknown)
            }

        rule type_var() -> TypeVar
            = "?" name:$(['A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) {
                TypeVar::new(name).unwrap()
            }

        rule _() = ([' ' | '\t' | '\r' | '\n' ] / "--" [^ '\n']* "\n")+
    }
}
