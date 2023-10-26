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

use std::{borrow::Cow, fmt};

use peg::{error::ParseError, str::LineCol};

use crate::{
    ast,
    drivers::bigquery::BigQueryName,
    errors::{format_err, Error, Result},
    known_files::{FileId, KnownFiles},
    tokenizer::{Ident, Span, Spanned},
    util::is_c_ident,
};

/// Sometimes we want concrete types, and sometimes we want types with type
/// variables. This trait convers both those cases.
pub trait TypeVarSupport: Clone + fmt::Display + PartialEq + Sized {
    /// Convert a [`TypeVar`] into a [`SimpleType`], if possible.
    fn simple_type_from_type_var(tv: TypeVar) -> Result<SimpleType<Self>, &'static str>;
}

/// This type can never be instantiated. We use this to represent a type variable with
/// all type variables resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedTypeVarsOnly {}

impl TypeVarSupport for ResolvedTypeVarsOnly {
    fn simple_type_from_type_var(_tv: TypeVar) -> Result<SimpleType<Self>, &'static str> {
        // This will be a parser error with `"expected "` prepended. So it's
        // hard to word well.
        Err("something other than a type variable")
    }
}

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

impl TypeVarSupport for TypeVar {
    fn simple_type_from_type_var(tv: TypeVar) -> Result<SimpleType<Self>, &'static str> {
        Ok(SimpleType::Parameter(tv))
    }
}

impl fmt::Display for TypeVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.name)?;
        Ok(())
    }
}

/// Basic types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    /// A value which may appear as a function argument.
    Argument(ArgumentType<TV>),
    /// The type of a table, as seen in `CREATE TABLE` statements, or
    /// as returned from a sub-`SELECT`, or as passed to `UNNEST`.
    Table(TableType),
    /// A function type.
    Function(FunctionType),
}

impl<TV: TypeVarSupport> Type<TV> {
    /// Convert this type into a [`ValueType`], if possible.
    pub fn try_as_value_type(&self, spanned: &dyn Spanned) -> Result<&ValueType<TV>> {
        match self {
            Type::Argument(ArgumentType::Value(t)) => Ok(t),
            _ => Err(Error::annotated(
                format!("expected value type, found {}", self),
                spanned.span(),
                "type mismatch",
            )),
        }
    }

    /// Convert this type into a [`TableType`], if possible.
    pub fn try_as_table_type(&self, spanned: &dyn Spanned) -> Result<&TableType> {
        match self {
            Type::Table(t) => Ok(t),
            _ => Err(Error::annotated(
                format!("expected table type, found {}", self),
                spanned.span(),
                "type mismatch",
            )),
        }
    }
}

impl<TV: TypeVarSupport> fmt::Display for Type<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Argument(v) => write!(f, "{}", v),
            Type::Table(t) => write!(f, "{}", t),
            Type::Function(func) => write!(f, "{}", func),
        }
    }
}

/// An argument type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArgumentType<TV: TypeVarSupport = ResolvedTypeVarsOnly> {
    /// A value type.
    Value(ValueType<TV>),
    /// An aggregating value type.
    Aggregating(ValueType<TV>),
}

impl<TV: TypeVarSupport> fmt::Display for ArgumentType<TV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArgumentType::Value(v) => write!(f, "{}", v),
            ArgumentType::Aggregating(v) => write!(f, "Agg<{}>", v),
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
    Array(SimpleType<TV>),
}

impl<TV: TypeVarSupport> ValueType<TV> {
    /// Is this a subtype of `other`?
    pub fn is_subtype_of(&self, other: &ValueType<TV>) -> bool {
        match (self, other) {
            // Every type is a subtype of itself.
            (a, b) if a == b => true,

            // Bottom is a subtype of every type.
            (ValueType::Simple(SimpleType::Bottom), _) => true,

            // Null is a subtype of every type except bottom.
            (_, ValueType::Simple(SimpleType::Bottom)) => false,
            (ValueType::Simple(SimpleType::Null), _) => true,

            // Integers are a subtype of floats.
            (ValueType::Simple(SimpleType::Int64), ValueType::Simple(SimpleType::Float64)) => true,

            // An ARRAY<⊥> is a subset of any other array type. However,
            // an ARRAY<INT64> is not a subtype of ARRAY<FLOAT64>, as
            (ValueType::Array(SimpleType::Bottom), ValueType::Array(_)) => true,

            // TODO: Structs with anonymous fields may be subtype of structs
            // with named fields.

            // TODO: Tables with unknown column names, built by `SELECT` and
            // combined with `UNION`, may be subtypes of tables with known
            // column names.

            // Otherwise, assume it isn't a subtype.
            _ => false,
        }
    }

    /// Return an error if we are not a subtype of `other`.
    pub fn expect_subtype_of(&self, other: &ValueType<TV>, spanned: &dyn Spanned) -> Result<()> {
        if !self.is_subtype_of(other) {
            return Err(Error::annotated(
                format!("expected {}, found {}", other, self),
                spanned.span(),
                "type mismatch",
            ));
        }
        Ok(())
    }

    /// Compute the greatest common subtype of two types. Will return ⊥ if it
    /// can't find anything better.
    ///
    /// For some nice theoretical terminology, see [this
    /// page](https://orc.csres.utexas.edu/documentation/html/refmanual/ref.types.subtyping.html).
    pub fn greatest_common_subtype<'a>(
        &'a self,
        other: &'a ValueType<TV>,
    ) -> Cow<'a, ValueType<TV>> {
        if self.is_subtype_of(other) {
            Cow::Borrowed(self)
        } else if other.is_subtype_of(self) {
            Cow::Borrowed(other)
        } else {
            Cow::Owned(ValueType::Simple(SimpleType::Bottom))
        }
    }

    /// Expect this type to be inhabited by at least one value.
    pub fn expect_inhabited(&self, spanned: &dyn Spanned) -> Result<()> {
        match self {
            ValueType::Simple(SimpleType::Bottom) => Err(Error::annotated(
                "cannot construct a value of this type",
                spanned.span(),
                "no valid values",
            )),
            _ => Ok(()),
        }
    }
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
    /// The "bottom" type "⊥", which contains no values, and is a subtype of
    /// all other types. The expression `ARRAY[]` has the type `ARRAY<⊥>`,
    /// because it can be used as an array of any type.
    Bottom,
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
    /// The NULL type. This is somewhat similar to `Bottom`. It's a subtype of
    /// almost every type. It contains a single value, `NULL`.
    Null,
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
            SimpleType::Bottom => write!(f, "⊥"),
            SimpleType::Bytes => write!(f, "BYTES"),
            SimpleType::Date => write!(f, "DATE"),
            SimpleType::Datepart => write!(f, "DATEPART"),
            SimpleType::Datetime => write!(f, "DATETIME"),
            SimpleType::Float64 => write!(f, "FLOAT64"),
            SimpleType::Geography => write!(f, "GEOGRAPHY"),
            SimpleType::Int64 => write!(f, "INT64"),
            SimpleType::Interval => write!(f, "INTERVAL"),
            SimpleType::Numeric => write!(f, "NUMERIC"),
            SimpleType::Null => write!(f, "NULL"),
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
pub struct TableType {
    pub columns: Vec<ColumnType>,
}

impl TableType {
    /// Look up a column by name.
    pub fn column_by_name(&self, name: &Ident) -> Option<&ColumnType> {
        for column in &self.columns {
            if let Some(column_name) = &column.name {
                if column_name == name {
                    return Some(column);
                }
            }
        }
        None
    }

    /// Look up a column by name, and return an error if it doesn't exist.
    pub fn column_by_name_or_err(&self, name: &Ident) -> Result<&ColumnType> {
        match self.column_by_name(name) {
            Some(column) => Ok(column),
            None => Err(Error::annotated(
                format!("no such column: {}", name.name),
                name.span(),
                "not defined",
            )),
        }
    }

    /// Is this table a subtype of `other`, ignoring nullability?
    pub fn is_subtype_ignoring_nullability_of(&self, other: &TableType) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }
        for (a, b) in self.columns.iter().zip(&other.columns) {
            if !a.is_subtype_ignoring_nullability_of(b) {
                return false;
            }
        }
        true
    }

    /// Return an error if this table is not a subtype of `other`.
    pub fn expect_subtype_ignoring_nullability_of(
        &self,
        other: &TableType,
        spanned: &dyn Spanned,
    ) -> Result<()> {
        if !self.is_subtype_ignoring_nullability_of(other) {
            return Err(Error::annotated(
                format!("expected {}, found {}", other, self),
                spanned.span(),
                "type mismatch",
            ));
        }
        Ok(())
    }
}

impl fmt::Display for TableType {
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
pub struct ColumnType {
    /// The name of the column, if it has one. Anonymous columns may be produced
    /// using things like `SELECT 1`, but they cannot occur in stored tables.
    pub name: Option<Ident>,
    pub ty: ValueType,
    pub not_null: bool,
}

impl ColumnType {
    /// Return an error if this column is not storable.
    pub fn expect_creatable(&self, spanned: &dyn Spanned) -> Result<()> {
        self.ty.expect_inhabited(spanned)?;
        if self.name.is_none() {
            return Err(Error::annotated(
                "cannot store a table with anonymous columns",
                spanned.span(),
                "type mismatch",
            ));
        }
        Ok(())
    }

    /// Is this column a subtype of `other`, ignoring nullability?
    pub fn is_subtype_ignoring_nullability_of(&self, other: &ColumnType) -> bool {
        self.ty.is_subtype_of(&other.ty)
            && match (&self.name, &other.name) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", BigQueryName(&name.name))?;
        }
        write!(f, "{}", self.ty)?;
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
    pub params: Vec<ArgumentType<TypeVar>>,
    /// Trailing `..` parameter, which may be repeated. We do not support
    /// aggregate functions with rest parameters.
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

impl<TV: TypeVarSupport> TryFrom<&ast::DataType> for ValueType<TV> {
    type Error = Error;

    fn try_from(value: &ast::DataType) -> Result<Self, Self::Error> {
        match value {
            ast::DataType::Bool(_) => Ok(ValueType::Simple(SimpleType::Bool)),
            ast::DataType::Bytes(_) => Ok(ValueType::Simple(SimpleType::Bytes)),
            ast::DataType::Date(_) => Ok(ValueType::Simple(SimpleType::Date)),
            ast::DataType::Datetime(_) => Ok(ValueType::Simple(SimpleType::Datetime)),
            ast::DataType::Float64(_) => Ok(ValueType::Simple(SimpleType::Float64)),
            ast::DataType::Geography(_) => Ok(ValueType::Simple(SimpleType::Geography)),
            ast::DataType::Int64(_) => Ok(ValueType::Simple(SimpleType::Int64)),
            ast::DataType::Numeric(_) => Ok(ValueType::Simple(SimpleType::Numeric)),
            ast::DataType::String(_) => Ok(ValueType::Simple(SimpleType::String)),
            ast::DataType::Time(_) => Ok(ValueType::Simple(SimpleType::Time)),
            ast::DataType::Timestamp(_) => Ok(ValueType::Simple(SimpleType::Timestamp)),
            ast::DataType::Array { data_type, .. } => {
                if let ValueType::Simple(elem_type) = data_type.as_ref().try_into()? {
                    Ok(ValueType::Array(elem_type))
                } else {
                    // TODO: Get scope from `data_type`.
                    Err(format_err!("ARRAY<..> must not contain another ARRAY"))
                }
            }
            ast::DataType::Struct { fields, .. } => {
                let mut struct_fields = vec![];
                for field in fields.node_iter() {
                    struct_fields.push(StructElementType {
                        name: field.name.clone(),
                        ty: ValueType::try_from(&field.data_type)?,
                    });
                }
                Ok(ValueType::Simple(SimpleType::Struct(StructType {
                    fields: struct_fields,
                })))
            }
        }
    }
}

impl<TV: TypeVarSupport> TryFrom<&ast::DataType> for Type<TV> {
    type Error = Error;

    fn try_from(ty: &ast::DataType) -> Result<Self, Self::Error> {
        Ok(Type::Argument(ArgumentType::Value(ValueType::try_from(
            ty,
        )?)))
    }
}

/// Helper function called by type-parsing functions.
fn parse_helper<T, F>(files: &KnownFiles, file_id: FileId, f: F) -> Result<T>
where
    F: FnOnce(&str) -> Result<T, ParseError<LineCol>>,
{
    let s = files.source_code(file_id)?;
    match f(s) {
        Ok(parsed) => Ok(parsed),
        Err(e) => Err(Error::annotated(
            "error parsing type expression",
            Span::from_line_col(file_id, e.location),
            format!("expected {}", e.expected),
        )),
    }
}

/// Parse a set of function declarations.
pub fn parse_function_decls(
    files: &KnownFiles,
    file_id: FileId,
) -> Result<Vec<(Ident, FunctionType)>> {
    parse_helper(files, file_id, type_grammar::function_decls)
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

        pub rule ty<TV: TypeVarSupport>() -> Type<TV>
            = t:argument_type() { Type::Argument(t) }
            / t:table_type() { Type::Table(t) }
            / t:function_type() { Type::Function(t) }

        rule argument_type<TV: TypeVarSupport>() -> ArgumentType<TV>
            = "Agg" _? "<" _? t:value_type() _? ">" { ArgumentType::Aggregating(t) }
            / t:value_type() { ArgumentType::Value(t) }

        rule value_type<TV: TypeVarSupport>() -> ValueType<TV>
            = t:simple_type() { ValueType::Simple(t) }
            / "ARRAY" _? "<" _? t:simple_type() _? ">" { ValueType::Array(t) }

        // Longest match first.
        rule simple_type<TV: TypeVarSupport>() -> SimpleType<TV>
            = "BOOL" { SimpleType::Bool }
            / "⊥" { SimpleType::Bottom }
            / "BYTES" { SimpleType::Bytes }
            / "DATETIME" { SimpleType::Datetime }
            / "DATEPART" { SimpleType::Datepart }
            / "DATE" { SimpleType::Date }
            / "FLOAT64" { SimpleType::Float64 }
            / "GEOGRAPHY" { SimpleType::Geography }
            / "INT64" { SimpleType::Int64 }
            / "INTERVAL" { SimpleType::Interval }
            / "NULL" { SimpleType::Null }
            / "NUMERIC" { SimpleType::Numeric }
            / "STRING" { SimpleType::String }
            / "TIMESTAMP" { SimpleType::Timestamp }
            / "TIME" { SimpleType::Time }
            / "STRUCT" _? "<" _? fields:(struct_field() ** (_? "," _?)) _? ">" {
                SimpleType::Struct(StructType { fields }) }
            / type_var:type_var() {? TV::simple_type_from_type_var(type_var) }

        rule struct_field<TV: TypeVarSupport>() -> StructElementType<TV>
            = t:value_type() { StructElementType { name: None, ty: t } }
            / name:ident() _ t:value_type() { StructElementType { name: Some(name), ty: t } }

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

        rule function_params() -> (Vec<ArgumentType<TypeVar>>, Option<ValueType<TypeVar>>)
            = params:(argument_type() ++ (_? "," _?))
              rest_params:(("," _? ".." _? rest_params:value_type() { rest_params })?)
            {
                (params, rest_params)
            }
            / ".." _? rest_params:value_type() { (Vec::new(), Some(rest_params)) }
            / { (Vec::new(), None) }

        rule table_type() -> TableType
            = "TABLE" _? "<" _? columns:(column_type() ** (_? "," _?)) _? ">" {
                TableType { columns }
            }

        rule column_type() -> ColumnType
            = ty:value_type() not_null:not_null() {
                ColumnType { name: None, ty, not_null }
            }
            / name:ident() _ ty:value_type() not_null:not_null() {
                ColumnType { name: Some(name), ty, not_null }
            }

        rule not_null() -> bool
            = _ "NOT" _ "NULL" { true }
            / { false }

        rule type_vars() -> Vec<TypeVar>
            = "<" _? vars:(type_var() ** (_? "," _?)) _? ">" _? { vars }
            / { Vec::new() }

        rule ident() -> Ident
            = name:$(['A'..='Z' | 'a'..='z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) {
                Ident::new(name, Span::Unknown)
            }

        rule type_var() -> TypeVar
            = "?" name:$(['A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) {
                TypeVar::new(name).unwrap()
            }

        rule _() = ([' ' | '\t' | '\r' | '\n' ] / "--" [^ '\n']* "\n")+
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Parse a type declaration.
    pub fn ty(s: &str) -> Type {
        // We use local `KnownFiles` here, because we panic on parse errors, and
        // our caller doesn't need to know we parse at all.
        let mut files = KnownFiles::new();
        let file_id = files.add_string("type", s);
        match parse_helper(&files, file_id, type_grammar::ty) {
            Ok(ty) => ty,
            Err(e) => {
                e.emit(&files);
                panic!("parse error");
            }
        }
    }

    #[test]
    fn test_parse_function_decls() {
        let mut files = KnownFiles::new();
        let file_id = files.add_string("fn_decls", "ANY_VALUE = FnAgg<?T>(Agg<?T>) -> ?T;");
        match parse_function_decls(&files, file_id) {
            Ok(decls) => {
                assert_eq!(decls.len(), 1);
                assert_eq!(decls[0].0.name, "ANY_VALUE");
                assert_eq!(decls[0].1.to_string(), "FnAgg<?T>(Agg<?T>) -> ?T");
            }
            Err(e) => {
                e.emit(&files);
                panic!("parse error");
            }
        }
    }

    #[test]
    fn parse_type() {
        assert_eq!(
            ty("BOOL"),
            Type::Argument(ArgumentType::Value(ValueType::Simple(SimpleType::Bool)))
        );
    }
}
