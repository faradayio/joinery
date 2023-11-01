//! Namespace for SQL.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::{
    ast::Name,
    errors::{format_err, Error, Result},
    known_files::KnownFiles,
    tokenizer::Spanned,
    types::{parse_function_decls, ArgumentType, TableType, Type},
};

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
    names: BTreeMap<Name, ScopeEntry>,
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
                .add(Name::from(name), Type::Function(ty))
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
    pub fn add(&mut self, name: Name, value: ScopeValue) -> Result<()> {
        if self.names.contains_key(&name) {
            // We don't try to do anything fancy like replacing a hidden name
            // with a defined name, because we probably don't need that.
            return Err(format_err!("duplicate name: {}", name.unescaped_bigquery()));
        }
        self.names.insert(name, ScopeEntry::Defined(value));
        Ok(())
    }

    /// "Drop" a value from the scope by hiding it. This is used to implement
    /// `DROP TABLE`, etc.
    ///
    /// You cannot do this once you have called [`Self::into_handle()`].
    pub fn hide(&mut self, name: &Name) -> Result<()> {
        if self.names.contains_key(name) {
            // We do not allow hiding a name that was defined in this scope.
            // Make a new scope for that.
            return Err(format_err!(
                "cannot hide name {} because it was defined in this scope",
                name.unescaped_bigquery()
            ));
        }
        self.names.insert(name.clone(), ScopeEntry::Hidden);
        Ok(())
    }

    /// Get a value from the scope.
    pub fn get<'scope>(&'scope self, name: &Name) -> Option<&'scope ScopeValue> {
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
    pub fn get_or_err(&self, name: &Name) -> Result<&ScopeValue> {
        self.get(name).ok_or_else(|| {
            Error::annotated(
                format!("unknown name: {}", name.unescaped_bigquery()),
                name.span(),
                "not defined",
            )
        })
    }
}

/// Built-in function declarations in the default scope.
static BUILT_IN_FUNCTIONS: &str = "

-- Primitives.

%AND = Fn(BOOL, BOOL) -> BOOL;
%ARRAY = Fn<?T>(..?T) -> ARRAY<?T>;
%BETWEEN = Fn<?T>(?T, ?T, ?T) -> BOOL;
%IF = Fn<?T>(BOOL, ?T, ?T) -> ?T;
-- Second argument to IN is actually TABLE<?T>, but we just do the lookup using
-- the column type.
%IN = Fn<?T>(?T, ?T) -> BOOL;
%IS = Fn<?T>(?T, NULL) -> BOOL | Fn(BOOL, BOOL) -> BOOL;
%NOT = Fn(BOOL) -> BOOL;
%OR = Fn(BOOL, BOOL) -> BOOL;

%= = Fn<?T>(?T, ?T) -> BOOL;
%!= = Fn<?T>(?T, ?T) -> BOOL;
%<= = Fn<?T>(?T, ?T) -> BOOL;
%< = Fn<?T>(?T, ?T) -> BOOL;
%> = Fn<?T>(?T, ?T) -> BOOL;
%>= = Fn<?T>(?T, ?T) -> BOOL;
%+ = Fn(INT64, INT64) -> INT64 | Fn(FLOAT64, FLOAT64) -> FLOAT64;
%- = Fn(INT64, INT64) -> INT64 | Fn(FLOAT64, FLOAT64) -> FLOAT64;
%* = Fn(INT64, INT64) -> INT64 | Fn(FLOAT64, FLOAT64) -> FLOAT64;
%/ = Fn(INT64, INT64) -> INT64 | Fn(FLOAT64, FLOAT64) -> FLOAT64;
%[] = Fn<?T>(ARRAY<?T>, INT64) -> ?T;

-- Functions.

ANY_VALUE = FnAgg<?T>(Agg<?T>) -> ?T;
ARRAY_LENGTH = Fn<?T>(ARRAY<?T>) -> INT64;
ARRAY_TO_STRING = Fn<?T>(ARRAY<?T>, STRING) -> STRING;
AVG = FnAgg(Agg<INT64>) -> FLOAT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
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

#[derive(Clone, Debug)]
pub struct ColumnName {
    table: Option<Name>,
    column: Name,
}

impl ColumnName {
    /// Does this column name match `name`? `name` may be either a column name
    /// or a table name and a column name. Matches follow SQL rules, so `x`
    /// matches `t.x`.
    pub fn matches(&self, name: &Name) -> bool {
        let (table, column) = name.split_table_and_column();
        if let Some(table) = table {
            if self.table.as_ref() != Some(&table) {
                return false;
            }
        }
        self.column == Name::from(column)
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    column_name: Option<ColumnName>,
    ty: ArgumentType,
}

/// A set of columns and types.
///
/// This is output by a `FROM`, `JOIN`, `GROUP BY` or `PARTITION BY` clause.
#[derive(Clone, Debug)]
pub struct ColumnSet {
    columns: Vec<Column>,
}

impl ColumnSet {
    /// Build a column set from a table type.
    pub fn from_table(table_name: Name, table_type: TableType) -> Self {
        let columns = table_type
            .columns
            .into_iter()
            .map(|col| {
                let column_name = col.name.map(|col_name| ColumnName {
                    table: Some(table_name.clone()),
                    column: col_name.into(),
                });
                Column {
                    column_name,
                    ty: col.ty,
                }
            })
            .collect();

        Self { columns }
    }

    /// Join another column set, returning a new column set. This corresponds to
    /// `JOIN` clauses that work like `ON`, which preserve all columns from both
    /// sides.
    pub fn join(&self, other: &Self) -> Self {
        let mut columns = self.columns.clone();
        columns.extend(other.columns.iter().cloned());
        Self { columns }
    }

    /// Join another column set using the variables from a `USING` clause,
    /// returning a new column set.
    ///
    /// For the variables that are in the `USING` clause, we only include one
    /// copy of each without the table name.
    pub fn join_using(&self, other: &Self, using: &[Name]) -> Result<Self> {
        // Create a hash map, indicating which columns we have seen so far.
        let mut seen_with_type = HashMap::new();
        for name in using {
            seen_with_type.insert(name.clone(), None);
        }

        // Iterate over all our columns, and add them to the output, being sure
        // to handle using columns specially.
        let columns_iter = self
            .columns
            .iter()
            .cloned()
            .chain(other.columns.iter().cloned());
        let mut columns = vec![];
        for col in columns_iter {
            if let Some(name) = &col.column_name {
                match seen_with_type.get_mut(&name.column) {
                    Some(None) => {
                        // We have not seen this column yet. Add it to the
                        // output, removing the table name.
                        seen_with_type.insert(name.column.clone(), Some(col.ty.clone()));
                        columns.push(Column {
                            column_name: Some(ColumnName {
                                table: None,
                                column: name.column.clone(),
                            }),
                            ty: col.ty,
                        });
                    }
                    Some(Some(ty)) => {
                        // We have already seen this column. Make sure the types
                        // match.
                        if col.ty.common_supertype(ty).is_none() {
                            return Err(Error::annotated(
                                format!(
                                    "column {} has type {} in one table and type {} in another",
                                    name.column.unescaped_bigquery(),
                                    ty,
                                    col.ty
                                ),
                                name.column.span(),
                                "types do not match",
                            ));
                        }
                    }
                    None => {
                        // This column is not in the `USING` clause. Add it to
                        // the output.
                        columns.push(col);
                    }
                }
            } else {
                // This column has no name. Add it to the output.
                columns.push(col);
            }
        }
        Ok(Self { columns })
    }

    /// To implement `GROUP BY`, we need to iterate over all columns, wrapping
    /// any column not mentioned in the `GROUP BY` clause with
    /// `ArgumentType::Aggregating`.
    pub fn group_by(&self, group_by: &[Name]) -> Result<Self> {
        let mut columns = vec![];
        for col in &self.columns {
            if let Some(name) = &col.column_name {
                if group_by.contains(&name.column) {
                    // This column is mentioned in the `GROUP BY` clause. Add it
                    // to the output as is.
                    columns.push(col.clone());
                } else {
                    // This column is not mentioned in the `GROUP BY` clause.
                    // Wrap it in `ArgumentType::Aggregating` and add it to the
                    // output.
                    columns.push(Column {
                        column_name: col.column_name.clone(),
                        ty: ArgumentType::Aggregating(Box::new(col.ty.clone())),
                    });
                }
            } else {
                // Not mentioned in the `GROUP BY` clause, so aggregate it.
                columns.push(Column {
                    column_name: col.column_name.clone(),
                    ty: ArgumentType::Aggregating(Box::new(col.ty.clone())),
                });
            }
        }
        Ok(Self { columns })
    }

    /// Look up a column type by name. Returns an error if ambiguous.
    pub fn get(&self, name: &Name) -> Result<&ArgumentType> {
        let mut matches = vec![];
        for col in &self.columns {
            if let Some(column_name) = &col.column_name {
                if column_name.matches(name) {
                    matches.push(&col.ty);
                }
            }
        }
        match matches.len() {
            0 => Err(Error::annotated(
                format!("unknown column: {}", name.unescaped_bigquery()),
                name.span(),
                "not defined",
            )),
            1 => Ok(matches[0]),
            _ => Err(Error::annotated(
                format!("ambiguous column: {}", name.unescaped_bigquery()),
                name.span(),
                "multiple columns match",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_built_in_functions() {
        Scope::root();
    }
}
