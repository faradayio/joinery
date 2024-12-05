//! Scope (aka namespace) support for SQL.
//!
//! This is surprisingly complicated. We need to support:
//!
//! - Built-in functions.
//! - `CREATE` and `DROP` statements that add or remove items from the
//!   top-level scope.
//! - CTEs, which add items to a query's scope.
//! - `FROM` and `JOIN` clauses, which create special scopes containing column
//!   names.
//!   - `USING` clauses, which remove the table name from some columns.
//! - `GROUP BY` and `PARTITION BY` clauses, which aggregate all columns not
//!   mentioned in the clause.
//!   - There are also implicit aggregations, like `SELECT COUNT(*) FROM t`
//!     and `SUM(x) OVER ()`, but we leave those to our callers.

use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::{
    ast::Name,
    errors::{format_err, Error, Result, SourceError},
    known_files::KnownFiles,
    tokenizer::Spanned,
    types::{parse_function_decls, ArgumentType, ColumnType, FunctionType, TableType, Type},
};

/// A value we can store in a scope. Details may change.
pub type ScopeValue = Type;

/// Common interface to all things that support a scope-like `get` function.
///
/// SQL has multiple kinds of scopes, including:
///
/// - [`Scope`] (often wrapped by [`ScopeHandle`] for reference counting), which
///   works more-or-less like a scope in a typical programming language. This
///   has a few unusual features: names can be hidden, or looking up a name
///   might return an error.
/// - [`ColumnSetScope`] (wrapping a [`ColumnSet`]), which implements the
///   special rules for looking up column names in SQL.
pub trait ScopeGet {
    /// Get the [`ScopeValue`] associated with `name`.
    ///
    /// Returns `Ok(None)` if `name` is not defined. Returns an error if `name`
    /// is ambiguous. Returns an owned because some implementations may need to
    /// modify types before returning them, and because trying to use
    /// [`std::borrow::Cow`] pays too high a "Rust tax".
    fn get(&self, name: &Name) -> Result<Option<ScopeValue>>;

    /// Get a value, or return an error if it is not defined.
    fn get_or_err(&self, name: &Name) -> Result<ScopeValue> {
        self.get(name)?.ok_or_else(|| {
            Error::annotated(
                format!("unknown name: {}", name.unescaped_bigquery()),
                name.span(),
                "not defined",
            )
        })
    }

    /// Look up `name` as an [`ArgumentType`], if possible.
    fn get_argument_type(&self, name: &Name) -> Result<ArgumentType> {
        self.get_or_err(name)?.try_as_argument_type(name).cloned()
    }

    /// Look up `name` as a [`TableType`], if possible.
    fn get_table_type(&self, name: &Name) -> Result<TableType> {
        self.get_or_err(name)?.try_as_table_type(name).cloned()
    }

    /// Look up `name` as a [`FunctionType`], if possible.
    fn get_function_type(&self, name: &Name) -> Result<FunctionType> {
        self.get_or_err(name)?.try_as_function_type(name).cloned()
    }
}

/// A handle to a scope.
pub type ScopeHandle = Arc<Scope>;

impl ScopeGet for ScopeHandle {
    fn get(&self, name: &Name) -> Result<Option<ScopeValue>> {
        (**self).get(name)
    }
}

/// We need to both define and hide names in a scope.
#[derive(Clone, Debug)]
enum ScopeEntry {
    /// A name that is defined in this scope.
    Defined(ScopeValue),
    /// A name that is hidden in this scope. This will happen when a top-level
    /// SQL statement like `DROP TABLE` creates a new scope that hides a name
    /// defined in the "parent" scope defined by the preceding statements.
    Hidden,
    /// A name that returns an error when looked up. This may happen if two
    /// names conflict, or sometimes because we're inside a subquery and certain
    /// names from outer scopes are not available.
    Error(SourceError),
}

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
        // Only build the root scope once, because it's moderately expensive.
        static ROOT: once_cell::sync::Lazy<ScopeHandle> =
            once_cell::sync::Lazy::new(Scope::build_root);
        ROOT.clone()
    }

    /// Helper function for `root`.
    fn build_root() -> ScopeHandle {
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

    /// Add a new value to the scope. Returns an error if the name is already
    /// defined in the local scope. If the name is in a parent scope, it will
    /// be shadowed.
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
    /// `DROP TABLE`, etc. You cannot hide a name that was defined in this
    /// scope.
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

    /// Mark a name as returning an error. This will overwrite an existing
    /// definition of that name, because this is normally used for ambiguous
    /// names.
    pub fn add_error(&mut self, name: Name, err: SourceError) {
        self.names.insert(name, ScopeEntry::Error(err));
    }
}

impl ScopeGet for Scope {
    fn get(&self, name: &Name) -> Result<Option<ScopeValue>> {
        match self.names.get(name) {
            Some(ScopeEntry::Defined(value)) => Ok(Some(value.clone())),
            Some(ScopeEntry::Hidden) => Ok(None),
            Some(ScopeEntry::Error(err)) => Err(Error::Source(Box::new(err.clone()))),
            None => {
                if let Some(parent) = self.parent.as_ref() {
                    parent.get(name)
                } else {
                    Ok(None)
                }
            }
        }
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

%UNARY- = Fn(INT64) -> INT64 | Fn(FLOAT64) -> FLOAT64;

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
APPROX_COUNT_DISTINCT = FnAgg<?T>(Agg<?T>) -> INT64;
APPROX_QUANTILES = FnAgg<?T>(Agg<?T>, INT64) -> ARRAY<?T>;
ARRAY_AGG = FnAgg<?T>(Agg<?T>) -> ARRAY<?T>;
ARRAY_LENGTH = Fn<?T>(ARRAY<?T>) -> INT64;
ARRAY_TO_STRING = Fn<?T>(ARRAY<?T>, STRING) -> STRING;
AVG = FnAgg(Agg<INT64>) -> FLOAT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
COALESCE = Fn<?T>(?T, ..?T) -> ?T;
CONCAT = Fn(STRING, ..STRING) -> STRING | Fn(BYTES, ..BYTES) -> BYTES;
COUNT = FnAgg<?T>(Agg<?T>) -> INT64;
COUNTIF = FnAgg(Agg<BOOL>) -> INT64 | FnOver(Agg<BOOL>) -> INT64;
CURRENT_DATE = Fn() -> DATE;
CURRENT_DATETIME = Fn() -> DATETIME;
DATE = Fn(STRING) -> DATE;
DATE_ADD = Fn(DATE, INTERVAL) -> DATE;
DATE_DIFF = Fn(DATE, DATE, DATEPART) -> INT64;
DATE_SUB = Fn(DATE, INTERVAL) -> DATE;
DATE_TRUNC = Fn(DATE, DATEPART) -> DATE;
DATETIME = Fn(STRING) -> DATETIME | Fn(DATE) -> DATETIME;
DATETIME_ADD = Fn(DATETIME, INTERVAL) -> DATETIME;
DATETIME_DIFF = Fn(DATETIME, DATETIME, DATEPART) -> INT64;
DATETIME_SUB = Fn(DATETIME, INTERVAL) -> DATETIME;
DATETIME_TRUNC = Fn(DATETIME, DATEPART) -> DATETIME;
EXP = Fn(FLOAT64) -> FLOAT64;
FARM_FINGERPRINT = Fn(STRING) -> INT64 | Fn(BYTES) -> INT64;
FIRST_VALUE = FnOver<?T>(Agg<?T>) -> ?T;
FORMAT_DATETIME = Fn(DATETIME, STRING) -> STRING;
FROM_HEX = Fn(STRING) -> BYTES;
GENERATE_DATE_ARRAY = Fn(DATE, DATE, INTERVAL) -> ARRAY<DATE>;
GENERATE_UUID = Fn() -> STRING;
GREATEST = Fn<?T>(?T, ..?T) -> ?T;
LAG = FnOver<?T>(Agg<?T>) -> ?T;
LAST_VALUE = FnOver<?T>(Agg<?T>) -> ?T;
LEAST = Fn<?T>(?T, ..?T) -> ?T;
LENGTH = Fn(STRING) -> INT64 | Fn(BYTES) -> INT64;
LOWER = Fn(STRING) -> STRING;
MAX = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
MIN = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64;
MOD = Fn(INT64, INT64) -> INT64;
RAND = Fn() -> FLOAT64;
RANK = FnOver() -> INT64;
REGEXP_CONTAINS = Fn(STRING, STRING) -> BOOL;
REGEXP_EXTRACT = Fn(STRING, STRING) -> STRING;
REGEXP_REPLACE = Fn(STRING, STRING, STRING) -> STRING;
REPLACE = Fn(STRING, STRING, STRING) -> STRING;
ROW_NUMBER = FnOver() -> INT64;
SHA256 = Fn(STRING) -> BYTES;
SUBSTR = Fn(STRING, INT64) -> STRING | Fn(STRING, INT64, INT64) -> STRING;
SUM = FnAgg(Agg<INT64>) -> INT64 | FnAgg(Agg<FLOAT64>) -> FLOAT64
    | FnOver(Agg<INT64>) -> INT64 | FnOver(Agg<FLOAT64>) -> FLOAT64;
TO_HEX = Fn(BYTES) -> STRING;
TRIM = Fn(STRING) -> STRING;
UPPER = Fn(STRING) -> STRING;
";

/// The name of a column in a [`ColumnSet`].
///
/// In SQL, `t.x` may also be referred to as `x` (if `x` is unambiguous). But
/// depending on how the [`ColumnSet`] was created, a column name may not always
/// have a table name, thanks to things like `USING`. See
/// [`ColumnSet::join_using`].
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSetColumnName {
    table: Option<Name>,
    column: Name,
}

impl ColumnSetColumnName {
    /// Create a new column name.
    pub fn new(table: Option<Name>, column: Name) -> Self {
        Self { table, column }
    }

    /// Convert to a single [`Name`].
    pub fn to_name(&self) -> Name {
        match &self.table {
            Some(table) => Name::combine(table, &self.column),
            None => self.column.clone(),
        }
    }

    /// Does `name` match this column name? `name` may be either a column name
    /// or a table name and a column name. Matches follow SQL rules, so `x`
    /// matches `t.x`.
    pub fn matched_by(&self, name: &Name) -> bool {
        let (table, column) = name.split_table_and_column();
        if let Some(table) = table {
            if self.table.as_ref() != Some(&table) {
                return false;
            }
        }
        self.column == Name::from(column)
    }
}

impl fmt::Display for ColumnSetColumnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table.unescaped_bigquery())?;
        }
        write!(f, "{}", self.column.unescaped_bigquery())
    }
}

/// A column in a [`ColumnSet`].
///
/// This may be unnamed, because SQL allows `SELECT` clauses to contain
/// anonymous columns. It even allows referring to columns by their ordinal
/// position in the `SELECT` clause!
///
/// ```sql
/// SELECT
///     SUM(goals_scored) AS total_goals,
///     UPPER(last_name),
///     UPPER(first_name)
/// FROM players
/// GROUP BY 2, 3;
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSetColumn {
    column_name: Option<ColumnSetColumnName>,
    ty: ArgumentType,
}

impl ColumnSetColumn {
    /// Create a new column.
    pub fn new(column_name: Option<ColumnSetColumnName>, ty: ArgumentType) -> Self {
        Self { column_name, ty }
    }
}

impl fmt::Display for ColumnSetColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(column_name) = &self.column_name {
            write!(f, "{} ", column_name)?;
        }
        write!(f, "{}", self.ty)
    }
}

/// A set of columns and types.
///
/// This is output by a `FROM`, `JOIN`, `GROUP BY` or `PARTITION BY` clause.
///
/// A [`ColumnSet`] also acts as a specialized kind of scope, different from a
/// regular [`Scope`] because of various bits of magic in how SQL looks up
/// names. Normally, you will use a [`ColumnSetScope`], which allows seeing both
/// column names _and_ names from the parent scope.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSet {
    columns: Vec<ColumnSetColumn>,
}

impl ColumnSet {
    /// Create a new column set.
    pub fn new(columns: Vec<ColumnSetColumn>) -> Self {
        Self { columns }
    }

    /// Build a column set from a table type.
    ///
    /// The `table_name` may be missing if the the table type was defined by
    /// something like `FROM (SELECT 1 AS a)` without an `AS` alias.
    pub fn from_table(table_name: Option<Name>, table_type: TableType) -> Self {
        let columns = table_type
            .columns
            .into_iter()
            .map(|col| {
                let column_name = col.name.map(|col_name| ColumnSetColumnName {
                    table: table_name.clone(),
                    column: col_name.into(),
                });
                ColumnSetColumn {
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
        let mut seen_at_output_index: HashMap<Name, Option<usize>> = HashMap::new();
        for name in using {
            seen_at_output_index.insert(name.clone(), None);
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
                match seen_at_output_index.get_mut(&name.column) {
                    Some(None) => {
                        // We have not seen this column yet. Add it to the
                        // output, removing the table name.
                        seen_at_output_index.insert(name.column.clone(), Some(columns.len()));
                        columns.push(ColumnSetColumn {
                            column_name: Some(ColumnSetColumnName {
                                table: None,
                                column: name.column.clone(),
                            }),
                            ty: col.ty,
                        });
                    }
                    Some(Some(idx)) => {
                        // We have already seen this column. Make sure the types
                        // are compatible, and update the type if necessary.
                        let ty = &mut columns[*idx].ty;
                        if let Some(supertype) = col.ty.common_supertype(ty) {
                            *ty = supertype;
                        } else {
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

        // Make sure we saw all the columns in the `USING` clause.
        for name in using {
            if let Some(None) = seen_at_output_index.get(name) {
                return Err(Error::annotated(
                    format!("column {} not found", name.unescaped_bigquery()),
                    name.span(),
                    "not found",
                ));
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
                if group_by.iter().any(|n| name.matched_by(n)) {
                    // This column is mentioned in the `GROUP BY` clause. Add it
                    // to the output as is.
                    columns.push(col.clone());
                } else {
                    // This column is not mentioned in the `GROUP BY` clause.
                    // Wrap it in `ArgumentType::Aggregating` and add it to the
                    // output.
                    columns.push(ColumnSetColumn {
                        column_name: col.column_name.clone(),
                        ty: ArgumentType::Aggregating(Box::new(col.ty.clone())),
                    });
                }
            } else {
                // Not mentioned in the `GROUP BY` clause, so aggregate it.
                columns.push(ColumnSetColumn {
                    column_name: col.column_name.clone(),
                    ty: ArgumentType::Aggregating(Box::new(col.ty.clone())),
                });
            }
        }
        Ok(Self { columns })
    }

    /// Implement `*` in `SELECT * FROM t` by constructing a table type. Returns
    /// an error if the resulting table would contain no columns, or duplicate
    /// column names.
    pub fn star(&self, spanned: &dyn Spanned) -> Result<TableType> {
        let mut columns = vec![];
        for col in &self.columns {
            columns.push(ColumnType {
                name: col
                    .column_name
                    .as_ref()
                    .map(|n| n.column.clone().try_into())
                    .transpose()?,
                ty: col.ty.clone(),
                not_null: false,
            });
        }
        let table_type = TableType { columns };
        table_type.expect_creatable(spanned)?;
        Ok(table_type)
    }

    /// Implement `table.*` in `SELECT table.* FROM table` by constructing a
    /// table type. Returns an error if the resulting table would contain no
    /// columns, or duplicate column names.
    pub fn star_for(&self, table: &Name, spanned: &dyn Spanned) -> Result<TableType> {
        let mut columns = vec![];
        for col in &self.columns {
            if let Some(name) = &col.column_name {
                if name.table.as_ref() == Some(table) {
                    columns.push(ColumnType {
                        name: Some(name.column.clone().try_into()?),
                        ty: col.ty.clone(),
                        not_null: false,
                    });
                }
            }
        }
        let table_type = TableType { columns };
        table_type.expect_creatable(spanned)?;
        Ok(table_type)
    }
}

impl ScopeGet for ColumnSet {
    fn get(&self, name: &Name) -> Result<Option<ScopeValue>> {
        let mut matches = vec![];
        for col in &self.columns {
            if let Some(column_name) = &col.column_name {
                if column_name.matched_by(name) {
                    matches.push(&col.ty);
                }
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(Type::Argument(matches[0].clone()))),
            _ => Err(Error::annotated(
                format!("ambiguous column: {}", name.unescaped_bigquery()),
                name.span(),
                "multiple columns match",
            )),
        }
    }
}

impl fmt::Display for ColumnSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for col in &self.columns {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }
        Ok(())
    }
}

/// Wraps a [`ColumnSet`] and allows it to have a [`Scope`] as a parent.
#[derive(Clone, Debug)]
pub struct ColumnSetScope {
    parent: ScopeHandle,
    column_set: ColumnSet,
}

impl ColumnSetScope {
    /// Create a new column set scope.
    pub fn new(parent: &ScopeHandle, column_set: ColumnSet) -> Self {
        Self {
            parent: parent.clone(),
            column_set,
        }
    }

    /// Create a new column set scope with no columns.
    pub fn new_empty(parent: &ScopeHandle) -> Self {
        Self {
            parent: parent.clone(),
            column_set: ColumnSet::new(vec![]),
        }
    }

    /// Create a column set scope from a [`TableType`]. This is used to
    /// implement some of the more complicated rules around things like `(...
    /// UNION ALL ...) ORDER BY ...`
    pub fn new_from_table_type(parent: &ScopeHandle, table_type: &TableType) -> Self {
        Self {
            parent: parent.to_owned(),
            column_set: ColumnSet::from_table(None, table_type.to_owned()),
        }
    }

    /// Get our [`ColumnSet`].
    pub fn column_set(&self) -> &ColumnSet {
        &self.column_set
    }

    /// Try to transform the underlying [`ColumnSet`] using `f`.
    pub fn try_transform<F>(self, f: F) -> Result<Self>
    where
        F: FnOnce(ColumnSet) -> Result<ColumnSet>,
    {
        Ok(Self {
            parent: self.parent.clone(),
            column_set: f(self.column_set)?,
        })
    }

    /// Generate a new scope which can be passed to a sub-`SELECT` statement,
    /// which will require a `ScopeHandle` instead of a `ColumnSetScope`.
    ///
    /// This raises a bunch of questions about how BigQuery and standard SQL
    /// handle things:
    ///
    /// 1. Can you reference an aggregate column in a subquery? Let's assume no,
    ///    because it's a weird thing to do and it might not be easy to
    ///    transpile.
    /// 2. Ambiguous column names still need to return errors.
    /// 3. Both scoped and unscoped names should be allowed.
    ///
    /// We could make very different choices about how to implement this, but
    /// this version has the advantage of walking through the entire
    /// [`ColumnSet`] and making decisions.
    pub fn try_into_handle_for_subquery(self) -> Result<ScopeHandle> {
        let mut scope = Scope::new(&self.parent);
        for col in self.column_set.columns {
            // Columns with a table name should be added as both the table name
            // and the bare table name. Both aggregate columns and ambiguous
            // columns should be added as errors using `add_error`.
            if let Some(name) = col.column_name {
                if let ArgumentType::Aggregating(_) = &col.ty {
                    // This is an aggregate type, so add an error instead.
                    scope.add_error(
                        name.to_name(),
                        SourceError::simple(
                            format!(
                                "cannot use aggregate column {} in subquery or ORDER BY clause",
                                name.column.unescaped_bigquery()
                            ),
                            name.column.span(),
                            "aggregate columns cannot be used here",
                        ),
                    );
                    continue;
                }

                // We have a table name, so add the full `table.column` name.
                if name.table.is_some() {
                    scope.add(name.to_name(), Type::Argument(col.ty.clone()))?;
                }

                // Try to add just the column name. If this fails, then add an
                // ambiguous column error.
                if scope
                    .add(name.column.clone(), Type::Argument(col.ty.clone()))
                    .is_err()
                {
                    scope.add_error(
                        name.column.clone(), // Just the column, without the table.
                        SourceError::simple(
                            format!("ambiguous column: {}", name.column.unescaped_bigquery()),
                            name.column.span(),
                            "multiple columns match",
                        ),
                    );
                }
            }
        }
        Ok(scope.into_handle())
    }
}

impl ScopeGet for ColumnSetScope {
    fn get(&self, name: &Name) -> Result<Option<ScopeValue>> {
        match self.column_set.get(name) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => self.parent.get(name),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        tokenizer::Span,
        types::tests::{column_set, ty},
    };

    #[test]
    fn parse_built_in_functions() {
        Scope::root();
    }

    #[test]
    fn matched_by() {
        let column_name = ColumnSetColumnName::new(None, Name::new("a", Span::Unknown));
        assert!(column_name.matched_by(&Name::new("a", Span::Unknown)));
        assert!(!column_name.matched_by(&Name::new("b", Span::Unknown)));
        assert!(!column_name.matched_by(&Name::new_table_column("t", "a", Span::Unknown)));

        let column_name = ColumnSetColumnName::new(
            Some(Name::new("t", Span::Unknown)),
            Name::new("a", Span::Unknown),
        );
        assert!(column_name.matched_by(&Name::new_table_column("t", "a", Span::Unknown)));
        assert!(column_name.matched_by(&Name::new("a", Span::Unknown)));
        assert!(!column_name.matched_by(&Name::new_table_column("t", "b", Span::Unknown)));
    }

    /// Make a column set for a test.
    fn column_set_from(name: &str, table_ty: &str) -> ColumnSet {
        let t1_name = Name::new("t1", Span::Unknown);
        let table_type = ty(table_ty).try_as_table_type(&t1_name).unwrap().clone();
        ColumnSet::from_table(Some(Name::new(name, Span::Unknown)), table_type)
    }

    #[test]
    fn join_column_sets() {
        let left = column_set_from("t1", "TABLE<a INT64, b STRING>");
        let right = column_set_from("t2", "TABLE<a INT64, c STRING>");
        let joined = left.join(&right);
        let expected = column_set("t1.a INT64, t1.b STRING, t2.a INT64, t2.c STRING");
        assert_eq!(joined, expected);
    }

    #[test]
    fn join_using_column_sets() {
        let left = column_set_from("t1", "TABLE<a INT64, b STRING>");
        let right = column_set_from("t2", "TABLE<a FLOAT64, c STRING>");
        let joined = left
            .join_using(&right, &[Name::new("a", Span::Unknown)])
            .unwrap();
        let expected = column_set("a FLOAT64, t1.b STRING, t2.c STRING");
        assert_eq!(joined, expected);
    }

    #[test]
    fn join_with_overlapping_columns_includes_both() {
        let left = column_set_from("t1", "TABLE<a INT64, b STRING>");
        let right = column_set_from("t2", "TABLE<a INT64, b STRING>");
        let joined = left.join(&right);
        let expected = column_set("t1.a INT64, t1.b STRING, t2.a INT64, t2.b STRING");
        assert_eq!(joined, expected);
    }

    #[test]
    fn group_by() {
        let input = column_set("a INT64, b STRING, c INT64");
        let group_by = vec![Name::new("a", Span::Unknown)];
        let output = input.group_by(&group_by).unwrap();
        let expected = column_set("a INT64, b Agg<STRING>, c Agg<INT64>");
        assert_eq!(output, expected);
    }

    #[test]
    fn group_by_empty() {
        let input = column_set("a INT64, b STRING, c INT64");
        let output = input.group_by(&[]).unwrap();
        let expected = column_set("a Agg<INT64>, b Agg<STRING>, c Agg<INT64>");
        assert_eq!(output, expected);
    }

    #[test]
    fn column_set_get() {
        // Check absent, present, and ambiguous columns.
        let column_set = column_set("t1.a INT64, t1.b STRING, t2.b FLOAT64");
        assert_eq!(
            column_set.get(&Name::new("a", Span::Unknown)).unwrap(),
            Some(ty("INT64"))
        );
        assert!(column_set.get(&Name::new("b", Span::Unknown)).is_err());
        assert_eq!(
            column_set
                .get(&Name::new_table_column("t1", "b", Span::Unknown))
                .unwrap(),
            Some(ty("STRING"))
        );
        assert_eq!(
            column_set
                .get(&Name::new_table_column("t2", "b", Span::Unknown))
                .unwrap(),
            Some(ty("FLOAT64"))
        );
        assert_eq!(
            column_set.get(&Name::new("c", Span::Unknown)).unwrap(),
            None
        );
    }
}
