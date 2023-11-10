//! Run our SQL test suite.

use std::{
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anstream::{print, println};
use clap::Parser;
use once_cell::sync::Lazy;
use owo_colors::OwoColorize;
use regex::Regex;
use tracing::{instrument, trace};

use crate::{
    ast::{self, parse_sql, CreateTableStatement, CreateViewStatement, Target},
    drivers::{self, Driver},
    errors::{format_err, Context, Error, Result},
    infer::InferTypes,
    known_files::{FileId, KnownFiles},
    scope::Scope,
};

/// Run SQL tests from a directory.
#[derive(Debug, Parser)]
pub struct SqlTestOpt {
    /// A directory containing SQL test files.
    dir_paths: Vec<PathBuf>,

    /// A database locator to run tests against.
    #[clap(long, visible_alias = "db", default_value = "sqlite3::memory:")]
    database: String,

    /// Run pending tests.
    #[clap(long)]
    pending: bool,
}

/// Run our SQL test suite.
#[instrument(skip(opt))]
pub async fn cmd_sql_test(files: &mut KnownFiles, opt: &SqlTestOpt) -> Result<()> {
    // Get a database driver for our target.
    let locator = opt.database.parse::<Box<dyn drivers::Locator>>()?;

    // Keep track of our test results.
    let mut test_ok_count = 0usize;
    let mut test_failures: Vec<(PathBuf, Error)> = vec![];
    let mut pending: Vec<PendingTestInfo> = vec![];

    // Loop over test directories.
    for dir_path in &opt.dir_paths {
        // Build a glob matching our test files, for use with `glob`.
        let dir_path_str = dir_path.as_os_str().to_str().ok_or_else(|| {
            format_err!("Failed to convert path to UTF-8: {}", dir_path.display())
        })?;
        let (base_dir, pattern) = if dir_path.is_dir() {
            (dir_path.clone(), format!("{}/**/*.sql", dir_path_str))
        } else {
            let parent = dir_path.parent().ok_or_else(|| {
                // This should be impossible, since we already checked that this
                // isn't a directory, and should therefore have a parent.
                format_err!("Failed to get parent directory: {}", dir_path.display())
            })?;
            (parent.to_owned(), dir_path_str.to_owned())
        };

        // Loop over test files.
        for entry in glob::glob(&pattern).context("Failed to read test directory")? {
            let path = entry.context("Failed to read test file")?;

            // Read file.
            let file_id = files.add(&path)?;
            let sql = files.source_code(file_id)?;

            // Skip pending tests unless asked to run them.
            if !opt.pending {
                let short_path = path.strip_prefix(&base_dir).unwrap_or(&path);
                if let Some(pending_test_info) =
                    PendingTestInfo::for_target(locator.target(), short_path, sql)
                {
                    progress('P');
                    pending.push(pending_test_info);
                    continue;
                }
            }

            // Test query.
            let mut driver = locator.driver().await?;
            match run_test(&mut *driver, files, file_id).await {
                Ok(_) => {
                    progress('.');
                    test_ok_count += 1;
                }
                Err(e) => {
                    progress('E');
                    test_failures.push((path, e));
                }
            }
        }
    }
    println!();

    for (i, (path, e)) in test_failures.iter().enumerate() {
        println!("\n{} {}: {}", "FAILED".red(), i + 1, path.display());
        e.emit(files);
    }

    if !pending.is_empty() {
        println!("\nPending tests:");
        for p in &pending {
            println!("  {} ({})", p.path.display(), p.comment);
        }
    }

    let result = if test_failures.is_empty() {
        print!("\n{} {} tests passed", "OK:".green(), test_ok_count);
        Ok(())
    } else {
        print!(
            "\n{} {} tests failed, {} passed",
            "FAIL:".red(),
            test_failures.len(),
            test_ok_count,
        );
        Err(Error::Other("Some tests failed".into()))
    };

    if !pending.is_empty() {
        print!(", {} pending", pending.len());
    }
    println!();
    result
}

/// Print a progress indicator for a single test.
fn progress(c: char) {
    print!("{}", c);
    let _ = io::stdout().flush();
}

#[instrument(skip_all)]
async fn run_test(
    driver: &mut dyn Driver,
    files: &mut KnownFiles,
    file_id: FileId,
) -> std::result::Result<(), Error> {
    let mut ast = parse_sql(files, file_id)?;

    // Type check the AST.
    let scope = Scope::root();
    ast.infer_types(&scope)?;

    //eprintln!("SQLite3: {}", ast.emit_to_string(Target::SQLite3));
    let output_tables = find_output_tables(&ast)?;

    // TODO: Verify that all non-output tables are temporary.

    // Clean up any output tables from previous runs that we didn't clean up.
    for OutputTablePair { result, expected } in output_tables.iter() {
        let result = result.unescaped_bigquery();
        let expected = expected.unescaped_bigquery();
        driver.drop_table_if_exists(&result).await?;
        driver.drop_table_if_exists(&expected).await?;
    }

    // Execute the AST and compare output tables.
    driver.execute_ast(&ast).await?;
    for OutputTablePair { result, expected } in output_tables {
        let result = result.unescaped_bigquery();
        let expected = expected.unescaped_bigquery();
        driver.compare_tables(&result, &expected).await?;
        driver.drop_table_if_exists(&result).await?;
        driver.drop_table_if_exists(&expected).await?;
    }
    Ok(())
}

/// Information on a pending test.
struct PendingTestInfo {
    path: PathBuf,
    database: String,
    comment: String,
}

impl PendingTestInfo {
    /// Find the `PendingTestInfo` for a given database.
    fn for_target(target: Target, path: &Path, sql: &str) -> Option<PendingTestInfo> {
        let database = target.to_string();
        Self::all_from_test(path, sql)
            .into_iter()
            .find(|info| info.database == database)
    }

    /// Find all `pending:` lines in a test file.
    fn all_from_test(path: &Path, sql: &str) -> Vec<PendingTestInfo> {
        // Look for lines of the form `-- pending: db1 Comment`.
        static PENDING_RE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?m)^--\s*pending:\s*([a-zA-Z0-9_]+)(\s+.*)?").unwrap());
        PENDING_RE
            .captures_iter(sql)
            .map(|cap| {
                let database = cap.get(1).unwrap().as_str();
                let comment = cap.get(2).map_or("", |m| m.as_str().trim());
                PendingTestInfo {
                    path: path.to_owned(),
                    database: database.to_owned(),
                    comment: comment.to_owned(),
                }
            })
            .collect()
    }
}

/// Tables output by a test suite. This normally stores `ast::TableName`s, but
/// we use `Option<TableName>` while extracting the table names from the AST.
#[derive(Clone, Debug, Default)]
struct OutputTablePair<Name: Clone + fmt::Debug = ast::Name> {
    result: Name,
    expected: Name,
}

/// Find the names of all tables that are output by this query.
fn find_output_tables(ast: &ast::SqlProgram) -> Result<Vec<OutputTablePair>> {
    let mut tables = Vec::<OutputTablePair<Option<ast::Name>>>::default();

    for s in ast.statements.node_iter() {
        let name = match s {
            ast::Statement::CreateTable(CreateTableStatement { table_name, .. }) => table_name,
            ast::Statement::CreateView(CreateViewStatement { view_name, .. }) => view_name,
            _ => continue,
        };
        let unescaped = name.unescaped_bigquery();
        trace!(table = %unescaped, "found output table");

        // Parse output table names. None of these `unwrap()`s should fail.
        static OUTPUT_TABLE_RE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"^__(expected|result)([0-9])+$").unwrap());
        if let Some(caps) = OUTPUT_TABLE_RE.captures(&unescaped) {
            let output_table_type = caps.get(1).unwrap().as_str();
            let idx = caps.get(2).unwrap().as_str().parse::<usize>().unwrap();
            if idx < 1 {
                return Err(format_err!(
                    "output table index must be greater than zero: {}",
                    unescaped
                ));
            }

            // Make sure we have enough space in our vector.
            if idx > tables.len() {
                tables.resize(idx, OutputTablePair::default());
            }
            let lvalue = tables.get_mut(idx - 1).unwrap();

            // Keep track of what we've found.
            match output_table_type {
                "expected" => {
                    if lvalue.expected.is_some() {
                        return Err(format_err!(
                            "duplicate expected output table: {}",
                            unescaped
                        ));
                    }
                    lvalue.expected = Some(name.to_owned());
                }
                "result" => {
                    if lvalue.result.is_some() {
                        return Err(format_err!("duplicate result output table: {}", unescaped));
                    }
                    lvalue.result = Some(name.to_owned());
                }
                _ => unreachable!("should be prevented by regex match"),
            }
        }
    }

    // Convert `OutputTablePair<Option<ast::TableName>>` to
    // `OutputTablePair<ast::TableName>`, returning an error if any of the
    // `expected` or `result` tables are `None`.
    tables
        .into_iter()
        .enumerate()
        .map(|(i, pair)| {
            let OutputTablePair { result, expected } = pair;
            Ok(OutputTablePair {
                result: result.ok_or_else(|| format_err!("missing table __result{}", i + 1))?,
                expected: expected
                    .ok_or_else(|| format_err!("missing table __expected{}", i + 1))?,
            })
        })
        .collect::<Result<Vec<_>>>()
}
