//! Run our SQL test suite.

use std::{
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
};

use clap::Parser;
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::instrument;

use crate::{
    ast::{self, parse_sql, CreateTableStatement, CreateViewStatement},
    drivers::{self, Driver},
    errors::{format_err, Context, Error, Result},
};

/// Run SQL tests from a directory.
#[derive(Debug, Parser)]
pub struct SqlTestOpt {
    /// A directory containing SQL test files.
    dir_path: PathBuf,

    /// A database locator to run tests against.
    #[clap(long, visible_alias = "db", default_value = "sqlite3::memory:")]
    database: String,

    /// Run pending tests.
    #[clap(long)]
    pending: bool,
}

/// Run our SQL test suite.
#[instrument(skip(opt))]
pub async fn cmd_sql_test(opt: &SqlTestOpt) -> Result<()> {
    // Get a database driver for our target.
    let locator = opt.database.parse::<Box<dyn drivers::Locator>>()?;
    let mut driver = locator.driver().await?;

    // Keep track of our test results.
    let mut test_count = 0usize;
    let mut test_failures: Vec<(PathBuf, Error)> = vec![];
    let mut pending_paths: Vec<PathBuf> = vec![];

    // Build a glob matching our test files, for use with `glob`.
    let dir_path_str = opt.dir_path.as_os_str().to_str().ok_or_else(|| {
        format_err!(
            "Failed to convert path to UTF-8: {}",
            opt.dir_path.display()
        )
    })?;
    let (base_dir, pattern) = if opt.dir_path.is_dir() {
        (opt.dir_path.clone(), format!("{}/**/*.sql", dir_path_str))
    } else {
        let parent = opt.dir_path.parent().ok_or_else(|| {
            // This should be impossible, since we already checked that this
            // isn't a directory, and should therefore have a parent.
            format_err!("Failed to get parent directory: {}", opt.dir_path.display())
        })?;
        (parent.to_owned(), dir_path_str.to_owned())
    };

    // Loop over test files.
    for entry in glob::glob(&pattern).context("Failed to read test directory")? {
        let path = entry.context("Failed to read test file")?;
        test_count += 1;

        // Read file.
        let query = std::fs::read_to_string(&path).context("Failed to read test file")?;

        // Skip pending tests unless asked to run them.
        if !opt.pending {
            // Look for lines of the form `-- pending: db1, db2, ...`.
            static PENDING_RE: Lazy<Regex> = Lazy::new(|| {
                Regex::new(r"(?m)^--\s*pending:\s*([a-zA-Z0-9_][a-zA-Z0-9_, ]*)").unwrap()
            });
            let target_string = driver.target().to_string();
            if let Some(caps) = PENDING_RE.captures(&query) {
                let dbs = caps.get(1).unwrap().as_str();
                if dbs.split(',').any(|db| db.trim() == target_string) {
                    print!("P");
                    let _ = io::stdout().flush();

                    pending_paths.push(
                        path.strip_prefix(&base_dir)
                            .unwrap_or_else(|_| &path)
                            .to_owned(),
                    );

                    continue;
                }
            }
        }

        // Test query.
        match run_test(&mut *driver, &path, &query).await {
            Ok(_) => {
                print!(".");
                let _ = io::stdout().flush();
            }
            Err(e) => {
                print!("E");
                let _ = io::stdout().flush();
                test_failures.push((path, e));
            }
        }
    }
    println!();

    for (i, (path, e)) in test_failures.iter().enumerate() {
        println!("\nFAILED {}: {}", i + 1, path.display());
        e.emit();
    }

    if !pending_paths.is_empty() {
        println!("\nPending tests:");
        for path in &pending_paths {
            println!("  {}", path.display());
        }
    }

    if test_count == 0 {
        Err(Error::Other("No tests found".into()))
    } else if test_failures.is_empty() {
        print!("\nOK: {} tests passed", test_count);
        if !pending_paths.is_empty() {
            print!(", {} pending", pending_paths.len());
        }
        println!();
        Ok(())
    } else {
        print!(
            "\nFAIL: {} of {} tests failed",
            test_failures.len(),
            test_count,
        );
        if !pending_paths.is_empty() {
            print!(", {} pending", pending_paths.len());
        }
        println!();

        Err(Error::Other("Some tests failed".into()))
    }
}

#[instrument(skip_all, fields(path = %path.display()))]
async fn run_test(
    driver: &mut dyn Driver,
    path: &Path,
    sql: &str,
) -> std::result::Result<(), Error> {
    let ast = parse_sql(&path.display().to_string(), sql)?;
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

/// Tables output by a test suite. This normally stores `ast::TableName`s, but
/// we use `Option<TableName>` while extracting the table names from the AST.
#[derive(Clone, Debug, Default)]
struct OutputTablePair<Name: Clone + fmt::Debug = ast::TableName> {
    result: Name,
    expected: Name,
}

/// Find the names of all tables that are output by this query.
fn find_output_tables(ast: &ast::SqlProgram) -> Result<Vec<OutputTablePair>> {
    let mut tables = Vec::<OutputTablePair<Option<ast::TableName>>>::default();

    for s in &ast.statements {
        let name = match s {
            ast::Statement::CreateTable(CreateTableStatement { table_name, .. }) => table_name,
            ast::Statement::CreateView(CreateViewStatement { view_name, .. }) => view_name,
            _ => continue,
        };
        let unescaped = name.unescaped_bigquery();

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