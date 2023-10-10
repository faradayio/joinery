//! Run our SQL test suite.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use crate::{
    ast::{self, parse_sql, CreateTableStatement, CreateViewStatement},
    drivers::{self, Driver},
    errors::{format_err, Context, Error, Result},
};

/// Run our SQL test suite.
pub fn cmd_sql_test(dir_path: &Path) -> Result<()> {
    // Get a database driver for our target.
    let driver = drivers::driver_for_target(ast::Target::SQLite3)?;

    // Keep track of our test results.
    let mut test_count = 0usize;
    let mut test_failures: Vec<(PathBuf, Error)> = vec![];

    // Read directory using `glob`.
    let pattern = format!("{}/**/*.sql", dir_path.display());
    for entry in glob::glob(&pattern).context("Failed to read test directory")? {
        let path = entry.context("Failed to read test file")?;
        test_count += 1;

        // Read file.
        let query = std::fs::read_to_string(&path).context("Failed to read test file")?;

        // Test query.
        match run_test(&*driver, &query) {
            Ok(_) => {
                print!(".");
            }
            Err(e) => {
                print!("E");
                test_failures.push((path, e));
            }
        }
    }
    println!();

    if test_count == 0 {
        Err(Error::Other("No tests found".into()))
    } else if test_failures.is_empty() {
        println!("OK: {} tests passed", test_count);
        Ok(())
    } else {
        for (i, (path, e)) in test_failures.iter().enumerate() {
            println!("\nFAILED {}: {}", i + 1, path.display());
            e.emit();
        }

        println!(
            "FAIL: {} of {} tests failed",
            test_failures.len(),
            test_count
        );

        Err(Error::Other("Some tests failed".into()))
    }
}

fn run_test(driver: &dyn Driver, sql: &str) -> std::result::Result<(), Error> {
    let ast = parse_sql(sql)?;
    //eprintln!("SQLite3: {}", ast.emit_to_string(Target::SQLite3));
    let output_tables = find_output_tables(&ast)?;

    // TODO: Verify that all non-output tables are temporary.

    // Clean up any output tables from previous runs that we didn't clean up.
    for OutputTablePair { result, expected } in output_tables.iter() {
        let result = result.unescaped_bigquery();
        let expected = expected.unescaped_bigquery();
        driver.drop_table_if_exists(&result)?;
        driver.drop_table_if_exists(&expected)?;
    }

    // Execute the AST and compare output tables.
    driver.execute_ast(&ast)?;
    for OutputTablePair { result, expected } in output_tables {
        let result = result.unescaped_bigquery();
        let expected = expected.unescaped_bigquery();
        driver.compare_tables(&result, &expected)?;
        driver.drop_table_if_exists(&result)?;
        driver.drop_table_if_exists(&expected)?;
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
        static OUTPUT_TABLE_RE: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| {
                regex::Regex::new(r"^__(expected|result)([0-9])+$").unwrap()
            });
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
