use std::path::{Path, PathBuf};

use clap::Parser;
use serde::Deserialize;

mod ast;
mod errors;
mod unnest;

use errors::{Context, Error, Result, SourceError};

#[derive(Debug, Parser)]
enum Opt {
    /// Parse SQL from a CSV file containing `id` and `query` columns.
    Parse {
        /// A CSV file containing `id` and `query` columns.
        csv_path: PathBuf,
    },
    /// Run SQL tests from a directory.
    SqlTest {
        /// A directory containing SQL test files.
        dir_path: PathBuf,
    },
}

/// A row in our CSV file.
#[derive(Debug, Deserialize)]
struct Row {
    /// Query ID.
    id: String,
    /// Query text.
    query: String,
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    match opt {
        Opt::Parse { csv_path } => cmd_parse(&csv_path),
        Opt::SqlTest { dir_path } => cmd_sql_test(&dir_path),
    }
}

fn cmd_parse(csv_path: &Path) -> Result<()> {
    // Keep track of how many rows we've processed and how many queries we've
    // successfully parsed.
    let mut row_count = 0;
    let mut ok_count = 0;

    // Read CSV file.
    let mut rdr = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open CSV file: {}", csv_path.display()))?;
    for result in rdr.deserialize() {
        row_count += 1;
        let row: Row =
            result.with_context(|| format!("Failed to parse CSV file: {}", csv_path.display()))?;

        // Parse query.
        match ast::parse_sql(&row.query) {
            Ok(_) => {
                ok_count += 1;
                println!("OK {}", row.id);
            }
            Err(e) => {
                println!("ERR {}", row.id);
                e.emit();
            }
        }
    }

    println!("Parsed {} of {} queries", ok_count, row_count);

    Ok(())
}

fn cmd_sql_test(dir_path: &Path) -> Result<()> {
    let mut test_count = 0usize;
    let mut test_failures: Vec<(PathBuf, Box<SourceError>)> = vec![];

    // Read directory using `glob`.
    let pattern = format!("{}/**/*.sql", dir_path.display());
    for entry in glob::glob(&pattern).context("Failed to read test directory")? {
        let path = entry.context("Failed to read test file")?;
        test_count += 1;

        // Read file.
        let query = std::fs::read_to_string(&path).context("Failed to read test file")?;

        // Test query.
        match ast::parse_sql(&query) {
            Ok(_) => {
                print!(".");
            }
            Err(Error::Source(e)) => {
                print!("E");
                test_failures.push((path, e));
            }
            Err(err) => return Err(err),
        }
        println!();
    }

    if test_count == 0 {
        Err(Error::Other("No tests found".into()))
    } else if test_failures.is_empty() {
        println!("OK: {} tests passed", test_count);
        Ok(())
    } else {
        println!(
            "FAIL: {} of {} tests failed",
            test_failures.len(),
            test_count
        );
        for (i, (path, e)) in test_failures.iter().enumerate() {
            println!("FAILED {}: {}", i + 1, path.display());
            e.emit();
        }
        Err(Error::Other("Some tests failed".into()))
    }
}
