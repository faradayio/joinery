//! Parse queries from a CSV file.

use std::path::Path;

use serde::Deserialize;

use crate::{
    analyze::FunctionCallCounts,
    ast::{self},
    errors::{Context, Result},
};

/// A row in our CSV file.
#[derive(Debug, Deserialize)]
struct Row {
    /// Query ID.
    id: String,
    /// Query text.
    query: String,
}

/// Parse queries from a CSV file.
pub fn cmd_parse(csv_path: &Path, count_function_calls: bool) -> Result<()> {
    // Keep track of how many rows we've processed and how many queries we've
    // successfully parsed.
    let mut row_count = 0;
    let mut line_count = 0;
    let mut ok_count = 0;
    let mut ok_line_count = 0;
    let mut ml_count = 0;

    // We can optionally count function calls.
    let mut function_call_counts = FunctionCallCounts::default();

    // Read CSV file.
    let mut rdr = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open CSV file: {}", csv_path.display()))?;
    for result in rdr.deserialize() {
        let row: Row =
            result.with_context(|| format!("Failed to parse CSV file: {}", csv_path.display()))?;

        // Skip ML queries, which we don't translate to other databases,
        // anyways.
        if row.query.contains("FROM ML.") {
            ml_count += 1;
            continue;
        } else {
            row_count += 1;
            line_count += row.query.lines().count();
        }

        // Parse query.
        let filename = format!("{}/{}", csv_path.display(), row.id);
        match ast::parse_sql(&filename, &row.query) {
            Ok(sql_program) => {
                ok_count += 1;
                ok_line_count += row.query.lines().count();
                println!("OK {}", row.id);
                if count_function_calls {
                    function_call_counts.visit(&sql_program);
                }
            }
            Err(e) => {
                println!("ERR {}", row.id);
                e.emit();
            }
        }
    }

    println!(
        "Parsed {} of {} queries, skipped {} with `ML.`. Parsed {}/{} lines.",
        ok_count, row_count, ml_count, ok_line_count, line_count
    );

    if count_function_calls {
        println!();
        println!("Function call counts:");
        for (function_name, count) in function_call_counts.counts() {
            println!("  {:>5} {}", count, function_name);
        }
    }

    Ok(())
}
