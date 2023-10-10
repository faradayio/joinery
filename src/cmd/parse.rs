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
    let mut ok_count = 0;

    // We can optionally count function calls.
    let mut function_call_counts = FunctionCallCounts::default();

    // Read CSV file.
    let mut rdr = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open CSV file: {}", csv_path.display()))?;
    for result in rdr.deserialize() {
        row_count += 1;
        let row: Row =
            result.with_context(|| format!("Failed to parse CSV file: {}", csv_path.display()))?;

        // Parse query.
        match ast::parse_sql(&row.query) {
            Ok(sql_program) => {
                ok_count += 1;
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

    println!("Parsed {} of {} queries", ok_count, row_count);

    if count_function_calls {
        println!();
        println!("Function call counts:");
        for (function_name, count) in function_call_counts.counts() {
            println!("  {:>5} {}", count, function_name);
        }
    }

    Ok(())
}
