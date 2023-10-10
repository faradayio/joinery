//! Parse queries from a CSV file.

use std::path::Path;

use serde::Deserialize;

use crate::{
    ast,
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
pub fn cmd_parse(csv_path: &Path) -> Result<()> {
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
