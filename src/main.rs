use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term::{
        self,
        termcolor::{ColorChoice, StandardStream},
    },
};
use serde::Deserialize;

mod ast;

#[derive(Debug, Parser)]
struct Opt {
    /// A CSV file containing `id` and `query` columns.
    csv_path: PathBuf,
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
    let csv_path = &opt.csv_path;

    // Keep track of how many rows we've processed and how many queries we've
    // successfully parsed.
    let mut row_count = 0;
    let mut ok_count = 0;

    // Prepare to report errors.
    let writer = StandardStream::stderr(ColorChoice::Always);
    let config = term::Config::default();

    // Read CSV file.
    let mut rdr = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open CSV file: {}", csv_path.display()))?;
    for result in rdr.deserialize() {
        row_count += 1;
        let row: Row =
            result.with_context(|| format!("Failed to parse CSV file: {}", csv_path.display()))?;

        // Parse query.
        match ast::sql_program::sql_program(&row.query) {
            Ok(_) => {
                ok_count += 1;
                println!("OK {}", row.id);
            }
            Err(e) => {
                let mut files = SimpleFiles::new();
                let file_id = files.add(&row.id, row.query);
                let diagnostic = Diagnostic::error()
                    .with_message(format!("Failed to parse query {}", row.id))
                    .with_labels(vec![Label::primary(
                        file_id,
                        e.location.offset..e.location.offset + 1,
                    )
                    .with_message(format!("expected {}", e.expected))]);
                term::emit(&mut writer.lock(), &config, &files, &diagnostic)?;
            }
        }
    }

    println!("Parsed {} of {} queries", ok_count, row_count);

    Ok(())
}
