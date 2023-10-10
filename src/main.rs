use std::path::PathBuf;

use clap::Parser;

mod ast;
mod cmd;
mod drivers;
mod errors;

use cmd::{parse::cmd_parse, sql_test::cmd_sql_test};
use errors::Result;

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

fn main() -> Result<()> {
    let opt = Opt::parse();
    match opt {
        Opt::Parse { csv_path } => cmd_parse(&csv_path),
        Opt::SqlTest { dir_path } => cmd_sql_test(&dir_path),
    }
}
