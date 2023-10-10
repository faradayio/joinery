use std::{path::PathBuf, process::exit};

use clap::Parser;

mod ast;
mod cmd;
mod drivers;
mod errors;

use cmd::{parse::cmd_parse, sql_test::cmd_sql_test};

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

fn main() {
    let opt = Opt::parse();
    let result = match opt {
        Opt::Parse { csv_path } => cmd_parse(&csv_path),
        Opt::SqlTest { dir_path } => cmd_sql_test(&dir_path),
    };
    if let Err(e) = result {
        e.emit();
        exit(1);
    }
}
