use std::process::exit;

use clap::Parser;

mod analyze;
mod ast;
mod cmd;
mod drivers;
mod errors;
mod util;

use cmd::{
    parse::{cmd_parse, ParseOpt},
    sql_test::{cmd_sql_test, SqlTestOpt},
};

#[derive(Debug, Parser)]
enum Opt {
    /// Parse SQL from a CSV file containing `id` and `query` columns.
    Parse(ParseOpt),
    /// Run SQL tests from a directory.
    SqlTest(SqlTestOpt),
}

fn main() {
    let opt = Opt::parse();
    let result = match opt {
        Opt::Parse(parse_opt) => cmd_parse(&parse_opt),
        Opt::SqlTest(sql_test_opt) => cmd_sql_test(&sql_test_opt),
    };
    if let Err(e) = result {
        e.emit();
        exit(1);
    }
}
