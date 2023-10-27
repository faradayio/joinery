use std::process::exit;

use clap::Parser;

mod analyze;
mod ast;
mod cmd;
mod drivers;
mod errors;
mod infer;
mod known_files;
mod scope;
mod tokenizer;
mod transforms;
mod types;
mod unification;
mod util;

use cmd::{
    parse::{cmd_parse, ParseOpt},
    sql_test::{cmd_sql_test, SqlTestOpt},
    transpile::{cmd_transpile, TranspileOpt},
};
use known_files::KnownFiles;
use tracing::info_span;

#[derive(Debug, Parser)]
enum Opt {
    /// Parse SQL from a CSV file containing `id` and `query` columns.
    Parse(ParseOpt),
    /// Run SQL tests from a directory.
    SqlTest(SqlTestOpt),
    /// Transpile BigQuery SQL to another dialect.
    Transpile(TranspileOpt),
}

#[tokio::main]
async fn main() {
    // Configure tracing.
    tracing_subscriber::fmt::init();
    let _span = info_span!("joinery").entered();

    let opt = Opt::parse();
    let mut files = KnownFiles::new();
    let result = match opt {
        Opt::Parse(parse_opt) => cmd_parse(&mut files, &parse_opt),
        Opt::SqlTest(sql_test_opt) => cmd_sql_test(&mut files, &sql_test_opt).await,
        Opt::Transpile(transpile_opt) => cmd_transpile(&mut files, &transpile_opt).await,
    };
    if let Err(e) = result {
        e.emit(&files);
        exit(1);
    }
}
