use std::path::PathBuf;

use clap::Parser;
use tracing::instrument;

use crate::{
    ast::parse_sql, drivers, errors::Result, infer::InferTypes, known_files::KnownFiles,
    scope::Scope,
};

/// Run an SQL file using the specified database.
#[derive(Debug, Parser)]
pub struct RunOpt {
    /// An SQL file to transpile.
    sql_path: PathBuf,

    /// A database locator to run tests against. (For now, this must be a
    /// an actual database locator, and not the name of a dialect.)
    #[clap(long, visible_alias = "db", default_value = "sqlite3::memory:")]
    database: String,
}

/// Run our SQL test suite.
///
/// TODO: Deduplicate this with the transpile command.
#[instrument(skip(opt))]
pub async fn cmd_run(files: &mut KnownFiles, opt: &RunOpt) -> Result<()> {
    // Get a database driver for our target.
    let locator = opt.database.parse::<Box<dyn drivers::Locator>>()?;
    let mut driver = locator.driver().await?;

    // Parse our SQL.
    let file_id = files.add(&opt.sql_path)?;
    let mut ast = parse_sql(files, file_id)?;

    // Run the type checker, but do not fail on errors.
    let scope = Scope::root();
    if let Err(err) = ast.infer_types(&scope) {
        err.emit(files);
        eprintln!("\nType checking failed. Manual fixes will probably be required!");
    }

    // Run our AST.
    driver.execute_ast(&ast).await
}
