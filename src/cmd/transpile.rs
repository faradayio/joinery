//! Transpile code to another dialect of SQL.

use std::path::PathBuf;

use clap::Parser;
use tracing::instrument;

use crate::{
    ast::{parse_sql, Emit},
    drivers,
    errors::Result,
    infer::InferTypes,
    known_files::KnownFiles,
    scope::Scope,
};

/// Run SQL tests from a directory.
#[derive(Debug, Parser)]
pub struct TranspileOpt {
    /// An SQL file to transpile.
    sql_path: PathBuf,

    /// A database locator to run tests against. (For now, this must be a
    /// an actual database locator, and not the name of a dialect.)
    #[clap(long, visible_alias = "db", default_value = "sqlite3::memory:")]
    database: String,
}

/// Run our SQL test suite.
#[instrument(skip(opt))]
pub async fn cmd_transpile(files: &mut KnownFiles, opt: &TranspileOpt) -> Result<()> {
    // Get a database driver for our target.
    let locator = opt.database.parse::<Box<dyn drivers::Locator>>()?;
    let driver = locator.driver().await?;

    // Parse our SQL.
    let file_id = files.add(&opt.sql_path)?;
    let mut ast = parse_sql(files, file_id)?;

    // Run the type checker, but do not fail on errors.
    let scope = Scope::root();
    if let Err(err) = ast.infer_types(&scope) {
        err.emit(files);
        eprintln!("\nType checking failed. Manual fixes will probably be required!");
    }

    // Rewrite our AST.
    let rewritten_ast = driver.rewrite_ast(&ast)?;

    // Print our rewritten AST.
    for statement in rewritten_ast.extra.native_setup_sql {
        println!("{};", statement);
    }
    let transpiled_sql = rewritten_ast.ast.emit_to_string(locator.target().await?);
    println!("{}", transpiled_sql);
    for statement in rewritten_ast.extra.native_teardown_sql {
        println!("{};", statement);
    }
    Ok(())
}
