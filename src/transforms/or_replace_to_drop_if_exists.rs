//! Transform `OR REPLACE` to the equivalent `DROP IF EXISTS`.

use joinery_macros::sql_quote;

use crate::{
    ast::{self, CreateTableStatement, CreateViewStatement, NodeOrSep},
    errors::Result,
};

use super::{Transform, TransformExtra};

/// Transform `OR REPLACE` to the equivalent `DROP IF EXISTS`.
pub struct OrReplaceToDropIfExists;

impl Transform for OrReplaceToDropIfExists {
    fn name(&self) -> &'static str {
        "OrReplaceToDropIfExists"
    }

    fn transform(self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        let old_statements = sql_program.statements.take();
        for mut node_or_sep in old_statements {
            match &mut node_or_sep {
                NodeOrSep::Node(ast::Statement::CreateTable(CreateTableStatement {
                    or_replace: or_replace @ Some(_),
                    table_name,
                    ..
                })) => {
                    // Insert a `DROP TABLE IF EXISTS` statement before the `CREATE TABLE`.
                    let drop_statement = sql_quote! {
                        DROP TABLE IF EXISTS #table_name
                    }
                    .try_into_statement()
                    .expect("generated SQL should always parse");
                    sql_program.statements.push(drop_statement);

                    // Remove the `OR REPLACE` clause.
                    *or_replace = None;
                }
                NodeOrSep::Node(ast::Statement::CreateView(CreateViewStatement {
                    or_replace: or_replace @ Some(_),
                    view_name,
                    ..
                })) => {
                    let drop_statement = sql_quote! {
                        DROP VIEW IF EXISTS #view_name
                    }
                    .try_into_statement()
                    .expect("generated SQL should always parse");
                    // Insert a `DROP VIEW IF EXISTS` statement before the `CREATE VIEW`.
                    sql_program.statements.push(drop_statement);

                    // Remove the `OR REPLACE` clause.
                    *or_replace = None;
                }
                _ => {}
            }
            sql_program.statements.push_node_or_sep(node_or_sep);
        }
        Ok(TransformExtra::default())
    }
}
