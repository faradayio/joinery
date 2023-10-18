//! Transform `OR REPLACE` to the equivalent `DROP IF EXISTS`.

use crate::{
    ast::{self, CreateTableStatement, CreateViewStatement, NodeOrSep},
    errors::Result,
};

use super::Transform;

/// Transform `OR REPLACE` to the equivalent `DROP IF EXISTS`.
pub struct OrReplaceToDropIfExists;

impl Transform for OrReplaceToDropIfExists {
    fn transform(self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<Vec<String>> {
        let old_statements = sql_program.statements.take();
        for mut node_or_sep in old_statements {
            match &mut node_or_sep {
                NodeOrSep::Node(ast::Statement::CreateTable(CreateTableStatement {
                    create_token,
                    or_replace: or_replace @ Some(_),
                    table_token,
                    table_name,
                    ..
                })) => {
                    // Insert a `DROP TABLE IF EXISTS` statement before the `CREATE TABLE`.
                    sql_program.statements.push(ast::Statement::DropTable(
                        ast::DropTableStatement {
                            // For now, give DROP the same whitespace and source
                            // location as the original CREATE.
                            drop_token: create_token.with_token_str("DROP"),
                            table_token: table_token.clone(),
                            if_exists: Some(if_exists_clause(table_token)),
                            table_name: table_name.clone(),
                        },
                    ));

                    // Remove the `OR REPLACE` clause.
                    *or_replace = None;
                }
                NodeOrSep::Node(ast::Statement::CreateView(CreateViewStatement {
                    create_token,
                    or_replace: or_replace @ Some(_),
                    view_token,
                    view_name,
                    ..
                })) => {
                    // Insert a `DROP VIEW IF EXISTS` statement before the `CREATE VIEW`.
                    sql_program
                        .statements
                        .push(ast::Statement::DropView(ast::DropViewStatement {
                            // For now, give DROP the same whitespace and source
                            // location as the original CREATE.
                            drop_token: create_token.with_token_str("DROP"),
                            view_token: view_token.clone(),
                            if_exists: Some(if_exists_clause(view_token)),
                            view_name: view_name.clone(),
                        }));

                    // Remove the `OR REPLACE` clause.
                    *or_replace = None;
                }
                _ => {}
            }
            sql_program.statements.push_node_or_sep(node_or_sep);
        }
        Ok(vec![])
    }
}

/// Genrate an `IF EXISTS` clause for a `DROP` statement.
fn if_exists_clause(token_for_span: &mut ast::Token) -> ast::IfExists {
    // TODO: We really need to formalize how we create synthetic tokens.
    ast::IfExists {
        if_token: token_for_span.with_token_str("IF").ensure_ws().into_owned(),
        exists_token: token_for_span
            .with_token_str("EXISTS")
            .ensure_ws()
            .into_owned(),
    }
}
