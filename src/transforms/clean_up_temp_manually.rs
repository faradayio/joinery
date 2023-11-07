use crate::{ast, errors::Result};

use super::{Transform, TransformExtra};

/// Transform `OR REPLACE` to the equivalent `DROP IF EXISTS`.
pub struct CleanUpTempManually {
    /// Format a table or view name.
    pub format_name: &'static dyn Fn(&ast::Name) -> String,
}

impl Transform for CleanUpTempManually {
    fn name(&self) -> &'static str {
        "CleanUpTempManually"
    }

    fn transform(self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        let mut native_teardown_sql = vec![];

        #[allow(clippy::single_match)]
        for statement in sql_program.statements.node_iter_mut() {
            match statement {
                ast::Statement::CreateTable(ast::CreateTableStatement {
                    temporary: temporary @ Some(_),
                    table_name,
                    ..
                }) => {
                    *temporary = None;
                    native_teardown_sql.push(format!(
                        "DROP TABLE IF EXISTS {}",
                        (self.format_name)(table_name)
                    ));
                }
                _ => {}
            }
        }

        Ok(TransformExtra {
            native_setup_sql: vec![],
            native_teardown_sql,
        })
    }
}
