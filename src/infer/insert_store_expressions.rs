//! A preliminary, once-only type inference step where we patch up the AST
//! to include [`ast::StoreExpression`].

use crate::{
    ast::{self},
    errors::Result,
};

use super::nyi;

/// Walk an AST tree, inserting [`ast::StoreExpression`] everywhere we need it.
///
/// This is called only once, before the first time we run type inference.
pub trait InsertStoreExpressions {
    /// Find all the places that need a [`ast::StoreExpression`] and insert them.
    fn insert_store_expressions(&mut self) -> Result<()>;
}

impl InsertStoreExpressions for ast::SqlProgram {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.statements.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::Statement {
    fn insert_store_expressions(&mut self) -> Result<()> {
        match self {
            ast::Statement::Query(stmt) => stmt.insert_store_expressions(),
            ast::Statement::DeleteFrom(_) => Ok(()),
            ast::Statement::InsertInto(stmt) => stmt.insert_store_expressions(),
            ast::Statement::CreateTable(stmt) => stmt.insert_store_expressions(),
            // This is a problem for another day and another poor developer. Do
            // views output values in memory format or backend-specific storage
            // format?
            ast::Statement::CreateView(_) => Err(nyi(self, "CREATE VIEW storage expressions")),
            ast::Statement::DropTable(_) => Ok(()),
            ast::Statement::DropView(_) => Ok(()),
        }
    }
}
impl InsertStoreExpressions for ast::QueryStatement {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.query_expression.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::QueryExpression {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.query.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::QueryExpressionQuery {
    fn insert_store_expressions(&mut self) -> Result<()> {
        match self {
            ast::QueryExpressionQuery::Select(expr) => expr.insert_store_expressions(),
            ast::QueryExpressionQuery::Nested { query, .. } => query.insert_store_expressions(),
            ast::QueryExpressionQuery::SetOperation { left, right, .. } => {
                left.insert_store_expressions()?;
                right.insert_store_expressions()
            }
        }
    }
}

impl InsertStoreExpressions for ast::SelectExpression {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.select_list.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::SelectList {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.items.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::SelectListItem {
    fn insert_store_expressions(&mut self) -> Result<()> {
        match self {
            ast::SelectListItem::Expression { expression, .. } => {
                expression.insert_store_expressions()
            }
            ast::SelectListItem::Wildcard { .. } => {
                Err(nyi(self, "InsertStoreExpressions(Wildcard)"))
            }
            ast::SelectListItem::TableNameWildcard { .. } => {
                Err(nyi(self, "InsertStoreExpressions(TableNameWildcard)"))
            }
            ast::SelectListItem::ExpressionWildcard { .. } => {
                Err(nyi(self, "InsertStoreExpressions(TableNameWildcard)"))
            }
        }
    }
}

impl InsertStoreExpressions for ast::Expression {
    /// Wrap ourselves in a `StoreExpression`. Not recursive!
    fn insert_store_expressions(&mut self) -> Result<()> {
        let store_expr = ast::Expression::Store(ast::StoreExpression {
            memory_type: None,
            expression: Box::new(self.clone()),
        });
        *self = store_expr;
        Ok(())
    }
}

impl InsertStoreExpressions for ast::InsertIntoStatement {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.inserted_data.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::InsertedData {
    fn insert_store_expressions(&mut self) -> Result<()> {
        match self {
            ast::InsertedData::Values { rows, .. } => rows.insert_store_expressions(),
            ast::InsertedData::Select { query, .. } => query.insert_store_expressions(),
        }
    }
}

impl InsertStoreExpressions for ast::ValuesRow {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.expressions.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::CreateTableStatement {
    fn insert_store_expressions(&mut self) -> Result<()> {
        self.definition.insert_store_expressions()
    }
}

impl InsertStoreExpressions for ast::CreateTableDefinition {
    fn insert_store_expressions(&mut self) -> Result<()> {
        match self {
            // We don't need to do anything here because we aren't actually
            // storing anything. It is a plain column definition.
            ast::CreateTableDefinition::Columns { .. } => Ok(()),
            ast::CreateTableDefinition::As {
                query_statement, ..
            } => query_statement.insert_store_expressions(),
        }
    }
}

impl<T: InsertStoreExpressions + ast::Node> InsertStoreExpressions for ast::NodeVec<T> {
    fn insert_store_expressions(&mut self) -> Result<()> {
        for item in self.node_iter_mut() {
            item.insert_store_expressions()?;
        }
        Ok(())
    }
}
