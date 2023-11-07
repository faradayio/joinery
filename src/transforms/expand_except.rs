use std::collections::HashSet;

use derive_visitor::{DriveMut, VisitorMut};
use joinery_macros::sql_quote;

use crate::{
    ast::{self, Name, NodeOrSep, NodeVec, SelectList, SelectListItem},
    errors::{Error, Result},
    tokenizer::Spanned,
};

use super::{Transform, TransformExtra};

/// Transform `* EXCEPT` and `t.* EXCEPT` into an explicit list of columns.
#[derive(Default, VisitorMut)]
#[visitor(SelectList(enter))]
pub struct ExpandExcept {
    error: Option<Error>,
}

impl ExpandExcept {
    fn enter_select_list(&mut self, select_list: &mut ast::SelectList) {
        // If we've already encountered an error, don't bother.
        if self.error.is_some() {
            return;
        }

        // Build a fresh list of select list items, with all EXCEPTs expanded.
        let mut new_items = NodeVec::new(",");
        for item_or_sep in select_list.items.iter() {
            let (ty, table_name, dot, except) = match item_or_sep {
                NodeOrSep::Node(SelectListItem::Wildcard {
                    ty,
                    except: Some(except),
                    ..
                }) => (ty, None, None, except),
                NodeOrSep::Node(SelectListItem::TableNameWildcard {
                    ty,
                    // TODO: I don't think we actually need this? I think the
                    // type checker has done most of the work for us?
                    table_name,
                    dot,
                    except: Some(except),
                    ..
                }) => (ty, Some(table_name), Some(dot), except),
                _ => {
                    new_items.push_node_or_sep(item_or_sep.clone());
                    continue;
                }
            };

            // Build a hash set of names to exclude.
            let exclude = except
                .columns
                .node_iter()
                .cloned()
                .map(Name::from)
                .collect::<HashSet<_>>();

            // Build a list of columns to include.
            let ty = ty
                .clone()
                .expect("must run type inference before ExpandExcept");
            let mut matched_at_least_one_column = false;
            'column: for column in &ty.columns {
                if let Some(column_name) = &column.name {
                    let column_name = Name::from(column_name.clone());
                    if exclude.contains(&column_name) {
                        continue 'column;
                    }
                    matched_at_least_one_column = true;
                    let expression = sql_quote! { #table_name #dot #column_name }
                        .try_into_expression()
                        .expect("could not parse output of sql_quote!");
                    new_items.push(SelectListItem::Expression {
                        expression,
                        alias: None,
                    });
                } else {
                    // We don't know the name of this column, so we can't exclude it.
                    self.error = Some(Error::annotated(
                        "cannot use EXCEPT with unnamed column",
                        except.span(),
                        "used here",
                    ));
                }
            }
            if !matched_at_least_one_column {
                self.error = Some(Error::annotated(
                    "EXCEPT excluded all columns",
                    except.span(),
                    "used here",
                ));
            }
        }
        select_list.items = new_items;
    }
}

impl Transform for ExpandExcept {
    fn name(&self) -> &'static str {
        "ExpandExcept"
    }

    fn requires_types(&self) -> bool {
        true
    }

    fn transform(mut self: Box<Self>, sql_program: &mut ast::SqlProgram) -> Result<TransformExtra> {
        sql_program.drive_mut(self.as_mut());
        if let Some(error) = self.error {
            Err(error)
        } else {
            Ok(TransformExtra::default())
        }
    }
}
