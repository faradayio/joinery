//! Quasi-quoting for SQL.
//!
//! This is similar to Rust's `quote` crate. It allows you to write SQL queries
//! inline in Rust code, with Rust expressions interpolated into the query.
//!
//! The output of this macro is a `joinery::tokenizer::TokenStream`, which is
//! used by the `joinery` crate to generate SQL.

use proc_macro2::{Delimiter, TokenStream as TokenStream2, TokenTree};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

// TODO: Make `sql_quote` take a leading span argument, like `quote_spanned`:
//
// ```
// sql_quote!(table.span() => SELECT * FROM #table WHERE id = #id);
// ```
pub(crate) fn impl_sql_quote(input: TokenStream2) -> TokenStream2 {
    let mut sql_token_exprs = vec![];
    emit_sql_token_exprs(&mut sql_token_exprs, input.into_iter());
    let capacity = sql_token_exprs.len();
    quote! {
        {
            use crate::tokenizer::{Literal, Span, Token, TokenStream, ToTokens as _};
            let mut __tokens = Vec::with_capacity(#capacity);
            #( #sql_token_exprs; )*
            TokenStream::from_tokens(__tokens, Span::Unknown)
        }
    }
}

fn emit_sql_token_exprs(
    sql_token_exprs: &mut Vec<TokenStream2>,
    mut tokens: impl Iterator<Item = TokenTree>,
) {
    while let Some(token) = tokens.next() {
        match token {
            // Treat `#` as interpolation.
            TokenTree::Punct(p) if p.to_string() == "#" => {
                if let Some(expr) = tokens.next() {
                    sql_token_exprs.push(quote_spanned! { expr.span() =>
                        (#expr).to_tokens(&mut __tokens)
                    });
                } else {
                    sql_token_exprs.push(quote_spanned! { p.span() =>
                        compile_error!("expected expression after `#`")
                    });
                }
            }
            TokenTree::Group(group) => {
                // We flatten this and use `Punct::new`.
                let (open, close) = delimiter_pair(group.delimiter());
                if let Some(open) = open {
                    sql_token_exprs.push(quote_spanned! { open.span() =>
                        __tokens.push(Token::punct(#open, Span::Unknown))
                    });
                }
                emit_sql_token_exprs(sql_token_exprs, group.stream().into_iter());
                if let Some(close) = close {
                    sql_token_exprs.push(quote_spanned! { close.span() =>
                        __tokens.push(Token::punct(#close, Span::Unknown))
                    });
                }
            }
            TokenTree::Ident(ident) => {
                let ident_str = ident.to_string();
                if let Some(ident_str) = ident_str.strip_prefix("r#") {
                    sql_token_exprs.push(quote_spanned! { ident_str.span() =>
                        __tokens.push(Token::quoted_ident(#ident_str, Span::Unknown))
                    });
                } else {
                    sql_token_exprs.push(quote_spanned! { ident_str.span() =>
                        __tokens.push(Token::ident(#ident_str, Span::Unknown))
                    });
                }
            }
            TokenTree::Punct(punct) => {
                let punct_str = punct.to_string();
                sql_token_exprs.push(quote_spanned! { punct_str.span() =>
                __tokens.push(Token::punct(#punct_str, Span::Unknown)) });
            }
            TokenTree::Literal(lit) => {
                // There's probably a better way to do this.
                let lit: syn::Lit = syn::parse_quote!(#lit);
                match lit {
                    syn::Lit::Int(i) => {
                        sql_token_exprs.push(quote_spanned! { i.span() =>
                            __tokens.push(Token::Literal(Literal::int(#i, Span::Unknown)))
                        });
                    }
                    syn::Lit::Str(s) => {
                        sql_token_exprs.push(quote_spanned! { s.span() =>
                            __tokens.push(Token::Literal(Literal::string(#s, Span::Unknown)))
                        });
                    }
                    syn::Lit::Float(f) => {
                        sql_token_exprs.push(quote_spanned! { f.span() =>
                            __tokens.push(Token::Literal(Literal::float(#f, Span::Unknown)))
                        });
                    }
                    // syn::Lit::ByteStr(_) => todo!(),
                    // syn::Lit::Byte(_) => todo!(),
                    // syn::Lit::Char(_) => todo!(),
                    // syn::Lit::Bool(_) => todo!(),
                    // syn::Lit::Verbatim(_) => todo!(),
                    _ => {
                        sql_token_exprs.push(quote_spanned! {
                            lit.span() =>
                            compile_error!("unsupported literal type")
                        });
                    }
                }
            }
        }
    }
}

fn delimiter_pair(d: Delimiter) -> (Option<&'static str>, Option<&'static str>) {
    match d {
        Delimiter::Parenthesis => (Some("("), Some(")")),
        Delimiter::Brace => (Some("{"), Some("}")),
        Delimiter::Bracket => (Some("["), Some("]")),
        Delimiter::None => (None, None),
    }
}
