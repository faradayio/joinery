//! Macros used internally by the `joinery` crate. Not intended for use by
//! anything but the `joinery` crate itself.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

use crate::{
    emit::{impl_emit_default_macro, impl_emit_macro},
    spanned::impl_spanned_macro,
    sql_quote::impl_sql_quote,
    to_tokens::impl_to_tokens_macro,
};

mod emit;
mod field_info;
mod spanned;
mod sql_quote;
mod to_tokens;

/// Use `#[derive(Emit)]` to generate a simple implementation of `Emit` that
/// calls `EmitDefault::emit_default`.
///
/// If you need to customize the code that's generated for a specific database,
/// you can implement `Emit` manually and call `EmitDefault::emit_default` for
/// any fields that you don't want to customize.
#[proc_macro_derive(Emit)]
pub fn emit_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_emit_macro(&ast).into()
}

/// Use `#[derive(EmitDefault)]` to generate an implementation of `EmitDefault`
/// that calls `Emit::emit` for each field.
///
/// You should never need to implement `EmitDefault` manually. It exists only to
/// allow `Emit` to call `EmitDefault::emit_default` for any fields that aren't
/// customized. If all fields are customized, then `EmitDefault` doesn't need to
/// be implemented.
#[proc_macro_derive(EmitDefault, attributes(emit))]
pub fn emit_default_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_emit_default_macro(&ast).into()
}

/// Implement `Spanned` for a struct or enum.
#[proc_macro_derive(Spanned)]
pub fn spanned_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_spanned_macro(&ast).into()
}

/// Use `#[sql_quote]` to write SQL queries inline in Rust code, with Rust
/// expressions interpolated into the query using `#expr`. Similar to
/// [`quote::quote!`], except for SQL instead of Rust.
///
/// The output of this macro is a `joinery::tokenizer::TokenStream`, which can
/// then be re-parsed using various methods on
/// `joinery::tokenizer::TokenStream`.
///
/// # Example
///
/// ```no_compile
/// use joinery::sql_quote;
///
/// let table = Ident::new("foo");
/// let query = sql_quote! {
///    SELECT * FROM #table
/// }.try_into_statement()?;
/// ```
#[proc_macro]
pub fn sql_quote(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);
    impl_sql_quote(input).into()
}

/// Use `#[derive(ToTokens)]` to generate an implementation of `ToTokens` that
/// recursively calls `ToTokens::to_tokens` for each field. This is used to
/// convert parsed SQL back into tokens, for use with [`sql_quote!`].
#[proc_macro_derive(ToTokens, attributes(to_tokens))]
pub fn to_owned_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_to_tokens_macro(&ast).into()
}
