use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

pub(crate) fn impl_spanned_macro(ast: &syn::DeriveInput) -> TokenStream2 {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = &ast.generics.split_for_impl();

    // For now, just use the default fallback impl based on `ToTokens`.
    quote! {
        impl #impl_generics Spanned for #name #ty_generics #where_clause {}
    }
}
