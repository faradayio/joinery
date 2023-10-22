use darling::{util::Flag, FromField};
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::field_info::{AttrWithSkip, FieldInfo};

/// The contents of a `#[to_tokens(...)]` attribute, parsed using the
/// [`darling`](https://github.com/TedDriggs/darling) crate.
#[derive(Default, FromField)]
#[darling(default, attributes(to_tokens))]
struct ToTokensAttr {
    /// Should we omit this field from our output?
    skip: Flag,
}

impl AttrWithSkip for ToTokensAttr {
    fn skip(&self) -> &Flag {
        &self.skip
    }
}

/// [`ToTokens::to_tokens`] should just iterate over the struct or enum, and
/// generate a recursive call to [`ToTokens::to_tokens`] for each field.
///
/// If we see `#[to_tokens(skip)]` on a field, we should skip it.
pub(crate) fn impl_to_tokens_macro(ast: &syn::DeriveInput) -> TokenStream2 {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = &ast.generics.split_for_impl();
    let implementation = to_tokens_body(name, &ast.data);
    quote! {
        impl #impl_generics ToTokens for #name #ty_generics #where_clause {
            fn to_tokens(&self, tokens: &mut Vec<Token>) {
                #implementation
            }
        }
    }
}

fn to_tokens_body(name: &syn::Ident, data: &syn::Data) -> TokenStream2 {
    match data {
        syn::Data::Struct(s) => to_tokens_body_struct(s),
        syn::Data::Enum(e) => to_tokens_body_enum(name, e),
        syn::Data::Union(_) => panic!("Cannot derive ToTokens for unions"),
    }
}

fn to_tokens_body_struct(data: &syn::DataStruct) -> TokenStream2 {
    match &data.fields {
        syn::Fields::Named(fields) => {
            let field_names = FieldInfo::<ToTokensAttr>::named_iter(fields)
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.struct_field());
            quote! {
                #( self.#field_names.to_tokens(tokens); )*
            }
        }
        syn::Fields::Unnamed(fields) => {
            let field_names = FieldInfo::<ToTokensAttr>::unnamed_iter(fields)
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.struct_field());
            quote! {
                #( self.#field_names.to_tokens(tokens); )*
            }
        }
        syn::Fields::Unit => quote! {},
    }
}

fn to_tokens_body_enum(name: &syn::Ident, data: &syn::DataEnum) -> TokenStream2 {
    let variants = data
        .variants
        .iter()
        .map(|v| to_tokens_body_enum_variant(name, v));
    quote! {
        match self {
            #( #variants )*
        }
    }
}

fn to_tokens_body_enum_variant(name: &syn::Ident, variant: &syn::Variant) -> TokenStream2 {
    let variant_name = &variant.ident;
    match &variant.fields {
        syn::Fields::Named(fields) => {
            let fields = FieldInfo::<ToTokensAttr>::named_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.enum_name());
            quote! {
                #name::#variant_name { #(#patterns),* } => {
                    #( #field_names.to_tokens(tokens); )*
                }
            }
        }
        syn::Fields::Unnamed(fields) => {
            let fields = FieldInfo::<ToTokensAttr>::unnamed_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.enum_name());
            quote! {
                #name::#variant_name( #(#patterns),* ) => {
                    #( #field_names.to_tokens(tokens); )*
                }
            }
        }
        syn::Fields::Unit => quote! {},
    }
}
