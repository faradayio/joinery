//! Implementations of `#[derive(Emit)]` and `#[derive(EmitDefault)]`.

use darling::{util::Flag, FromField};
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::field_info::{AttrWithSkip, FieldInfo};

/// The contents of an `#[emit(...)]` attribute, parsing using the
/// [`darling`](https://github.com/TedDriggs/darling) crate.
#[derive(Default, FromField)]
#[darling(default, attributes(emit))]
struct EmitAttr {
    /// Should we omit this field from our output?
    skip: Flag,
}

impl AttrWithSkip for EmitAttr {
    fn skip(&self) -> &Flag {
        &self.skip
    }
}

pub(crate) fn impl_emit_macro(ast: &syn::DeriveInput) -> TokenStream2 {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = &ast.generics.split_for_impl();
    quote! {
        impl #impl_generics Emit for #name #ty_generics #where_clause {
            fn emit(&self, t: Target, f: &mut TokenWriter<'_>) -> ::std::io::Result<()> {
                <#name #ty_generics as EmitDefault>::emit_default(self, t, f)
            }
        }
    }
}

/// [`EmitDefault::emit_default`] should just iterate over the struct or enum,
/// and generate a recursive call to [`Emit::emit`] for each field.
///
/// TODO: If we see `#[emit(skip)]` on a field, we should skip it.
pub(crate) fn impl_emit_default_macro(ast: &syn::DeriveInput) -> TokenStream2 {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = &ast.generics.split_for_impl();
    let implementation = emit_default_body(name, &ast.data);
    quote! {
        impl #impl_generics EmitDefault for #name #ty_generics #where_clause {
            fn emit_default(&self, t: Target, f: &mut TokenWriter<'_>) -> ::std::io::Result<()> {
                #implementation
            }
        }
    }
}
fn emit_default_body(name: &syn::Ident, data: &syn::Data) -> TokenStream2 {
    match data {
        syn::Data::Struct(s) => emit_default_body_struct(s),
        syn::Data::Enum(e) => emit_default_body_enum(name, e),
        syn::Data::Union(_) => panic!("Cannot derive EmitDefault for unions"),
    }
}

fn emit_default_body_struct(data: &syn::DataStruct) -> TokenStream2 {
    match &data.fields {
        syn::Fields::Named(fields) => {
            let field_names = FieldInfo::<EmitAttr>::named_iter(fields)
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.struct_field());
            quote! {
                #( self.#field_names.emit(t, f)?; )*
                Ok(())
            }
        }
        syn::Fields::Unnamed(fields) => {
            let field_names = FieldInfo::<EmitAttr>::unnamed_iter(fields)
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.struct_field());
            quote! {
                #( self.#field_names.emit(t, f)?; )*
                Ok(())
            }
        }
        syn::Fields::Unit => quote! { Ok(()) },
    }
}

fn emit_default_body_enum(name: &syn::Ident, data: &syn::DataEnum) -> TokenStream2 {
    let variants = data
        .variants
        .iter()
        .map(|v| emit_default_body_enum_variant(name, v));
    quote! {
        match self {
            #( #variants )*
        }
    }
}

fn emit_default_body_enum_variant(name: &syn::Ident, variant: &syn::Variant) -> TokenStream2 {
    let variant_name = &variant.ident;
    match &variant.fields {
        syn::Fields::Named(fields) => {
            let fields = FieldInfo::<EmitAttr>::named_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.enum_name());
            quote! {
                #name::#variant_name { #(#patterns),* } => {
                    #( #field_names.emit(t, f)?; )*
                    Ok(())
                }
            }
        }
        syn::Fields::Unnamed(fields) => {
            let fields = FieldInfo::<EmitAttr>::unnamed_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip().is_present())
                .map(|f| f.enum_name());
            quote! {
                #name::#variant_name( #(#patterns),* ) => {
                    #( #field_names.emit(t, f)?; )*
                    Ok(())
                }
            }
        }
        syn::Fields::Unit => quote! { Ok(()) },
    }
}
