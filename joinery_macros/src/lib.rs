use std::borrow::Cow;

use darling::{util::Flag, FromField};
use proc_macro::TokenStream;
use proc_macro2::{Delimiter, Span, TokenStream as TokenStream2, TokenTree};
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, Field, Ident};

#[proc_macro_derive(Emit)]
pub fn emit_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_emit_macro(&ast).into()
}

fn impl_emit_macro(ast: &syn::DeriveInput) -> TokenStream2 {
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

#[proc_macro_derive(EmitDefault, attributes(emit))]
pub fn emit_default_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_emit_default_macro(&ast).into()
}

/// [`EmitDefault::emit_default`] should just iterate over the struct or enum,
/// and generate a recursive call to [`Emit::emit`] for each field.
///
/// TODO: If we see `#[emit(skip)]` on a field, we should skip it.
fn impl_emit_default_macro(ast: &syn::DeriveInput) -> TokenStream2 {
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
            let field_names = FieldInfo::named_iter(fields)
                .filter(|f| !f.attr().skip.is_present())
                .map(|f| f.struct_field());
            quote! {
                #( self.#field_names.emit(t, f)?; )*
                Ok(())
            }
        }
        syn::Fields::Unnamed(fields) => {
            let field_names = FieldInfo::unnamed_iter(fields)
                .filter(|f| !f.attr().skip.is_present())
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
            let fields = FieldInfo::named_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip.is_present())
                .map(|f| f.enum_name());
            quote! {
                #name::#variant_name { #(#patterns),* } => {
                    #( #field_names.emit(t, f)?; )*
                    Ok(())
                }
            }
        }
        syn::Fields::Unnamed(fields) => {
            let fields = FieldInfo::unnamed_iter(fields).collect::<Vec<_>>();
            let patterns = fields.iter().map(|f| f.enum_pattern());
            let field_names = fields
                .iter()
                .filter(|f| !f.attr().skip.is_present())
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

/// Information we have about a field. Needed to generate the correct code.
enum FieldInfo<'a> {
    /// A named field, such a `struct S { foo: Foo }`.
    Named { ident: &'a Ident, attr: EmitAttr },
    /// An unnamed field, such as `struct S(Foo)`.
    Unnamed {
        index: usize,
        span: Span,
        attr: EmitAttr,
    },
}

impl<'a> FieldInfo<'a> {
    /// Collect info about named fields.
    fn named_iter(fields: &'a syn::FieldsNamed) -> impl Iterator<Item = FieldInfo<'a>> + 'a {
        fields.named.iter().map(Self::named)
    }

    /// Collect info about unnamed fields.
    fn unnamed_iter(fields: &'a syn::FieldsUnnamed) -> impl Iterator<Item = FieldInfo<'a>> + 'a {
        fields
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| Self::unnamed(i, f))
    }

    /// Collect info about a named field.
    fn named(f: &'a Field) -> Self {
        Self::Named {
            ident: f.ident.as_ref().expect("field should be named"),
            attr: EmitAttr::from_field(f).unwrap_or_default(),
        }
    }

    /// Collect info about an unnamed field.
    fn unnamed(index: usize, f: &'a Field) -> Self {
        Self::Unnamed {
            index,
            span: f.span(),
            attr: EmitAttr::from_field(f).unwrap_or_default(),
        }
    }

    fn attr(&self) -> &EmitAttr {
        match self {
            Self::Named { attr, .. } => attr,
            Self::Unnamed { attr, .. } => attr,
        }
    }

    /// How to name this field when accessing it as a struct field.
    fn struct_field(&self) -> TokenStream2 {
        match self {
            Self::Named { ident, .. } => quote! { #ident },
            Self::Unnamed { index, span, .. } => {
                let index = syn::Index {
                    index: *index as u32,
                    span: *span,
                };
                quote! { #index }
            }
        }
    }

    /// How to name this field when accessing inside a `match` arm.
    fn enum_name(&self) -> Cow<'_, Ident> {
        match self {
            Self::Named { ident, .. } => Cow::Borrowed(ident),
            Self::Unnamed { index, span, .. } => {
                Cow::Owned(syn::Ident::new(&format!("f{}", index), *span))
            }
        }
    }

    /// How to name this field when using it as a pattern in a `match` arm.
    fn enum_pattern(&self) -> TokenStream2 {
        let name = self.enum_name();
        match (self, self.attr().skip.is_present()) {
            (Self::Named { .. }, true) => quote! { #name: _ },
            (Self::Named { .. }, false) => quote! { #name },
            (Self::Unnamed { .. }, true) => quote! { _ },
            (Self::Unnamed { .. }, false) => quote! { #name },
        }
    }
}

/// The contents of an `#[emit(...)]` attribute, parsing using the
/// [`darling`](https://github.com/TedDriggs/darling) crate.
#[derive(Default, FromField)]
#[darling(default, attributes(emit))]
struct EmitAttr {
    /// Should we omit this field from our output?
    skip: Flag,
}

#[proc_macro]
pub fn sql_quote(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);

    let mut sql_token_exprs = vec![];
    for token in input {
        emit_sql_token_exprs(&mut sql_token_exprs, token);
    }
    let output = quote! {
        crate::tokenizer::TokenStream::from_tokens(&[#(#sql_token_exprs),*][..])
    };
    output.into()
}

fn emit_sql_token_exprs(sql_token_exprs: &mut Vec<TokenStream2>, token: TokenTree) {
    match token {
        TokenTree::Group(group) => {
            // We flatten this and use `Punct::new`.
            let (open, close) = delimiter_pair(group.delimiter());
            if let Some(open) = open {
                sql_token_exprs.push(quote! {
                    crate::tokenizer::Token::Punct(crate::tokenizer::Punct::new(#open))
                });
            }
            for token in group.stream() {
                emit_sql_token_exprs(sql_token_exprs, token);
            }
            if let Some(close) = close {
                sql_token_exprs.push(quote! {
                    crate::tokenizer::Token::Punct(crate::tokenizer::Punct::new(#close))
                });
            }
        }
        TokenTree::Ident(ident) => {
            let ident_str = ident.to_string();
            sql_token_exprs.push(quote! {
                crate::tokenizer::Token::Ident(crate::tokenizer::Ident::new(#ident_str))
            });
        }
        TokenTree::Punct(punct) => {
            let punct_str = punct.to_string();
            sql_token_exprs.push(quote! {
                crate::tokenizer::Token::Punct(crate::tokenizer::Punct::new(#punct_str))
            });
        }
        TokenTree::Literal(lit) => {
            // There's probably a better way to do this.
            let lit: syn::Lit = syn::parse_quote!(#lit);
            match lit {
                syn::Lit::Int(i) => {
                    sql_token_exprs.push(quote! {
                        crate::tokenizer::Token::Literal(crate::tokenizer::Literal::int(#i))
                    });
                }
                syn::Lit::Str(s) => {
                    sql_token_exprs.push(quote! {
                        crate::tokenizer::Token::Literal(crate::tokenizer::Literal::string(#s))
                    });
                }
                syn::Lit::Float(f) => {
                    sql_token_exprs.push(quote! {
                        crate::tokenizer::Token::Literal(crate::tokenizer::Literal::float(#f))
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

fn delimiter_pair(d: Delimiter) -> (Option<&'static str>, Option<&'static str>) {
    match d {
        Delimiter::Parenthesis => (Some("("), Some(")")),
        Delimiter::Brace => (Some("{"), Some("}")),
        Delimiter::Bracket => (Some("["), Some("]")),
        Delimiter::None => (None, None),
    }
}
