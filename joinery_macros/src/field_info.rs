//! Information about a field in a `struct` or `enum` with a `#[derive(..)]`
//! attribute.

use std::borrow::Cow;

use darling::{util::Flag, FromField};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{spanned::Spanned, Field, Ident};

pub(crate) trait AttrWithSkip: Default + FromField + 'static {
    /// Should we skip this field?
    fn skip(&self) -> &Flag;
}

/// Information we have about a field. Needed to generate the correct code.
pub(crate) enum FieldInfo<'a, Attr: AttrWithSkip> {
    /// A named field, such a `struct S { foo: Foo }`.
    Named { ident: &'a Ident, attr: Attr },
    /// An unnamed field, such as `struct S(Foo)`.
    Unnamed {
        index: usize,
        span: Span,
        attr: Attr,
    },
}

impl<'a, Attr: AttrWithSkip> FieldInfo<'a, Attr> {
    /// Collect info about named fields.
    pub(crate) fn named_iter(
        fields: &'a syn::FieldsNamed,
    ) -> impl Iterator<Item = FieldInfo<'a, Attr>> + 'a {
        fields.named.iter().map(Self::named)
    }

    /// Collect info about unnamed fields.
    pub(crate) fn unnamed_iter(
        fields: &'a syn::FieldsUnnamed,
    ) -> impl Iterator<Item = FieldInfo<'a, Attr>> + 'a {
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
            attr: Attr::from_field(f).unwrap_or_default(),
        }
    }

    /// Collect info about an unnamed field.
    fn unnamed(index: usize, f: &'a Field) -> Self {
        Self::Unnamed {
            index,
            span: f.span(),
            attr: Attr::from_field(f).unwrap_or_default(),
        }
    }

    pub(crate) fn attr(&self) -> &Attr {
        match self {
            Self::Named { attr, .. } => attr,
            Self::Unnamed { attr, .. } => attr,
        }
    }

    /// How to name this field when accessing it as a struct field.
    pub(crate) fn struct_field(&self) -> TokenStream2 {
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
    pub(crate) fn enum_name(&self) -> Cow<'_, Ident> {
        match self {
            Self::Named { ident, .. } => Cow::Borrowed(ident),
            Self::Unnamed { index, span, .. } => {
                Cow::Owned(syn::Ident::new(&format!("f{}", index), *span))
            }
        }
    }

    /// How to name this field when using it as a pattern in a `match` arm.
    pub(crate) fn enum_pattern(&self) -> TokenStream2 {
        let name = self.enum_name();
        match (self, self.attr().skip().is_present()) {
            (Self::Named { .. }, true) => quote! { #name: _ },
            (Self::Named { .. }, false) => quote! { #name },
            (Self::Unnamed { .. }, true) => quote! { _ },
            (Self::Unnamed { .. }, false) => quote! { #name },
        }
    }
}
