//! Unification, Prolog style. No backtracking, though. Also we only handle
//! a special case of unification, where one side may contain type variables
//! and the other side is always a concrete type.
//!
//! There's an [introduction to unification on Wikipedia][wiki]. If you're
//! interested in the theory behind this, check out [_The Reasoned
//! Schemer_][reasoned] and [miniKanren][minikanren].
//!
//! Wikipedia's example is a good one:
//!
//! ```txt
//! cons(x,cons(x,nil)) = cons(2,y)
//! ```
//!
//! Copilot is happy to explain what's going on here:
//!
//! > The solution is `x = 2` and `y = cons(2,nil)`. We can solve this by
//! > recursively unifying the two sides of the equation. We'll start by
//! > unifying `cons(x,cons(x,nil))` and `cons(2,y)`. We'll unify the first
//! > elements, `x` and `2`, and then the second elements, `cons(x,nil)` and
//! > `y`.
//!
//! And already know that `x = 2`, so we can substitute that in.
//!
//! ## Why we have an easier time
//!
//! When we unify two types, `t1` and `t2`, we known that only `t1` may contain
//! type variables. `t2` is always a concrete type. This eliminates the need
//! for many of the special cases that a general unification algorithm would
//! need to handle.
//!
//! [wiki]: https://en.wikipedia.org/wiki/Unification_(computer_science)
//! [reasoned]: https://mitpress.mit.edu/9780262535519/the-reasoned-schemer/
//! [minikanren]: https://minikanren.org/

use std::collections::BTreeMap;

use crate::{
    errors::{Error, Result},
    tokenizer::Spanned,
    types::{ArgumentType, ResolvedTypeVarsOnly, SimpleType, TypeVar, ValueType},
};

/// A unification table.
#[derive(Debug, Default)]
pub struct UnificationTable {
    mappings: BTreeMap<TypeVar, ArgumentType<ResolvedTypeVarsOnly>>,
}

impl UnificationTable {
    /// Declare a new type variable. All type variables start out as ⊥.
    pub fn declare(&mut self, var: TypeVar, spanned: &dyn Spanned) -> Result<()> {
        let bottom = ArgumentType::Value(ValueType::Simple(SimpleType::Bottom));
        if self.mappings.insert(var.clone(), bottom).is_some() {
            return Err(Error::annotated(
                format!("duplicate type variable in signature: {}", var),
                spanned.span(),
                "function declaration incorrect",
            ));
        }
        Ok(())
    }

    /// Create a new type variable, declare it, and return an `ArgumentType`.
    ///
    /// This is handy when we're forced to implement custom unification logic.
    pub fn type_var(
        &mut self,
        name: impl Into<String>,
        spanned: &dyn Spanned,
    ) -> Result<ArgumentType<TypeVar>> {
        let var = TypeVar::new(name)?;
        self.declare(var.clone(), spanned)?;
        Ok(ArgumentType::Value(ValueType::Simple(
            SimpleType::Parameter(var),
        )))
    }

    /// Update a type variable to a new type.
    pub fn update(
        &mut self,
        var: TypeVar,
        ty: ArgumentType<ResolvedTypeVarsOnly>,
        spanned: &dyn Spanned,
    ) -> Result<ArgumentType<ResolvedTypeVarsOnly>> {
        match self.mappings.get_mut(&var) {
            Some(old_ty) => {
                // Try to combine the old type and the new type. This is how
                // `ARRAY[1, 2.0, NULL]` becomes an `ARRAY<FLOAT64>`, as we
                // refine the element type. And `ARRAY[]` remains `ARRAY<⊥>`.
                let new_ty = old_ty.common_supertype(&ty).ok_or_else(|| {
                    Error::annotated(
                        format!("cannot find common supertype of `{}` and `{}`", old_ty, ty),
                        spanned.span(),
                        format!("declares {}", var),
                    )
                })?;
                *old_ty = new_ty.clone();
                Ok(new_ty)
            }
            None => Err(Error::annotated(
                format!("unknown type variable: {}", var),
                spanned.span(),
                "function declaration incorrect",
            )),
        }
    }

    /// Get the type of a type variable.
    pub fn get(&self, var: TypeVar) -> Option<&ArgumentType<ResolvedTypeVarsOnly>> {
        self.mappings.get(&var)
    }
}

/// Interface for types supporting unification.
pub trait Unify: Sized {
    /// The type after unification. This is the concrete type (with no type
    /// variables) corresponding to `Self`.
    type Resolved;

    /// Unify two types, updating any type variables in `self` to be a type
    /// consistent with `other`. This may involve binding a pattern variable
    /// `?T` to a concrete type, or "loosening" an existing binding like `?T:
    /// INT64` to `?T: FLOAT64` so that it can hold all the types we've seen.
    ///
    /// If a type variable is already bound to a type like `?T: INT64`, and
    /// we're asked to unify it with an incompatible type `STRING`, we'll return
    /// an error.
    ///
    /// This is what allows us to deduce that `ARRAY[1, 2.0, NULL]` is an
    /// `ARRAY<FLOAT64>`.
    fn unify(
        &self,
        other: &Self::Resolved,
        table: &mut UnificationTable,
        spanned: &dyn Spanned,
    ) -> Result<Self::Resolved>;

    /// Resolve any type variables in a value.
    fn resolve(&self, table: &UnificationTable, spanned: &dyn Spanned) -> Result<Self::Resolved>;
}
