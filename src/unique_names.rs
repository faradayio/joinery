//! Generate a unique name.
//!
//! When transpiling SQL, we occasionally need to introduce unique names for
//! things like QUALIFY. This fulfills the same role as [`gensym`][gensym] in
//! Lisp, but doesn't actually have the ability to create magic "uninterned"
//! symbols in SQL. But we also want to minimize the risk of collisions even if
//! we're transforming SQL where column names are under the control of an end
//! user.

use std::{borrow::Cow, sync::RwLock};

/// State for generating unique names.
struct NameGenerator {
    prefix: Cow<'static, str>,
    counter: usize,
}

impl NameGenerator {
    /// Create a new name generator with the given prefix.
    const fn default_const(prefix: &'static str) -> Self {
        Self {
            prefix: Cow::Borrowed(prefix),
            counter: 0,
        }
    }

    /// Generate a new unique name, including a random component.
    fn generate(&mut self) -> String {
        let random = rand::random::<u64>();
        let name = format!("{}_{}_{:016x}", self.prefix, self.counter, random);
        self.counter += 1;
        name
    }
}

/// Our global generator for unique names.
static NAME_GENERATOR: RwLock<NameGenerator> = RwLock::new(NameGenerator::default_const("joinery"));

/// Generate a new unique name.
///
/// This holds a [`RwLock`] internally, but only briefly.
pub fn unique_name() -> String {
    NAME_GENERATOR.write().expect("lock poisoned").generate()
}
