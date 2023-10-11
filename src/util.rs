//! The inevitable grab-bag of utility functions.

/// Is `s` a valid C identifier?
pub fn is_c_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        None => false,
        Some(c) if !c.is_ascii_alphabetic() && c != '_' => false,
        _ => chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
    }
}
