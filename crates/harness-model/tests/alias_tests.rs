//! Tests for the alias registry.
//!
//! Purpose: verify that `AliasRegistry` resolves known aliases, passes
//! through unknown names, and handles bulk insertions.
//!
//! Scenarios:
//! - Unknown name returns the input unchanged.
//! - Known alias returns the registered model id.
//! - Last insert wins for duplicate aliases.
//! - Bulk extend populates multiple entries.

use harness_model::AliasRegistry;

/// Unknown names pass through without modification.
#[test]
fn resolve_unknown_passes_through() {
    let reg = AliasRegistry::new();
    assert_eq!(reg.resolve("gpt-4o"), "gpt-4o");
}

/// A registered alias returns its resolved model id.
#[test]
fn resolve_known_returns_resolved() {
    let mut reg = AliasRegistry::new();
    reg.insert("fast", "gpt-4o-mini");
    assert_eq!(reg.resolve("fast"), "gpt-4o-mini");
}

/// When the same alias is inserted twice, the last value wins.
#[test]
fn resolve_prefers_last_insert() {
    let mut reg = AliasRegistry::new();
    reg.insert("fast", "gpt-4o");
    reg.insert("fast", "gpt-4o-mini");
    assert_eq!(reg.resolve("fast"), "gpt-4o-mini");
}

/// Bulk extend correctly inserts all pairs.
#[test]
fn extend_bulk_insert() {
    let mut reg = AliasRegistry::new();
    reg.extend([("a".into(), "x".into()), ("b".into(), "y".into())]);
    assert_eq!(reg.len(), 2);
    assert_eq!(reg.resolve("a"), "x");
    assert_eq!(reg.resolve("b"), "y");
}

/// A fresh registry is empty.
#[test]
fn new_registry_is_empty() {
    let reg = AliasRegistry::new();
    assert!(reg.is_empty());
    assert_eq!(reg.len(), 0);
}
