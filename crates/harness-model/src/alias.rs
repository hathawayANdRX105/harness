//! Model alias resolution: a name-based registry that maps logical handles
//! to fully-qualified model identifiers.
//!
//! The builder crate (#10) populates the registry from config; this crate
//! only defines the type and the lookup logic, with no dependency on
//! harness-config.
//!
//! Design provenance: harness-rs's `AliasResolver`
//! (`crates/harness-models/src/alias.rs`), stripped of its file-watch
//! reload (deferred to the caller).

use std::collections::HashMap;

/// A name-based alias registry for model identifiers.
///
/// A user-configured alias like `"fast"` can point to a concrete model
/// id such as `"gpt-4o-mini"`. Resolution is a simple `HashMap` lookup
/// with passthrough — if the name is not in the map, it is returned
/// as-is so that bare model ids work without an alias entry.
///
/// # Builder boundary
///
/// This crate defines the type. Populating it with config values belongs
/// to the builder crate (#10).
#[derive(Debug, Clone, Default)]
pub struct AliasRegistry {
    entries: HashMap<String, String>,
}

impl AliasRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Insert or overwrite one alias entry.
    ///
    /// `alias` is the user-facing handle; `resolved` is the model id the
    /// adapter sends over the wire.
    pub fn insert(&mut self, alias: impl Into<String>, resolved: impl Into<String>) {
        self.entries.insert(alias.into(), resolved.into());
    }

    /// Extend this registry from an iterator of `(alias, resolved)` pairs.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = (String, String)>) {
        self.entries.extend(iter);
    }

    /// Resolve an alias to its model id.
    ///
    /// If `name` is not registered, it is returned unchanged — this allows
    /// bare model ids like `"gpt-4o"` to pass through without a wrapping
    /// alias entry.
    pub fn resolve<'a>(&'a self, name: &'a str) -> &'a str {
        self.entries.get(name).map_or(name, |v| v.as_str())
    }

    /// Number of registered aliases.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no aliases are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// Tests live in crates/harness-model/tests/ — see the contract tests.
// #[cfg(test)] modules inside src/ are not the convention in this repo.
