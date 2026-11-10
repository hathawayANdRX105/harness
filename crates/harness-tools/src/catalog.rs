//! Tool catalog: a simple named index over `Vec<Arc<dyn Tool>>`.
//!
//! The harness loop and builder hold tools as a `Vec<Arc<dyn Tool>>` — core
//! deliberately has no `ToolRegistry`/`ToolCatalog` trait (one implementor
//! does not justify a trait). This module provides that implementor: a cheap
//! name → index lookup for dispatch.
//!
//! Design provenance: nanoharness's `ToolRegistry` — its `__getitem__` by
//! name and `__iter__` over all tools are the two operations a dispatcher
//! needs; no ceremony beyond that.

use std::sync::Arc;

use harness_core::tool::Tool;

/// A named index of registered tools.
///
/// Construction is O(n) — tools are registered once at startup and queried
/// many times during a run, so the linear scan in `get` is fine for the
/// expected tool count (tens, not thousands).
#[derive(Default)]
pub struct ToolCatalog {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolCatalog {
    /// An empty catalog.
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    /// Register one tool.
    ///
    /// If a tool with the same name already exists, the later registration
    /// replaces the earlier one (last-write-wins). This lets overlays or
    /// session-scoped overrides shadow a base tool without mutating the
    /// base catalog.
    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        // Remove any existing tool with the same name so the new one replaces it.
        let name = tool.name().to_string();
        self.tools.retain(|t| t.name() != name);
        self.tools.push(tool);
    }

    /// Look up a tool by name.
    ///
    /// Returns `None` when no tool with that name is registered.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools.iter().find(|t| t.name() == name)
    }

    /// All registered tool names.
    pub fn names(&self) -> Vec<&str> {
        self.tools.iter().map(|t| t.name()).collect()
    }

    /// Consume the catalog and return the underlying tool vector.
    ///
    /// Useful when passing tools to a builder or loop that expects
    /// `Vec<Arc<dyn Tool>>`.
    pub fn into_inner(self) -> Vec<Arc<dyn Tool>> {
        self.tools
    }

    /// Number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl From<Vec<Arc<dyn Tool>>> for ToolCatalog {
    fn from(tools: Vec<Arc<dyn Tool>>) -> Self {
        Self { tools }
    }
}
