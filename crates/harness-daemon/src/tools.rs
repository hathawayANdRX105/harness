//! Per-session active-tool allowlist over the daemon's one assembled tool
//! list. The base set (`AssembledRuntime.tools`) is fixed at startup;
//! `active(session)` filters it by the session's override (default
//! permissive), so a session restricts its tools at runtime without a
//! restart — pi's `setActiveToolsByName` seam. Model, policy and budget
//! stay shared across sessions.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use harness_core::run::SessionId;
use harness_core::tool::Tool;
use serde::Serialize;
use thiserror::Error;

/// Status of one base tool for one session: whether the session's
/// allowlist currently admits it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolStatus {
    pub name: String,
    pub active: bool,
}

/// Typed error for [`ToolCatalog::set_active`]: the requested names that
/// are not in the base catalog, plus every valid name so a client can
/// retry without guessing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown tool name(s): {unknown:?}; available: {available:?}")]
pub struct ToolCatalogError {
    /// Names from the request that have no matching base tool.
    pub unknown: Vec<String>,
    /// Every base tool name: the client retries from this list.
    pub available: Vec<String>,
}

/// The runtime tool catalog: the base set assembled once at startup plus
/// per-session allowlist overrides.
pub struct ToolCatalog {
    /// The assembled runtime's tools, registered once at startup. The base
    /// set is fixed after construction — runtime tool acquisition is out
    /// of scope for the catalog.
    base: Vec<Arc<dyn Tool>>,
    /// Per-session allowlist overrides. A session with no entry gets all
    /// base tools (permissive default); `set_active` restricts.
    sessions: RwLock<HashMap<SessionId, HashSet<String>>>,
}

impl ToolCatalog {
    /// Build a catalog from the runtime's assembled tool list: a one-time
    /// snapshot taken by the daemon after `build_runtime`.
    pub fn new(base: Vec<Arc<dyn Tool>>) -> Self {
        Self {
            base,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Look one base tool up by name, e.g. for resolving a profile's tool
    /// subset at subagent-spawn time.
    pub fn find(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.base.iter().find(|tool| tool.name() == name).cloned()
    }

    /// Every base tool's name, in base order — for error messages that list
    /// what a caller may select.
    pub fn names(&self) -> Vec<&str> {
        self.base.iter().map(|tool| tool.name()).collect()
    }

    /// The tools `session` may run: the base tools whose names are in the
    /// session's override, in base order (stable so tool-schema ordering
    /// stays stable for prefix caching), or every base tool when the
    /// session has no override.
    pub fn active(&self, session: &SessionId) -> Vec<Arc<dyn Tool>> {
        // Recover a poisoned read lock instead of panicking into the
        // caller: the guarded data is plain state, and one request must
        // not wedge the daemon.
        let sessions = self
            .sessions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match sessions.get(session) {
            None => self.base.clone(),
            Some(names) => self
                .base
                .iter()
                .filter(|tool| names.contains(tool.name()))
                .cloned()
                .collect(),
        }
    }

    /// Every base tool with whether it is active for `session`, in base
    /// order: the wire surface for `tools.list`.
    pub fn list(&self, session: &SessionId) -> Vec<ToolStatus> {
        let sessions = self
            .sessions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let override_names = sessions.get(session);
        self.base
            .iter()
            .map(|tool| ToolStatus {
                name: tool.name().to_string(),
                // No override means default-permissive: every base tool is
                // active; an override gates membership in the allowlist.
                active: override_names.is_none_or(|names| names.contains(tool.name())),
            })
            .collect()
    }

    /// Store `session`'s allowlist: only the named base tools become
    /// active. Duplicate names are deduped into the set. An unknown name
    /// rejects the whole request as a [`ToolCatalogError`] carrying the
    /// rejected names and every valid name — nothing is stored on
    /// rejection, so a failed call never leaves a partial allowlist.
    pub fn set_active(
        &self,
        session: &SessionId,
        names: Vec<String>,
    ) -> Result<(), ToolCatalogError> {
        let available: HashSet<&str> = self.base.iter().map(|tool| tool.name()).collect();
        let mut seen_unknown: HashSet<String> = HashSet::new();
        let unknown: Vec<String> = names
            .iter()
            .filter(|name| {
                !available.contains(name.as_str()) && seen_unknown.insert((*name).clone())
            })
            .cloned()
            .collect();
        if !unknown.is_empty() {
            return Err(ToolCatalogError {
                unknown,
                available: self.base_names(),
            });
        }
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.insert(session.clone(), names.into_iter().collect());
        Ok(())
    }

    /// Drop `session`'s override so it goes back to all base tools.
    /// No-op when the session has no override.
    pub fn reset(&self, session: &SessionId) {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.remove(session);
    }

    /// Every base tool name: feeds [`ToolCatalogError::available`] after
    /// a rejected `set_active`.
    fn base_names(&self) -> Vec<String> {
        self.base
            .iter()
            .map(|tool| tool.name().to_string())
            .collect()
    }
}
