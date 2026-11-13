//! Per-session active-tool allowlist: the runtime tool catalog.
//!
//! The daemon assembles one static tool list at startup
//! (`AssembledRuntime.tools`) and injects the same list into every run
//! via `runtime.loop_deps()`.  The catalog wraps that base list and lets
//! a session opt into a restricted subset:
//! `ToolCatalog::active(session)` returns only the base tools whose
//! `name()` is in the session's override, or every base tool when the
//! session has no entry — the default is permissive; `set_active` is
//! what restricts.
//!
//! # WHY
//!
//! A session can run a restricted tool subset without restarting the
//! daemon: pi's `setActiveToolsByName` seam maps onto
//! `ToolCatalog::set_active`, and every run built against the catalog sees
//! only its session's active tools while model, policy, budget and
//! everything else stays shared.

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
        let mut unknown: Vec<String> = Vec::new();
        let mut seen_unknown: HashSet<String> = HashSet::new();
        for name in &names {
            if !available.contains(name.as_str()) && seen_unknown.insert(name.clone()) {
                unknown.push(name.clone());
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::tool::{ToolError, ToolResult, ToolRisk, ToolSchema};

    /// Stub tool with a fixed name and no behaviour: mirrors the repo's
    /// existing test stubs (harness-builder's `FixedTool`, the loop
    /// engine's `MockTool`) — enough for `ToolCatalog` to filter by
    /// `name()`.
    struct StubTool {
        name: &'static str,
    }

    impl StubTool {
        fn new(name: &'static str) -> Self {
            Self { name }
        }
    }

    #[async_trait::async_trait]
    impl Tool for StubTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            ToolSchema {
                name: self.name.to_string(),
                description: "stub tool".into(),
                input: serde_json::json!({}),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _cancel: &tokio_util::sync::CancellationToken,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                content: "stub".into(),
                is_error: false,
                trace: None,
            })
        }
    }

    fn catalog(names: &[&'static str]) -> ToolCatalog {
        ToolCatalog::new(
            names
                .iter()
                .map(|name| Arc::new(StubTool::new(name)) as Arc<dyn Tool>)
                .collect(),
        )
    }

    fn tool_names(tools: &[Arc<dyn Tool>]) -> Vec<String> {
        tools.iter().map(|tool| tool.name().to_string()).collect()
    }

    /// Purpose: a session with no catalog entry sees every base tool.
    /// Scenario: `ToolCatalog::new` over `[read, write, edit]`;
    /// `active(&session)` for a session never registered with
    /// `set_active`.
    /// Acceptance: `active` returns all three tools in base order, and
    /// `list` marks every tool active.
    #[test]
    fn default_active_is_all_base_tools() {
        let catalog = catalog(&["read", "write", "edit"]);
        let session = SessionId("session-a".to_string());

        let active = tool_names(&catalog.active(&session));
        assert_eq!(
            active,
            vec!["read".to_string(), "write".to_string(), "edit".to_string()]
        );

        let statuses = catalog.list(&session);
        assert_eq!(
            statuses,
            vec![
                ToolStatus {
                    name: "read".to_string(),
                    active: true
                },
                ToolStatus {
                    name: "write".to_string(),
                    active: true
                },
                ToolStatus {
                    name: "edit".to_string(),
                    active: true
                },
            ],
            "every base tool is active for a session without an override"
        );
    }

    /// Purpose: `set_active` restricts a session and `active()` preserves
    /// base order, not request order.
    /// Scenario: `set_active(&session, ["edit", "read"])` then
    /// `active(&session)` and `list(&session)`.
    /// Acceptance: `active` returns `[read, edit]` — base order, not the
    /// request's order — and `list` marks read and edit active while
    /// write is inactive.
    #[test]
    fn set_active_restricts_and_preserves_base_order() {
        let catalog = catalog(&["read", "write", "edit"]);
        let session = SessionId("session-b".to_string());

        if let Err(err) = catalog.set_active(&session, vec!["edit".to_string(), "read".to_string()])
        {
            panic!("known names must be accepted: {err}");
        }

        let active = tool_names(&catalog.active(&session));
        assert_eq!(
            active,
            vec!["read".to_string(), "edit".to_string()],
            "active tools stay in base order"
        );

        let statuses = catalog.list(&session);
        assert_eq!(
            statuses,
            vec![
                ToolStatus {
                    name: "read".to_string(),
                    active: true
                },
                ToolStatus {
                    name: "write".to_string(),
                    active: false
                },
                ToolStatus {
                    name: "edit".to_string(),
                    active: true
                },
            ],
            "list reports the allowlist per base tool"
        );
    }

    /// Purpose: an unknown name is a typed error carrying the available
    /// names, and nothing is stored.
    /// Scenario: `set_active(&session, ["bogus", "read"])`.
    /// Acceptance: the error is `ToolCatalogError` whose `unknown` is
    /// `["bogus"]` and whose `available` lists every base name; a
    /// subsequent `active(&session)` still returns all base tools
    /// (rejection left no partial allowlist).
    #[test]
    fn set_active_unknown_name_reports_available_and_stores_nothing() {
        let catalog = catalog(&["read", "write"]);
        let session = SessionId("session-c".to_string());

        let err = match catalog.set_active(&session, vec!["bogus".to_string(), "read".to_string()])
        {
            Ok(()) => panic!("unknown name must be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.unknown, vec!["bogus".to_string()]);
        assert_eq!(
            err.available,
            vec!["read".to_string(), "write".to_string()],
            "error carries every valid name for the retry"
        );
        assert_eq!(
            tool_names(&catalog.active(&session)),
            vec!["read".to_string(), "write".to_string()],
            "rejected call leaves the session unrestricted"
        );
    }

    /// Purpose: duplicate names are deduped and accepted.
    /// Scenario: `set_active(&session, ["read", "read"])`.
    /// Acceptance: the call succeeds and `active(&session)` returns
    /// `[read]` exactly once.
    #[test]
    fn set_active_deduplicates_names() {
        let catalog = catalog(&["read", "write"]);
        let session = SessionId("session-d".to_string());

        if let Err(err) = catalog.set_active(&session, vec!["read".to_string(), "read".to_string()])
        {
            panic!("duplicate names must be accepted: {err}");
        }
        assert_eq!(
            tool_names(&catalog.active(&session)),
            vec!["read".to_string()],
            "duplicates are deduped into the allowlist"
        );
    }

    /// Purpose: `reset` drops the override and returns the session to all
    /// base tools.
    /// Scenario: `set_active(&session, ["read"])` then
    /// `reset(&session)` then `active(&session)`.
    /// Acceptance: after `reset`, all base tools are active again.
    #[test]
    fn reset_returns_to_all_base_tools() {
        let catalog = catalog(&["read", "write"]);
        let session = SessionId("session-e".to_string());

        if let Err(err) = catalog.set_active(&session, vec!["read".to_string()]) {
            panic!("known name must be accepted: {err}");
        }
        catalog.reset(&session);

        assert_eq!(
            tool_names(&catalog.active(&session)),
            vec!["read".to_string(), "write".to_string()],
            "reset restores the permissive default"
        );
    }

    /// Purpose: sessions are isolated — one session's override never
    /// leaks into another.
    /// Scenario: `set_active(&session_a, ["read"])`, session B untouched.
    /// Acceptance: `active(session_a)` is `[read]` while
    /// `active(session_b)` is every base tool.
    #[test]
    fn sessions_are_isolated() {
        let catalog = catalog(&["read", "write", "edit"]);
        let session_a = SessionId("session-a".to_string());
        let session_b = SessionId("session-b".to_string());

        if let Err(err) = catalog.set_active(&session_a, vec!["read".to_string()]) {
            panic!("known name must be accepted: {err}");
        }

        assert_eq!(
            tool_names(&catalog.active(&session_a)),
            vec!["read".to_string()],
        );
        assert_eq!(
            tool_names(&catalog.active(&session_b)),
            vec!["read".to_string(), "write".to_string(), "edit".to_string()],
            "an unrelated session still sees every base tool"
        );
    }
}
