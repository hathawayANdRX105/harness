//! Integration tests for the daemon's tool catalog.

use std::sync::Arc;

use harness_core::run::SessionId;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};

use harness_daemon::tools::{ToolCatalog, ToolStatus};

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

    if let Err(err) = catalog.set_active(&session, vec!["edit".to_string(), "read".to_string()]) {
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

    let err = match catalog.set_active(&session, vec!["bogus".to_string(), "read".to_string()]) {
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

    if let Err(err) = catalog.set_active(&session, vec!["read".to_string(), "read".to_string()]) {
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
