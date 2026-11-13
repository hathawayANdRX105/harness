//! Integration tests for the daemon's wire seams: the search command's
//! recall wiring, the store-backed EventLog resolver, and the per-session
//! tool catalog (`tools.list` / `tools.set_active` / `tools.reset`).
//!
//! Each test opens its own temp turso store, so the tests are independent
//! and parallel-safe.

use std::env;
use std::fs;
use std::process::id;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::event::RunEventKind;
use harness_core::run::{RunId, SessionId};
use harness_core::{RunEvent, Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use harness_daemon::tools::ToolCatalog;
use harness_daemon::wire::{DaemonHandler, StoreEventLog};
use harness_scheduler::Scheduler;
use harness_session::turso::TursoSessionStore;
use harness_session::{RecallStore, SessionStore};
use harness_transport::{CommandHandler, EventLog, SlashCommand, TransportError};

static SEARCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Open a temp store with one session and two runs' events, each carrying
/// a unique marker text in its payload.  A unique temp dir per call keeps
/// parallel tests from contending on one turso file lock.
async fn search_fixture() -> std::sync::Arc<TursoSessionStore> {
    let seq = SEARCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = env::temp_dir().join(format!("harness-search-{}-{seq}", id()));
    let _ = fs::create_dir_all(&dir);
    let store = TursoSessionStore::open(dir.join("search.db"))
        .await
        .unwrap_or_else(|e| panic!("open store: {e}"));
    let session = SessionId("search-session".into());
    store
        .create_session(&session)
        .await
        .unwrap_or_else(|e| panic!("create session: {e}"));
    for (run, marker) in [
        ("search-alpha", "daemon-search-marker-alpha"),
        ("search-beta", "daemon-search-marker-beta"),
    ] {
        let run_id = RunId(run.into());
        for seq in 0..2u64 {
            let event = RunEvent {
                run_id: run_id.clone(),
                session_id: session.clone(),
                seq,
                timestamp_ms: 0,
                kind: RunEventKind::TextDelta {
                    text: format!("{marker} event {seq}"),
                },
            };
            store
                .append_event(&event)
                .await
                .unwrap_or_else(|e| panic!("append event: {e}"));
        }
    }
    std::sync::Arc::new(store)
}

/// Stub tool with a fixed name and no behaviour: enough for the catalog to
/// filter by `name()` and for the wire test to round-trip through the
/// handler.
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

/// Open an empty temp store and pair it with its recall seam, to satisfy
/// the [`DaemonHandler`] constructor without asserting on persistence.
async fn store_and_recall() -> (Arc<dyn SessionStore>, Arc<dyn RecallStore>) {
    let dir = env::temp_dir().join(format!("harness-wire-tools-{}", id()));
    let _ = fs::create_dir_all(&dir);
    let store_inner = TursoSessionStore::open(dir.join("tools.db"))
        .await
        .unwrap_or_else(|e| panic!("open store: {e}"));
    let recall: Arc<dyn RecallStore> = Arc::new(store_inner.recall());
    let store: Arc<dyn SessionStore> = Arc::new(store_inner);
    (store, recall)
}

/// Purpose: `DaemonHandler::handle(Search)` queries the recall seam and
/// returns JSON hits.
/// Scenario: a store holds marker events; the handler is built with
/// `store.recall()` and handles `SlashCommand::Search` for a present term
/// and an absent term.
/// Acceptance: the present term returns exactly the marker events, every
/// hit carries run_id/session_id/seq/score/snippet, hits are ordered by
/// score ascending, and the absent term returns an empty hit list.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_search_returns_marker_events_as_json_hits() -> Result<(), Box<dyn std::error::Error>>
{
    let store = search_fixture().await;
    let recall: Arc<dyn RecallStore> = Arc::new(store.recall());
    let scheduler = Scheduler::new(4, 4);
    let catalog = Arc::new(ToolCatalog::new(Vec::new()));
    let handler = DaemonHandler::new(scheduler, store, recall, catalog);

    let value = match CommandHandler::handle(
        &handler,
        SlashCommand::Search {
            query: "daemon-search-marker-alpha".into(),
        },
    ) {
        Ok(value) => value,
        Err(err) => return Err(format!("search failed: {err}").into()),
    };
    assert_eq!(
        value.get("query").and_then(|v| v.as_str()),
        Some("daemon-search-marker-alpha"),
        "the response must echo the query"
    );
    let hits = value
        .get("hits")
        .and_then(|v| v.as_array())
        .ok_or("search response is missing a hits array")?;
    assert_eq!(hits.len(), 2, "both alpha-marked events must match");
    assert!(
        hits.iter()
            .all(|hit| { hit.get("run_id").and_then(|v| v.as_str()) == Some("search-alpha") }),
        "every alpha hit must come from the alpha run"
    );
    let mut scores: Vec<f64> = Vec::new();
    for hit in hits {
        assert!(
            hit.get("session_id").is_some()
                && hit.get("seq").is_some()
                && hit.get("score").is_some()
                && hit.get("snippet").is_some(),
            "every hit must carry session_id, seq, score and snippet"
        );
        scores.push(
            hit.get("score")
                .and_then(|v| v.as_f64())
                .ok_or("hit score is missing")?,
        );
    }
    assert!(
        scores.windows(2).all(|pair| pair[0] <= pair[1]),
        "hits must be ordered by score ascending (lower = more relevant)"
    );

    let empty = match CommandHandler::handle(
        &handler,
        SlashCommand::Search {
            query: "__no_such_marker_term__".into(),
        },
    ) {
        Ok(value) => value,
        Err(err) => return Err(format!("search failed: {err}").into()),
    };
    let empty_hits = empty
        .get("hits")
        .and_then(|v| v.as_array())
        .ok_or("search response is missing a hits array")?;
    assert!(empty_hits.is_empty(), "an absent term must return no hits");
    Ok(())
}

/// Purpose: `StoreEventLog::latest_run_for_session` resolves a session
/// to its newest run by delegating to the session store.
/// Scenario: a store holds run-a (one event at seq 1) and run-b (two
/// events at seq 1 and 2) under one session; query the seam for that
/// session and for an unknown session.
/// Acceptance: the seam returns run-b for the session (highest max seq
/// wins) and `None` for a session without events.
#[tokio::test]
async fn store_event_log_resolves_latest_run_for_session() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = env::temp_dir().join(format!("harness-wire-latest-{}", id()));
    let _ = fs::create_dir_all(&dir);
    let store = TursoSessionStore::open(dir.join("latest.db")).await?;
    let session = SessionId("wire-session".into());
    store.create_session(&session).await?;
    let run_a = RunId("wire-run-a".into());
    let run_b = RunId("wire-run-b".into());
    for (run, seq) in [
        (run_a.clone(), 1u64),
        (run_b.clone(), 1),
        (run_b.clone(), 2),
    ] {
        store
            .append_event(&RunEvent {
                run_id: run,
                session_id: session.clone(),
                seq,
                timestamp_ms: 0,
                kind: RunEventKind::TextDelta {
                    text: "wire probe".into(),
                },
            })
            .await?;
    }
    let log = StoreEventLog::new(Arc::new(store));
    assert_eq!(
        log.latest_run_for_session(&session).await,
        Some(run_b),
        "the run whose latest event has the highest seq is the session's latest"
    );
    assert_eq!(
        log.latest_run_for_session(&SessionId("no-such-wire-session".into()))
            .await,
        None,
        "a session without events resolves to no run"
    );
    Ok(())
}

/// Purpose: the daemon's `tools.*` commands consult the per-session catalog:
/// listing reports the active flag, restricting keeps base order, an
/// unknown name is rejected as a typed transport error, and the rejection
/// stores nothing.
/// Scenario: build a handler whose catalog holds `read` and `write`;
/// handle `ToolsList`, then `ToolsSetActive { names: ["read"] }`, then
/// `ToolsList`, then `ToolsSetActive { names: ["bogus"] }`.
/// Acceptance: initial list marks both tools active; after restricting to
/// `read` the success body lists `["read"]` and the next list marks `read`
/// active and `write` inactive; restricting to `["bogus"]` returns the
/// typed `TransportError::UnknownTool` carrying `unknown=["bogus"]` and
/// `available=["read","write"]`, and a final list still shows both tools
/// (nothing stored).
#[tokio::test]
async fn daemon_tools_commands_consult_catalog() {
    let scheduler = Scheduler::new(4, 4);
    let (store, recall) = store_and_recall().await;
    let catalog = Arc::new(ToolCatalog::new(vec![
        Arc::new(StubTool::new("read")) as Arc<dyn Tool>,
        Arc::new(StubTool::new("write")) as Arc<dyn Tool>,
    ]));
    let handler = DaemonHandler::new(scheduler, store, recall, catalog);

    let list = |handler: &DaemonHandler| match CommandHandler::handle(
        handler,
        SlashCommand::ToolsList { session_id: None },
    ) {
        Ok(value) => value,
        Err(err) => panic!("tools.list must not fail: {err}"),
    };

    let statuses = |value: &serde_json::Value| {
        value
            .get("tools")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("tools.list response carries a tools array: {value}"))
            .iter()
            .map(|v| {
                (
                    v.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or_else(|| panic!("tool entry carries a name: {v}"))
                        .to_string(),
                    v.get("active")
                        .and_then(|a| a.as_bool())
                        .unwrap_or_else(|| panic!("tool entry carries an active flag: {v}")),
                )
            })
            .collect::<Vec<(String, bool)>>()
    };

    // Default: every base tool is active.
    let mut seen = statuses(&list(&handler));
    seen.sort();
    assert_eq!(
        seen,
        vec![("read".to_string(), true), ("write".to_string(), true)],
        "every base tool is active without an override"
    );

    // Restrict to `read`: succeeds and reports `["read"]` active.
    let set = match CommandHandler::handle(
        &handler,
        SlashCommand::ToolsSetActive {
            session_id: None,
            names: vec!["read".to_string()],
        },
    ) {
        Ok(value) => value,
        Err(err) => panic!("set_active with known names must succeed: {err}"),
    };
    assert_eq!(
        set.get("active"),
        Some(&serde_json::json!(["read"])),
        "set_active success body lists the active names"
    );

    // `read` active, `write` inactive.
    seen = statuses(&list(&handler));
    seen.sort();
    assert_eq!(
        seen,
        vec![("read".to_string(), true), ("write".to_string(), false)],
        "restricting to read deactivates write"
    );

    // Unknown name is a typed error carrying the available names and stores
    // nothing.
    let err = match CommandHandler::handle(
        &handler,
        SlashCommand::ToolsSetActive {
            session_id: None,
            names: vec!["bogus".to_string()],
        },
    ) {
        Err(err) => err,
        Ok(value) => panic!("set_active with unknown name must fail: {value:?}"),
    };
    match err {
        TransportError::UnknownTool { unknown, available } => {
            assert_eq!(unknown, vec!["bogus".to_string()]);
            assert_eq!(
                available,
                vec!["read".to_string(), "write".to_string()],
                "error lists every available name"
            );
        }
        other => panic!("expected UnknownTool, got {other:?}"),
    }

    // Rejection stored nothing: the session keeps the `read`-only override
    // it had BEFORE the rejected call.
    seen = statuses(&list(&handler));
    seen.sort();
    assert_eq!(
        seen,
        vec![("read".to_string(), true), ("write".to_string(), false)],
        "rejected set_active leaves the prior override untouched"
    );
}
