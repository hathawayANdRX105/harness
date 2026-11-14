//! Integration tests for the daemon's wire seams: the search command's
//! recall wiring and the store-backed EventLog resolver.
//!
//! Each test opens its own temp turso store, so the tests are independent
//! and parallel-safe.

use std::env;
use std::fs;
use std::process::id;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::RunEvent;
use harness_core::event::RunEventKind;
use harness_core::run::{RunId, SessionId};
use harness_daemon::wire::{DaemonHandler, StoreEventLog};
use harness_scheduler::Scheduler;
use harness_session::turso::TursoSessionStore;
use harness_session::{RecallStore, SessionStore};
use harness_transport::{CommandHandler, EventLog, SlashCommand};

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
    let handler = DaemonHandler::new(scheduler, store, recall);

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
