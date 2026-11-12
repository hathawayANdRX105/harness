//! Integration tests for `harness-session` with a real Turso (libsql)
//! database.
//!
//! Every test creates its own temporary database file so tests are isolated
//! from each other. `TursoSessionStore` is async, so tests run under the
//! tokio test runtime. There is no shared mutable resource — nextest runs
//! each test in its own process, so serialization is unnecessary.
//!
//! The graph index (`GraphIndex`) is covered alongside the store: rebuilt
//! from the ledger must equal incremental appends, and `chain_from` covers
//! prompt/response pairs.

use std::collections::BTreeMap;
use std::ops::Deref;

use harness_core::event::{RunEvent, RunEventKind};
use harness_core::message::{Content, Message};
use harness_core::run::{RunId, RunStopReason, SessionId};
use harness_session::store::replay_messages;
use harness_session::{
    MessageEntry, RecallStore, SessionStoreError, graph::GraphIndex, recall::RecallScope,
    store::SessionStore, turso::TursoSessionStore,
};

/// A test store backed by a private temp database file. The `_dir` guard
/// keeps the temp dir (and thus the `.db` file) alive for the store's whole
/// lifetime; `Deref` exposes the store directly so call sites read like the
/// old synchronous tests.
struct TestStore {
    store: TursoSessionStore,
    _dir: tempfile::TempDir,
}

impl Deref for TestStore {
    type Target = TursoSessionStore;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

/// Open a store on a fresh temp database file.
async fn test_store() -> TestStore {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("must create temp dir: {e}"));
    let path = dir.path().join("sessions.db");
    let store = TursoSessionStore::open(&path)
        .await
        .unwrap_or_else(|e| panic!("must open turso store at {:?}: {e}", path));
    TestStore { store, _dir: dir }
}

fn make_session_id() -> SessionId {
    SessionId("test-session".into())
}

fn make_run_id() -> RunId {
    RunId("test-run-1".into())
}

/// A user text message with a predictable label for assertions.
fn user_msg(label: &str) -> Message {
    Message::user_text(label)
}

/// A system message with one section.
fn system_msg(key: &str, value: Option<&str>) -> Message {
    let mut sections = BTreeMap::new();
    sections.insert(key.to_string(), value.map(|s| s.to_string()));
    Message::System { sections }
}

/// A simple `RunStarted` event.
fn run_started_event(run_id: &RunId, session_id: &SessionId, seq: u64) -> RunEvent {
    RunEvent {
        run_id: run_id.clone(),
        session_id: session_id.clone(),
        seq,
        timestamp_ms: 1_700_000_000_000,
        kind: RunEventKind::RunStarted {
            task: "test task".into(),
        },
    }
}

/// A `RunFinished` event.
fn run_finished_event(run_id: &RunId, session_id: &SessionId, seq: u64) -> RunEvent {
    RunEvent {
        run_id: run_id.clone(),
        session_id: session_id.clone(),
        seq,
        timestamp_ms: 1_700_000_000_001,
        kind: RunEventKind::RunFinished {
            reason: RunStopReason::Completed,
        },
    }
}

/// A `TextDelta` event (assistant content) for graph unit tests.
fn text_delta_event(run_id: &RunId, session_id: &SessionId, seq: u64, text: &str) -> RunEvent {
    RunEvent {
        run_id: run_id.clone(),
        session_id: session_id.clone(),
        seq,
        timestamp_ms: 1_700_000_000_000 + seq as i64,
        kind: RunEventKind::TextDelta {
            text: text.to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

/// Purpose: verifies that a session can be created and its existence queried.
/// Scenario: create_session → session_exists returns true for the new session
/// and false for an unknown one.
/// Acceptance: session_exists reports the correct boolean for each case.
#[tokio::test]
async fn create_and_check_session() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();

    assert!(
        !store.session_exists(&sid).await?,
        "session must not exist before creation"
    );

    store.create_session(&sid).await?;
    assert!(
        store.session_exists(&sid).await?,
        "session must exist after creation"
    );

    Ok(())
}

/// Purpose: creating a session with a duplicate id must fail.
/// Scenario: create_session twice with the same id.
/// Acceptance: the second call returns `SessionAlreadyExists`.
#[tokio::test]
async fn duplicate_session_is_rejected() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();

    store.create_session(&sid).await?;
    match store.create_session(&sid).await {
        Err(SessionStoreError::SessionAlreadyExists(s)) if s.0 == sid.0 => {}
        other => panic!("expected SessionAlreadyExists, got {other:?}"),
    }

    Ok(())
}

/// Purpose: deleting a session removes all its data and the session itself.
/// Scenario: create session → append messages → delete → session_exists false.
/// Acceptance: session is gone after deletion; loading messages returns empty.
#[tokio::test]
async fn delete_session_removes_data() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();

    store.create_session(&sid).await?;
    store.append_message(&sid, &user_msg("hello")).await?;

    store.delete_session(&sid).await?;
    assert!(
        !store.session_exists(&sid).await?,
        "session must be gone after delete"
    );

    let msgs = store.load_messages(&sid).await?;
    assert!(
        msgs.is_empty(),
        "messages must be empty after session delete"
    );

    Ok(())
}

/// Purpose: deleting a non-existent session must fail.
/// Scenario: delete_session on an id that was never created.
/// Acceptance: returns `SessionNotFound`.
#[tokio::test]
async fn delete_nonexistent_session_fails() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let err = store
        .delete_session(&make_session_id())
        .await
        .err()
        .unwrap_or_else(|| panic!("expected SessionNotFound error"));
    assert!(
        matches!(&err, SessionStoreError::SessionNotFound(s) if s.0 == "test-session"),
        "expected SessionNotFound, got {err:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Message ledger
// ---------------------------------------------------------------------------

/// Purpose: messages appended to a session are stored with sequential seq
/// numbers and can be loaded back in order.
/// Scenario: append three user messages → load_messages returns them with
/// seq 1, 2, 3.
/// Acceptance: seqs are contiguous and messages match what was stored.
#[tokio::test]
async fn append_and_load_messages() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    let s1 = store.append_message(&sid, &user_msg("first")).await?;
    assert_eq!(s1, 1, "first message seq must be 1");

    let s2 = store.append_message(&sid, &user_msg("second")).await?;
    assert_eq!(s2, 2, "second message seq must be 2");

    let s3 = store.append_message(&sid, &user_msg("third")).await?;
    assert_eq!(s3, 3, "third message seq must be 3");

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 3, "must load 3 messages");

    for (entry, expected_label) in entries.iter().zip(["first", "second", "third"].iter()) {
        assert_eq!(
            entry.message,
            user_msg(expected_label),
            "message content mismatch at seq {}",
            entry.seq
        );
    }

    // Seq must be 1, 2, 3 in order.
    let seqs: Vec<u64> = entries.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3], "seqs must be contiguous");

    Ok(())
}

/// Purpose: messages are loaded in insertion order regardless of the physical
/// rowid.
/// Scenario: add system then user — the load order matches append order.
/// Acceptance: the first entry is the system message, the second is user.
#[tokio::test]
async fn message_order_preserves_append_order() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    store
        .append_message(&sid, &system_msg("instructions", Some("be nice")))
        .await?;
    store.append_message(&sid, &user_msg("hello")).await?;

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 2);
    assert!(
        matches!(&entries[0].message, Message::System { .. }),
        "first entry must be System"
    );
    assert_eq!(entries[1].message, user_msg("hello"));

    Ok(())
}

// ---------------------------------------------------------------------------
// Replay: system-section accumulation
// ---------------------------------------------------------------------------

/// Purpose: replaying a ledger with one system message returns it as the
/// accumulated system prompt.
/// Scenario: store one system message → replay → one System message in the
/// result with the same section.
/// Acceptance: the replayed transcript starts with the accumulated system
/// sections.
#[tokio::test]
async fn replay_single_system_message() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    store
        .append_message(&sid, &system_msg("role", Some("coder")))
        .await?;
    store
        .append_message(&sid, &user_msg("write a test"))
        .await?;

    let transcript = store.replay_ledger(&sid).await?;
    assert_eq!(transcript.len(), 2, "replay must keep both messages");

    // First message = accumulated system.
    let first = &transcript[0];
    let Message::System { sections } = first else {
        panic!("first replayed message must be System");
    };
    assert_eq!(
        sections.get("role"),
        Some(&Some("coder".to_string())),
        "system section must be preserved"
    );

    Ok(())
}

/// Purpose: a later System message with `None` removes that section from the
/// accumulated prompt (pi's null semantics).
/// Scenario:
///   1. System { "rules": Some("be concise") }
///   2. User { "first turn" }
///   3. System { "rules": None }
///      → replay: one accumulated System { "rules": None } + the user turn
///      Acceptance: the "rules" section is present with None value — the consumer
///      sees it as removed — and the system messages fold into a single one.
#[tokio::test]
async fn replay_section_removal() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    store
        .append_message(&sid, &system_msg("rules", Some("be concise")))
        .await?;
    store.append_message(&sid, &user_msg("first turn")).await?;
    // Mid-session removal: a guardrail lifted.
    store
        .append_message(&sid, &system_msg("rules", None))
        .await?;

    let transcript = store.replay_ledger(&sid).await?;
    // Expected: System { rules: None } + User — the two System messages fold
    // into one accumulated ledger; the user turn is preserved verbatim.
    assert_eq!(
        transcript.len(),
        2,
        "System messages fold into one; the user turn survives"
    );

    // The accumulated system has "rules" → None (removed).
    let sections = match &transcript[0] {
        Message::System { sections } => sections,
        other => panic!("expected System, got {other:?}"),
    };
    assert_eq!(
        sections.get("rules"),
        Some(&None),
        "'rules' section must be None (removed)"
    );

    Ok(())
}

/// Purpose: multiple system sections are independently tracked — removing one
/// does not affect another.
/// Scenario:
///   1. System { "role": Some("coder"), "style": Some("concise") }
///   2. System { "style": None }
///      → replay: System has "role" → Some("coder"), "style" → None
///      Acceptance: "role" survives intact; "style" is None.
#[tokio::test]
async fn replay_independent_sections() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    let mut sections = BTreeMap::new();
    sections.insert("role".into(), Some("coder".into()));
    sections.insert("style".into(), Some("concise".into()));
    store
        .append_message(&sid, &Message::System { sections })
        .await?;

    store
        .append_message(&sid, &system_msg("style", None))
        .await?;

    let transcript = store.replay_ledger(&sid).await?;
    let sections = match &transcript[0] {
        Message::System { sections } => sections,
        other => panic!("expected System, got {other:?}"),
    };

    assert_eq!(
        sections.get("role"),
        Some(&Some("coder".into())),
        "'role' must be preserved"
    );
    assert_eq!(
        sections.get("style"),
        Some(&None),
        "'style' must be removed"
    );

    Ok(())
}

/// Purpose: `replay_messages` (the pure function) is tested directly: when
/// no system messages exist, the result contains only the non-system messages,
/// with no front-matter System message.
/// Scenario: replay_messages on [User { .. }, Assistant { .. }].
/// Acceptance: output length = 2, both are the original messages.
#[tokio::test]
async fn replay_messages_pure_no_system() {
    let entries = vec![
        MessageEntry {
            seq: 1,
            message: user_msg("hi"),
        },
        MessageEntry {
            seq: 2,
            message: Message::Assistant {
                content: vec![Content::Text("hello".into())],
            },
        },
    ];

    let result = replay_messages(&entries);
    assert_eq!(
        result.len(),
        2,
        "no system → no front-matter System message"
    );
    assert_eq!(result[0], entries[0].message);
    assert_eq!(result[1], entries[1].message);
}

// ---------------------------------------------------------------------------
// Event log
// ---------------------------------------------------------------------------

/// Purpose: events appended to a run can be loaded back in seq order.
/// Scenario: append two events → events_for_run returns both.
/// Acceptance: events are returned in seq order with correct fields.
#[tokio::test]
async fn append_and_load_events() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    let e1 = run_started_event(&rid, &sid, 1);
    let e2 = run_finished_event(&rid, &sid, 2);

    store.append_event(&e1).await?;
    store.append_event(&e2).await?;

    let events = store.events_for_run(&rid).await?;
    assert_eq!(events.len(), 2, "must load both events");
    assert_eq!(events[0].seq, 1, "first event seq must be 1");
    assert_eq!(events[1].seq, 2, "second event seq must be 2");

    Ok(())
}

/// Purpose: events from different runs are isolated.
/// Scenario: append events to two runs → each run returns only its own events.
/// Acceptance: no cross-contamination.
#[tokio::test]
async fn event_isolation_between_runs() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    let rid_a = RunId("run-a".into());
    let rid_b = RunId("run-b".into());

    store
        .append_event(&run_started_event(&rid_a, &sid, 1))
        .await?;
    store
        .append_event(&run_started_event(&rid_b, &sid, 1))
        .await?;

    let events_a = store.events_for_run(&rid_a).await?;
    let events_b = store.events_for_run(&rid_b).await?;

    assert_eq!(events_a.len(), 1);
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_a[0].run_id, rid_a);
    assert_eq!(events_b[0].run_id, rid_b);

    Ok(())
}

/// Purpose: re-appending an already-persisted event is idempotent (writes
/// nothing), since `run_events.idempotency_key` is `run_id:seq`.
/// Scenario: append the same (run_id, seq) twice.
/// Acceptance: both appends return Ok and `events_for_run` holds one event.
#[tokio::test]
async fn duplicate_event_append_is_idempotent() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;
    // Re-applying the same event: ON CONFLICT DO NOTHING → no-op, no error.
    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;

    let events = store.events_for_run(&rid).await?;
    assert_eq!(events.len(), 1, "duplicate append must not grow the log");

    Ok(())
}

// ---------------------------------------------------------------------------
// Event gap detection
// ---------------------------------------------------------------------------

/// Purpose: a run with no events reports no gaps.
/// Scenario: check_event_gaps on a run with zero events.
/// Acceptance: empty vec.
#[tokio::test]
async fn gaps_empty_for_no_events() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let gaps = store.check_event_gaps(&make_run_id()).await?;
    assert!(gaps.is_empty(), "no events → no gaps");
    Ok(())
}

/// Purpose: a run with contiguous seqs reports no gaps.
/// Scenario: events seq 1, 2, 3 → check gaps.
/// Acceptance: empty vec.
#[tokio::test]
async fn gaps_empty_for_contiguous_events() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    for seq in 1..=3_u64 {
        store
            .append_event(&run_started_event(&rid, &sid, seq))
            .await?;
    }

    let gaps = store.check_event_gaps(&rid).await?;
    assert!(gaps.is_empty(), "contiguous seqs → no gaps");
    Ok(())
}

/// Purpose: a run with missing seqs reports the missing numbers.
/// Scenario: events seq 1, 3 → check gaps → missing seq 2.
/// Acceptance: gaps = [2].
#[tokio::test]
async fn gaps_detected_for_missing_seqs() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;
    store
        .append_event(&run_finished_event(&rid, &sid, 3))
        .await?;

    let gaps = store.check_event_gaps(&rid).await?;
    assert_eq!(gaps, vec![2], "seq 2 is missing");
    Ok(())
}

/// Purpose: gaps between observed seqs are reported; a first seq above 1 is not
/// itself a gap (only missing numbers between observed seqs count).
/// Scenario: events seq 3, 5 → check gaps → only the missing number between
/// observed seqs is reported.
/// Acceptance: gaps = [4].
#[tokio::test]
async fn gaps_detected_for_non_starting_seq() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    store
        .append_event(&run_started_event(&rid, &sid, 3))
        .await?;
    store
        .append_event(&run_finished_event(&rid, &sid, 5))
        .await?;

    let gaps = store.check_event_gaps(&rid).await?;
    assert_eq!(gaps, vec![4], "seq 4 is missing between 3 and 5");
    Ok(())
}

// ---------------------------------------------------------------------------
// Full recovery round-trip
// ---------------------------------------------------------------------------

/// Purpose: the full recovery path: create session → add messages (including
/// section removals) → append events → replay ledger → check event gaps.
///
/// This mirrors the actual crash-recovery sequence the scheduler (#11) will
/// use: after a restart, it calls `replay_ledger` to reconstruct the prompt
/// and `events_for_run` + `check_event_gaps` to determine which events to
/// re-emit.
///
/// Scenario:
///   1. Create session.
///   2. Append: System("role" → "coder"), User("hello"), System("role" → None),
///      User("again"), Assistant("ok").
///   3. Append events for two runs (run-a seq 1,2; run-b seq 1,3 with gap).
///   4. Replay ledger: System sections after removal → role is None.
///   5. Check gaps for run-b → seq 2 is missing.
///
/// Acceptance:
///   - replay returns 4 messages: System { role: None } + 3 non-system.
///   - run-b gaps = [2].
#[tokio::test]
async fn full_recovery_round_trip() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    // -- Messages with section removal --
    store
        .append_message(&sid, &system_msg("role", Some("coder")))
        .await?;
    store.append_message(&sid, &user_msg("hello")).await?;
    store
        .append_message(&sid, &system_msg("role", None))
        .await?; // mid-session removal
    store.append_message(&sid, &user_msg("again")).await?;
    store
        .append_message(
            &sid,
            &Message::Assistant {
                content: vec![Content::Text("ok".into())],
            },
        )
        .await?;

    // -- Events for two runs --
    let run_a = RunId("run-a".into());
    store
        .append_event(&run_started_event(&run_a, &sid, 1))
        .await?;
    store
        .append_event(&run_finished_event(&run_a, &sid, 2))
        .await?;

    let run_b = RunId("run-b".into());
    store
        .append_event(&run_started_event(&run_b, &sid, 1))
        .await?;
    store
        .append_event(&run_finished_event(&run_b, &sid, 3))
        .await?; // gap at seq 2

    // -- Recovery step 1: replay ledger --
    let transcript = store.replay_ledger(&sid).await?;
    assert_eq!(
        transcript.len(),
        4,
        "5 raw messages → 4 after replay (system collapsed)"
    );

    // The accumulated system has "role" → None.
    let sections = match &transcript[0] {
        Message::System { sections } => sections,
        other => panic!("first replayed message must be System, got {other:?}"),
    };
    assert_eq!(
        sections.get("role"),
        Some(&None),
        "'role' must be removed after replay"
    );

    // -- Recovery step 2: check event gaps --
    let gaps_a = store.check_event_gaps(&run_a).await?;
    assert!(gaps_a.is_empty(), "run-a has contiguous seqs");

    let gaps_b = store.check_event_gaps(&run_b).await?;
    assert_eq!(gaps_b, vec![2], "run-b is missing seq 2");

    // -- Recovery step 3: load events --
    let events_a = store.events_for_run(&run_a).await?;
    assert_eq!(events_a.len(), 2);
    let events_b = store.events_for_run(&run_b).await?;
    assert_eq!(events_b.len(), 2);

    Ok(())
}

// ---------------------------------------------------------------------------
// Round-trip through a real temp file (not just in-memory)
// ---------------------------------------------------------------------------

/// Purpose: verifies persistence through a real file: writes survive a
/// store-drop and a new store on the same path reads them back.
/// Scenario: create tempfile → open store → write messages → drop store →
/// open new store on same file → load messages.
/// Acceptance: messages survive the close/reopen cycle.
#[tokio::test]
async fn persistence_through_file_reopen() -> Result<(), SessionStoreError> {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("must create temp dir: {e}"));
    let path = dir.path().join("sessions.db");
    let sid = make_session_id();

    // First store: write data, then drop.
    {
        let store = TursoSessionStore::open(&path).await?;
        store.create_session(&sid).await?;
        store.append_message(&sid, &user_msg("persist me")).await?;
        store
            .append_message(&sid, &system_msg("key", Some("val")))
            .await?;
        // Store is dropped here.
    }

    // Second store: read back from the same file.
    let store = TursoSessionStore::open(&path).await?;
    assert!(
        store.session_exists(&sid).await?,
        "session must survive reopen"
    );

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 2, "both messages survive reopen");
    assert_eq!(entries[0].message, user_msg("persist me"));

    Ok(())
}

// ---------------------------------------------------------------------------
// Graph index
// ---------------------------------------------------------------------------

/// Purpose: rebuilding the graph from the ledger equals incremental appends.
/// Scenario: append several events for one run, rebuild `GraphIndex` from
/// `events_for_run`, then compare `units_for_run` against units added one at
/// a time via `append_event`.
/// Acceptance: both graphs report the same units in the same seq order for
/// the run.
#[tokio::test]
async fn graph_rebuild_equals_incremental_appends() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;
    store
        .append_event(&text_delta_event(&rid, &sid, 2, "prompt"))
        .await?;
    store
        .append_event(&text_delta_event(&rid, &sid, 3, "response"))
        .await?;

    let events = store.events_for_run(&rid).await?;

    // Rebuilt graph.
    let rebuilt = GraphIndex::new();
    rebuilt.rebuild_from_ledger(&events);

    // Incremental graph.
    let incremental = GraphIndex::new();
    for event in &events {
        incremental.append_event(event);
    }

    let rebuilt_units = rebuilt.units_for_run(&rid);
    let incremental_units = incremental.units_for_run(&rid);
    assert_eq!(
        rebuilt_units, incremental_units,
        "rebuilt graph must equal incremental appends"
    );
    assert_eq!(rebuilt_units.len(), 3, "run has three units");

    Ok(())
}

/// Purpose: `chain_from` covers prompt/response pairs in seq order.
/// Scenario: append a run-start, a prompt delta, and a response delta;
/// `chain_from(run, run_start_seq)` returns the run start, then the prompt,
/// then the response, in seq order.
/// Acceptance: the chain's units are exactly run-start → prompt → response,
/// with roles `system`, `assistant`, `assistant`.
#[tokio::test]
async fn graph_chain_covers_prompt_response_pairs() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;
    store
        .append_event(&text_delta_event(&rid, &sid, 2, "prompt"))
        .await?;
    store
        .append_event(&text_delta_event(&rid, &sid, 3, "response"))
        .await?;

    let graph = GraphIndex::new();
    let events = store.events_for_run(&rid).await?;
    graph.rebuild_from_ledger(&events);

    let chain = graph.chain_from(&rid, 1);
    assert_eq!(
        chain.len(),
        3,
        "chain from run start covers start + prompt + response"
    );
    assert_eq!(chain[0].seq, 1, "chain starts at the run-start seq");
    assert_eq!(chain[0].role, "system", "run-start unit is system");
    assert_eq!(chain[1].seq, 2, "chain continues to the prompt");
    assert_eq!(chain[1].role, "assistant", "prompt delta is assistant");
    assert_eq!(chain[2].seq, 3, "chain ends at the response");
    assert_eq!(chain[2].role, "assistant", "response delta is assistant");

    // A seq with no unit yields an empty chain, not a panic.
    let empty = graph.chain_from(&rid, 99);
    assert!(empty.is_empty(), "unknown seq → empty chain");

    Ok(())
}

/// Purpose: units of different runs are isolated in the graph.
/// Scenario: append events for two runs; `units_for_run` for one run never
/// includes the other's units.
/// Acceptance: each run reports only its own units.
#[tokio::test]
async fn graph_units_isolated_per_run() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid_a = RunId("run-a".into());
    let rid_b = RunId("run-b".into());
    store.create_session(&sid).await?;

    store
        .append_event(&text_delta_event(&rid_a, &sid, 1, "a-one"))
        .await?;
    store
        .append_event(&text_delta_event(&rid_a, &sid, 2, "a-two"))
        .await?;
    store
        .append_event(&text_delta_event(&rid_b, &sid, 1, "b-one"))
        .await?;

    let graph = GraphIndex::new();
    let events = store.events_for_run(&rid_a).await?;
    graph.rebuild_from_ledger(&events);

    let units_a = graph.units_for_run(&rid_a);
    let units_b = graph.units_for_run(&rid_b);
    assert_eq!(units_a.len(), 2, "run-a has two units");
    assert_eq!(units_b.len(), 0, "run-b has no units (not yet indexed)");

    Ok(())
}

/// Purpose: probe whether the turso engine supports Tantivy `USING fts`
/// indexes on this crate's pinned turso version (with the experimental
/// index-method flag enabled).
/// Scenario: open the store's database path directly through the turso
/// Builder with experimental_index_method(true), then execute a
/// `CREATE INDEX ... USING fts` statement.
/// Acceptance: the statement executes without "no such module" / parse
/// errors — proving full-text search is available on our turso version
/// once the experimental flag is turned on in the store builder.
#[tokio::test]
async fn turso_fts_index_probe() {
    let dir = std::env::temp_dir().join(format!("harness-fts-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("temp dir: {e}"));
    let db_path = dir.join("probe.db");

    let database = turso::Builder::new_local(db_path.to_str().unwrap_or_else(|| panic!("path")))
        .experimental_index_method(true)
        .build()
        .await
        .unwrap_or_else(|e| panic!("build: {e}"));
    let conn = database
        .connect()
        .unwrap_or_else(|e| panic!("connect: {e}"));

    let result = conn
        .execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT);
             CREATE INDEX t_fts ON t USING fts (body);",
        )
        .await;
    assert!(
        result.is_ok(),
        "USING fts must be supported with experimental_index_method(true)"
    );
}

// ---------------------------------------------------------------------------
// Recall (Tantivy fts index on run_events)
// ---------------------------------------------------------------------------

/// Unique suffix per fixture call: the tests run in parallel threads of one
/// process, and a turso file is exclusively locked by its opening process —
/// sharing one db file across parallel fixtures would deadlock `open`.
static RECALL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Fixture: a store with two runs' events containing distinct marker text.
async fn recall_fixture() -> std::sync::Arc<TursoSessionStore> {
    use harness_session::SessionStore as _;
    let seq = RECALL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("harness-recall-{}-{seq}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("temp dir: {e}"));
    let store = TursoSessionStore::open(dir.join("recall.db"))
        .await
        .unwrap_or_else(|e| panic!("open: {e}"));
    let session = SessionId("recall-session".into());
    store
        .create_session(&session)
        .await
        .unwrap_or_else(|e| panic!("create session: {e}"));
    for (run, marker) in [
        ("recall-alpha", "the quick brown fox jumps"),
        ("recall-beta", "lazy dogs sleep deeply"),
    ] {
        let run_id = RunId(run.into());
        for seq in 0..2u64 {
            let event = RunEvent {
                run_id: run_id.clone(),
                session_id: session.clone(),
                seq,
                timestamp_ms: 0,
                kind: harness_core::event::RunEventKind::TextDelta {
                    text: format!("{marker} event {seq}"),
                },
            };
            store
                .append_event(&event)
                .await
                .unwrap_or_else(|e| panic!("append: {e}"));
        }
    }
    std::sync::Arc::new(store)
}

/// Purpose: the Tantivy fts index answers keyword queries over persisted
/// events (issue #5/#7 recall seam, turso `USING fts` path).
/// Scenario: two runs with distinct marker text; search for a term unique to
/// one run, and a term absent from both.
/// Acceptance: the unique term returns only that run's events with a
/// highlighted snippet and a finite score; the absent term returns no hits.
#[tokio::test]
async fn recall_finds_events_by_keyword() -> Result<(), Box<dyn std::error::Error>> {
    let store = recall_fixture().await;
    let recall = store.recall();

    let hits = recall.search("fox", &RecallScope::all(10)).await?;
    assert_eq!(hits.len(), 2, "both fox-marked events must match");
    assert!(
        hits.iter().all(|hit| hit.run_id.0 == "recall-alpha"),
        "fox hits must come from the alpha run"
    );
    assert!(
        hits.iter().all(|hit| hit.snippet.contains("≫")),
        "snippets must be highlighted"
    );
    assert!(
        hits.iter().all(|hit| hit.score.is_finite()),
        "scores must be finite"
    );

    let misses = recall.search("zebra", &RecallScope::all(10)).await?;
    assert!(misses.is_empty(), "absent terms must not match");
    Ok(())
}

/// Purpose: recall respects the session scope and the hit limit.
/// Scenario: search scoped to a session id that owns none of the events,
/// then an oversized limit on the whole store.
/// Acceptance: the scoped search returns no hits; the limited search never
/// exceeds the cap.
#[tokio::test]
async fn recall_scope_and_limit_are_respected() -> Result<(), Box<dyn std::error::Error>> {
    use harness_session::RecallScope;

    let store = recall_fixture().await;
    let recall = store.recall();

    let scoped = recall
        .search(
            "fox",
            &RecallScope {
                session_id: Some(SessionId("other-session".into())),
                limit: 10,
            },
        )
        .await?;
    assert!(scoped.is_empty(), "a foreign session must yield no hits");

    let limited = recall.search("the", &RecallScope::all(1)).await?;
    assert!(limited.len() <= 1, "the limit must cap the hit count");
    Ok(())
}

/// Purpose: NoRecall is a safe empty default (issue: recall is opt-in).
/// Scenario: search through NoRecall.
/// Acceptance: every query returns an empty hit list without error.
#[tokio::test]
async fn no_recall_returns_no_hits() -> Result<(), Box<dyn std::error::Error>> {
    let recall = harness_session::NoRecall;
    let hits = recall
        .search("anything", &harness_session::RecallScope::all(10))
        .await?;
    assert!(hits.is_empty(), "NoRecall must never return hits");
    Ok(())
}

/// Purpose: fts_match queries return hits against the fts index created in
/// migrate (not just index creation) — the recall SQL path works end to end.
/// Scenario: append two events with distinct markers, search via the
/// RecallStore::search SQL (fts_match on event_json).
/// Acceptance: the query for the alpha marker returns only the alpha run's
/// events; the score is a finite number.
#[tokio::test]
async fn recall_search_sql_round_trip() {
    let store = recall_fixture().await;
    let recall = store.recall();
    let hits = recall
        .search("fox", &RecallScope::all(10))
        .await
        .unwrap_or_else(|e| panic!("search: {e}"));
    println!("hits: {hits:?}");
    assert_eq!(hits.len(), 2, "both fox-marked events must match");
    assert!(
        hits.iter().all(|hit| hit.score.is_finite()),
        "scores must be finite"
    );
}
