//! Integration tests for `harness-session` with a real Turso (libsql)
//! database.
//!
//! Every test creates its own temporary database file so tests are isolated
//! from each other. `TursoSessionStore` is async, so tests run under the
//! tokio test runtime. There is no shared mutable resource — nextest runs
//! each test in its own process, so serialization is unnecessary.
//!
//! The graph index (`GraphIndex`) is covered alongside the store: rebuilt
//! from the ledger must equal incremental appends, ancestors cover the unit
//! chain, and the graph replay path matches direct message assembly.

use std::collections::BTreeMap;
use std::ops::Deref;

use harness_core::event::{RunEvent, RunEventKind};
use harness_core::message::{Content, Message};
use harness_core::run::{RunId, RunStopReason, SessionId};
use harness_session::store::{RewindBoundary, replay_messages};
use harness_session::{
    MessageEntry, RecallStore, SessionStoreError, Unit, UnitKind, graph::GraphIndex,
    recall::RecallScope, store::SessionStore, turso::TursoSessionStore,
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

// ---------------------------------------------------------------------------
// latest run resolution (#9)
// ---------------------------------------------------------------------------

/// Purpose: `latest_run_for_session` resolves a session to the run whose
/// latest event carries the highest seq, so a client that knows only the
/// session can re-attach to its most recent run.
/// Scenario: create a session, append one event to run-a (seq 1) and two
/// events to run-b (seq 1, 2), then query `latest_run_for_session`.
/// Acceptance: returns `Some(run_b)` — highest max seq wins.
#[tokio::test]
async fn latest_run_for_session_returns_newest_run() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let session = SessionId("resume-session".into());
    store.create_session(&session).await?;
    let run_a = RunId("run-a".into());
    let run_b = RunId("run-b".into());
    store
        .append_event(&run_started_event(&run_a, &session, 1))
        .await?;
    store
        .append_event(&run_started_event(&run_b, &session, 1))
        .await?;
    store
        .append_event(&run_finished_event(&run_b, &session, 2))
        .await?;
    let latest = store.latest_run_for_session(&session).await?;
    assert_eq!(latest, Some(run_b), "the run with the highest max seq wins");
    Ok(())
}

/// Purpose: `latest_run_for_session` reports `None` for a session with no
/// runs, so the transport can reject a `session_id`-only `subscribe`.
/// Scenario: query `latest_run_for_session` for a session that was never
/// created.
/// Acceptance: returns `Ok(None)`.
#[tokio::test]
async fn latest_run_for_session_none_for_unknown_session() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let session = SessionId("no-such-session".into());
    let latest = store.latest_run_for_session(&session).await?;
    assert_eq!(latest, None, "an unknown session has no runs to resume");
    Ok(())
}

// ---------------------------------------------------------------------------
// Event gap detection

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
// Unit graph
// ---------------------------------------------------------------------------

/// Build one unit of `session` at `order`, parented to `parent`.
fn unit(session: &SessionId, order: u64, message_seq: u64, parent: Option<&str>) -> Unit {
    let event_id = format!("{}:main:1:{order}", session.0);
    Unit {
        event_id: event_id.clone(),
        session_id: session.clone(),
        branch_id: "main".into(),
        epoch: 1,
        event_order: order,
        message_seq,
        role: "user".into(),
        kind: UnitKind::Prompt,
        run_id: None,
        source_kind: "user".into(),
        tool_call_id: None,
        idempotency_key: Some(format!("{event_id}:key")),
        integrity_hash: format!("{event_id}:hash"),
        parent_event_id: parent.map(str::to_string),
        created_at_ms: 0,
    }
}

/// Purpose: rebuilding the graph from the ledger equals incremental appends.
/// Scenario: three chained units, rebuilt in one pass vs appended one by one.
/// Acceptance: both graphs report the same units in the same order and the
/// same root→head ancestor chain.
#[tokio::test]
async fn graph_rebuild_equals_incremental_appends() {
    let session = SessionId("graph".into());
    let first = unit(&session, 1, 1, None);
    let second = unit(&session, 2, 2, Some(&first.event_id));
    let third = unit(&session, 3, 3, Some(&second.event_id));
    let units = vec![first.clone(), second.clone(), third.clone()];

    let rebuilt = GraphIndex::new();
    rebuilt.rebuild(&units);

    let incremental = GraphIndex::new();
    incremental.rebuild(&[first]);
    incremental.append(&[second, third], &units[2].event_id);

    assert_eq!(
        rebuilt.units_for_session(&session),
        incremental.units_for_session(&session),
        "rebuild and incremental appends must agree"
    );
    assert_eq!(
        rebuilt.ancestors(&units[2].event_id),
        vec![
            units[0].event_id.clone(),
            units[1].event_id.clone(),
            units[2].event_id.clone()
        ],
        "the ancestor chain runs root -> head"
    );
    assert!(
        rebuilt.ancestors("missing-unit").is_empty(),
        "an unknown unit has no chain"
    );
}

/// Purpose: units of different sessions are isolated in the graph.
/// Scenario: two sessions, each with its own chain.
/// Acceptance: each session reports only its own units.
#[tokio::test]
async fn graph_units_isolated_per_session() {
    let a = SessionId("graph-a".into());
    let b = SessionId("graph-b".into());
    let a1 = unit(&a, 1, 1, None);
    let a2 = unit(&a, 2, 2, Some(&a1.event_id));
    let b1 = unit(&b, 1, 1, None);

    let graph = GraphIndex::new();
    graph.rebuild(&[a1, a2, b1]);

    assert_eq!(graph.units_for_session(&a).len(), 2, "session a keeps two");
    assert_eq!(graph.units_for_session(&b).len(), 1, "session b keeps one");
}

/// Purpose: the ledger splits an assistant message into blocks and reassembles
/// them in order, so the graph's unit granularity does not change the model
/// transcript.
/// Scenario: append a user prompt, an assistant message carrying text plus a
/// tool call, and a tool result; load and replay.
/// Acceptance: `load_messages` returns three messages with the assistant's two
/// blocks in declaration order, and `replay_ledger` (graph path) equals
/// `replay_messages(load_messages)`.
#[tokio::test]
async fn unit_blocks_reassemble_and_graph_replay_matches() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    store.append_message(&sid, &user_msg("question")).await?;
    store
        .append_message(
            &sid,
            &Message::Assistant {
                content: vec![
                    Content::Text("looking".into()),
                    Content::ToolCall(harness_core::message::ToolCall {
                        id: "call-1".into(),
                        name: "grep".into(),
                        args: serde_json::json!({"q": "x"}),
                    }),
                ],
            },
        )
        .await?;
    store
        .append_message(
            &sid,
            &Message::ToolResult(harness_core::message::ToolResultMessage {
                tool_call_id: "call-1".into(),
                tool_name: "grep".into(),
                content: "found".into(),
                is_error: false,
            }),
        )
        .await?;

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 3, "three messages");
    let Message::Assistant { content } = &entries[1].message else {
        panic!("second message must be the assistant reply");
    };
    assert_eq!(content.len(), 2, "text plus tool call");
    assert!(matches!(content[0], Content::Text(_)), "text first");
    assert!(matches!(content[1], Content::ToolCall(_)), "call second");

    let replayed = store.replay_ledger(&sid).await?;
    assert_eq!(
        replayed,
        replay_messages(&entries),
        "the graph replay path must match the direct assembly"
    );
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

/// Purpose: the search query is bound, not interpolated, so quotes and
/// injection-shaped input cannot break or reshape the statement.
/// Scenario: search for an apostrophe-containing term and for a string that
/// would close the literal and add `OR 1=1`.
/// Acceptance: the quoted search succeeds without a storage error; the
/// injection-shaped search returns no hits instead of every event.
#[tokio::test]
async fn recall_search_binds_quotes_and_injection_shaped_input()
-> Result<(), Box<dyn std::error::Error>> {
    let store = recall_fixture().await;
    let recall = store.recall();

    let quoted = recall.search("it's", &RecallScope::all(10)).await?;
    assert!(quoted.is_empty(), "no event contains the quoted term");

    let injected = recall
        .search("zzz') OR 1=1 OR ('z'=", &RecallScope::all(10))
        .await;
    match injected {
        Ok(hits) => assert!(
            hits.is_empty(),
            "injection-shaped input must not return every row: {hits:?}"
        ),
        Err(error) => assert!(
            matches!(error, SessionStoreError::Storage(_)),
            "the FTS parser may reject the expression, but not as a successful match: {error}"
        ),
    }
    Ok(())
}

/// Purpose: an oversized query is rejected before it reaches the FTS engine.
/// Scenario: 17 KiB of text against the 16 KiB cap.
/// Acceptance: `InvalidQuery`, and the search does not hit the database.
#[tokio::test]
async fn recall_search_rejects_oversized_queries() -> Result<(), Box<dyn std::error::Error>> {
    let store = recall_fixture().await;
    let recall = store.recall();
    let query = "x".repeat(17 * 1024);
    let result = recall.search(&query, &RecallScope::all(10)).await;
    assert!(
        matches!(result, Err(SessionStoreError::InvalidQuery(_))),
        "oversized queries must be rejected: {result:?}"
    );
    Ok(())
}

/// Purpose: a corrupt parent cycle cannot hang the ancestor walk.
/// Scenario: two units point at each other as parents.
/// Acceptance: `ancestors` returns an empty chain instead of looping.
#[tokio::test]
async fn graph_ancestors_bounded_on_cycle() {
    let session = SessionId("cycle".into());
    let mut first = unit(&session, 1, 1, Some("cycle:main:1:2"));
    let second = unit(&session, 2, 2, Some("cycle:main:1:1"));
    first.parent_event_id = Some(second.event_id.clone());
    let graph = GraphIndex::new();
    graph.rebuild(&[first.clone(), second]);
    assert!(
        graph.ancestors(&first.event_id).is_empty(),
        "a cycle yields no provable chain"
    );
}

/// Purpose: an assistant message with no content blocks still occupies one
/// message sequence and survives the unit split/assemble round trip.
/// Scenario: user prompt, empty assistant, user prompt.
/// Acceptance: three entries with seq 1,2,3; the empty assistant is present
/// with an empty content vector.
#[tokio::test]
async fn assistant_empty_content_round_trip() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;

    store.append_message(&sid, &user_msg("one")).await?;
    store
        .append_message(
            &sid,
            &Message::Assistant {
                content: Vec::new(),
            },
        )
        .await?;
    store.append_message(&sid, &user_msg("two")).await?;

    let entries = store.load_messages(&sid).await?;
    let seqs: Vec<u64> = entries.iter().map(|entry| entry.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3], "the empty assistant keeps its slot");
    let Message::Assistant { content } = &entries[1].message else {
        panic!("second entry must be the assistant message");
    };
    assert!(content.is_empty(), "no blocks were invented");
    Ok(())
}

/// Purpose: multiple tool calls in one assistant message stay one message and
/// keep their declaration order after the unit round trip.
/// Scenario: assistant with two ToolCall blocks and no text.
/// Acceptance: one assistant entry whose content is two ToolCalls in order.
#[tokio::test]
async fn assistant_multiple_tool_calls_reassemble() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    let call = |id: &str, name: &str| harness_core::message::ToolCall {
        id: id.into(),
        name: name.into(),
        args: serde_json::json!({}),
    };
    store
        .append_message(
            &sid,
            &Message::Assistant {
                content: vec![
                    Content::ToolCall(call("c1", "read")),
                    Content::ToolCall(call("c2", "grep")),
                ],
            },
        )
        .await?;

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 1, "one message, two units");
    let Message::Assistant { content } = &entries[0].message else {
        panic!("expected an assistant message");
    };
    assert_eq!(content.len(), 2, "both calls survive");
    assert!(matches!(&content[0], Content::ToolCall(c) if c.id == "c1"));
    assert!(matches!(&content[1], Content::ToolCall(c) if c.id == "c2"));
    Ok(())
}

/// Purpose: replay after closing and reopening the store returns the same
/// transcript (the graph is rebuilt from the ledger, not cached across).
/// Scenario: append user/assistant messages, drop the store, reopen the same
/// database file, replay.
/// Acceptance: the replay equals the pre-restart replay.
#[tokio::test]
async fn replay_survives_store_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("sessions.db");
    let sid = make_session_id();
    let before = {
        let store = TursoSessionStore::open(&path).await?;
        store.create_session(&sid).await?;
        store.append_message(&sid, &user_msg("remember me")).await?;
        store
            .append_message(
                &sid,
                &Message::Assistant {
                    content: vec![Content::Text("noted".into())],
                },
            )
            .await?;
        store.replay_ledger(&sid).await?
    };
    let store = TursoSessionStore::open(&path).await?;
    let after = store.replay_ledger(&sid).await?;
    assert_eq!(before, after, "replay is rebuilt from the ledger");
    assert_eq!(after.len(), 2, "both messages survived the reopen");
    Ok(())
}

/// Purpose: a system section removed and later re-added replays with the final
/// value, matching the prompt-ledger contract.
/// Scenario: role=coder, role=None, role=coder again, then a user turn.
/// Acceptance: the replay starts with role=coder and keeps the user message.
#[tokio::test]
async fn replay_system_section_readded() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    store
        .append_message(&sid, &system_msg("role", Some("coder")))
        .await?;
    store
        .append_message(&sid, &system_msg("role", None))
        .await?;
    store
        .append_message(&sid, &system_msg("role", Some("coder")))
        .await?;
    store.append_message(&sid, &user_msg("hello")).await?;

    let replay = store.replay_ledger(&sid).await?;
    let Message::System { sections } = &replay[0] else {
        panic!("replay must lead with the accumulated system ledger");
    };
    assert_eq!(
        sections.get("role").map(|value| value.as_deref()),
        Some(Some("coder")),
        "the re-added section wins"
    );
    assert_eq!(replay.len(), 2, "system ledger plus the user turn");
    Ok(())
}

/// Purpose: `latest_event_for_run` point-reads the tail of a run.
/// Scenario: two events appended to one run.
/// Acceptance: the method returns the higher-seq event, and `None` for an
/// unknown run.
#[tokio::test]
async fn latest_event_for_run_returns_tail() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;
    store
        .append_event(&run_started_event(&rid, &sid, 1))
        .await?;
    store
        .append_event(&run_finished_event(&rid, &sid, 2))
        .await?;

    let latest = store.latest_event_for_run(&rid).await?;
    assert_eq!(latest.map(|event| event.seq), Some(2), "tail is seq 2");
    assert!(
        store
            .latest_event_for_run(&RunId("missing".into()))
            .await?
            .is_none(),
        "an unknown run has no tail"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Provenance columns (PR-1: turn/run source, idempotency key, integrity hash)
// ---------------------------------------------------------------------------

/// Purpose: run-tagged appends record provenance on every unit row.
/// Scenario: one user message and one assistant message with a tool call,
/// appended through `append_message_for_run` with a run id.
/// Acceptance: every unit carries the run id, a `source_kind` matching its
/// role family, the tool call's id, a run-scoped idempotency key, and a
/// sha256-length integrity hash; `load_messages` still assembles the round
/// trip.
#[tokio::test]
async fn run_tagged_appends_record_provenance() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;

    let assistant = Message::Assistant {
        content: vec![
            Content::Text("thinking".into()),
            Content::ToolCall(crate_style_tool_call("call-1")),
        ],
    };
    store
        .append_message_for_run(&sid, Some(&rid), None, &user_msg("tagged"))
        .await?;
    store
        .append_message_for_run(&sid, Some(&rid), None, &assistant)
        .await?;

    let units = store.load_units(&sid).await?;
    assert_eq!(units.len(), 3, "user + text + tool call = three units");
    for stored in &units {
        assert_eq!(
            stored.unit.run_id.as_deref(),
            Some(rid.0.as_str()),
            "unit {} carries the run id",
            stored.unit.event_id
        );
        assert!(
            stored.unit.idempotency_key.is_some(),
            "run-tagged units carry an idempotency key"
        );
        assert_eq!(
            stored.unit.integrity_hash.len(),
            64,
            "integrity hash is sha256 hex"
        );
    }
    assert_eq!(units[0].unit.source_kind, "user");
    assert_eq!(units[1].unit.source_kind, "model");
    assert_eq!(units[2].unit.source_kind, "model");
    assert_eq!(
        units[2].unit.tool_call_id.as_deref(),
        Some("call-1"),
        "the tool call unit records its call id"
    );

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 2, "both messages reassemble");
    Ok(())
}

/// Purpose: steering appends are recorded with their own unit kind.
/// Scenario: one run-tagged steering message, then a replay.
/// Acceptance: the stored unit has `kind = steering` with `source_kind =
/// user`; replay surfaces it as a user turn so the model treats it like any
/// instruction.
#[tokio::test]
async fn steering_appends_record_kind_and_replay_as_user() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;
    store
        .append_message_for_run(&sid, Some(&rid), None, &user_msg("task"))
        .await?;
    store
        .append_steering_message(&sid, Some(&rid), &user_msg("steer: stop"))
        .await?;

    let units = store.load_units(&sid).await?;
    assert_eq!(units.len(), 2);
    assert_eq!(units[1].unit.kind, UnitKind::Steering);
    assert_eq!(units[1].unit.source_kind, "user");
    assert_eq!(
        units[1].unit.run_id.as_deref(),
        Some(rid.0.as_str()),
        "steering carries the run id too"
    );

    let replay = store.replay_ledger(&sid).await?;
    assert_eq!(replay.len(), 2, "steering replays as a user turn");
    assert_eq!(replay[1], user_msg("steer: stop"));
    Ok(())
}

// ---------------------------------------------------------------------------
// History rewind (PR-2: markers, retained-prefix replay, clear)
// ---------------------------------------------------------------------------

/// Purpose: rewinding to a unit keeps only the retained prefix in replay.
/// Scenario: three user messages; rewind to the second message's unit.
/// Acceptance: the epoch advances, a marker with the cutoff is recorded, the
/// head points at the cutoff unit, and replay yields exactly messages one and
/// two.
#[tokio::test]
async fn rewind_to_unit_keeps_retained_prefix() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    for label in ["one", "two", "three"] {
        store.append_message(&sid, &user_msg(label)).await?;
    }
    let units = store.load_units(&sid).await?;
    let cutoff = units[1].unit.event_id.clone();

    let new_epoch = store
        .rewind_to_unit(&sid, &cutoff, RewindBoundary::Clean)
        .await?;
    assert_eq!(new_epoch, 2, "rewind opens epoch 2");

    let head = store.load_head(&sid).await?;
    assert_eq!(head.epoch, 2);
    assert_eq!(
        head.head_event_id.as_deref(),
        Some(cutoff.as_str()),
        "the head continues from the cutoff unit"
    );

    let replay = store.replay_ledger(&sid).await?;
    assert_eq!(
        replay,
        vec![user_msg("one"), user_msg("two")],
        "replay keeps the retained prefix only"
    );

    let markers = store.history_markers(&sid).await?;
    assert_eq!(markers.len(), 1, "one marker recorded");
    assert_eq!(markers[0].from_epoch, 1);
    assert_eq!(markers[0].to_epoch, 2);
    assert_eq!(markers[0].cutoff_event_id.as_deref(), Some(cutoff.as_str()));
    assert_eq!(markers[0].boundary, RewindBoundary::Clean);
    Ok(())
}

/// Purpose: appends after a rewind continue from the cutoff.
/// Scenario: rewind to the second message, then append a new message.
/// Acceptance: the new unit lands in the new epoch, chains from the cutoff
/// unit, its message sequence continues the old numbering, and replay shows
/// old prefix + new message.
#[tokio::test]
async fn append_after_rewind_chains_from_cutoff() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    for label in ["one", "two", "three"] {
        store.append_message(&sid, &user_msg(label)).await?;
    }
    let cutoff = store.load_units(&sid).await?[1].unit.event_id.clone();
    store
        .rewind_to_unit(&sid, &cutoff, RewindBoundary::Clean)
        .await?;

    store.append_message(&sid, &user_msg("fresh")).await?;
    let units = store.load_units(&sid).await?;
    let fresh = units
        .iter()
        .find(|stored| stored.unit.epoch == 2)
        .ok_or_else(|| SessionStoreError::Internal("new message not in epoch 2".into()))?;
    assert_eq!(
        fresh.unit.parent_event_id.as_deref(),
        Some(cutoff.as_str()),
        "the new unit chains from the cutoff"
    );
    assert_eq!(
        fresh.unit.message_seq, 4,
        "sequence numbers stay monotonic across rewinds: the superseded tail's
         numbers are never reused"
    );

    let replay = store.replay_ledger(&sid).await?;
    assert_eq!(
        replay,
        vec![user_msg("one"), user_msg("two"), user_msg("fresh")],
        "replay is retained prefix plus the new turn"
    );
    Ok(())
}

/// Purpose: rewinding to the current head is a no-op.
/// Scenario: rewind to the head unit of a two-message session.
/// Acceptance: the epoch does not move and no marker is written.
#[tokio::test]
async fn rewind_to_head_is_a_noop() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    store.append_message(&sid, &user_msg("one")).await?;
    store.append_message(&sid, &user_msg("two")).await?;
    let head_unit = store
        .load_head(&sid)
        .await?
        .head_event_id
        .ok_or_else(|| SessionStoreError::Internal("head has no unit".into()))?;

    let epoch = store
        .rewind_to_unit(&sid, &head_unit, RewindBoundary::Clean)
        .await?;
    assert_eq!(epoch, 1, "no rewind happened");
    assert!(
        store.history_markers(&sid).await?.is_empty(),
        "a no-op writes no marker"
    );
    Ok(())
}

/// Purpose: rewinding outside the head scope is rejected.
/// Scenario: rewind to a unit of a different session.
/// Acceptance: the call errors and no marker is written.
#[tokio::test]
async fn rewind_rejects_foreign_units() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    store.append_message(&sid, &user_msg("one")).await?;

    let other = SessionId("elsewhere".into());
    let mut foreign = unit(&other, 1, 1, None);
    foreign.event_id = "elsewhere:main:1:1".into();
    let result = store
        .rewind_to_unit(&sid, &foreign.event_id, RewindBoundary::Clean)
        .await;
    assert!(
        result.is_err(),
        "a unit outside the session cannot be a cutoff"
    );
    assert!(store.history_markers(&sid).await?.is_empty());
    Ok(())
}

/// Purpose: clear empties replay but keeps stored units for audit.
/// Scenario: two messages, rewind_clear, then inspect replay and the ledger.
/// Acceptance: replay is empty, a ClearEmpty marker exists, the old units
/// remain stored, and a fresh append starts a parentless chain whose replay
/// contains only the new message.
#[tokio::test]
async fn rewind_clear_empties_replay_keeps_audit() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    store.append_message(&sid, &user_msg("one")).await?;
    store.append_message(&sid, &user_msg("two")).await?;

    let epoch = store.rewind_clear(&sid).await?;
    assert_eq!(epoch, 2);

    let replay = store.replay_ledger(&sid).await?;
    assert!(replay.is_empty(), "clear yields an empty transcript");

    let markers = store.history_markers(&sid).await?;
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].boundary, RewindBoundary::ClearEmpty);
    assert_eq!(markers[0].cutoff_event_id, None);

    let units = store.load_units(&sid).await?;
    assert_eq!(units.len(), 2, "old units stay stored for audit");

    store.append_message(&sid, &user_msg("after clear")).await?;
    let units = store.load_units(&sid).await?;
    let fresh = units
        .iter()
        .find(|stored| stored.unit.epoch == 2)
        .ok_or_else(|| SessionStoreError::Internal("post-clear append not in epoch 2".into()))?;
    assert!(
        fresh.unit.parent_event_id.is_none(),
        "a cleared epoch starts a parentless chain"
    );
    let replay = store.replay_ledger(&sid).await?;
    assert_eq!(
        replay,
        vec![user_msg("after clear")],
        "replay shows only the post-clear turn"
    );
    Ok(())
}

/// Purpose: delete_session removes markers with the rest of the session.
/// Scenario: rewind, then delete the session.
/// Acceptance: markers are gone and a fresh session with the same id can be
/// created.
#[tokio::test]
async fn delete_session_removes_markers() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    store.create_session(&sid).await?;
    store.append_message(&sid, &user_msg("one")).await?;
    store.rewind_clear(&sid).await?;
    assert_eq!(store.history_markers(&sid).await?.len(), 1);

    store.delete_session(&sid).await?;
    assert!(
        store.history_markers(&sid).await?.is_empty(),
        "markers die with the session"
    );
    store.create_session(&sid).await?;
    Ok(())
}

/// A minimal tool call for provenance assertions.
fn crate_style_tool_call(id: &str) -> harness_core::message::ToolCall {
    harness_core::message::ToolCall {
        id: id.to_string(),
        name: "read".into(),
        args: serde_json::json!({"path": "/tmp/x"}),
    }
}

/// Purpose: run-tagged appends keep working after a rewind.
/// Scenario: three run-tagged messages, rewind to the second message's unit,
/// then append a fourth message with the same run id.
/// Acceptance: the append succeeds (its idempotency key continues at seq 4
/// and does not collide with the superseded tail's keys) and both
/// load_messages and replay_ledger return every turn: prefix plus the new
/// message, with the discarded third message gone.
#[tokio::test]
async fn run_tagged_append_after_rewind_does_not_collide() -> Result<(), SessionStoreError> {
    let store = test_store().await;
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid).await?;
    for label in ["one", "two", "three"] {
        store
            .append_message_for_run(&sid, Some(&rid), None, &user_msg(label))
            .await?;
    }
    let cutoff = store.load_units(&sid).await?[1].unit.event_id.clone();
    store
        .rewind_to_unit(&sid, &cutoff, RewindBoundary::Clean)
        .await?;

    store
        .append_message_for_run(&sid, Some(&rid), None, &user_msg("fresh"))
        .await?;

    let units = store.load_units(&sid).await?;
    assert_eq!(
        units.len(),
        4,
        "prefix (2 units) + discarded tail (1) + new message (1)"
    );
    // load_messages reads the full ledger: the superseded tail stays visible
    // for audit, and monotonic sequences keep the cross-epoch rows from
    // merging into one message.
    let loaded = store.load_messages(&sid).await?;
    let loaded: Vec<Message> = loaded.into_iter().map(|entry| entry.message).collect();
    assert_eq!(
        loaded,
        vec![
            user_msg("one"),
            user_msg("two"),
            user_msg("three"),
            user_msg("fresh"),
        ],
        "the full ledger keeps every turn, none merged away"
    );
    // replay_ledger reads the retained history: prefix plus the new turn.
    let replay = store.replay_ledger(&sid).await?;
    assert_eq!(
        replay,
        vec![user_msg("one"), user_msg("two"), user_msg("fresh")]
    );
    Ok(())
}

/// Purpose: a database written before the provenance migration stays
/// readable after the upgrade.
/// Scenario: build a v1-schema database file by hand (no provenance
/// columns), insert legacy unit rows the old writer would have written, then
/// open the store on it so migrations run.
/// Acceptance: open + migrations succeed, legacy rows decode (source_kind
/// derived from role, integrity verification skipped for the empty legacy
/// hash), load_messages assembles them, and a fresh append writes full
/// provenance.
#[tokio::test]
async fn pre_upgrade_rows_stay_readable_after_migration() -> Result<(), SessionStoreError> {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("must create temp dir: {e}"));
    let path = dir.path().join("legacy.db");

    // v1 schema exactly as the pre-provenance writer left it: units with the
    // ten original columns and no markers table.
    let v1_sql = "CREATE TABLE sessions ( session_id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL ); CREATE TABLE units ( event_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, branch_id TEXT NOT NULL, epoch INTEGER NOT NULL, event_order INTEGER NOT NULL, message_seq INTEGER NOT NULL, role TEXT NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, parent_event_id TEXT, created_at_ms INTEGER NOT NULL, FOREIGN KEY (session_id) REFERENCES sessions(session_id), UNIQUE(session_id, branch_id, epoch, event_order) ); CREATE TABLE session_heads ( session_id TEXT PRIMARY KEY, branch_id TEXT NOT NULL, epoch INTEGER NOT NULL, head_event_id TEXT, last_event_order INTEGER NOT NULL, last_message_seq INTEGER NOT NULL, FOREIGN KEY (session_id) REFERENCES sessions(session_id) ); CREATE TABLE run_events ( id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, session_id TEXT NOT NULL, seq INTEGER NOT NULL, event_json TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, idempotency_key TEXT UNIQUE NOT NULL, FOREIGN KEY (session_id) REFERENCES sessions(session_id), UNIQUE(run_id, seq) ); CREATE INDEX idx_units_session_order ON units(session_id, event_order); CREATE INDEX idx_run_events_run_seq ON run_events(run_id, seq);";
    {
        let db = turso::Builder::new_local(
            path.to_str()
                .ok_or_else(|| SessionStoreError::Storage("bad path".into()))?,
        )
        .build()
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let conn = db
            .connect()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        conn.execute_batch(v1_sql)
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        conn.execute_batch(
            "INSERT INTO sessions VALUES ('legacy', 1);
             INSERT INTO session_heads VALUES ('legacy', 'main', 1, 'legacy:main:1:1', 1, 1);
             INSERT INTO units VALUES ('legacy:main:1:1', 'legacy', 'main', 1, 1, 1, 'user', 'prompt', '[{\"Text\":\"old turn\"}]', NULL, 1);",
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    }

    // Opening runs the migrations over the legacy file.
    let store = TursoSessionStore::open(&path).await?;
    let sid = SessionId("legacy".into());
    assert!(store.session_exists(&sid).await?);

    let entries = store.load_messages(&sid).await?;
    assert_eq!(entries.len(), 1, "the legacy turn survives the upgrade");
    assert_eq!(
        entries[0].message,
        Message::User {
            content: vec![Content::Text("old turn".into())],
        },
    );
    let units = store.load_units(&sid).await?;
    assert_eq!(
        units[0].unit.source_kind, "user",
        "legacy source_kind is derived from the role"
    );
    assert!(
        units[0].unit.integrity_hash.is_empty(),
        "legacy rows carry no hash and verification skips them"
    );

    // New appends land with full provenance on the upgraded schema.
    store
        .append_message(&sid, &user_msg("after upgrade"))
        .await?;
    let units = store.load_units(&sid).await?;
    let fresh = units
        .iter()
        .find(|stored| stored.unit.created_at_ms > 1)
        .ok_or_else(|| SessionStoreError::Internal("post-upgrade append not found".into()))?;
    assert!(
        !fresh.unit.integrity_hash.is_empty(),
        "post-upgrade rows carry a verifiable hash"
    );
    Ok(())
}
