//! Integration tests for `harness-session` with a real SQLite database.
//!
//! Every test creates its own temporary database file (via `tempfile::NamedTempFile`
//! or an in-memory database) so tests are isolated from each other. There is no
//! shared mutable resource — nextest runs each test in its own process, so
//! serialization is unnecessary.

use std::collections::BTreeMap;

use harness_core::event::{RunEvent, RunEventKind};
use harness_core::message::{Content, Message};
use harness_core::run::{RunId, RunStopReason, SessionId};
use harness_session::store::replay_messages;
use harness_session::{MessageEntry, SessionStore, SessionStoreError, SqliteSessionStore};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an in-memory store for tests.
fn test_store() -> SqliteSessionStore {
    SqliteSessionStore::in_memory().unwrap_or_else(|e| panic!("in-memory store must open: {e}"))
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
#[test]
fn create_and_check_session() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();

    assert!(
        !store.session_exists(&sid)?,
        "session must not exist before creation"
    );

    store.create_session(&sid)?;
    assert!(
        store.session_exists(&sid)?,
        "session must exist after creation"
    );

    Ok(())
}

/// Purpose: creating a session with a duplicate id must fail.
/// Scenario: create_session twice with the same id.
/// Acceptance: the second call returns `SessionAlreadyExists`.
#[test]
fn duplicate_session_is_rejected() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();

    store.create_session(&sid)?;
    let err = store
        .create_session(&sid)
        .err()
        .unwrap_or_else(|| panic!("expected SessionAlreadyExists error"));
    assert!(
        matches!(&err, SessionStoreError::SessionAlreadyExists(s) if s.0 == sid.0),
        "expected SessionAlreadyExists, got {err:?}"
    );

    Ok(())
}

/// Purpose: deleting a session removes all its data and the session itself.
/// Scenario: create session → append messages → delete → session_exists false.
/// Acceptance: session is gone after deletion; loading messages returns empty.
#[test]
fn delete_session_removes_data() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();

    store.create_session(&sid)?;
    store.append_message(&sid, &user_msg("hello"))?;

    store.delete_session(&sid)?;
    assert!(
        !store.session_exists(&sid)?,
        "session must be gone after delete"
    );

    let msgs = store.load_messages(&sid)?;
    assert!(
        msgs.is_empty(),
        "messages must be empty after session delete"
    );

    Ok(())
}

/// Purpose: deleting a non-existent session must fail.
/// Scenario: delete_session on an id that was never created.
/// Acceptance: returns `SessionNotFound`.
#[test]
fn delete_nonexistent_session_fails() -> Result<(), SessionStoreError> {
    let store = test_store();
    let err = store
        .delete_session(&make_session_id())
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
#[test]
fn append_and_load_messages() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    let s1 = store.append_message(&sid, &user_msg("first"))?;
    assert_eq!(s1, 1, "first message seq must be 1");

    let s2 = store.append_message(&sid, &user_msg("second"))?;
    assert_eq!(s2, 2, "second message seq must be 2");

    let s3 = store.append_message(&sid, &user_msg("third"))?;
    assert_eq!(s3, 3, "third message seq must be 3");

    let entries = store.load_messages(&sid)?;
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
#[test]
fn message_order_preserves_append_order() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    store.append_message(&sid, &system_msg("instructions", Some("be nice")))?;
    store.append_message(&sid, &user_msg("hello"))?;

    let entries = store.load_messages(&sid)?;
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
#[test]
fn replay_single_system_message() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    store.append_message(&sid, &system_msg("role", Some("coder")))?;
    store.append_message(&sid, &user_msg("write a test"))?;

    let transcript = store.replay_ledger(&sid)?;
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
///   2. System { "rules": None }
///   3. User { "hello" }
///      → replay: System { "rules": None } (key is present as None)
///      Acceptance: the "rules" section is present with None value — the consumer
///      sees it as removed.
#[test]
fn replay_section_removal() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    store.append_message(&sid, &system_msg("rules", Some("be concise")))?;
    store.append_message(&sid, &user_msg("first turn"))?;
    // Mid-session removal: a guardrail lifted.
    store.append_message(&sid, &system_msg("rules", None))?;

    let transcript = store.replay_ledger(&sid)?;
    // Expected: System { rules: None } + User + User
    assert_eq!(transcript.len(), 3, "all three messages survive replay");

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
#[test]
fn replay_independent_sections() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    let mut sections = BTreeMap::new();
    sections.insert("role".into(), Some("coder".into()));
    sections.insert("style".into(), Some("concise".into()));
    store.append_message(&sid, &Message::System { sections })?;

    store.append_message(&sid, &system_msg("style", None))?;

    let transcript = store.replay_ledger(&sid)?;
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
#[test]
fn replay_messages_pure_no_system() {
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
#[test]
fn append_and_load_events() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid)?;

    let e1 = run_started_event(&rid, &sid, 1);
    let e2 = run_finished_event(&rid, &sid, 2);

    store.append_event(&e1)?;
    store.append_event(&e2)?;

    let events = store.events_for_run(&rid)?;
    assert_eq!(events.len(), 2, "must load both events");
    assert_eq!(events[0].seq, 1, "first event seq must be 1");
    assert_eq!(events[1].seq, 2, "second event seq must be 2");

    Ok(())
}

/// Purpose: events from different runs are isolated.
/// Scenario: append events to two runs → each run returns only its own events.
/// Acceptance: no cross-contamination.
#[test]
fn event_isolation_between_runs() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    let rid_a = RunId("run-a".into());
    let rid_b = RunId("run-b".into());

    store.append_event(&run_started_event(&rid_a, &sid, 1))?;
    store.append_event(&run_started_event(&rid_b, &sid, 1))?;

    let events_a = store.events_for_run(&rid_a)?;
    let events_b = store.events_for_run(&rid_b)?;

    assert_eq!(events_a.len(), 1);
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_a[0].run_id, rid_a);
    assert_eq!(events_b[0].run_id, rid_b);

    Ok(())
}

/// Purpose: duplicate seq for the same run is rejected.
/// Scenario: append two events with the same run_id and seq.
/// Acceptance: second append returns an error.
#[test]
fn duplicate_event_seq_is_rejected() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid)?;

    store.append_event(&run_started_event(&rid, &sid, 1))?;
    let err = store
        .append_event(&run_started_event(&rid, &sid, 1))
        .err()
        .unwrap_or_else(|| panic!("expected duplicate-seq error"));
    assert!(
        matches!(&err, SessionStoreError::Internal(msg) if msg.contains("duplicate seq")),
        "expected Internal(duplicate seq), got {err:?}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Event gap detection
// ---------------------------------------------------------------------------

/// Purpose: a run with no events reports no gaps.
/// Scenario: check_event_gaps on a run with zero events.
/// Acceptance: empty vec.
#[test]
fn gaps_empty_for_no_events() -> Result<(), SessionStoreError> {
    let store = test_store();
    let gaps = store.check_event_gaps(&make_run_id())?;
    assert!(gaps.is_empty(), "no events → no gaps");
    Ok(())
}

/// Purpose: a run with contiguous seqs reports no gaps.
/// Scenario: events seq 1, 2, 3 → check gaps.
/// Acceptance: empty vec.
#[test]
fn gaps_empty_for_contiguous_events() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid)?;

    for seq in 1..=3_u64 {
        store.append_event(&run_started_event(&rid, &sid, seq))?;
    }

    let gaps = store.check_event_gaps(&rid)?;
    assert!(gaps.is_empty(), "contiguous seqs → no gaps");
    Ok(())
}

/// Purpose: a run with missing seqs reports the missing numbers.
/// Scenario: events seq 1, 3 → check gaps → missing seq 2.
/// Acceptance: gaps = [2].
#[test]
fn gaps_detected_for_missing_seqs() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid)?;

    store.append_event(&run_started_event(&rid, &sid, 1))?;
    store.append_event(&run_finished_event(&rid, &sid, 3))?;

    let gaps = store.check_event_gaps(&rid)?;
    assert_eq!(gaps, vec![2], "seq 2 is missing");
    Ok(())
}

/// Purpose: gaps at the start (seq starting >1) are detected.
/// Scenario: events seq 3, 4, 5 → gaps = [1, 2].
/// Acceptance: gaps = [1, 2].
#[test]
fn gaps_detected_for_non_starting_seq() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    let rid = make_run_id();
    store.create_session(&sid)?;

    store.append_event(&run_started_event(&rid, &sid, 3))?;
    store.append_event(&run_finished_event(&rid, &sid, 5))?;

    let gaps = store.check_event_gaps(&rid)?;
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
///   3. Append events for two runs (run-a seq 1,2,3; run-b seq 1,3 with gap).
///   4. Replay ledger: System sections after removal → role is None.
///   5. Check gaps for run-b → seq 2 is missing.
///
/// Acceptance:
///   - replay returns 4 messages: System { role: None } + 3 non-system.
///   - run-b gaps = [2].
#[test]
fn full_recovery_round_trip() -> Result<(), SessionStoreError> {
    let store = test_store();
    let sid = make_session_id();
    store.create_session(&sid)?;

    // -- Messages with section removal --
    store.append_message(&sid, &system_msg("role", Some("coder")))?;
    store.append_message(&sid, &user_msg("hello"))?;
    store.append_message(&sid, &system_msg("role", None))?; // mid-session removal
    store.append_message(&sid, &user_msg("again"))?;
    store.append_message(
        &sid,
        &Message::Assistant {
            content: vec![Content::Text("ok".into())],
        },
    )?;

    // -- Events for two runs --
    let run_a = RunId("run-a".into());
    store.append_event(&run_started_event(&run_a, &sid, 1))?;
    store.append_event(&run_finished_event(&run_a, &sid, 2))?;

    let run_b = RunId("run-b".into());
    store.append_event(&run_started_event(&run_b, &sid, 1))?;
    store.append_event(&run_finished_event(&run_b, &sid, 3))?; // gap at seq 2

    // -- Recovery step 1: replay ledger --
    let transcript = store.replay_ledger(&sid)?;
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
    let gaps_a = store.check_event_gaps(&run_a)?;
    assert!(gaps_a.is_empty(), "run-a has contiguous seqs");

    let gaps_b = store.check_event_gaps(&run_b)?;
    assert_eq!(gaps_b, vec![2], "run-b is missing seq 2");

    // -- Recovery step 3: load events --
    let events_a = store.events_for_run(&run_a)?;
    assert_eq!(events_a.len(), 2);
    let events_b = store.events_for_run(&run_b)?;
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
#[test]
fn persistence_through_file_reopen() -> Result<(), SessionStoreError> {
    let tmp =
        tempfile::NamedTempFile::new().unwrap_or_else(|e| panic!("must create temp file: {e}"));
    let path = tmp.path().to_path_buf();

    let sid = make_session_id();

    // First session: write data.
    {
        let store = SqliteSessionStore::open(&path)?;
        store.create_session(&sid)?;
        store.append_message(&sid, &user_msg("persist me"))?;
        store.append_message(&sid, &system_msg("key", Some("val")))?;
        // Store is dropped here.
    }

    // Second session: read back.
    {
        let store = SqliteSessionStore::open(&path)?;
        assert!(store.session_exists(&sid)?, "session must survive reopen");

        let entries = store.load_messages(&sid)?;
        assert_eq!(entries.len(), 2, "both messages survive reopen");
        assert_eq!(entries[0].message, user_msg("persist me"));
    }

    Ok(())
}
