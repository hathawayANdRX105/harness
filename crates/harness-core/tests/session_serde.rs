//! Session record shapes: role parsing, and serde round-trips of the rows
//! the daemon, the store and the wire share.
//!
//! Acceptance: roles round-trip through their string form, and the
//! `#[serde(default)]` rules on summary/message rows keep older payloads
//! (written before those fields existed) deserializable with their original
//! meaning.

use harness_core::session::{Attachment, RunRecord, SessionMessage, SessionRole, SessionSummary};
use serde_json::json;

#[test]
fn roles_round_trip_through_the_string_form() {
    for role in [
        SessionRole::User,
        SessionRole::Assistant,
        SessionRole::System,
        SessionRole::Tool,
    ] {
        assert_eq!(SessionRole::parse(role.as_str()), Ok(role));
        assert_eq!(role.to_string(), role.as_str());
    }
    // Parse accepts any case; the raw input comes back on miss so the caller
    // maps it into its own error type.
    assert_eq!(SessionRole::parse("ASSISTANT"), Ok(SessionRole::Assistant));
    assert_eq!(SessionRole::parse("narrator"), Err("narrator".to_string()));
}

#[test]
fn summary_round_trips_and_defaults_legacy_payloads() -> serde_json::Result<()> {
    let summary = SessionSummary {
        id: "s-1".into(),
        title: "demo".into(),
        parent_id: Some("s-0".into()),
        cwd: "/repo".into(),
        created_at_ms: 1_700_000_000_000,
        updated_at_ms: 1_700_000_001_000,
        message_count: 42,
    };
    let wire = serde_json::to_value(&summary)?;
    // `parent_id` is emitted explicitly so every summary carries the key.
    assert_eq!(wire["parent_id"], json!("s-0"));

    let back: SessionSummary = serde_json::from_value(wire)?;
    assert_eq!(back, summary);

    // A legacy payload without `parent_id`/`cwd` deserializes as unbound,
    // which is exactly their meaning.
    let legacy: SessionSummary = serde_json::from_value(json!({
        "id": "s-1",
        "title": "demo",
        "created_at_ms": 1,
        "updated_at_ms": 1,
        "message_count": 0,
    }))?;
    assert_eq!(legacy.parent_id, None);
    assert_eq!(legacy.cwd, "");
    Ok(())
}

#[test]
fn run_record_round_trips() -> serde_json::Result<()> {
    let in_flight = RunRecord {
        seq: 3,
        run_id: "r-9".into(),
        session_id: "s-1".into(),
        started_at_ms: 1,
        finished_at_ms: None,
        status: None,
    };
    let wire = serde_json::to_value(&in_flight)?;
    assert!(wire.get("finished_at_ms").is_none());
    let back: RunRecord = serde_json::from_value(wire)?;
    assert_eq!(back, in_flight);

    let done = RunRecord {
        seq: 4,
        run_id: "r-10".into(),
        session_id: "s-1".into(),
        started_at_ms: 1,
        finished_at_ms: Some(99),
        status: Some("completed".into()),
    };
    let wire = serde_json::to_value(&done)?;
    assert_eq!(wire["finished_at_ms"], 99);
    assert_eq!(wire["status"], "completed");
    let back: RunRecord = serde_json::from_value(wire)?;
    assert_eq!(back, done);
    Ok(())
}

#[test]
fn message_round_trips_with_legacy_attachments_and_cards() -> serde_json::Result<()> {
    let rich = SessionMessage {
        session_id: "s-1".into(),
        seq: 1,
        role: SessionRole::User,
        text: "see screenshot".into(),
        created_at_ms: 5,
        attachments: vec![Attachment {
            name: "shot.png".into(),
            media_type: "image/png".into(),
            data: "aGVsbG8=".into(),
        }],
        tool_calls: vec![json!({"id": "call-1"})],
    };
    let wire = serde_json::to_value(&rich)?;
    assert_eq!(wire["attachments"][0]["media_type"], "image/png");
    let back: SessionMessage = serde_json::from_value(wire)?;
    assert_eq!(back, rich);

    // Plain rows written before attachments/tool_calls existed.
    let plain: SessionMessage = serde_json::from_value(json!({
        "session_id": "s-1",
        "seq": 1,
        "role": "assistant",
        "text": "ok",
        "created_at_ms": 5,
    }))?;
    assert!(plain.attachments.is_empty());
    assert!(plain.tool_calls.is_empty());
    Ok(())
}
