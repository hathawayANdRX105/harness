//! Pure-domain tests for the unit ledger: message ↔ unit split/assemble and
//! the integrity hash. No storage engine involved — these run against the
//! port types only.

use harness_core::message::{Content, Message, ToolCall};
use harness_core::run::SessionId;
use harness_session::store::replay_messages;
use harness_session::unit::MessageEntry;
use harness_session::unit::{
    INITIAL_EPOCH, MAIN_BRANCH, UnitKind, assemble_messages, integrity_hash, split_message,
};

/// A minimal tool call for reassembly assertions.
fn tool_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        name: "read".into(),
        args: serde_json::json!({"path": "/tmp/x"}),
    }
}

/// The ledger position one fresh append starts from.
fn fresh_head(session: &SessionId) -> harness_session::HeadState {
    harness_session::HeadState {
        session_id: session.clone(),
        branch_id: MAIN_BRANCH.to_string(),
        epoch: INITIAL_EPOCH,
        head_event_id: None,
        last_event_order: 0,
        last_message_seq: 0,
    }
}

/// Purpose: one message splits into the documented unit kinds.
/// Scenario: an assistant message with a text block and a tool call.
/// Acceptance: two units — an assistant unit and a tool_call unit carrying
/// the call id — chained by parent edges, with the message sequence shared.
#[test]
fn assistant_message_splits_into_blocks() -> Result<(), harness_session::SessionStoreError> {
    let session = SessionId("split".into());
    let message = Message::Assistant {
        content: vec![
            Content::Text("thinking".into()),
            Content::ToolCall(tool_call("c1")),
        ],
    };
    let units = split_message(&fresh_head(&session), Some("run-1"), None, &message, 1)?;

    assert_eq!(units.len(), 2, "text block + tool call");
    assert_eq!(units[0].0.kind, UnitKind::Assistant);
    assert_eq!(units[1].0.kind, UnitKind::ToolCall);
    assert_eq!(units[1].0.tool_call_id.as_deref(), Some("c1"));
    assert_eq!(
        units[1].0.parent_event_id.as_deref(),
        Some(units[0].0.event_id.as_str()),
        "blocks chain via parent edges"
    );
    assert_eq!(units[0].0.message_seq, units[1].0.message_seq);
    assert_eq!(units[0].0.source_kind, "model");
    Ok(())
}

/// Purpose: assembled units rebuild the original message.
/// Scenario: split the message, feed the stored rows to `assemble_messages`.
/// Acceptance: the assistant content comes back with block order intact.
#[test]
fn split_then_assemble_round_trips() -> Result<(), harness_session::SessionStoreError> {
    let session = SessionId("roundtrip".into());
    let message = Message::Assistant {
        content: vec![
            Content::ToolCall(tool_call("c1")),
            Content::Text("after".into()),
        ],
    };
    let units = split_message(&fresh_head(&session), None, None, &message, 1)?;
    let stored: Vec<harness_session::StoredUnit> = units
        .into_iter()
        .map(|(unit, payload)| harness_session::StoredUnit {
            unit,
            payload_json: payload,
        })
        .collect();

    let entries = assemble_messages(&stored)?;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].message,
        Message::Assistant {
            content: vec![
                Content::ToolCall(tool_call("c1")),
                Content::Text("after".into())
            ],
        },
        "block order survives the ledger round trip"
    );
    Ok(())
}

/// Purpose: steering units assemble back into user turns.
/// Scenario: a steering-kind unit written through the override path.
/// Acceptance: `assemble_messages` yields the original user message, and the
/// unit's kind is `Steering` with a user source family.
#[test]
fn steering_units_assemble_as_user_turns() -> Result<(), harness_session::SessionStoreError> {
    let session = SessionId("steering".into());
    let message = Message::user_text("steer: stop");
    let units = split_message(
        &fresh_head(&session),
        None,
        Some(UnitKind::Steering),
        &message,
        1,
    )?;
    assert_eq!(units.len(), 1, "steering is one whole-message unit");
    assert_eq!(units[0].0.kind, UnitKind::Steering);
    assert_eq!(units[0].0.source_kind, "user");

    let stored: Vec<harness_session::StoredUnit> = units
        .into_iter()
        .map(|(unit, payload)| harness_session::StoredUnit {
            unit,
            payload_json: payload,
        })
        .collect();
    let entries = assemble_messages(&stored)?;
    assert_eq!(entries[0].message, message);
    Ok(())
}

/// Purpose: the integrity hash is deterministic over the covered fields and
/// changes when content changes.
/// Scenario: hash one unit twice, then flip the payload.
/// Acceptance: identical inputs hash identically; a payload flip changes the
/// hash. (The parent edge is excluded by design — ferrite's P1 limitation.)
#[test]
fn integrity_hash_is_deterministic_and_content_sensitive()
-> Result<(), harness_session::SessionStoreError> {
    let session = SessionId("hash".into());
    let message = Message::user_text("hash me");
    let (unit, payload) = split_message(&fresh_head(&session), Some("run-1"), None, &message, 1)?
        .pop()
        .ok_or_else(|| {
            harness_session::SessionStoreError::Internal("a user message yields one unit".into())
        })?;

    let again = integrity_hash(&unit, &payload)?;
    assert_eq!(unit.integrity_hash, again, "hashing is deterministic");

    let flipped = integrity_hash(&unit, "{\"tampered\":true}")?;
    assert_ne!(
        unit.integrity_hash, flipped,
        "content changes move the hash"
    );
    Ok(())
}

/// Purpose: replay_messages passes non-system messages through untouched.
/// Scenario: two entries, no system message.
/// Acceptance: the transcript is unchanged — no front-matter System message
/// is invented.
#[test]
fn replay_messages_passes_plain_entries_through() {
    let entries = vec![
        MessageEntry {
            seq: 1,
            message: Message::user_text("hi"),
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
