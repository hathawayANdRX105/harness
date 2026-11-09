//! Message & content model behavior: the transcript vocabulary and the
//! system-prompt ledger.
//!
//! Acceptance: every message kind round-trips through serde with its wire
//! shape pinned (`role`-tagged cards stay intact), tool results carry
//! `is_error` as a first-class field, and replaying a transcript's system
//! messages section-merges with last-write-wins — the property a daemon
//! relies on to resume a session from storage without a second config
//! object.

use std::collections::BTreeMap;

use harness_core::message::{Content, Message, Role, ToolCall, ToolResultMessage};
use serde_json::json;

#[test]
fn every_message_kind_round_trips() -> serde_json::Result<()> {
    let transcript = [
        Message::System {
            sections: BTreeMap::from([
                ("identity".to_string(), "You are a harness.".to_string()),
                ("tools".to_string(), "read, edit".to_string()),
            ]),
        },
        Message::user_text("fix the bug"),
        Message::Assistant {
            content: vec![
                Content::Reasoning("tracing".into()),
                Content::Text("I'll read the file.".into()),
                Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "read".into(),
                    args: json!({"path": "src/lib.rs"}),
                }),
            ],
        },
        Message::ToolResult(ToolResultMessage {
            tool_call_id: "call-1".into(),
            tool_name: "read".into(),
            content: "file contents".into(),
            is_error: false,
        }),
    ];
    let wire = serde_json::to_value(&transcript)?;
    let back: Vec<Message> = serde_json::from_value(wire.clone())?;
    assert_eq!(back, transcript, "round-trip changed the transcript");
    // The ledger's map shape is the frozen contract with adapters and the
    // session store — pin the keys, not just equality.
    assert_eq!(wire[0]["sections"]["identity"], "You are a harness.");
    assert_eq!(wire[3]["tool_call_id"], "call-1");
    Ok(())
}

#[test]
fn failed_tools_are_values_not_missing_answers() -> serde_json::Result<()> {
    let failed = Message::ToolResult(ToolResultMessage {
        tool_call_id: "call-9".into(),
        tool_name: "bash".into(),
        content: "exit 1: permission denied".into(),
        is_error: true,
    });
    let wire = serde_json::to_value(&failed)?;
    assert_eq!(wire["is_error"], true);
    let back: Message = serde_json::from_value(wire)?;
    assert_eq!(back, failed);
    Ok(())
}

#[test]
fn role_of_each_message_kind() {
    assert_eq!(
        Message::System {
            sections: BTreeMap::new()
        }
        .role(),
        Role::System
    );
    assert_eq!(Message::user_text("hi").role(), Role::User);
    assert_eq!(
        Message::Assistant { content: vec![] }.role(),
        Role::Assistant
    );
    // Tool results answer an assistant turn, so they report Assistant —
    // adapters that key on role never see a "tool" role it must invent.
    let result = Message::ToolResult(ToolResultMessage {
        tool_call_id: "c".into(),
        tool_name: "read".into(),
        content: "".into(),
        is_error: false,
    });
    assert_eq!(result.role(), Role::Assistant);
}

/// Replaying a transcript's system messages merges sections: a later
/// `System` message replaces the sections it names and adds the rest. This
/// is the property pi calls the system-message ledger and the one
/// harness-session (#12) depends on for resume.
#[test]
fn system_message_replay_merges_with_last_write_wins() {
    let transcript = [
        Message::System {
            sections: BTreeMap::from([
                ("identity".to_string(), "v1".to_string()),
                ("constraints".to_string(), "first".to_string()),
            ]),
        },
        Message::user_text("hi"),
        Message::System {
            sections: BTreeMap::from([
                ("identity".to_string(), "v2".to_string()),
                ("tools".to_string(), "read".to_string()),
            ]),
        },
    ];
    let merged: BTreeMap<String, String> = transcript
        .iter()
        .filter_map(|m| match m {
            Message::System { sections } => Some(sections),
            _ => None,
        })
        .flatten()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(merged["identity"], "v2");
    assert_eq!(merged["constraints"], "first");
    assert_eq!(merged["tools"], "read");
}
