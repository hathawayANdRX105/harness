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

/// Purpose: the transcript vocabulary survives serialization. Scenario: one
/// message of each kind, with reasoning + tool call blocks. Acceptance:
/// round-trip equals the input and the ledger/tool-card wire keys are pinned.
#[test]
fn every_message_kind_round_trips() -> serde_json::Result<()> {
    let transcript = [
        Message::System {
            sections: BTreeMap::from([
                (
                    "identity".to_string(),
                    Some("You are a harness.".to_string()),
                ),
                ("tools".to_string(), Some("read, edit".to_string())),
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

/// Purpose: a tool failure is a value on the wire. Scenario: a ToolResult
/// message with is_error. Acceptance: the field round-trips as `true` —
/// consumers never see a failed tool as a missing answer.
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

/// Purpose: the role view adapters key on. Scenario: every message kind.
/// Acceptance: tool results report `Assistant` (they answer an assistant
/// turn), so no "tool" role leaks into role-keyed renderers.
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

/// Replaying a transcript's system messages reconstructs the prompt: a
/// `Some` section replaces, a `None` removes (pi's `null`), and untouched
/// sections survive. This is the property pi calls the system-message
/// ledger and the one harness-session (#12) depends on for resume.
#[test]
fn system_message_replay_replaces_and_removes() {
    let transcript = [
        Message::System {
            sections: BTreeMap::from([
                ("identity".to_string(), Some("v1".to_string())),
                ("constraints".to_string(), Some("first".to_string())),
            ]),
        },
        Message::user_text("hi"),
        Message::System {
            sections: BTreeMap::from([
                ("identity".to_string(), Some("v2".to_string())),
                ("tools".to_string(), Some("read".to_string())),
            ]),
        },
        Message::System {
            sections: BTreeMap::<String, Option<String>>::from([("constraints".to_string(), None)]),
        },
    ];
    // Replay: apply the ledger entries in transcript order — Some replaces,
    // None removes — and the surviving map is the prompt the model would see.
    let mut prompt: BTreeMap<String, String> = BTreeMap::new();
    for (name, value) in replayed_sections(&transcript) {
        match value {
            Some(text) => {
                prompt.insert(name.clone(), text.clone());
            }
            None => {
                prompt.remove(name);
            }
        }
    }
    assert_eq!(prompt["identity"], "v2");
    assert!(
        !prompt.contains_key("constraints"),
        "a None section removes it"
    );
    assert_eq!(prompt["tools"], "read");
}

fn replayed_sections(transcript: &[Message]) -> Vec<(&String, &Option<String>)> {
    transcript
        .iter()
        .filter_map(|message| match message {
            Message::System { sections } => Some(sections),
            _ => None,
        })
        .flatten()
        .collect()
}

/// A `None` section survives the wire as JSON `null` — the removal travels
/// with the transcript, which is what makes a persisted session replayable.
#[test]
fn none_section_round_trips_as_json_null() -> serde_json::Result<()> {
    let message = Message::System {
        sections: BTreeMap::from([("constraints".into(), None)]),
    };
    let wire = serde_json::to_value(&message)?;
    assert_eq!(wire["role"], "system");
    assert_eq!(wire["sections"]["constraints"], serde_json::Value::Null);
    let back: Message = serde_json::from_value(wire)?;
    assert_eq!(back, message);
    Ok(())
}
