//! `AgentEvent` is the frozen cross-crate event contract.
//!
//! Acceptance: every variant survives a JSON round-trip unchanged, and the
//! wire shape (internally tagged `"type"`, snake_case) stays pinned — the
//! daemon's subscribers and the wire protocol parse these strings, so a
//! variant rename or tag change is a breaking change to them.

use harness_core::event::{AgentEvent, ToolCallSpec, TurnStop};
use serde_json::{Value, json};

fn round_trip(event: &AgentEvent) -> Result<Value, serde_json::Error> {
    let wire = serde_json::to_value(event)?;
    let back: AgentEvent = serde_json::from_value(wire.clone())?;
    assert_eq!(&back, event, "round-trip changed the event: {wire}");
    Ok(wire)
}

#[test]
fn every_variant_round_trips() -> Result<(), serde_json::Error> {
    let events = [
        AgentEvent::TurnStart,
        AgentEvent::AssistantText {
            delta: "你好 🌍".into(),
        },
        AgentEvent::AssistantReasoning {
            delta: "thinking".into(),
        },
        AgentEvent::ToolCall(ToolCallSpec {
            id: "call-1".into(),
            name: "read".into(),
            args: json!({"path": "src/lib.rs"}),
        }),
        AgentEvent::ToolStart {
            id: "call-1".into(),
            name: "read".into(),
        },
        AgentEvent::ToolResult {
            id: "call-1".into(),
            name: "read".into(),
            result: "file contents".into(),
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::EndTurn,
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::MaxTokens,
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::Aborted,
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::Error,
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::MaxTurns,
        },
        AgentEvent::TurnEnd {
            stop_reason: TurnStop::BudgetBlocked {
                required_chars: 4096,
                available_chars: 1024,
            },
        },
    ];
    for event in &events {
        let _ = round_trip(event)?;
    }
    Ok(())
}

#[test]
fn wire_shape_is_pinned() -> Result<(), serde_json::Error> {
    let start = round_trip(&AgentEvent::TurnStart)?;
    assert_eq!(start, json!({"type": "turn_start"}));

    let text = round_trip(&AgentEvent::AssistantText { delta: "hi".into() })?;
    assert_eq!(text, json!({"type": "assistant_text", "delta": "hi"}));

    let call = round_trip(&AgentEvent::ToolCall(ToolCallSpec {
        id: "call-1".into(),
        name: "read".into(),
        args: json!({"path": "src/lib.rs"}),
    }))?;
    assert_eq!(
        call,
        json!({
            "type": "tool_call",
            "id": "call-1",
            "name": "read",
            "args": {"path": "src/lib.rs"},
        })
    );

    let blocked = round_trip(&AgentEvent::TurnEnd {
        stop_reason: TurnStop::BudgetBlocked {
            required_chars: 4096,
            available_chars: 1024,
        },
    })?;
    assert_eq!(
        blocked,
        json!({
            "type": "turn_end",
            "stop_reason": {
                "budget_blocked": {
                    "required_chars": 4096,
                    "available_chars": 1024,
                }
            }
        })
    );
    Ok(())
}
