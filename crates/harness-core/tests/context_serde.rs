//! Assembled-prompt shapes: block variety, turn round-trip, budget ratio
//! guard and the `Context` default.
//!
//! Acceptance: every block kind round-trips through serde, a zero budget
//! window reports ratio 0.0 instead of dividing by zero, and `Context::default`
//! is a usable empty prompt the builder fills in (no field assembly required
//! to construct one).

use harness_core::context::{Block, Budget, Context, Task, Turn, TurnRole};
use serde_json::{Value, json};

#[test]
fn every_block_kind_round_trips() -> serde_json::Result<()> {
    let blocks = vec![
        Block::Text("system preamble".into()),
        Block::FileRef {
            path: "docs/arch.md".into(),
            hash: Some("sha-abc".into()),
            excerpt: Some("first lines".into()),
        },
        Block::Skill {
            name: "rust-engineer".into(),
            body: "## Instructions\nWrite idiomatic Rust.".into(),
        },
        Block::ToolCall {
            call_id: "call-1".into(),
            name: "echo".into(),
            args: json!({"text": "hi"}),
        },
        Block::ToolResult {
            call_id: "call-1".into(),
            content: json!({"output": "hi"}),
        },
        Block::Reasoning("deep".into()),
    ];
    let wire = serde_json::to_value(&blocks)?;
    let back: Vec<Block> = serde_json::from_value(wire)?;
    assert_eq!(back, blocks);
    Ok(())
}

#[test]
fn turn_round_trips() -> serde_json::Result<()> {
    let turn = Turn {
        role: TurnRole::User,
        blocks: vec![Block::Text("hello".into()), Block::Reasoning("why".into())],
    };
    let wire = serde_json::to_value(&turn)?;
    assert_eq!(wire["role"], "user");
    let back: Turn = serde_json::from_value(wire)?;
    assert_eq!(back, turn);

    // Role form is the lowercase wire string for every variant.
    for (role, name) in [
        (TurnRole::User, "user"),
        (TurnRole::Assistant, "assistant"),
        (TurnRole::System, "system"),
        (TurnRole::Tool, "tool"),
    ] {
        let t = Turn {
            role,
            blocks: vec![],
        };
        let wire = serde_json::to_value(&t)?;
        assert_eq!(wire["role"], name);
    }
    Ok(())
}

#[test]
fn budget_ratio_guard() {
    let none = Budget { used: 5, window: 0 };
    assert_eq!(none.ratio(), 0.0);

    let half = Budget {
        used: 50,
        window: 200,
    };
    assert_eq!(half.ratio(), 0.25);
}

#[test]
fn context_default_is_an_empty_prompt() -> serde_json::Result<()> {
    let ctx = Context::default();
    assert!(ctx.system.is_empty());
    assert!(ctx.guides.is_empty());
    assert!(ctx.history.is_empty());
    assert_eq!(ctx.task.description, "");
    assert_eq!(ctx.metadata, json!(Value::Null));

    // A filled task survives a round trip — the description is what
    // compaction keeps so a restarted context still knows what it was asked.
    let task = Task {
        description: "port harness-core".into(),
        source: Some("issue/2".into()),
        deadline: None,
    };
    let wire = serde_json::to_value(&task)?;
    let back: Task = serde_json::from_value(wire)?;
    assert_eq!(back, task);
    Ok(())
}
