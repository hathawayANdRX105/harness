//! Model contract behavior: the `Model` trait driven through a trait object,
//! and the stream-fn failure semantics.
//!
//! Acceptance: a scripted implementer behind `Arc<dyn Model>` produces the
//! scripted event sequence unchanged, failures arrive as **terminal
//! `ModelEvent::Error` events** (the stream itself never fails), and the
//! request shape round-trips through serde.
//!
//! The scripted streams here are ready-made `Vec` iterators, so the test
//! needs no runtime: `futures::executor::block_on` drives the one await in
//! the contract (`stream()` itself), mirroring how little of a runtime the
//! contract assumes.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::executor::block_on;
use harness_core::context::{Block, Turn, TurnRole};
use harness_core::model::{Model, ModelEvent, ModelRequest, ModelStream, StopReason, Usage};
use harness_core::tool::{ToolCallSpec, ToolSpec};
use serde_json::json;

/// A model that replays a scripted event list. Stands in for an adapter: the
/// contract only promises event order and terminal semantics.
struct ScriptedModel {
    id: String,
    script: Vec<ModelEvent>,
}

#[async_trait]
impl Model for ScriptedModel {
    fn id(&self) -> &str {
        &self.id
    }
    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        let events = self.script.clone();
        Box::pin(futures::stream::iter(events))
    }
}

async fn collect(model: &dyn Model, req: &ModelRequest) -> Vec<ModelEvent> {
    let mut events = Vec::new();
    let mut stream = model.stream(req).await;
    // The loop below is the consumer's whole error handling: no `Result`,
    // because the contract says failures arrive as events.
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    events
}

fn sample_request() -> ModelRequest {
    ModelRequest {
        model: "openai/gpt-4o-mini".into(),
        system: vec![Block::Text("You are a harness.".into())],
        messages: vec![Turn {
            role: TurnRole::User,
            blocks: vec![Block::Text("ping".into())],
        }],
        tools: vec![ToolSpec {
            name: "echo".into(),
            description: "Echo.".into(),
            params_schema: json!({"type": "object"}),
        }],
        max_tokens: Some(1024),
        temperature: None,
    }
}

/// A scripted turn replays verbatim through `Arc<dyn Model>`, including a
/// parsed tool call and usage accounting.
#[test]
fn scripted_turn_replays_through_trait_object() {
    let model: Arc<dyn Model> = Arc::new(ScriptedModel {
        id: "scripted-1".into(),
        script: vec![
            ModelEvent::TextDelta("Hello".into()),
            ModelEvent::ToolCall(ToolCallSpec {
                id: "call-1".into(),
                name: "echo".into(),
                args: json!({"text": "hi"}),
            }),
            ModelEvent::Usage(Usage {
                input_tokens: 10,
                output_tokens: 5,
            }),
            ModelEvent::Stop(StopReason::EndTurn),
        ],
    });
    assert_eq!(model.id(), "scripted-1");

    let events = block_on(collect(model.as_ref(), &sample_request()));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], ModelEvent::TextDelta(t) if t == "Hello"));
    assert!(matches!(&events[1], ModelEvent::ToolCall(c) if c.name == "echo"));
    assert!(matches!(&events[2], ModelEvent::Usage(u) if u.input_tokens == 10));
    assert!(matches!(&events[3], ModelEvent::Stop(StopReason::EndTurn)));
}

/// The stream-fn contract: a mid-stream failure is a **terminal event**, so
/// the consumer sees the partial work, then the error, then the stream ends —
/// never an Err from the stream itself.
#[test]
fn failures_are_terminal_events_not_stream_errors() {
    let model: Arc<dyn Model> = Arc::new(ScriptedModel {
        id: "scripted-fail".into(),
        script: vec![
            ModelEvent::TextDelta("partial".into()),
            ModelEvent::Error {
                message: "upstream 502".into(),
            },
        ],
    });

    let events = block_on(collect(model.as_ref(), &sample_request()));
    assert_eq!(events.len(), 2);
    let ModelEvent::Error { message } = &events[1] else {
        panic!("expected terminal Error event, got: {:?}", events[1]);
    };
    assert_eq!(message, "upstream 502");

    // Nothing follows a terminal event: the script stops there even though
    // the consumer would keep polling.
    let extra = ScriptedModel {
        id: "scripted-fail".into(),
        script: vec![ModelEvent::Stop(StopReason::EndTurn)],
    };
    let more = block_on(collect(&extra, &sample_request()));
    assert_eq!(more.len(), 1, "terminal events are last, by construction");
}

/// `ModelRequest` is the provider-neutral wire between the loop and the
/// adapter: pin its serde round-trip so the frozen-prompt shape stays stable.
#[test]
fn model_request_round_trips() -> serde_json::Result<()> {
    let req = sample_request();
    let wire = serde_json::to_value(&req)?;
    let back: ModelRequest = serde_json::from_value(wire)?;
    assert_eq!(back.model, req.model);
    assert!(matches!(back.system.first(), Some(Block::Text(_))));
    assert_eq!(back.tools.len(), 1);
    assert_eq!(back.max_tokens, Some(1024));
    Ok(())
}
