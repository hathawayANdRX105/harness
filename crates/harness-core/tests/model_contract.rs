//! Model contract behavior: the `Model` trait through a trait object, the
//! stream-fn failure semantics, and incremental tool-call assembly.
//!
//! Acceptance: a scripted implementer behind `Arc<dyn Model>` replays its
//! events verbatim, failures arrive as **terminal `ModelEvent::Error`
//! events** (the stream itself never fails), and a streaming loop rebuilds
//! a complete tool call from `ToolCallStart`/`Args`/`End` deltas alone.
//!
//! The scripted streams are ready-made iterators, so the test needs no
//! runtime: `futures::executor::block_on` drives the one await in the
//! contract (`stream()` itself).

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::executor::block_on;
use harness_core::message::{Message, StopReason};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream, Usage};
use serde_json::json;

struct ScriptedModel {
    info: ModelInfo,
    script: Vec<ModelEvent>,
}

#[async_trait]
impl Model for ScriptedModel {
    fn info(&self) -> &ModelInfo {
        &self.info
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
        messages: vec![Message::user_text("ping")],
        tools: vec![],
        max_tokens: Some(1024),
        temperature: None,
    }
}

fn sample_info() -> ModelInfo {
    ModelInfo {
        handle: "fast".into(),
        provider: "openai".into(),
        model: "gpt-4o-mini".into(),
        context_window: 128_000,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

/// A scripted turn replays verbatim through `Arc<dyn Model>`, including a
/// streaming tool call with partial JSON and usage accounting.
#[test]
fn scripted_turn_replays_through_trait_object() {
    let model: Arc<dyn Model> = Arc::new(ScriptedModel {
        info: sample_info(),
        script: vec![
            ModelEvent::TextDelta("Hello".into()),
            ModelEvent::ToolCallStart {
                id: "call-1".into(),
                name: "read".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call-1".into(),
                partial_json: "{\"path\": ".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call-1".into(),
                partial_json: "\"src/lib.rs\"}".into(),
            },
            ModelEvent::ToolCallEnd {
                id: "call-1".into(),
            },
            ModelEvent::Usage(Usage {
                input_tokens: 10,
                output_tokens: 5,
            }),
            ModelEvent::Stop(StopReason::ToolUse),
        ],
    });
    assert_eq!(model.info().model, "gpt-4o-mini");

    let events = block_on(collect(model.as_ref(), &sample_request()));
    assert_eq!(events.len(), 7);
    assert!(matches!(&events[0], ModelEvent::TextDelta(t) if t == "Hello"));
    assert!(matches!(&events[1], ModelEvent::ToolCallStart { name, .. } if name == "read"));
    assert!(
        matches!(&events[3], ModelEvent::ToolCallArgs { partial_json, .. } if partial_json == "\"src/lib.rs\"}")
    );
    assert!(matches!(&events[6], ModelEvent::Stop(StopReason::ToolUse)));

    // The full call is rebuildable from the deltas alone — the shape a
    // streaming loop assembles against.
    let mut partial = String::new();
    for event in &events {
        if let ModelEvent::ToolCallArgs { partial_json, .. } = event {
            partial.push_str(partial_json);
        }
    }
    assert_eq!(partial, "{\"path\": \"src/lib.rs\"}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&partial).unwrap_or(json!(null)),
        json!({"path": "src/lib.rs"})
    );
}

/// The stream-fn contract: a mid-stream failure is a **terminal event**, so
/// the consumer sees the partial work, then the error, then the stream ends —
/// never an error from the stream itself.
#[test]
fn failures_are_terminal_events_not_stream_errors() {
    let model: Arc<dyn Model> = Arc::new(ScriptedModel {
        info: sample_info(),
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
}

/// A clean stop is terminal too — nothing follows it even when the consumer
/// keeps polling.
#[test]
fn stop_is_terminal() {
    let model = ScriptedModel {
        info: sample_info(),
        script: vec![ModelEvent::Stop(StopReason::EndTurn)],
    };
    let events = block_on(collect(&model, &sample_request()));
    assert_eq!(events.len(), 1, "terminal events are last, by construction");
}

#[test]
fn model_request_round_trips() -> serde_json::Result<()> {
    let req = sample_request();
    let wire = serde_json::to_value(&req)?;
    let back: ModelRequest = serde_json::from_value(wire)?;
    assert_eq!(back, req);
    Ok(())
}
