//! The ordered run event log: serde contract, sequence monotonicity, and
//! the EventSink seam.
//!
//! Acceptance: every event kind round-trips with its `"type"` tag pinned
//! (subscribers parse these strings), `RunEvent` correlates to its run, and
//! the `EventSink` trait is usable from a collecting observer without
//! blocking the emitter.

use parking_lot::Mutex;

use harness_core::event::{EventSink, RunEvent, RunEventKind};
use harness_core::message::ToolCall;
use harness_core::policy::{ApprovalResult, PolicyOutcome, PolicyStage};
use harness_core::run::{RunId, RunStopReason, SessionId};
use serde_json::json;

fn event(seq: u64, kind: RunEventKind) -> RunEvent {
    RunEvent {
        run_id: RunId("r-1".into()),
        session_id: SessionId("s-1".into()),
        seq,
        timestamp_ms: 1_000 + seq as i64,
        kind,
    }
}

#[test]
fn every_kind_round_trips_with_type_tag() -> serde_json::Result<()> {
    let events = vec![
        event(
            1,
            RunEventKind::RunStarted {
                task: "demo".into(),
            },
        ),
        event(
            2,
            RunEventKind::RunFinished {
                reason: RunStopReason::Completed,
            },
        ),
        event(
            3,
            RunEventKind::RunFinished {
                reason: RunStopReason::Cancelled,
            },
        ),
        event(
            4,
            RunEventKind::RunFinished {
                reason: RunStopReason::Error {
                    message: "upstream 502".into(),
                },
            },
        ),
        event(
            5,
            RunEventKind::RunFinished {
                reason: RunStopReason::BudgetBlocked {
                    required_tokens: 4096,
                    available_tokens: 1024,
                },
            },
        ),
        event(
            6,
            RunEventKind::SteeringApplied {
                message: "stop".into(),
            },
        ),
        event(7, RunEventKind::TurnStarted { turn: 1 }),
        event(8, RunEventKind::TurnFinished { turn: 1 }),
        event(9, RunEventKind::TextDelta { text: "hi".into() }),
        event(
            10,
            RunEventKind::ToolCall(ToolCall {
                id: "c1".into(),
                name: "read".into(),
                args: json!({"path": "src/lib.rs"}),
            }),
        ),
        event(
            11,
            RunEventKind::ToolStarted {
                call_id: "c1".into(),
                name: "read".into(),
            },
        ),
        event(
            12,
            RunEventKind::ToolFinished {
                call_id: "c1".into(),
                name: "read".into(),
                is_error: false,
            },
        ),
        event(
            13,
            RunEventKind::PolicyDecided {
                stage: PolicyStage::BeforeTool,
                outcome: PolicyOutcome::Deny,
                reason: "destructive".into(),
            },
        ),
        event(
            14,
            RunEventKind::ApprovalRequested {
                call_id: "c1".into(),
            },
        ),
        event(
            15,
            RunEventKind::ApprovalResolved {
                call_id: "c1".into(),
                result: ApprovalResult::Approved,
            },
        ),
        event(
            16,
            RunEventKind::Error {
                message: "boom".into(),
            },
        ),
    ];
    for event in &events {
        let wire = serde_json::to_value(event)?;
        let back: RunEvent = serde_json::from_value(wire.clone())?;
        assert_eq!(
            back,
            *event,
            "round-trip changed the event: {}",
            serde_json::to_string(&wire).unwrap_or_default()
        );
    }
    // The `"type"` tag is the frozen contract subscribers parse — pin a
    // representative shape per family.
    let wire = serde_json::to_value(&events[0])?;
    assert_eq!(wire["type"], "run_started");
    assert_eq!(wire["run_id"], "r-1");
    assert_eq!(wire["seq"], 1);
    let wire = serde_json::to_value(&events[4])?;
    assert_eq!(wire["type"], "run_finished");
    assert_eq!(wire["reason"]["kind"], "budget_blocked");
    assert_eq!(wire["reason"]["required_tokens"], 4096);
    assert_eq!(wire["reason"]["available_tokens"], 1024);
    let wire = serde_json::to_value(&events[8])?;
    assert_eq!(wire["type"], "text_delta");
    assert_eq!(wire["text"], "hi");
    let wire = serde_json::to_value(&events[1])?;
    assert_eq!(wire["type"], "run_finished");
    assert_eq!(wire["reason"]["kind"], "completed");
    Ok(())
}

/// A sink collects what the emitter pushed; the trait is exactly the
/// "push, never block" surface the module promises.
#[derive(Default)]
struct CollectSink {
    events: Mutex<Vec<RunEvent>>,
}

impl EventSink for CollectSink {
    fn emit(&self, event: RunEvent) {
        self.events.lock().push(event);
    }
}

/// The log's sequence is monotonic per run — a replay consumer detects gaps
/// by checking `seq` increments, so the sink contract keeps the emitter's
/// numbering intact.
#[test]
fn sink_collects_in_order_with_monotonic_seq() {
    let sink = CollectSink::default();
    for seq in 1..=5 {
        sink.emit(event(
            seq,
            RunEventKind::TextDelta {
                text: format!("chunk-{seq}"),
            },
        ));
    }
    let collected = sink.events.lock().clone();
    let seqs: Vec<u64> = collected.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    for collected in &collected {
        assert_eq!(collected.run_id, RunId("r-1".into()));
    }
}
