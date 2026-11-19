//! Turn-boundary budget gate: the loop consults the context budget before
//! every provider call, runs compaction once when a stage is active, and
//! refuses the round with `BudgetBlocked` semantics when the next turn still
//! cannot fit -- all without touching the frozen
//! `harness_core::run::RunStopReason`.
//!
//! | Test | Scenario | Acceptance |
//! |---|---|---|
//! | `budget_exhausted_from_start_refuses_before_provider` | Budget exhausted before turn 0 | Stop is `Error`; the message round-trips through
//!   `BudgetBlocked::from_error_message`; one `RunEventKind::Error` is emitted;
//!   compaction runs once; no `TurnStarted` ever fires.
//! | `compaction_frees_room_then_turn_proceeds` | Oversized task, compactor can shrink | `compact` runs exactly once; the turn proceeds to
//!   `Completed`; no error event is emitted.
//!
//! All tests use mock `Model` and `Compactor` implementations -- no network,
//! no real model provider.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use harness_context::{
    Budget, BudgetBlocked, CompactionCheckpoint, CompactionResult, CompactionStage, Compactor,
    DefaultCompactor,
};
use harness_core::event::{EventSink, RunEvent, RunEventKind};
use harness_core::message::{Content, Message};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::topology::Topology;
use harness_core::{RunControl, StopReason};
use harness_loop::{LoopDeps, LoopHooks, ToolExecutionMode, run};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

/// A mock model returning pre-recorded turn sequences, FIFO.
struct MockModel {
    info: ModelInfo,
    turns: Mutex<VecDeque<Vec<ModelEvent>>>,
    /// Every request the model saw, in call order.
    requests: Mutex<Vec<ModelRequest>>,
}

impl MockModel {
    fn new(info: ModelInfo, turns: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            info,
            turns: Mutex::new(turns.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|e| panic!("model lock poisoned: {e}"))
            .clone()
    }
}

#[async_trait]
impl Model for MockModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_> {
        self.requests
            .lock()
            .unwrap_or_else(|e| panic!("model lock poisoned: {e}"))
            .push(req.clone());
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|e| panic!("mock model lock poisoned: {e}"))
            .pop_front()
            .unwrap_or_else(|| panic!("mock model turn queue exhausted"));
        Box::pin(futures::stream::iter(events))
    }
}

/// Collects emitted events for inspection.
#[derive(Default)]
struct TestSink {
    events: Mutex<Vec<RunEvent>>,
}

impl EventSink for TestSink {
    fn emit(&self, event: RunEvent) {
        self.events
            .lock()
            .unwrap_or_else(|e| panic!("test sink lock poisoned: {e}"))
            .push(event);
    }
}

impl TestSink {
    fn events(&self) -> Vec<RunEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| panic!("test sink lock poisoned: {e}"))
            .clone()
    }
}

/// A policy that allows everything.
struct AllowAllPolicy;

impl ToolPolicy for AllowAllPolicy {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &harness_core::tool::ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::Allow,
            reason: "test policy allows all".into(),
            source: "test".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// Reports the budget as exhausted on every read and records `compact` calls.
/// It never shrinks the transcript, so compaction cannot relieve the
/// pressure and the gate must refuse the round.
struct ExhaustedCompactor {
    stages: Vec<CompactionStage>,
    compactions: AtomicU64,
}

impl ExhaustedCompactor {
    fn new() -> Self {
        Self {
            stages: vec![CompactionStage {
                threshold: 0.50,
                target_ratio: 0.25,
                label: "soft".into(),
            }],
            compactions: AtomicU64::new(0),
        }
    }

    fn compactions(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Compactor for ExhaustedCompactor {
    fn read_budget(&self, _messages: &[Message], _info: &ModelInfo) -> Budget {
        Budget {
            used_tokens: 4096,
            available_tokens: 0,
        }
    }

    async fn compact(
        &self,
        _messages: &mut Vec<Message>,
        _budget: &Budget,
        _checkpoint: Option<&CompactionCheckpoint>,
        _cancel: &CancellationToken,
    ) -> CompactionResult {
        self.compactions.fetch_add(1, Ordering::Relaxed);
        CompactionResult {
            before: 4096,
            after: 4096,
            checkpoint: CompactionCheckpoint::fresh(),
            completed: true,
        }
    }

    fn stages(&self) -> &[CompactionStage] {
        &self.stages
    }
}

/// Counts tokens as `chars / 4` of the transcript and, on `compact`,
/// replaces it with a short summary, dropping every stage below its
/// threshold.
struct ShrinkingCompactor {
    stages: Vec<CompactionStage>,
    available_tokens: u64,
    compactions: AtomicU64,
    snapshots: Mutex<Vec<Vec<Message>>>,
}

impl ShrinkingCompactor {
    fn new(available_tokens: u64) -> Self {
        Self {
            stages: vec![CompactionStage {
                threshold: 0.50,
                target_ratio: 0.25,
                label: "soft".into(),
            }],
            available_tokens,
            compactions: AtomicU64::new(0),
            snapshots: Mutex::new(Vec::new()),
        }
    }

    /// Rough token estimate: `chars / 4`.
    fn estimate_tokens(messages: &[Message]) -> u64 {
        messages
            .iter()
            .map(|m| match m {
                Message::User { content } | Message::Assistant { content } => content
                    .iter()
                    .filter_map(|c| match c {
                        Content::Text(t) | Content::Reasoning(t) => Some(t.len()),
                        _ => None,
                    })
                    .sum::<usize>(),
                Message::ToolResult(tr) => tr.content.len(),
                _ => 0,
            })
            .sum::<usize>() as u64
            / 4
    }

    fn compactions(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Compactor for ShrinkingCompactor {
    fn read_budget(&self, messages: &[Message], _info: &ModelInfo) -> Budget {
        Budget {
            used_tokens: Self::estimate_tokens(messages),
            available_tokens: self.available_tokens,
        }
    }

    async fn compact(
        &self,
        messages: &mut Vec<Message>,
        _budget: &Budget,
        _checkpoint: Option<&CompactionCheckpoint>,
        _cancel: &CancellationToken,
    ) -> CompactionResult {
        let before = Self::estimate_tokens(messages);
        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.snapshots
            .lock()
            .unwrap_or_else(|e| panic!("snapshot lock poisoned: {e}"))
            .push(messages.clone());
        messages.clear();
        messages.push(Message::user_text("summarized prior context"));
        CompactionResult {
            before,
            after: Self::estimate_tokens(messages),
            checkpoint: CompactionCheckpoint::fresh(),
            completed: true,
        }
    }

    fn stages(&self) -> &[CompactionStage] {
        &self.stages
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_ctx(max_turns: u32) -> RunContext {
    RunContext {
        run_id: RunId("test-run".into()),
        session_id: SessionId("test-session".into()),
        task: Task {
            description: "test task".into(),
            source: None,
            deadline: None,
        },
        limits: RunLimits {
            max_turns,
            ..RunLimits::default()
        },
        started_at_ms: 0,
    }
}

fn make_model_info() -> ModelInfo {
    ModelInfo {
        handle: "test-model".into(),
        provider: "test".into(),
        model: "test-model".into(),
        context_window: 4096,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Budget exhausted from the start: the round is refused before the provider call.
///
/// Scenario:
/// 1. `LoopDeps` carries a budget and an `ExhaustedCompactor` whose
///    `read_budget` always reports `used=4096 / available=0`, so the single
///    compaction stage is active before turn 0.
/// 2. The gate refreshes the budget, runs compaction once (recorded), and
///    re-checks: still exhausted, so it terminates.
///
/// Acceptance:
/// - stop is `RunStopReason::Error` whose message round-trips through
///   `BudgetBlocked::from_error_message` with `needed=4096`, `available=0`,
///   `stage="soft"` -- the budget-blocked distinction survives the frozen
///   core enum;
/// - exactly one `RunEventKind::Error` event carries that same message;
/// - `compact` was invoked once (compaction was attempted before giving up);
/// - no `TurnStarted` event: the provider was never called.
#[tokio::test]
async fn budget_exhausted_from_start_refuses_before_provider() -> Result<(), String> {
    let compactor = Arc::new(ExhaustedCompactor::new());
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("never reached".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        prompt_topology: Topology::default(),
        tool_mode: ToolExecutionMode::Sequential,
        budget: Some(Budget {
            used_tokens: 0,
            available_tokens: 0,
        }),
        compactor: Some(Arc::clone(&compactor) as Arc<dyn Compactor>),
        seed: Vec::new(),
        transcript_sink: None,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    let RunStopReason::Error { message } = reason else {
        return Err(format!("expected Error stop reason, got {reason:?}"));
    };

    let recovered = BudgetBlocked::from_error_message(&message)
        .unwrap_or_else(|| panic!("{message:?} should parse as BudgetBlocked"));
    assert_eq!(recovered.needed, 4096, "needed tokens must round-trip");
    assert_eq!(recovered.available, 0, "available tokens must round-trip");
    assert_eq!(recovered.stage, "soft", "stage label must round-trip");

    let events = sink.events();
    let error_events: Vec<&RunEvent> = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::Error { .. }))
        .collect();
    assert_eq!(
        error_events.len(),
        1,
        "exactly one budget-blocked error event"
    );
    if let RunEventKind::Error {
        message: event_message,
    } = &error_events[0].kind
    {
        assert_eq!(
            event_message, &message,
            "error event carries the structured message"
        );
    } else {
        panic!("expected Error event");
    }

    assert_eq!(
        compactor.compactions(),
        1,
        "compaction should run once before the run is refused"
    );

    let turn_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::TurnStarted { .. }))
        .count();
    assert_eq!(
        turn_started, 0,
        "no provider call before the budget refusal"
    );
    Ok(())
}

/// Compaction frees enough room: `compact` runs once and the turn proceeds.
///
/// Scenario:
/// 1. `LoopDeps` carries a budget and a `ShrinkingCompactor` counting
///    `chars / 4` against `available_tokens = 256`. The task seeds a 1200-char
///    user message (~300 tokens), so the `soft` stage (0.50) is active.
/// 2. The gate runs compaction once, which shrinks the transcript to a short
///    summary (~7 tokens); the re-check passes.
///
/// Acceptance:
/// - stop is `RunStopReason::Completed`;
/// - `compact` ran exactly once and saw the oversized transcript;
/// - exactly one `TurnStarted` fired and no `Error` event was emitted.
#[tokio::test]
async fn compaction_frees_room_then_turn_proceeds() -> Result<(), String> {
    let compactor = Arc::new(ShrinkingCompactor::new(256));
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("understood".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let mut ctx = make_ctx(10);
    ctx.task.description = "A".repeat(1200);
    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        prompt_topology: Topology::default(),
        tool_mode: ToolExecutionMode::Sequential,
        budget: Some(Budget {
            used_tokens: 0,
            available_tokens: 256,
        }),
        compactor: Some(Arc::clone(&compactor) as Arc<dyn Compactor>),
        seed: Vec::new(),
        transcript_sink: None,
    });

    let control = RunControl::new();
    let reason = run(ctx, deps, control).await;

    assert_eq!(
        reason,
        RunStopReason::Completed,
        "loop should complete once compaction frees room"
    );

    assert_eq!(
        compactor.compactions(),
        1,
        "compaction should run exactly once at the turn boundary"
    );

    let snapshots = compactor
        .snapshots
        .lock()
        .unwrap_or_else(|e| panic!("snapshot lock poisoned: {e}"));
    assert!(
        !snapshots.is_empty(),
        "compaction should have been attempted"
    );
    let total_chars: usize = snapshots[0]
        .iter()
        .map(|m| match m {
            Message::User { content } | Message::Assistant { content } => content
                .iter()
                .filter_map(|c| match c {
                    Content::Text(t) | Content::Reasoning(t) => Some(t.len()),
                    _ => None,
                })
                .sum::<usize>(),
            _ => 0,
        })
        .sum();
    assert!(
        total_chars > 800,
        "compaction should see the oversized transcript"
    );
    drop(snapshots);

    let events = sink.events();
    let turn_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::TurnStarted { .. }))
        .count();
    assert_eq!(
        turn_started, 1,
        "the model turn should proceed after compaction"
    );
    let error_events: Vec<&RunEvent> = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::Error { .. }))
        .collect();
    assert_eq!(
        error_events.len(),
        0,
        "no budget-blocked error when compaction relieves pressure"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Real DefaultCompactor through the gate
// ---------------------------------------------------------------------------

/// Purpose: the shipped structural compactor (not a mock) makes an oversized
/// history fit through the budget gate, so the turn proceeds instead of
/// being refused.
/// Scenario: a seed ledger of six oversized tool results (24k chars ≈ 7.2k
/// tokens by the 0.30 chars/token estimate) against a 4k-token window — every
/// ladder stage is active on the first boundary.
/// Acceptance: exactly one model call happens; the request the model sees
/// carries the compaction digest and no oversized tool output; the run
/// completes.
#[tokio::test]
async fn real_default_compactor_shrinks_oversized_history() {
    /// The oversized seed: one task, then assistant/tool-result pairs whose
    /// outputs dwarf the 4k-token window (24k chars ≈ 7.2k tokens).
    fn seed_oversized_history() -> Vec<Message> {
        let mut messages = vec![Message::user_text("collect twelve markers")];
        for index in 0..6 {
            messages.push(Message::Assistant {
                content: vec![
                    Content::Text(format!("calling tool {index}")),
                    Content::ToolCall(harness_core::ToolCall {
                        id: format!("call-{index}"),
                        name: "grep".into(),
                        args: serde_json::json!({}),
                    }),
                ],
            });
            messages.push(Message::ToolResult(
                harness_core::message::ToolResultMessage {
                    tool_call_id: format!("call-{index}"),
                    tool_name: "grep".into(),
                    content: "y".repeat(2_000),
                    is_error: false,
                },
            ));
        }
        messages
    }

    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("done".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));
    let compactor = Arc::new(harness_context::DefaultCompactor::new());
    let deps = Arc::new(LoopDeps {
        model: Arc::clone(&model) as Arc<dyn Model>,
        tools: Vec::new(),
        event_sink: Arc::new(TestSink::default()),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        prompt_topology: Topology::default(),
        tool_mode: ToolExecutionMode::Sequential,
        budget: None,
        compactor: Some(Arc::clone(&compactor) as Arc<dyn Compactor>),
        seed: seed_oversized_history(),
        transcript_sink: None,
    });

    let reason = run(make_ctx(10), deps, RunControl::new()).await;

    assert_eq!(
        reason,
        RunStopReason::Completed,
        "compaction must make the history fit"
    );
    let requests = model.requests();
    assert_eq!(requests.len(), 1, "exactly one model call");
    let seen = serde_json::to_string(&requests[0]).unwrap_or_else(|e| panic!("serialize: {e}"));
    assert!(
        seen.contains("[compaction:"),
        "the model must see the compaction digest: {seen}"
    );
    let request_chars: usize = requests[0]
        .messages
        .iter()
        .map(|message| match message {
            Message::User { content } | Message::Assistant { content } => content
                .iter()
                .filter_map(|block| match block {
                    Content::Text(text) | Content::Reasoning(text) => Some(text.len()),
                    _ => None,
                })
                .sum::<usize>(),
            Message::ToolResult(result) => result.content.len(),
            _ => 0,
        })
        .sum();
    assert!(
        request_chars < 10_000,
        "the model request must be far smaller than the 24k-char seed: {request_chars} chars"
    );
}

/// Purpose: with the real structural compactor, a ledger that cannot shrink
/// (a single oversized user message: nothing to trim, no legal digest split)
/// is refused with `BudgetBlocked` semantics instead of being sent to the
/// provider.
/// Scenario: seed = one user message of 100k chars against a 4k window; the
/// model is never called because the gate fires first.
/// Acceptance: stop is `Error` whose message round-trips through
/// `BudgetBlocked::from_error_message`, and the model saw zero requests.
#[tokio::test]
async fn real_default_compactor_refuses_unshrinkable_history() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("should never run".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));
    let deps = Arc::new(LoopDeps {
        model: Arc::clone(&model) as Arc<dyn Model>,
        tools: Vec::new(),
        event_sink: Arc::new(TestSink::default()),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        prompt_topology: Topology::default(),
        tool_mode: ToolExecutionMode::Sequential,
        budget: None,
        compactor: Some(Arc::new(harness_context::DefaultCompactor::new()) as Arc<dyn Compactor>),
        seed: vec![Message::user_text("y".repeat(100_000))],
        transcript_sink: None,
    });

    let reason = run(make_ctx(10), deps, RunControl::new()).await;

    let RunStopReason::Error { message } = reason else {
        panic!("an unshrinkable ledger must be refused, got {reason:?}");
    };
    let blocked = BudgetBlocked::from_error_message(&message)
        .unwrap_or_else(|| panic!("the error must carry BudgetBlocked semantics: {message}"));
    // The recorded stage is the FIRST active stage (the gate records where
    // compaction was attempted), not necessarily the deepest one reached.
    assert_eq!(blocked.stage, "soft", "the first active stage is recorded");
    assert_eq!(
        model.requests().len(),
        0,
        "the provider call never happened"
    );
}

/// Purpose: the host's configured per-run input limit caps the compactor's
/// model-window estimate, so `[budget].max_input_tokens` actually gates
/// compaction instead of being ignored.
/// Scenario: the model window is 4096 tokens; the configured budget allows
/// only 100; the ledger is 1000 bytes ≈ 300 tokens — fits the window, not the
/// configured cap.
/// Acceptance: the gate refuses the round with BudgetBlocked semantics and the
/// model sees zero requests (without the cap this run would have proceeded).
#[tokio::test]
async fn configured_budget_caps_the_compactor_window() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("should never run".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));
    let deps = Arc::new(LoopDeps {
        model: Arc::clone(&model) as Arc<dyn Model>,
        tools: Vec::new(),
        event_sink: Arc::new(TestSink::default()),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        prompt_topology: Topology::default(),
        tool_mode: ToolExecutionMode::Sequential,
        budget: Some(Budget {
            used_tokens: 0,
            available_tokens: 100,
        }),
        compactor: Some(Arc::new(DefaultCompactor::new()) as Arc<dyn Compactor>),
        seed: vec![Message::user_text("x".repeat(1_000))],
        transcript_sink: None,
    });

    let reason = run(make_ctx(10), deps, RunControl::new()).await;

    let RunStopReason::Error { message } = reason else {
        panic!("the configured cap must refuse the round, got {reason:?}");
    };
    let blocked = BudgetBlocked::from_error_message(&message)
        .unwrap_or_else(|| panic!("BudgetBlocked semantics expected: {message}"));
    assert_eq!(blocked.available, 100, "the configured cap is reported");
    assert_eq!(model.requests().len(), 0, "no provider call");
}
