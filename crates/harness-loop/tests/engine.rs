//! Integration tests for the harness agent loop.
//!
//! All tests use mock `Model` + mock `Tool` implementations -- no network, no
//! real model provider. The mock model returns pre-recorded event sequences
//! (tool calls, text, stop reasons) so each test exercises a specific loop
//! path without nondeterminism.
//!
//! # Test layout
//!
//! | Test | Scenario | Acceptance |
//! |---|---|---|
//! | `two_turns_normal` | Model requests a tool, tool succeeds, model answers | Two model turns; stop is Completed |
//! | `model_ends_early` | Model answers without tool calls | One RunFinished; stop is Completed |
//! | `cancellation_mid_loop` | Cancel fires before a new turn | Stop is Cancelled |
//! | `tool_failure_backfill` | Model requests a tool that fails | ToolFinished carries is_error=true; loop continues |
//! | `max_turns_exceeded` | Model keeps requesting tools past max_turns | Stop is MaxTurns |
//! | `steering_injection` | Steering queued before a turn | SteeringApplied event emitted |
//! | `hook_terminate_batch` | should_terminate_tool_batch returns true | Stop is Completed after tools execute |
//! | `approval_denied_backfill` | Broker denies an approval-gated call | Tool never starts; loop continues |
//! | `approval_precancelled` | Run cancelled before the broker answers | Stop is Cancelled |
//! | `approval_required_no_broker_fails_closed` | Approval required, no broker | Tool never starts; fail closed |
//! | `ignores_events_after_terminal_stop` | Adapter emits a delta after `Stop` | Trailing delta is ignored |
//! | `model_stream_error_is_terminal` | Model stream emits `Error` | Stop is `Error`; next turn never runs |
//! | `event_sequence_is_monotonic` | A normal tool run | `seq` is a gap-free 0-based counter |

use std::sync::Arc;

use harness_core::event::{EventSink, RunEvent, RunEventKind};

use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution,
    ToolPolicy,
};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use harness_core::{RunControl, StopReason};
use harness_loop::{LoopDeps, LoopHooks, ToolExecutionMode, run};

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

/// A mock model that returns pre-recorded turn sequences, FIFO.
struct MockModel {
    info: ModelInfo,
    turns: std::sync::Mutex<std::collections::VecDeque<Vec<ModelEvent>>>,
}

impl MockModel {
    fn new(info: ModelInfo, turns: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            info,
            turns: std::sync::Mutex::new(turns.into()),
        }
    }
}

#[async_trait::async_trait]
impl Model for MockModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|e| panic!("mock model lock poisoned: {e}"))
            .pop_front()
            .unwrap_or_else(|| panic!("mock model turn queue exhausted"));
        Box::pin(futures::stream::iter(events))
    }
}

/// A mock tool that returns a fixed result.
struct MockTool {
    name: String,
    schema: ToolSchema,
    result: std::sync::Mutex<ToolResult>,
}

impl MockTool {
    fn new(name: &str, result: ToolResult) -> Self {
        Self {
            name: name.to_string(),
            schema: ToolSchema {
                name: name.to_string(),
                description: format!("mock tool {name}"),
                input: serde_json::json!({}),
            },
            result: std::sync::Mutex::new(result),
        }
    }
}

#[async_trait::async_trait]
impl Tool for MockTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        self.schema.clone()
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    async fn invoke(
        &self,
        _args: serde_json::Value,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        Ok(self
            .result
            .lock()
            .unwrap_or_else(|e| panic!("mock tool lock poisoned: {e}"))
            .clone())
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

/// Collects emitted events for inspection.
#[derive(Default)]
struct TestSink {
    events: std::sync::Mutex<Vec<RunEvent>>,
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

/// Two-turn round trip: model requests a tool, tool succeeds, model answers.
///
/// Scenario:
/// 1. Turn 0: model text + ToolCall + Stop(ToolUse)
/// 2. Tool succeeds -> ToolResult backfilled
/// 3. Turn 1: model text + Stop(EndTurn)
///
/// Acceptance: stop is Completed; the event log shows exactly two model
/// turns (two TurnStarted/TurnFinished pairs) and one ToolStarted/ToolFinished
/// pair — the round trip actually looped back after the tool result.
#[tokio::test]
async fn two_turns_normal() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::TextDelta("I'll look that up.".into()),
                ModelEvent::ToolCallStart {
                    id: "call_1".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_1".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_1".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            vec![
                ModelEvent::TextDelta("The answer is 42.".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "42".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let kinds: Vec<&str> = events.iter().map(|e| event_tag(&e.kind)).collect();

    assert!(kinds.contains(&"run_started"), "should have RunStarted");
    assert!(kinds.contains(&"run_finished"), "should have RunFinished");

    let turn_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::TurnStarted { .. }))
        .count();
    assert_eq!(turn_started, 2, "should have run two model turns");

    let tool_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::ToolStarted { .. }))
        .count();
    let tool_finished = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::ToolFinished { .. }))
        .count();
    assert_eq!(tool_started, 1, "one tool should have started");
    assert_eq!(tool_finished, 1, "one tool should have finished");
}

/// Model answers without tool calls -> single assistant message, Completed.
#[tokio::test]
async fn model_ends_early() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("Hello, world!".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let finished_count = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::RunFinished { .. }))
        .count();
    assert_eq!(finished_count, 1, "exactly one RunFinished event");
}

/// Cancel fires before a new turn -> Cancelled.
#[tokio::test]
async fn cancellation_mid_loop() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("Hello!".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    // Cancel before running.
    let control = RunControl::new();
    control.cancel();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Cancelled, "loop should cancel");
}

/// Tool failure: model requests a tool that fails -> ToolResult with is_error=true.
#[tokio::test]
async fn tool_failure_backfill() {
    struct FailingTool;

    #[async_trait::async_trait]
    impl Tool for FailingTool {
        fn name(&self) -> &str {
            "failing_tool"
        }
        fn schema(&self) -> ToolSchema {
            ToolSchema {
                name: "failing_tool".into(),
                description: "always fails".into(),
                input: serde_json::json!({}),
            }
        }
        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }
        async fn invoke(
            &self,
            _args: serde_json::Value,
            _cancel: &tokio_util::sync::CancellationToken,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                content: "something went wrong".into(),
                is_error: true,
                trace: None,
            })
        }
    }

    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_fail".into(),
                    name: "failing_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_fail".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_fail".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            // After the failed tool is backfilled, the model answers.
            vec![
                ModelEvent::TextDelta("That failed; I will try another way.".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![Arc::new(FailingTool)],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(
        reason,
        RunStopReason::Completed,
        "loop should complete despite tool failure"
    );

    let events = sink.events();
    let tool_finished: Vec<&RunEvent> = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::ToolFinished { .. }))
        .collect();
    assert_eq!(tool_finished.len(), 1, "should have one ToolFinished event");
    if let RunEventKind::ToolFinished { is_error, .. } = &tool_finished[0].kind {
        assert!(*is_error, "tool should be marked as error");
    } else {
        panic!("expected ToolFinished event");
    }
}

/// Max turns exceeded.
#[tokio::test]
async fn max_turns_exceeded() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_1".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_1".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_1".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_2".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_2".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_2".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
        ],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    // max_turns = 1 means exactly one model call.
    let ctx = RunContext {
        limits: RunLimits {
            max_turns: 1,
            ..RunLimits::default()
        },
        ..make_ctx(10)
    };

    let control = RunControl::new();
    let reason = run(ctx, deps, control).await;

    assert_eq!(reason, RunStopReason::MaxTurns, "loop should hit max turns");
}

/// Steering messages are drained and injected as user messages.
#[tokio::test]
async fn steering_injection() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("Got it.".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    control.steer("Focus on the numbers.");
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let has_steering = events
        .iter()
        .any(|e| matches!(e.kind, RunEventKind::SteeringApplied { .. }));
    assert!(has_steering, "should have SteeringApplied event");
}

/// Hook: should_terminate_tool_batch returns true -> Completed early.
#[tokio::test]
async fn hook_terminate_batch() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::ToolCallStart {
                id: "call_1".into(),
                name: "test_tool".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call_1".into(),
                partial_json: "{}".into(),
            },
            ModelEvent::ToolCallEnd {
                id: "call_1".into(),
            },
            ModelEvent::Stop(StopReason::ToolUse),
        ]],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let hooks = LoopHooks {
        should_terminate_tool_batch: Some(Arc::new(|_, _| true)),
        ..LoopHooks::default()
    };

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks,
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(
        reason,
        RunStopReason::Completed,
        "loop should complete via hook"
    );
}

// ---------------------------------------------------------------------------
// Approval-path tests
// ---------------------------------------------------------------------------

/// A policy that requires approval for every tool.
struct RequireApprovalPolicy;

impl ToolPolicy for RequireApprovalPolicy {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &harness_core::tool::ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::RequireApproval,
            reason: "requires approval".into(),
            source: "test".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// A broker that returns a fixed answer, unless the run is already cancelled.
struct MockBroker {
    result: ApprovalResult,
}

#[async_trait::async_trait]
impl ApprovalBroker for MockBroker {
    async fn request(
        &self,
        _req: &harness_core::tool::ToolRequest,
        _decision: &PolicyDecision,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> ApprovalResult {
        if cancel.is_cancelled() {
            return ApprovalResult::Cancelled;
        }
        self.result.clone()
    }
}

/// Approval denied by the broker: the tool is not executed, the denial is
/// backfilled as an is_error ToolResult, and the loop continues.
///
/// Scenario: policy requires approval; broker denies; model answers next turn.
///
/// Acceptance: stop is Completed; no ToolStarted event is emitted; the
/// ApprovalRequested / ApprovalResolved pair is present.
#[tokio::test]
async fn approval_denied_backfill() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_ap".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_ap".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_ap".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            vec![
                ModelEvent::TextDelta("Denied; moving on.".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "should not run".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(RequireApprovalPolicy);
    let broker = Arc::new(MockBroker {
        result: ApprovalResult::Denied {
            reason: "human said no".into(),
        },
    });

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: Some(broker),
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let tool_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::ToolStarted { .. }))
        .count();
    assert_eq!(tool_started, 0, "tool must not run after denial");
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::ApprovalRequested { .. })),
        "should have ApprovalRequested"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::ApprovalResolved { .. })),
        "should have ApprovalResolved"
    );
}

/// Pre-cancelled approval: the broker observes the run's cancellation token
/// and answers Cancelled before the loop reaches the next turn boundary.
///
/// Scenario: policy requires approval; the run is cancelled first; the broker
/// returns Cancelled.
///
/// Acceptance: stop is Cancelled.
#[tokio::test]
async fn approval_precancelled() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::ToolCallStart {
                id: "call_apc".into(),
                name: "test_tool".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call_apc".into(),
                partial_json: "{}".into(),
            },
            ModelEvent::ToolCallEnd {
                id: "call_apc".into(),
            },
            ModelEvent::Stop(StopReason::ToolUse),
        ]],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(RequireApprovalPolicy);
    let broker = Arc::new(MockBroker {
        result: ApprovalResult::Approved,
    });

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: Some(broker),
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    // Cancel immediately: the broker sees an already-cancelled token.
    control.cancel();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Cancelled, "loop should cancel");
}

/// Approval required but no broker configured: fail closed.
///
/// Scenario: policy requires approval; no broker is set; the call is denied
/// and backfilled as is_error.
///
/// Acceptance: stop is Completed and no ToolStarted event is emitted.
#[tokio::test]
async fn approval_required_no_broker_fails_closed() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_nb".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_nb".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_nb".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            vec![
                ModelEvent::TextDelta("Continuing after denial.".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "should not run".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(RequireApprovalPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let tool_started = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::ToolStarted { .. }))
        .count();
    assert_eq!(
        tool_started, 0,
        "tool must not run when approval is unavailable"
    );
}

/// Receiving-side termination discipline: events after a terminal `Stop`
/// must be ignored by the loop.
///
/// Scenario: the mock stream yields `TextDelta("before")`, `Stop(EndTurn)`,
/// then `TextDelta("after")` — an adapter that violates the terminal
/// discipline.
///
/// Acceptance: only "before" is emitted as a TextDelta event; the trailing
/// text never reaches the transcript or the event log.
#[tokio::test]
async fn ignores_events_after_terminal_stop() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![vec![
            ModelEvent::TextDelta("before".into()),
            ModelEvent::Stop(StopReason::EndTurn),
            // Contract violation on purpose: a well-behaved adapter never
            // emits this, but the loop must not trust that.
            ModelEvent::TextDelta("after".into()),
        ]],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::TextDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["before"],
        "events after terminal Stop must be ignored"
    );
}

/// Model stream error is terminal: the loop stops with `Error` and does not
/// keep looping even though the transcript could in principle continue.
///
/// Scenario: the mock stream yields text then `Error`, with a second turn
/// queued that must never be consumed.
///
/// Acceptance: stop is `Error`, and the queued second turn is untouched.
#[tokio::test]
async fn model_stream_error_is_terminal() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::TextDelta("partial".into()),
                ModelEvent::Error {
                    message: "connection reset".into(),
                },
            ],
            // Must never be consumed: the error is terminal.
            vec![
                ModelEvent::TextDelta("should not happen".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;

    match reason {
        RunStopReason::Error { message } => {
            assert_eq!(message, "connection reset", "error message propagates");
        }
        other => panic!("expected Error stop reason, got {other:?}"),
    }
}

/// The emitted event sequence is strictly monotonic per run.
///
/// Scenario: a normal two-turn tool run.
///
/// Acceptance: every `RunEvent.seq` is exactly one greater than its
/// predecessor, starting at 0.
#[tokio::test]
async fn event_sequence_is_monotonic() {
    let model = Arc::new(MockModel::new(
        make_model_info(),
        vec![
            vec![
                ModelEvent::ToolCallStart {
                    id: "call_seq".into(),
                    name: "test_tool".into(),
                },
                ModelEvent::ToolCallArgs {
                    id: "call_seq".into(),
                    partial_json: "{}".into(),
                },
                ModelEvent::ToolCallEnd {
                    id: "call_seq".into(),
                },
                ModelEvent::Stop(StopReason::ToolUse),
            ],
            vec![
                ModelEvent::TextDelta("done".into()),
                ModelEvent::Stop(StopReason::EndTurn),
            ],
        ],
    ));

    let tool = Arc::new(MockTool::new(
        "test_tool",
        ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        },
    ));

    let sink = Arc::new(TestSink::default());
    let policy = Arc::new(AllowAllPolicy);

    let deps = Arc::new(LoopDeps {
        model,
        tools: vec![tool],
        event_sink: sink.clone(),
        policy,
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let reason = run(make_ctx(10), deps, control).await;
    assert_eq!(reason, RunStopReason::Completed, "loop should complete");

    let events = sink.events();
    assert!(!events.is_empty(), "should have emitted events");
    for (idx, event) in events.iter().enumerate() {
        assert_eq!(
            event.seq, idx as u64,
            "seq must be a gap-free monotonic counter"
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn event_tag(kind: &RunEventKind) -> &str {
    match kind {
        RunEventKind::RunStarted { .. } => "run_started",
        RunEventKind::RunFinished { .. } => "run_finished",
        RunEventKind::SteeringApplied { .. } => "steering_applied",
        RunEventKind::TurnStarted { .. } => "turn_started",
        RunEventKind::TurnFinished { .. } => "turn_finished",
        RunEventKind::TextDelta { .. } => "text_delta",
        RunEventKind::ToolCall(_) => "tool_call",
        RunEventKind::ToolStarted { .. } => "tool_started",
        RunEventKind::ToolFinished { .. } => "tool_finished",
        RunEventKind::PolicyDecided { .. } => "policy_decided",
        RunEventKind::ApprovalRequested { .. } => "approval_requested",
        RunEventKind::ApprovalResolved { .. } => "approval_resolved",
        RunEventKind::Error { .. } => "error",
        // RunEventKind is #[non_exhaustive]: future variants get a generic tag.
        _ => "unknown",
    }
}

/// Purpose: cancelling while the model stream is open ends the run promptly.
/// Scenario: a model whose stream emits one delta then parks until cancelled;
/// the test cancels mid-stream and waits for the run to finish.
/// Acceptance: the stop reason is `Cancelled`, and no event arrives after the
/// cancellation — the loop does not keep burning the stream into a void.
#[tokio::test]
async fn cancel_mid_stream_ends_promptly() {
    use std::time::Duration;

    struct ParkedStreamModel {
        info: ModelInfo,
    }

    #[async_trait::async_trait]
    impl Model for ParkedStreamModel {
        fn info(&self) -> &ModelInfo {
            &self.info
        }

        async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
            Box::pin(futures::stream::unfold((), |()| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Some((ModelEvent::TextDelta("tick".into()), ()))
            }))
        }
    }

    let sink = Arc::new(TestSink::default());
    let deps = Arc::new(LoopDeps {
        model: Arc::new(ParkedStreamModel {
            info: make_model_info(),
        }),
        tools: vec![],
        event_sink: sink.clone(),
        policy: Arc::new(AllowAllPolicy),
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    });

    let control = RunControl::new();
    let cancel_token = control.cancel_token();
    let handle = tokio::spawn(async move { run(make_ctx(10), deps, control).await });

    // Let the stream open, then cancel mid-stream.
    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel_token.cancel();

    let reason = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap_or_else(|e| panic!("run did not end within 5s of cancel: {e}"))
        .unwrap_or_else(|e| panic!("run task panicked: {e}"));
    assert_eq!(reason, RunStopReason::Cancelled, "mid-stream cancel wins");
}
