//! Scheduler contract tests.
//!
//! All tests use a mock `Model` and a mock `Tool` — no network, no provider.
//! They exercise the scheduler's own contracts: bounded queue rejection,
//! per-run cancellation, multi-subscriber fan-out, the drop protocol, the
//! per-run `seq` guarantee across the fan-out, and the approval mid-ask race
//! under a real Tokio runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use harness_core::StopReason;
use harness_core::event::{EventSink, RunEvent, RunEventKind};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution,
    ToolPolicy,
};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use harness_loop::{LoopDeps, LoopHooks, ToolExecutionMode};
use harness_scheduler::{
    EventBus, RunRequest, RunStatus, ScheduleError, Scheduler, bus::ReceiveError,
};

/// A model that streams `turns` turns, then repeats the last one forever so a
/// cancelled run has something to be cancelled in the middle of.
struct SlowModel {
    info: ModelInfo,
    turns: std::sync::Mutex<std::collections::VecDeque<Vec<ModelEvent>>>,
    /// Delay before each stream yields its first event — long enough for a test
    /// to cancel mid-run.
    delay: Duration,
}

impl SlowModel {
    fn new(turns: Vec<Vec<ModelEvent>>, delay: Duration) -> Self {
        Self {
            info: ModelInfo {
                handle: "mock".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
            turns: std::sync::Mutex::new(turns.into()),
            delay,
        }
    }
}

#[async_trait::async_trait]
impl Model for SlowModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        tokio::time::sleep(self.delay).await;
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|e| panic!("mock model lock poisoned: {e}"))
            .pop_front()
            .unwrap_or_else(|| vec![ModelEvent::Stop(StopReason::EndTurn)]);
        Box::pin(futures::stream::iter(events))
    }
}

/// A tool that counts its invocations.
struct CountingTool {
    name: String,
    invocations: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Tool for CountingTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.name.clone(),
            description: "counts invocations".into(),
            input: serde_json::json!({"type": "object"}),
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
        self.invocations.fetch_add(1, Ordering::Relaxed);
        Ok(ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        })
    }
}

/// A policy that allows everything.
struct AllowAll;

impl ToolPolicy for AllowAll {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &harness_core::tool::ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::Allow,
            reason: String::new(),
            source: "test-policy".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// An approval broker that never answers until the run's token fires, letting a
/// test cancel while the question is open (the mid-ask race).
struct HangingBroker {
    asked: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl ApprovalBroker for HangingBroker {
    async fn request(
        &self,
        _req: &harness_core::tool::ToolRequest,
        _decision: &PolicyDecision,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> ApprovalResult {
        self.asked.fetch_add(1, Ordering::Relaxed);
        // The only way out is cancellation: this is the mid-ask window.
        cancel.cancelled().await;
        ApprovalResult::Cancelled
    }
}

fn ctx(run_id: &str) -> RunContext {
    RunContext {
        run_id: RunId(run_id.into()),
        session_id: SessionId("s".into()),
        task: Task {
            description: "test task".into(),
            source: None,
            deadline: None,
        },
        limits: RunLimits::default(),
        started_at_ms: 0,
    }
}

fn deps(model: Arc<dyn Model>, tools: Vec<Arc<dyn Tool>>, sink: Arc<dyn EventSink>) -> LoopDeps {
    LoopDeps {
        model,
        tools,
        event_sink: sink,
        policy: Arc::new(AllowAll),
        approval_broker: None,
        hooks: LoopHooks::default(),
        tool_mode: ToolExecutionMode::Sequential,
    }
}

/// Purpose: a full queue rejects explicitly instead of dropping the run.
/// Scenario: scheduler capacity 1, one run active (slow), two submitted.
/// Acceptance: the first queued submission is accepted, the second returns
/// `QueueFull`, and nothing was silently discarded.
#[tokio::test]
async fn full_queue_rejects_explicitly() {
    let scheduler = Scheduler::new(1);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(500)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx("a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("first run must start: {e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx("b"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("second run must queue: {e}"));

    let err = scheduler
        .submit(RunRequest {
            ctx: ctx("c"),
            deps: deps(model, vec![], sink),
        })
        .err();
    assert_eq!(err, Some(ScheduleError::QueueFull { capacity: 1 }));
    assert_eq!(scheduler.queued(), 1);
}

/// Purpose: cancelling the active run stops it and the queued run takes over.
/// Scenario: a slow active run, one queued, cancel the active one.
/// Acceptance: the active run ends `Cancelled` and the queued run reaches
/// `Running`.
#[tokio::test]
async fn cancel_active_promotes_the_queue() {
    let scheduler = Scheduler::new(4);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(200)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx("first"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx("second"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(
        scheduler.status(&RunId("second".into())),
        Some(RunStatus::Queued { position: 0 })
    );

    assert!(scheduler.cancel(&RunId("first".into())));
    wait_for_status(&scheduler, &RunId("first".into()), |status| {
        matches!(status, RunStatus::Finished(RunStopReason::Cancelled))
    })
    .await;

    wait_for_status(&scheduler, &RunId("second".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
}

/// Purpose: two subscribers both receive the run's events (multiplex).
/// Scenario: subscribe twice to one run, let it finish.
/// Acceptance: both subscriptions observe the run's terminal `RunFinished`.
#[tokio::test]
async fn two_subscribers_receive_the_same_run() {
    let scheduler = Scheduler::new(1);
    let model = Arc::new(SlowModel::new(
        vec![vec![
            ModelEvent::TextDelta("hi".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
        Duration::from_millis(0),
    ));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());
    let bus = scheduler.events().clone();
    let run_id = RunId("multi".into());

    // Registration is synchronous inside `submit`, while the run's task only
    // starts polling when this task first awaits — so subscribing immediately
    // after `submit` cannot miss an event.
    scheduler
        .submit(RunRequest {
            ctx: ctx("multi"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    let mut first = subscribe_after_start(&scheduler, &bus, &run_id).await;
    let mut second = bus
        .subscribe(&run_id)
        .unwrap_or_else(|| panic!("second subscription must exist"));

    let finished_first = tokio::spawn(async move {
        let mut terminal = None;
        loop {
            match first.recv().await {
                Ok(event) => {
                    if let RunEventKind::RunFinished { .. } = event.kind {
                        terminal = Some(event);
                        break;
                    }
                }
                Err(ReceiveError::Closed) => break,
                Err(ReceiveError::Lagged(_)) => continue,
            }
        }
        terminal
    });
    let finished_second = tokio::spawn(async move {
        let mut terminal = None;
        loop {
            match second.recv().await {
                Ok(event) => {
                    if let RunEventKind::RunFinished { .. } = event.kind {
                        terminal = Some(event);
                        break;
                    }
                }
                Err(ReceiveError::Closed) => break,
                Err(ReceiveError::Lagged(_)) => continue,
            }
        }
        terminal
    });

    let first_terminal = finished_first
        .await
        .unwrap_or_else(|e| panic!("first subscriber task: {e}"));
    let second_terminal = finished_second
        .await
        .unwrap_or_else(|e| panic!("second subscriber task: {e}"));
    assert!(
        first_terminal.is_some(),
        "first subscriber must see RunFinished"
    );
    assert!(
        second_terminal.is_some(),
        "second subscriber must see RunFinished"
    );
}

/// Purpose: the drop protocol cancels a run whose last subscriber disappears.
/// Scenario: subscribe, then drop the subscription while the run is active.
/// Acceptance: the run ends `Cancelled` — nobody is left to observe it.
#[tokio::test]
async fn dropping_the_last_subscriber_cancels_the_run() {
    let scheduler = Scheduler::new(1);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(5_000)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());
    let bus = scheduler.events().clone();
    let run_id = RunId("dropped".into());

    // The run is active and slow; the drop below must cancel it.
    scheduler
        .submit(RunRequest {
            ctx: ctx("dropped"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    let subscription = subscribe_after_start(&scheduler, &bus, &run_id).await;
    drop(subscription);

    wait_for_status(&scheduler, &run_id, |status| {
        matches!(status, RunStatus::Finished(RunStopReason::Cancelled))
    })
    .await;
}

/// Purpose: `seq` stays per-run monotonic and gap-free across the fan-out.
/// Scenario: one run with several events, one subscriber.
/// Acceptance: the received `seq` values are exactly 0..n with no gaps —
/// the scheduler preserves the loop's emitter-side counter.
#[tokio::test]
async fn sequence_stays_monotonic_across_the_fan_out() {
    let scheduler = Scheduler::new(1);
    let model = Arc::new(SlowModel::new(
        vec![vec![
            ModelEvent::TextDelta("a".into()),
            ModelEvent::TextDelta("b".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]],
        Duration::from_millis(0),
    ));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());
    let bus = scheduler.events().clone();
    let run_id = RunId("seq".into());

    // Registration is synchronous inside `submit`; the run's task starts
    // polling only when this task awaits, so the subscription below attaches
    // before the first event exists.
    scheduler
        .submit(RunRequest {
            ctx: ctx("seq"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    let mut subscription = subscribe_after_start(&scheduler, &bus, &run_id).await;

    let collector = tokio::spawn(async move {
        let mut seqs = Vec::new();
        loop {
            match subscription.recv().await {
                Ok(event) => {
                    let terminal = matches!(event.kind, RunEventKind::RunFinished { .. });
                    seqs.push(event.seq);
                    if terminal {
                        break;
                    }
                }
                Err(ReceiveError::Closed) => break,
                Err(ReceiveError::Lagged(_)) => continue,
            }
        }
        seqs
    });

    let seqs = collector
        .await
        .unwrap_or_else(|e| panic!("collector task: {e}"));
    assert!(seqs.len() > 2, "expected several events, got {seqs:?}");
    let expected: Vec<u64> = (0..seqs.len() as u64).collect();
    assert_eq!(seqs, expected, "per-run seq must be gap-free and 0-based");
}

/// Purpose: the approval mid-ask race — cancelling while the broker is deciding
/// must not hang and must end the run as `Cancelled`.
/// Scenario: a policy that requires approval for `gated`, a broker that blocks
/// until cancelled, and a timer that cancels the run mid-question.
/// Acceptance: the broker was asked exactly once, the run terminates
/// `Cancelled`, and the tool never executed.
#[tokio::test]
async fn approval_mid_ask_race_ends_the_run() {
    let asked = Arc::new(AtomicU64::new(0));
    let invocations = Arc::new(AtomicU64::new(0));

    let scheduler = Scheduler::new(1);
    let model = Arc::new(SlowModel::new(
        vec![vec![
            ModelEvent::ToolCallStart {
                id: "call-1".into(),
                name: "gated".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call-1".into(),
                partial_json: "{}".into(),
            },
            ModelEvent::ToolCallEnd {
                id: "call-1".into(),
            },
            ModelEvent::Stop(StopReason::ToolUse),
        ]],
        Duration::from_millis(0),
    ));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());
    let tool = Arc::new(CountingTool {
        name: "gated".into(),
        invocations: invocations.clone(),
    });

    let run_id = RunId("race".into());
    scheduler
        .submit(RunRequest {
            ctx: ctx("race"),
            deps: LoopDeps {
                model,
                tools: vec![tool],
                event_sink: sink,
                policy: Arc::new(NeedsApproval),
                approval_broker: Some(Arc::new(HangingBroker {
                    asked: asked.clone(),
                })),
                hooks: LoopHooks::default(),
                tool_mode: ToolExecutionMode::Sequential,
            },
        })
        .unwrap_or_else(|e| panic!("{e}"));

    // Give the loop time to reach the broker, then cancel mid-question.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        scheduler.cancel(&run_id),
        "the active run must be cancellable"
    );

    wait_for_status(&scheduler, &run_id, |status| {
        matches!(status, RunStatus::Finished(RunStopReason::Cancelled))
    })
    .await;
    assert_eq!(
        asked.load(Ordering::Relaxed),
        1,
        "broker asked exactly once"
    );
    assert_eq!(
        invocations.load(Ordering::Relaxed),
        0,
        "the gated tool must never run"
    );
}

/// A policy that requires approval for every call — the mid-ask race needs one.
struct NeedsApproval;

impl ToolPolicy for NeedsApproval {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &harness_core::tool::ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::RequireApproval,
            reason: "gated".into(),
            source: "test-policy".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// A sink that records nothing — the bus sink is replaced by the scheduler, so
/// a pre-registration sink must never be the one events flow through.
#[derive(Default)]
struct CountingSink {
    events: AtomicU64,
}

impl EventSink for CountingSink {
    fn emit(&self, _event: RunEvent) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }
}

/// Submit a run and subscribe to it once the scheduler has registered it.
async fn subscribe_after_start(
    scheduler: &Scheduler,
    bus: &EventBus,
    run_id: &RunId,
) -> harness_scheduler::Subscription {
    for _ in 0..100 {
        if let Some(subscription) = bus.subscribe(run_id) {
            return subscription;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = scheduler;
    panic!("run {run_id} was never registered on the bus");
}

/// Poll `status` until `predicate` holds (bounded so a regression fails rather
/// than hangs).
async fn wait_for_status<F>(scheduler: &Scheduler, run_id: &RunId, predicate: F)
where
    F: Fn(&RunStatus) -> bool,
{
    for _ in 0..200 {
        if let Some(status) = scheduler.status(run_id)
            && predicate(&status)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} never reached the expected status");
}
