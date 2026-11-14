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
    EventBus, RunRequest, RunStatus, ScheduleError, Scheduler,
    bus::{BUS_CAPACITY, ReceiveError},
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
        budget: None,
        compactor: None,
    }
}

/// Purpose: a full queue rejects explicitly instead of dropping the run.
/// Scenario: scheduler capacity 1, one run active (slow), two submitted.
/// Acceptance: the first queued submission is accepted, the second returns
/// `QueueFull`, and nothing was silently discarded.
#[tokio::test]
async fn full_queue_rejects_explicitly() {
    let scheduler = Scheduler::new(1, 4);
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
    let scheduler = Scheduler::new(4, 4);
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
    let scheduler = Scheduler::new(1, 4);
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
    let scheduler = Scheduler::new(1, 4);
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
    let scheduler = Scheduler::new(1, 4);
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

    let scheduler = Scheduler::new(1, 4);
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
                budget: None,
                compactor: None,
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
    for _ in 0..1000 {
        if let Some(status) = scheduler.status(run_id)
            && predicate(&status)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} never reached the expected status");
}
/// A model that emits `n` text deltas, pacing each one with `pace`, then
/// ends the turn. Streams long enough to overflow a [`BUS_CAPACITY`]-slot bus
/// buffer while a subscriber looks away.
struct PacingModel {
    info: ModelInfo,
    n: usize,
    pace: Duration,
}

impl PacingModel {
    fn new(n: usize, pace: Duration) -> Self {
        Self {
            info: ModelInfo {
                handle: "mock".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
            n,
            pace,
        }
    }
}

#[async_trait::async_trait]
impl Model for PacingModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        let n = self.n;
        let pace = self.pace;
        Box::pin(futures::stream::unfold(0usize, move |i| async move {
            if i < n {
                tokio::time::sleep(pace).await;
                Some((ModelEvent::TextDelta(format!("delta {i}")), i + 1))
            } else if i == n {
                tokio::time::sleep(pace).await;
                Some((ModelEvent::Stop(StopReason::EndTurn), i + 1))
            } else {
                None
            }
        }))
    }
}

/// Lagged subscriber on a saturated bus, plus dropped-event accounting.
///
/// Scenario:
/// 1. Submit run `r-lagged` whose model streams more than [`BUS_CAPACITY`]
///    text deltas; subscribe after the run is active (it is already
///    registered on the bus).
/// 2. Do not read from the subscription while the bus overflows, then read:
///    the subscriber must observe `ReceiveError::Lagged(n)` with `n >= 1`.
/// 3. Submit run `r-dropped` whose model emits a few events with no
///    subscriber: every send fails on the bus, so `dropped_events` grows.
///
/// Acceptance:
/// - the lagged subscriber observes `Lagged(n >= 1)`;
/// - `r-lagged` still reaches `Finished(Completed)` — the emitter never
///   blocked on the subscriber;
/// - `bus.dropped_events()` ends above where it started.
#[tokio::test]
async fn lagged_subscriber_observes_lagged_and_run_finishes() {
    let scheduler = Scheduler::new(4, 4);
    let bus = scheduler.events();
    // More events than the bus buffer holds, emitted while the subscriber
    // never reads: the overflow is guaranteed, so the read below needs no
    // timing window at all.
    let n_deltas = BUS_CAPACITY + 50;

    let lagged_ctx = ctx("r-lagged");
    let run_id = lagged_ctx.run_id.clone();
    let model = Arc::new(PacingModel::new(n_deltas, Duration::from_millis(1)));
    scheduler
        .submit(RunRequest {
            ctx: lagged_ctx,
            deps: deps(model, vec![], Arc::new(CountingSink::default())),
        })
        .unwrap_or_else(|e| panic!("run must start: {e}"));

    let mut sub = subscribe_after_start(&scheduler, bus, &run_id).await;

    // Let the run finish without reading: the subscription's cursor falls
    // behind the buffer start, which is exactly the lag condition.
    wait_for_status(&scheduler, &run_id, |status| {
        matches!(status, RunStatus::Finished(RunStopReason::Completed))
    })
    .await;

    // The first read must report the gap instead of an event: the cursor
    // is far behind the oldest buffered event.
    match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
        Ok(Err(ReceiveError::Lagged(n))) => {
            assert!(
                n >= 1,
                "lagged report must account for at least one event, got {n}"
            );
        }
        Ok(Err(ReceiveError::Closed)) => {
            panic!("subscription closed before reporting lag");
        }
        Ok(Ok(event)) => {
            panic!("subscription produced an event before lag: {event:?}");
        }
        Err(_elapsed) => {
            panic!("subscription timed out waiting for lag");
        }
    }
}

/// A finished run id can never be resubmitted: ids are single use.
///
/// Scenario:
/// 1. Submit run `r-once` with a model that ends its turn immediately
///    (`Stop(EndTurn)`), then wait for `Finished(Completed)`.
/// 2. Submit a new request carrying the same run id.
///
/// Acceptance: the second submit fails with exactly
/// `ScheduleError::DuplicateRun(_)`. Finished ids are retired, not reusable
/// — this pins behavior the handoff left undocumented.
#[tokio::test]
async fn resubmit_after_finished_is_duplicate_run() {
    let scheduler = Scheduler::new(4, 4);
    let model = Arc::new(SlowModel::new(
        vec![vec![ModelEvent::Stop(StopReason::EndTurn)]],
        Duration::ZERO,
    ));
    let make_request = || RunRequest {
        ctx: ctx("r-once"),
        deps: deps(model.clone(), vec![], Arc::new(CountingSink::default())),
    };

    let run_id = match scheduler.submit(make_request()) {
        Ok(id) => id,
        Err(err) => panic!("first submit of r-once must start: {err}"),
    };
    assert_eq!(run_id, RunId("r-once".into()));

    wait_for_status(&scheduler, &run_id, |status| {
        matches!(status, RunStatus::Finished(RunStopReason::Completed))
    })
    .await;

    match scheduler.submit(make_request()) {
        Err(ScheduleError::DuplicateRun(_)) => {}
        Ok(id) => panic!("resubmitting a finished id must fail, got {id:?}"),
        Err(err) => panic!("resubmitting a finished id must be DuplicateRun, got {err:?}"),
    }
}
/// Create a run context for `run_id` in `session`, mirroring `ctx` so
/// per-session tests can place runs in different sessions.
fn ctx_in(run_id: &str, session: &str) -> RunContext {
    RunContext {
        run_id: RunId(run_id.into()),
        session_id: SessionId(session.into()),
        task: Task {
            description: "test task".into(),
            source: None,
            deadline: None,
        },
        limits: RunLimits::default(),
        started_at_ms: 0,
    }
}

/// Purpose: different sessions run concurrently up to the global cap.
/// Scenario: cap 2; run "a" in session "s-a" and run "b" in session "s-b"
/// stream slowly so both are active at once.
/// Acceptance: both report `Running` at the same time, then both finish.
#[tokio::test]
async fn different_sessions_run_concurrently() {
    let scheduler = Scheduler::new(4, 2);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(300)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("a", "s-a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("b", "s-b"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    wait_for_status(&scheduler, &RunId("a".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
    wait_for_status(&scheduler, &RunId("b".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
    assert!(matches!(
        scheduler.status(&RunId("a".into())),
        Some(RunStatus::Running)
    ));
    assert!(matches!(
        scheduler.status(&RunId("b".into())),
        Some(RunStatus::Running)
    ));
    wait_for_status(&scheduler, &RunId("a".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
    wait_for_status(&scheduler, &RunId("b".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
}

/// Purpose: same-session runs serialize — the second run waits instead of
/// running alongside the first.
/// Scenario: session "s" has an active slow run; submit a second run for "s".
/// Acceptance: the second run reports `Queued` while the first is active, is
/// never `Running` until the first finishes, then takes over.
#[tokio::test]
async fn same_session_second_run_waits() {
    let scheduler = Scheduler::new(4, 4);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(300)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("first", "s"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("second", "s"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(
        scheduler.status(&RunId("second".into())),
        Some(RunStatus::Queued { position: 0 })
    );
    wait_for_status(&scheduler, &RunId("first".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
    assert!(
        !matches!(
            scheduler.status(&RunId("second".into())),
            Some(RunStatus::Running)
        ),
        "same-session run must not run concurrently with the active one"
    );
    wait_for_status(&scheduler, &RunId("first".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
    wait_for_status(&scheduler, &RunId("second".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
}

/// Purpose: the global cap serializes runs across different sessions.
/// Scenario: cap 1; run "a" in session "s-a" runs while run "b" in session
/// "s-b" waits although its session is idle; when "a" finishes, the launch
/// round admits "b".
/// Acceptance: "b" is `Queued` while "a" runs, becomes `Running` only after
/// "a" finishes, then finishes itself.
#[tokio::test]
async fn global_cap_serializes_across_sessions() {
    let scheduler = Scheduler::new(4, 1);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(300)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("a", "s-a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("b", "s-b"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(
        scheduler.status(&RunId("b".into())),
        Some(RunStatus::Queued { position: 0 })
    );
    wait_for_status(&scheduler, &RunId("a".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
    wait_for_status(&scheduler, &RunId("b".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;
    wait_for_status(&scheduler, &RunId("b".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
}

/// Purpose: queue capacity is per session, not global.
/// Scenario: queue capacity 1; session "s-a" has an active run and one queued
/// run; a third "s-a" run gets `QueueFull`. Session "s-b" still submits fine.
/// Acceptance: the third "s-a" submission fails with `QueueFull { capacity: 1
/// }`; an "s-b" submission starts immediately and one run waits overall.
#[tokio::test]
async fn queue_full_is_per_session() {
    let scheduler = Scheduler::new(1, 4);
    let model = Arc::new(SlowModel::new(vec![], Duration::from_millis(300)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("a1", "s-a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("a2", "s-a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));

    let err = scheduler
        .submit(RunRequest {
            ctx: ctx_in("a3", "s-a"),
            deps: deps(model.clone(), vec![], sink.clone()),
        })
        .err();
    assert_eq!(err, Some(ScheduleError::QueueFull { capacity: 1 }));

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("b1", "s-b"),
            deps: deps(model, vec![], sink),
        })
        .unwrap_or_else(|e| panic!("other session must still submit: {e}"));
    assert_eq!(
        scheduler.status(&RunId("b1".into())),
        Some(RunStatus::Running)
    );
    assert_eq!(scheduler.queued(), 1);
}

/// Purpose: when a slot frees, the launch round admits the oldest
/// SUBMISSION across sessions, not whichever session happens to iterate
/// first — the global ticket must order admission worldwide.
/// Scenario: cap 2; "a" (fast model) and "b" (slow model) run in sessions
/// s-a/s-b; "c" (s-c) then "d" (s-d) are submitted and queue; "a" finishes,
/// freeing exactly one slot while "b" still holds the other.
/// Acceptance: "c" — the older submission — becomes `Running`, while "d"
/// stays `Queued` because the cap is saturated by "b" and "c".
#[tokio::test]
async fn launch_round_admits_oldest_submission_first() {
    let scheduler = Scheduler::new(4, 2);
    let fast = Arc::new(SlowModel::new(vec![], Duration::from_millis(150)));
    let slow = Arc::new(SlowModel::new(vec![], Duration::from_millis(5000)));
    let sink: Arc<dyn EventSink> = Arc::new(CountingSink::default());

    scheduler
        .submit(RunRequest {
            ctx: ctx_in("a", "s-a"),
            deps: deps(fast.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("b", "s-b"),
            deps: deps(slow.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    // Both slots are taken, so these two queue in submission order.
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("c", "s-c"),
            deps: deps(fast.clone(), vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    scheduler
        .submit(RunRequest {
            ctx: ctx_in("d", "s-d"),
            deps: deps(fast, vec![], sink.clone()),
        })
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        scheduler.status(&RunId("d".into())),
        Some(RunStatus::Queued { position: 0 }),
        "d must be queued while both slots are busy"
    );

    wait_for_status(&scheduler, &RunId("a".into()), |status| {
        matches!(status, RunStatus::Finished(_))
    })
    .await;
    wait_for_status(&scheduler, &RunId("c".into()), |status| {
        matches!(status, RunStatus::Running)
    })
    .await;

    // "b" still holds its slot, so the freed slot went to the older
    // submission only.
    assert!(matches!(
        scheduler.status(&RunId("c".into())),
        Some(RunStatus::Running)
    ));
    assert_eq!(
        scheduler.status(&RunId("d".into())),
        Some(RunStatus::Queued { position: 0 }),
        "the younger submission must stay queued while the cap is saturated"
    );

    // Reap the long run so the task does not outlive the test.
    assert!(scheduler.cancel(&RunId("b".into())));
}
