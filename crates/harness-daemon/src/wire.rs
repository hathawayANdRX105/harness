//! The daemon's transport seams: the real [`CommandHandler`] and the v1
//! [`EventLog`].
//!
//! [`DaemonHandler`] maps slash commands onto the scheduler: `run` submits a
//! run assembled by the builder, `cancel` / `status` / `list` read scheduler
//! state, `steer` is rejected until the scheduler exposes steering. Events
//! flow loop → bus → a per-run pump task that appends into the in-memory log
//! and forwards into the transport broadcast channel, so every transport
//! client sees every event.
//!
//! v1 notes (both reported to the maintainer, not silent):
//! - The pump holds the run's only bus subscription, so the bus drop protocol
//!   fires only from daemon-internal decisions — client disconnects cannot
//!   cancel a run through it.
//! - The pump subscribes right after `submit` returns; the scheduler registers
//!   the run synchronously inside `submit`, but on a multi-thread runtime the
//!   spawned loop task can in principle emit before the pump attaches. The
//!   window is a retry-bounded best effort; a replay seam in the scheduler
//!   channel would close it for good.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness_builder::AssembledRuntime;
use harness_core::RunEvent;
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunContext, RunId, SessionId, Task};
use harness_core::tool::ToolRequest;
use harness_scheduler::bus::ReceiveError;
use harness_scheduler::{EventBus, RunRequest, RunStatus, Scheduler, Subscription};
use harness_transport::{CommandHandler, EventLog, SlashCommand, TransportError};

/// Per-run retention cap of the in-memory event log (trimmed from the head).
const EVENT_LOG_CAP: usize = 10_000;
/// Bounded retries waiting for a run's bus channel to appear.
const SUBSCRIBE_RETRIES: usize = 100;
/// Delay between bus-channel retries.
const SUBSCRIBE_RETRY_DELAY: Duration = Duration::from_millis(1);

/// Recover a poisoned `std` mutex instead of panicking into the caller: the
/// guarded data is plain state, and a panic in one request must not wedge the
/// daemon.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Unix epoch milliseconds.
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// In-memory per-run event log backing the transport [`EventLog`] seam.
///
/// v1 keeps events in process memory; SQLite-backed replay through the session
/// crate (#12) is the follow-up. Retention is capped per run and trimmed from
/// the head so a long-lived daemon stays bounded.
#[derive(Default)]
pub struct InMemoryEventLog {
    events: Mutex<HashMap<RunId, Vec<RunEvent>>>,
}

impl InMemoryEventLog {
    /// Create an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    fn append(&self, run_id: &RunId, event: RunEvent) {
        let mut events = lock(&self.events);
        let bucket = events.entry(run_id.clone()).or_default();
        if bucket.len() >= EVENT_LOG_CAP {
            bucket.remove(0);
        }
        bucket.push(event);
    }
}

impl EventLog for InMemoryEventLog {
    fn read_events_since(&self, run_id: &RunId, after_seq: u64) -> Vec<RunEvent> {
        let events = lock(&self.events);
        events
            .get(run_id)
            .map(|bucket| {
                bucket
                    .iter()
                    .filter(|event| event.seq > after_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The daemon's v1 tool policy: allow every call, attributed so logs name the
/// decider. The builder deliberately has no fallback — this is the host
/// injecting its choice. A config-driven policy chain replaces it when the
/// daemon grows permissions.
pub struct DaemonPolicy;

impl ToolPolicy for DaemonPolicy {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::Allow,
            reason: String::new(),
            source: "daemon-allow-all".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// The daemon's [`CommandHandler`]: dispatches slash commands onto the
/// scheduler and pumps run events into the transport broadcast channel.
pub struct DaemonHandler {
    scheduler: Scheduler,
    runtime: OnceLock<AssembledRuntime>,
    event_tx: OnceLock<tokio::sync::broadcast::Sender<RunEvent>>,
    runs: Mutex<Vec<RunId>>,
    log: Arc<InMemoryEventLog>,
    run_counter: AtomicU64,
}

impl DaemonHandler {
    /// Create the handler; the runtime and the event channel are wired right
    /// after construction, before the transports start listening.
    pub fn new(scheduler: Scheduler, log: Arc<InMemoryEventLog>) -> Self {
        Self {
            scheduler,
            runtime: OnceLock::new(),
            event_tx: OnceLock::new(),
            runs: Mutex::new(Vec::new()),
            log,
            run_counter: AtomicU64::new(0),
        }
    }

    /// Wire the assembled runtime (called once at startup).
    pub fn set_runtime(&self, runtime: AssembledRuntime) {
        let _ = self.runtime.set(runtime);
    }

    /// Wire the transport broadcast channel (called once at startup).
    pub fn set_event_tx(&self, tx: tokio::sync::broadcast::Sender<RunEvent>) {
        let _ = self.event_tx.set(tx);
    }

    /// Cancel every running run; returns how many were cancelled. Used by the
    /// graceful shutdown drain.
    pub fn cancel_all_running(&self) -> usize {
        let runs = lock(&self.runs);
        runs.iter()
            .filter(|id| {
                matches!(self.scheduler.status(id), Some(RunStatus::Running))
                    && self.scheduler.cancel(id)
            })
            .count()
    }

    /// Whether any submitted run is still running (the drain's wait predicate).
    pub fn has_running(&self) -> bool {
        let runs = lock(&self.runs);
        runs.iter()
            .any(|id| matches!(self.scheduler.status(id), Some(RunStatus::Running)))
    }

    fn fresh_run_id(&self) -> String {
        let seq = self.run_counter.fetch_add(1, Ordering::Relaxed);
        format!("run-{}-{seq}", unix_millis())
    }

    fn start_run(&self, task: String) -> Result<serde_json::Value, TransportError> {
        let runtime = self
            .runtime
            .get()
            .ok_or_else(|| TransportError::Internal("runtime not assembled yet".into()))?;
        let run_id = RunId(self.fresh_run_id());
        let ctx = RunContext {
            run_id: run_id.clone(),
            session_id: SessionId("default".into()),
            task: Task {
                description: task,
                source: None,
                deadline: None,
            },
            limits: runtime.limits,
            started_at_ms: unix_millis(),
        };
        let deps = runtime.loop_deps();
        self.scheduler
            .submit(RunRequest { ctx, deps })
            .map_err(|err| TransportError::Internal(format!("submit failed: {err}")))?;
        lock(&self.runs).push(run_id.clone());
        self.spawn_pump(run_id.clone());
        Ok(serde_json::json!({ "run_id": run_id.0 }))
    }

    fn spawn_pump(&self, run_id: RunId) {
        let Some(tx) = self.event_tx.get().cloned() else {
            eprintln!("daemon: event channel missing; run {run_id} events are dropped");
            return;
        };
        let bus = self.scheduler.events().clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let Some(mut sub) = wait_for_channel(&bus, &run_id).await else {
                eprintln!("daemon: run {run_id} never registered on the bus; events lost");
                return;
            };
            loop {
                match sub.recv().await {
                    Ok(event) => {
                        log.append(&run_id, event.clone());
                        let _ = tx.send(event);
                    }
                    Err(ReceiveError::Lagged(skipped)) => {
                        eprintln!("daemon: run {run_id} pump lagged, {skipped} events dropped");
                    }
                    Err(ReceiveError::Closed) => break,
                }
            }
        });
    }

    fn cancel(&self, run_id: &str) -> Result<serde_json::Value, TransportError> {
        let run_id = RunId(run_id.to_string());
        if !self.scheduler.cancel(&run_id) {
            return Err(TransportError::NoActiveRun(run_id.0));
        }
        Ok(serde_json::json!({ "run_id": run_id.0, "cancelled": true }))
    }

    fn status(&self, run_id: &str) -> Result<serde_json::Value, TransportError> {
        let run_id = RunId(run_id.to_string());
        let status = self
            .scheduler
            .status(&run_id)
            .ok_or_else(|| TransportError::NoActiveRun(run_id.0.clone()))?;
        Ok(serde_json::json!({ "run_id": run_id.0, "status": status_label(&status) }))
    }

    fn list(&self) -> serde_json::Value {
        let runs = lock(&self.runs);
        let items = runs
            .iter()
            .filter_map(|id| {
                self.scheduler.status(id).map(
                    |status| serde_json::json!({ "run_id": id.0, "status": status_label(&status) }),
                )
            })
            .collect::<Vec<_>>();
        serde_json::json!({ "runs": items })
    }
}

impl CommandHandler for DaemonHandler {
    fn handle(&self, cmd: SlashCommand) -> Result<serde_json::Value, TransportError> {
        match cmd {
            SlashCommand::Run { task } => self.start_run(task),
            SlashCommand::Cancel { run_id } => self.cancel(&run_id),
            SlashCommand::Status { run_id } => self.status(&run_id),
            SlashCommand::List => Ok(self.list()),
            SlashCommand::Steer { run_id, .. } => Err(TransportError::Internal(format!(
                "steering run {run_id} is not wired yet: the scheduler does not expose steering in v1"
            ))),
        }
    }
}

/// Wait (bounded) for the run's bus channel to appear after a synchronous
/// `submit` — registration happens inside `submit`, so the first attempt
/// normally hits.
async fn wait_for_channel(bus: &EventBus, run_id: &RunId) -> Option<Subscription> {
    for _ in 0..SUBSCRIBE_RETRIES {
        if let Some(sub) = bus.subscribe(run_id) {
            return Some(sub);
        }
        tokio::time::sleep(SUBSCRIBE_RETRY_DELAY).await;
    }
    None
}

/// Human-facing status label for a run.
fn status_label(status: &RunStatus) -> String {
    match status {
        RunStatus::Running => "running".into(),
        RunStatus::Queued { position } => format!("queued:{position}"),
        RunStatus::Finished(reason) => format!("finished:{reason:?}"),
    }
}
