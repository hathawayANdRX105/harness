//! The daemon's transport seams: the real [`CommandHandler`] and the v1
//! [`EventLog`].
/// DaemonHandler maps slash commands onto the scheduler: `run` submits a
/// run assembled by the builder, `cancel` / `status` / `list` read scheduler
/// state, `steer` is rejected until the scheduler exposes steering. Events
/// flow loop → bus → a per-run pump task that appends into the session store
/// and forwards into the transport broadcast channel, so every transport
/// client sees every event.
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness_builder::AssembledRuntime;
use harness_core::RunEvent;
use harness_core::event::RunEventKind;
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunContext, RunId, SessionId, Task};
use harness_core::tool::ToolRequest;
use harness_scheduler::bus::ReceiveError;
use harness_scheduler::{EventBus, RunRequest, RunStatus, Scheduler, Subscription};
use harness_session::{RecallHit, RecallScope, RecallStore, SessionStore, SessionStoreError};
use harness_transport::{CommandHandler, EventLog, SlashCommand, TransportError};

/// Delay between bus-channel polls while a queued run has no channel yet.
const BUS_CHANNEL_POLL: Duration = Duration::from_millis(10);

/// Maximum hits the `search` command returns: bounds the response and the
/// FTS scan.
const DEFAULT_SEARCH_LIMIT: u32 = 20;

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

/// Transport [`EventLog`] backed by the persistent [`SessionStore`]: replays
/// past runs' events from the session store so reconnection survives daemon
/// restarts. One store backs both the handler's bookkeeping appends and this
/// seam's reads, so there is a single source of truth for the event log.
pub struct StoreEventLog {
    store: Arc<dyn SessionStore>,
}

impl StoreEventLog {
    /// Wrap a session store as the transport [`EventLog`] seam.
    pub fn new(store: Arc<dyn SessionStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl EventLog for StoreEventLog {
    /// Return the run's persisted events with `seq` strictly greater than
    /// `after_seq`, in ascending order.  Read failures yield an empty replay
    /// window rather than failing the connection.
    async fn read_events_since(&self, run_id: &RunId, after_seq: u64) -> Vec<RunEvent> {
        self.store
            .events_for_run(run_id)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .filter(|event| event.seq > after_seq)
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
    runs: Arc<Mutex<Vec<RunId>>>,
    store: Arc<dyn SessionStore>,
    recall: Arc<dyn RecallStore>,
    run_counter: AtomicU64,
}

impl DaemonHandler {
    /// Create the handler; the runtime and the event channel are wired right
    /// after construction, before the transports start listening.
    ///
    /// `recall` is the search capability: a `TursoSessionStore`-backed
    /// [`RecallStore`] for hosts that opted into recall, [`NoRecall`]
    /// otherwise. The handler never constructs it — the host (main.rs) knows
    /// which store it opened.
    pub fn new(
        scheduler: Scheduler,
        store: Arc<dyn SessionStore>,
        recall: Arc<dyn RecallStore>,
    ) -> Self {
        Self {
            scheduler,
            runtime: OnceLock::new(),
            event_tx: OnceLock::new(),
            runs: Arc::new(Mutex::new(Vec::new())),
            store,
            recall,
            run_counter: AtomicU64::new(0),
        }
    }

    /// Wire the assembled runtime (called once at startup).
    pub fn set_runtime(&self, runtime: AssembledRuntime) {
        if self.runtime.set(runtime).is_err() {
            eprintln!("daemon: runtime already wired; a second set_runtime is a bug, ignored");
        }
    }

    /// Wire the transport broadcast channel (called once at startup).
    pub fn set_event_tx(&self, tx: tokio::sync::broadcast::Sender<RunEvent>) {
        if self.event_tx.set(tx).is_err() {
            eprintln!("daemon: event tx already wired; a second set_event_tx is a bug, ignored");
        }
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
        let session_id = SessionId("default".into());
        // Ensure the session row exists before the run starts: the session is
        // the run's durable anchor across restarts; re-creation is idempotent.
        // The CommandHandler seam is sync, so drive the async store call on
        // the current thread without a tokio reactor (block_in_place).
        match tokio::task::block_in_place(|| {
            futures::executor::block_on(self.store.create_session(&session_id))
        }) {
            Ok(()) => {}
            Err(SessionStoreError::SessionAlreadyExists(_)) => {}
            Err(e) => {
                return Err(TransportError::Internal(format!(
                    "ensure session failed: {e}"
                )));
            }
        }
        let ctx = RunContext {
            run_id: run_id.clone(),
            session_id,
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
        let scheduler = self.scheduler.clone();
        let store = self.store.clone();
        let runs = self.runs.clone();
        tokio::spawn(async move {
            let Some(mut sub) = wait_for_channel(&bus, &scheduler, &run_id).await else {
                eprintln!("daemon: run {run_id} never registered on the bus; events lost");
                Self::prune_run(&runs, &run_id);
                return;
            };
            loop {
                match sub.recv().await {
                    Ok(event) => {
                        persist_event(&store, &run_id, &event).await;
                        let _ = tx.send(event);
                    }
                    Err(ReceiveError::Lagged(skipped)) => {
                        eprintln!("daemon: run {run_id} pump lagged, {skipped} events dropped");
                    }
                    Err(ReceiveError::Closed) => break,
                }
            }
            // The loop emits `RunFinished` (carrying the stop reason) on every
            // terminal path and the pump persisted it above, so Close only
            // drops this run from in-memory bookkeeping.
            Self::prune_run(&runs, &run_id);
        });
    }

    /// Drop a finished run from the handler's run bookkeeping: its pump has
    /// ended (the subscription closed), so the run reached a terminal status.
    fn prune_run(runs: &Mutex<Vec<RunId>>, run_id: &RunId) {
        let mut rs = lock(runs);
        rs.retain(|id| id != run_id);
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
        if let Some(status) = self.scheduler.status(&run_id) {
            return Ok(serde_json::json!({ "run_id": run_id.0, "status": status_label(&status) }));
        }
        // Not known to the scheduler (e.g. after a restart): a finished run's
        // events survive in the store, so report its last recorded status.
        let events = match tokio::task::block_in_place(|| {
            futures::executor::block_on(self.store.events_for_run(&run_id))
        }) {
            Ok(events) => events,
            Err(e) => {
                return Err(TransportError::Internal(format!(
                    "load run events failed: {e}"
                )));
            }
        };
        let event = match events.last() {
            Some(event) => event.clone(),
            None => return Err(TransportError::NoActiveRun(run_id.0)),
        };
        Ok(serde_json::json!({
            "run_id": run_id.0,
            "status": event_status_label(&event.kind),
        }))
    }

    fn list(&self) -> serde_json::Value {
        let runs = lock(&self.runs);
        let mut items: Vec<serde_json::Value> = runs
            .iter()
            .filter_map(|id| {
                self.scheduler.status(id).map(
                    |status| serde_json::json!({ "run_id": id.0, "status": status_label(&status) }),
                )
            })
            .collect();
        // Historical runs: runs whose events survived in the store but are no
        // longer tracked in memory (after a restart, for example).  Merge
        // them in so `list` shows past runs across restarts.
        let historical =
            tokio::task::block_in_place(|| futures::executor::block_on(self.store.run_ids()))
                .unwrap_or_default();
        for id in historical {
            if runs.iter().any(|active| active == &id) {
                continue;
            }
            items.push(serde_json::json!({
                "run_id": id.0,
                "status": historical_status(&self.store, &id),
            }));
        }
        serde_json::json!({ "runs": items })
    }

    /// Search persisted run events through the [`RecallStore`] seam.
    ///
    /// # WHY
    ///
    /// Tantivy's default `fts_score` ranks lower = more relevant (the score
    /// semantics are documented on [`RecallHit::score`]), so the response
    /// hits are ordered by score ascending: the most relevant hit is first,
    /// independent of the backend's own ordering. The search limit comes
    /// from [`DEFAULT_SEARCH_LIMIT`] so a broad query cannot blow up the
    /// response.
    fn search(&self, query: &str) -> Result<serde_json::Value, TransportError> {
        let hits = tokio::task::block_in_place(|| {
            futures::executor::block_on(
                self.recall
                    .search(query, &RecallScope::all(DEFAULT_SEARCH_LIMIT)),
            )
        })
        .map_err(|e| TransportError::Internal(format!("search failed: {e}")))?;
        // WHY: lower fts_score = more relevant, so ascending order puts the
        // best hit first regardless of how the backend ranked the rows.
        let mut ordered: Vec<RecallHit> = hits;
        ordered.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(CmpOrdering::Equal));
        let hits: Vec<serde_json::Value> = ordered
            .into_iter()
            .map(|hit| {
                serde_json::json!({
                    "run_id": hit.run_id.0,
                    "session_id": hit.session_id.0,
                    "seq": hit.seq,
                    "score": hit.score,
                    "snippet": hit.snippet,
                })
            })
            .collect();
        Ok(serde_json::json!({ "query": query, "hits": hits }))
    }
}

impl CommandHandler for DaemonHandler {
    fn handle(&self, cmd: SlashCommand) -> Result<serde_json::Value, TransportError> {
        match cmd {
            SlashCommand::Run { task } => self.start_run(task),
            SlashCommand::Cancel { run_id } => self.cancel(&run_id),
            SlashCommand::Status { run_id } => self.status(&run_id),
            SlashCommand::List => Ok(self.list()),
            SlashCommand::Search { query } => self.search(&query),
            SlashCommand::Steer { run_id, .. } => Err(TransportError::Internal(format!(
                "steering run {run_id} is not wired yet: the scheduler does not expose steering in v1"
            ))),
        }
    }
}

/// Wait for the run's bus channel to appear, retrying until the run is
/// terminal: a queued run has no channel until it activates, so a fixed
/// retry cap would silently lose all of its events.
async fn wait_for_channel(
    bus: &EventBus,
    scheduler: &Scheduler,
    run_id: &RunId,
) -> Option<Subscription> {
    loop {
        if let Some(sub) = bus.subscribe(run_id) {
            return Some(sub);
        }
        if matches!(scheduler.status(run_id), Some(RunStatus::Finished(_))) {
            return None;
        }
        tokio::time::sleep(BUS_CHANNEL_POLL).await;
    }
}

/// Human-facing status label for a run.
fn status_label(status: &RunStatus) -> String {
    match status {
        RunStatus::Running => "running".into(),
        RunStatus::Queued { position } => format!("queued:{position}"),
        RunStatus::Finished(reason) => format!("finished:{reason:?}"),
    }
}

/// Persist one event to the session store.  A persistence failure is logged
/// and swallowed: it must not drop the live broadcast, and event sequence
/// numbers are owned by the loop, so a retry would risk a duplicate-seq
/// error against the append-only log.
async fn persist_event(store: &Arc<dyn SessionStore>, run_id: &RunId, event: &RunEvent) {
    let store = store.clone();
    let run_id = run_id.clone();
    let event = event.clone();
    match store.append_event(&event).await {
        Ok(()) => {}
        Err(e) => eprintln!("daemon: failed to persist event for run {run_id}: {e}"),
    }
}

/// Last recorded status of a historical run, derived from its final event.
/// The loop emits `RunFinished` (carrying the stop reason) on every terminal
/// path, so a run present in the store but no longer scheduled is reported
/// as finished; an unreachable run is reported as unknown.
fn historical_status(store: &Arc<dyn SessionStore>, run_id: &RunId) -> String {
    let event = match tokio::task::block_in_place(|| {
        futures::executor::block_on(store.events_for_run(run_id))
    }) {
        Ok(events) => match events.last() {
            Some(event) => event.clone(),
            None => return "finished:unknown".into(),
        },
        Err(_) => return "finished:unknown".into(),
    };
    event_status_label(&event.kind)
}

/// Human-facing label for the last event of a historical run.
fn event_status_label(kind: &RunEventKind) -> String {
    match kind {
        RunEventKind::RunFinished { reason } => format!("finished:{reason:?}"),
        _ => "finished".into(),
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::process::id;

    use harness_core::event::RunEventKind;
    use harness_session::turso::TursoSessionStore;

    use super::*;

    static SEARCH_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Open a temp store with one session and two runs' events, each carrying
    /// a unique marker text in its payload.  A unique temp dir per call keeps
    /// parallel tests from contending on one turso file lock.
    async fn search_fixture() -> std::sync::Arc<TursoSessionStore> {
        let seq = SEARCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!("harness-search-{}-{seq}", id()));
        let _ = fs::create_dir_all(&dir);
        let store = TursoSessionStore::open(dir.join("search.db"))
            .await
            .unwrap_or_else(|e| panic!("open store: {e}"));
        let session = SessionId("search-session".into());
        store
            .create_session(&session)
            .await
            .unwrap_or_else(|e| panic!("create session: {e}"));
        for (run, marker) in [
            ("search-alpha", "daemon-search-marker-alpha"),
            ("search-beta", "daemon-search-marker-beta"),
        ] {
            let run_id = RunId(run.into());
            for seq in 0..2u64 {
                let event = RunEvent {
                    run_id: run_id.clone(),
                    session_id: session.clone(),
                    seq,
                    timestamp_ms: 0,
                    kind: RunEventKind::TextDelta {
                        text: format!("{marker} event {seq}"),
                    },
                };
                store
                    .append_event(&event)
                    .await
                    .unwrap_or_else(|e| panic!("append event: {e}"));
            }
        }
        std::sync::Arc::new(store)
    }

    /// Purpose: `DaemonHandler::handle(Search)` queries the recall seam and
    /// returns JSON hits.
    /// Scenario: a store holds marker events; the handler is built with
    /// `store.recall()` and handles `SlashCommand::Search` for a present term
    /// and an absent term.
    /// Acceptance: the present term returns exactly the marker events, every
    /// hit carries run_id/session_id/seq/score/snippet, hits are ordered by
    /// score ascending, and the absent term returns an empty hit list.
    #[tokio::test(flavor = "multi_thread")]
    async fn daemon_search_returns_marker_events_as_json_hits()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = search_fixture().await;
        let recall: Arc<dyn RecallStore> = Arc::new(store.recall());
        let scheduler = Scheduler::new(4);
        let handler = DaemonHandler::new(scheduler, store, recall);

        let value = match CommandHandler::handle(
            &handler,
            SlashCommand::Search {
                query: "daemon-search-marker-alpha".into(),
            },
        ) {
            Ok(value) => value,
            Err(err) => return Err(format!("search failed: {err}").into()),
        };
        assert_eq!(
            value.get("query").and_then(|v| v.as_str()),
            Some("daemon-search-marker-alpha"),
            "the response must echo the query"
        );
        let hits = value
            .get("hits")
            .and_then(|v| v.as_array())
            .ok_or("search response is missing a hits array")?;
        assert_eq!(hits.len(), 2, "both alpha-marked events must match");
        assert!(
            hits.iter()
                .all(|hit| { hit.get("run_id").and_then(|v| v.as_str()) == Some("search-alpha") }),
            "every alpha hit must come from the alpha run"
        );
        let mut scores: Vec<f64> = Vec::new();
        for hit in hits {
            assert!(
                hit.get("session_id").is_some()
                    && hit.get("seq").is_some()
                    && hit.get("score").is_some()
                    && hit.get("snippet").is_some(),
                "every hit must carry session_id, seq, score and snippet"
            );
            scores.push(
                hit.get("score")
                    .and_then(|v| v.as_f64())
                    .ok_or("hit score is missing")?,
            );
        }
        assert!(
            scores.windows(2).all(|pair| pair[0] <= pair[1]),
            "hits must be ordered by score ascending (lower = more relevant)"
        );

        let empty = match CommandHandler::handle(
            &handler,
            SlashCommand::Search {
                query: "__no_such_marker_term__".into(),
            },
        ) {
            Ok(value) => value,
            Err(err) => return Err(format!("search failed: {err}").into()),
        };
        let empty_hits = empty
            .get("hits")
            .and_then(|v| v.as_array())
            .ok_or("search response is missing a hits array")?;
        assert!(empty_hits.is_empty(), "an absent term must return no hits");
        Ok(())
    }
}
