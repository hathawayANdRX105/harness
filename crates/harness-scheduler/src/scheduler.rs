//! The scheduler: one active run per session, a bounded queue per session, and
//! a global concurrency cap.
//!
//! # Lifecycle
//!
//! ```text
//! submit ──► (session free & global slot) spawn task ──► Running
//!        └─► (session busy or cap reached)  queue or reject (QueueFull)
//! Running ──► task ends ──► Finished(reason) ──► launch round admits the
//!        ┘                                              oldest queued ticket
//! ```
//! Same-session runs serialize; different sessions run concurrently up to the
//! global cap. When a run finishes, queued runs are admitted oldest-ticket-first
//! across sessions whose own slot is still free.
//
//! Zombie reclamation is structural: the spawned task owns its registry entry,
//! and the completion watcher is the only thing that removes it. There is no
//! reaper thread to forget to run.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::control::RunControl;
use harness_core::run::{RunId, RunStopReason, SessionId};
use harness_loop::{LoopDeps, run as run_loop};
use parking_lot::Mutex;

use crate::bus::{BusSink, EventBus};
use crate::queue::{BoundedQueue, RunRequest};

/// Why a submission did not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    /// The queue is at capacity. Rejection is explicit: the caller knows the
    /// run will not start, and nothing was dropped silently.
    #[error("run queue is full ({capacity} waiting)")]
    QueueFull {
        /// Configured capacity, for the caller's message.
        capacity: usize,
    },
    /// A run with this id is already active, queued, or finished.
    #[error("run `{0}` is already scheduled")]
    DuplicateRun(String),
}

/// Where a run is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    /// Owns the active slot; its Tokio task is running.
    Running,
    /// Waiting in its session's queue behind that session's active run.
    Queued {
        /// Position in the queue, 0-based.
        position: usize,
    },
    /// Ended with this reason.
    Finished(RunStopReason),
}

/// One active run's bookkeeping.
struct ActiveRun {
    control: RunControl,
}

struct State {
    /// At most one active run per session key.
    active: HashMap<SessionId, (RunId, ActiveRun)>,
    /// One bounded FIFO queue per session; created on demand when a run
    /// for that session has to wait.
    queues: HashMap<SessionId, BoundedQueue>,
    finished: HashMap<RunId, RunStopReason>,
    /// Global submission counter: every queued run carries the ticket it got
    /// here, so the launch round admits the oldest SUBMISSION across sessions
    /// (per-queue counters would not be comparable between sessions).
    next_ticket: u64,
}

struct Inner {
    state: Mutex<State>,
    bus: EventBus,
    /// Maximum active runs across all sessions; clamped to at least 1.
    max_concurrency: usize,
    /// Capacity of each per-session queue; clamped to at least 1.
    queue_capacity: usize,
    /// Monotonic per-scheduler counter for measuring run turnover; not the
    /// event `seq` (the loop owns that per run).
    started_total: AtomicU64,
}

/// The scheduler handle.
///
/// Cloneable and `Send + Sync`: every clone drives the same daemon state.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Inner>,
}

impl Scheduler {
    /// Create a scheduler with a global concurrency cap and a per-session queue
    /// capacity. Both are clamped to at least 1 — zero is meaningless for either.
    pub fn new(queue_capacity: usize, max_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    active: HashMap::new(),
                    queues: HashMap::new(),
                    finished: HashMap::new(),
                    next_ticket: 0,
                }),
                bus: EventBus::new(),
                max_concurrency: max_concurrency.max(1),
                queue_capacity: queue_capacity.max(1),
                started_total: AtomicU64::new(0),
            }),
        }
    }

    /// The event bus every run in this scheduler publishes to.
    pub fn events(&self) -> &EventBus {
        &self.inner.bus
    }

    /// Submit a run.
    ///
    /// Starts it when this run's session has no active run and a global
    /// concurrency slot is free; otherwise enqueues it in this session's queue
    /// and rejects with `QueueFull` when the queue is full — never a silent
    /// drop. A run id is single use: active in any session, waiting in any
    /// queue, or already finished is a duplicate.
    pub fn submit(&self, request: RunRequest) -> Result<RunId, ScheduleError> {
        let run_id = request.ctx.run_id.clone();
        let session_id = request.ctx.session_id.clone();
        let mut state = self.inner.state.lock();

        if state.is_known(&run_id) || state.finished.contains_key(&run_id) {
            return Err(ScheduleError::DuplicateRun(run_id.0));
        }

        // This session has no active run and a global slot is free: start now.
        if !state.active.contains_key(&session_id)
            && state.active.len() < self.inner.max_concurrency
        {
            state.active.insert(
                session_id.clone(),
                (run_id.clone(), self.start_run(request)),
            );
            return Ok(run_id);
        }

        // Otherwise wait in this session's queue. A full queue is an explicit
        // rejection, so nothing is ever silently dropped. The ticket is global
        // (submission order across ALL sessions) so the launch round admits
        // oldest submissions first, not whichever session HashMap-iterates
        // first.
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        let queue = state
            .queues
            .entry(session_id)
            .or_insert_with(|| BoundedQueue::new(self.inner.queue_capacity));
        if !queue.push(request, ticket) {
            return Err(ScheduleError::QueueFull {
                capacity: queue.capacity(),
            });
        };
        Ok(run_id)
    }

    /// Cancel a run by id.
    ///
    /// An active run has its token cancelled, which the loop observes at its
    /// next step boundary; the slot is reclaimed by `finish_run` when the loop
    /// ends. A queued run is removed from its session's queue (it never
    /// started, so there is nothing to cancel).
    pub fn cancel(&self, run_id: &RunId) -> bool {
        let mut state = self.inner.state.lock();

        if let Some((_, active)) = state.active.values_mut().find(|(id, _)| id == run_id) {
            active.control.cancel();
            return true;
        }

        state.queue_remove(run_id)
    }

    /// Current status of a run, if the scheduler knows it.
    pub fn status(&self, run_id: &RunId) -> Option<RunStatus> {
        let state = self.inner.state.lock();

        if state.active.values().any(|(id, _)| id == run_id) {
            return Some(RunStatus::Running);
        }
        for queue in state.queues.values() {
            if let Some(position) = queue
                .iter()
                .position(|queued| &queued.request.ctx.run_id == run_id)
            {
                return Some(RunStatus::Queued { position });
            }
        }
        state
            .finished
            .get(run_id)
            .map(|r| RunStatus::Finished(r.clone()))
    }

    /// Number of runs waiting across all sessions (excluding active ones).
    pub fn queued(&self) -> usize {
        let state = self.inner.state.lock();
        state.queues.values().map(|queue| queue.len()).sum()
    }

    /// Total runs started since construction (metric).
    pub fn started_total(&self) -> u64 {
        self.inner.started_total.load(Ordering::Relaxed)
    }

    /// Build and spawn the task for `request`, returning its bookkeeping.
    ///
    /// The run's `RunControl` is created here — not by the caller — so the drop
    /// protocol and `cancel` both act on the handle the bus knows.
    fn start_run(&self, request: RunRequest) -> ActiveRun {
        let control = RunControl::new();
        let run_id = request.ctx.run_id.clone();
        let sink: BusSink = self.inner.bus.register(&run_id, control.clone());
        let ctx = request.ctx;
        let deps = Arc::new(LoopDeps {
            event_sink: Arc::new(sink),
            ..request.deps
        });
        let run_control = control.clone();
        let scheduler = self.clone();
        let finished_id = run_id.clone();
        self.inner.started_total.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let reason = run_loop(ctx, deps, run_control).await;
            scheduler.finish_run(&finished_id, reason);
        });

        ActiveRun { control }
    }

    /// Record the terminal reason, retire the run's bus channel, and run a
    /// launch round: while a global concurrency slot exists, admit the
    /// oldest-ticket queued run from any session whose own slot is free.
    ///
    /// The spawned task still owns its registry entry; only `finish_run`
    /// retires it, so a completion race never clobbers another run's slot.
    fn finish_run(&self, run_id: &RunId, reason: RunStopReason) {
        let mut state = self.inner.state.lock();

        // Drop this run's session slot, but only if it is still the active one.
        let session_id = state
            .active
            .iter()
            .find(|(_, (id, _))| id == run_id)
            .map(|(session_id, _)| session_id.clone());
        // Drop this run's session slot, but only if it is still the active
        // one: a completion race could have replaced it, and clobbering
        // another run's slot would leak its cancellation handle.
        if let Some(sid) = session_id {
            state.active.remove(&sid);
        }
        state.finished.insert(run_id.clone(), reason);
        self.inner.bus.retire(run_id);

        // Launch round: oldest ticket first, only from sessions with a free
        // slot, until the global cap is reached or nothing is waiting.
        while state.active.len() < self.inner.max_concurrency {
            let mut best: Option<(SessionId, u64)> = None;
            for (sid, queue) in &state.queues {
                if state.active.contains_key(sid) {
                    continue; // this session already owns the active slot
                }
                let Some(ticket) = queue.iter().next().map(|queued| queued.ticket) else {
                    continue;
                };
                match &best {
                    None => best = Some((sid.clone(), ticket)),
                    Some((_, current)) if ticket < *current => {
                        best = Some((sid.clone(), ticket));
                    }
                    _ => {}
                }
            }
            let Some(sid) = best.map(|(sid, _)| sid) else {
                break;
            };
            let Some(queued) = state.queues.get_mut(&sid).and_then(|queue| queue.pop()) else {
                break;
            };
            let id = queued.request.ctx.run_id.clone();
            state
                .active
                .insert(sid, (id, self.start_run(queued.request)));
        }

        // Drop queue entries that drained to nothing: entries are recreated on
        // demand in submit, so keeping empty ones would grow the map with every
        // session that ever waited.
        state.queues.retain(|_, queue| !queue.is_empty());
    }
}

impl State {
    /// Whether `run_id` is active in some session or waiting in some queue —
    /// run ids are single use.
    fn is_known(&self, run_id: &RunId) -> bool {
        self.active.values().any(|(id, _)| id == run_id)
            || self.queues.values().any(|queue| {
                queue
                    .iter()
                    .any(|queued| &queued.request.ctx.run_id == run_id)
            })
    }

    /// Remove `run_id` from whichever session's queue holds it, if any.
    ///
    /// A queue left empty by the removal is dropped: entries are recreated on
    /// demand, so an empty one would only leak map space.
    fn queue_remove(&mut self, run_id: &RunId) -> bool {
        let removed = self
            .queues
            .iter_mut()
            .any(|(_, queue)| queue.remove_if(|queued| &queued.request.ctx.run_id == run_id));
        self.queues.retain(|_, queue| !queue.is_empty());
        removed
    }
}
