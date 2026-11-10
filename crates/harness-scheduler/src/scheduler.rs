//! The scheduler: one active run, a bounded queue, per-run cancellation.
//!
//! # Lifecycle
//!
//! ```text
//! submit ──► (idle)      spawn task ──► Running
//!        └─► (busy)      queue or reject (QueueFull)
//! Running ──► task ends ──► Finished(reason) ──► next queued run starts
//! ```
//!
//! Zombie reclamation is structural: the spawned task owns its registry entry,
//! and the completion watcher is the only thing that removes it. There is no
//! reaper thread to forget to run.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::control::RunControl;
use harness_core::run::{RunId, RunStopReason};
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
    /// A run with this id is already active or queued.
    #[error("run `{0}` is already scheduled")]
    DuplicateRun(String),
}

/// Where a run is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    /// Owns the active slot; its Tokio task is running.
    Running,
    /// Waiting in the queue behind the active run.
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
    active: Option<(RunId, ActiveRun)>,
    queue: BoundedQueue,
    finished: HashMap<RunId, RunStopReason>,
}

struct Inner {
    state: Mutex<State>,
    bus: EventBus,
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
    /// Create a scheduler whose queue holds at most `queue_capacity` waiting
    /// runs (a capacity of 0 is raised to 1 — a zero-length queue has no useful
    /// meaning).
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    active: None,
                    queue: BoundedQueue::new(queue_capacity),
                    finished: HashMap::new(),
                }),
                bus: EventBus::new(),
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
    /// Starts it when the scheduler is idle, enqueues it when it is busy, and
    /// rejects it when the queue is full — never a silent drop.
    pub fn submit(&self, request: RunRequest) -> Result<RunId, ScheduleError> {
        let run_id = request.ctx.run_id.clone();
        let mut state = self.inner.state.lock();

        if state.active.as_ref().is_some_and(|(id, _)| id == &run_id)
            || state.queue_contains(&run_id)
            || state.finished.contains_key(&run_id)
        {
            return Err(ScheduleError::DuplicateRun(run_id.0));
        }

        if state.active.is_none() {
            state.active = Some((run_id.clone(), self.start_run(request)));
            return Ok(run_id);
        }

        let Some(_ticket) = state.queue.push(request) else {
            return Err(ScheduleError::QueueFull {
                capacity: state.queue.capacity(),
            });
        };
        Ok(run_id)
    }

    /// Cancel a run by id.
    ///
    /// A queued run is removed (it never started, so there is nothing to
    /// cancel); an active run has its token cancelled, which the loop observes
    /// at its next step boundary.
    pub fn cancel(&self, run_id: &RunId) -> bool {
        let mut state = self.inner.state.lock();

        if let Some((id, active)) = state.active.as_ref()
            && id == run_id
        {
            active.control.cancel();
            return true;
        }

        state.queue_remove(run_id)
    }

    /// Current status of a run, if the scheduler knows it.
    pub fn status(&self, run_id: &RunId) -> Option<RunStatus> {
        let state = self.inner.state.lock();

        if let Some((id, _)) = state.active.as_ref()
            && id == run_id
        {
            return Some(RunStatus::Running);
        }
        if let Some(position) = state.queue_position(run_id) {
            return Some(RunStatus::Queued { position });
        }
        state
            .finished
            .get(run_id)
            .map(|r| RunStatus::Finished(r.clone()))
    }

    /// Number of runs waiting (excluding the active one).
    pub fn queued(&self) -> usize {
        self.inner.state.lock().queue.len()
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

    /// Record the terminal reason, retire the run's bus channel, and start the
    /// next queued run.
    fn finish_run(&self, run_id: &RunId, reason: RunStopReason) {
        let next = {
            let mut state = self.inner.state.lock();
            // Only clear the active slot if it is still ours: a cancellation race
            // could have replaced it, and clobbering another run's slot would
            // leak its cancellation handle.
            let is_ours = state.active.as_ref().is_some_and(|(id, _)| id == run_id);
            if is_ours {
                state.active = None;
            }
            state.finished.insert(run_id.clone(), reason);
            self.inner.bus.retire(run_id);
            if is_ours { state.queue.pop() } else { None }
        };

        if let Some(queued) = next {
            let mut state = self.inner.state.lock();
            let id = queued.request.ctx.run_id.clone();
            state.active = Some((id, self.start_run(queued.request)));
        }
    }
}

impl State {
    fn queue_contains(&self, run_id: &RunId) -> bool {
        self.queue_position(run_id).is_some()
    }

    fn queue_position(&self, run_id: &RunId) -> Option<usize> {
        self.queue
            .iter()
            .position(|queued| &queued.request.ctx.run_id == run_id)
    }

    fn queue_remove(&mut self, run_id: &RunId) -> bool {
        self.queue
            .remove_if(|queued| &queued.request.ctx.run_id == run_id)
    }
}
