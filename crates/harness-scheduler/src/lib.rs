//! Harness run scheduler.
//!
//! One daemon, one active run (v1): a request either starts immediately, waits
//! in a bounded queue, or is **rejected** — a full queue never silently drops a
//! run. Each run gets its own Tokio task and its own [`RunControl`] handle, so
//! cancellation is per-run and observed at the loop's step boundaries.
//!
//! # Design provenance
//!
//! - **pi** (`packages/durable/src/harness/scheduler.ts`) — the task-graph notion
//!   of "scheduled work with lifecycle state". v1 keeps the queue, not the graph.
//! - **NanoHarness** (`extensions/scheduler/`) — the scheduling semantics. Its
//!   polling thread is the negative lesson: everything here is async and woken
//!   by Tokio, never by a sleep loop.
//! - **harness-rs** — its self-documented one-way, loop-scoped cancellation
//!   token (`harness-loop/src/lib.rs:870-891`) is not repeated: every run owns a
//!   proper [`RunControl`] held by this scheduler.
//!
//! # The three debts this crate pays
//!
//! 1. **`RunEvent.seq`** — assigned by the emitter (the loop, per run). The
//!    scheduler's job is to *preserve* monotonicity across the fan-out, not to
//!    renumber: re-assigning here would break the loop's per-run counter. Tests
//!    pin that a multiplexed stream stays gap-free per run.
//! 2. **Drop protocol** — when a run's last subscriber disappears, the run is
//!    cancelled (nanoharness `core/engine.py:378`: the caller dropped the stream,
//!    so nobody is listening). [`bus::EventBus`] owns that observation.
//! 3. **Approval mid-ask race** — a real-runtime test in this crate cancels a run
//!    while an [`ApprovalBroker`](harness_core::policy::ApprovalBroker) question
//!    is open.
//!
//! # Back-pressure
//!
//! The event bus is a bounded `broadcast` channel per run. Emitting never
//! blocks: a lagging subscriber observes `Lagged(n)` and the bus counts dropped
//! events for metrics. The loop is never held up by a slow reader — the pi
//! `processEvents` lesson (`agent.ts:619-621`).

pub mod bus;
pub mod queue;
pub mod scheduler;

pub use bus::{BusSink, EventBus, Subscription};
pub use queue::{QueuedRun, RunRequest};
pub use scheduler::{RunStatus, ScheduleError, Scheduler};
