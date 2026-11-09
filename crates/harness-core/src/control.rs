//! Run control: await-able cancellation plus a steering mailbox.
//!
//! Design provenance: nanoharness's `RunControl` (`core/runtime.py`, 47 lines)
//! contributes the shape — `cancel` observed cooperatively, `steer` as a
//! mailbox drained by the loop, steering messages replayed as user-role
//! messages before the next model turn (the injection point in
//! `core/engine.py:194-203`). Two deliberate upgrades for the daemon:
//!
//! - cancellation is a `tokio_util::sync::CancellationToken`, so it is
//!   *await-able* at `.await` points inside tools and the model stream, not
//!   just pollable at step boundaries — harness-rs documents its own one-way,
//!   loop-scoped token as wrong for a multi-turn server
//!   (`crates/harness-loop/src/lib.rs:870-891`), and we inherit neither the
//!   limitation nor the workaround.
//! - steering is a plain FIFO queue behind a lock; the loop owns the moment
//!   of injection, so a daemon can steer a run from any transport.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

/// One steering message queued for a run. `message` is empty-string-hostile:
/// an empty steer is dropped on arrival rather than injected as a user turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Steer {
    pub message: String,
}

/// Cooperative run control, owned once per run and shared by value (`Clone`
/// is cheap — the token is `Arc`-backed, the mailbox behind one lock).
///
/// The loop checks `is_cancelled` at turn boundaries and races
/// `cancelled().await` against awaits inside tools; transport clients call
/// `steer` from any connection.
#[derive(Debug, Clone)]
pub struct RunControl {
    cancel: CancellationToken,
    steering: Arc<SteeringMailbox>,
}

#[derive(Debug, Default)]
struct SteeringMailbox {
    queue: Mutex<VecDeque<Steer>>,
}

impl RunControl {
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            steering: Arc::new(SteeringMailbox::default()),
        }
    }

    /// Cancel the run. Observed at the next `await` point or step boundary —
    /// never preempts a running tool mid-flight; long tools race the token
    /// themselves.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Poll-style view of cancellation (step boundaries, sync tools).
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The await-able view of cancellation: the token a caller races against
    /// model streams, approval asks and tool bodies. Named for the handle it
    /// returns, not a predicate — the poll view is `is_cancelled`.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Queue a steering message for the run; empty messages are dropped —
    /// nanoharness raises `ValueError` on the same input, we silently skip,
    /// because the queue is fed by untrusted transport input.
    pub fn steer(&self, message: impl Into<String>) {
        let text = message.into();
        if text.trim().is_empty() {
            return;
        }
        self.steering
            .queue
            .lock()
            .push_back(Steer { message: text });
    }

    /// Drain the mailbox, FIFO. The loop calls this before each model turn
    /// and replays each message as a user-role turn.
    pub fn drain_steering(&self) -> Vec<Steer> {
        self.steering.queue.lock().drain(..).collect::<Vec<_>>()
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self::new()
    }
}
