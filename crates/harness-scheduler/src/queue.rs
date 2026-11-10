//! The bounded run queue.
//!
//! A request that does not fit is rejected with [`ScheduleError::QueueFull`](crate::ScheduleError::QueueFull).
//! Silent drops are forbidden: a caller that got `Ok` must be able to assume its
//! run will start, and a caller that got `Err` knows it did not.

use std::collections::VecDeque;

use harness_core::run::RunContext;
use harness_loop::LoopDeps;

/// One unit of scheduled work: the run identity plus the assembled loop
/// dependencies the builder (#10) produced.
pub struct RunRequest {
    /// Identity, limits and task description of the run.
    pub ctx: RunContext,
    /// Assembled loop dependencies. The scheduler replaces `event_sink` with its
    /// own bus sink so every subscriber sees the run.
    pub deps: LoopDeps,
}

/// A request waiting for the active run to finish.
pub struct QueuedRun {
    /// Monotonic ticket: submission order, used to keep the queue FIFO even
    /// though `RunId` is a caller-chosen string.
    pub ticket: u64,
    /// The queued work.
    pub request: RunRequest,
}

/// A FIFO queue with a hard capacity.
pub(crate) struct BoundedQueue {
    capacity: usize,
    next_ticket: u64,
    items: VecDeque<QueuedRun>,
}

impl BoundedQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            // A zero-capacity queue would reject every request; one slot is the
            // smallest useful queue and keeps `is_full` meaningful.
            capacity: capacity.max(1),
            next_ticket: 0,
            items: VecDeque::new(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// Append one request, assigning the next ticket.
    ///
    /// Returns the ticket, or `None` when the queue is full — the caller turns
    /// that into an explicit rejection.
    pub(crate) fn push(&mut self, request: RunRequest) -> Option<u64> {
        if self.is_full() {
            return None;
        }
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.items.push_back(QueuedRun { ticket, request });
        Some(ticket)
    }

    pub(crate) fn pop(&mut self) -> Option<QueuedRun> {
        self.items.pop_front()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &QueuedRun> {
        self.items.iter()
    }

    /// Remove every item matching `predicate`; returns whether one was removed.
    pub(crate) fn remove_if<F>(&mut self, mut predicate: F) -> bool
    where
        F: FnMut(&QueuedRun) -> bool,
    {
        let before = self.items.len();
        self.items.retain(|queued| !predicate(queued));
        self.items.len() != before
    }
}
