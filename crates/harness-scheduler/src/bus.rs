//! Per-run event fan-out with a bounded, non-blocking channel.
//!
//! [`EventBus`] owns one `tokio::sync::broadcast` sender per run. The loop emits
//! through [`BusSink`], which is an [`EventSink`] — fire-and-forget by contract.
//! Subscribers get a [`Subscription`]; when the last one is dropped, the bus
//! cancels the run (the drop protocol).
//!
//! # Overflow policy
//!
//! `broadcast` is bounded. A subscriber that falls behind receives
//! `RecvError::Lagged(n)` and the bus increments [`EventBus::dropped_events`].
//! The emitter never waits: `send` either fans out or reports "nobody is
//! listening", and the scheduler treats the latter as the drop protocol's cue.
//! That is the deliberate difference from pi's `processEvents`, which awaited
//! every listener in the loop's critical path.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::control::RunControl;
use harness_core::event::{EventSink, RunEvent};
use harness_core::run::RunId;
use parking_lot::Mutex;
use tokio::sync::broadcast;

/// Capacity of one run's broadcast channel.
///
/// Events are small and usually consumed promptly; 256 is deep enough for a
/// burst (a tool batch plus deltas) without letting a dead subscriber pin an
/// unbounded amount of memory.
pub const BUS_CAPACITY: usize = 256;

/// One run's channel plus the subscriber accounting the drop protocol needs.
struct RunChannel {
    tx: broadcast::Sender<RunEvent>,
    subscribers: usize,
    /// Cancelling this control handle ends the run when the last subscriber
    /// leaves.
    control: RunControl,
}

/// The scheduler-owned event bus.
///
/// Cloneable: every clone shares the same channels and counters.
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Mutex<HashMap<RunId, RunChannel>>>,
    dropped: Arc<AtomicU64>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Register a run and return the sink the loop emits through.
    ///
    /// The `control` handle is the one the bus cancels when the last subscriber
    /// goes away, so it must be the same handle the run is driven with.
    pub fn register(&self, run_id: &RunId, control: RunControl) -> BusSink {
        let (tx, _rx) = broadcast::channel(BUS_CAPACITY);
        self.inner.lock().insert(
            run_id.clone(),
            RunChannel {
                tx: tx.clone(),
                subscribers: 0,
                control,
            },
        );
        BusSink {
            run_id: run_id.clone(),
            bus: self.clone(),
            tx,
        }
    }

    /// Stop routing events for a finished run and drop its channel.
    pub fn retire(&self, run_id: &RunId) {
        self.inner.lock().remove(run_id);
    }

    /// Subscribe to one run's events.
    ///
    /// Returns `None` when the run is unknown (never registered or already
    /// retired) — a caller asking about a finished run gets a clear "no", not an
    /// empty stream that never ends.
    pub fn subscribe(&self, run_id: &RunId) -> Option<Subscription> {
        let mut map = self.inner.lock();
        let channel = map.get_mut(run_id)?;
        channel.subscribers += 1;
        let rx = channel.tx.subscribe();
        Some(Subscription {
            run_id: run_id.clone(),
            bus: self.clone(),
            rx,
        })
    }

    /// Number of live subscribers for a run (0 for an unknown run).
    pub fn subscriber_count(&self, run_id: &RunId) -> usize {
        self.inner
            .lock()
            .get(run_id)
            .map_or(0, |channel| channel.subscribers)
    }

    /// Total events dropped because a subscriber lagged, across all runs.
    ///
    /// This is a metric, not a correctness problem: the run keeps going and the
    /// subscriber that lagged can recover its position from the event log.
    pub fn dropped_events(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Drop-protocol bookkeeping: one subscriber left.
    fn unsubscribe(&self, run_id: &RunId) {
        let mut map = self.inner.lock();
        let Some(channel) = map.get_mut(run_id) else {
            return;
        };
        channel.subscribers = channel.subscribers.saturating_sub(1);
        if channel.subscribers == 0 {
            // nanoharness's rule: the caller dropped the stream, so nobody can
            // observe this run — cancel it rather than burn tokens into a void.
            channel.control.cancel();
        }
    }
}

/// The [`EventSink`] a run emits through.
///
/// Holds its run's sender directly so `emit` stays a hash-map-free operation on
/// the loop's critical path.
pub struct BusSink {
    run_id: RunId,
    bus: EventBus,
    tx: broadcast::Sender<RunEvent>,
}

impl EventSink for BusSink {
    fn emit(&self, event: RunEvent) {
        // Fire-and-forget: a full channel or absent subscriber must never block
        // the loop (pi's sequential listener await is the counter-example).
        if self.tx.send(event).is_err() {
            self.bus.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl BusSink {
    /// The run this sink belongs to.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}

/// A live subscription to one run's event stream.
///
/// Dropping it decrements the run's subscriber count; dropping the **last** one
/// cancels the run (the drop protocol).
pub struct Subscription {
    run_id: RunId,
    bus: EventBus,
    rx: broadcast::Receiver<RunEvent>,
}

impl Subscription {
    /// Receive the next event.
    ///
    /// `Ok(None)` means the channel closed (the run retired). A lagging
    /// subscriber gets `Ok(Some(Missed))` semantics through [`ReceiveError`]:
    /// the bus counted the gap, and the caller can resume from the event log
    /// with `after_seq`.
    pub async fn recv(&mut self) -> Result<RunEvent, ReceiveError> {
        match self.rx.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(ReceiveError::Lagged(skipped)),
            Err(broadcast::error::RecvError::Closed) => Err(ReceiveError::Closed),
        }
    }

    /// The run this subscription belongs to.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus.unsubscribe(&self.run_id);
    }
}

/// Why a subscription receive did not produce an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReceiveError {
    /// The subscriber fell behind and `n` events were dropped.
    #[error("subscription lagged and {0} events were dropped")]
    Lagged(u64),
    /// The run was retired and its channel closed.
    #[error("run channel closed")]
    Closed,
}
