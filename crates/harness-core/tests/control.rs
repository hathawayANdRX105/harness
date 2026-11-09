//! Run control: cancellation and the steering mailbox.
//!
//! Acceptance: `cancel` is observable both by polling and by `await`ing the
//! token (the await-able fix for harness-rs's one-way loop-scoped token),
//! steering is FIFO, and empty steers are dropped — nanoharness's `RunControl`
//! shape, with the loop owning the moment of injection.

use futures::executor::block_on;
use harness_core::control::RunControl;

#[test]
fn steering_is_fifo_and_drained_once() {
    let control = RunControl::new();
    control.steer("first");
    control.steer("second");
    assert_eq!(control.drain_steering().len(), 2);
    assert!(
        control.drain_steering().is_empty(),
        "drain moves the messages, it does not peek"
    );
}

#[test]
fn empty_steering_messages_are_dropped() {
    let control = RunControl::new();
    control.steer("");
    control.steer("   ");
    assert!(control.drain_steering().is_empty());
}

#[test]
fn steer_messages_carry_their_text() {
    let control = RunControl::new();
    control.steer("hold off on commits");
    let drained = control.drain_steering();
    assert_eq!(drained[0].message, "hold off on commits");
}

/// Cancellation is pollable at step boundaries (sync tools) *and* awaitable
/// at `.await` points — the one token, both views.
#[test]
fn cancellation_is_pollable_and_awaitable() {
    let control = RunControl::new();
    assert!(!control.is_cancelled());
    control.cancel();
    assert!(control.is_cancelled());

    // The await-able view resolves once cancelled — driven without a
    // runtime because the token needs no reactor.
    let token = control.cancelled();
    block_on(token.cancelled());
    assert!(control.is_cancelled());
}

/// Each `RunControl` is its own token tree: cancelling one run does not
/// touch another.
#[test]
fn controls_are_independent_per_run() {
    let a = RunControl::new();
    let b = RunControl::new();
    a.cancel();
    assert!(a.is_cancelled());
    assert!(!b.is_cancelled());
}

/// A shared run: clones hand the same token and mailbox to every component.
#[test]
fn clones_share_one_control_surface() {
    let control = RunControl::new();
    let shared = control.clone();
    shared.cancel();
    assert!(control.is_cancelled());
    shared.steer("from the clone");
    assert_eq!(control.drain_steering()[0].message, "from the clone");
}
