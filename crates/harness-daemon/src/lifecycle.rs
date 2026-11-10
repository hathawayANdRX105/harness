//! Process lifecycle: signal watching and the graceful drain.
//!
//! On SIGINT / SIGTERM the daemon cancels every running run and waits (bounded)
//! for them to reach a terminal status before returning — in-flight work is
//! never abandoned silently, and a hung run cannot wedge the process forever.

use std::path::Path;
use std::time::Duration;

use crate::wire::DaemonHandler;

/// How long the drain waits for in-flight runs before exiting anyway.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while draining.
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Block until SIGINT or SIGTERM arrives.
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        Err(err) => {
            eprintln!("daemon: SIGTERM watcher unavailable: {err}");
            let _ = ctrl_c.await;
            return;
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = term.recv() => {}
    }
}

/// Cancel every running run, wait (bounded) for them to reach a terminal
/// status, then remove the socket file.
pub async fn drain(handler: &DaemonHandler, socket_path: &Path) {
    let cancelled = handler.cancel_all_running();
    if cancelled > 0 {
        eprintln!("daemon: cancelled {cancelled} running run(s); draining");
    }
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    while tokio::time::Instant::now() < deadline && handler.has_running() {
        tokio::time::sleep(DRAIN_POLL).await;
    }
    let still_running = handler.has_running();
    if still_running {
        eprintln!("daemon: drain timed out with runs still active; exiting anyway");
    }
    // Served runs may outlive the wait only in the timeout case; the statuses
    // stay queryable until the process exits.
    let _ = tokio::fs::remove_file(socket_path).await;
}
