//! Transcript persistence for runs: the loop's [`TranscriptSink`] backed by
//! the session store.
//!
//! One sink is created per run and shared by that run's loop dependencies. The
//! loop awaits every append, so a run that reaches a terminal state has already
//! written its messages; the next run in the same session can replay the ledger
//! without racing a background writer.
//!
//! A write failure is logged and dropped: the run event log remains the run's
//! failure record, and a storage error must not abort a run mid-transcript.
//! Steering appends are tagged with their own unit kind so history shows the
//! injection point.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::TranscriptSink;
use harness_core::message::Message;
use harness_core::run::{RunId, SessionId};
use harness_session::SessionStore;

/// [`TranscriptSink`] that appends to the session's message ledger, tagging
/// every unit with the run that produced it.
pub struct SessionMessageSink {
    store: Arc<dyn SessionStore>,
    run_id: RunId,
}

impl SessionMessageSink {
    /// Build a sink over the daemon's shared store for one run.
    pub fn new(store: Arc<dyn SessionStore>, run_id: RunId) -> Self {
        Self { store, run_id }
    }
}

#[async_trait]
impl TranscriptSink for SessionMessageSink {
    async fn append(&self, session_id: &SessionId, message: &Message) {
        if let Err(err) = self
            .store
            .append_message_for_run(session_id, Some(&self.run_id), None, message)
            .await
        {
            eprintln!(
                "daemon: failed to persist message for session {}: {err}",
                session_id.0
            );
        }
    }

    async fn append_steering(&self, session_id: &SessionId, message: &Message) {
        if let Err(err) = self
            .store
            .append_steering_message(session_id, Some(&self.run_id), message)
            .await
        {
            eprintln!(
                "daemon: failed to persist steering for session {}: {err}",
                session_id.0
            );
        }
    }
}
