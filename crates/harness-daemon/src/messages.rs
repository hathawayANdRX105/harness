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

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::run::SessionId;
use harness_loop::TranscriptSink;
use harness_session::SessionStore;

/// [`TranscriptSink`] that appends to the session's message ledger.
pub struct SessionMessageSink {
    store: Arc<dyn SessionStore>,
}

impl SessionMessageSink {
    /// Build a sink over the daemon's shared store.
    pub fn new(store: Arc<dyn SessionStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl TranscriptSink for SessionMessageSink {
    async fn append(&self, session_id: &SessionId, message: &Message) {
        if let Err(err) = self.store.append_message(session_id, message).await {
            eprintln!(
                "daemon: failed to persist message for session {}: {err}",
                session_id.0
            );
        }
    }
}
