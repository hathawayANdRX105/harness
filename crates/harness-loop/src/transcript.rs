//! Transcript seam: the loop records every message it appends so the host can
//! persist the conversation ledger.
//!
//! The loop owns the in-memory `Vec<Message>`; durability belongs to the host.
//! [`TranscriptSink`] is the narrow boundary between them: the loop calls
//! [`TranscriptSink::append`] in transcript order, and the host decides where
//! the message goes. Seeded history is *not* replayed through the sink — the
//! host already persisted it before the run started.
//!
//! The call is awaited: a message reaches durable storage before the loop
//! advances the transcript, so a run that reports `Finished` has a complete
//! ledger and the next run can replay it without racing a background writer.

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::run::SessionId;

/// Host-side sink for messages the loop appends to its transcript.
///
/// Implementations receive the task seed, steering messages, assistant
/// messages and tool results, in transcript order.
#[async_trait]
pub trait TranscriptSink: Send + Sync {
    /// Record one message for `session_id`.
    ///
    /// The loop awaits this call, so an implementation may do I/O; it must not
    /// fail the run. Errors are the implementation's to log or retry — the
    /// loop has no recovery action and keeps the in-memory transcript
    /// authoritative for the current run.
    async fn append(&self, session_id: &SessionId, message: &Message);
}

/// Append `message` to the in-memory transcript, mirroring it to the sink
/// first.
///
/// Persist-then-push: a crash between the two leaves a stored message the run
/// did not use, which replay tolerates; the reverse order would lose a message
/// the model already saw.
pub(crate) async fn push(
    transcript: &mut Vec<Message>,
    sink: Option<&dyn TranscriptSink>,
    session_id: &SessionId,
    message: Message,
) {
    if let Some(sink) = sink {
        sink.append(session_id, &message).await;
    }
    transcript.push(message);
}
