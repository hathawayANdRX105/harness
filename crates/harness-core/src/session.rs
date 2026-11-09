//! Session data types shared by the daemon, the session store and the wire
//! protocol: role / session summary / run record / attachments / message rows.
//!
//! Persistence lives in `harness-session`; this module carries only the
//! serialized shapes the daemon and its consumers agree on.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A message role. Kept small and string-backed so it can round-trip through
/// both a SQL column and serde without an extra enum-mapping layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionRole {
    User,
    Assistant,
    System,
    Tool,
}

impl SessionRole {
    /// Canonical lowercase wire/storage form (`"user"`, `"assistant"`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            SessionRole::User => "user",
            SessionRole::Assistant => "assistant",
            SessionRole::System => "system",
            SessionRole::Tool => "tool",
        }
    }

    /// Parse from any case; the raw input string comes back on miss so the
    /// caller maps it into its own error type.
    pub fn parse(s: &str) -> Result<SessionRole, String> {
        match s.to_ascii_lowercase().as_str() {
            "user" => Ok(SessionRole::User),
            "assistant" => Ok(SessionRole::Assistant),
            "system" => Ok(SessionRole::System),
            "tool" => Ok(SessionRole::Tool),
            _ => Err(s.to_string()),
        }
    }
}

impl fmt::Display for SessionRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// -----------------------------------------------------------------------------
// Records
// -----------------------------------------------------------------------------

/// Row from a session listing — the fields a transport/UI needs to render a
/// session list. `message_count` is always populated by list/lookup paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub title: String,
    /// Parent session id — the lineage edge for grouping. `None` for a root
    /// session.
    ///
    /// Serde-optional: a client that omits the field deserializes as `None`,
    /// and `None` is emitted as an explicit `null` so every summary the daemon
    /// returns carries the key.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// The session's own workspace directory. Empty means "unbound": the run
    /// falls back to the daemon-wide cwd.
    ///
    /// Serde-optional: rows written before the column existed deserialize as
    /// the empty string, which is exactly their meaning.
    #[serde(default)]
    pub cwd: String,
    /// Unix epoch milliseconds. Matches `created_at` / `updated_at` columns.
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub message_count: u64,
}

/// One run recorded in the daemon's run ledger. `finished_at_ms` is `None`
/// while the run is in flight. Lives in `core` (not in the daemon crate)
/// because both the daemon and the session store read these rows and neither
/// may depend on the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    #[serde(default)]
    pub seq: i64,
    pub run_id: String,
    pub session_id: String,
    pub started_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// One image attached to a user message, stored alongside the text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// Original file name, for display and diagnostics only.
    pub name: String,
    /// Whitelisted MIME type, e.g. "image/png" — validation happens upstream
    /// of the store.
    pub media_type: String,
    /// Plain base64 payload, no "data:" prefix.
    pub data: String,
}

/// One message row, in storage order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    pub session_id: String,
    /// Monotonic per-session, starting at 1. Assigned by the store inside a
    /// single transaction.
    pub seq: i64,
    pub role: SessionRole,
    pub text: String,
    /// Unix epoch milliseconds — set when the row is appended.
    pub created_at_ms: i64,
    /// Images attached to this message; empty when it carries none.
    /// `#[serde(default)]` keeps payloads written before this field
    /// deserializable as "no attachments".
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// Tool-call cards carried by this message (an assistant turn), passed
    /// through as an opaque JSON array. Empty = a plain-text message.
    #[serde(default)]
    pub tool_calls: Vec<serde_json::Value>,
}
