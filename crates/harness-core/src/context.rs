//! The assembled prompt the model consumes: blocks, turns, the task, the
//! token budget, and the [`Context`] struct that carries them.
//!
//! Blocks are grouped so long-stable prefixes (system + guides) stay
//! cacheable across turns ("prompt caching"): the model adapter renders
//! `system` into the request's stable prefix and `history` after it. The
//! loop owns mutation of this struct; guides/hooks (a later batch) inject
//! blocks through it.

use serde::{Deserialize, Serialize};

/// A single block of content within the assembled prompt.
///
/// Media blocks (image / audio) are deliberately absent: the harness carries
/// attachments through [`crate::session::Attachment`] on the wire, and the
/// multimodal block joins this enum when the loop adopts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Block {
    /// Plain prompt text.
    Text(String),
    /// Reference to a file in the workspace. The runtime decides whether to
    /// inline contents or hand the agent a tool call to read it.
    FileRef {
        path: String,
        hash: Option<String>,
        excerpt: Option<String>,
    },
    /// Reference to an activated SKILL.md body.
    Skill { name: String, body: String },
    /// A tool call the assistant requested.
    ToolCall {
        call_id: String,
        name: String,
        args: serde_json::Value,
    },
    /// The result of a previous tool call.
    ToolResult {
        call_id: String,
        content: serde_json::Value,
    },
    /// Provider-specific reasoning trace (e.g. DeepSeek `reasoning_content`).
    /// Must be echoed back to the provider on subsequent calls or the API
    /// rejects the request.
    Reasoning(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TurnRole {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub role: TurnRole,
    pub blocks: Vec<Block>,
}

/// What the agent is working on. The loop seeds the task from the user
/// message; compaction keeps the description so a restarted context still
/// knows what it was asked to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub description: String,
    /// Where the task came from (a chat message, an issue URL, …).
    pub source: Option<String>,
    /// Unix epoch seconds, when the task must be finished by.
    pub deadline: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub used: u32,
    pub window: u32,
}

impl Budget {
    /// Fraction of the window already spent. A zero window reports `0.0`
    /// rather than dividing by zero — the caller compares against thresholds.
    pub fn ratio(&self) -> f32 {
        if self.window == 0 {
            0.0
        } else {
            self.used as f32 / self.window as f32
        }
    }
}

/// The assembled prompt the model consumes.
///
/// `system` form the stable cache-friendly prefix; `history` is the turn log
/// the compactor (harness-context) trims; `metadata` is a free-form channel
/// for the host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Context {
    pub system: Vec<Block>,
    pub guides: Vec<Block>,
    pub history: Vec<Turn>,
    pub task: Task,
    pub budget: Budget,
    pub metadata: serde_json::Value,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            system: Vec::new(),
            guides: Vec::new(),
            history: Vec::new(),
            task: Task {
                description: String::new(),
                source: None,
                deadline: None,
            },
            budget: Budget { used: 0, window: 0 },
            metadata: serde_json::Value::Null,
        }
    }
}
