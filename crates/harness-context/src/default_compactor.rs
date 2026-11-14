//! The default structural compactor: deterministic, model-free reduction of
//! the message ledger, driven by the stage ladder.
//!
//! Design provenance: harness-rs's `DefaultCompactor`
//! (`crates/harness-loop/src/compactor.rs`) contributes the split between
//! cheap structural stages (trim, digest, drop) and a model-backed summariser
//! for the inferential stages. This implementation ships the structural half:
//! every stage is a local, deterministic edit, so compaction never costs a
//! model call and never fails on provider errors. A model-backed summariser
//! that handles the inferential stages is the natural follow-up and plugs
//! into the same [`Compactor`] trait.
//!
//! # Stage actions by label
//!
//! | Label     | Action                                                              |
//! |-----------|---------------------------------------------------------------------|
//! | `soft`    | Trim oversized tool-result bodies older than the last 8 messages.   |
//! | `medium`  | Trim oversized assistant texts older than the last 8 messages.      |
//! | `firm`    | Digest-and-drop a prefix at a safe split, keeping the last 6.       |
//! | `strong`  | Same, keeping the last 4.                                           |
//! | `maximal` | Same, keeping the last 2.                                           |
//!
//! Unknown labels are skipped (harness-rs's forward-compat rule), so a config
//! can add stages without breaking this implementation.
//!
//! # Pairing invariant
//!
//! Digest-and-drop only cuts at a *safe split*: the first kept message may
//! not be a `ToolResult`, because the assistant message that earned it would
//! sit in the dropped prefix and providers reject the orphaned result. See
//! [`safe_split_start`] — harness-rs learned this the hard way when Gemini
//! answered a cut between a tool request and its result with a permanent
//! 400 that retried into the same broken history.
//!
//! # System ledger protection
//!
//! A leading `System` run (the replayed prompt ledger) is never digested:
//! digest ranges start after it, so compaction can shrink a seeded session
//! without touching the prompt.

use async_trait::async_trait;
use harness_core::message::{Content, Message};
use harness_core::model::ModelInfo;
use tokio_util::sync::CancellationToken;

use crate::budget::Budget;
use crate::checkpoint::CompactionCheckpoint;
use crate::compactor::CompactionResult;
use crate::stage::CompactionStage;
use crate::{Compactor, default_stages};

/// How many characters one token is worth — harness-rs's coarse estimate.
/// The budget only drives threshold comparisons, not billing.
pub const DEFAULT_TOKENS_PER_CHAR: f64 = 0.30;

/// Tool results older than this many trailing messages are "old".
const OLD_WINDOW: usize = 8;
/// Character budget for an old tool result before `soft` trims it.
const OLD_TOOL_RESULT_LIMIT: usize = 800;
/// Characters kept from a trimmed old tool result.
const OLD_TOOL_RESULT_KEEP: usize = 200;
/// Character budget for an old assistant text before `medium` trims it.
const OLD_TEXT_LIMIT: usize = 1000;
/// Characters kept from a trimmed old assistant text.
const OLD_TEXT_KEEP: usize = 200;
/// Digest-and-drop stage → how many trailing messages survive.
const KEEP_RECENT_BY_LABEL: [(&str, usize); 3] = [("firm", 6), ("strong", 4), ("maximal", 2)];
/// Characters of each digested message that survive into the summary.
const DIGEST_CHARS: usize = 80;

/// Structural, deterministic compactor driven by a stage ladder.
#[derive(Debug, Clone)]
pub struct DefaultCompactor {
    stages: Vec<CompactionStage>,
    tokens_per_char: f64,
}

impl DefaultCompactor {
    /// A compactor over the default five-stage ladder.
    pub fn new() -> Self {
        Self::with_stages(default_stages())
    }

    /// A compactor over an explicit ladder (the spec-provided stages).
    pub fn with_stages(stages: Vec<CompactionStage>) -> Self {
        Self {
            stages,
            tokens_per_char: DEFAULT_TOKENS_PER_CHAR,
        }
    }

    /// Override the chars-per-token estimate.
    pub fn with_tokens_per_char(mut self, tokens_per_char: f64) -> Self {
        self.tokens_per_char = tokens_per_char;
        self
    }

    /// Rough token estimate for the ledger.
    fn estimate(&self, messages: &[Message]) -> u64 {
        let chars: usize = messages.iter().map(message_chars).sum();
        (chars as f64 * self.tokens_per_char) as u64
    }

    /// Apply one stage's action; returns whether the ledger changed.
    fn apply_stage(&self, label: &str, messages: &mut Vec<Message>) -> bool {
        match label {
            "soft" => trim_old_tool_results(
                messages,
                OLD_WINDOW,
                OLD_TOOL_RESULT_LIMIT,
                OLD_TOOL_RESULT_KEEP,
            ),
            "medium" => {
                trim_old_assistant_texts(messages, OLD_WINDOW, OLD_TEXT_LIMIT, OLD_TEXT_KEEP)
            }
            other => KEEP_RECENT_BY_LABEL
                .iter()
                .find(|(known, _)| *known == other)
                .is_some_and(|(_, keep)| digest_prefix(messages, *keep, other)),
        }
    }
}

impl Default for DefaultCompactor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Compactor for DefaultCompactor {
    fn read_budget(&self, messages: &[Message], info: &ModelInfo) -> Budget {
        Budget {
            used_tokens: self.estimate(messages),
            available_tokens: u64::from(info.context_window),
        }
    }

    async fn compact(
        &self,
        messages: &mut Vec<Message>,
        budget: &Budget,
        checkpoint: Option<&CompactionCheckpoint>,
        cancel: &CancellationToken,
    ) -> CompactionResult {
        let before = self.estimate(messages);
        let skip: &[String] = checkpoint
            .map(|cp| cp.applied_stages.as_slice())
            .unwrap_or(&[]);
        let mut applied: Vec<String> = Vec::new();
        let mut summary_at = checkpoint.map_or(0, |cp| cp.summary_at);

        for stage in &self.stages {
            // Cancellation is checked between stages: the structural edits are
            // synchronous, so a stage that started always finishes.
            if cancel.is_cancelled() || skip.contains(&stage.label) {
                continue;
            }
            if !stage.is_active(budget) {
                continue;
            }
            if self.apply_stage(&stage.label, messages) {
                applied.push(stage.label.clone());
                summary_at = messages.len();
            }
        }

        let after = self.estimate(messages);
        CompactionResult {
            before,
            after,
            checkpoint: CompactionCheckpoint {
                applied_stages: applied,
                summary_at,
            },
            completed: !cancel.is_cancelled(),
        }
    }

    fn stages(&self) -> &[CompactionStage] {
        &self.stages
    }
}

/// Characters a message contributes to the token estimate.
fn message_chars(message: &Message) -> usize {
    match message {
        Message::System { sections } => sections
            .iter()
            .map(|(name, content)| name.len() + content.as_ref().map_or(0, String::len))
            .sum(),
        Message::User { content } | Message::Assistant { content } => {
            content.iter().map(content_chars).sum()
        }
        Message::ToolResult(result) => result.content.len(),
        _ => 0,
    }
}

/// Characters one content block contributes to the token estimate.
fn content_chars(content: &Content) -> usize {
    match content {
        Content::Text(text) => text.len(),
        Content::Reasoning(text) => text.len(),
        Content::ToolCall(call) => call.id.len() + call.name.len() + call.args.to_string().len(),
        _ => 0,
    }
}

/// Trim oversized tool-result bodies that sit outside the trailing window.
/// Recent results are left alone: they are probably what the run is working
/// on, and the point is to make the context fit, not to erase it.
fn trim_old_tool_results(
    messages: &mut [Message],
    window: usize,
    limit: usize,
    keep: usize,
) -> bool {
    let split = messages.len().saturating_sub(window);
    let mut changed = false;
    for message in &mut messages[..split] {
        let Message::ToolResult(result) = message else {
            continue;
        };
        if result.content.len() > limit {
            let preview: String = result.content.chars().take(keep).collect();
            result.content = format!(
                "[tool result {} trimmed from {} chars] {}…",
                result.tool_name,
                result.content.len(),
                preview
            );
            changed = true;
        }
    }
    changed
}

/// Trim oversized assistant texts outside the trailing window. Text is safe
/// to trim — it carries no pairing obligation.
fn trim_old_assistant_texts(
    messages: &mut [Message],
    window: usize,
    limit: usize,
    keep: usize,
) -> bool {
    let split = messages.len().saturating_sub(window);
    let mut changed = false;
    for message in &mut messages[..split] {
        let Message::Assistant { content } = message else {
            continue;
        };
        for block in content.iter_mut() {
            let Content::Text(text) = block else {
                continue;
            };
            if text.len() > limit {
                let preview: String = text.chars().take(keep).collect();
                *text = format!(
                    "[assistant text trimmed from {} chars] {}…",
                    text.len(),
                    preview
                );
                changed = true;
            }
        }
    }
    changed
}

/// Replace everything before the trailing window with a one-line digest, at a
/// cut the providers will accept.
fn digest_prefix(messages: &mut Vec<Message>, keep_recent: usize, label: &str) -> bool {
    let system_prefix = messages
        .iter()
        .take_while(|message| matches!(message, Message::System { .. }))
        .count();
    if messages.len() <= keep_recent + system_prefix {
        return false;
    }
    let want = messages.len() - keep_recent;
    let Some(split) = safe_split_start(messages, want, system_prefix) else {
        return false;
    };

    let mut summary = format!("[compaction:{label}]\n");
    for message in &messages[system_prefix..split] {
        summary.push_str(&digest_message(message));
    }
    let summary_message = Message::user_text(summary);

    // Cut: head (the system ledger) survives untouched, the middle becomes
    // the summary message, the tail is kept verbatim.
    let tail = messages.split_off(split);
    let head = messages.drain(..system_prefix).collect::<Vec<_>>();
    messages.clear();
    messages.extend(head);
    messages.push(summary_message);
    messages.extend(tail);
    true
}

/// The nearest legal cut at or before `want`, then the nearest after it.
///
/// A cut is legal where the first kept message does not orphan a tool result:
/// `System` and `User` starts are always legal, and an assistant turn that
/// only speaks is legal too. `min_index` protects the leading system ledger —
/// a cut may not land inside it, so nothing at or before it is considered.
fn safe_split_start(messages: &[Message], want: usize, min_index: usize) -> Option<usize> {
    if want <= min_index || want >= messages.len() {
        return None;
    }
    (min_index + 1..=want)
        .rev()
        .chain(want + 1..messages.len())
        .find(|&index| !matches!(messages[index], Message::ToolResult(_)))
}

/// One digest line for the summary: role tag, first `DIGEST_CHARS` chars of
/// text, and tool markers so the summary still shows what happened.
fn digest_message(message: &Message) -> String {
    let mut line = String::new();
    match message {
        Message::User { content } | Message::Assistant { content } => {
            let role = if matches!(message, Message::User { .. }) {
                "user"
            } else {
                "assistant"
            };
            line.push_str(&format!("- {role}: "));
            for block in content {
                match block {
                    Content::Text(text) => {
                        line.push_str(&text.chars().take(DIGEST_CHARS).collect::<String>());
                        line.push(' ');
                    }
                    Content::ToolCall(call) => {
                        line.push_str(&format!("(call:{}) ", call.name));
                    }
                    _ => {}
                }
            }
        }
        Message::ToolResult(result) => {
            let preview: String = result.content.chars().take(DIGEST_CHARS).collect();
            line.push_str(&format!("- tool[{}]: {preview}\n", result.tool_name));
        }
        Message::System { .. } => {}
        _ => {}
    }
    if !line.ends_with('\n') {
        line.push('\n');
    }
    line
}
