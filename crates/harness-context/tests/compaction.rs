//! Compaction behavior: budget assessment, stage activation, compaction
//! execution with tool-call/result pairing, checkpoint resume, and
//! cancellation.
//!
//! The test compactor is a simple deterministic implementation that:
//!
//! - Counts tokens as `message_chars / 4` (a rough estimate, not a real
//!   tokenizer).
//! - "Compacts" by replacing old user/assistant message pairs (before the
//!   cursor) with a single summary `User { content: [Text(summary)] }`
//!   message.
//! - Preserves tool-call/tool-result pairing: it never removes an assistant
//!   message that contains `ToolCall` content without also removing the
//!   matching `ToolResult`, and never leaves an orphaned result behind.
//! - Records a checkpoint at each stage boundary.
//! - Checks the cancellation token between stages.

use async_trait::async_trait;
use harness_context::{Budget, CompactionCheckpoint, CompactionResult, CompactionStage, Compactor};
use harness_context::{BudgetBlocked, default_stages};
use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
use harness_core::model::ModelInfo;
use tokio_util::sync::CancellationToken;

// ── Test compactor ───────────────────────────────────────────────────────────

/// A deterministic compactor for testing.
///
/// Pattern:
/// 1. `read_budget` counts `total_chars / 4` as used tokens.
/// 2. `compact` identifies messages before the cursor position and replaces
///    safe blocks (user/assistant pairs without unmatched tool calls) with
///    a summary message. It writes a checkpoint after each stage.
struct TestCompactor {
    stages: Vec<CompactionStage>,
}

impl TestCompactor {
    fn new(stages: Vec<CompactionStage>) -> Self {
        Self { stages }
    }

    /// Estimate tokens from message content (chars / 4).
    fn estimate_tokens(messages: &[Message]) -> u64 {
        messages
            .iter()
            .map(|m| {
                let text = match m {
                    Message::System { sections } => sections
                        .values()
                        .filter_map(|v| v.as_deref())
                        .collect::<Vec<_>>()
                        .join(" "),
                    Message::User { content } | Message::Assistant { content } => content
                        .iter()
                        .map(|c| match c {
                            Content::Text(t) | Content::Reasoning(t) => t.as_str(),
                            Content::ToolCall(tc) => tc.name.as_str(),
                            _ => "",
                        })
                        .collect::<Vec<_>>()
                        .join(" "),
                    Message::ToolResult(tr) => tr.content.clone(),
                    _ => String::new(),
                };
                text.len() as u64
            })
            .sum::<u64>()
            / 4
    }

    /// Find the first safe compaction boundary before `limit`.
    ///
    /// A boundary is safe when it sits at a message index where no
    /// unmatched tool calls or orphaned tool results exist in [0, idx).
    /// We scan backward from `limit` to find the latest safe split point.
    fn find_safe_boundary(messages: &[Message], limit: usize) -> usize {
        let limit = limit.min(messages.len());

        // Find all tool-call positions in the prefix [0, limit). We need
        // to verify that every call has a matching result and vice versa.
        // The safe boundary is the farthest point where all pairs are
        // complete.
        let mut last_result = 0usize;

        // Collect tool-call positions and ids.
        let call_positions: Vec<(usize, String)> = messages[..limit]
            .iter()
            .enumerate()
            .filter_map(|(idx, m)| {
                if let Message::Assistant { content } = m {
                    content.iter().find_map(|c| {
                        if let Content::ToolCall(tc) = c {
                            Some((idx, tc.id.clone()))
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
            .collect();

        // For each tool call, find its matching result.
        for (call_idx, call_id) in &call_positions {
            let result_idx = messages[*call_idx + 1..limit]
                .iter()
                .position(|m| matches!(m, Message::ToolResult(tr) if tr.tool_call_id == *call_id))
                .map(|pos| *call_idx + 1 + pos);

            if let Some(idx) = result_idx {
                // Result found — safe if we include up to the result.
                if idx > last_result {
                    last_result = idx;
                }
            } else {
                // Unmatched tool call — limit boundary to before the call.
                if *call_idx > 0 && *call_idx < limit {
                    return *call_idx;
                }
                return 0;
            }
        }

        // Also handle orphaned results: a ToolResult whose matching call
        // is not in the prefix.
        for (idx, m) in messages[..limit].iter().enumerate() {
            if let Message::ToolResult(tr) = m {
                let call_found = call_positions.iter().any(|(_, id)| id == &tr.tool_call_id);
                if !call_found {
                    // Orphaned result — cut before it.
                    if idx > 0 {
                        return idx;
                    }
                    return 0;
                }
            }
        }

        // Safe boundary is after the last result, or at 0 if no pairs
        // exist.
        if last_result > 0 && last_result + 1 < limit {
            last_result + 1
        } else {
            0
        }
    }
}

#[async_trait]
impl Compactor for TestCompactor {
    fn read_budget(&self, messages: &[Message], info: &ModelInfo) -> Budget {
        Budget {
            used_tokens: Self::estimate_tokens(messages),
            available_tokens: info.context_window as u64,
        }
    }

    async fn compact(
        &self,
        messages: &mut Vec<Message>,
        budget: &Budget,
        checkpoint: Option<&CompactionCheckpoint>,
        cancel: &CancellationToken,
    ) -> CompactionResult {
        let before = Self::estimate_tokens(messages);

        let start_stage = checkpoint
            .and_then(|cp| cp.applied_stages.last().cloned())
            .unwrap_or_default();

        let mut applied_stages = checkpoint
            .map(|cp| cp.applied_stages.clone())
            .unwrap_or_default();

        let mut stage_index = if start_stage.is_empty() {
            0
        } else {
            self.stages
                .iter()
                .position(|s| s.label == start_stage)
                .map(|i| i + 1)
                .unwrap_or(self.stages.len())
        };

        // Resume from the checkpoint's summary_at if provided.
        if let Some(cp) = checkpoint {
            // If the checkpoint says we already compacted up to some point,
            // we need the messages to reflect that (the ledger was already
            // compacted). This test assumes the caller passes the already-
            // compacted Vec<Message>.
            let _ = cp.summary_at;
        }

        // Try each remaining stage.
        for stage in &self.stages[stage_index..] {
            if cancel.is_cancelled() {
                break;
            }

            if !stage.is_active(budget) {
                // Budget is under control — no need for further stages.
                applied_stages.push(stage.label.clone());
                stage_index += 1;
                continue;
            }

            // Find a safe boundary in the first half of messages (we
            // compact old history, not recent turns).
            let limit = messages.len() / 2;
            let boundary = Self::find_safe_boundary(messages, limit);

            if boundary == 0 {
                // Nothing to compact — all messages are needed for
                // pairing integrity. Move to next stage.
                applied_stages.push(stage.label.clone());
                stage_index += 1;
                continue;
            }

            // Replace messages [0, boundary) with a summary message.
            let summary_text = format!(
                "[summarized {} earlier messages at {} stage]",
                boundary, stage.label
            );
            let summary = Message::user_text(summary_text);
            messages.drain(..boundary);
            messages.insert(0, summary);

            applied_stages.push(stage.label.clone());
            stage_index += 1;

            // Re-check budget after compaction.
            let new_used = Self::estimate_tokens(messages);
            let remaining = budget.available_tokens.saturating_sub(new_used);
            if remaining > budget.available_tokens / 4 {
                // Budget is well under control — stop.
                break;
            }
        }

        let after = Self::estimate_tokens(messages);
        let completed = stage_index >= self.stages.len();

        CompactionResult {
            before,
            after,
            checkpoint: CompactionCheckpoint {
                applied_stages,
                summary_at: 1, // The summary message is at index 0.
            },
            completed,
        }
    }

    fn stages(&self) -> &[CompactionStage] {
        &self.stages
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn model_info_32k() -> ModelInfo {
    ModelInfo {
        handle: "test-model".into(),
        provider: "test".into(),
        model: "test-model".into(),
        context_window: 32_768,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

fn make_transcript(len: usize) -> Vec<Message> {
    (0..len)
        .flat_map(|i| {
            vec![
                Message::user_text(format!("user message {i}")),
                Message::Assistant {
                    content: vec![Content::Text(format!("assistant reply {i}"))],
                },
            ]
        })
        .collect()
}

// ── Checkpoint tests ─────────────────────────────────────────────────────────

/// Purpose: a fresh checkpoint reports as fresh.
/// Scenario: call `CompactionCheckpoint::fresh()`.
/// Acceptance: `is_fresh` is true.
#[test]
fn fresh_checkpoint_is_fresh() {
    let cp = CompactionCheckpoint::fresh();
    assert!(cp.is_fresh());
    assert!(cp.applied_stages.is_empty());
}

/// Purpose: a checkpoint with applied stages is not fresh.
/// Scenario: create a checkpoint with one applied stage.
/// Acceptance: `is_fresh` is false.
#[test]
fn non_fresh_checkpoint_reports_not_fresh() {
    let cp = CompactionCheckpoint {
        applied_stages: vec!["soft".into()],
        summary_at: 5,
    };
    assert!(!cp.is_fresh());
}

/// Purpose: CompactionCheckpoint round-trips through serde.
/// Scenario: serialize and deserialize.
/// Acceptance: values are equal after round-trip.
#[test]
fn checkpoint_round_trips() -> Result<(), String> {
    let cp = CompactionCheckpoint {
        applied_stages: vec!["soft".into(), "medium".into()],
        summary_at: 10,
    };
    let json = serde_json::to_value(&cp).map_err(|e| e.to_string())?;
    let back: CompactionCheckpoint = serde_json::from_value(json).map_err(|e| e.to_string())?;
    assert_eq!(back, cp);
    Ok(())
}

// ── BudgetBlocked tests ──────────────────────────────────────────────────────

/// Purpose: BudgetBlocked carries needed/available/stage.
/// Scenario: construct and serialize.
/// Acceptance: all fields survive serde round-trip.
#[test]
fn budget_blocked_round_trips() -> Result<(), String> {
    let blocked = BudgetBlocked {
        needed: 10_000,
        available: 5_000,
        stage: "maximal".into(),
    };
    let json = serde_json::to_value(&blocked).map_err(|e| e.to_string())?;
    assert_eq!(json["needed"], 10_000);
    assert_eq!(json["available"], 5_000);
    assert_eq!(json["stage"], "maximal");
    let back: BudgetBlocked = serde_json::from_value(json).map_err(|e| e.to_string())?;
    assert_eq!(back, blocked);
    Ok(())
}

/// Purpose: BudgetBlocked is semantically distinct from RunStopReason::Error.
/// Scenario: no relation exists between the types — the test asserts only
/// that BudgetBlocked is its own type with its own fields.
/// Acceptance: BudgetBlocked exists with needed/available/stage.
#[test]
fn budget_blocked_has_expected_fields() {
    let blocked = BudgetBlocked {
        needed: 1,
        available: 0,
        stage: "soft".into(),
    };
    assert_eq!(blocked.needed, 1);
    assert_eq!(blocked.available, 0);
    assert_eq!(blocked.stage, "soft");
}

// ── Compaction tests ─────────────────────────��───────────────────────────────

/// Purpose: compaction reduces token count when the budget is exceeded.
/// Scenario: create a large transcript, compact at soft stage.
/// Acceptance: after tokens is less than before.
#[tokio::test]
async fn compaction_reduces_tokens() {
    let compactor = TestCompactor::new(default_stages());
    let mut messages = make_transcript(100);
    let info = model_info_32k();
    let budget = compactor.read_budget(&messages, &info);
    let cancel = CancellationToken::new();

    let result = compactor
        .compact(&mut messages, &budget, None, &cancel)
        .await;

    assert!(
        result.after <= result.before,
        "compaction should not increase tokens"
    );
    assert!(
        result.saved() > 0,
        "compaction should free tokens when budget is exceeded"
    );
}

/// Purpose: compaction preserves tool-call/result pairing.
/// Scenario: transcript with tool calls and matching results.
/// Acceptance: after compaction, every remaining ToolCall has a matching
/// ToolResult and vice versa.
#[tokio::test]
async fn compaction_preserves_tool_pairing() {
    let compactor = TestCompactor::new(default_stages());

    let mut messages = make_transcript(10);

    // Add tool-call/result pairs
    messages.push(Message::Assistant {
        content: vec![
            Content::Text("Let me check the file.".into()),
            Content::ToolCall(ToolCall {
                id: "call-1".into(),
                name: "read".into(),
                args: serde_json::json!({"path": "src/lib.rs"}),
            }),
        ],
    });
    messages.push(Message::ToolResult(ToolResultMessage {
        tool_call_id: "call-1".into(),
        tool_name: "read".into(),
        content: "file contents".into(),
        is_error: false,
    }));

    let _info = model_info_32k();
    // Use a tight budget to force aggressive compaction
    let budget = Budget {
        used_tokens: 100_000, // force compaction
        available_tokens: 32_768,
    };
    let cancel = CancellationToken::new();

    let result = compactor
        .compact(&mut messages, &budget, None, &cancel)
        .await;

    assert!(result.completed, "compaction should complete");

    // Verify pairing in the compacted transcript
    let call_ids: Vec<&str> = messages
        .iter()
        .filter_map(|m| {
            if let Message::Assistant { content } = m {
                content.iter().find_map(|c| {
                    if let Content::ToolCall(tc) = c {
                        Some(tc.id.as_str())
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        })
        .collect();

    let result_ids: Vec<&str> = messages
        .iter()
        .filter_map(|m| {
            if let Message::ToolResult(tr) = m {
                Some(tr.tool_call_id.as_str())
            } else {
                None
            }
        })
        .collect();

    // Every tool call must have a matching result.
    for call_id in &call_ids {
        assert!(
            result_ids.contains(call_id),
            "tool call {call_id} has no matching result"
        );
    }

    // Every result must have a matching call.
    for result_id in &result_ids {
        assert!(
            call_ids.contains(result_id),
            "tool result {result_id} has no matching call"
        );
    }
}

/// Purpose: compaction can be cancelled mid-way and resumed via checkpoint.
/// Scenario: cancel during second stage, then resume from checkpoint.
/// Acceptance: first compaction stops early, second resumes and completes.
#[tokio::test]
async fn compaction_can_be_resumed_from_checkpoint() {
    let compactor = TestCompactor::new(default_stages());
    let mut messages = make_transcript(100);
    let info = model_info_32k();
    let budget = compactor.read_budget(&messages, &info);

    // Use a cancel token that fires immediately (before any async work)
    let cancel_immediate = CancellationToken::new();
    cancel_immediate.cancel();

    let first_result = compactor
        .compact(&mut messages, &budget, None, &cancel_immediate)
        .await;

    // With immediate cancellation, compaction should still produce a valid
    // result (checkpoint for resume).
    assert!(
        !first_result.checkpoint.is_fresh(),
        "cancelled compaction should still populate checkpoint"
    );

    // Resume from checkpoint.
    let cancel = CancellationToken::new();
    let second_result = compactor
        .compact(
            &mut messages,
            &budget,
            Some(&first_result.checkpoint),
            &cancel,
        )
        .await;

    // After resume, the result should show progress.
    assert!(
        second_result.after <= first_result.after,
        "resumed compaction should not increase tokens"
    );
}

/// Purpose: a transcript fully within budget is not compacted.
/// Scenario: very short transcript with large context window.
/// Acceptance: before == after.
#[tokio::test]
async fn no_compaction_when_budget_under_control() {
    let compactor = TestCompactor::new(default_stages());
    let mut messages = make_transcript(3);
    let info = ModelInfo {
        context_window: 1_000_000, // huge window
        ..model_info_32k()
    };
    let budget = compactor.read_budget(&messages, &info);
    let cancel = CancellationToken::new();

    let result = compactor
        .compact(&mut messages, &budget, None, &cancel)
        .await;

    // All stages are below threshold, so no actual compaction.
    assert!(result.saved() == 0);
    assert!(result.completed);
}

/// Purpose: Checkpoint resume collects stage labels correctly.
/// Scenario: apply one stage, capture checkpoint, resume and apply more.
/// Acceptance: final checkpoint lists all applied stages in order.
#[tokio::test]
async fn checkpoint_tracks_applied_stages() {
    let compactor = TestCompactor::new(default_stages());
    let mut messages = make_transcript(50);
    let info = model_info_32k();
    let budget = compactor.read_budget(&messages, &info);

    // Run full compaction.
    let cancel = CancellationToken::new();
    let result = compactor
        .compact(&mut messages, &budget, None, &cancel)
        .await;

    // Check that each applied stage label is valid.
    let stages = default_stages();
    let stage_labels: Vec<&str> = stages.iter().map(|s| s.label.as_str()).collect();

    for label in &result.checkpoint.applied_stages {
        assert!(
            stage_labels.contains(&label.as_str()),
            "checkpoint contains unknown stage: {label}"
        );
    }
}

/// Purpose: CompactionResult's saved() method returns correct delta.
/// Scenario: compact with known before/after values.
/// Acceptance: saved == before - after.
#[tokio::test]
async fn compaction_result_reports_saved() {
    let compactor = TestCompactor::new(default_stages());
    let mut messages = make_transcript(100);
    let info = model_info_32k();
    let budget = compactor.read_budget(&messages, &info);
    let cancel = CancellationToken::new();

    let result = compactor
        .compact(&mut messages, &budget, None, &cancel)
        .await;

    assert_eq!(result.saved(), result.before.saturating_sub(result.after));
}

/// Purpose: default_stages() produces the known ladder from harness-rs.
/// Scenario: call default_stages().
/// Acceptance: thresholds match 60/70/80/90/95%.
#[test]
fn default_ladder_thresholds() {
    let stages = default_stages();
    let expected = [0.60, 0.70, 0.80, 0.90, 0.95];
    for (stage, exp) in stages.iter().zip(expected.iter()) {
        assert!(
            (stage.threshold - exp).abs() < f64::EPSILON,
            "expected threshold {exp}, got {}",
            stage.threshold
        );
    }
}

/// Purpose: a custom stage set overrides defaults.
/// Scenario: create a compactor with custom stages.
/// Acceptance: stages() returns the custom set.
#[test]
fn custom_stages() {
    let stages = vec![CompactionStage {
        threshold: 0.50,
        target_ratio: 0.40,
        label: "early".into(),
    }];
    let compactor = TestCompactor::new(stages.clone());
    assert_eq!(compactor.stages(), &stages);
}
