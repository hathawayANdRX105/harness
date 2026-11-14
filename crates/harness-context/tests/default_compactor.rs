//! Integration tests for the `DefaultCompactor`: token estimation, the
//! per-stage structural actions, safe-split digest, checkpoint resume, and
//! cancellation semantics — all deterministic, no model involvement.
//!
//! | Test | Scenario | Acceptance |
//! |---|---|---|
//! | `read_budget_estimates_tokens` | known-size ledger | used = chars × 0.30; available = context window |
//! | `soft_trims_old_tool_results` | 12 oversized results, window 8 | the 4 old results are trimmed, the recent ones untouched |
//! | `medium_trims_old_assistant_text` | oversized old assistant text | text replaced with a trim marker |
//! | `digest_respects_safe_split` | cut would orphan a tool result | first kept message is not a tool result |
//! | `digest_never_touches_system_ledger` | System message at index 0 | system content survives verbatim, digest starts after it |
//! | `cancelled_compaction_is_partial` | token cancelled before compact | `completed=false`, nothing applied |
//! | `checkpoint_skips_applied_stages` | checkpoint lists `soft` | `soft` is skipped, later stages still run |
//!
//! The default ladder (soft 60% → maximal 95%) is used with a budget far
//! above every threshold, so each test drives the exact stage under
//! examination and every earlier stage first.

use std::sync::Arc;

use harness_context::{
    Budget, CompactionCheckpoint, CompactionResult, Compactor, DefaultCompactor,
};
use harness_core::ToolCall;
use harness_core::message::{Content, Message, ToolResultMessage};
use harness_core::model::ModelInfo;
use tokio_util::sync::CancellationToken;

/// A model info with a small window so tiny ledgers look oversized.
fn small_window_info() -> ModelInfo {
    ModelInfo {
        handle: "test-model".into(),
        provider: "test".into(),
        model: "test-model".into(),
        context_window: 4_096,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

/// A user message with `chars` characters of text.
fn big_user_text(chars: usize) -> Message {
    Message::user_text("x".repeat(chars))
}

/// A tool result with `chars` characters of output.
fn big_tool_result(name: &str, chars: usize) -> Message {
    Message::ToolResult(ToolResultMessage {
        tool_call_id: format!("call-{name}"),
        tool_name: name.to_string(),
        content: "y".repeat(chars),
        is_error: false,
    })
}

/// An assistant message with `chars` characters of text (no tool calls).
fn big_assistant_text(chars: usize) -> Message {
    Message::Assistant {
        content: vec![Content::Text("z".repeat(chars))],
    }
}

/// A budget far above every threshold in the default ladder.
fn oversized_budget() -> Budget {
    Budget {
        used_tokens: u64::MAX / 2,
        available_tokens: 1,
    }
}

#[tokio::test]
async fn read_budget_estimates_tokens() {
    let compactor = DefaultCompactor::new();
    let messages = vec![big_user_text(1_000), big_tool_result("grep", 1_000)];
    // 1000 chars user + 1000 chars tool result = 2000 chars × 0.30.
    let budget = compactor.read_budget(&messages, &small_window_info());
    assert_eq!(budget.used_tokens, 600, "chars × 0.30, truncated to u64");
    assert_eq!(budget.available_tokens, 4_096, "available is the window");
}

#[tokio::test]
async fn soft_trims_old_tool_results() {
    let compactor = DefaultCompactor::new();
    // 12 tool results: the first 8 are old (outside the trailing window of
    // 8 messages... here every message is a tool result, so the trailing 8
    // are recent and the first 4 are old).
    let mut messages: Vec<Message> = Vec::new();
    for index in 0..12 {
        messages.push(big_tool_result(&format!("tool{index}"), 2_000));
    }
    let before = compactor
        .read_budget(&messages, &small_window_info())
        .used_tokens;

    let result = compactor
        .compact(
            &mut messages,
            &oversized_budget(),
            None,
            &CancellationToken::new(),
        )
        .await;

    assert!(result.completed, "no cancellation fired");
    assert!(result.after < before, "compaction must shrink the ledger");
    // The first 4 (old) results are trimmed to 200 chars plus the marker.
    let first = match &messages[0] {
        Message::ToolResult(result) => &result.content,
        other => panic!("expected a tool result, got {other:?}"),
    };
    assert!(
        first.contains("trimmed from 2000 chars"),
        "the marker names the original size: {first}"
    );
    assert!(first.len() < 400, "the kept preview is small: {first}");
    // The trailing 8 are recent and untouched.
    let last = match &messages[11] {
        Message::ToolResult(result) => &result.content,
        other => panic!("expected a tool result, got {other:?}"),
    };
    assert_eq!(last.len(), 2_000, "recent results are left alone");
}

#[tokio::test]
async fn medium_trims_old_assistant_text() {
    let compactor = DefaultCompactor::with_stages(vec![harness_context::CompactionStage {
        threshold: 0.0,
        target_ratio: 0.0,
        label: "medium".into(),
    }]);
    // 12 messages: 6 oversized assistant texts interleaved with filler; the
    // first 4 assistant texts sit outside the trailing window of 8.
    let mut messages: Vec<Message> = Vec::new();
    for index in 0..12 {
        if index % 2 == 0 {
            messages.push(big_assistant_text(2_000));
        } else {
            messages.push(big_user_text(10));
        }
    }
    let before = compactor
        .read_budget(&messages, &small_window_info())
        .used_tokens;

    let result = compactor
        .compact(
            &mut messages,
            &oversized_budget(),
            None,
            &CancellationToken::new(),
        )
        .await;

    assert!(result.completed);
    assert!(result.after < before, "assistant text was trimmed");
    for (index, message) in messages.iter().enumerate() {
        if index % 2 == 0 && index < 4 {
            let Message::Assistant { content } = message else {
                panic!("expected assistant at {index}");
            };
            let Content::Text(text) = &content[0] else {
                panic!("expected text");
            };
            assert!(text.contains("trimmed from 2000 chars"), "old text trimmed");
        }
    }
}

#[tokio::test]
async fn digest_respects_safe_split() {
    let compactor = DefaultCompactor::with_stages(vec![harness_context::CompactionStage {
        threshold: 0.0,
        target_ratio: 0.0,
        label: "maximal".into(),
    }]);
    // A ledger whose natural cut (keep the last 2) would land on a tool
    // result — the assistant message that earned it must not be orphaned.
    let mut messages: Vec<Message> = vec![
        big_user_text(500),
        Message::Assistant {
            content: vec![
                Content::Text("calling tool".into()),
                Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "grep".into(),
                    args: serde_json::json!({}),
                }),
            ],
        },
        big_tool_result("grep", 2_000),
        Message::Assistant {
            content: vec![
                Content::Text("calling again".into()),
                Content::ToolCall(ToolCall {
                    id: "call-2".into(),
                    name: "grep".into(),
                    args: serde_json::json!({}),
                }),
            ],
        },
        big_tool_result("grep", 2_000),
        big_user_text(500),
        big_assistant_text(500),
    ];

    let result = compactor
        .compact(
            &mut messages,
            &oversized_budget(),
            None,
            &CancellationToken::new(),
        )
        .await;

    assert!(result.completed);
    assert!(result.after < result.before, "digest shrinks the ledger");
    // Find the digest message and check the first kept message after it.
    let digest_position = messages
        .iter()
        .position(|message| {
            matches!(message, Message::User { content }
                if content.iter().any(|block| matches!(block, Content::Text(text)
                    if text.starts_with("[compaction:maximal]"))))
        })
        .unwrap_or_else(|| panic!("a digest message was inserted"));
    let next = messages
        .get(digest_position + 1)
        .unwrap_or_else(|| panic!("kept messages follow the digest"));
    assert!(
        !matches!(next, Message::ToolResult(_)),
        "the first kept message must not orphan a tool result"
    );
    // The pairing invariant over the kept tail: every tool result that
    // survives must be immediately preceded by the assistant message that
    // earned it. A tool result at the head of the tail (or after a user
    // message) would mean its request was dropped.
    let tail = &messages[digest_position + 1..];
    for window in tail.windows(2) {
        if matches!(window[1], Message::ToolResult(_)) {
            assert!(
                matches!(window[0], Message::Assistant { .. }),
                "a tool result in the kept tail must follow its assistant message"
            );
        }
    }
}

#[tokio::test]
async fn digest_never_touches_system_ledger() {
    let compactor = DefaultCompactor::with_stages(vec![harness_context::CompactionStage {
        threshold: 0.0,
        target_ratio: 0.0,
        label: "maximal".into(),
    }]);
    // A replayed session starts with the accumulated system ledger.
    let system_message = Message::System {
        sections: [("identity".to_string(), Some("You are harness.".to_string()))]
            .into_iter()
            .collect(),
    };
    let mut messages: Vec<Message> = vec![system_message];
    for index in 0..10 {
        messages.push(big_user_text(300 + index));
    }

    let result = compactor
        .compact(
            &mut messages,
            &oversized_budget(),
            None,
            &CancellationToken::new(),
        )
        .await;

    assert!(result.completed);
    let Message::System { sections } = &messages[0] else {
        panic!("the system ledger must stay at index 0");
    };
    assert_eq!(
        sections.get("identity").map(|c| c.as_deref()),
        Some(Some("You are harness.")),
        "the system content survives verbatim"
    );
    // The digest carries the digested non-system messages, not the system one.
    let digest = messages
        .iter()
        .find_map(|message| match message {
            Message::User { content } => content.iter().find_map(|block| match block {
                Content::Text(text) if text.starts_with("[compaction:maximal]") => {
                    Some(text.clone())
                }
                _ => None,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a digest message was inserted"));
    assert!(
        !digest.contains("You are harness."),
        "the system content must not be digested"
    );
}

#[tokio::test]
async fn cancelled_compaction_is_partial() {
    let compactor = DefaultCompactor::new();
    let mut messages: Vec<Message> = (0..12)
        .map(|index| big_tool_result(&format!("tool{index}"), 2_000))
        .collect();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = compactor
        .compact(&mut messages, &oversized_budget(), None, &cancel)
        .await;

    assert!(!result.completed, "a cancelled pass is partial");
    assert!(result.checkpoint.applied_stages.is_empty());
    assert_eq!(
        result.before, result.after,
        "nothing was edited after cancellation"
    );
}

#[tokio::test]
async fn checkpoint_skips_applied_stages() {
    let compactor = DefaultCompactor::new();
    // A realistic ledger: one user task, then assistant/tool-result pairs
    // with oversized outputs. The digest stages need legal cut points, which
    // only exist when assistant messages carry the tool calls.
    let mut messages: Vec<Message> = vec![big_user_text(100)];
    for index in 0..6 {
        messages.push(Message::Assistant {
            content: vec![
                Content::Text(format!("calling tool {index}")),
                Content::ToolCall(ToolCall {
                    id: format!("call-{index}"),
                    name: "grep".into(),
                    args: serde_json::json!({}),
                }),
            ],
        });
        messages.push(big_tool_result(&format!("grep{index}"), 2_000));
    }
    let checkpoint = CompactionCheckpoint {
        applied_stages: vec!["soft".into(), "medium".into()],
        summary_at: 0,
    };

    let result = compactor
        .compact(
            &mut messages,
            &oversized_budget(),
            Some(&checkpoint),
            &CancellationToken::new(),
        )
        .await;

    assert!(result.completed);
    assert_eq!(
        result.checkpoint.applied_stages,
        vec![
            "firm".to_string(),
            "strong".to_string(),
            "maximal".to_string()
        ],
        "the recorded stages are skipped, the rest still run"
    );
}

/// The trait object shape the builder stores: `Arc<dyn Compactor>` must work.
#[tokio::test]
async fn compactor_is_object_safe_behind_a_trait_object() {
    let compactor: Arc<dyn Compactor> = Arc::new(DefaultCompactor::new());
    let messages = vec![big_user_text(100)];
    let budget = compactor.read_budget(&messages, &small_window_info());
    assert_eq!(budget.available_tokens, 4_096);
    let mut clone = messages.clone();
    let result = compactor
        .compact(&mut clone, &budget, None, &CancellationToken::new())
        .await;
    assert!(result.completed);
}

/// Keep `CompactionResult` in scope for the doc table above.
#[allow(dead_code)]
fn _result_type_witness(_result: CompactionResult) {}
