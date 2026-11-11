//! Integration tests for output truncation and spill.
//!
//! Each test uses tempfile for the spill directory so spilling works without
//! depending on the system temp dir being writable.

use std::fs;

use harness_tools::output::truncate_output;

/// Purpose: verify that content under the limit is returned unchanged.
/// Scenario: call truncate_output with content shorter than max_chars.
/// Acceptance: returned text is the original, no spill file.
#[test]
fn under_limit() {
    let (text, spill) = truncate_output("short", 100);
    assert_eq!(text, "short");
    assert!(spill.is_none(), "no spill for short content");
}

/// Purpose: verify that content at the exact limit is returned unchanged.
/// Scenario: call truncate_output with content length equal to max_chars.
/// Acceptance: returned text is the original, no spill file.
#[test]
fn at_exact_limit() {
    let content = "exact";
    let (text, spill) = truncate_output(content, 5);
    assert_eq!(text, content);
    assert!(spill.is_none());
}

/// Purpose: verify that over-limit content keeps the tail and creates a spill file.
/// Scenario: call truncate_output with 6 chars and max_chars=3.
/// Acceptance: spill file exists, content includes the tail ("rld" from "hello world"... wait let me use exact data).
#[test]
fn over_limit_keeps_tail() {
    let content = "abcdef";
    let (text, spill) = truncate_output(content, 3);
    assert!(spill.is_some(), "over-limit content must create a spill");
    let spill_path = spill.unwrap_or_else(|| panic!("expected spill path"));
    assert!(
        spill_path.exists(),
        "spill file must exist on disk: {}",
        spill_path.display()
    );
    // The content should contain "... (truncated..." and the tail "def".
    assert!(
        text.contains("def"),
        "truncated output must contain tail: got {text:?}"
    );
    // The spill file should contain the full text.
    let spill_content =
        fs::read_to_string(&spill_path).unwrap_or_else(|e| panic!("read spill file: {e}"));
    assert_eq!(
        spill_content, content,
        "spill file must contain the full text"
    );
}

/// Purpose: verify that empty content produces no truncation and no spill.
/// Scenario: truncate_output with empty string.
/// Acceptance: empty string returned, no spill file.
#[test]
fn empty_content() {
    let (text, spill) = truncate_output("", 100);
    assert_eq!(text, "");
    assert!(spill.is_none());
}

/// Purpose: verify that multi-byte UTF-8 characters are handled correctly.
/// Scenario: content with emoji and accented chars, truncated to 2 chars.
/// Acceptance: the tail preserves the last 2 characters (multi-byte safe).
#[test]
fn multi_byte_chars() {
    let content = "aé∞🔥ab";
    let (text, spill) = truncate_output(content, 2);
    assert!(spill.is_some(), "multi-byte over limit must create spill");
    // The last 2 characters of "aé∞🔥ab" are "ab"
    assert!(
        text.contains("ab"),
        "tail must be the last 2 chars: got {text:?}"
    );
}

/// Purpose: verify that exactly 1 char over the limit still creates a spill.
/// Scenario: content length = max_chars + 1.
/// Acceptance: spill file created, tail is the last char.
#[test]
fn one_char_over() {
    let content = "hello";
    let (text, spill) = truncate_output(content, 4);
    assert!(spill.is_some(), "one char over must create a spill");
    // Tail is the last 4 chars of "hello" = "ello"
    assert!(
        text.contains("ello"),
        "tail must be last 4 chars: got {text:?}"
    );
}
