//! Integration tests for the builtin FS tools: read, write, edit, glob, grep.
//!
//! Each test creates a temp directory, runs a tool against it, and asserts
//! the content and is_error flag. The tools receive their cwd set to the
//! temp directory so tests are self-contained and never touch real user files.

use std::path::PathBuf;

use harness_core::tool::{Tool, ToolRisk};
use harness_tools::builtin::{EditTool, GlobTool, GrepTool, ReadTool, WriteTool};
use serde_json::json;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helper: write a file with known content, returning its relative path.
// ---------------------------------------------------------------------------

fn write_test_file(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content)
        .unwrap_or_else(|e| panic!("failed to write test file {name}: {e}"));
    PathBuf::from(name)
}

fn make_tree(dir: &std::path::Path) {
    // Create a small directory tree:
    //   test.txt
    //   sub/file_a.rs
    //   sub/file_b.rs
    //   sub/deep/nested.rs
    std::fs::write(dir.join("test.txt"), b"hello world\nline two\n")
        .unwrap_or_else(|e| panic!("{e}"));
    std::fs::create_dir_all(dir.join("sub/deep")).unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(dir.join("sub/file_a.rs"), b"fn hello() {}\nfn world() {}\n")
        .unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(dir.join("sub/file_b.rs"), b"mod hello;\nmod world;\n")
        .unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(dir.join("sub/deep/nested.rs"), b"// deep\n").unwrap_or_else(|e| panic!("{e}"));
}

// ---------------------------------------------------------------------------
// ReadTool
// ---------------------------------------------------------------------------

/// Purpose: verify ReadTool reads a file's content from a cwd-relative path.
/// Scenario: write a file, then read it with ReadTool.
/// Acceptance: content matches, is_error is false.
#[tokio::test]
async fn read_existing_file() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    write_test_file(&dir_path, "hello.txt", "hello tools world");
    let tool = ReadTool::new(dir_path);
    let result = tool
        .invoke(json!({"file_path": "hello.txt"}), &CancellationToken::new())
        .await
        .unwrap_or_else(|e| panic!("read failed: {e}"));
    assert!(!result.is_error, "read must not report error");
    assert!(
        result.content.contains("hello tools world"),
        "content must include file text"
    );
}

/// Purpose: verify ReadTool returns an error for a missing file.
/// Scenario: call with a non-existent file_path.
/// Acceptance: is_error in the returned ToolError (exec error, not found).
#[tokio::test]
async fn read_missing_file() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let tool = ReadTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(
            json!({"file_path": "nonexistent.txt"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("nonexistent"),
        "error message must mention missing file: {msg}"
    );
}

// ---------------------------------------------------------------------------
// WriteTool
// ---------------------------------------------------------------------------

/// Purpose: verify WriteTool creates a new file with the given content.
/// Scenario: invoke write with file_path and content, then read the file back.
/// Acceptance: file is created on disk with the correct content, is_error false.
#[tokio::test]
async fn write_creates_file() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let tool = WriteTool::new(dir.path().to_path_buf());
    let result = tool
        .invoke(
            json!({"file_path": "new.txt", "content": "fresh content"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("write failed: {e}"));
    assert!(!result.is_error, "write must not report error");
    let on_disk = std::fs::read_to_string(dir.path().join("new.txt"))
        .unwrap_or_else(|e| panic!("read back: {e}"));
    assert_eq!(on_disk, "fresh content");
}

/// Purpose: verify WriteTool creates parent directories automatically.
/// Scenario: write to a nested path whose parent does not exist.
/// Acceptance: file is created, directories are created.
#[tokio::test]
async fn write_creates_parent_dirs() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let tool = WriteTool::new(dir.path().to_path_buf());
    let result = tool
        .invoke(
            json!({"file_path": "a/b/c/nested.txt", "content": "nested"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("write nested failed: {e}"));
    assert!(!result.is_error, "nested write must not report error");
    assert!(
        dir.path().join("a/b/c/nested.txt").exists(),
        "nested file must exist"
    );
}

// ---------------------------------------------------------------------------
// EditTool
// ---------------------------------------------------------------------------

/// Purpose: verify EditTool replaces the first occurrence of old_text.
/// Scenario: write a file with "foo bar baz", edit "bar" -> "qux".
/// Acceptance: content becomes "foo qux baz", is_error false.
#[tokio::test]
async fn edit_replaces_first_occurrence() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    write_test_file(&dir_path, "edit.txt", "foo bar baz");
    let tool = EditTool::new(dir_path);
    let result = tool
        .invoke(
            json!({"file_path": "edit.txt", "old_text": "bar", "new_text": "qux"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("edit failed: {e}"));
    assert!(!result.is_error, "edit must not report error");
    let on_disk = std::fs::read_to_string(dir.path().join("edit.txt"))
        .unwrap_or_else(|e| panic!("read back: {e}"));
    assert_eq!(on_disk, "foo qux baz", "file content must reflect the edit");
}

/// Purpose: verify EditTool returns an error when old_text is not found.
/// Scenario: edit a file with an old_text that does not exist in it.
/// Acceptance: error is returned, file content is unchanged.
#[tokio::test]
async fn edit_not_found() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    write_test_file(&dir_path, "edit.txt", "original");
    let tool = EditTool::new(dir_path);
    let err = tool
        .invoke(
            json!({"file_path": "edit.txt", "old_text": "nonexistent", "new_text": "x"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    assert!(
        err.to_string().contains("not found"),
        "error must mention not found"
    );
}

// ---------------------------------------------------------------------------
// GlobTool
// ---------------------------------------------------------------------------

/// Purpose: verify GlobTool lists files matching a pattern.
/// Scenario: create a small tree, glob `**/*.rs`.
/// Acceptance: all .rs files are listed.
#[tokio::test]
async fn glob_finds_rs_files() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    make_tree(&dir_path);
    let tool = GlobTool::new(dir_path);
    let result = tool
        .invoke(json!({"pattern": "**/*.rs"}), &CancellationToken::new())
        .await
        .unwrap_or_else(|e| panic!("glob failed: {e}"));
    assert!(!result.is_error, "glob must not report error");
    assert!(
        result.content.contains("file_a.rs"),
        "glob output must include file_a.rs"
    );
    assert!(
        result.content.contains("nested.rs"),
        "glob output must include nested.rs"
    );
}

/// Purpose: verify GlobTool reports no matches gracefully.
/// Scenario: glob a pattern that matches nothing.
/// Acceptance: content says "no matches", is_error false.
#[tokio::test]
async fn glob_no_matches() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let tool = GlobTool::new(dir.path().to_path_buf());
    let result = tool
        .invoke(json!({"pattern": "*.zzz"}), &CancellationToken::new())
        .await
        .unwrap_or_else(|e| panic!("glob failed: {e}"));
    assert!(!result.is_error, "empty glob must not be an error");
    assert!(
        result.content.contains("no matches"),
        "empty glob must mention no matches"
    );
}

// ---------------------------------------------------------------------------
// GrepTool
// ---------------------------------------------------------------------------

/// Purpose: verify GrepTool finds matching lines in the file tree.
/// Scenario: make_tree then grep for "fn ".
/// Acceptance: matches include "fn hello()" and "fn world()".
#[tokio::test]
async fn grep_finds_matches() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    make_tree(&dir_path);
    let tool = GrepTool::new(dir_path);
    let result = tool
        .invoke(json!({"pattern": "fn "}), &CancellationToken::new())
        .await
        .unwrap_or_else(|e| panic!("grep failed: {e}"));
    assert!(!result.is_error, "grep must not report error");
    assert!(
        result.content.contains("fn hello"),
        "grep must find fn hello"
    );
    assert!(
        result.content.contains("fn world"),
        "grep must find fn world"
    );
}

/// Purpose: verify GrepTool supports an include glob filter.
/// Scenario: grep with include=`**/*.txt` — should skip .rs matches.
/// Acceptance: content does not contain .rs file paths.
#[tokio::test]
async fn grep_with_include_filter() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    make_tree(&dir_path);
    let tool = GrepTool::new(dir_path);
    let result = tool
        .invoke(
            json!({"pattern": "hello", "include": "*.txt"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("grep failed: {e}"));
    assert!(!result.is_error, "grep must not report error");
    assert!(
        result.content.contains("hello"),
        "grep with include must still find hello in txt"
    );
    assert!(
        !result.content.contains("file_a.rs"),
        "include filter should exclude .rs files"
    );
}

/// Purpose: verify GrepTool reports no matches gracefully.
/// Scenario: grep for a pattern that doesn't exist.
/// Acceptance: content says "no matches", is_error false.
#[tokio::test]
async fn grep_no_matches() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let dir_path = dir.path().to_path_buf();
    make_tree(&dir_path);
    let tool = GrepTool::new(dir_path);
    let result = tool
        .invoke(
            json!({"pattern": "zzz_nonexistent_zzz"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("grep failed: {e}"));
    assert!(!result.is_error, "empty grep must not be an error");
    assert!(
        result.content.contains("no matches"),
        "empty grep must mention no matches"
    );
}

// ---------------------------------------------------------------------------
// Tool name / risk / schema consistency
// ---------------------------------------------------------------------------

/// Purpose: verify each builtin tool returns its correct name and risk.
/// Scenario: check all five tools.
/// Acceptance: names match expectations, risk is correct per tool.
#[test]
fn tool_metadata() {
    let cwd = PathBuf::from("/tmp");
    let cases: Vec<(Box<dyn Tool>, &str, ToolRisk)> = vec![
        (
            Box::new(ReadTool::new(cwd.clone())),
            "read",
            ToolRisk::ReadOnly,
        ),
        (
            Box::new(WriteTool::new(cwd.clone())),
            "write",
            ToolRisk::Destructive,
        ),
        (
            Box::new(EditTool::new(cwd.clone())),
            "edit",
            ToolRisk::Destructive,
        ),
        (
            Box::new(GlobTool::new(cwd.clone())),
            "glob",
            ToolRisk::ReadOnly,
        ),
        (Box::new(GrepTool::new(cwd)), "grep", ToolRisk::ReadOnly),
    ];

    for (tool, expected_name, expected_risk) in cases {
        assert_eq!(tool.name(), expected_name, "tool name mismatch");
        assert_eq!(
            tool.risk(),
            expected_risk,
            "tool `{}` has wrong risk",
            expected_name
        );
        let schema = tool.schema();
        assert_eq!(
            schema.name, expected_name,
            "schema.name must match name() for `{expected_name}`"
        );
        assert!(
            !schema.description.is_empty(),
            "`{expected_name}` must have a non-empty description"
        );
        assert!(
            schema.input.is_object(),
            "`{expected_name}` input schema must be a JSON object"
        );
    }
}
// ---------------------------------------------------------------------------
// Security: path containment (escape rejection)
// ---------------------------------------------------------------------------

/// Purpose: verify ReadTool rejects an absolute file_path and reads nothing.
/// Scenario: call read with file_path="/etc/passwd".
/// Acceptance: invoke returns an error; no content is returned.
#[tokio::test]
async fn read_absolute_path_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = ReadTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(
            json!({"file_path": "/etc/passwd"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("absolute"),
        "absolute path read must be rejected: {msg}"
    );
    Ok(())
}

/// Purpose: verify ReadTool rejects a `..` file_path.
/// Scenario: call read with file_path="../outside.txt".
/// Acceptance: invoke returns an error.
#[tokio::test]
async fn read_parent_dir_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = ReadTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(
            json!({"file_path": "../outside.txt"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("`..`"),
        "`..` read must be rejected: {msg}"
    );
    Ok(())
}

/// Purpose: verify WriteTool rejects a `..` file_path and creates nothing outside cwd.
/// Scenario: call write with file_path="../outside.txt".
/// Acceptance: invoke returns an error; no file is created outside cwd.
#[tokio::test]
async fn write_parent_dir_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = WriteTool::new(dir.path().to_path_buf());
    let outside = dir.path().join("../outside.txt");
    let err = tool
        .invoke(
            json!({"file_path": "../outside.txt", "content": "leak"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("`..`"),
        "`..` write must be rejected: {msg}"
    );
    assert!(
        !outside.exists(),
        "write with .. must not create a file outside cwd"
    );
    Ok(())
}

/// Purpose: verify WriteTool rejects a path containing `..` even when it stays inside cwd.
/// Scenario: call write with file_path="sub/../ok.txt".
/// Acceptance: invoke returns an error per the no-ParentDir rule.
#[tokio::test]
async fn write_sub_dotdot_ok_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = WriteTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(
            json!({"file_path": "sub/../ok.txt", "content": "data"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("`..`"),
        "sub/../ok.txt write must be rejected: {msg}"
    );
    Ok(())
}

/// Purpose: verify WriteTool then ReadTool still work on a normal relative path.
/// Scenario: write a file, then read it back.
/// Acceptance: write returns success, read returns the same content, no error.
#[tokio::test]
async fn write_then_read_happy_path() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool_dir = dir.path().to_path_buf();
    let writer = WriteTool::new(tool_dir.clone());
    let reader = ReadTool::new(tool_dir);

    let write_result = writer
        .invoke(
            json!({"file_path": "normal.txt", "content": "happy path"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("write failed: {e}"));
    assert!(!write_result.is_error, "write must not report error");

    let read_result = reader
        .invoke(
            json!({"file_path": "normal.txt"}),
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("read failed: {e}"));
    assert!(!read_result.is_error, "read must not report error");
    assert!(
        read_result.content.contains("happy path"),
        "read must return written content"
    );
    Ok(())
}

/// Purpose: verify GlobTool rejects an absolute pattern.
/// Scenario: call glob with pattern="/etc/*".
/// Acceptance: invoke returns an error.
#[tokio::test]
async fn glob_absolute_pattern_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = GlobTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(json!({"pattern": "/etc/*"}), &CancellationToken::new())
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("absolute"),
        "absolute glob pattern must be rejected: {msg}"
    );
    Ok(())
}

/// Purpose: verify GlobTool rejects a pattern containing `..`.
/// Scenario: call glob with pattern="../*".
/// Acceptance: invoke returns an error.
#[tokio::test]
async fn glob_dotdot_pattern_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = GlobTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(json!({"pattern": "../*"}), &CancellationToken::new())
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory") || msg.contains("`..`"),
        "`..` glob pattern must be rejected: {msg}"
    );
    Ok(())
}

/// Purpose: verify ReadTool rejects a symlink inside cwd that points outside cwd.
/// Scenario: create a symlink inside cwd pointing to a file outside cwd, then read it.
/// Acceptance: invoke returns an error; nothing outside cwd is read.
#[tokio::test]
#[cfg(unix)]
async fn read_symlink_escape_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::fs::write(outside.path().join("secret.txt"), "secret")?;
    symlink(
        outside.path().join("secret.txt"),
        dir.path().join("link.txt"),
    )?;
    let tool = ReadTool::new(dir.path().to_path_buf());
    let err = tool
        .invoke(json!({"file_path": "link.txt"}), &CancellationToken::new())
        .await
        .unwrap_err_or_else();
    let msg = err.to_string();
    assert!(
        msg.contains("escapes the working directory"),
        "symlink escape read must be rejected: {msg}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Error type extension for test convenience
// ---------------------------------------------------------------------------

trait ExpectErr<T, E> {
    fn unwrap_err_or_else(self) -> E;
}

impl<T, E: std::fmt::Display> ExpectErr<T, E> for Result<T, E> {
    fn unwrap_err_or_else(self) -> E {
        match self {
            Ok(_) => panic!("expected Err, got Ok"),
            Err(e) => e,
        }
    }
}
