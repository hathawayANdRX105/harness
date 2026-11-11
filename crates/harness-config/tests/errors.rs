//! Tests for config error messages — the three required error kinds
//! (missing field, type error, unknown field) each produce a clear message
//! with file:line information.
//!
//! Scenarios:
//! - Missing required field (`upstream` section absent) → `MissingField`.
//! - Type mismatch (string where a number is expected) → `TypeError`.
//! - Unknown field in a section with `deny_unknown_fields` → `UnknownField`.
//!
//! Acceptance: every error message contains the field name and the source file
//! path (temp directory).

use std::io::Write;
use tempfile::TempDir;

/// Write a TOML string to a file inside a temp dir and return the path.
fn write_toml(dir: &TempDir, content: &str) -> Result<std::path::PathBuf, std::io::Error> {
    let path = dir.path().join("config.toml");
    let mut f = std::fs::File::create(&path)?;
    write!(f, "{content}")?;
    Ok(path)
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// Purpose: omitting the required `upstream` section produces a `MissingField`
/// error mentioning the field name and the file path.
/// Scenarios: TOML with only `version`, no `[upstream]`.
/// Acceptance: the error message contains "missing field", "upstream", and the
/// temp file path.
#[test]
fn missing_field_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, "version = 1\n")?;
    let file_display = path.display().to_string();

    let result = harness_config::load_from_path(&path);
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("config without upstream should have failed".into()),
    };

    assert!(
        msg.contains("missing field"),
        "should mention 'missing field', got: {msg}"
    );
    assert!(
        msg.contains("upstream"),
        "should mention 'upstream', got: {msg}"
    );
    assert!(
        msg.contains(&file_display),
        "should contain file path, got: {msg}"
    );
    Ok(())
}

/// Purpose: a field with the wrong type (e.g. string where u32 is expected)
/// produces a `TypeError` mentioning the field and the file path.
/// Scenarios: `max_turns = "not-a-number"` in the `[budget]` section.
/// Acceptance: the error message contains "type error", the field path, and
/// the file path.
#[test]
fn type_error_message() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"http://localhost:8080\"\n\n[budget]\nmax_turns = \"not-a-number\"\n"
    })?;
    let file_display = path.display().to_string();

    let result = harness_config::load_from_path(&path);
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("type mismatch should have failed".into()),
    };

    assert!(
        msg.contains("type error"),
        "should mention 'type error', got: {msg}"
    );
    assert!(
        msg.contains("max_turns") || msg.contains("budget"),
        "should mention the field, got: {msg}"
    );
    assert!(
        msg.contains(&file_display),
        "should contain file path, got: {msg}"
    );
    Ok(())
}

/// Purpose: an unknown field in a `deny_unknown_fields` section produces an
/// `UnknownField` error mentioning the field name and the file path.
/// Scenarios: `typo_field = true` inside the `[upstream]` section.
/// Acceptance: the error message contains "unknown field", the field name,
/// and the file path.
#[test]
fn unknown_field_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"http://localhost:8080\"\ntypo_field = true\n"
    })?;
    let file_display = path.display().to_string();

    let result = harness_config::load_from_path(&path);
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("unknown field should have failed".into()),
    };

    assert!(
        msg.contains("unknown field"),
        "should mention 'unknown field', got: {msg}"
    );
    assert!(
        msg.contains("typo_field"),
        "should mention the unknown field name, got: {msg}"
    );
    assert!(
        msg.contains(&file_display),
        "should contain file path, got: {msg}"
    );
    Ok(())
}
