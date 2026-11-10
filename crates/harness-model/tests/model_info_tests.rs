//! Tests for the ModelInfo directory loader.
//!
//! Purpose: verify that `ModelInfoLoader::load_all` reads JSON model
//! definitions, skips non-JSON files, returns empty on missing directory,
//! and reports parse errors correctly.
//!
//! Scenarios:
//! - Non-existent directory returns empty Vec (not an error).
//! - Single JSON file loads into one ModelInfo entry.
//! - Multiple JSON files load into multiple entries.
//! - Non-JSON files are silently skipped.
//! - Malformed JSON returns a parse error.

use std::io::Write;

use harness_model::ModelInfoLoader;
use tempfile::TempDir;

/// A helper that writes a JSON model definition to a file.
fn write_model(dir: &std::path::Path, name: &str, json: &str) {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap_or_else(|e| panic!("create test file: {e}"));
    write!(f, "{json}").unwrap_or_else(|e| panic!("write test file: {e}"));
}

/// A missing directory yields an empty list.
#[test]
fn missing_directory_returns_empty() -> Result<(), Box<dyn std::error::Error>> {
    let result = ModelInfoLoader::load_all(std::path::Path::new("/tmp/__nonexistent_models__"));
    assert!(result.is_ok(), "missing dir should not be an error");
    assert!(result?.is_empty());
    Ok(())
}

/// A single valid JSON file produces one ModelInfo.
#[test]
fn single_json_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    write_model(
        dir.path(),
        "gpt4.json",
        r#"{
            "handle": "gpt-4o",
            "provider": "openai",
            "model": "gpt-4o-2024-08-06",
            "context_window": 128000
        }"#,
    );

    let mut models = ModelInfoLoader::load_all(dir.path())?;
    assert_eq!(models.len(), 1);
    let m = models.remove(0);
    assert_eq!(m.handle, "gpt-4o");
    assert_eq!(m.provider, "openai");
    assert_eq!(m.model, "gpt-4o-2024-08-06");
    assert_eq!(m.context_window, 128_000);
    assert!(m.supports_tool_use);
    assert!(m.supports_streaming);
    Ok(())
}

/// Multiple JSON files load into multiple ModelInfo entries.
#[test]
fn multiple_json_files() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    write_model(
        dir.path(),
        "fast.json",
        r#"{
            "handle": "fast",
            "provider": "openai",
            "model": "gpt-4o-mini",
            "context_window": 128000
        }"#,
    );
    write_model(
        dir.path(),
        "big.json",
        r#"{
            "handle": "big",
            "provider": "openai",
            "model": "gpt-4o",
            "context_window": 128000,
            "supports_tool_use": true,
            "supports_streaming": true
        }"#,
    );

    let models = ModelInfoLoader::load_all(dir.path())?;
    assert_eq!(models.len(), 2);
    Ok(())
}

/// Non-JSON files are silently skipped.
#[test]
fn skips_non_json_files() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    write_model(dir.path(), "notes.txt", "this is not json");
    write_model(
        dir.path(),
        "model.json",
        r#"{
            "handle": "m",
            "provider": "test",
            "model": "test-model",
            "context_window": 4096
        }"#,
    );

    let models = ModelInfoLoader::load_all(dir.path())?;
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].handle, "m");
    Ok(())
}

/// Malformed JSON returns a `LoadError::Parse`.
#[test]
fn malformed_json_returns_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    write_model(dir.path(), "broken.json", "this is not json");

    let result = ModelInfoLoader::load_all(dir.path());
    assert!(result.is_err(), "malformed JSON should produce an error");
    let err = result.err().unwrap_or_else(|| panic!("expected an error"));
    let msg = err.to_string();
    assert!(
        msg.contains("parse error"),
        "error should mention 'parse error': {msg}"
    );
    Ok(())
}
