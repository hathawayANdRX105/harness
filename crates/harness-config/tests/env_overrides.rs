//! Tests for env variable override behavior in config loading.
//!
//! The loading order is: code defaults ← TOML file ← env overrides. These
//! tests verify that env vars correctly override TOML values and code defaults.
//!
//! Scenarios:
//! - Env var overrides a TOML value for a scalar field.
//! - Env var fills in a field that was absent from the TOML (code default
//!   overridden).
//! - Comma-separated env vars override list fields (registry hooks/skills).
//! - Env var parse errors produce a `TypeError` with the env var name.
//!
//! Acceptance: every test writes a real TOML file, sets the relevant env var,
//! calls [`harness_config::load_from_path`], and asserts the override took
//! effect. Env vars are restored after each test via `EnvGuard`.

use std::io::Write;
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;

/// Serializes env mutation across the file's tests: plain `cargo test` runs
/// tests as threads of one process sharing the process env (nextest runs them
/// as separate processes), so a parallel sibling mid-`load_from_path` would
/// observe another test's override. Every EnvGuard holds this lock for its
/// whole lifetime.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Helper: set an env var for the duration of a test.
struct EnvGuard {
    key: String,
    previous: Option<String>,
    _lock: MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn set(key: &str, value: &str) -> Self {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var(key).ok();
        // `set_var` is unsafe in edition 2024; this test owns every
        // variable it touches and holds ENV_LOCK so siblings cannot
        // observe the mutation.
        unsafe { std::env::set_var(key, value) };
        Self {
            key: key.into(),
            previous,
            _lock: lock,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(v) => unsafe { std::env::set_var(&self.key, v) },
            None => unsafe { std::env::remove_var(&self.key) },
        }
    }
}

/// Write a TOML string to a file inside a temp dir and return the path.
fn write_toml(dir: &TempDir, content: &str) -> Result<std::path::PathBuf, std::io::Error> {
    let path = dir.path().join("config.toml");
    let mut f = std::fs::File::create(&path)?;
    write!(f, "{content}")?;
    Ok(path)
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// Purpose: env var `HARNESS_UPSTREAM_BASE_URL` overrides the TOML value for
/// `upstream.base_url`.
/// Scenarios: TOML sets `base_url` to one value, env var sets another.
/// Acceptance: the loaded config uses the env var value.
#[test]
fn env_overrides_upstream_base_url() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = EnvGuard::set(
        "HARNESS_UPSTREAM_BASE_URL",
        "https://env-override.example.com",
    );
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"https://toml-value.example.com\"\n"
    })?;

    let config = harness_config::load_from_path(&path)?;
    assert_eq!(
        config.upstream.base_url, "https://env-override.example.com",
        "env var should override TOML"
    );
    Ok(())
}

/// Purpose: env var `HARNESS_SESSION_DIRECTORY` fills in a session directory
/// when the TOML file has no `[session]` section at all.
/// Scenarios: no `[session]` in TOML; env var is set.
/// Acceptance: the loaded config uses the env var value, not the code default.
#[test]
fn env_fills_session_directory() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = EnvGuard::set("HARNESS_SESSION_DIRECTORY", "/env/sessions");
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"http://localhost:8080\"\n"
    })?;

    let config = harness_config::load_from_path(&path)?;
    assert_eq!(
        config.session.directory, "/env/sessions",
        "env var should fill session directory"
    );
    Ok(())
}

/// Purpose: env var `HARNESS_REGISTRY_HOOKS` (comma-separated) overrides the
/// TOML hooks list.
/// Scenarios: TOML has one hooks path; env var has two paths.
/// Acceptance: the loaded config uses the env var's two paths.
#[test]
fn env_overrides_registry_hooks() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = EnvGuard::set("HARNESS_REGISTRY_HOOKS", "/env/hooks/a,/env/hooks/b");
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        concat!(
            "version = 1\n",
            "\n",
            "[upstream]\n",
            "base_url = \"http://localhost:8080\"\n",
            "\n",
            "[registry]\n",
            "hooks = [\"/toml/hooks\"]\n",
        )
    })?;

    let config = harness_config::load_from_path(&path)?;
    assert_eq!(
        config.registry.hooks,
        vec!["/env/hooks/a".to_string(), "/env/hooks/b".to_string()],
        "env var should override TOML hooks"
    );
    Ok(())
}

/// Purpose: an env var with an unparseable integer produces a `TypeError`
/// containing the env var name.
/// Scenarios: `HARNESS_BUDGET_MAX_TURNS` set to a non-numeric string.
/// Acceptance: the error is a `TypeError` and mentions the env var.
#[test]
fn env_var_parse_error_is_type_error() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = EnvGuard::set("HARNESS_BUDGET_MAX_TURNS", "not-a-number");
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"http://localhost:8080\"\n"
    })?;

    let result = harness_config::load_from_path(&path);
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("invalid env var value should have failed".into()),
    };
    assert!(
        msg.contains("type error") && msg.contains("HARNESS_BUDGET_MAX_TURNS"),
        "error should mention type error and env var, got: {msg}"
    );
    Ok(())
}
