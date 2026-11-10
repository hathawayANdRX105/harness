//! Tests for file-based config loading: TOML parsing, migration, and
//! end-to-end deserialization.
//!
//! Scenarios:
//! - A minimal config file (required fields only) is loaded successfully with
//!   code defaults filling in optional sections.
//! - A full config file with every section is loaded and all field values
//!   match the input.
//! - A config without an explicit `version` field (treated as v0) migrates
//!   to v1 automatically.
//! - An empty config without the required `upstream` section fails with a
//!   missing-field error.
//! - A config with version > CURRENT_CONFIG_VERSION fails with a migration
//!   error.
//!
//! Acceptance: every test writes a real TOML file to a temp directory and
//! calls [`harness_config::load_from_path`] against it.

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

/// Purpose: a minimal config (only `upstream.base_url`) should load and fill
/// every other field with its code-level default.
/// Scenarios: required field present; optional fields absent.
/// Acceptance: all optional sections have their default values.
#[test]
fn minimal_config_loads() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 1\n\n[upstream]\nbase_url = \"https://api.example.com/v1\"\n"
    })?;

    let config = harness_config::load_from_path(&path)?;
    assert_eq!(config.version, 1, "version from file");
    assert_eq!(
        config.upstream.base_url, "https://api.example.com/v1",
        "upstream.base_url from TOML"
    );
    assert_eq!(
        config.upstream.api_key_var, "HARNESS_API_KEY",
        "api_key_var code default"
    );
    assert!(
        config.models.aliases.is_empty(),
        "no model aliases by default"
    );
    assert_eq!(
        config.session.directory, "~/.local/share/harness/sessions",
        "session directory code default"
    );
    assert_eq!(config.budget.max_turns, 50, "budget.max_turns code default");
    assert!(config.mcp.servers.is_empty(), "no MCP servers by default");
    assert!(config.registry.hooks.is_empty(), "no hooks by default");
    Ok(())
}

/// Purpose: a config with every section populated should round-trip through
/// the loader without data loss.
/// Scenarios: nested arrays (model aliases, MCP server args), optional fields
/// (`upstream`, `env_var`), all scalar types.
/// Acceptance: every field in the loaded `Config` matches the TOML input.
#[test]
fn full_config_loads_correctly() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        concat!(
            "version = 1\n",
            "\n",
            "[upstream]\n",
            "base_url = \"https://api.openai.com/v1\"\n",
            "api_key_var = \"OPENAI_API_KEY\"\n",
            "\n",
            "[models]\n",
            "aliases = [\n",
            "  { name = \"gpt4\", model_id = \"gpt-4-turbo\" },\n",
            "  { name = \"claude\", model_id = \"claude-3-opus\", upstream = \"anthropic\" },\n",
            "]\n",
            "\n",
            "[session]\n",
            "directory = \"/var/harness/sessions\"\n",
            "\n",
            "[budget]\n",
            "max_input_tokens = 100000\n",
            "max_output_tokens = 4000\n",
            "max_turns = 25\n",
            "\n",
            "[mcp]\n",
            "servers = [\n",
            "  { name = \"filesystem\", command = \"npx\", args = [\"-y\", \"@mcps/filesystem\"], env_var = \"FS_KEY\" },\n",
            "]\n",
            "\n",
            "[registry]\n",
            "hooks = [\"/usr/lib/harness/hooks/\", \"/etc/harness/hooks/\"]\n",
            "skills = [\"/usr/lib/harness/skills/\"]\n",
        )
    })?;

    let config = harness_config::load_from_path(&path)?;

    assert_eq!(config.version, 1);
    assert_eq!(config.upstream.base_url, "https://api.openai.com/v1");
    assert_eq!(config.upstream.api_key_var, "OPENAI_API_KEY");

    assert_eq!(config.models.aliases.len(), 2);
    assert_eq!(config.models.aliases[0].name, "gpt4");
    assert_eq!(config.models.aliases[0].model_id, "gpt-4-turbo");
    assert!(config.models.aliases[0].upstream.is_none());
    assert_eq!(config.models.aliases[1].name, "claude");
    assert_eq!(
        config.models.aliases[1].upstream.as_deref(),
        Some("anthropic")
    );

    assert_eq!(config.session.directory, "/var/harness/sessions");
    assert_eq!(config.budget.max_input_tokens, 100_000);
    assert_eq!(config.budget.max_output_tokens, 4_000);
    assert_eq!(config.budget.max_turns, 25);

    assert_eq!(config.mcp.servers.len(), 1);
    assert_eq!(config.mcp.servers[0].name, "filesystem");
    assert_eq!(config.mcp.servers[0].command, "npx");
    assert_eq!(
        config.mcp.servers[0].args,
        vec!["-y".to_string(), "@mcps/filesystem".to_string()]
    );
    assert_eq!(config.mcp.servers[0].env_var.as_deref(), Some("FS_KEY"));

    assert_eq!(config.registry.hooks.len(), 2);
    assert_eq!(config.registry.skills.len(), 1);
    Ok(())
}

/// Purpose: a config without an explicit `version` field should be treated as
/// version 0 and migrated to v1 during loading.
/// Scenarios: no version key.
/// Acceptance: the loaded config has `version == 1` and all fields intact.
#[test]
fn versionless_config_migrates_to_v1() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "\n[upstream]\nbase_url = \"http://localhost:8080\"\n"
    })?;

    let config = harness_config::load_from_path(&path)?;
    assert_eq!(config.version, 1, "migrated to version 1");
    assert_eq!(config.upstream.base_url, "http://localhost:8080");
    Ok(())
}

/// Purpose: a config that lacks the required `upstream` section should fail
/// with an error mentioning "missing field" and "upstream".
/// Scenarios: only `version` present, no `[upstream]` section.
/// Acceptance: the error type mentions `missing field` and `upstream`.
#[test]
fn empty_config_fails_missing_upstream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, "version = 1\n")?;

    let result = harness_config::load_from_path(&path);
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("config without upstream should have failed".into()),
    };
    assert!(
        err.contains("missing field") && err.contains("upstream"),
        "error should mention missing upstream, got: {err}"
    );
    Ok(())
}

/// Purpose: a config whose version exceeds `CURRENT_CONFIG_VERSION` should
/// fail with a migration error.
/// Scenarios: `version = 99` with otherwise valid content.
/// Acceptance: the error mentions "migration" and "newer".
#[test]
fn future_version_fails() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = write_toml(&dir, {
        "version = 99\n\n[upstream]\nbase_url = \"http://localhost:8080\"\n"
    })?;

    let result = harness_config::load_from_path(&path);
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => return Err("future version should have failed".into()),
    };
    assert!(
        err.contains("migration") && err.contains("newer"),
        "error should mention newer version, got: {err}"
    );
    Ok(())
}
