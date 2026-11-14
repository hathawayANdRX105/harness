//! Hierarchical config loading: code defaults ← TOML file ← env overrides.
//!
//! # Load order
//! 1. Parse the TOML file; serde's `#[serde(default)]` fills in any fields the
//!    file omitted (the **code defaults** layer).
//! 2. Run schema migrations on the raw TOML table.
//! 3. Apply env variable overrides for known keys (the **env overrides** layer).
//!
//! # All values without a TOML or env source carry their code default.
//!
//! Design provenance: ferrite's `ConfigHandle` (`config/src/lib.rs:581`)
//! provides the three-layer overlay pattern (env > DB > code-default) that
//! this module adapts for TOML+env-only, removing the DB layer.

use std::path::Path;

use crate::config::*;
use crate::error::{self, ConfigError};
use toml::Table;

/// Load the config from the default file path, run migrations, and apply env
/// overrides.
///
/// Returns the overlay: code defaults ← file ← env.
///
/// When the config file does not exist, returns an error (the daemon requires
/// explicit configuration). Use [`load_or_defaults`] for a fallback variant.
pub fn load() -> Result<Config, ConfigError> {
    let path = resolve_config_path()?;
    load_from(&path)
}

/// Like [`load`], but returns code defaults when no config file is found
/// (useful for unit tests and minimal dev setups).
pub fn load_or_defaults() -> Result<Config, ConfigError> {
    match resolve_config_path() {
        Ok(path) if path.exists() => load_from(&path),
        _ => {
            // Code defaults only: the literal has all required fields, serde
            // fills in `#[serde(default)]` for everything else.
            let config = defaults_only();
            apply_env_overrides(config)
        }
    }
}

/// Load config from a specific file path, run migrations, and apply env
/// overrides.
///
/// This is the primary entry point for test code that wants to control the
/// file path directly instead of going through `$HARNESS_CONFIG` resolution.
pub fn load_from_path(path: &Path) -> Result<Config, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;

    let mut table: Table = toml::de::from_str(&content).map_err(|e| {
        let file = path.display().to_string();
        error::classify_toml_error(&e, &file, &content)
    })?;

    // Run schema migrations (versionless → v1, v1 → v2, …).
    let file_version = table
        .get("version")
        .and_then(|v| v.as_integer())
        .map_or(0, |n| n as u32);
    crate::migration::run_migrations(&mut table)?;

    // Deserialize from the original text whenever no migration rewrote the
    // table: toml's spans then point at the user's real file:line instead of
    // at a regenerated copy. A migration changes the values, so only the
    // regenerated text describes what is being deserialized.
    let source_text = if file_version == CURRENT_CONFIG_VERSION {
        content
    } else {
        toml::to_string(&toml::Value::Table(table)).map_err(|_| ConfigError::Migration {
            from_version: file_version,
            target_version: CURRENT_CONFIG_VERSION,
            detail: "migration produced invalid TOML — internal error".into(),
        })?
    };

    // `toml`'s own type errors omit the offending key; the serde path is the
    // only way to name the field (`budget.max_turns`) in the user's message.
    let config: Config = serde_path_to_error::deserialize(toml::de::Deserializer::new(
        &source_text,
    ))
    .map_err(|e| {
        let file = path.display().to_string();
        let field_path = e.path().to_string();
        let inner = e.into_inner();
        error::classify_toml_error_with_path(&inner, &file, &source_text, &field_path)
    })?;

    // Apply env overrides on top of the file config.
    apply_env_overrides(config)
}

/// Internal alias used by the public `load` and `load_or_defaults` entries.
fn load_from(path: &Path) -> Result<Config, ConfigError> {
    load_from_path(path)
}

/// Build a Config with only code-level defaults.
///
/// This is the base layer returned when no config file exists:
/// every section gets its `#[serde(default)]` value, and `upstream`
/// gets a localhost development default so the result is self-contained.
fn defaults_only() -> Config {
    Config {
        version: CURRENT_CONFIG_VERSION,
        upstream: UpstreamConfig {
            base_url: "http://localhost:3211/v1".into(),
            api_key_var: default_api_key_var(),
        },
        models: ModelsConfig { aliases: vec![] },
        session: SessionConfig {
            directory: default_session_dir(),
            auth_token_var: None,
        },
        budget: BudgetConfig {
            max_input_tokens: default_max_input_tokens(),
            max_output_tokens: default_max_output_tokens(),
            max_turns: default_max_turns(),
        },
        mcp: McpConfig { servers: vec![] },
        registry: RegistryConfig {
            hooks: vec![],
            skills: vec![],
        },
        scheduler: SchedulerConfig::default(),
    }
}

/// Resolve the config file path.
///
/// Priority: `$HARNESS_CONFIG` (a set-but-empty value counts as unset), then
/// the XDG config dir — `$XDG_CONFIG_HOME/harness/config.toml`, falling back
/// to the XDG default `$HOME/.config/harness/config.toml`.
fn resolve_config_path() -> Result<std::path::PathBuf, ConfigError> {
    if let Some(path) = std::env::var_os("HARNESS_CONFIG")
        && !path.is_empty()
    {
        return Ok(std::path::PathBuf::from(path));
    }
    match std::env::var_os("XDG_CONFIG_HOME") {
        Some(dir) if !dir.is_empty() => {
            Ok(std::path::PathBuf::from(dir).join("harness/config.toml"))
        }
        _ => home_config_path(),
    }
}

/// `$HOME/.config/harness/config.toml` — the XDG fallback used when
/// `XDG_CONFIG_HOME` is unset.
fn home_config_path() -> Result<std::path::PathBuf, ConfigError> {
    let home = std::env::var("HOME").map_err(|_| ConfigError::MissingField {
        field: "HOME".into(),
        file: "<env>".into(),
        line: 0,
    })?;
    Ok(std::path::Path::new(&home).join(".config/harness/config.toml"))
}

// ── Env overrides ───────────────────────────────────────────────────────────

/// Apply env variable overrides to a loaded Config, returning the overridden
/// config.
///
/// Each key's env var name is derived by prefixing `HARNESS_` and uppercasing
/// the TOML path. Supported overrides:
///
/// | Env var                          | Config field           |
/// | -------------------------------- | ---------------------- |
/// | `HARNESS_UPSTREAM_BASE_URL`      | `upstream.base_url`    |
/// | `HARNESS_UPSTREAM_API_KEY_VAR`   | `upstream.api_key_var` |
/// | `HARNESS_SESSION_DIRECTORY`      | `session.directory`    |
/// | `HARNESS_BUDGET_MAX_INPUT_TOKENS`| `budget.max_input_tokens` |
/// | `HARNESS_BUDGET_MAX_OUTPUT_TOKENS`| `budget.max_output_tokens` |
/// | `HARNESS_BUDGET_MAX_TURNS`       | `budget.max_turns`    |
/// | `HARNESS_REGISTRY_HOOKS`         | `registry.hooks` (comma-separated) |
/// | `HARNESS_REGISTRY_SKILLS`        | `registry.skills` (comma-separated) |
///
/// One string env override: the env var name and the field setter.
type StringOverride = (&'static str, fn(&mut Config, String));
/// One integer env override: env var name, config field path (for the
/// TypeError), and the field setter.
type U32Override = (&'static str, &'static str, fn(&mut Config, u32));
/// One comma-separated list env override.
type ListOverride = (&'static str, fn(&mut Config, Vec<String>));

/// Only present env vars are applied; unset or empty vars are ignored.
fn apply_env_overrides(mut config: Config) -> Result<Config, ConfigError> {
    let overrides: &[StringOverride] = &[
        ("HARNESS_UPSTREAM_BASE_URL", |c, v| c.upstream.base_url = v),
        ("HARNESS_UPSTREAM_API_KEY_VAR", |c, v| {
            c.upstream.api_key_var = v
        }),
        ("HARNESS_SESSION_DIRECTORY", |c, v| c.session.directory = v),
    ];
    let int_overrides: &[U32Override] = &[
        (
            "HARNESS_BUDGET_MAX_INPUT_TOKENS",
            "budget.max_input_tokens",
            |c, v| c.budget.max_input_tokens = v,
        ),
        (
            "HARNESS_BUDGET_MAX_OUTPUT_TOKENS",
            "budget.max_output_tokens",
            |c, v| c.budget.max_output_tokens = v,
        ),
        ("HARNESS_BUDGET_MAX_TURNS", "budget.max_turns", |c, v| {
            c.budget.max_turns = v
        }),
    ];
    let list_overrides: &[ListOverride] = &[
        ("HARNESS_REGISTRY_HOOKS", |c, v| c.registry.hooks = v),
        ("HARNESS_REGISTRY_SKILLS", |c, v| c.registry.skills = v),
    ];

    for (name, setter) in overrides {
        if let Some(value) = non_empty_env(name) {
            setter(&mut config, value);
        }
    }
    for (name, field, setter) in int_overrides {
        if let Some(value) = non_empty_env(name) {
            let parsed: u32 = value.parse().map_err(|e| ConfigError::TypeError {
                field: field.to_string(),
                detail: format!("env var {name} cannot parse '{value}' as u32: {e}"),
                file: String::new(),
                line: 0,
            })?;
            setter(&mut config, parsed);
        }
    }
    for (name, setter) in list_overrides {
        if let Some(value) = non_empty_env(name) {
            setter(&mut config, split_list(&value));
        }
    }
    Ok(config)
}

/// The env var's value when set to something non-empty. Empty counts as
/// unset: a shell export with no value must never clobber the file layer.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn split_list(val: &str) -> Vec<String> {
    val.split(',').map(|s| s.trim().to_string()).collect()
}
