//! Configuration sections — independent serde structs consumed by the model,
//! mcp, session, and builder upstream crates.
//!
//! Design provenance: ferrite/harness/config (`config/src/lib.rs`) provides the
//! port-based design that this layer refines for TOML+env-only (no DB backend).
//! pi's `settings-schema.ts` contributes the section-per-domain separation.
//!
//! # Security
//! Key-like fields store **variable names** only (e.g. `"OPENAI_API_KEY"`),
//! never the key value itself. The `Debug` impl masks these fields to prevent
//! accidental leakage.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Current config schema version. Incremented when a backward-incompatible
/// change to the TOML format is made; the loader runs [`Migration`](crate::Migration)s
/// to advance older configs to this version.
pub const CURRENT_CONFIG_VERSION: u32 = 1;

// ── Top-level config ───────────────────────────────────────────────────────

/// Harness daemon configuration, loaded from a TOML file with optional env
/// overrides.
///
/// Section fields use independent serde structs so that crates consuming a
/// section (model, mcp, builder) can deserialize just their portion without
/// importing the full `Config`.
///
/// # Defaults
/// Every section except `upstream` has a code-level default; `upstream` must
/// be present in the TOML file.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Schema version. Defaults to [`CURRENT_CONFIG_VERSION`] when omitted
    /// from the file.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Upstream API configuration (base URL, auth key variable).
    /// **Required** — the daemon needs at least one upstream address.
    pub upstream: UpstreamConfig,
    /// Model alias mappings.
    #[serde(default)]
    pub models: ModelsConfig,
    /// Session storage directory.
    #[serde(default)]
    pub session: SessionConfig,
    /// Token and run budget limits.
    #[serde(default)]
    pub budget: BudgetConfig,
    /// MCP server definitions for tool execution.
    #[serde(default)]
    pub mcp: McpConfig,
    /// Hook and skill registration sources.
    #[serde(default)]
    pub registry: RegistryConfig,
}

pub(crate) fn default_version() -> u32 {
    CURRENT_CONFIG_VERSION
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("version", &self.version)
            .field("upstream", &self.upstream)
            .field("models", &self.models)
            .field("session", &self.session)
            .field("budget", &self.budget)
            .field("mcp", &self.mcp)
            .field("registry", &self.registry)
            .finish()
    }
}

// ── Upstream ────────────────────────────────────────────────────────────────

/// Upstream API endpoint configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    /// Base URL of the upstream API (e.g. `"https://api.openai.com/v1"`).
    pub base_url: String,
    /// Environment variable name holding the API key (e.g. `"HARNESS_API_KEY"`).
    /// Stores the variable name only — never the key value.
    #[serde(default = "default_api_key_var")]
    pub api_key_var: String,
}

pub(crate) const DEFAULT_API_KEY_VAR: &str = "HARNESS_API_KEY";

pub(crate) fn default_api_key_var() -> String {
    DEFAULT_API_KEY_VAR.into()
}

/// Manual Debug impl: masks `api_key_var` so the env variable name does not
/// appear in logs or error output (defense-in-depth: the field stores a name,
/// not a secret, but we treat it conservatively).
impl fmt::Debug for UpstreamConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamConfig")
            .field("base_url", &self.base_url)
            .field("api_key_var", &"*masked*")
            .finish()
    }
}

// ── Models ──────────────────────────────────────────────────────────────────

/// Model alias mappings.
///
/// Design provenance: the model crate (#4) reads this section to build its
/// alias → model_id lookup table. An empty alias list is valid (the daemon
/// can still resolve models by full ID).
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelsConfig {
    /// Ordered list of model aliases. Evaluated in order; the first match
    /// wins during model resolution.
    #[serde(default)]
    pub aliases: Vec<ModelAlias>,
}

impl fmt::Debug for ModelsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelsConfig")
            .field("aliases", &self.aliases)
            .finish()
    }
}

/// A single model alias entry.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAlias {
    /// Short alias name used in the daemon's model selection
    /// (e.g. `"gpt4"` → `"gpt-4-turbo"`).
    pub name: String,
    /// Full model identifier for the upstream API.
    pub model_id: String,
    /// Optional upstream name to scope this alias. When `None`, the alias
    /// applies to the default (sole) upstream.
    #[serde(default)]
    pub upstream: Option<String>,
}

impl fmt::Debug for ModelAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelAlias")
            .field("name", &self.name)
            .field("model_id", &self.model_id)
            .field("upstream", &self.upstream)
            .finish()
    }
}

// ── Session ─────────────────────────────────────────────────────────────────

/// Session storage configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// Directory for session persistence. `~` is expanded at load time using
    /// the `HOME` env var. Defaults to `~/.local/share/harness/sessions`.
    #[serde(default = "default_session_dir")]
    pub directory: String,
    /// Environment variable name holding the UDS bearer token (read by the
    /// daemon at startup). Stores the variable name only — never the token
    /// value itself. `None` disables UDS token authentication.
    #[serde(default)]
    pub auth_token_var: Option<String>,
}

/// Default session persistence directory (under `$HOME`, expanded at load
/// time).
pub(crate) const DEFAULT_SESSION_DIR: &str = "~/.local/share/harness/sessions";

pub(crate) fn default_session_dir() -> String {
    DEFAULT_SESSION_DIR.into()
}

impl fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionConfig")
            .field("directory", &self.directory)
            .field(
                "auth_token_var",
                &self.auth_token_var.as_ref().map(|_| "*masked*"),
            )
            .finish()
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            directory: default_session_dir(),
            auth_token_var: None,
        }
    }
}

// ── Budget ──────────────────────────────────────────────────────────────────

/// Token and run budget limits.
///
/// Design provenance: the builder (#10) reads these defaults and passes them
/// to [`harness_core::run::RunLimits`] when creating a run context. The limits
/// here match the `RunLimits` defaults in harness-core.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    /// Maximum input tokens per run.
    #[serde(default = "default_max_input_tokens")]
    pub max_input_tokens: u32,
    /// Maximum output tokens per run.
    #[serde(default = "default_max_output_tokens")]
    pub max_output_tokens: u32,
    /// Maximum round-trips (turns) per run.
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
}

pub(crate) const DEFAULT_MAX_INPUT_TOKENS: u32 = 150_000;
pub(crate) const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 8_000;
pub(crate) const DEFAULT_MAX_TURNS: u32 = 50;

pub(crate) fn default_max_input_tokens() -> u32 {
    DEFAULT_MAX_INPUT_TOKENS
}

pub(crate) fn default_max_output_tokens() -> u32 {
    DEFAULT_MAX_OUTPUT_TOKENS
}

pub(crate) fn default_max_turns() -> u32 {
    DEFAULT_MAX_TURNS
}

impl fmt::Debug for BudgetConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetConfig")
            .field("max_input_tokens", &self.max_input_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("max_turns", &self.max_turns)
            .finish()
    }
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_input_tokens: default_max_input_tokens(),
            max_output_tokens: default_max_output_tokens(),
            max_turns: default_max_turns(),
        }
    }
}

// ── MCP ─────────────────────────────────────────────────────────────────────

/// MCP (Model Context Protocol) server definitions.
///
/// Design provenance: the mcp crate (#6) reads this section to start
/// subprocess servers at daemon startup. Each entry describes a subprocess
/// command and its arguments.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    /// List of MCP server entries to register at startup.
    #[serde(default)]
    pub servers: Vec<McpServerEntry>,
}

impl fmt::Debug for McpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpConfig")
            .field("servers", &self.servers)
            .finish()
    }
}

/// A single MCP server entry describing a subprocess to start.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerEntry {
    /// Human-readable name for this server (used in log messages and errors).
    pub name: String,
    /// Command to start the server process (e.g. `"npx"`, `"python"`).
    pub command: String,
    /// Arguments passed to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Optional env variable name for server-specific secrets.
    #[serde(default)]
    pub env_var: Option<String>,
}

/// Manual Debug impl: masks `env_var` to avoid leaking the variable name in
/// logs (defense-in-depth).
impl fmt::Debug for McpServerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpServerEntry")
            .field("name", &self.name)
            .field("command", &self.command)
            .field("args", &self.args)
            .field("env_var", &self.env_var.as_ref().map(|_| "*masked*"))
            .finish()
    }
}

// ── Registry ────────────────────────────────────────────────────────────────

/// Hook and skill registration sources.
///
/// Design provenance: the hooks/skills runtime reads these paths at startup
/// to discover available extensions. Paths support `~` expansion.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryConfig {
    /// Directories or paths to scan for hook scripts.
    #[serde(default)]
    pub hooks: Vec<String>,
    /// Directories or paths to scan for skill definitions.
    #[serde(default)]
    pub skills: Vec<String>,
}

impl fmt::Debug for RegistryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryConfig")
            .field("hooks", &self.hooks)
            .field("skills", &self.skills)
            .finish()
    }
}
