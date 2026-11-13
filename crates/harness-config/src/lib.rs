//! Harness config: hierarchical TOML+env layered configuration loader.
//!
//! Provides the `Config` struct and its section types that the daemon uses
//! to resolve upstream addresses, model aliases, key variable names, session
//! directories, budget limits, MCP server lists, and hook/skill registration
//! sources.
//!
//! # Loading order
//! **Code defaults ← TOML file ← env overrides**. No database is involved.
//! The config file is read from `$HARNESS_CONFIG` or `~/.config/harness/config.toml`.
//!
//! # Design provenance
//! - **ferrite/harness/config** (Rust): port-based `ConfigSource` trait and
//!   `ConfigHandle` overlay pattern (env > DB > code-default), adapted here
//!   for TOML+env-only — the DB layer is removed because this crate does not
//!   read or write a database.
//! - **pi's settings-manager** (TypeScript): `FileSettingsStorage` +
//!   `SettingsManager` triple-layer merge, `migrations.ts` schema evolution,
//!   and section-per-domain separation.
//! - **harness-core**: reused error-propagation discipline (`Result` / `?` /
//!   `thiserror`).
//!
//! # Section design for upstream crates
//! Each section is an independent serde struct so the model (#4), mcp (#6) and
//! builder (#10) crates can deserialize just their portion without importing
//! the full config.
//!
//! # Security
//! Key-like fields store **variable names only** (e.g. `"OPENAI_API_KEY"`),
//! never the key value itself. The `Debug` impl on `UpstreamConfig` and
//! `McpServerEntry` masks sensitive fields.

mod config;
mod error;
mod loader;
mod migration;

pub use config::{
    BudgetConfig, CURRENT_CONFIG_VERSION, Config, McpConfig, McpServerEntry, ModelAlias,
    ModelsConfig, RegistryConfig, SchedulerConfig, SessionConfig, UpstreamConfig,
};
pub use error::ConfigError;
pub use loader::{load, load_from_path, load_or_defaults};
pub use migration::Migration;
