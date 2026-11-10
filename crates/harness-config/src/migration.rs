//! Config schema migration — versioned transformations applied to the raw TOML
//! table before deserialization.
//!
//! Design provenance: pi's `migrations.ts` applies sequential format
//! transformations keyed by schema version. Each migration step is its own
//! implementor of [`Migration`], chained in the `migration_chain()` order.
//!
//! # Adding a migration
//! 1. Increment [`CURRENT_CONFIG_VERSION`](crate::CURRENT_CONFIG_VERSION).
//! 2. Implement [`Migration`] with `source_version()` set to the old version.
//! 3. Append the new instance to `migration_chain()`.
//!
//! The loader calls [`run_migrations`] before deserializing, so the `Config`
//! struct always receives a table at `CURRENT_CONFIG_VERSION`.

use crate::error::ConfigError;
use toml::{Table, Value};

/// Transform a TOML table from one schema version to the next.
///
/// Each implementor handles exactly one version step (vN → vN+1). The loader
/// chains them to advance from the file's version to [`CURRENT_CONFIG_VERSION`](crate::CURRENT_CONFIG_VERSION).
pub trait Migration {
    /// The version this migration reads from (exclusive lower bound).
    /// A migration with `source_version = 1` transforms v1 → v2.
    fn source_version(&self) -> u32;
    /// Transform the config table **in place**. Called only when the file's
    /// `version` field equals `self.source_version()`.
    fn migrate(&self, table: &mut Table) -> Result<(), ConfigError>;
}

/// The ordered chain of all registered migrations, from oldest to newest.
///
/// Each entry transforms version N → N+1. An empty chain is correct for v1
/// (the initial schema has no predecessors).
fn migration_chain() -> Vec<Box<dyn Migration>> {
    vec![]
}

/// Run all migrations needed to advance `table` from its current version to
/// [`CURRENT_CONFIG_VERSION`](crate::CURRENT_CONFIG_VERSION).
///
/// Tables without a `version` key are treated as version 0 (pre-migration).
/// The chain is applied sequentially; if any step fails, the error identifies
/// which transition failed.
pub fn run_migrations(table: &mut Table) -> Result<(), ConfigError> {
    let file_version = table
        .get("version")
        .and_then(|v| v.as_integer())
        .map_or(0, |n| n as u32);

    let target = crate::CURRENT_CONFIG_VERSION;

    if file_version > target {
        return Err(ConfigError::Migration {
            from_version: file_version,
            target_version: target,
            detail: format!(
                "config version {file_version} is newer than the daemon supports ({target})"
            ),
        });
    }

    if file_version == target {
        return Ok(());
    }

    let chain = migration_chain();
    let mut current = file_version;

    for migration in chain.iter() {
        if current != migration.source_version() {
            continue;
        }

        let target_step = migration.source_version() + 1;
        migration
            .migrate(table)
            .map_err(|e| ConfigError::Migration {
                from_version: current,
                target_version: target_step,
                detail: e.to_string(),
            })?;

        // Update version in the table after each successful step.
        table.insert("version".into(), Value::Integer(target_step as i64));
        current = target_step;

        if current == target {
            return Ok(());
        }
    }

    // v0 → v1 when the chain is empty (initial schema, no predecessor).
    if file_version == 0 && target == 1 {
        table.insert("version".into(), Value::Integer(1));
        return Ok(());
    }

    if current < target {
        Err(ConfigError::Migration {
            from_version: current,
            target_version: target,
            detail: format!(
                "no migration registered for v{current} → v{target}; \
                 the chain covers versions 0..{}",
                migration_chain().len(),
            ),
        })
    } else {
        Ok(())
    }
}
