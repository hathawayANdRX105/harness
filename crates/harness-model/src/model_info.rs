//! `ModelInfo` catalog loading from a configuration directory.
//!
//! The `ModelInfoLoader` scans a directory of JSON/YAML model definitions
//! and returns a list of `ModelInfo` entries. The format is deliberately
//! simple — each file defines one model — so that the builder crate (#10)
//! can merge config-provided entries with file-discovered ones.
//!
//! Design provenance: harness-rs's directory-based loader
//! (`crates/harness-models/src/model_info.rs`), simplified to a single-pass
//! reader without file-watch (watch/reload belongs to the caller).
//!
//! # Builder boundary
//!
//! This crate defines the type and the file-reading logic. The builder
//! (#10) decides where to look for these files and how to merge them with
//! embedded defaults.

use std::io;
use std::path::Path;

use harness_core::model::ModelInfo;
use serde::Deserialize;

/// Loader for `ModelInfo` entries from a model-definitions directory.
///
/// Each file in the directory must contain exactly one model definition
/// as JSON or YAML. The format follows the `ModelInfo` shape.
#[derive(Debug, Clone)]
pub struct ModelInfoLoader;

/// The on-disk format for a model definition file.
///
/// Mirrors `ModelInfo` but uses the exact field names the config files
/// carry. Deserialized with serde.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ModelDefinition {
    handle: String,
    provider: String,
    model: String,
    context_window: u32,
    #[serde(default = "default_true")]
    supports_tool_use: bool,
    #[serde(default = "default_true")]
    supports_streaming: bool,
}

/// serde's `default` attribute takes a callable path, and `bool::default()`
/// is `false` — both of these flags mean "true" when the catalog file omits
/// them, so this adapter is the smallest shape that can express that default.
const fn default_true() -> bool {
    true
}

impl ModelInfoLoader {
    /// Load all model definitions from a directory.
    ///
    /// Reads every file with a `.json` or `.yaml`/`.yml` extension in
    /// `dir`, deserializes each as a single `ModelDefinition`, and
    /// converts to `ModelInfo`.  Non-recursive — only direct children of
    /// `dir` are scanned.
    ///
    /// Returns an empty `Vec` when the directory does not exist (so a
    /// config without model files is not an error).
    pub fn load_all(dir: &Path) -> Result<Vec<ModelInfo>, LoadError> {
        let entries = match dir.read_dir() {
            Ok(reader) => reader,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(LoadError::Io(e)),
        };

        let mut models = Vec::new();

        for entry in entries {
            let entry = entry.map_err(LoadError::Io)?;
            let path = entry.path();

            // Only regular files with known extensions
            if !path.is_file() {
                continue;
            }

            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default();

            let def: ModelDefinition = match ext {
                "json" => {
                    let text = std::fs::read_to_string(&path).map_err(LoadError::Io)?;
                    serde_json::from_str(&text).map_err(|e| LoadError::Parse {
                        path: path.to_string_lossy().into_owned(),
                        detail: e.to_string(),
                    })?
                }
                "yaml" | "yml" => {
                    // YAML is not a direct dependency — read as JSON (the
                    // serde_yaml crate is optional). When yaml support is
                    // needed, add serde_yaml and branch here.
                    // For now, skip yaml files with an informative error.
                    return Err(LoadError::UnsupportedFormat(
                        path.to_string_lossy().into_owned(),
                    ));
                }
                _ => continue,
            };

            models.push(ModelInfo {
                handle: def.handle,
                provider: def.provider,
                model: def.model,
                context_window: def.context_window,
                supports_tool_use: def.supports_tool_use,
                supports_streaming: def.supports_streaming,
            });
        }

        Ok(models)
    }
}

/// Errors that can occur while loading model definitions.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// Filesystem error (permission denied, I/O, etc.).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// JSON parse error in a model definition file.
    #[error("parse error in `{path}`: {detail}")]
    Parse { path: String, detail: String },

    /// Unsupported file format (e.g. YAML without serde_yaml).
    #[error("unsupported format: `{0}` — JSON only")]
    UnsupportedFormat(String),
}
