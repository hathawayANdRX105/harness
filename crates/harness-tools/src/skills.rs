//! Skill discovery: scan directories for `SKILL.md` files.
//!
//! The harness agent loop loads skills from well-known directories at startup
//! and makes them available as tool-callable capabilities. Each `SKILL.md`
//! file represents one skill: its name is the parent directory name, its
//! description is the first heading or front-matter summary.
//!
//! Design provenance: pi's `skills.ts`
//! (`packages/coding-agent/src/core/skills.ts`) — the `SKILL.md` filename
//! convention, directory-per-skill layout, and the `discoverSkills` entry
//! point that a loop calls at activation. The parse is intentionally simple:
//! pi uses front-matter parsing (`front-matter` npm package); here we read
//! the first non-blank line after the heading as the description.

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

use crate::args::require_string;

/// One discovered skill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillMeta {
    /// The skill's name — derived from the parent directory name.
    pub name: String,
    /// Path to the `SKILL.md` file.
    pub path: PathBuf,
    /// One-line summary extracted from the file content.
    pub description: String,
}

/// Scan a directory for `SKILL.md` files, one level deep (each subdirectory
/// that contains a `SKILL.md` is one skill).
///
/// Non-recursive: skills are expected in a flat directory layout
/// (`skills/<name>/SKILL.md`, not deeply nested).
///
/// # Errors
///
/// Returns an empty vec when `base_dir` does not exist or is not readable
/// — the caller treats a missing skills directory as "no extra skills", not
/// a fatal configuration error.
pub fn discover_skills(base_dir: &Path) -> Vec<SkillMeta> {
    let Ok(dir) = fs::read_dir(base_dir) else {
        return Vec::new();
    };

    let mut skills: Vec<SkillMeta> = Vec::new();
    for entry in dir.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let skill_path = path.join("SKILL.md");
        if !skill_path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        let description = extract_description(&skill_path);
        skills.push(SkillMeta {
            name,
            path: skill_path,
            description,
        });
    }
    skills
}

/// Read the first meaningful line of a `SKILL.md` as the description.
///
/// Skips empty lines and the first `#` heading, then takes the next
/// non-blank line (up to 120 characters). Returns an empty string when
/// the file cannot be read or has no content after the heading.
fn extract_description(path: &Path) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return String::new();
    };

    let mut seen_heading = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !seen_heading {
            // Skip the first heading line.
            if trimmed.starts_with('#') {
                seen_heading = true;
            }
            continue;
        }
        // First non-blank, non-heading line after the heading is the description.
        let desc = trimmed.chars().take(120).collect::<String>();
        return desc;
    }
    String::new()
}
// ---------------------------------------------------------------------------
// SkillTool: activate a skill by reading its SKILL.md body.
// ---------------------------------------------------------------------------

/// A tool that activates a skill by reading its `SKILL.md` body from disk.
///
/// The loop calls it with `{"name": "<skill>"}`. The tool looks the skill up
/// in the metadata it was constructed with, reads `SKILL.md`, and returns the
/// markdown body as content. An unknown name is a value-shaped failure
/// (`is_error: true`) — the loop surfaces it as a normal tool result instead
/// of a failed call, so a bad skill name never aborts a turn.
///
/// `skills_root` resolves skill body paths that are relative; paths returned
/// by [`discover_skills`] are absolute when discovered from an absolute root
/// and are used as-is.
pub struct SkillTool {
    /// Discovered skill metadata: names, descriptions, and `SKILL.md` paths.
    skills: Vec<SkillMeta>,
    /// Root resolved against a relative skill body path.
    skills_root: PathBuf,
}

impl SkillTool {
    /// Construct a tool exposing the given discovered skills.
    pub fn new(skills: Vec<SkillMeta>, skills_root: PathBuf) -> Self {
        Self {
            skills,
            skills_root,
        }
    }
}

#[async_trait]
impl Tool for SkillTool {
    fn name(&self) -> &str {
        "skill"
    }

    fn schema(&self) -> ToolSchema {
        let names: Vec<String> = self.skills.iter().map(|skill| skill.name.clone()).collect();
        ToolSchema {
            name: "skill".into(),
            description: if names.is_empty() {
                "Activate a skill by reading its SKILL.md body. No skills are declared.".into()
            } else {
                format!(
                    "Activate a skill by reading its SKILL.md body. Available skills: {}",
                    names.join(", ")
                )
            },
            input: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the skill to activate (see the description for the available skills).",
                    }
                },
                "required": ["name"],
                "additionalProperties": false,
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    async fn invoke(
        &self,
        args: Value,
        _cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let name = require_string(&args, "name")?;
        let Some(skill) = self.skills.iter().find(|skill| skill.name == name) else {
            let known: Vec<&str> = self
                .skills
                .iter()
                .map(|skill| skill.name.as_str())
                .collect();
            return Ok(ToolResult {
                content: if known.is_empty() {
                    format!("skill `{name}` is not available; no skills are declared")
                } else {
                    format!(
                        "skill `{name}` is not available; known skills: {}",
                        known.join(", ")
                    )
                },
                is_error: true,
                trace: None,
            });
        };
        let body_path = if skill.path.is_absolute() {
            skill.path.clone()
        } else {
            self.skills_root.join(&skill.path)
        };
        let body = tokio::fs::read_to_string(&body_path).await.map_err(|e| {
            ToolError::Exec(format!(
                "failed to read skill `{name}` from {}: {e}",
                body_path.display()
            ))
        })?;
        Ok(ToolResult {
            content: body,
            is_error: false,
            trace: None,
        })
    }
}
