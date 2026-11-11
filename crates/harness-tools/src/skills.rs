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

use std::fs;
use std::path::{Path, PathBuf};

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
