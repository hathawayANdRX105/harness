//! Integration tests for skill discovery.
//!
//! Each test creates a temp directory with known SKILL.md files and verifies
//! that discover_skills returns the expected metadata. No mock filesystem —
//! all tests use real tempfile directories.

use std::fs;
use std::path::Path;

use harness_tools::skills::discover_skills;

/// Purpose: verify that discover_skills finds SKILL.md in subdirectories.
/// Scenario: create a temp dir with two skill dirs, each containing SKILL.md.
/// Acceptance: both skills are discovered with correct names and descriptions.
#[test]
fn discovers_multiple_skills() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let base = dir.path();

    // Create skills/skill_a/SKILL.md
    let skill_a_dir = base.join("skill_a");
    fs::create_dir_all(&skill_a_dir).unwrap_or_else(|e| panic!("create skill_a: {e}"));
    fs::write(skill_a_dir.join("SKILL.md"), "# Skill A\n\nDoes thing A.\n")
        .unwrap_or_else(|e| panic!("write skill_a SKILL.md: {e}"));

    // Create skills/skill_b/SKILL.md
    let skill_b_dir = base.join("skill_b");
    fs::create_dir_all(&skill_b_dir).unwrap_or_else(|e| panic!("create skill_b: {e}"));
    fs::write(
        skill_b_dir.join("SKILL.md"),
        "# Skill B\n\nDoes thing B with a long description that should be truncated at 120 chars.",
    )
    .unwrap_or_else(|e| panic!("write skill_b SKILL.md: {e}"));

    let skills = discover_skills(base);
    assert_eq!(
        skills.len(),
        2,
        "must discover 2 skills, got {}",
        skills.len()
    );

    // Find skill_a
    let a = skills
        .iter()
        .find(|s| s.name == "skill_a")
        .unwrap_or_else(|| panic!("skill_a not found"));
    assert_eq!(a.name, "skill_a");
    assert!(
        a.description.contains("Does thing A"),
        "desc must contain intro: {}",
        a.description
    );
    assert!(
        a.path.ends_with("skill_a/SKILL.md"),
        "path mismatch: {}",
        a.path.display()
    );

    // Find skill_b
    let b = skills
        .iter()
        .find(|s| s.name == "skill_b")
        .unwrap_or_else(|| panic!("skill_b not found"));
    assert_eq!(b.name, "skill_b");
    assert!(
        b.description.starts_with("Does thing B"),
        "desc must start with intro: {}",
        b.description
    );
}

/// Purpose: verify that discover_skills skips directories without SKILL.md.
/// Scenario: create a dir with a subdirectory that has no SKILL.md.
/// Acceptance: only the skill with SKILL.md is discovered.
#[test]
fn skips_dirs_without_skill_md() {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let base = dir.path();

    // Create a valid skill.
    let valid = base.join("valid_skill");
    fs::create_dir_all(&valid).unwrap_or_else(|e| panic!("create valid: {e}"));
    fs::write(valid.join("SKILL.md"), "# Valid\n\nDoes something.\n")
        .unwrap_or_else(|e| panic!("write valid SKILL.md: {e}"));

    // Create a subdirectory without SKILL.md.
    fs::create_dir_all(base.join("empty_dir")).unwrap_or_else(|e| panic!("create empty_dir: {e}"));

    let skills = discover_skills(base);
    assert_eq!(skills.len(), 1, "must discover exactly 1 skill");
    assert_eq!(skills[0].name, "valid_skill");
}

/// Purpose: verify that discover_skills returns an empty vec for a missing dir.
/// Scenario: call discover_skills with a non-existent path.
/// Acceptance: empty vec, no panic.
#[test]
fn missing_base_dir() {
    let skills = discover_skills(Path::new("/tmp/nonexistent_harness_test_dir_xyz"));
    assert!(skills.is_empty(), "must be empty for missing dir");
}
