//! Dependency analysis over the spec's capability manifests: detect missing
//! providers, ambiguous providers, conflicts and cycles, then derive a
//! deterministic topological install order.
//!
//! # Design provenance
//!
//! NanoHarness's `HarnessBuilder._analyze`
//! (`refs/nanoharness/nanoharness/profiles/builder.py:236`) validates the
//! extension manifest's `provides` / `requires` / `conflicts` and derives an
//! install order before binding services. This module is the Rust, static
//! reading of the same idea: units are the spec's installable components,
//! capabilities are strings, and the graph is a pure function of the spec —
//! no registry, no dynamic install.
//!
//! The install order is the order in which components would be wired:
//! a provider always precedes the units that require its capability. Ties
//! (and independent units) follow the spec's declaration order — tools, then
//! prompt sections, skills, hooks, MCP servers — so the output is stable for
//! a given spec.
//!
//! An MCP tool implicitly requires its server (`mcp:<server>`) even when the
//! spec does not spell the requirement out: the server must be connected
//! before its tools can be installed.

use std::collections::{BTreeMap, BTreeSet};

use crate::AnalyzeError;
use crate::spec::{HarnessSpec, ToolKind, UnitManifest};

/// The result of dependency analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Analysis {
    /// Unit ids in dependency-respecting install order.
    pub install_order: Vec<String>,
}

/// One installable unit: an id, its declared manifest, and the requirements
/// the spec implies without spelling them out (an MCP tool's server).
struct Unit<'a> {
    id: String,
    manifest: &'a UnitManifest,
    implicit_requires: Vec<String>,
}

/// Resolve the spec's provides/requires/conflicts graph.
///
/// # Errors
///
/// - [`AnalyzeError::MissingRequirement`] when a required capability has no
///   provider;
/// - [`AnalyzeError::AmbiguousProvider`] when two units provide the same
///   capability (the graph cannot pick one deterministically);
/// - [`AnalyzeError::Conflict`] when a unit conflicts with a capability that
///   some unit provides;
/// - [`AnalyzeError::Cycle`] when the requirement graph has a cycle (the
///   units that could not be ordered are listed).
pub fn analyze(spec: &HarnessSpec) -> Result<Analysis, AnalyzeError> {
    let units = collect_units(spec);
    let providers = collect_providers(&units)?;
    let (edges, indegree) = collect_requirement_edges(&units, &providers)?;
    detect_conflicts(&units, &providers)?;
    let install_order = topological_order(&units, &edges, indegree)?;
    Ok(Analysis { install_order })
}

fn collect_units(spec: &HarnessSpec) -> Vec<Unit<'_>> {
    let tools = spec.tools.iter().map(|tool| Unit {
        id: tool.unit_id(),
        manifest: &tool.manifest,
        implicit_requires: match tool.kind {
            ToolKind::Mcp => tool
                .server
                .as_ref()
                .map(|server| vec![format!("mcp:{server}")])
                .unwrap_or_default(),
            ToolKind::Builtin | ToolKind::External => Vec::new(),
        },
    });
    let prompt_sections = spec.prompt_sections.iter().map(|section| Unit {
        id: section.unit_id(),
        manifest: &section.manifest,
        implicit_requires: Vec::new(),
    });
    let skills = spec.skills.iter().map(|skill| Unit {
        id: skill.unit_id(),
        manifest: &skill.manifest,
        implicit_requires: Vec::new(),
    });
    let hooks = spec.hooks.iter().map(|hook| Unit {
        id: hook.unit_id(),
        manifest: &hook.manifest,
        implicit_requires: Vec::new(),
    });
    let mcp_servers = spec.mcp_servers.iter().map(|server| Unit {
        id: server.unit_id(),
        manifest: &server.manifest,
        implicit_requires: Vec::new(),
    });

    tools
        .chain(prompt_sections)
        .chain(skills)
        .chain(hooks)
        .chain(mcp_servers)
        .collect()
}

/// Capability -> providing unit indices, in declaration order. A unit that
/// lists the same capability twice still counts once.
fn collect_providers<'a>(
    units: &[Unit<'a>],
) -> Result<BTreeMap<&'a str, Vec<usize>>, AnalyzeError> {
    let mut providers: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, unit) in units.iter().enumerate() {
        for capability in &unit.manifest.provides {
            let entries = providers.entry(capability.as_str()).or_default();
            if !entries.contains(&index) {
                entries.push(index);
            }
        }
    }
    for (capability, indices) in &providers {
        if indices.len() > 1 {
            return Err(AnalyzeError::AmbiguousProvider {
                capability: capability.to_string(),
                providers: indices
                    .iter()
                    .map(|&index| units[index].id.clone())
                    .collect(),
            });
        }
    }
    Ok(providers)
}

/// Provider -> dependent edges plus each unit's indegree. Self-requirements
/// are satisfied by the unit itself and add no edge.
fn collect_requirement_edges(
    units: &[Unit<'_>],
    providers: &BTreeMap<&str, Vec<usize>>,
) -> Result<(Vec<BTreeSet<usize>>, Vec<usize>), AnalyzeError> {
    let mut edges: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); units.len()];
    let mut indegree = vec![0usize; units.len()];
    for (index, unit) in units.iter().enumerate() {
        for capability in unit
            .manifest
            .requires
            .iter()
            .chain(unit.implicit_requires.iter())
        {
            let Some(&provider_index) = providers
                .get(capability.as_str())
                .and_then(|indices| indices.first())
            else {
                return Err(AnalyzeError::MissingRequirement {
                    unit: unit.id.clone(),
                    capability: capability.clone(),
                });
            };
            if provider_index == index {
                continue;
            }
            if edges[provider_index].insert(index) {
                indegree[index] += 1;
            }
        }
    }
    Ok((edges, indegree))
}

fn detect_conflicts(
    units: &[Unit<'_>],
    providers: &BTreeMap<&str, Vec<usize>>,
) -> Result<(), AnalyzeError> {
    for unit in units {
        for capability in &unit.manifest.conflicts {
            let Some(&provider_index) = providers
                .get(capability.as_str())
                .and_then(|indices| indices.first())
            else {
                continue;
            };
            return Err(AnalyzeError::Conflict {
                unit: unit.id.clone(),
                capability: capability.clone(),
                provider: units[provider_index].id.clone(),
            });
        }
    }
    Ok(())
}

/// Kahn's algorithm with a `BTreeSet` ready queue, so equal-indegree units are
/// installed in declaration order rather than hash order.
fn topological_order(
    units: &[Unit<'_>],
    edges: &[BTreeSet<usize>],
    mut indegree: Vec<usize>,
) -> Result<Vec<String>, AnalyzeError> {
    let mut ready: BTreeSet<usize> = (0..units.len())
        .filter(|&index| indegree[index] == 0)
        .collect();
    let mut order = Vec::with_capacity(units.len());
    while let Some(index) = ready.pop_first() {
        order.push(index);
        for &dependent in &edges[index] {
            indegree[dependent] -= 1;
            if indegree[dependent] == 0 {
                ready.insert(dependent);
            }
        }
    }
    if order.len() != units.len() {
        let blocked: Vec<String> = (0..units.len())
            .filter(|&index| indegree[index] > 0)
            .map(|index| units[index].id.clone())
            .collect();
        return Err(AnalyzeError::Cycle { units: blocked });
    }
    Ok(order
        .into_iter()
        .map(|index| units[index].id.clone())
        .collect())
}
