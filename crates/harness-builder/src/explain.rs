//! Dry-run introspection: render what a spec would assemble, without
//! building anything.
//!
//! `explain()` is the builder's `--dry-run`: it prints the declared units,
//! the model catalog, the compaction policy, the dependency-analysis result
//! and the install order. It never spawns an MCP process, connects a server
//! or reads the filesystem, so it is safe to run on a laptop against a
//! production spec.
//!
//! Design provenance: NanoHarness's `HarnessBuilder.explain`
//! (`refs/nanoharness/nanoharness/profiles/builder.py`) — a text summary of
//! the manifest and the resolved install order. The output here is
//! deterministic (declaration order, no timestamps) so it can be asserted in
//! tests and diffed between spec revisions.

use std::fmt::{self, Write as _};

use crate::analyze;
use crate::spec::{HarnessSpec, HookKind, McpServerSpec, ToolKind, UnitManifest};

/// Render a spec's assembly plan as deterministic text.
pub fn explain(spec: &HarnessSpec) -> String {
    let mut out = String::new();
    write_line(&mut out, format_args!("harness spec `{}`", spec.name));
    explain_tools(&mut out, spec);
    explain_prompt_sections(&mut out, spec);
    explain_skills(&mut out, spec);
    explain_hooks(&mut out, spec);
    explain_mcp_servers(&mut out, spec);
    explain_models(&mut out, spec);
    explain_compaction(&mut out, spec);
    explain_analysis(&mut out, spec);
    out
}

fn explain_tools(out: &mut String, spec: &HarnessSpec) {
    write_line(out, format_args!("  tools ({}):", spec.tools.len()));
    for tool in &spec.tools {
        let mut line = format!("    - {} [{}]", tool.unit_id(), tool_detail(tool));
        if let Some(remote) = &tool.remote {
            line.push_str(&format!(" remote={remote}"));
        }
        line.push_str(&manifest_suffix(&tool.manifest));
        push_line(out, &line);
    }
}

fn explain_prompt_sections(out: &mut String, spec: &HarnessSpec) {
    write_line(
        out,
        format_args!("  prompt sections ({}):", spec.prompt_sections.len()),
    );
    for section in &spec.prompt_sections {
        let mut line = format!(
            "    - {} ({} chars)",
            section.unit_id(),
            section.content.chars().count()
        );
        line.push_str(&manifest_suffix(&section.manifest));
        push_line(out, &line);
    }
}

fn explain_skills(out: &mut String, spec: &HarnessSpec) {
    write_line(out, format_args!("  skills ({}):", spec.skills.len()));
    for skill in &spec.skills {
        let mut line = format!("    - {} (path `{}`)", skill.unit_id(), skill.path);
        line.push_str(&manifest_suffix(&skill.manifest));
        push_line(out, &line);
    }
}

fn explain_hooks(out: &mut String, spec: &HarnessSpec) {
    write_line(out, format_args!("  hooks ({}):", spec.hooks.len()));
    for hook in &spec.hooks {
        let mut line = format!("    - {} [{}]", hook.unit_id(), hook_label(&hook.kind));
        line.push_str(&manifest_suffix(&hook.manifest));
        push_line(out, &line);
    }
}

fn explain_mcp_servers(out: &mut String, spec: &HarnessSpec) {
    write_line(
        out,
        format_args!("  mcp servers ({}):", spec.mcp_servers.len()),
    );
    for server in &spec.mcp_servers {
        let mut line = format!(
            "    - {} ({})",
            server.unit_id(),
            mcp_endpoint_label(server)
        );
        line.push_str(&manifest_suffix(&server.manifest));
        push_line(out, &line);
    }
}

fn explain_models(out: &mut String, spec: &HarnessSpec) {
    write_line(out, format_args!("  models ({}):", spec.models.len()));
    for model in &spec.models {
        write_line(
            out,
            format_args!(
                "    - {}: {}/{} (context_window={})",
                model.handle, model.provider, model.model, model.context_window
            ),
        );
    }
    let default = spec.default_model.as_deref().unwrap_or("none");
    write_line(out, format_args!("  default model: {default}"));
}

fn explain_compaction(out: &mut String, spec: &HarnessSpec) {
    if spec.compaction.stages.is_empty() {
        write_line(out, format_args!("  compaction: none"));
        return;
    }
    write_line(
        out,
        format_args!("  compaction ({} stages):", spec.compaction.stages.len()),
    );
    for stage in &spec.compaction.stages {
        write_line(
            out,
            format_args!(
                "    - {}: threshold={}, target={}",
                stage.label, stage.threshold, stage.target_ratio
            ),
        );
    }
}

fn explain_analysis(out: &mut String, spec: &HarnessSpec) {
    match spec.validate() {
        Ok(()) => write_line(out, format_args!("  validation: ok")),
        Err(error) => write_line(out, format_args!("  validation: FAILED: {error}")),
    }
    match analyze::analyze(spec) {
        Ok(analysis) => {
            write_line(
                out,
                format_args!("  install order ({} units):", analysis.install_order.len()),
            );
            for (position, unit) in analysis.install_order.iter().enumerate() {
                write_line(out, format_args!("    {}. {unit}", position + 1));
            }
        }
        Err(error) => write_line(out, format_args!("  install order: unavailable ({error})")),
    }
}

fn tool_detail(tool: &crate::ToolSpec) -> String {
    match tool.kind {
        ToolKind::Builtin => "builtin".to_string(),
        ToolKind::Mcp => format!(
            "mcp server={}",
            tool.server.as_deref().unwrap_or("<missing>")
        ),
        ToolKind::External => "external".to_string(),
    }
}

fn hook_label(kind: &HookKind) -> String {
    match kind {
        HookKind::DenyTools { tools } => format!("deny_tools {tools:?}"),
        HookKind::TerminateAfterToolBatch => "terminate_after_tool_batch".to_string(),
        HookKind::InjectInstruction { text } => {
            format!("inject_instruction ({} chars)", text.chars().count())
        }
    }
}

fn mcp_endpoint_label(server: &McpServerSpec) -> String {
    if server.from_config {
        return "from config".to_string();
    }
    if let Some(url) = &server.url {
        return format!("http {url}");
    }
    match &server.command {
        Some(command) => {
            let args = if server.args.is_empty() {
                String::new()
            } else {
                format!(" {}", server.args.join(" "))
            };
            format!("stdio `{command}{args}`")
        }
        None => "no endpoint".to_string(),
    }
}

fn manifest_suffix(manifest: &UnitManifest) -> String {
    let mut parts = Vec::new();
    if !manifest.provides.is_empty() {
        parts.push(format!("provides={:?}", manifest.provides));
    }
    if !manifest.requires.is_empty() {
        parts.push(format!("requires={:?}", manifest.requires));
    }
    if !manifest.conflicts.is_empty() {
        parts.push(format!("conflicts={:?}", manifest.conflicts));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" {}", parts.join(" "))
    }
}

fn push_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

fn write_line(out: &mut String, args: fmt::Arguments<'_>) {
    // Writing into a String is infallible; the fmt::Result carries no failure.
    let _ = out.write_fmt(args);
    out.push('\n');
}
