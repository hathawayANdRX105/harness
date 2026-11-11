//! Error taxonomy for the three builder stages: spec validation, dependency
//! analysis, and runtime assembly.
//!
//! The split mirrors the stage that can produce the error, so a caller can
//! tell "the spec is malformed" from "the spec is well formed but its
//! dependencies do not line up" from "the host did not inject what the spec
//! asked for". Every message names the unit, capability or field involved —
//! a builder failure is a configuration bug report, not a stack trace.

use harness_mcp::McpError;

/// Structural problems in a [`crate::HarnessSpec`] — checked by
/// [`crate::HarnessSpec::validate`] without any config, builder or filesystem.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum SpecError {
    #[error("spec name must not be empty")]
    EmptySpecName,
    #[error("duplicate unit id `{id}`")]
    DuplicateUnit { id: String },
    #[error("unit `{unit}` has an empty capability name")]
    EmptyCapability { unit: String },
    #[error("tool name must not be empty")]
    EmptyToolName,
    #[error("tool `{name}` is not a known builtin")]
    UnknownBuiltin { name: String },
    #[error("tool `{tool}` sets `server`/`remote` without kind `mcp`")]
    UnexpectedMcpFields { tool: String },
    #[error("tool `{tool}` has kind `mcp` but no `server`")]
    MissingMcpServer { tool: String },
    #[error("tool `{tool}` references MCP server `{server}`, which the spec does not declare")]
    UnknownMcpServer { tool: String, server: String },
    #[error("tool `{tool}` has an empty `remote` filter")]
    EmptyRemoteTool { tool: String },
    #[error("MCP server name must not be empty")]
    EmptyMcpServerName,
    #[error("MCP server `{server}` has an empty command")]
    EmptyMcpCommand { server: String },
    #[error("MCP server `{server}` has an empty url")]
    EmptyMcpUrl { server: String },
    #[error("MCP server `{server}` needs a `command` or `url`")]
    MissingMcpEndpoint { server: String },
    #[error("MCP server `{server}` has an empty environment variable name")]
    EmptyEnvKey { server: String },
    #[error("prompt section name must not be empty")]
    EmptyPromptSection,
    #[error("prompt section `{name}` has empty content")]
    EmptyPromptContent { name: String },
    #[error("skill name and path must not be empty")]
    EmptySkill,
    #[error("hook name must not be empty")]
    EmptyHookName,
    #[error("more than one hook occupies the `{seam}` loop seam")]
    DuplicateHookKind { seam: &'static str },
    #[error("hook `{hook}` declares an empty tool list")]
    EmptyHookTools { hook: String },
    #[error("hook `{hook}` has empty instruction text")]
    EmptyInstruction { hook: String },
    #[error("model `{handle}` has an empty `{field}`")]
    EmptyModelField { handle: String, field: &'static str },
    #[error("model `{handle}` has context_window 0")]
    ZeroContextWindow { handle: String },
    #[error("model handle `{handle}` is declared more than once")]
    DuplicateModel { handle: String },
    #[error("default_model must not be empty")]
    EmptyDefaultModel,
    #[error("default_model `{handle}` is not declared in `models`")]
    UnknownDefaultModel { handle: String },
    #[error("compaction stage label must not be empty")]
    EmptyStageLabel,
    #[error("compaction stage `{label}` has threshold {threshold}, outside (0, 1]")]
    InvalidThreshold { label: String, threshold: f64 },
    #[error("compaction stage `{label}` has target {target}, outside [0, threshold]")]
    InvalidTarget { label: String, target: f64 },
    #[error("compaction stage `{label}` does not raise the threshold above the previous stage")]
    UnorderedStages { label: String },
}

/// Dependency-analysis problems: the capability graph declared by the spec
/// cannot be installed as a whole.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AnalyzeError {
    #[error("unit `{unit}` requires `{capability}`, which no unit provides")]
    MissingRequirement { unit: String, capability: String },
    #[error("capability `{capability}` is provided by more than one unit: {providers:?}")]
    AmbiguousProvider {
        capability: String,
        providers: Vec<String>,
    },
    #[error("unit `{unit}` conflicts with capability `{capability}` provided by `{provider}`")]
    Conflict {
        unit: String,
        capability: String,
        provider: String,
    },
    #[error("dependency cycle blocks these units: {units:?}")]
    Cycle { units: Vec<String> },
}

/// Assembly problems: the spec is valid and analyzable, but the runtime
/// cannot be produced — typically because the host did not inject something
/// the spec declared.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    Spec(#[from] SpecError),
    #[error(transparent)]
    Analyze(#[from] AnalyzeError),
    #[error("no model was injected; the builder never falls back to a default model")]
    MissingModel,
    #[error("injected model `{actual}` does not match default_model `{expected}`")]
    ModelMismatch { expected: String, actual: String },
    #[error("no tool policy was injected; the builder never falls back to allow-all")]
    MissingPolicy,
    #[error("no event sink was injected")]
    MissingEventSink,
    #[error("builtin tools need a working directory; call `with_cwd`")]
    MissingCwd,
    #[error("builtin tool `{name}` is not known")]
    UnknownBuiltin { name: String },
    #[error("external tool `{name}` is declared by the spec but was not injected")]
    MissingExternalTool { name: String },
    #[error("tool `{name}` is installed more than once")]
    DuplicateTool { name: String },
    #[error("install order names unknown tool unit `{unit}`")]
    UnknownToolUnit { unit: String },
    #[error("tool `{tool}` has kind `mcp` but no `server`")]
    MissingMcpServer { tool: String },
    #[error("tool `{tool}` references undeclared MCP server `{server}`")]
    UnknownMcpServer { tool: String, server: String },
    #[error("MCP server `{server}` has neither a command nor a url")]
    MissingMcpEndpoint { server: String },
    #[error("MCP server `{server}` is config-backed but not present in the bound config")]
    ConfigServerNotFound { server: String },
    #[error("MCP server `{server}` did not advertise tool `{tool}`")]
    McpToolNotFound { server: String, tool: String },
    #[error("MCP server `{server}` advertised no tools")]
    McpServerHasNoTools { server: String },
    #[error("skill `{name}` was not found under `{path}`")]
    SkillNotFound { name: String, path: String },
    #[error("mcp server `{server}`: {source}")]
    Mcp {
        server: String,
        #[source]
        source: McpError,
    },
    #[error("environment variable `{name}` for MCP server `{server}` is not set")]
    MissingEnvVar { server: String, name: String },
}
