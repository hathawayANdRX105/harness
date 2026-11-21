//! Error taxonomy for runtime assembly.
//!
//! The profile is declarative data without validation; every failure here is
//! "the runtime cannot be produced" — typically because the profile declares
//! something the host did not inject, or a declaration cannot be resolved at
//! assembly time. Every message names the tool, server or skill involved — a
//! builder failure is a configuration bug report, not a stack trace.

use harness_mcp::McpError;

/// Assembly problems: the runtime cannot be produced from the profile and
/// the host's injections.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("no model was injected; the builder never falls back to a default model")]
    MissingModel,
    #[error("injected model `{actual}` does not match default_model `{expected}`")]
    ModelMismatch { expected: String, actual: String },
    #[error("no tool policy was injected; the builder never falls back to allow-all")]
    MissingPolicy,
    #[error("duplicate hook on seam `{seam}`; a profile may declare at most one hook per seam")]
    DuplicateHookKind { seam: &'static str },
    #[error("no event sink was injected")]
    MissingEventSink,
    #[error("builtin tools need a working directory; call `with_cwd`")]
    MissingCwd,
    #[error("builtin tool `{name}` is not known")]
    UnknownBuiltin { name: String },
    #[error("external tool `{name}` is declared by the profile but was not injected")]
    MissingExternalTool { name: String },
    #[error("tool `{name}` is installed more than once")]
    DuplicateTool { name: String },
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
