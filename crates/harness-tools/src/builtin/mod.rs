//! Builtin tool implementations: read, write, edit, glob, grep, terminal, webfetch.
//!
//! Each tool implements `harness_core::Tool` with cwd-aware execution and
//! the correct risk classification. Tools that modify state (write, edit)
//! are marked `Destructive`; read-only tools (read, glob, grep, webfetch)
//! are `ReadOnly`; terminal runs arbitrary commands and is `Destructive`.

pub mod edit;
pub mod glob;
pub mod grep;
pub mod path;
pub mod read;
pub mod terminal;
pub mod webfetch;
pub mod write;

pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use read::ReadTool;
pub use terminal::TerminalTool;
pub use webfetch::WebFetchTool;
pub use write::WriteTool;
