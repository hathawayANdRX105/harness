//! Builtin tool implementations: read, write, edit, glob, grep.
//!
//! Each tool implements `harness_core::Tool` with cwd-aware execution and
//! the correct risk classification. Tools that modify state (write, edit)
//! are marked `Destructive`; read-only tools (read, glob, grep) are `ReadOnly`.

pub mod edit;
pub mod glob;
pub mod grep;
pub mod read;
pub mod write;

pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use read::ReadTool;
pub use write::WriteTool;
