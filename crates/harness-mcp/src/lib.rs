//! MCP (Model Context Protocol) client: self-contained JSON-RPC 2.0 over
//! stdio or HTTP, mapping remote tools onto `harness_core::Tool`.
//!
//! # Design rationale
//!
//! **Self-implemented JSON-RPC (no `rmcp` dependency).** The crate builds its
//! own JSON-RPC 2.0 framing instead of pulling in the `rmcp` library. This
//! keeps the dependency surface minimal, makes every protocol encoding
//! explicit (no magic deserialisation behind a facade), and lets the bridge
//! to `harness_core::Tool` stay a direct adapter rather than a chain of
//! trait conversions.
//!
//! **Sync transport trait, async Tool adapter.** [`McpTransport`] is a
//! blocking (sync) trait — stdio and HTTP transports are fundamentally
//! blocking I/O. The [`McpProxyTool`] bridges to `Tool::invoke` (async) via
//! `tokio::task::spawn_blocking`, forwarding a `CancellationToken` to an
//! `AtomicBool` flag the transport polls.
//!
//! **No `harness-config` dependency.** [`McpServerConfig`] is defined locally;
//! the builder crate (#10) constructs these from the application config.
//!
//! # Layering
//!
//! JSON-RPC framing is free functions over a [`McpTransport`], so the protocol
//! is testable without spawning anything. [`StdioTransport`] is the process
//! implementation, [`HttpTransport`] posts to a running server, and
//! [`McpReconnect`] wraps either with exponential-backoff retries.

pub mod client;
pub mod config;
pub mod error;
pub mod http;
pub mod proxy;
pub mod reconnect;
pub mod stdio;
pub mod transport;

// Re-exports: the names consumers use bare.
pub use client::Mcp;
pub use config::McpServerConfig;
pub use error::McpError;
pub use http::HttpTransport;
pub use proxy::McpProxyTool;
pub use reconnect::{McpReconnect, ReconnectPolicy};
pub use stdio::StdioTransport;
pub use transport::{MCP_TIMEOUT, POLL_INTERVAL};
pub use transport::{McpTransport, PROTOCOL_VERSION, ToolMeta};
