//! Provider dialect configuration — controls how the SSE delta parser maps
//! wire fields to `ModelEvent` variants.
//!
//! # Design provenance
//!
//! The dialect enum mirrors pi's provider adapter dispatch
//! (`packages/ai/src/providers/`): each provider has a small set of
//! parsing differences (field names, finish-reason vocabulary, tool-call
//! shape) while sharing the OpenAI wire format.

/// Provider dialect — influences SSE delta field names and event assembly.
///
/// This crate only defines the two supported dialects. Adding a new
/// dialect (Anthropic, Gemini) means adding a variant here and extending
/// the match arms in the delta parser (`openai.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// Generic OpenAI-compatible API (the default).
    ///
    /// Fields: `content`, `tool_calls`; finish reasons `stop`, `length`,
    /// `tool_calls`.
    #[default]
    OpenAI,
    /// DeepSeek API: same wire format as OpenAI, with the addition of a
    /// `reasoning_content` delta field for chain-of-thought traces.
    DeepSeek,
}
