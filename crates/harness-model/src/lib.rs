//! Harness model: the upstream-facing half of the model loop -- an
//! OpenAI-compatible HTTP adapter that turns SSE chunk deltas into
//! [`ModelEvent`] values, plus the supporting types for alias resolution,
//! retry/backoff, and `ModelInfo` catalog loading.
//!
//! # Design provenance
//!
//! - **pi** (`packages/agent/src/stream-fn.ts`) contributes the failure
//!   discipline: the stream never carries `Result`; errors land as terminal
//!   `ModelEvent::Error`. The SSE parser and event assembler uphold this
//!   contract at every level -- a connection failure, a 4xx response, a
//!   malformed JSON delta or an interrupted transport all terminate the
//!   stream via `Error`, never through `panic` or an `Err` variant.
//! - **harness-rs** (`crates/harness-models/src/`) contributes the delta
//!   vocabulary: ToolCallStart/Args/End for incremental tool-call
//!   assembly, and the RetryPolicy / AliasResolver shapes.
//!
//! # Builder boundary
//!
//! This crate owns the **adapter implementation** and the **data types**
//! (alias registry, retry policy, model-info loader), but not the
//! construction logic that wires config → model. That belongs to the
//! builder crate (#10): `harness-config` drives these fields and the
//! builder maps them into `OpenAIModel` instances. This crate keeps no
//! dependency on `harness-config`.

pub use alias::AliasRegistry;
pub use backoff::RetryPolicy;
pub use dialect::Dialect;
pub use model_info::ModelInfoLoader;
pub use openai::OpenAIModel;

pub mod alias;
pub mod backoff;
pub mod dialect;
pub mod model_info;
pub mod openai;

// Re-export only the types that belong to this crate's public surface.
// harness_core::model::Model / ModelEvent / etc. are accessed through the
// harness_core crate, not re-exported here.
