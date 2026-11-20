//! Default prompt nodes — the facade's prompt vocabulary.
//!
//! These two nodes machine the message ledger itself (seating the system
//! prompt, appending a declared instruction once) and carry no domain
//! dependency beyond core, so they live here rather than in a capability
//! crate: the builder is the assembly surface that decides what a default
//! prompt topology looks like.

mod instruction;
mod system_ledger;

pub use instruction::InstructionProcessor;
pub use system_ledger::SystemLedgerProcessor;
