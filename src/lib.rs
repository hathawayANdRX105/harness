//! Harness scaffold root — intentionally empty until the project's crates land.
//!
//! This library exists only so the workspace is buildable while the toolchain
//! (nightly + Cranelift dev profile, LLVM ci profile) is wired and verifiable.
//! Real code goes under `crates/` as workspace members.

/// Scaffold placeholder so the dev-profile Cranelift backend is exercised.
pub fn scaffold() -> u32 {
    42
}
