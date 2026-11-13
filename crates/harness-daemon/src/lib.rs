//! harness-daemon library: the daemon's transport seams and process helpers.
//!
//! The binary is a thin `main` over these modules; exposing them as a
//! library lets `tests/` drive the wire seams (handler, event log) directly
//! instead of reaching into `src/`.

pub mod lifecycle;
pub mod pidlock;
pub mod wire;
