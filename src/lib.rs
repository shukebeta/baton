//! Baton: an AI-to-AI harness focused on structured agent communication.
//!
//! This phase establishes stable module boundaries for the single-turn
//! first-prompt / first-reply path. Each module is intentionally thin so later
//! tickets extend it rather than rework it:
//!
//! - [`config`] — environment-backed runtime configuration.
//! - [`model`] — typed prompt/reply structures.
//! - [`transport`] — the provider transport boundary.
//! - [`error`] — shared error and result types.
//! - [`cli`] — the command-line entry surface.

pub mod cli;
pub mod config;
pub mod error;
pub mod model;
pub mod transport;
