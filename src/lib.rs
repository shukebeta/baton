//! Baton: an AI-to-AI harness focused on structured agent communication.
//!
//! This phase establishes stable module boundaries for the single-turn
//! first-prompt / first-reply path. Each module is intentionally thin so later
//! tickets extend it rather than rework it:
//!
//! - [`config`] — environment-backed runtime configuration.
//! - [`model`] — typed prompt/reply structures.
//! - [`transport`] — the provider transport boundary.
//! - [`events`] — structured JSONL recording of each exchange.
//! - [`log`] — reading and rendering the recorded exchange trail.
//! - [`message`] — the `baton.message/v1` A2A peer-message envelope.
//! - [`participant`] — the envelope-in / envelope-out participant seam.
//! - [`error`] — shared error and result types.
//! - [`cli`] — the command-line entry surface.

pub mod cli;
pub mod config;
pub mod error;
pub mod events;
pub mod log;
pub mod message;
pub mod model;
pub mod participant;
pub mod transport;
