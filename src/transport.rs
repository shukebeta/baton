//! Provider transport boundary.
//!
//! This module defines the seam between Baton's typed model and a concrete
//! provider client. The Claude-compatible Messages implementation lands in a
//! later ticket; defining the trait now keeps that boundary stable and lets the
//! CLI and tests depend on it rather than on a concrete client.

use crate::error::Result;
use crate::model::{AssistantReply, Prompt};

/// Sends a single prompt and returns a single reply.
///
/// Intentionally synchronous and single-turn for this phase: no streaming, no
/// message history, no tool calling. Implementations map a [`Prompt`] onto a
/// provider request and the response back onto an [`AssistantReply`].
pub trait Transport {
    /// Sends `prompt` and returns the assistant's reply.
    fn send(&self, prompt: &Prompt) -> Result<AssistantReply>;
}
