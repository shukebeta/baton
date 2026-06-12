//! Provider transport boundary.
//!
//! This module defines the seam between Baton's typed model and a concrete
//! provider client. [`Transport`] is the stable boundary the CLI and tests
//! depend on; the [`claude`] submodule provides the first concrete
//! implementation (a non-streaming Claude-compatible Messages client), and
//! [`http`] isolates the underlying HTTP execution so the request/response
//! logic can be tested without a network.

pub mod claude;
pub mod http;

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
