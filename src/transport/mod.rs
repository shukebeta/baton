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
use crate::model::{AssistantReply, Message, Prompt};

/// Sends a conversation and returns a single reply.
///
/// Intentionally synchronous: no streaming or tool calling. The primitive is
/// [`Transport::send_conversation`], which maps the full message history onto a
/// provider request — so a multi-turn session resends its accumulated turns on
/// every call. [`Transport::send`] is a single-turn convenience wrapping one
/// user prompt, provided so the `ask` path needs no separate implementation.
pub trait Transport {
    /// Sends `messages` (the full conversation history, oldest first) and
    /// returns the assistant's reply to the latest turn.
    fn send_conversation(&self, messages: &[Message]) -> Result<AssistantReply>;

    /// Sends a single user `prompt` and returns the assistant's reply.
    ///
    /// Wraps the prompt as a one-message user conversation and delegates to
    /// [`Transport::send_conversation`], so single-turn callers and tests are
    /// unchanged by the multi-turn primitive.
    fn send(&self, prompt: &Prompt) -> Result<AssistantReply> {
        self.send_conversation(std::slice::from_ref(&Message::user(prompt.text.as_str())))
    }
}
