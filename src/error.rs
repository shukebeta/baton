//! Error types shared across Baton's runtime surfaces.
//!
//! This phase only needs to distinguish configuration failures from transport
//! failures. Later tickets extend [`BatonError`] as new surfaces (the Messages
//! client, the CLI) introduce their own failure modes.

use std::fmt;

/// Convenience alias for results produced by Baton's runtime.
pub type Result<T> = std::result::Result<T, BatonError>;

/// Top-level error type for Baton.
#[derive(Debug)]
pub enum BatonError {
    /// Configuration could not be loaded or was invalid (e.g. a missing or
    /// malformed environment variable).
    Config(String),
    /// A transport-level failure while talking to a provider. The transport
    /// implementation itself lands in a later ticket; the variant exists now so
    /// the boundary is stable.
    Transport(String),
}

impl fmt::Display for BatonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatonError::Config(msg) => write!(f, "configuration error: {msg}"),
            BatonError::Transport(msg) => write!(f, "transport error: {msg}"),
        }
    }
}

impl std::error::Error for BatonError {}
