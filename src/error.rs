//! Error types shared across Baton's runtime surfaces.
//!
//! Configuration failures and the provider transport's failure modes are
//! modelled as distinct variants so callers can react to them explicitly. The
//! Messages client maps HTTP and decode failures onto these variants rather
//! than collapsing everything into a single opaque error.

use std::fmt;

/// Convenience alias for results produced by Baton's runtime.
pub type Result<T> = std::result::Result<T, BatonError>;

/// Top-level error type for Baton.
#[derive(Debug)]
pub enum BatonError {
    /// A command-line argument was missing, unrecognised, or malformed. Carries
    /// a human-readable explanation plus the one-line usage summary.
    Usage(String),
    /// Configuration could not be loaded or was invalid (e.g. a missing or
    /// malformed environment variable).
    Config(String),
    /// A transport-level failure with no HTTP response: connection refused,
    /// DNS failure, TLS error, timeout, etc.
    Transport(String),
    /// The provider rejected the credentials (HTTP 401).
    Auth(String),
    /// The provider rate-limited the request (HTTP 429).
    RateLimited(String),
    /// The provider returned a server-side failure (HTTP 5xx).
    Server {
        /// The HTTP status code.
        status: u16,
        /// The provider's error message, or the raw body when it could not be
        /// parsed.
        message: String,
    },
    /// The provider returned some other non-success status (e.g. 400 Bad
    /// Request) that does not map to a more specific variant.
    Api {
        /// The HTTP status code.
        status: u16,
        /// The provider's error message, or the raw body when it could not be
        /// parsed.
        message: String,
    },
    /// A 2xx response could not be decoded into an [`AssistantReply`], because
    /// the body was malformed, partial, or carried no assistant text.
    ///
    /// [`AssistantReply`]: crate::model::AssistantReply
    Decode(String),
    /// A local I/O operation failed (e.g. opening the configured event log).
    Io(String),
    /// A recorded exchange-log line could not be parsed (malformed JSON, or a
    /// known event missing required fields). Distinct from [`BatonError::Io`]
    /// (the read succeeded) and [`BatonError::Decode`] (a provider response).
    Log(String),
}

impl BatonError {
    /// A stable, machine-readable class for this error.
    ///
    /// Used by structured event recording so consumers can branch on the
    /// failure kind without parsing the human-readable message.
    pub fn kind(&self) -> &'static str {
        match self {
            BatonError::Usage(_) => "usage",
            BatonError::Config(_) => "config",
            BatonError::Transport(_) => "transport",
            BatonError::Auth(_) => "auth",
            BatonError::RateLimited(_) => "rate_limited",
            BatonError::Server { .. } => "server",
            BatonError::Api { .. } => "api",
            BatonError::Decode(_) => "decode",
            BatonError::Io(_) => "io",
            BatonError::Log(_) => "log",
        }
    }
}

impl fmt::Display for BatonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatonError::Usage(msg) => write!(f, "usage error: {msg}"),
            BatonError::Config(msg) => write!(f, "configuration error: {msg}"),
            BatonError::Transport(msg) => write!(f, "transport error: {msg}"),
            BatonError::Auth(msg) => write!(f, "authentication error: {msg}"),
            BatonError::RateLimited(msg) => write!(f, "rate limited: {msg}"),
            BatonError::Server { status, message } => {
                write!(f, "provider server error ({status}): {message}")
            }
            BatonError::Api { status, message } => {
                write!(f, "provider error ({status}): {message}")
            }
            BatonError::Decode(msg) => write!(f, "response decode error: {msg}"),
            BatonError::Io(msg) => write!(f, "io error: {msg}"),
            BatonError::Log(msg) => write!(f, "log error: {msg}"),
        }
    }
}

impl std::error::Error for BatonError {}
