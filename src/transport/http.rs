//! HTTP execution boundary for provider clients.
//!
//! [`HttpClient`] is the seam the Claude client depends on so its request
//! building and response parsing can be unit-tested with a fake client, without
//! touching the network — mirroring the testable split in
//! [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
//!
//! A non-2xx status is *not* an error at this layer: it is returned as an
//! ordinary [`HttpResponse`] carrying the status and body so the caller can map
//! it onto the appropriate [`BatonError`] variant. Only failures with no HTTP
//! response (connection refused, DNS, TLS, timeout) become
//! [`BatonError::Transport`].

use std::time::Duration;

use crate::error::{BatonError, Result};

/// A completed HTTP response: the status code and the raw body text.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// The HTTP status code.
    pub status: u16,
    /// The response body, read as a UTF-8 string.
    pub body: String,
}

/// Sends a single JSON POST request and returns the raw response.
///
/// Implementations must return `Ok` for any completed HTTP exchange, including
/// non-2xx statuses, and reserve `Err(BatonError::Transport(..))` for failures
/// where no response was received.
pub trait HttpClient {
    /// POSTs `body` to `url` with the given `headers` (name, value pairs).
    fn post_json(&self, url: &str, headers: &[(&str, &str)], body: &str) -> Result<HttpResponse>;
}

/// A [`HttpClient`] backed by [`ureq`], with a per-request global timeout.
///
/// Blocking by design: it matches the synchronous [`Transport`] trait, so there
/// is no async runtime to manage for the single-turn first-reply path.
///
/// [`Transport`]: crate::transport::Transport
pub struct UreqHttpClient {
    agent: ureq::Agent,
}

impl UreqHttpClient {
    /// Creates a client whose requests time out after `timeout`.
    pub fn new(timeout: Duration) -> Self {
        // `http_status_as_error(false)` makes ureq return non-2xx responses as
        // `Ok` instead of an error, so the caller sees the status and body and
        // maps them onto Baton's error variants.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(timeout))
            .build()
            .into();
        Self { agent }
    }
}

impl HttpClient for UreqHttpClient {
    fn post_json(&self, url: &str, headers: &[(&str, &str)], body: &str) -> Result<HttpResponse> {
        let mut request = self.agent.post(url);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }

        let mut response = request
            .send(body)
            .map_err(|err| BatonError::Transport(err.to_string()))?;

        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .read_to_string()
            .map_err(|err| BatonError::Transport(format!("failed to read response body: {err}")))?;

        Ok(HttpResponse { status, body })
    }
}
