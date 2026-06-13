//! Environment-backed runtime configuration.
//!
//! [`BatonConfig`] holds everything the single-turn first-reply path needs to
//! reach a provider. Loading is split into [`BatonConfig::from_env`] (the real
//! entry point) and a pure [`BatonConfig::from_lookup`] so parsing can be tested
//! deterministically without mutating the process environment.
//!
//! Authentication is modelled as a typed [`Credential`] so Baton accepts either
//! an Anthropic API key (`ANTHROPIC_API_KEY`) or an OAuth bearer token
//! (`ANTHROPIC_AUTH_TOKEN` / `CLAUDE_CODE_OAUTH_TOKEN`). The first present
//! variable in that precedence order is the resolved credential; the
//! transport then picks the matching `x-api-key` or `Authorization: Bearer`
//! header from the variant.

use std::time::Duration;

use crate::error::{BatonError, Result};

/// Default base URL for the Claude-compatible Messages API.
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// Default model id used when `BATON_MODEL` is unset.
pub const DEFAULT_MODEL: &str = "claude-sonnet-4-6";

/// Default request timeout in seconds when `BATON_TIMEOUT_SECS` is unset.
pub const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// An authentication credential accepted by the provider transport.
///
/// Variants map 1:1 onto the wire-format header the transport emits:
/// `ApiKey` -> `x-api-key`, `OAuth` -> `Authorization: Bearer <token>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// An Anthropic API key, sent as the `x-api-key` header.
    ApiKey(String),
    /// An OAuth bearer token, sent as the `Authorization: Bearer <token>`
    /// header.
    OAuth(String),
}

impl Credential {
    /// Returns the inner secret as a borrowed string slice.
    ///
    /// Used by the transport to read the credential value without exposing
    /// ownership; the lifetime is tied to the `Credential` itself.
    pub fn secret(&self) -> &str {
        match self {
            Credential::ApiKey(value) | Credential::OAuth(value) => value,
        }
    }
}

/// Runtime configuration for Baton's first-reply path.
#[derive(Debug, Clone)]
pub struct BatonConfig {
    /// Resolved provider credential (API key or OAuth bearer token).
    pub credential: Credential,
    /// Base URL for the Messages API. From `ANTHROPIC_BASE_URL`, defaulting to
    /// [`DEFAULT_BASE_URL`].
    pub base_url: String,
    /// Model id to request. From `BATON_MODEL`, defaulting to [`DEFAULT_MODEL`].
    pub model: String,
    /// Per-request timeout. Derived from `BATON_TIMEOUT_SECS`, defaulting to
    /// [`DEFAULT_TIMEOUT_SECS`].
    pub timeout: Duration,
}

impl BatonConfig {
    /// Loads configuration from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Loads configuration from an arbitrary key lookup.
    ///
    /// `lookup` returns the value for a variable name, or `None` when it is
    /// unset. This is the testable core behind [`BatonConfig::from_env`].
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let credential = resolve_credential(&lookup)?;

        let base_url =
            non_empty(lookup("ANTHROPIC_BASE_URL")).unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let model = non_empty(lookup("BATON_MODEL")).unwrap_or_else(|| DEFAULT_MODEL.to_string());

        let timeout_secs = match non_empty(lookup("BATON_TIMEOUT_SECS")) {
            Some(raw) => raw.parse::<u64>().map_err(|_| {
                BatonError::Config(format!(
                    "BATON_TIMEOUT_SECS must be a non-negative integer, got {raw:?}"
                ))
            })?,
            None => DEFAULT_TIMEOUT_SECS,
        };

        Ok(Self {
            credential,
            base_url,
            model,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

/// Resolves the provider credential with the documented precedence.
///
/// Iterates the candidate `(variable, variant)` pairs in order; the first
/// variable whose lookup returns `Some` is the resolved credential. A
/// present-but-blank value is an error (and does *not* fall through to a later
/// candidate), because exporting a credential variable empty is almost always
/// a misconfiguration rather than an explicit "skip me" signal. If no
/// variable is present at all, that is also an error.
type CredentialBuilder = fn(String) -> Credential;

fn resolve_credential(lookup: &impl Fn(&str) -> Option<String>) -> Result<Credential> {
    let candidates: [(&str, CredentialBuilder); 3] = [
        ("ANTHROPIC_API_KEY", Credential::ApiKey),
        ("ANTHROPIC_AUTH_TOKEN", Credential::OAuth),
        ("CLAUDE_CODE_OAUTH_TOKEN", Credential::OAuth),
    ];

    for (var, make) in candidates {
        let Some(raw) = lookup(var) else {
            continue;
        };
        if raw.trim().is_empty() {
            return Err(BatonError::Config(format!("{var} is set but empty")));
        }
        return Ok(make(raw));
    }

    Err(BatonError::Config(
        "no Anthropic credential set: set one of ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, or CLAUDE_CODE_OAUTH_TOKEN".to_string(),
    ))
}

/// Treats a present-but-blank value as absent so a defaulted variable that is
/// exported empty still falls back to its default.
fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn applies_defaults_when_only_api_key_present() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[("ANTHROPIC_API_KEY", "secret")]))
            .expect("config should load");
        assert_eq!(cfg.credential, Credential::ApiKey("secret".to_string()));
        assert_eq!(cfg.base_url, DEFAULT_BASE_URL);
        assert_eq!(cfg.model, DEFAULT_MODEL);
        assert_eq!(cfg.timeout, Duration::from_secs(DEFAULT_TIMEOUT_SECS));
    }

    #[test]
    fn overrides_are_honored() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_API_KEY", "secret"),
            ("ANTHROPIC_BASE_URL", "https://proxy.example"),
            ("BATON_MODEL", "claude-opus-4-8"),
            ("BATON_TIMEOUT_SECS", "5"),
        ]))
        .expect("config should load");
        assert_eq!(cfg.credential, Credential::ApiKey("secret".to_string()));
        assert_eq!(cfg.base_url, "https://proxy.example");
        assert_eq!(cfg.model, "claude-opus-4-8");
        assert_eq!(cfg.timeout, Duration::from_secs(5));
    }

    #[test]
    fn auth_token_resolves_to_oauth() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[("ANTHROPIC_AUTH_TOKEN", "bearer-tok")]))
            .expect("config should load");
        assert_eq!(cfg.credential, Credential::OAuth("bearer-tok".to_string()));
    }

    #[test]
    fn claude_code_oauth_token_resolves_to_oauth() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[("CLAUDE_CODE_OAUTH_TOKEN", "code-tok")]))
            .expect("config should load");
        assert_eq!(cfg.credential, Credential::OAuth("code-tok".to_string()));
    }

    #[test]
    fn api_key_wins_when_both_api_key_and_oauth_set() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_API_KEY", "key-wins"),
            ("ANTHROPIC_AUTH_TOKEN", "oauth-loses"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "oauth-also-loses"),
        ]))
        .expect("config should load");
        assert_eq!(cfg.credential, Credential::ApiKey("key-wins".to_string()));
    }

    #[test]
    fn auth_token_wins_over_claude_code_oauth_token() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_AUTH_TOKEN", "auth-token"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "code-token"),
        ]))
        .expect("config should load");
        assert_eq!(cfg.credential, Credential::OAuth("auth-token".to_string()));
    }

    #[test]
    fn missing_all_credentials_errors() {
        let err = BatonConfig::from_lookup(lookup_from(&[])).unwrap_err();
        match err {
            BatonError::Config(msg) => {
                assert!(
                    msg.contains("ANTHROPIC_API_KEY")
                        && msg.contains("ANTHROPIC_AUTH_TOKEN")
                        && msg.contains("CLAUDE_CODE_OAUTH_TOKEN"),
                    "message should name all three credential variables, got: {msg}"
                );
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn blank_api_key_errors() {
        let err =
            BatonConfig::from_lookup(lookup_from(&[("ANTHROPIC_API_KEY", "   ")])).unwrap_err();
        match err {
            BatonError::Config(msg) => assert!(msg.contains("ANTHROPIC_API_KEY")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn blank_auth_token_errors_naming_the_var() {
        // No API key set, so AUTH_TOKEN is the first present credential var.
        // A blank value is an error, even though a later
        // CLAUDE_CODE_OAUTH_TOKEN is valid.
        let err = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_AUTH_TOKEN", "  "),
            ("CLAUDE_CODE_OAUTH_TOKEN", "valid"),
        ]))
        .unwrap_err();
        match err {
            BatonError::Config(msg) => {
                assert!(msg.contains("ANTHROPIC_AUTH_TOKEN"));
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn blank_first_present_does_not_fall_through_to_later_var() {
        // The plan requires that a blank first-present credential var is an
        // error even when a later candidate is valid. Here the API key is
        // unset (so AUTH_TOKEN is the first present), but it is blank — and a
        // later valid CLAUDE_CODE_OAUTH_TOKEN is set. We must still error
        // naming ANTHROPIC_AUTH_TOKEN, not silently fall through.
        let err = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_AUTH_TOKEN", ""),
            ("CLAUDE_CODE_OAUTH_TOKEN", "valid"),
        ]))
        .unwrap_err();
        match err {
            BatonError::Config(msg) => {
                assert!(
                    msg.contains("ANTHROPIC_AUTH_TOKEN"),
                    "expected message naming ANTHROPIC_AUTH_TOKEN, got: {msg}"
                );
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn blank_optional_falls_back_to_default() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_API_KEY", "secret"),
            ("BATON_MODEL", "  "),
        ]))
        .expect("config should load");
        assert_eq!(cfg.model, DEFAULT_MODEL);
    }

    #[test]
    fn non_numeric_timeout_errors() {
        let err = BatonConfig::from_lookup(lookup_from(&[
            ("ANTHROPIC_API_KEY", "secret"),
            ("BATON_TIMEOUT_SECS", "soon"),
        ]))
        .unwrap_err();
        assert!(matches!(err, BatonError::Config(_)));
    }
}
