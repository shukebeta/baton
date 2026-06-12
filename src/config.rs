//! Environment-backed runtime configuration.
//!
//! [`BatonConfig`] holds everything the single-turn first-reply path needs to
//! reach a provider. Loading is split into [`BatonConfig::from_env`] (the real
//! entry point) and a pure [`BatonConfig::from_lookup`] so parsing can be tested
//! deterministically without mutating the process environment.

use std::time::Duration;

use crate::error::{BatonError, Result};

/// Default base URL for the Claude-compatible Messages API.
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// Default model id used when `BATON_MODEL` is unset.
pub const DEFAULT_MODEL: &str = "claude-sonnet-4-6";

/// Default request timeout in seconds when `BATON_TIMEOUT_SECS` is unset.
pub const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Runtime configuration for Baton's first-reply path.
#[derive(Debug, Clone)]
pub struct BatonConfig {
    /// Provider API key. Required; loaded from `ANTHROPIC_API_KEY`.
    pub api_key: String,
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
        let api_key = lookup("ANTHROPIC_API_KEY").ok_or_else(|| {
            BatonError::Config("ANTHROPIC_API_KEY is required but not set".to_string())
        })?;
        if api_key.trim().is_empty() {
            return Err(BatonError::Config(
                "ANTHROPIC_API_KEY is set but empty".to_string(),
            ));
        }

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
            api_key,
            base_url,
            model,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
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
    fn applies_defaults_when_only_key_present() {
        let cfg = BatonConfig::from_lookup(lookup_from(&[("ANTHROPIC_API_KEY", "secret")]))
            .expect("config should load");
        assert_eq!(cfg.api_key, "secret");
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
        assert_eq!(cfg.base_url, "https://proxy.example");
        assert_eq!(cfg.model, "claude-opus-4-8");
        assert_eq!(cfg.timeout, Duration::from_secs(5));
    }

    #[test]
    fn missing_key_errors() {
        let err = BatonConfig::from_lookup(lookup_from(&[])).unwrap_err();
        assert!(matches!(err, BatonError::Config(_)));
    }

    #[test]
    fn blank_key_errors() {
        let err =
            BatonConfig::from_lookup(lookup_from(&[("ANTHROPIC_API_KEY", "   ")])).unwrap_err();
        assert!(matches!(err, BatonError::Config(_)));
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
