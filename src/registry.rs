//! Static name → mailbox routing registry.
//!
//! An N-party conversation ring ([`crate::converse::converse_ring`]) addresses
//! its members by name, but a name is not a location: the driver needs a
//! concrete `{inbox, outbox}` pair to build each remote peer's
//! [`MailboxParticipant`](crate::participant::MailboxParticipant). This registry
//! is that lookup — a file loaded **once at startup**, mapping each participant
//! name to its mailbox pair.
//!
//! It is *pure lookup*: it holds no governance and makes no policy decision. The
//! driver remains the sole governance authority; the registry only answers
//! "where does a message to `alice` go?". Names are validated with the same
//! [`is_safe_key`](crate::mailbox::is_safe_key) guard the mailbox uses, so a
//! hostile name cannot escape the mailbox root via path components. Resolution
//! fails fast — an unknown name is a startup error, consistent with round-robin
//! never producing an unroutable name at runtime.
//!
//! ## Config format (JSON)
//!
//! ```json
//! {
//!   "participants": {
//!     "alice": { "inbox": "/tmp/alice/inbox", "outbox": "/tmp/alice/outbox" },
//!     "bob":   { "inbox": "/tmp/bob/inbox",   "outbox": "/tmp/bob/outbox" }
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{BatonError, Result};
use crate::mailbox::is_safe_key;

/// One participant's mailbox pair: where requests to it are delivered and where
/// its correlated replies are awaited.
///
/// A *pair*, not a single path — each remote peer is a
/// [`MailboxParticipant`](crate::participant::MailboxParticipant) with its own
/// inbox and outbox.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MailboxRef {
    /// Root of the peer's mailbox; requests are delivered to `<inbox>/pending/`.
    pub inbox: PathBuf,
    /// Directory the correlated reply is awaited from (the peer's outbox).
    pub outbox: PathBuf,
    /// Per-role max-runtime threshold, in milliseconds, above which a claim is
    /// read as `crashed-stale` by `baton status`. Optional and back-compatible:
    /// an omitted value leaves the threshold to the `status` caller (its
    /// `--max-runtime-ms` override or the documented default).
    #[serde(default)]
    pub max_runtime_ms: Option<u64>,
}

/// A static name → mailbox registry, loaded once at startup.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Registry {
    /// Each participant name mapped to its mailbox pair.
    participants: HashMap<String, MailboxRef>,
}

impl Registry {
    /// Loads and validates a registry from the JSON file at `path`.
    ///
    /// A missing or unreadable file, malformed JSON, or a participant name that
    /// is not a [safe key](crate::mailbox::is_safe_key) is a
    /// [`BatonError::Config`] — the command fails at startup, before any turn
    /// runs. An empty registry is allowed here; a roster's ≥2-member requirement
    /// is enforced where the ring is built.
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|err| {
            BatonError::Config(format!(
                "routing registry could not be read ({}): {err}",
                path.display()
            ))
        })?;
        let registry: Registry = serde_json::from_str(&raw).map_err(|err| {
            BatonError::Config(format!(
                "routing registry is not valid JSON ({}): {err}",
                path.display()
            ))
        })?;
        registry.validate_names()?;
        Ok(registry)
    }

    /// Rejects any participant name that could escape the mailbox root via path
    /// components, using the mailbox's own key guard.
    fn validate_names(&self) -> Result<()> {
        for name in self.participants.keys() {
            if !is_safe_key(name) {
                return Err(BatonError::Config(format!(
                    "routing registry participant name is not a safe mailbox key: {name:?}"
                )));
            }
        }
        Ok(())
    }

    /// Resolves `name` to its mailbox pair, or a [`BatonError::Config`] naming
    /// the unknown name.
    ///
    /// Fail-fast is deliberate: the ring driver never coins a name that is not in
    /// the roster, and every roster name is resolved at startup, so an unknown
    /// name is a misconfiguration surfaced before the conversation begins — not a
    /// runtime routing failure.
    pub fn resolve(&self, name: &str) -> Result<&MailboxRef> {
        self.participants.get(name).ok_or_else(|| {
            BatonError::Config(format!(
                "routing registry has no participant named {name:?}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes `content` to a uniquely-named temp file and returns its path,
    /// mirroring the temp-file idiom in `config.rs` tests.
    fn write_temp(tag: &str, content: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("baton-registry-{}-{tag}.json", std::process::id()));
        std::fs::write(&path, content).expect("write temp registry");
        path
    }

    const THREE_PARTY: &str = r#"{
        "participants": {
            "alice": { "inbox": "/tmp/alice/inbox", "outbox": "/tmp/alice/outbox" },
            "bob":   { "inbox": "/tmp/bob/inbox",   "outbox": "/tmp/bob/outbox" },
            "carol": { "inbox": "/tmp/carol/inbox", "outbox": "/tmp/carol/outbox" }
        }
    }"#;

    #[test]
    fn parses_and_resolves_each_name() {
        let path = write_temp("valid", THREE_PARTY);
        let registry = Registry::from_path(&path).expect("loads");
        assert_eq!(
            registry.resolve("bob").expect("bob resolves"),
            &MailboxRef {
                inbox: PathBuf::from("/tmp/bob/inbox"),
                outbox: PathBuf::from("/tmp/bob/outbox"),
                max_runtime_ms: None,
            }
        );
        assert_eq!(
            registry.resolve("carol").expect("carol resolves").inbox,
            PathBuf::from("/tmp/carol/inbox")
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unknown_name_is_config_error_naming_it() {
        let path = write_temp("unknown", THREE_PARTY);
        let registry = Registry::from_path(&path).expect("loads");
        match registry.resolve("dave").unwrap_err() {
            BatonError::Config(msg) => assert!(
                msg.contains("dave"),
                "message should name the unknown participant, got: {msg}"
            ),
            other => panic!("expected Config, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unsafe_names_are_rejected_at_load() {
        for bad in ["../evil", "a/b", "..", ".", ""] {
            let json = format!(
                r#"{{ "participants": {{ "{bad}": {{ "inbox": "/tmp/i", "outbox": "/tmp/o" }} }} }}"#
            );
            let path = write_temp(&format!("unsafe-{}", bad.len()), &json);
            match Registry::from_path(&path).unwrap_err() {
                BatonError::Config(_) => {}
                other => panic!("expected Config for name {bad:?}, got {other:?}"),
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn missing_file_is_config_error_naming_the_path() {
        let mut path = std::env::temp_dir();
        path.push(format!("baton-registry-{}-absent.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let path_str = path.display().to_string();
        match Registry::from_path(&path).unwrap_err() {
            BatonError::Config(msg) => assert!(
                msg.contains(&path_str),
                "message should name the path, got: {msg}"
            ),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn malformed_json_is_config_error() {
        let path = write_temp("malformed", "{ not json");
        match Registry::from_path(&path).unwrap_err() {
            BatonError::Config(msg) => assert!(msg.contains("JSON"), "got: {msg}"),
            other => panic!("expected Config, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_registry_loads_but_resolves_nothing() {
        let path = write_temp("empty", r#"{ "participants": {} }"#);
        let registry = Registry::from_path(&path).expect("empty is allowed at load");
        assert!(registry.resolve("anyone").is_err());
        let _ = std::fs::remove_file(&path);
    }
}
