//! Per-role home directories and layered identity resolution.
//!
//! A multi-party party has a distinct identity — system prompt, model,
//! credential, working directory, MCP config. This module makes that identity a
//! **per-role home directory** under a baton home root: `roles/<name>/` owns a
//! role's `config.json`, its optional `system.md`, MCP config, and (per #82) its
//! session history — analogous to `~/.claude` with one subdirectory per role. A
//! top-level `defaults.json` is inherited by every role, so common settings are
//! written once.
//!
//! Resolution is layered `env > role config > defaults`, exposed via
//! [`Identity::as_lookup`] as the exact `Fn(&str) -> Option<String>` the
//! [`BatonConfig`](crate::config::BatonConfig) loader already consumes — so the
//! full `flag > env > role > defaults > built-in default` chain falls out of the
//! existing machinery untouched: flags are applied by the caller over the loaded
//! config, and the built-in default is the loader's own fallback. With no role
//! selected the lookup is just the process environment, byte-for-byte the prior
//! behaviour. Credentials are always **referenced, never embedded**: a role names
//! the env var that carries its secret, and [`Identity`] never surfaces the
//! secret value — only the reference.
//!
//! ## Layout
//!
//! ```text
//! <BATON_HOME>/                 # BATON_HOME, else $HOME/.baton
//!   defaults.json               # base config inherited by every role
//!   roles/
//!     alice/
//!       config.json             # alice's identity overrides
//!       system.md               # optional; the default system prompt when
//!                               #   config.json names none
//! ```
//!
//! ## Role config (`config.json`)
//!
//! ```json
//! {
//!   "model": "claude-opus-4-8",
//!   "system_prompt": "system.md",
//!   "credential": { "kind": "oauth", "env": "ALICE_TOKEN" },
//!   "cwd": "/work/alice",
//!   "mcp_config": "mcp.json"
//! }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::{DEFAULT_BASE_URL, DEFAULT_MAX_TOKENS, DEFAULT_MODEL, DEFAULT_TIMEOUT_SECS};
use crate::error::{BatonError, Result};
use crate::mailbox::is_safe_key;

/// Environment variable naming the baton home root. Unset ⇒ `$HOME/.baton`.
pub const HOME_ENV: &str = "BATON_HOME";

/// The three credential env vars, in the same precedence order
/// [`crate::config`] resolves them. A role's `credential` reference is only
/// consulted when none of these is set directly in the environment (env
/// overrides the config file).
const CREDENTIAL_VARS: [&str; 3] = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
];

/// Which layer supplied a resolved identity value, for `baton role show`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The process environment.
    Env,
    /// The role's own `config.json`.
    Role,
    /// The shared `defaults.json`.
    Defaults,
    /// No layer supplied it; the built-in default (or unset) applies.
    Default,
}

impl Source {
    /// A stable lowercase label for display.
    pub fn label(self) -> &'static str {
        match self {
            Source::Env => "env",
            Source::Role => "role",
            Source::Defaults => "defaults",
            Source::Default => "default",
        }
    }
}

/// A role's credential reference: the auth scheme plus the env var that carries
/// the secret. The secret itself is never stored here — only its location.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CredentialRef {
    /// The auth scheme the referenced secret uses.
    pub kind: CredentialKind,
    /// The env var name holding the secret (never the secret itself).
    pub env: String,
}

/// The auth scheme a [`CredentialRef`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    /// An Anthropic API key, resolved under `ANTHROPIC_API_KEY`.
    ApiKey,
    /// An OAuth bearer token, resolved under `ANTHROPIC_AUTH_TOKEN`.
    Oauth,
}

impl CredentialKind {
    /// The credential env var this scheme injects the referenced secret under,
    /// so [`crate::config`] resolves it exactly as a directly-set variable.
    fn env_var(self) -> &'static str {
        match self {
            CredentialKind::ApiKey => "ANTHROPIC_API_KEY",
            CredentialKind::Oauth => "ANTHROPIC_AUTH_TOKEN",
        }
    }

    fn label(self) -> &'static str {
        match self {
            CredentialKind::ApiKey => "api_key",
            CredentialKind::Oauth => "oauth",
        }
    }
}

/// A role's on-disk identity config. Every field is optional; an absent field
/// falls through to the next layer (`defaults.json`, then the built-in default).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    /// Model id (`BATON_MODEL`).
    #[serde(default)]
    pub model: Option<String>,
    /// Messages API base URL (`ANTHROPIC_BASE_URL`).
    #[serde(default)]
    pub base_url: Option<String>,
    /// System-prompt file path (`BATON_SYSTEM_PROMPT`). A relative path resolves
    /// against the owning directory (the role dir for a role, the home root for
    /// `defaults.json`), keeping a role's home portable.
    #[serde(default)]
    pub system_prompt: Option<String>,
    /// Credential reference (never the secret).
    #[serde(default)]
    pub credential: Option<CredentialRef>,
    /// Working directory for an external agent (`serve --agent-cwd`).
    #[serde(default)]
    pub cwd: Option<String>,
    /// MCP config file path (`serve --agent-mcp-config`). Relative paths resolve
    /// as for `system_prompt`.
    #[serde(default)]
    pub mcp_config: Option<String>,
    /// Per-request timeout in seconds (`BATON_TIMEOUT_SECS`).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Max output tokens per reply (`BATON_MAX_TOKENS`).
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

impl RoleConfig {
    /// Loads a config from `path`. A missing file is an empty config (not an
    /// error), so a role that inherits everything from `defaults.json` needs no
    /// `config.json` at all. Malformed JSON is a [`BatonError::Config`] naming
    /// only that file — a broken role config breaks only that role.
    fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw).map_err(|err| {
                BatonError::Config(format!(
                    "role config is not valid JSON ({}): {err}",
                    path.display()
                ))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(err) => Err(BatonError::Io(format!(
                "could not read role config {}: {err}",
                path.display()
            ))),
        }
    }
}

/// The baton home root and its `roles/<name>/` layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolesHome {
    root: PathBuf,
}

impl RolesHome {
    /// Resolves the home root: `BATON_HOME` when set (and non-blank), else
    /// `$HOME/.baton` (`%USERPROFILE%\.baton` on Windows). The directory is not
    /// required to exist — resolution, listing, and `role show` all tolerate an
    /// absent home; it is created lazily by whoever first writes into it.
    pub fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let root = match non_empty(lookup(HOME_ENV)) {
            Some(dir) => PathBuf::from(dir),
            None => {
                let home = non_empty(lookup("HOME"))
                    .or_else(|| non_empty(lookup("USERPROFILE")))
                    .ok_or_else(|| {
                        BatonError::Config(
                            "cannot resolve the baton home: set BATON_HOME, or HOME".to_string(),
                        )
                    })?;
                PathBuf::from(home).join(".baton")
            }
        };
        Ok(Self { root })
    }

    /// Convenience for [`RolesHome::resolve`] over the process environment.
    pub fn from_env() -> Result<Self> {
        Self::resolve(|key| std::env::var(key).ok())
    }

    /// The home root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The `roles/` directory holding one subdirectory per role.
    pub fn roles_dir(&self) -> PathBuf {
        self.root.join("roles")
    }

    /// The shared `defaults.json` path.
    pub fn defaults_path(&self) -> PathBuf {
        self.root.join("defaults.json")
    }

    /// The `roles/<name>/` directory for `name`, rejecting a name that could
    /// escape the roles root via path components (the mailbox's own key guard).
    pub fn role_dir(&self, name: &str) -> Result<PathBuf> {
        if !is_safe_key(name) {
            return Err(BatonError::Config(format!(
                "role name is not a safe directory key: {name:?}"
            )));
        }
        Ok(self.roles_dir().join(name))
    }

    /// Lists the role names — the sorted subdirectory names under `roles/`. An
    /// absent `roles/` directory lists nothing (not an error).
    pub fn list_roles(&self) -> Result<Vec<String>> {
        let dir = self.roles_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read roles directory {}: {err}",
                    dir.display()
                )));
            }
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|err| BatonError::Io(format!("could not read a roles entry: {err}")))?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if is_safe_key(&name) {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    /// Resolves `role`'s effective identity by layering `env > role config >
    /// defaults`. `lookup` supplies the environment (parameterised for testing).
    pub fn resolve_identity(
        &self,
        role: &str,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Identity> {
        let role_dir = self.role_dir(role)?;
        let role_cfg = RoleConfig::load(&role_dir.join("config.json"))?;
        let defaults = RoleConfig::load(&self.defaults_path())?;
        Ok(build_identity(
            &role_dir, &self.root, &role_cfg, &defaults, &lookup,
        ))
    }
}

/// One resolved identity field: its friendly key, the resolved value (or `None`
/// when nothing supplies it), and the layer it came from. `secret_ref` marks the
/// credential entry, whose value is a reference (`kind (env NAME)`), never a
/// secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityValue {
    /// The friendly config-file key (e.g. `model`, `system_prompt`).
    pub key: &'static str,
    /// The resolved value, or `None` when unset with no built-in default.
    pub value: Option<String>,
    /// Which layer supplied the value.
    pub source: Source,
    /// True for the credential entry: `value` is a reference, not the secret.
    pub secret_ref: bool,
}

/// A role's resolved effective identity: an ordered view for `role show`, plus
/// the env-namespace lookup the [`BatonConfig`](crate::config::BatonConfig)
/// loader consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    entries: Vec<IdentityValue>,
    lookup: HashMap<String, String>,
}

impl Identity {
    /// The ordered per-field resolution, for `baton role show`.
    pub fn entries(&self) -> &[IdentityValue] {
        &self.entries
    }

    /// The resolved value for a friendly key (e.g. `cwd`, `mcp_config`,
    /// `system_prompt`), for `serve --role` agent-mode wiring. `None` when unset.
    pub fn value_of(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.key == key)
            .and_then(|e| e.value.as_deref())
    }

    /// A `Fn(&str) -> Option<String>` over the env-var namespace, for
    /// [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
    ///
    /// It carries the env-wins-then-config resolution for every variable the
    /// loader reads, so config values never shadow a directly-set env var, and a
    /// key no layer supplied returns `None` (letting the loader apply its own
    /// built-in default).
    pub fn as_lookup(&self) -> impl Fn(&str) -> Option<String> + '_ {
        move |key: &str| self.lookup.get(key).cloned()
    }
}

/// Builds a role's [`Identity`] from its config, the defaults, and the
/// environment. Pure over its inputs (path existence for the auto `system.md` is
/// the only filesystem touch), so precedence and provenance are unit-testable.
fn build_identity(
    role_dir: &Path,
    home_root: &Path,
    role: &RoleConfig,
    defaults: &RoleConfig,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Identity {
    let mut entries = Vec::new();
    let mut map: HashMap<String, String> = HashMap::new();

    // One simple pass-through field: (env var, friendly key, env value, role
    // value, built-in default). The env/role values are the already-resolved
    // per-layer strings; defaults are looked up separately below.
    type SimpleField = (
        &'static str,
        &'static str,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let simple: [SimpleField; 5] = [
        (
            "BATON_MODEL",
            "model",
            non_empty(lookup("BATON_MODEL")),
            role.model.clone(),
            Some(DEFAULT_MODEL.to_string()),
        ),
        (
            "ANTHROPIC_BASE_URL",
            "base_url",
            non_empty(lookup("ANTHROPIC_BASE_URL")),
            role.base_url.clone(),
            Some(DEFAULT_BASE_URL.to_string()),
        ),
        (
            "BATON_TIMEOUT_SECS",
            "timeout_secs",
            non_empty(lookup("BATON_TIMEOUT_SECS")),
            role.timeout_secs.map(|n| n.to_string()),
            Some(DEFAULT_TIMEOUT_SECS.to_string()),
        ),
        (
            "BATON_MAX_TOKENS",
            "max_tokens",
            non_empty(lookup("BATON_MAX_TOKENS")),
            role.max_tokens.map(|n| n.to_string()),
            Some(DEFAULT_MAX_TOKENS.to_string()),
        ),
        (
            "BATON_SYSTEM_PROMPT",
            "system_prompt",
            non_empty(lookup("BATON_SYSTEM_PROMPT")),
            resolve_system_prompt(role_dir, role),
            None,
        ),
    ];

    // Order the simple entries for display: model, base_url, system_prompt,
    // then (credential inserted below), cwd, mcp_config, timeout, max_tokens.
    let defaults_simple: HashMap<&str, Option<String>> = HashMap::from([
        ("model", defaults.model.clone()),
        ("base_url", defaults.base_url.clone()),
        (
            "system_prompt",
            defaults
                .system_prompt
                .as_deref()
                .map(|p| absolutize(home_root, p)),
        ),
        ("timeout_secs", defaults.timeout_secs.map(|n| n.to_string())),
        ("max_tokens", defaults.max_tokens.map(|n| n.to_string())),
    ]);

    for (env_var, friendly, env_val, role_val, builtin) in simple {
        let defaults_val = defaults_simple.get(friendly).cloned().flatten();
        let picked = pick(env_val, role_val, defaults_val);
        if let Some((value, _)) = &picked {
            map.insert(env_var.to_string(), value.clone());
        }
        let (value, source) = match picked {
            Some((value, source)) => (Some(value), source),
            None => (builtin, Source::Default),
        };
        entries.push(IdentityValue {
            key: friendly,
            value,
            source,
            secret_ref: false,
        });
    }

    // Credential: a reference, resolved only when the environment sets no
    // credential directly (env overrides the config file). The referenced
    // secret is injected under the scheme's env var so the loader resolves it
    // exactly as a directly-set variable; the entry surfaces only the reference.
    entries.push(resolve_credential(role, defaults, lookup, &mut map));

    // cwd / mcp_config: agent-mode paths, not part of the loader's env
    // namespace, so they populate the display/serve view but not `map`.
    entries.push(path_entry(
        "cwd",
        role.cwd.as_deref().map(|p| absolutize(role_dir, p)),
        defaults.cwd.as_deref().map(|p| absolutize(home_root, p)),
    ));
    entries.push(path_entry(
        "mcp_config",
        role.mcp_config.as_deref().map(|p| absolutize(role_dir, p)),
        defaults
            .mcp_config
            .as_deref()
            .map(|p| absolutize(home_root, p)),
    ));

    // Re-order to the documented display order.
    entries.sort_by_key(|e| display_rank(e.key));
    Identity {
        entries,
        lookup: map,
    }
}

/// Resolves the role's system-prompt path layer: the explicit `system_prompt`
/// (absolutised against the role dir), else the conventional `system.md` in the
/// role dir when it exists — the "inline" ergonomics (drop a `system.md` in the
/// role's home and it is the default prompt).
fn resolve_system_prompt(role_dir: &Path, role: &RoleConfig) -> Option<String> {
    if let Some(path) = &role.system_prompt {
        return Some(absolutize(role_dir, path));
    }
    let conventional = role_dir.join("system.md");
    conventional
        .is_file()
        .then(|| conventional.to_string_lossy().into_owned())
}

/// Builds the credential entry and injects the resolved secret into `map` when a
/// role/defaults reference applies. A directly-set credential env var wins
/// wholesale (env over config): the reference is not consulted, and the loader
/// reads the env var itself (so it need not be injected here).
fn resolve_credential(
    role: &RoleConfig,
    defaults: &RoleConfig,
    lookup: &impl Fn(&str) -> Option<String>,
    map: &mut HashMap<String, String>,
) -> IdentityValue {
    let env_present: Vec<(&str, String)> = CREDENTIAL_VARS
        .iter()
        .filter_map(|var| non_empty(lookup(var)).map(|secret| (*var, secret)))
        .collect();
    if let Some((first, _)) = env_present.first() {
        // A directly-set credential wins wholesale. Pass every present env
        // credential var through the lookup so the loader resolves it exactly as
        // it would from the process environment.
        for (var, secret) in &env_present {
            map.insert(var.to_string(), secret.clone());
        }
        return IdentityValue {
            key: "credential",
            value: Some(format!("env {first}")),
            source: Source::Env,
            secret_ref: true,
        };
    }

    let referenced = role
        .credential
        .as_ref()
        .map(|c| (c, Source::Role))
        .or_else(|| defaults.credential.as_ref().map(|c| (c, Source::Defaults)));

    match referenced {
        Some((cred, source)) => {
            // Inject the referenced secret only when its env var is actually set;
            // an unset reference still shows in `role show` (helping the operator
            // spot the missing export) and simply lets the loader fail later.
            if let Some(secret) = non_empty(lookup(&cred.env)) {
                map.insert(cred.kind.env_var().to_string(), secret);
            }
            IdentityValue {
                key: "credential",
                value: Some(format!("{} (env {})", cred.kind.label(), cred.env)),
                source,
                secret_ref: true,
            }
        }
        None => IdentityValue {
            key: "credential",
            value: None,
            source: Source::Default,
            secret_ref: true,
        },
    }
}

/// Builds a non-env path entry (`cwd` / `mcp_config`) from its role/defaults
/// layers (no env var, no built-in default).
fn path_entry(
    key: &'static str,
    role_val: Option<String>,
    defaults_val: Option<String>,
) -> IdentityValue {
    match pick(None, role_val, defaults_val) {
        Some((value, source)) => IdentityValue {
            key,
            value: Some(value),
            source,
            secret_ref: false,
        },
        None => IdentityValue {
            key,
            value: None,
            source: Source::Default,
            secret_ref: false,
        },
    }
}

/// Picks the highest-precedence present value across `env > role > defaults`.
fn pick(
    env: Option<String>,
    role: Option<String>,
    defaults: Option<String>,
) -> Option<(String, Source)> {
    if let Some(v) = env {
        Some((v, Source::Env))
    } else if let Some(v) = role {
        Some((v, Source::Role))
    } else {
        defaults.map(|v| (v, Source::Defaults))
    }
}

/// Absolutises `path` against `base`; an already-absolute path is returned as-is.
fn absolutize(base: &Path, path: &str) -> String {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        path.to_string()
    } else {
        base.join(candidate).to_string_lossy().into_owned()
    }
}

/// The documented `role show` display order for a friendly key.
fn display_rank(key: &str) -> u8 {
    match key {
        "model" => 0,
        "base_url" => 1,
        "system_prompt" => 2,
        "credential" => 3,
        "cwd" => 4,
        "mcp_config" => 5,
        "timeout_secs" => 6,
        "max_tokens" => 7,
        _ => u8::MAX,
    }
}

/// Treats a present-but-blank value as absent, matching [`crate::config`].
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

    /// A unique temp dir for one test, created fresh.
    fn temp_home(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("baton-roles-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp home");
        path
    }

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(path, contents).expect("write file");
    }

    #[test]
    fn resolve_prefers_baton_home_then_home_dot_baton() {
        let explicit =
            RolesHome::resolve(lookup_from(&[("BATON_HOME", "/custom/home")])).expect("resolves");
        assert_eq!(explicit.root(), Path::new("/custom/home"));

        let derived =
            RolesHome::resolve(lookup_from(&[("HOME", "/home/alice")])).expect("resolves");
        assert_eq!(derived.root(), Path::new("/home/alice/.baton"));
    }

    #[test]
    fn resolve_without_any_home_is_config_error() {
        let err = RolesHome::resolve(lookup_from(&[])).unwrap_err();
        assert!(matches!(err, BatonError::Config(_)));
    }

    #[test]
    fn list_roles_returns_sorted_dirs_and_tolerates_absent_root() {
        let root = temp_home("list");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        // Absent roles/ lists nothing.
        assert!(home.list_roles().expect("lists").is_empty());

        std::fs::create_dir_all(home.roles_dir().join("bob")).unwrap();
        std::fs::create_dir_all(home.roles_dir().join("alice")).unwrap();
        // A stray file is not a role.
        write(&home.roles_dir().join("notes.txt"), "x");
        assert_eq!(home.list_roles().expect("lists"), vec!["alice", "bob"]);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unsafe_role_name_is_rejected() {
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", "/tmp/h")])).expect("resolves");
        assert!(home.role_dir("../evil").is_err());
        assert!(home.role_dir("a/b").is_err());
        assert!(home.role_dir("").is_err());
    }

    #[test]
    fn layered_precedence_env_over_role_over_defaults_over_builtin() {
        let root = temp_home("precedence");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        write(
            &home.defaults_path(),
            r#"{ "model": "defaults-model", "base_url": "https://defaults", "max_tokens": 111 }"#,
        );
        write(
            &home.role_dir("alice").unwrap().join("config.json"),
            r#"{ "model": "role-model", "max_tokens": 222 }"#,
        );

        // base_url comes from defaults; model from the role; max_tokens from env.
        let id = home
            .resolve_identity("alice", lookup_from(&[("BATON_MAX_TOKENS", "999")]))
            .expect("resolves identity");

        let by_key = |k: &str| id.entries().iter().find(|e| e.key == k).unwrap().clone();
        let model = by_key("model");
        assert_eq!(model.value.as_deref(), Some("role-model"));
        assert_eq!(model.source, Source::Role);

        let base = by_key("base_url");
        assert_eq!(base.value.as_deref(), Some("https://defaults"));
        assert_eq!(base.source, Source::Defaults);

        let max = by_key("max_tokens");
        assert_eq!(max.value.as_deref(), Some("999"));
        assert_eq!(max.source, Source::Env);

        // Unset with a built-in default: timeout falls back to the default.
        let timeout = by_key("timeout_secs");
        assert_eq!(
            timeout.value.as_deref(),
            Some(&*DEFAULT_TIMEOUT_SECS.to_string())
        );
        assert_eq!(timeout.source, Source::Default);

        // The lookup carries env-resolved values for the loader.
        let lk = id.as_lookup();
        assert_eq!(lk("BATON_MODEL").as_deref(), Some("role-model"));
        assert_eq!(lk("BATON_MAX_TOKENS").as_deref(), Some("999"));
        // A defaulted key is absent from the lookup (loader applies its default).
        assert_eq!(lk("BATON_TIMEOUT_SECS"), None);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn env_credential_wins_over_role_reference() {
        let root = temp_home("cred-env");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        write(
            &home.role_dir("alice").unwrap().join("config.json"),
            r#"{ "credential": { "kind": "oauth", "env": "ALICE_TOKEN" } }"#,
        );
        let id = home
            .resolve_identity(
                "alice",
                lookup_from(&[("ANTHROPIC_API_KEY", "direct"), ("ALICE_TOKEN", "reftok")]),
            )
            .expect("resolves");
        let cred = id.entries().iter().find(|e| e.key == "credential").unwrap();
        assert_eq!(cred.source, Source::Env);
        assert!(cred.secret_ref);
        // Never surfaces the secret; the env-direct credential var passes through.
        assert_eq!(
            id.as_lookup()("ANTHROPIC_API_KEY").as_deref(),
            Some("direct")
        );
        assert_eq!(cred.value.as_deref(), Some("env ANTHROPIC_API_KEY"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn role_credential_reference_injects_secret_under_scheme_var() {
        let root = temp_home("cred-role");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        write(
            &home.role_dir("alice").unwrap().join("config.json"),
            r#"{ "credential": { "kind": "oauth", "env": "ALICE_TOKEN" } }"#,
        );
        let id = home
            .resolve_identity("alice", lookup_from(&[("ALICE_TOKEN", "secret-tok")]))
            .expect("resolves");
        let cred = id.entries().iter().find(|e| e.key == "credential").unwrap();
        assert_eq!(cred.source, Source::Role);
        assert_eq!(cred.value.as_deref(), Some("oauth (env ALICE_TOKEN)"));
        // Injected under the oauth scheme's env var for the loader.
        assert_eq!(
            id.as_lookup()("ANTHROPIC_AUTH_TOKEN").as_deref(),
            Some("secret-tok")
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn conventional_system_md_is_the_default_prompt() {
        let root = temp_home("sysmd");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        let role_dir = home.role_dir("alice").unwrap();
        write(&role_dir.join("system.md"), "You are alice.");
        // No config.json at all.
        let id = home
            .resolve_identity("alice", lookup_from(&[]))
            .expect("resolves");
        let sp = id
            .entries()
            .iter()
            .find(|e| e.key == "system_prompt")
            .unwrap();
        assert_eq!(sp.source, Source::Role);
        assert_eq!(
            sp.value.as_deref(),
            Some(&*role_dir.join("system.md").to_string_lossy())
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn relative_role_paths_absolutize_against_role_dir() {
        let root = temp_home("relpath");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        let role_dir = home.role_dir("alice").unwrap();
        write(
            &role_dir.join("config.json"),
            r#"{ "cwd": "work", "mcp_config": "mcp.json" }"#,
        );
        let id = home
            .resolve_identity("alice", lookup_from(&[]))
            .expect("resolves");
        assert_eq!(
            id.value_of("cwd"),
            Some(&*role_dir.join("work").to_string_lossy())
        );
        assert_eq!(
            id.value_of("mcp_config"),
            Some(&*role_dir.join("mcp.json").to_string_lossy())
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn malformed_role_config_errors_naming_only_that_role() {
        let root = temp_home("malformed");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        write(
            &home.role_dir("alice").unwrap().join("config.json"),
            "{ not json",
        );
        match home
            .resolve_identity("alice", lookup_from(&[]))
            .unwrap_err()
        {
            BatonError::Config(msg) => assert!(msg.contains("alice"), "got: {msg}"),
            other => panic!("expected Config, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn missing_config_and_defaults_yields_pure_env_lookup() {
        let root = temp_home("bare");
        let home = RolesHome::resolve(lookup_from(&[("BATON_HOME", root.to_str().unwrap())]))
            .expect("resolves");
        // No config.json, no defaults.json: identical to a pure env lookup.
        let id = home
            .resolve_identity("ghost", lookup_from(&[("BATON_MODEL", "envm")]))
            .expect("resolves");
        assert_eq!(id.as_lookup()("BATON_MODEL").as_deref(), Some("envm"));
        assert_eq!(id.as_lookup()("ANTHROPIC_BASE_URL"), None);
        let _ = std::fs::remove_dir_all(&root);
    }
}
