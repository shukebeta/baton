//! Command-line entry surface for Baton.
//!
//! The first user-facing command (`baton ask -p "..."`) lands in a later ticket.
//! For now this module owns the boundary between process entry and the runtime:
//! it loads configuration and reports readiness so the binary has something real
//! to exercise while staying within this phase's single-turn, non-interactive
//! scope (no REPL, no session orchestration).

use crate::config::BatonConfig;
use crate::error::Result;

/// Banner identifying the binary.
pub const BANNER: &str = "Baton: AI-to-AI communication harness";

/// Runs the default (argument-less) invocation: load configuration and report
/// that the runtime is ready.
///
/// Argument parsing and the `ask` subcommand arrive in a later ticket; this
/// keeps the binary honest by surfacing configuration errors today.
pub fn run() -> Result<()> {
    println!("{BANNER}");
    let config = BatonConfig::from_env()?;
    println!(
        "Runtime ready: model={}, base_url={}, timeout={}s",
        config.model,
        config.base_url,
        config.timeout.as_secs(),
    );
    Ok(())
}
