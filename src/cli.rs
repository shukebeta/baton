//! Command-line entry surface for Baton.
//!
//! This module owns the boundary between process entry and the runtime. It
//! parses arguments, loads configuration, and drives the single-turn
//! first-prompt / first-reply path via the [`Transport`] boundary.
//!
//! The only command today is `baton ask -p "..."`: one prompt in, one reply
//! out. Argument parsing ([`parse_args`]) and reply formatting ([`execute_ask`])
//! are kept pure and transport-agnostic so every branch is unit-testable without
//! a network or real environment — mirroring
//! [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
//!
//! Scope is deliberately narrow: no REPL, no conversation state, no streaming,
//! no tool execution.

use crate::config::BatonConfig;
use crate::error::{BatonError, Result};
use crate::model::Prompt;
use crate::transport::Transport;
use crate::transport::claude::ClaudeClient;

/// One-line usage summary, appended to argument errors.
pub const USAGE: &str = "usage: baton ask -p|--prompt <text>";

/// A parsed CLI invocation.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Send a single prompt and print the assistant reply.
    Ask { prompt: String },
}

/// Process entry point: parse arguments and dispatch.
///
/// On success the assistant reply text — and nothing else — is written to
/// stdout. All failures are returned as a [`BatonError`] for `main` to surface
/// on stderr with a non-zero exit code.
pub fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_args(&args)? {
        Command::Ask { prompt } => {
            let config = BatonConfig::from_env()?;
            let client = ClaudeClient::from_config(config);
            let reply = execute_ask(&client, &prompt)?;
            println!("{reply}");
            Ok(())
        }
    }
}

/// Sends `prompt` over `transport` and returns only the assistant text.
///
/// Split out so the "stdout is the assistant text and nothing else" contract can
/// be exercised against a fake transport, without a network or real config.
fn execute_ask(transport: &impl Transport, prompt: &str) -> Result<String> {
    let reply = transport.send(&Prompt::new(prompt))?;
    Ok(reply.text)
}

/// Parses CLI arguments (already stripped of the binary name) into a [`Command`].
///
/// Pure and environment-free so every branch is unit-testable.
fn parse_args(args: &[String]) -> Result<Command> {
    let mut iter = args.iter();
    let command = iter.next().ok_or_else(|| usage("no command given"))?;
    match command.as_str() {
        "ask" => parse_ask(iter),
        other => Err(usage(&format!("unknown command {other:?}"))),
    }
}

/// Parses the arguments following the `ask` subcommand.
///
/// Accepts `-p <text>`, `--prompt <text>`, and `--prompt=<text>`. The prompt is
/// required and must not be blank; anything else is a usage error.
fn parse_ask<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut prompt: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-p" | "--prompt" => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage(&format!("{arg} requires a value")))?;
                prompt = Some(value.clone());
            }
            other if other.starts_with("--prompt=") => {
                prompt = Some(other["--prompt=".len()..].to_string());
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    let prompt = prompt.ok_or_else(|| usage("missing required -p/--prompt argument"))?;
    if prompt.trim().is_empty() {
        return Err(usage("prompt must not be empty"));
    }
    Ok(Command::Ask { prompt })
}

/// Builds a usage error carrying `detail` and the one-line usage summary.
fn usage(detail: &str) -> BatonError {
    BatonError::Usage(format!("{detail}\n{USAGE}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AssistantReply;
    use std::cell::RefCell;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_short_flag() {
        let cmd = parse_args(&argv(&["ask", "-p", "hello"])).expect("should parse");
        assert_eq!(
            cmd,
            Command::Ask {
                prompt: "hello".to_string()
            }
        );
    }

    #[test]
    fn parses_long_flag() {
        let cmd = parse_args(&argv(&["ask", "--prompt", "hello world"])).expect("should parse");
        assert_eq!(
            cmd,
            Command::Ask {
                prompt: "hello world".to_string()
            }
        );
    }

    #[test]
    fn parses_long_flag_with_equals() {
        let cmd = parse_args(&argv(&["ask", "--prompt=hi there"])).expect("should parse");
        assert_eq!(
            cmd,
            Command::Ask {
                prompt: "hi there".to_string()
            }
        );
    }

    #[test]
    fn no_command_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&[])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn unknown_command_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["chat", "-p", "hi"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn ask_without_prompt_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["ask"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn flag_without_value_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["ask", "-p"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn blank_prompt_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["ask", "-p", "   "])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn unexpected_argument_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["ask", "-p", "hi", "extra"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    /// A transport that returns a canned reply and records the prompt it saw.
    struct OkTransport {
        text: String,
        seen: RefCell<Option<String>>,
    }

    impl Transport for OkTransport {
        fn send(&self, prompt: &Prompt) -> Result<AssistantReply> {
            *self.seen.borrow_mut() = Some(prompt.text.clone());
            Ok(AssistantReply::new(self.text.clone()))
        }
    }

    /// A transport that always fails at the transport layer.
    struct ErrTransport;

    impl Transport for ErrTransport {
        fn send(&self, _prompt: &Prompt) -> Result<AssistantReply> {
            Err(BatonError::Transport("network down".to_string()))
        }
    }

    #[test]
    fn execute_ask_returns_only_reply_text_and_forwards_prompt() {
        let transport = OkTransport {
            text: "the answer".to_string(),
            seen: RefCell::new(None),
        };
        let out = execute_ask(&transport, "the question").expect("should succeed");
        assert_eq!(out, "the answer");
        assert_eq!(transport.seen.borrow().as_deref(), Some("the question"));
    }

    #[test]
    fn execute_ask_propagates_transport_error() {
        assert!(matches!(
            execute_ask(&ErrTransport, "hi").unwrap_err(),
            BatonError::Transport(_)
        ));
    }
}
