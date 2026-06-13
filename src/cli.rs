//! Command-line entry surface for Baton.
//!
//! This module owns the boundary between process entry and the runtime. It
//! parses arguments, loads configuration, and drives the single-turn
//! first-prompt / first-reply path via the [`Transport`] boundary.
//!
//! The only command today is `baton ask -p "..."`: one prompt in, one reply
//! out. Argument parsing ([`parse_args`]) and the exchange itself
//! ([`execute_ask`]) are kept transport-agnostic and sink-agnostic so every
//! branch is unit-testable without a network or real environment — mirroring
//! [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
//!
//! Each exchange is also recorded as structured JSONL via an [`EventSink`] when
//! `BATON_EVENT_LOG` names a file (see [`open_event_sink`]). Recording is
//! auxiliary: stdout stays "assistant text and nothing else", and a failed
//! event write degrades to a stderr warning rather than failing the command.
//!
//! Scope is deliberately narrow: no REPL, no conversation state, no streaming,
//! no tool execution.

use std::fs::OpenOptions;
use std::time::Instant;

use crate::config::BatonConfig;
use crate::error::{BatonError, Result};
use crate::events::{EventSink, ExchangeEvent, ExchangeMeta, NoopSink, WriterSink, now_ms};
use crate::model::Prompt;
use crate::transport::Transport;
use crate::transport::claude::ClaudeClient;

/// Environment variable naming the JSONL event-log file. Unset or blank ⇒
/// recording is disabled.
pub const EVENT_LOG_ENV: &str = "BATON_EVENT_LOG";

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
            let meta = ExchangeMeta {
                model: config.model.clone(),
                base_url: config.base_url.clone(),
            };
            let mut sink = open_event_sink()?;
            let client = ClaudeClient::from_config(config);
            let reply = execute_ask(&client, sink.as_mut(), &meta, &prompt)?;
            println!("{reply}");
            Ok(())
        }
    }
}

/// Runs one exchange: records the request event, times the call, records the
/// outcome event, and returns only the assistant text.
///
/// Split out — and parameterised over a [`Transport`] and an [`EventSink`] — so
/// the "stdout is the assistant text and nothing else" contract and the event
/// trail can both be exercised with fakes, without a network or real config. A
/// failed event write is reported on stderr but never changes the exchange
/// result.
fn execute_ask(
    transport: &impl Transport,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    prompt: &str,
) -> Result<String> {
    emit(sink, &ExchangeEvent::request(now_ms(), meta, prompt));

    let start = Instant::now();
    let result = transport.send(&Prompt::new(prompt));
    let duration_ms = start.elapsed().as_millis() as u64;

    let event = match &result {
        Ok(reply) => ExchangeEvent::response_ok(now_ms(), duration_ms, &reply.text),
        Err(err) => ExchangeEvent::response_error(now_ms(), duration_ms, err),
    };
    emit(sink, &event);

    result.map(|reply| reply.text)
}

/// Records `event`, downgrading a persistence failure to a stderr warning.
///
/// The event trail is observability, not the user's result — a log write that
/// fails must not abort the command or pollute the stdout reply contract.
fn emit(sink: &mut dyn EventSink, event: &ExchangeEvent) {
    if let Err(err) = sink.record(event) {
        eprintln!("warning: failed to record exchange event: {err}");
    }
}

/// Opens the event sink described by [`EVENT_LOG_ENV`].
///
/// A non-blank path is opened for appending (created if absent), so successive
/// runs accumulate one exchange trail. An unset or blank value disables
/// recording. A genuine open failure is surfaced — recording was explicitly
/// requested, so silently dropping it would be wrong.
fn open_event_sink() -> Result<Box<dyn EventSink>> {
    match std::env::var(EVENT_LOG_ENV) {
        Ok(path) if !path.trim().is_empty() => {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|err| {
                    BatonError::Io(format!("failed to open {EVENT_LOG_ENV} {path:?}: {err}"))
                })?;
            Ok(Box::new(WriterSink::new(file)))
        }
        _ => Ok(Box::new(NoopSink)),
    }
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

    /// An [`EventSink`] that captures every recorded event in order.
    struct RecordingSink {
        events: Vec<ExchangeEvent>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self { events: Vec::new() }
        }
    }

    impl EventSink for RecordingSink {
        fn record(&mut self, event: &ExchangeEvent) -> std::io::Result<()> {
            self.events.push(event.clone());
            Ok(())
        }
    }

    /// An [`EventSink`] whose every write fails, to prove recording errors are
    /// swallowed rather than aborting the exchange.
    struct FailingSink;

    impl EventSink for FailingSink {
        fn record(&mut self, _event: &ExchangeEvent) -> std::io::Result<()> {
            Err(std::io::Error::other("sink is broken"))
        }
    }

    fn test_meta() -> ExchangeMeta {
        ExchangeMeta {
            model: "claude-test-model".to_string(),
            base_url: "https://api.anthropic.com".to_string(),
        }
    }

    #[test]
    fn execute_ask_returns_only_reply_text_and_forwards_prompt() {
        let transport = OkTransport {
            text: "the answer".to_string(),
            seen: RefCell::new(None),
        };
        let mut sink = NoopSink;
        let out = execute_ask(&transport, &mut sink, &test_meta(), "the question")
            .expect("should succeed");
        assert_eq!(out, "the answer");
        assert_eq!(transport.seen.borrow().as_deref(), Some("the question"));
    }

    #[test]
    fn execute_ask_propagates_transport_error() {
        let mut sink = NoopSink;
        assert!(matches!(
            execute_ask(&ErrTransport, &mut sink, &test_meta(), "hi").unwrap_err(),
            BatonError::Transport(_)
        ));
    }

    #[test]
    fn execute_ask_records_request_then_success_outcome() {
        let transport = OkTransport {
            text: "the answer".to_string(),
            seen: RefCell::new(None),
        };
        let mut sink = RecordingSink::new();
        execute_ask(&transport, &mut sink, &test_meta(), "the question").expect("should succeed");

        assert_eq!(sink.events.len(), 2, "request + outcome");
        match &sink.events[0] {
            ExchangeEvent::Request { prompt, model, .. } => {
                assert_eq!(prompt, "the question");
                assert_eq!(model, "claude-test-model");
            }
            other => panic!("expected Request, got {other:?}"),
        }
        match &sink.events[1] {
            ExchangeEvent::ResponseOk { reply, .. } => assert_eq!(reply, "the answer"),
            other => panic!("expected ResponseOk, got {other:?}"),
        }
    }

    #[test]
    fn execute_ask_records_error_outcome_even_on_failure() {
        let mut sink = RecordingSink::new();
        execute_ask(&ErrTransport, &mut sink, &test_meta(), "hi").expect_err("transport fails");

        assert_eq!(sink.events.len(), 2, "request + error outcome");
        assert!(matches!(sink.events[0], ExchangeEvent::Request { .. }));
        match &sink.events[1] {
            ExchangeEvent::ResponseError { kind, .. } => assert_eq!(*kind, "transport"),
            other => panic!("expected ResponseError, got {other:?}"),
        }
    }

    #[test]
    fn execute_ask_succeeds_even_when_event_recording_fails() {
        let transport = OkTransport {
            text: "the answer".to_string(),
            seen: RefCell::new(None),
        };
        let mut sink = FailingSink;
        // A sink that fails on every write must not change the exchange result.
        let out = execute_ask(&transport, &mut sink, &test_meta(), "the question")
            .expect("recording failure must not abort the exchange");
        assert_eq!(out, "the answer");
    }
}
