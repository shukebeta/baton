//! Command-line entry surface for Baton.
//!
//! This module owns the boundary between process entry and the runtime. It
//! parses arguments, loads configuration, and drives the exchange via the
//! [`Transport`] boundary.
//!
//! The commands: `baton ask -p "..."` is single-turn (one prompt in, one reply
//! out); `baton session` is an interactive REPL accumulating a [`Conversation`]
//! and resending the full history each turn; `baton exchange` is the A2A
//! request/reply verb (one `baton.message/v1` envelope in, one out); `baton log
//! show`/`replay` inspects and re-runs the recorded trail. Argument parsing
//! ([`parse_args`]) and the exchanges themselves are kept abstract over their
//! collaborators and sink-agnostic so every branch is unit-testable without a
//! network or real environment: [`execute_ask`] / [`execute_session`] over a
//! [`Transport`], and [`execute_exchange`] over a
//! [`Participant`](crate::participant::Participant) — mirroring
//! [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
//!
//! Each exchange is also recorded as structured JSONL via an [`EventSink`] when
//! `BATON_EVENT_LOG` names a file (see [`open_event_sink`]). Recording is
//! auxiliary: for `ask`, stdout stays "assistant text and nothing else", and a
//! failed event write degrades to a stderr warning rather than failing the
//! command.
//!
//! Streaming and tool execution remain out of scope.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use crate::config::BatonConfig;
use crate::converse::{self, Governance, Transcript};
use crate::error::{BatonError, Result};
use crate::events::{EventSink, ExchangeEvent, ExchangeMeta, NoopSink, WriterSink, now_ms};
use crate::log::{self, Exchange};
use crate::message::{MessageEnvelope, MessageKind};
use crate::model::{AssistantReply, Conversation, Prompt};
use crate::participant::{LocalParticipant, Participant};
use crate::transport::Transport;
use crate::transport::claude::ClaudeClient;

/// Environment variable naming the JSONL event-log file. Unset or blank ⇒
/// recording is disabled.
pub const EVENT_LOG_ENV: &str = "BATON_EVENT_LOG";

/// One-line usage summary, appended to argument errors.
pub const USAGE: &str = "usage: baton ask -p|--prompt <text> | baton session | baton exchange [--in <path>] [--out <path>] | baton converse [--a-system <path>] [--b-system <path>] [--a-model <id>] [--b-model <id>] (--seed <text> | --seed-file <path>) [--out <path>] | baton log show|replay [--file <path>] [--index <N>]";

/// The in-session command that ends the REPL cleanly (alongside EOF).
const SESSION_EXIT_COMMAND: &str = "/exit";

/// A parsed CLI invocation.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Send a single prompt and print the assistant reply.
    Ask { prompt: String },
    /// Start an interactive multi-turn REPL.
    Session,
    /// Run one A2A envelope exchange: read a `baton.message/v1` request
    /// envelope, run the provider call, and write one response envelope.
    /// `in_path`/`out_path` default to stdin/stdout when `None`.
    Exchange {
        in_path: Option<String>,
        out_path: Option<String>,
    },
    /// Drive a governed two-participant conversation from a seed. Each side is
    /// an in-process participant configured from the environment, overridden
    /// per side by its optional system-prompt file and model. The full
    /// `baton.message/v1` trail is written as JSONL to `out_path` (stdout when
    /// `None`).
    Converse {
        a_system: Option<String>,
        b_system: Option<String>,
        a_model: Option<String>,
        b_model: Option<String>,
        seed: SeedSource,
        out_path: Option<String>,
    },
    /// Pretty-print the recorded exchange trail.
    LogShow { file: Option<String> },
    /// Re-run a recorded exchange. `index` is 1-based; `None` ⇒ the last one.
    LogReplay {
        file: Option<String>,
        index: Option<usize>,
    },
}

/// Where the `converse` seed message comes from: inline text or a file path.
/// Resolved to the opening body only in [`run`], keeping [`parse_args`] free of
/// I/O.
#[derive(Debug, PartialEq, Eq)]
enum SeedSource {
    /// Inline `--seed <text>`.
    Text(String),
    /// `--seed-file <path>`, read at run time.
    File(String),
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
            let meta = exchange_meta(&config);
            let mut sink = open_event_sink()?;
            let client = ClaudeClient::from_config(config);
            let reply = execute_ask(&client, sink.as_mut(), &meta, &prompt)?;
            println!("{reply}");
            Ok(())
        }
        Command::Session => {
            let config = BatonConfig::from_env()?;
            let meta = exchange_meta(&config);
            let mut sink = open_event_sink()?;
            let client = ClaudeClient::from_config(config);
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            execute_session(&client, sink.as_mut(), &meta, stdin.lock(), stdout.lock())
        }
        Command::Exchange { in_path, out_path } => {
            let config = BatonConfig::from_env()?;
            let meta = exchange_meta(&config);
            let mut sink = open_event_sink()?;
            let client = ClaudeClient::from_config(config);
            let participant = LocalParticipant::new(client, meta.clone());

            // Read the request first. A malformed or unreadable request envelope
            // is a usage/IO error that exits non-zero having written *nothing*
            // to the response sink — the response is emitted only after a
            // completed exchange.
            let request = read_request_envelope(open_input(in_path.as_deref())?)?;
            let response = execute_exchange(&participant, sink.as_mut(), &meta, &request);
            write_response_envelope(&response, open_output(out_path.as_deref())?)
        }
        Command::Converse {
            a_system,
            b_system,
            a_model,
            b_model,
            seed,
            out_path,
        } => {
            let base = BatonConfig::from_env()?;
            let governance = Governance::from_lookup(|key| std::env::var(key).ok())?;
            let seed_body = resolve_seed(&seed)?;

            // Each side is the base config with its own system prompt / model
            // laid over the top; the credential and base URL are shared.
            let config_a = participant_config(&base, a_system.as_deref(), a_model)?;
            let config_b = participant_config(&base, b_system.as_deref(), b_model)?;
            let participant_a = LocalParticipant::new(
                ClaudeClient::from_config(config_a.clone()),
                exchange_meta(&config_a),
            );
            let participant_b = LocalParticipant::new(
                ClaudeClient::from_config(config_b.clone()),
                exchange_meta(&config_b),
            );

            let transcript = converse::converse(
                &participant_a,
                &participant_b,
                build_seed_envelope(&seed_body),
                &governance,
            );
            eprintln!("conversation ended: {:?}", transcript.reason);
            write_transcript(&transcript, open_output(out_path.as_deref())?)
        }
        Command::LogShow { file } => {
            let exchanges = read_log(file.as_deref())?;
            let stdout = std::io::stdout();
            execute_log_show(&exchanges, stdout.lock())
        }
        Command::LogReplay { file, index } => {
            let exchanges = read_log(file.as_deref())?;
            let request = &select_exchange(&exchanges, index)?.request;

            // Replay targets the logged exchange's model + base_url, but uses
            // the *current* credential (and timeout / max_tokens / system
            // prompt) from the environment — so a replay re-runs with today's
            // auth, not a credential that was never recorded.
            let mut config = BatonConfig::from_env()?;
            config.model = request.model.clone();
            config.base_url = request.base_url.clone();

            let meta = exchange_meta(&config);
            let prompt = request.prompt.clone();
            let mut sink = open_event_sink()?;
            let client = ClaudeClient::from_config(config);
            let reply = execute_ask(&client, sink.as_mut(), &meta, &prompt)?;
            println!("{reply}");
            Ok(())
        }
    }
}

/// Resolves the log path and parses it into exchanges.
///
/// The path is `--file` when given, else [`EVENT_LOG_ENV`]; with neither set,
/// there is nothing to read, which is a usage error. A path that cannot be
/// opened is an [`BatonError::Io`]. Non-fatal warnings collected by
/// [`log::parse_jsonl`] (e.g. a tolerated trailing partial line) are surfaced on
/// stderr here, keeping `parse_jsonl` pure over its reader.
fn read_log(file: Option<&str>) -> Result<Vec<Exchange>> {
    let path = resolve_log_path(file)?;
    let handle = File::open(&path)
        .map_err(|err| BatonError::Io(format!("failed to open log file {path:?}: {err}")))?;
    let report = log::parse_jsonl(handle)?;
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }
    Ok(report.exchanges)
}

/// Resolves the log file path: `--file` takes precedence, then [`EVENT_LOG_ENV`].
///
/// A blank value (in either source) is treated as absent. With no usable path
/// from either source, there is nothing to read — a usage error rather than a
/// silent empty result.
fn resolve_log_path(file: Option<&str>) -> Result<String> {
    if let Some(path) = file.filter(|p| !p.trim().is_empty()) {
        return Ok(path.to_string());
    }
    match std::env::var(EVENT_LOG_ENV) {
        Ok(path) if !path.trim().is_empty() => Ok(path),
        _ => Err(usage(&format!(
            "no log file: pass --file <path> or set {EVENT_LOG_ENV}"
        ))),
    }
}

/// Selects the exchange to replay: 1-based `index`, or the last when `None`.
///
/// An empty log, or an index outside `1..=len`, is an error naming the valid
/// range so the user can correct it.
fn select_exchange(exchanges: &[Exchange], index: Option<usize>) -> Result<&Exchange> {
    if exchanges.is_empty() {
        return Err(BatonError::Usage(
            "log contains no complete exchanges to replay".to_string(),
        ));
    }
    let position = match index {
        None => exchanges.len() - 1,
        Some(n) if (1..=exchanges.len()).contains(&n) => n - 1,
        Some(n) => {
            return Err(usage(&format!(
                "--index {n} is out of range; valid range is 1..={}",
                exchanges.len()
            )));
        }
    };
    Ok(&exchanges[position])
}

/// Writes each exchange as a human-readable block to `output`.
///
/// Parameterised over [`Write`] so the rendering is unit-testable with an
/// in-memory buffer. An empty log produces no output.
fn execute_log_show(exchanges: &[Exchange], mut output: impl Write) -> Result<()> {
    for (i, exchange) in exchanges.iter().enumerate() {
        write!(output, "{}", log::format_exchange(i + 1, exchange)).map_err(io_err)?;
    }
    Ok(())
}

/// Builds the replay-relevant [`ExchangeMeta`] shared by every exchange in a
/// command run.
fn exchange_meta(config: &BatonConfig) -> ExchangeMeta {
    ExchangeMeta {
        model: config.model.clone(),
        base_url: config.base_url.clone(),
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
    timed_exchange(sink, meta, prompt, || transport.send(&Prompt::new(prompt)))
        .map(|reply| reply.text)
}

/// Runs one A2A exchange: delegates the request→response envelope
/// transformation to `participant`, then mirrors the completed call into the
/// `BATON_EVENT_LOG` side trail.
///
/// Always returns an envelope — a provider failure is a *delivered* `error`
/// response, not a propagated error (the delivered-error contract; only a
/// malformed request or a usage/IO error, handled before this is called, exits
/// non-zero). The envelope shape (addressing swap, `in_reply_to`, nested
/// `baton.exchange/v1` record) is entirely [`Participant::respond`]'s
/// responsibility — this function owns only the side-trail wiring.
///
/// The `request` event is emitted *before* the call so the trail records the
/// attempt even if the provider hangs or the process dies mid-exchange (the
/// forensic value of `BATON_EVENT_LOG`). The terminal outcome event is then
/// *derived* from the response's nested record — one timing, shared by the trail
/// and the in-band record, so the two never diverge. A participant that ran no
/// call (nests no record) emits no outcome line.
///
/// Parameterised over [`Participant`]/[`EventSink`] so it is unit-testable with
/// fakes.
fn execute_exchange(
    participant: &impl Participant,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    request: &MessageEnvelope,
) -> MessageEnvelope {
    emit(sink, &ExchangeEvent::request(now_ms(), meta, &request.body));
    let response = participant.respond(request);
    if let Some(wrapped) = &response.exchange {
        emit(
            sink,
            &ExchangeEvent::from_outcome(&wrapped.exchange.outcome),
        );
    }
    response
}

/// Opens the exchange request source: `--in <path>` when given, else stdin.
fn open_input(path: Option<&str>) -> Result<Box<dyn Read>> {
    match path {
        Some(path) => {
            let file = File::open(path).map_err(|err| {
                BatonError::Io(format!("failed to open --in file {path:?}: {err}"))
            })?;
            Ok(Box::new(file))
        }
        None => Ok(Box::new(io::stdin())),
    }
}

/// Opens the exchange response sink: `--out <path>` when given (created,
/// truncated), else stdout.
fn open_output(path: Option<&str>) -> Result<Box<dyn Write>> {
    match path {
        Some(path) => {
            let file = File::create(path).map_err(|err| {
                BatonError::Io(format!("failed to create --out file {path:?}: {err}"))
            })?;
            Ok(Box::new(file))
        }
        None => Ok(Box::new(io::stdout())),
    }
}

/// Reads one `baton.message/v1` request envelope from `input`.
///
/// The whole source is parsed as a single JSON object (not line-oriented like
/// `session`). A read or JSON-parse failure is a usage error, so the caller
/// writes nothing to the response sink and exits non-zero — the
/// malformed-request contract.
fn read_request_envelope(input: impl Read) -> Result<MessageEnvelope> {
    serde_json::from_reader(input)
        .map_err(|err| usage(&format!("could not parse request envelope: {err}")))
}

/// Writes `envelope` as one JSON line to `output`.
fn write_response_envelope(envelope: &MessageEnvelope, mut output: impl Write) -> Result<()> {
    let line = serde_json::to_string(envelope)
        .map_err(|err| BatonError::Io(format!("could not serialize response envelope: {err}")))?;
    writeln!(output, "{line}").map_err(io_err)
}

/// Resolves the `converse` seed body: inline text as-is, or the content of the
/// named file. A blank inline seed or an unreadable file is a usage/IO error.
fn resolve_seed(seed: &SeedSource) -> Result<String> {
    match seed {
        SeedSource::Text(text) => {
            if text.trim().is_empty() {
                return Err(usage("--seed must not be empty"));
            }
            Ok(text.clone())
        }
        SeedSource::File(path) => std::fs::read_to_string(path)
            .map_err(|err| BatonError::Io(format!("failed to read --seed-file {path:?}: {err}"))),
    }
}

/// Builds one side's config: the base environment config with an optional
/// system-prompt file and model laid over the top. The credential and base URL
/// stay shared, so the two sides differ only in identity and model — the point
/// of the per-side flags.
fn participant_config(
    base: &BatonConfig,
    system_path: Option<&str>,
    model: Option<String>,
) -> Result<BatonConfig> {
    let mut config = base.clone();
    if let Some(path) = system_path {
        let prompt = std::fs::read_to_string(path).map_err(|err| {
            BatonError::Config(format!("failed to read system-prompt file {path:?}: {err}"))
        })?;
        config.system_prompt = Some(prompt);
    }
    if let Some(model) = model {
        config.model = model;
    }
    Ok(config)
}

/// Builds the seed request envelope: participant A's opening message addressed
/// to B. Ids are derived from the emission time so a run needs no external id
/// source; `baton.message/v1` places no format constraint on them beyond
/// uniqueness.
fn build_seed_envelope(body: &str) -> MessageEnvelope {
    let ts_ms = now_ms();
    let conversation_id = format!("conv-{ts_ms}");
    let message_id = format!("{conversation_id}-m0");
    MessageEnvelope::new(
        message_id,
        conversation_id,
        "agent-a",
        "agent-b",
        MessageKind::Request,
        body,
        ts_ms,
    )
}

/// Writes the conversation trail as JSONL — one `baton.message/v1` envelope per
/// line, in turn order (seed first, then each reply).
fn write_transcript(transcript: &Transcript, mut output: impl Write) -> Result<()> {
    for envelope in &transcript.trail {
        let line = serde_json::to_string(envelope)
            .map_err(|err| BatonError::Io(format!("could not serialize trail envelope: {err}")))?;
        writeln!(output, "{line}").map_err(io_err)?;
    }
    Ok(())
}

/// Drives an interactive multi-turn REPL over `input`/`output`.
///
/// Each line read from `input` becomes a user turn appended to the in-memory
/// [`Conversation`]; the full accumulated history is sent on every request, so
/// turn N carries all prior user and assistant turns. The assistant reply is
/// printed to `output` (and appended as the next turn). Blank lines are ignored;
/// EOF or a lone [`SESSION_EXIT_COMMAND`] line ends the loop cleanly (the caller
/// returns exit code 0).
///
/// `output` carries only assistant replies — one per turn — so it stays useful
/// to a programmatic consumer; the greeting banner and any error go to stderr.
///
/// A turn that fails at the transport layer is **not** fatal: the error is
/// reported on stderr and the loop continues. The failed user turn is rolled
/// back out of the history so it never produces two consecutive same-role turns,
/// which the Messages API rejects. Each turn still emits a `request` plus one
/// `response_ok`/`response_error` event, exactly like `ask`.
///
/// Parameterised over [`BufRead`]/[`Write`] so the whole loop — history
/// accumulation, exit conditions, and error rollback — is unit-testable with
/// in-memory buffers and a fake transport, without a terminal or a network.
fn execute_session(
    transport: &impl Transport,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    input: impl BufRead,
    mut output: impl Write,
) -> Result<()> {
    eprintln!(
        "baton session — type a message and press enter; Ctrl-D or {SESSION_EXIT_COMMAND} to quit"
    );

    let mut conversation = Conversation::new();
    for line in input.lines() {
        let line = line.map_err(io_err)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == SESSION_EXIT_COMMAND {
            break;
        }

        conversation.push_user(line.as_str());
        let result = timed_exchange(sink, meta, &line, || {
            transport.send_conversation(conversation.messages())
        });

        match result {
            Ok(reply) => {
                writeln!(output, "{}", reply.text).map_err(io_err)?;
                conversation.push_assistant(reply.text);
            }
            Err(err) => {
                // Roll the failed user turn back out so the next request does
                // not send two consecutive user turns. The loop continues —
                // a transient failure should not end an interactive session.
                conversation.pop();
                eprintln!("error: {err}");
            }
        }
    }

    Ok(())
}

/// Records the request event, times `call`, records the matching outcome event,
/// and returns the call's result.
///
/// Emits the `request` → `response_ok`/`response_error` event pair for the
/// `ask` and session paths, whose orchestration lives here. (`baton exchange`
/// does not route through this: it delegates the call to a [`Participant`] and
/// wires its own trail in [`execute_exchange`].) `event_prompt` is the user text
/// recorded on the `request` event (the turn's input). A failed event write is
/// downgraded to a stderr warning and never changes the exchange result.
fn timed_exchange(
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    event_prompt: &str,
    call: impl FnOnce() -> Result<AssistantReply>,
) -> Result<AssistantReply> {
    emit(sink, &ExchangeEvent::request(now_ms(), meta, event_prompt));

    let start = Instant::now();
    let result = call();
    let duration_ms = start.elapsed().as_millis() as u64;

    let event = match &result {
        Ok(reply) => ExchangeEvent::response_ok(now_ms(), duration_ms, &reply.text, &reply.usage),
        Err(err) => ExchangeEvent::response_error(now_ms(), duration_ms, err),
    };
    emit(sink, &event);

    result
}

/// Wraps a local I/O failure (reading stdin or writing stdout in the REPL) as a
/// [`BatonError::Io`].
fn io_err(err: io::Error) -> BatonError {
    BatonError::Io(err.to_string())
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
        "session" => parse_session(iter),
        "exchange" => parse_exchange(iter),
        "converse" => parse_converse(iter),
        "log" => parse_log(iter),
        other => Err(usage(&format!("unknown command {other:?}"))),
    }
}

/// Parses the arguments following the `log` command.
///
/// Requires a `show` or `replay` subcommand; anything else (including a missing
/// subcommand) is a usage error.
fn parse_log<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mode = iter
        .next()
        .ok_or_else(|| usage("log requires a subcommand: show or replay"))?;
    match mode.as_str() {
        "show" => {
            let file = parse_log_options(iter, false)?.file;
            Ok(Command::LogShow { file })
        }
        "replay" => {
            let opts = parse_log_options(iter, true)?;
            Ok(Command::LogReplay {
                file: opts.file,
                index: opts.index,
            })
        }
        other => Err(usage(&format!("unknown log subcommand {other:?}"))),
    }
}

/// Parsed options shared by `log show` / `log replay`.
struct LogOptions {
    file: Option<String>,
    index: Option<usize>,
}

/// Parses `--file <path>` (both subcommands) and, when `allow_index` is set,
/// `--index <N>` (replay only). The `--flag=value` form is accepted for both.
///
/// `--index` on `show`, an unknown flag, or a non-positive-integer index are all
/// usage errors.
fn parse_log_options<'a>(
    mut iter: impl Iterator<Item = &'a String>,
    allow_index: bool,
) -> Result<LogOptions> {
    let mut file: Option<String> = None;
    let mut index: Option<usize> = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--file" => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage("--file requires a value"))?;
                file = Some(value.clone());
            }
            other if other.starts_with("--file=") => {
                file = Some(other["--file=".len()..].to_string());
            }
            "--index" if allow_index => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage("--index requires a value"))?;
                index = Some(parse_index(value)?);
            }
            other if allow_index && other.starts_with("--index=") => {
                index = Some(parse_index(&other["--index=".len()..])?);
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    Ok(LogOptions { file, index })
}

/// Parses a 1-based `--index` value: a positive integer. Zero and non-numeric
/// values are usage errors (the range itself is validated against the log later).
fn parse_index(raw: &str) -> Result<usize> {
    let parsed = raw
        .parse::<usize>()
        .map_err(|_| usage(&format!("--index must be a positive integer, got {raw:?}")))?;
    if parsed == 0 {
        return Err(usage("--index is 1-based; 0 is not a valid exchange"));
    }
    Ok(parsed)
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

/// Parses the arguments following the `session` subcommand.
///
/// `session` takes no arguments; any trailing token is a usage error.
fn parse_session<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    if let Some(arg) = iter.next() {
        return Err(usage(&format!("unexpected argument {arg:?}")));
    }
    Ok(Command::Session)
}

/// Parses the arguments following the `exchange` subcommand.
///
/// Accepts optional `--in <path>` / `--out <path>` (and the `--flag=value`
/// form); with neither, the request is read from stdin and the response written
/// to stdout. A flag without a value, or any other token, is a usage error.
fn parse_exchange<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut in_path: Option<String> = None;
    let mut out_path: Option<String> = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--in" => {
                let value = iter.next().ok_or_else(|| usage("--in requires a value"))?;
                in_path = Some(value.clone());
            }
            other if other.starts_with("--in=") => {
                in_path = Some(other["--in=".len()..].to_string());
            }
            "--out" => {
                let value = iter.next().ok_or_else(|| usage("--out requires a value"))?;
                out_path = Some(value.clone());
            }
            other if other.starts_with("--out=") => {
                out_path = Some(other["--out=".len()..].to_string());
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    Ok(Command::Exchange { in_path, out_path })
}

/// Parses the arguments following the `converse` subcommand.
///
/// Accepts optional `--a-system`/`--b-system` (per-side system-prompt files),
/// `--a-model`/`--b-model` (per-side model overrides), `--out`, and the seed —
/// exactly one of `--seed <text>` or `--seed-file <path>` is required. Every
/// flag also accepts the `--flag=value` form. A flag without a value, both seed
/// forms together, a missing seed, or any other token is a usage error.
fn parse_converse<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut a_system: Option<String> = None;
    let mut b_system: Option<String> = None;
    let mut a_model: Option<String> = None;
    let mut b_model: Option<String> = None;
    let mut out_path: Option<String> = None;
    let mut seed_text: Option<String> = None;
    let mut seed_file: Option<String> = None;

    while let Some(arg) = iter.next() {
        let mut take = |flag: &str| -> Result<String> {
            iter.next()
                .cloned()
                .ok_or_else(|| usage(&format!("{flag} requires a value")))
        };
        match arg.as_str() {
            "--a-system" => a_system = Some(take("--a-system")?),
            other if other.starts_with("--a-system=") => {
                a_system = Some(other["--a-system=".len()..].to_string());
            }
            "--b-system" => b_system = Some(take("--b-system")?),
            other if other.starts_with("--b-system=") => {
                b_system = Some(other["--b-system=".len()..].to_string());
            }
            "--a-model" => a_model = Some(take("--a-model")?),
            other if other.starts_with("--a-model=") => {
                a_model = Some(other["--a-model=".len()..].to_string());
            }
            "--b-model" => b_model = Some(take("--b-model")?),
            other if other.starts_with("--b-model=") => {
                b_model = Some(other["--b-model=".len()..].to_string());
            }
            "--seed" => seed_text = Some(take("--seed")?),
            other if other.starts_with("--seed=") => {
                seed_text = Some(other["--seed=".len()..].to_string());
            }
            "--seed-file" => seed_file = Some(take("--seed-file")?),
            other if other.starts_with("--seed-file=") => {
                seed_file = Some(other["--seed-file=".len()..].to_string());
            }
            "--out" => out_path = Some(take("--out")?),
            other if other.starts_with("--out=") => {
                out_path = Some(other["--out=".len()..].to_string());
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    let seed = match (seed_text, seed_file) {
        (Some(_), Some(_)) => {
            return Err(usage("--seed and --seed-file are mutually exclusive"));
        }
        (Some(text), None) => SeedSource::Text(text),
        (None, Some(path)) => SeedSource::File(path),
        (None, None) => {
            return Err(usage(
                "missing seed: pass --seed <text> or --seed-file <path>",
            ));
        }
    };

    Ok(Command::Converse {
        a_system,
        b_system,
        a_model,
        b_model,
        seed,
        out_path,
    })
}

/// Builds a usage error carrying `detail` and the one-line usage summary.
fn usage(detail: &str) -> BatonError {
    BatonError::Usage(format!("{detail}\n{USAGE}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageKind;
    use crate::model::Message;
    use std::cell::RefCell;
    use std::io::Cursor;

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

    /// A transport that returns a canned reply and records the conversation it
    /// last saw (the single-turn `ask` path sends a one-message conversation).
    struct OkTransport {
        text: String,
        seen: RefCell<Vec<Message>>,
    }

    impl OkTransport {
        fn new(text: &str) -> Self {
            Self {
                text: text.to_string(),
                seen: RefCell::new(Vec::new()),
            }
        }
    }

    impl Transport for OkTransport {
        fn send_conversation(&self, messages: &[Message]) -> Result<AssistantReply> {
            *self.seen.borrow_mut() = messages.to_vec();
            Ok(AssistantReply::new(self.text.clone()))
        }
    }

    /// A transport that always fails at the transport layer.
    struct ErrTransport;

    impl Transport for ErrTransport {
        fn send_conversation(&self, _messages: &[Message]) -> Result<AssistantReply> {
            Err(BatonError::Transport("network down".to_string()))
        }
    }

    /// A transport that records every conversation it is sent and returns a
    /// distinct reply per call (`reply1`, `reply2`, …), so a session test can
    /// assert both the accumulated history and the per-turn replies.
    struct RecordingTransport {
        calls: RefCell<Vec<Vec<Message>>>,
    }

    impl RecordingTransport {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl Transport for RecordingTransport {
        fn send_conversation(&self, messages: &[Message]) -> Result<AssistantReply> {
            let mut calls = self.calls.borrow_mut();
            calls.push(messages.to_vec());
            Ok(AssistantReply::new(format!("reply{}", calls.len())))
        }
    }

    /// A transport whose first call fails and whose later calls succeed, to
    /// prove a failed turn is rolled back and the session keeps going.
    struct FailFirstTransport {
        calls: RefCell<Vec<Vec<Message>>>,
    }

    impl FailFirstTransport {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl Transport for FailFirstTransport {
        fn send_conversation(&self, messages: &[Message]) -> Result<AssistantReply> {
            let mut calls = self.calls.borrow_mut();
            calls.push(messages.to_vec());
            if calls.len() == 1 {
                Err(BatonError::Transport("first turn failed".to_string()))
            } else {
                Ok(AssistantReply::new(format!("reply{}", calls.len())))
            }
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
        let transport = OkTransport::new("the answer");
        let mut sink = NoopSink;
        let out = execute_ask(&transport, &mut sink, &test_meta(), "the question")
            .expect("should succeed");
        assert_eq!(out, "the answer");
        // `ask` sends a one-message user conversation carrying the prompt.
        assert_eq!(
            transport.seen.borrow().as_slice(),
            &[Message::user("the question")]
        );
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
        let transport = OkTransport::new("the answer");
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
    fn execute_ask_records_token_usage_from_reply() {
        use crate::model::TokenUsage;

        /// A transport that returns a reply carrying token usage.
        struct UsageTransport;
        impl Transport for UsageTransport {
            fn send_conversation(&self, _messages: &[Message]) -> Result<AssistantReply> {
                Ok(AssistantReply::with_usage(
                    "hi",
                    TokenUsage {
                        input_tokens: Some(12),
                        output_tokens: Some(34),
                    },
                ))
            }
        }

        let mut sink = RecordingSink::new();
        execute_ask(&UsageTransport, &mut sink, &test_meta(), "q").expect("should succeed");
        match &sink.events[1] {
            ExchangeEvent::ResponseOk {
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, Some(12));
                assert_eq!(*output_tokens, Some(34));
            }
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
        let transport = OkTransport::new("the answer");
        let mut sink = FailingSink;
        // A sink that fails on every write must not change the exchange result.
        let out = execute_ask(&transport, &mut sink, &test_meta(), "the question")
            .expect("recording failure must not abort the exchange");
        assert_eq!(out, "the answer");
    }

    #[test]
    fn parses_log_show_with_and_without_file() {
        assert_eq!(
            parse_args(&argv(&["log", "show"])).expect("parses"),
            Command::LogShow { file: None }
        );
        assert_eq!(
            parse_args(&argv(&["log", "show", "--file", "/tmp/x.jsonl"])).expect("parses"),
            Command::LogShow {
                file: Some("/tmp/x.jsonl".to_string())
            }
        );
        assert_eq!(
            parse_args(&argv(&["log", "show", "--file=/tmp/y.jsonl"])).expect("parses"),
            Command::LogShow {
                file: Some("/tmp/y.jsonl".to_string())
            }
        );
    }

    #[test]
    fn parses_log_replay_with_index() {
        assert_eq!(
            parse_args(&argv(&["log", "replay"])).expect("parses"),
            Command::LogReplay {
                file: None,
                index: None
            }
        );
        assert_eq!(
            parse_args(&argv(&[
                "log", "replay", "--index", "3", "--file", "/tmp/x"
            ]))
            .expect("parses"),
            Command::LogReplay {
                file: Some("/tmp/x".to_string()),
                index: Some(3),
            }
        );
        assert_eq!(
            parse_args(&argv(&["log", "replay", "--index=2"])).expect("parses"),
            Command::LogReplay {
                file: None,
                index: Some(2),
            }
        );
    }

    #[test]
    fn log_without_subcommand_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn unknown_log_subcommand_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log", "diff"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn index_flag_on_show_is_usage_error() {
        // `--index` is replay-only; on show it is an unexpected argument.
        assert!(matches!(
            parse_args(&argv(&["log", "show", "--index", "1"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn non_positive_or_non_numeric_index_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log", "replay", "--index", "0"])).unwrap_err(),
            BatonError::Usage(_)
        ));
        assert!(matches!(
            parse_args(&argv(&["log", "replay", "--index", "two"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    /// Builds a minimal Ok exchange carrying `prompt`, for selection tests.
    fn ok_exchange(prompt: &str) -> Exchange {
        use crate::log::{Outcome, RequestRecord};
        Exchange {
            request: RequestRecord {
                ts_ms: 0,
                model: "m".to_string(),
                base_url: "u".to_string(),
                prompt: prompt.to_string(),
            },
            outcome: Outcome::Ok {
                ts_ms: 0,
                duration_ms: 1,
                reply: "r".to_string(),
                input_tokens: None,
                output_tokens: None,
            },
        }
    }

    #[test]
    fn select_exchange_defaults_to_last() {
        let exchanges = vec![ok_exchange("first"), ok_exchange("second")];
        let selected = select_exchange(&exchanges, None).expect("selects");
        assert_eq!(selected.request.prompt, "second");
    }

    #[test]
    fn select_exchange_is_one_based() {
        let exchanges = vec![ok_exchange("first"), ok_exchange("second")];
        assert_eq!(
            select_exchange(&exchanges, Some(1))
                .expect("selects")
                .request
                .prompt,
            "first"
        );
        assert_eq!(
            select_exchange(&exchanges, Some(2))
                .expect("selects")
                .request
                .prompt,
            "second"
        );
    }

    #[test]
    fn select_exchange_out_of_range_names_the_valid_range() {
        let exchanges = vec![ok_exchange("only")];
        match select_exchange(&exchanges, Some(2)).unwrap_err() {
            BatonError::Usage(msg) => assert!(msg.contains("1..=1"), "got: {msg}"),
            other => panic!("expected Usage, got {other:?}"),
        }
    }

    #[test]
    fn select_exchange_on_empty_log_errors() {
        assert!(matches!(
            select_exchange(&[], None).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn execute_log_show_writes_a_block_per_exchange() {
        let exchanges = vec![ok_exchange("first"), ok_exchange("second")];
        let mut out: Vec<u8> = Vec::new();
        execute_log_show(&exchanges, &mut out).expect("renders");
        let text = String::from_utf8(out).expect("utf8");
        assert!(text.contains("#1") && text.contains("first"));
        assert!(text.contains("#2") && text.contains("second"));
    }

    #[test]
    fn execute_log_show_on_empty_log_writes_nothing() {
        let mut out: Vec<u8> = Vec::new();
        execute_log_show(&[], &mut out).expect("renders");
        assert!(out.is_empty());
    }

    #[test]
    fn parses_session_command() {
        assert_eq!(
            parse_args(&argv(&["session"])).expect("should parse"),
            Command::Session
        );
    }

    #[test]
    fn session_with_extra_argument_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["session", "extra"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn session_accumulates_history_across_turns_and_exits_on_eof() {
        let transport = RecordingTransport::new();
        let mut sink = RecordingSink::new();
        let input = Cursor::new("hello\nhow are you\n");
        let mut output: Vec<u8> = Vec::new();

        execute_session(&transport, &mut sink, &test_meta(), input, &mut output)
            .expect("EOF must exit cleanly");

        let calls = transport.calls.borrow();
        assert_eq!(calls.len(), 2, "one request per non-blank line");
        // Turn 1 sends only the first user turn.
        assert_eq!(calls[0], vec![Message::user("hello")]);
        // Turn 2 carries all prior turns: user, the turn-1 reply, then the new user.
        assert_eq!(
            calls[1],
            vec![
                Message::user("hello"),
                Message::assistant("reply1"),
                Message::user("how are you"),
            ]
        );

        // Each turn emits a request + response_ok pair.
        assert_eq!(sink.events.len(), 4, "two turns × (request + outcome)");
        assert!(matches!(sink.events[0], ExchangeEvent::Request { .. }));
        assert!(matches!(sink.events[1], ExchangeEvent::ResponseOk { .. }));
        assert!(matches!(sink.events[2], ExchangeEvent::Request { .. }));
        assert!(matches!(sink.events[3], ExchangeEvent::ResponseOk { .. }));

        // Both replies are printed to the REPL output.
        let printed = String::from_utf8(output).expect("utf8 output");
        assert!(printed.contains("reply1"), "got: {printed}");
        assert!(printed.contains("reply2"), "got: {printed}");
    }

    #[test]
    fn session_exit_command_stops_before_consuming_later_input() {
        let transport = RecordingTransport::new();
        let mut sink = NoopSink;
        let input = Cursor::new("hi\n/exit\nnever sent\n");
        let mut output: Vec<u8> = Vec::new();

        execute_session(&transport, &mut sink, &test_meta(), input, &mut output)
            .expect("/exit must exit cleanly");

        let calls = transport.calls.borrow();
        assert_eq!(calls.len(), 1, "only the line before /exit is sent");
        assert_eq!(calls[0], vec![Message::user("hi")]);
    }

    #[test]
    fn session_skips_blank_lines() {
        let transport = RecordingTransport::new();
        let mut sink = NoopSink;
        let input = Cursor::new("\n   \nhi\n");
        let mut output: Vec<u8> = Vec::new();

        execute_session(&transport, &mut sink, &test_meta(), input, &mut output)
            .expect("blank-only input still exits cleanly");

        let calls = transport.calls.borrow();
        assert_eq!(calls.len(), 1, "blank lines never produce a request");
        assert_eq!(calls[0], vec![Message::user("hi")]);
    }

    #[test]
    fn session_rolls_back_failed_turn_and_continues() {
        let transport = FailFirstTransport::new();
        let mut sink = RecordingSink::new();
        let input = Cursor::new("first\nsecond\n");
        let mut output: Vec<u8> = Vec::new();

        execute_session(&transport, &mut sink, &test_meta(), input, &mut output)
            .expect("a turn error must not be fatal");

        let calls = transport.calls.borrow();
        assert_eq!(calls.len(), 2, "both turns are attempted");
        assert_eq!(calls[0], vec![Message::user("first")]);
        // The failed "first" user turn was rolled back, so the second request
        // is a clean single user turn — never two consecutive user turns.
        assert_eq!(calls[1], vec![Message::user("second")]);

        // Turn 1 records an error outcome; turn 2 records success.
        assert_eq!(sink.events.len(), 4);
        assert!(matches!(
            sink.events[1],
            ExchangeEvent::ResponseError { .. }
        ));
        assert!(matches!(sink.events[3], ExchangeEvent::ResponseOk { .. }));
    }

    // -- baton exchange ----------------------------------------------------

    #[test]
    fn parses_exchange_defaults_to_std_streams() {
        assert_eq!(
            parse_args(&argv(&["exchange"])).expect("parses"),
            Command::Exchange {
                in_path: None,
                out_path: None,
            }
        );
    }

    #[test]
    fn parses_exchange_with_in_and_out_paths() {
        assert_eq!(
            parse_args(&argv(&[
                "exchange",
                "--in",
                "/tmp/req.json",
                "--out",
                "/tmp/resp.json"
            ]))
            .expect("parses"),
            Command::Exchange {
                in_path: Some("/tmp/req.json".to_string()),
                out_path: Some("/tmp/resp.json".to_string()),
            }
        );
        assert_eq!(
            parse_args(&argv(&["exchange", "--in=/tmp/a", "--out=/tmp/b"])).expect("parses"),
            Command::Exchange {
                in_path: Some("/tmp/a".to_string()),
                out_path: Some("/tmp/b".to_string()),
            }
        );
    }

    #[test]
    fn exchange_flag_without_value_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["exchange", "--in"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn exchange_unexpected_argument_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["exchange", "--who"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    // -- baton converse ----------------------------------------------------

    #[test]
    fn parses_converse_with_inline_seed_and_defaults() {
        assert_eq!(
            parse_args(&argv(&["converse", "--seed", "hello"])).expect("parses"),
            Command::Converse {
                a_system: None,
                b_system: None,
                a_model: None,
                b_model: None,
                seed: SeedSource::Text("hello".to_string()),
                out_path: None,
            }
        );
    }

    #[test]
    fn parses_converse_with_all_flags_and_equals_forms() {
        assert_eq!(
            parse_args(&argv(&[
                "converse",
                "--a-system=/tmp/a.txt",
                "--b-system",
                "/tmp/b.txt",
                "--a-model=model-a",
                "--b-model",
                "model-b",
                "--seed-file=/tmp/seed.txt",
                "--out",
                "/tmp/trail.jsonl",
            ]))
            .expect("parses"),
            Command::Converse {
                a_system: Some("/tmp/a.txt".to_string()),
                b_system: Some("/tmp/b.txt".to_string()),
                a_model: Some("model-a".to_string()),
                b_model: Some("model-b".to_string()),
                seed: SeedSource::File("/tmp/seed.txt".to_string()),
                out_path: Some("/tmp/trail.jsonl".to_string()),
            }
        );
    }

    #[test]
    fn converse_missing_seed_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["converse", "--a-model", "m"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn converse_both_seed_forms_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&[
                "converse",
                "--seed",
                "hi",
                "--seed-file",
                "/tmp/s"
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn converse_flag_without_value_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["converse", "--seed"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn converse_unexpected_argument_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["converse", "--who"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn resolve_seed_rejects_blank_inline_and_reads_file() {
        assert!(matches!(
            resolve_seed(&SeedSource::Text("   ".to_string())).unwrap_err(),
            BatonError::Usage(_)
        ));
        assert_eq!(
            resolve_seed(&SeedSource::Text("go".to_string())).expect("ok"),
            "go"
        );
    }

    #[test]
    fn build_seed_envelope_is_an_a_to_b_request() {
        let seed = build_seed_envelope("kick off");
        assert_eq!(seed.kind, MessageKind::Request);
        assert_eq!(seed.from, "agent-a");
        assert_eq!(seed.to, "agent-b");
        assert_eq!(seed.body, "kick off");
        assert!(seed.in_reply_to.is_none());
    }

    /// Builds a `request`-kind envelope for the exchange tests.
    fn request_envelope() -> MessageEnvelope {
        MessageEnvelope::new(
            "m-req-1",
            "conv-42",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "what is 2+2?",
            1_700_000_000_000,
        )
    }

    /// Wraps a transport as the in-process participant `baton exchange` uses,
    /// so the wiring tests exercise the same delegation as production.
    fn participant_over(transport: impl Transport) -> LocalParticipant<impl Transport> {
        LocalParticipant::new(transport, test_meta())
    }

    #[test]
    fn execute_exchange_returns_the_participants_response() {
        let mut sink = NoopSink;
        let request = request_envelope();

        // execute_exchange owns only the trail wiring; the envelope it returns is
        // the participant's, unchanged. (The transformation itself is covered in
        // `participant`'s own tests.)
        let response = execute_exchange(
            &participant_over(OkTransport::new("four")),
            &mut sink,
            &test_meta(),
            &request,
        );

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "four");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        assert!(response.exchange.is_some(), "provider call nested in-band");
    }

    #[test]
    fn execute_exchange_records_request_then_ok_outcome_pair() {
        let mut sink = RecordingSink::new();
        execute_exchange(
            &participant_over(OkTransport::new("four")),
            &mut sink,
            &test_meta(),
            &request_envelope(),
        );

        // The `request` line is emitted before the call; the outcome line is
        // derived from the response's nested record.
        assert_eq!(sink.events.len(), 2, "request + outcome");
        assert!(matches!(sink.events[0], ExchangeEvent::Request { .. }));
        assert!(matches!(sink.events[1], ExchangeEvent::ResponseOk { .. }));
    }

    #[test]
    fn execute_exchange_records_request_then_error_outcome_pair() {
        let mut sink = RecordingSink::new();
        // A delivered-error envelope still yields a well-formed request → error
        // trail pair, derived from the nested failure record.
        let response = execute_exchange(
            &participant_over(ErrTransport),
            &mut sink,
            &test_meta(),
            &request_envelope(),
        );

        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(sink.events.len(), 2, "request + outcome");
        assert!(matches!(sink.events[0], ExchangeEvent::Request { .. }));
        match &sink.events[1] {
            ExchangeEvent::ResponseError { kind, .. } => assert_eq!(kind, "transport"),
            other => panic!("expected ResponseError, got {other:?}"),
        }
    }

    #[test]
    fn read_request_envelope_rejects_malformed_json() {
        let malformed = b"not a json envelope".as_slice();
        assert!(matches!(
            read_request_envelope(malformed).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn write_then_read_response_envelope_round_trips() {
        let mut sink = NoopSink;
        let response = execute_exchange(
            &participant_over(OkTransport::new("four")),
            &mut sink,
            &test_meta(),
            &request_envelope(),
        );

        let mut buf: Vec<u8> = Vec::new();
        write_response_envelope(&response, &mut buf).expect("writes");
        // Exactly one JSON line.
        let text = String::from_utf8(buf).expect("utf8");
        assert_eq!(text.lines().count(), 1, "one envelope, one line");

        let back = read_request_envelope(text.as_bytes()).expect("parses back");
        assert_eq!(back, response);
    }
}
