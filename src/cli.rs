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
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::config::BatonConfig;
use crate::converse::{self, Governance, RingMember, Transcript};
use crate::error::{BatonError, Result};
use crate::events::{EventSink, ExchangeEvent, ExchangeMeta, NoopSink, WriterSink, now_ms};
use crate::log::{self, Exchange};
use crate::mailbox::{self, Mailbox, MailboxState, MailboxStatus};
use crate::message::{MessageEnvelope, MessageKind};
use crate::model::{AssistantReply, Conversation, Prompt};
use crate::participant::{
    ExternalAgentParticipant, LocalParticipant, MailboxParticipant, OutputAdapter, Participant,
};
use crate::registry::Registry;
use crate::transport::Transport;
use crate::transport::claude::ClaudeClient;

/// Environment variable naming the JSONL event-log file. Unset or blank ⇒
/// recording is disabled.
pub const EVENT_LOG_ENV: &str = "BATON_EVENT_LOG";

/// One-line usage summary, appended to argument errors.
pub const USAGE: &str = "usage: baton ask -p|--prompt <text> | baton session [--resume <file> [--session <id>]] | baton exchange [--in <path>] [--out <path>] | baton converse [--a-system <path>] [--b-system <path>] [--a-model <id>] [--b-model <id>] [--b-mailbox --b-inbox <dir> --b-outbox <dir> [--b-await-ms <n>]] (--seed <text> | --seed-file <path>) [--out <path>] | baton converse-ring --registry <path> --roster <a,b,c> (--seed <text> | --seed-file <path>) [--await-ms <n>] [--out <path>] | baton serve --inbox <dir> --outbox <dir> [--poll-ms <n>] [--once] [--agent-cmd <program> [--agent-arg <arg>]... [--agent-cwd <dir>] [--agent-timeout-ms <n>] [--agent-output raw|json [--agent-result-key <key>]] [--agent-system <path>] [--agent-mcp-config <path>]] | baton serve --stop --inbox <dir> | baton send (--inbox <dir> | --registry <path>) (--body <text> [--to <role>] | --in <path>) [--from <id>] [--conversation <id>] [--await [--outbox <dir>] [--timeout-ms <n>]] | baton status (--mailbox <root> | --registry <path> --role <role>) [--max-runtime-ms <n>] | baton log show|replay [--file <path>] [--index <N>] | baton log merge --conversation <id> <trail>...";

/// Default `baton serve` inbox poll interval, in milliseconds, when `--poll-ms`
/// is unset.
const DEFAULT_SERVE_POLL_MS: u64 = 500;

/// Default `baton serve --agent-cmd` read timeout for one headless agent run, in
/// milliseconds, when `--agent-timeout-ms` is unset. Very generous: a full-tooled
/// agent run is many tool calls (git, edits, MCP), not one provider turn, so a
/// short deadline would kill a live-but-working agent mid-task.
const DEFAULT_AGENT_TIMEOUT_MS: u64 = 600_000;

/// Default `baton send --await` timeout, in milliseconds, when `--timeout-ms` is
/// unset.
const DEFAULT_SEND_TIMEOUT_MS: u64 = 30_000;

/// Default `baton converse --b-mailbox` await timeout, in milliseconds, when
/// `--b-await-ms` is unset. Generous relative to [`DEFAULT_SEND_TIMEOUT_MS`]:
/// each mailbox-backed turn is a full provider turn run by the peer `serve`
/// daemon, so a short deadline would synthesize a timeout mid-answer.
const DEFAULT_CONVERSE_AWAIT_MS: u64 = 60_000;

/// Interval between `baton send --await` polls of the outbox, in milliseconds.
/// Fixed (not user-tunable): the await is bounded by `--timeout-ms`, and a tight
/// interval keeps a local round-trip responsive without a flag for it.
const SEND_POLL_INTERVAL_MS: u64 = 50;

/// Default `baton status` max-runtime threshold, in milliseconds, when neither
/// `--max-runtime-ms` nor a per-role registry `max_runtime_ms` is set. A claim
/// older than this reads as `crashed-stale`. Sized above [`DEFAULT_AGENT_TIMEOUT_MS`]
/// (the serve-side agent cap) so a slow-but-alive worker is never misjudged
/// crashed; a team with longer legitimate runs raises it per role.
const DEFAULT_MAX_RUNTIME_MS: u64 = 900_000;

/// The in-session command that ends the REPL cleanly (alongside EOF).
const SESSION_EXIT_COMMAND: &str = "/exit";

/// A parsed CLI invocation.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Send a single prompt and print the assistant reply.
    Ask { prompt: String },
    /// Start an interactive multi-turn REPL. `resume` is `None` for a fresh
    /// session; `Some` rehydrates a prior session from its JSONL trail and
    /// continues appending to it (see [`ResumeArgs`]).
    Session { resume: Option<ResumeArgs> },
    /// Run one A2A envelope exchange: read a `baton.message/v1` request
    /// envelope, run the provider call, and write one response envelope.
    /// `in_path`/`out_path` default to stdin/stdout when `None`.
    Exchange {
        in_path: Option<String>,
        out_path: Option<String>,
    },
    /// Drive a governed two-participant conversation from a seed. Side A is an
    /// in-process participant configured from the environment, overridden by its
    /// optional system-prompt file and model. Side B (the first responder) is
    /// the same in-process participant *unless* `b_mailbox` selects a
    /// mailbox-backed participant — then B is a governed client of a live
    /// `baton serve` peer, delivering each request to `b_inbox` and awaiting the
    /// reply from `b_outbox` (bounded by `b_await_ms`). The full
    /// `baton.message/v1` trail is written as JSONL to `out_path` (stdout when
    /// `None`).
    Converse {
        a_system: Option<String>,
        b_system: Option<String>,
        a_model: Option<String>,
        b_model: Option<String>,
        b_mailbox: bool,
        b_inbox: Option<String>,
        b_outbox: Option<String>,
        b_await_ms: u64,
        seed: SeedSource,
        out_path: Option<String>,
    },
    /// Drive an N-party (N ≥ 2) round-robin ring whose members are all remote
    /// mailbox peers. `registry` is a JSON file mapping each participant name to
    /// its `{inbox, outbox}` pair; `roster` is the ring order (comma-separated on
    /// the command line). Every roster name is resolved against the registry at
    /// startup — an unknown name is a fail-fast error before any turn runs — and
    /// each becomes a [`MailboxParticipant`] awaiting replies for `await_ms`. The
    /// seed is addressed from `roster[0]` to `roster[1]`, so `roster[1]` answers
    /// first (see [`converse::converse_ring`]). The full trail is written as JSONL
    /// to `out_path` (stdout when `None`).
    ConverseRing {
        registry: String,
        roster: Vec<String>,
        seed: SeedSource,
        await_ms: u64,
        out_path: Option<String>,
    },
    /// Serve a file-mailbox: drain `inbox`'s `pending/` requests through the
    /// participant seam, writing each reply to `outbox`. `once` drains a single
    /// pass and exits; otherwise the inbox is polled every `poll_ms`.
    ///
    /// Without `--agent-cmd`, each reply is one in-process Messages-API call
    /// (`LocalParticipant`, requiring `BatonConfig`/an API key). With
    /// `--agent-cmd`, each reply is one **headless native-agent run**
    /// (`ExternalAgentParticipant`) in `agent_cwd`, carrying its own credentials
    /// — no `BatonConfig` is loaded and no tmux is involved.
    Serve {
        inbox: String,
        outbox: String,
        poll_ms: u64,
        once: bool,
        /// The native agent CLI to run headless per message; `None` ⇒ the
        /// in-process `LocalParticipant`.
        agent_cmd: Option<String>,
        /// Fixed arguments passed to the agent on every run (headless/role flags).
        agent_args: Vec<String>,
        /// Working directory (git worktree) for the agent; `None` ⇒ the serve
        /// process's own cwd. Only meaningful with `agent_cmd`.
        agent_cwd: Option<String>,
        /// Read timeout (ms) for one agent run; `None` ⇒ [`DEFAULT_AGENT_TIMEOUT_MS`].
        agent_timeout_ms: Option<u64>,
        /// Output adapter selector: `raw` (whole stdout) or `json` (final JSON
        /// line's result field). `None` ⇒ `raw`. Only meaningful with `agent_cmd`.
        agent_output: Option<String>,
        /// JSON result-field key for `--agent-output json`; `None` ⇒ `result`.
        agent_result_key: Option<String>,
        /// Path to a role system-prompt/identity file, injected as the reference
        /// agent's `--append-system-prompt <contents>`. Only meaningful with
        /// `agent_cmd`.
        agent_system: Option<String>,
        /// Path to an MCP config file, injected as the reference agent's
        /// `--mcp-config <path>`. Only meaningful with `agent_cmd`.
        agent_mcp_config: Option<String>,
    },
    /// Cooperatively stop a running `baton serve` on `inbox` (Option C graceful
    /// shutdown): drop a stop sentinel the daemon observes between messages, so
    /// it finishes the in-flight message and exits 0. If no daemon holds the
    /// lock, reports that nothing is running and still exits 0 (idempotent).
    ServeStop { inbox: String },
    /// Post a `baton.message/v1` request into a mailbox's `pending/` (the
    /// producer over the atomic deliver path), optionally awaiting the correlated
    /// reply from `outbox`. The message is built from `--body` (+ optional
    /// addressing) or read whole from `--in`. `await_reply` requires `outbox`.
    Send {
        /// Explicit mailbox root; `None` when the destination is resolved from
        /// `registry` + the addressee role instead.
        inbox: Option<String>,
        /// Routing registry path; when set, the addressee role (the `--body`
        /// `--to`, or the `--in` envelope's `to`) resolves the inbox/outbox.
        registry: Option<String>,
        source: SendSource,
        to: Option<String>,
        from: Option<String>,
        conversation: Option<String>,
        await_reply: bool,
        outbox: Option<String>,
        timeout_ms: u64,
    },
    /// Report a mailbox's liveness — `idle-done` / `busy` / `crashed-stale` plus
    /// queue depth — over an explicit `--mailbox <root>` or a `--registry
    /// <path> --role <role>` lookup.
    Status {
        /// Explicit mailbox root; mutually exclusive with `registry`/`role`.
        mailbox: Option<String>,
        /// Routing registry path, resolving `role` to a mailbox.
        registry: Option<String>,
        /// Role name resolved through `registry`.
        role: Option<String>,
        /// `--max-runtime-ms` override; takes precedence over the per-role
        /// registry value and the default.
        max_runtime_ms: Option<u64>,
    },
    /// Pretty-print the recorded exchange trail.
    LogShow { file: Option<String> },
    /// Re-run a recorded exchange. `index` is 1-based; `None` ⇒ the last one.
    LogReplay {
        file: Option<String>,
        index: Option<usize>,
    },
    /// Merge `baton.message/v1` envelopes sharing `conversation` across several
    /// trail files (explicit paths; a directory expands to its files) into one
    /// causal-chain–ordered view.
    LogMerge {
        paths: Vec<String>,
        conversation: String,
    },
}

/// Selects the session trail to rehydrate for `baton session --resume`.
///
/// `file` is the JSONL trail path (`--resume <file>`); `session_id` optionally
/// disambiguates when the file is a shared append log holding several sessions
/// (`--session <id>`). Resolved to a [`ResumedSession`] only in [`run`], keeping
/// [`parse_args`] free of I/O.
#[derive(Debug, PartialEq, Eq)]
struct ResumeArgs {
    /// The JSONL session trail to read the prior turns from.
    file: String,
    /// The `session_id` to select; `None` selects the sole session in the file
    /// (an error when the file holds zero or more than one).
    session_id: Option<String>,
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

/// Where a `baton send` message comes from: an inline body (from which a full
/// envelope is constructed) or a path to a complete `baton.message/v1` envelope.
/// Resolved to the envelope only in [`run`], keeping [`parse_args`] free of I/O.
#[derive(Debug, PartialEq, Eq)]
enum SendSource {
    /// `--body <text>`: construct a request envelope around this body.
    Body(String),
    /// `--in <path>`: read a complete envelope from this path at run time.
    Envelope(String),
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
        Command::Session { resume } => {
            let config = BatonConfig::from_env()?;
            let meta = exchange_meta(&config);
            let client = ClaudeClient::from_config(config);
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            match resume {
                None => {
                    let mut sink = open_event_sink()?;
                    execute_session(&client, sink.as_mut(), &meta, stdin.lock(), stdout.lock())
                }
                // Resume: load + select the prior session *before* opening any
                // sink, so a bad selection (missing id, empty/ambiguous trail)
                // exits non-zero having written nothing. The sink then appends to
                // the resumed trail itself — not `BATON_EVENT_LOG` — so new turns
                // extend the same file.
                Some(args) => {
                    let resumed = load_resume(&args.file, args.session_id.as_deref())?;
                    let mut sink = open_append_sink(&args.file)?;
                    execute_session_resumed(
                        &client,
                        sink.as_mut(),
                        &meta,
                        stdin.lock(),
                        stdout.lock(),
                        resumed,
                    )
                }
            }
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
            b_mailbox,
            b_inbox,
            b_outbox,
            b_await_ms,
            seed,
            out_path,
        } => {
            let governance = Governance::from_lookup(|key| std::env::var(key).ok())?;
            let seed_body = resolve_seed(&seed)?;

            // Side A is always the in-process, environment-configured
            // participant: the base config with A's system prompt / model laid
            // over the top. A mailbox-backed side B needs no provider config of
            // its own (the peer `serve` daemon carries that), so the base config
            // is loaded lazily — only when a side actually runs a local call.
            let build_local =
                |system: Option<&str>, model: Option<String>| -> Result<Box<dyn Participant>> {
                    let config = participant_config(&BatonConfig::from_env()?, system, model)?;
                    Ok(Box::new(LocalParticipant::new(
                        ClaudeClient::from_config(config.clone()),
                        exchange_meta(&config),
                    )))
                };

            let participant_a = build_local(a_system.as_deref(), a_model)?;
            let participant_b: Box<dyn Participant> = if b_mailbox {
                // Guaranteed `Some` by `parse_converse` whenever `--b-mailbox`.
                let inbox = b_inbox.expect("--b-mailbox requires --b-inbox");
                let outbox = b_outbox.expect("--b-mailbox requires --b-outbox");
                Box::new(MailboxParticipant::new(
                    inbox,
                    outbox,
                    Duration::from_millis(b_await_ms),
                    Duration::from_millis(SEND_POLL_INTERVAL_MS),
                ))
            } else {
                build_local(b_system.as_deref(), b_model)?
            };

            let transcript = converse::converse(
                participant_a.as_ref(),
                participant_b.as_ref(),
                build_seed_envelope(&seed_body),
                &governance,
            );
            eprintln!("conversation ended: {:?}", transcript.reason);
            write_transcript(&transcript, open_output(out_path.as_deref())?)
        }
        Command::ConverseRing {
            registry,
            roster,
            seed,
            await_ms,
            out_path,
        } => {
            let governance = Governance::from_lookup(|key| std::env::var(key).ok())?;
            let seed_body = resolve_seed(&seed)?;

            // Load the registry once at startup and resolve every roster name up
            // front, so an unroutable name fails before any turn runs. All ring
            // members are remote mailbox peers (a `baton serve` daemon each); the
            // driver holds no local participant and runs no provider call itself.
            let registry = Registry::from_path(Path::new(&registry))?;
            let await_timeout = Duration::from_millis(await_ms);
            let poll = Duration::from_millis(SEND_POLL_INTERVAL_MS);
            let participants = roster
                .iter()
                .map(|name| {
                    let mailbox = registry.resolve(name)?;
                    Ok(MailboxParticipant::new(
                        mailbox.inbox.clone(),
                        mailbox.outbox.clone(),
                        await_timeout,
                        poll,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let ring: Vec<RingMember> = roster
                .iter()
                .zip(&participants)
                .map(|(name, participant)| RingMember::new(name.clone(), participant))
                .collect();

            // Seed is addressed roster[0] -> roster[1]; guaranteed ≥2 by the parser.
            let seed_envelope = build_ring_seed_envelope(&seed_body, &roster[0], &roster[1]);
            let transcript = converse::converse_ring(&ring, seed_envelope, &governance);
            eprintln!("conversation ended: {:?}", transcript.reason);
            write_transcript(&transcript, open_output(out_path.as_deref())?)
        }
        Command::Serve {
            inbox,
            outbox,
            poll_ms,
            once,
            agent_cmd,
            agent_args,
            agent_cwd,
            agent_timeout_ms,
            agent_output,
            agent_result_key,
            agent_system,
            agent_mcp_config,
        } => {
            let mut sink = open_event_sink()?;

            // Two participant backings behind one drain loop. An external agent
            // carries its own credentials and MCP config, so agent mode loads no
            // `BatonConfig` and builds no `ClaudeClient` — the whole point of a
            // key-free, tmux-free role host. The `meta` stamped on the side-trail
            // request line is then a placeholder naming the agent, since no
            // provider model/base_url applies.
            let (participant, meta): (Box<dyn Participant>, ExchangeMeta) = match agent_cmd {
                Some(program) => {
                    let cwd = agent_cwd
                        .map(PathBuf::from)
                        .map(Ok)
                        .unwrap_or_else(std::env::current_dir)
                        .map_err(|err| {
                            BatonError::Io(format!("could not resolve the agent cwd: {err}"))
                        })?;
                    let read_timeout =
                        Duration::from_millis(agent_timeout_ms.unwrap_or(DEFAULT_AGENT_TIMEOUT_MS));
                    let output = build_output_adapter(agent_output.as_deref(), agent_result_key)?;
                    // Assemble the first-class role config into the agent arg list
                    // *before* the operator's `--agent-arg` values, mapped to the
                    // reference agent's (Claude Code) flags. The participant stays
                    // backend-neutral — it never learns a flag spelling.
                    let args = build_agent_args(
                        agent_system.as_deref(),
                        agent_mcp_config.as_deref(),
                        agent_args,
                    )?;
                    let meta = ExchangeMeta {
                        model: program.clone(),
                        base_url: "external-agent".to_string(),
                    };
                    let participant = ExternalAgentParticipant::new(
                        program,
                        args,
                        std::iter::empty::<(String, String)>(),
                        cwd,
                        output,
                        read_timeout,
                    );
                    (Box::new(participant), meta)
                }
                None => {
                    let config = BatonConfig::from_env()?;
                    let meta = exchange_meta(&config);
                    let client = ClaudeClient::from_config(config);
                    (Box::new(LocalParticipant::new(client, meta.clone())), meta)
                }
            };

            // Lock first, then reclaim: with the single-instance lock held, no
            // other daemon is mid-answer, so returning abandoned `claimed/`
            // messages to `pending/` cannot race a concurrent processor.
            let mailbox = Mailbox::open(&inbox)?;
            // Discard any stale stop sentinel so a leftover from a prior daemon
            // cannot make this fresh start exit immediately.
            mailbox.poll_stop()?;
            mailbox.reclaim_stale()?;
            let outbox = Path::new(&outbox);

            let poll = Duration::from_millis(poll_ms);
            loop {
                match drain_mailbox(&mailbox, outbox, participant.as_ref(), sink.as_mut(), &meta)? {
                    // Cooperative stop observed between messages ⇒ exit 0.
                    Drain::Stopped => break,
                    Drain::Drained(processed) => {
                        if once {
                            break;
                        }
                        // Nothing waiting — sleep before the next scan rather than spin.
                        if processed == 0 {
                            std::thread::sleep(poll);
                        }
                    }
                }
            }
            Ok(())
        }
        Command::ServeStop { inbox } => {
            match mailbox::request_stop(&inbox)? {
                mailbox::StopRequest::Signalled => {
                    println!("requested cooperative stop of baton serve on {inbox}");
                }
                mailbox::StopRequest::NoDaemon => {
                    eprintln!("no running baton serve on {inbox}; nothing to stop");
                }
            }
            Ok(())
        }
        Command::Send {
            inbox,
            registry,
            source,
            to,
            from,
            conversation,
            await_reply,
            outbox,
            timeout_ms,
        } => {
            // A producer runs no provider call, so `send` needs no credential —
            // it does not load `BatonConfig`. Only the event sink is wired.
            let mut sink = open_event_sink()?;
            let envelope = build_send_envelope(&source, to, from, conversation)?;
            // Resolve the delivery inbox and await outbox: either explicit paths,
            // or the addressee role (the envelope's `to`) looked up in the
            // registry. An unknown role fails fast via `Registry::resolve`.
            let (inbox_path, outbox_path) = match &registry {
                Some(registry_path) => {
                    let registry = Registry::from_path(Path::new(registry_path))?;
                    let mailbox_ref = registry.resolve(&envelope.to)?;
                    (mailbox_ref.inbox.clone(), Some(mailbox_ref.outbox.clone()))
                }
                None => (
                    PathBuf::from(inbox.expect("parse_send guarantees --inbox without --registry")),
                    outbox.map(PathBuf::from),
                ),
            };
            let stdout = std::io::stdout();
            execute_send(
                &inbox_path,
                outbox_path.as_deref(),
                &envelope,
                await_reply,
                Duration::from_millis(timeout_ms),
                Duration::from_millis(SEND_POLL_INTERVAL_MS),
                sink.as_mut(),
                stdout.lock(),
            )
        }
        Command::Status {
            mailbox,
            registry,
            role,
            max_runtime_ms,
        } => {
            // Resolve the mailbox root and threshold: an explicit `--mailbox`
            // uses the flag/default threshold; a `--registry --role` lookup falls
            // back to the per-role `max_runtime_ms` when no override is given.
            let (root, threshold_ms) = match (mailbox, registry, role) {
                (Some(mailbox), None, None) => (
                    PathBuf::from(mailbox),
                    max_runtime_ms.unwrap_or(DEFAULT_MAX_RUNTIME_MS),
                ),
                (None, Some(registry_path), Some(role)) => {
                    let registry = Registry::from_path(Path::new(&registry_path))?;
                    let mailbox_ref = registry.resolve(&role)?;
                    let threshold = max_runtime_ms
                        .or(mailbox_ref.max_runtime_ms)
                        .unwrap_or(DEFAULT_MAX_RUNTIME_MS);
                    (mailbox_ref.inbox.clone(), threshold)
                }
                _ => {
                    return Err(usage(
                        "status needs --mailbox <root> or --registry <path> --role <role>",
                    ));
                }
            };
            let status = mailbox::status(&root, Duration::from_millis(threshold_ms))?;
            let stdout = std::io::stdout();
            execute_status(&status, threshold_ms, stdout.lock())
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
        Command::LogMerge {
            paths,
            conversation,
        } => {
            let envelopes = read_message_trails(&paths)?;
            let merged = log::merge_conversation(envelopes, &conversation);
            let stdout = std::io::stdout();
            execute_log_merge(&merged, stdout.lock())
        }
    }
}

/// Reads every `baton.message/v1` envelope across `paths`, concatenated in the
/// order given.
///
/// Each path is a trail file, or a directory whose file entries (sorted for a
/// deterministic order) are each read as a trail — so a caller can point the
/// merge at a whole directory of trails. Non-fatal warnings from
/// [`log::parse_message_trail`] (a skipped malformed line in one trail) are
/// surfaced on stderr, keeping the merge robust to one corrupt line without
/// bricking the unified view.
fn read_message_trails(paths: &[String]) -> Result<Vec<MessageEnvelope>> {
    let mut envelopes = Vec::new();
    for path in paths {
        for file in trail_files_at(path)? {
            let handle = File::open(&file).map_err(|err| {
                BatonError::Io(format!("failed to open trail file {file:?}: {err}"))
            })?;
            let report = log::parse_message_trail(handle)?;
            for warning in &report.warnings {
                eprintln!("warning: {file:?}: {warning}");
            }
            envelopes.extend(report.envelopes);
        }
    }
    Ok(envelopes)
}

/// Resolves one merge argument into the trail files to read: a directory
/// expands to its file entries (sorted); any other path is taken verbatim.
fn trail_files_at(path: &str) -> Result<Vec<std::path::PathBuf>> {
    let meta = std::fs::metadata(path)
        .map_err(|err| BatonError::Io(format!("failed to stat {path:?}: {err}")))?;
    if !meta.is_dir() {
        return Ok(vec![std::path::PathBuf::from(path)]);
    }
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(path)
        .map_err(|err| BatonError::Io(format!("failed to read directory {path:?}: {err}")))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.is_file())
        .collect();
    files.sort();
    Ok(files)
}

/// Writes each merged message as a human-readable block to `output`.
///
/// Parameterised over [`Write`] so the rendering is unit-testable with an
/// in-memory buffer. An empty merge (no matching envelope) produces no output.
fn execute_log_merge(merged: &[MessageEnvelope], mut output: impl Write) -> Result<()> {
    for (i, envelope) in merged.iter().enumerate() {
        write!(output, "{}", log::format_message(i + 1, envelope)).map_err(io_err)?;
    }
    Ok(())
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
    timed_exchange(sink, meta, prompt, None, || {
        transport.send(&Prompt::new(prompt))
    })
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
    participant: &dyn Participant,
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

/// The result of one [`drain_mailbox`] pass.
enum Drain {
    /// Drained every currently-claimable request; carries how many were processed.
    Drained(usize),
    /// A cooperative stop sentinel was observed between messages; the caller
    /// should exit 0 without draining further.
    Stopped,
}

/// Drains every currently-claimable request from `mailbox` through one
/// participant, writing each reply to `outbox` keyed by the request id, and
/// returns how many were processed — unless a cooperative stop is observed.
///
/// The stop sentinel is checked **between messages** (at the top of each claim
/// iteration), so an in-flight `respond()` is never interrupted mid-call: a stop
/// dropped while a message is being answered is seen only after that message
/// completes to `done`, then the pass returns [`Drain::Stopped`].
///
/// Each message runs the same [`execute_exchange`] path as `baton exchange` — so
/// the response envelope and the `BATON_EVENT_LOG` trail are produced identically
/// — then advances `claimed → done`. Parameterised over [`Participant`] /
/// [`EventSink`] so it is unit-testable with fakes and a tempdir mailbox, no
/// network. A single pass: the caller decides whether to loop.
fn drain_mailbox(
    mailbox: &Mailbox,
    outbox: &Path,
    participant: &dyn Participant,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
) -> Result<Drain> {
    let mut processed = 0;
    loop {
        if mailbox.poll_stop()? {
            return Ok(Drain::Stopped);
        }
        let Some(claimed) = mailbox.claim_next()? else {
            return Ok(Drain::Drained(processed));
        };
        let response = execute_exchange(participant, sink, meta, &claimed.request);
        mailbox.deliver_response(outbox, &claimed.key, &response)?;
        mailbox.complete(claimed)?;
        processed += 1;
    }
}

/// Delivers `envelope` into `inbox`'s `pending/` (lock-free producer), records
/// the send, and — when `await_reply` — consumes the correlated reply from
/// `outbox` and writes it to `out`.
///
/// The delivery goes through [`mailbox::deliver_to`], which does **not** take the
/// single-instance lock, so it posts to a mailbox a live `baton serve` owns.
/// Without `--await`, the sent `message_id` is written to `out` (the command's
/// result) and the function returns. With `--await`, `out` instead carries the
/// reply envelope (one JSON line); the `message_id` confirmation goes to stderr
/// so a consumer piping stdout parses only the reply.
///
/// The await is bounded to this single invocation by `timeout`: on expiry the
/// request stays in the mailbox and this returns an error. A consumed reply must
/// carry `in_reply_to == message_id`; a mismatch is a hard error, not a silent
/// accept. At-least-once means a later reclaim-driven second reply reappears as a
/// fresh outbox file — a subsequent `--await` would consume it, so consumers
/// dedup on `in_reply_to` / `conversation_id`.
///
/// Parameterised over [`EventSink`] / [`Write`] so it is unit-testable with a
/// tempdir mailbox and no network. `outbox` is `Some` whenever `await_reply` is
/// set (guaranteed by [`parse_send`]).
#[allow(clippy::too_many_arguments)]
fn execute_send(
    inbox: &Path,
    outbox: Option<&Path>,
    envelope: &MessageEnvelope,
    await_reply: bool,
    timeout: Duration,
    poll_interval: Duration,
    sink: &mut dyn EventSink,
    mut out: impl Write,
) -> Result<()> {
    mailbox::deliver_to(inbox, envelope)?;
    emit(sink, &ExchangeEvent::message_sent(now_ms(), envelope));

    let Some(outbox) = outbox.filter(|_| await_reply) else {
        writeln!(out, "{}", envelope.message_id).map_err(io_err)?;
        return Ok(());
    };

    eprintln!("sent {} — awaiting reply", envelope.message_id);
    let reply = await_response(outbox, &envelope.message_id, timeout, poll_interval)?;

    // Correlation check: the consumed reply must answer *this* request.
    if reply.in_reply_to.as_deref() != Some(envelope.message_id.as_str()) {
        return Err(BatonError::Io(format!(
            "consumed reply {:?} has in_reply_to {:?}, expected {:?}",
            reply.message_id, reply.in_reply_to, envelope.message_id
        )));
    }

    emit(sink, &ExchangeEvent::reply_consumed(now_ms(), &reply));
    write_response_envelope(&reply, out)
}

/// Writes a mailbox `status` snapshot as one JSON line to `out`.
///
/// The stable machine-readable contract a gate-check parses: `state` is
/// `idle-done` / `busy` / `crashed-stale`, `queue_depth` the pending count,
/// `claim_age_ms` the oldest claim's age in ms (null when idle), and
/// `max_runtime_ms` the threshold the state was decided against. Parameterised
/// over [`Write`] so it is unit-testable with an in-memory buffer.
fn execute_status(status: &MailboxStatus, max_runtime_ms: u64, mut out: impl Write) -> Result<()> {
    let state = match status.state {
        MailboxState::IdleDone => "idle-done",
        MailboxState::Busy => "busy",
        MailboxState::CrashedStale => "crashed-stale",
    };
    let claim_age = match status.claim_age_ms {
        Some(ms) => ms.to_string(),
        None => "null".to_string(),
    };
    writeln!(
        out,
        "{{\"state\":\"{state}\",\"queue_depth\":{},\"claim_age_ms\":{claim_age},\"max_runtime_ms\":{max_runtime_ms}}}",
        status.queue_depth
    )
    .map_err(io_err)
}

/// Polls `outbox` for the reply keyed by `key`, claiming it atomically, until it
/// appears or `timeout` elapses.
///
/// Each poll is a single [`mailbox::try_claim_response`] (an atomic rename that
/// claims ownership); `Ok(None)` means keep waiting. The reply is checked before
/// the deadline each iteration, so one that lands exactly at the deadline is
/// still claimed. On timeout the request is left in the mailbox and a diagnostic
/// error is returned. The sleep is clamped to the remaining time so a short
/// timeout is honoured tightly.
fn await_response(
    outbox: &Path,
    key: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<MessageEnvelope> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(reply) = mailbox::try_claim_response(outbox, key)? {
            return Ok(reply);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BatonError::Io(format!(
                "timed out after {}ms awaiting a reply to {key:?}; the request remains in the mailbox",
                timeout.as_millis()
            )));
        }
        std::thread::sleep(poll_interval.min(remaining));
    }
}

/// Resolves the `send` message to deliver: builds a request envelope from
/// `--body` (with optional addressing overrides), or reads a complete envelope
/// from `--in`.
///
/// The `--body` ids are derived from the emission time plus the process id so a
/// send needs no external id source and two rapid invocations never collide on a
/// mailbox filename. For `--in`, the addressing arguments are absent (rejected by
/// [`parse_send`]), so the envelope is taken verbatim.
fn build_send_envelope(
    source: &SendSource,
    to: Option<String>,
    from: Option<String>,
    conversation: Option<String>,
) -> Result<MessageEnvelope> {
    match source {
        SendSource::Body(body) => {
            let ts_ms = now_ms();
            let conversation_id = conversation.unwrap_or_else(|| format!("conv-{ts_ms}"));
            let message_id = format!("{conversation_id}-{ts_ms}-{}", std::process::id());
            Ok(MessageEnvelope::new(
                message_id,
                conversation_id,
                from.unwrap_or_else(|| "agent-a".to_string()),
                to.unwrap_or_else(|| "agent-b".to_string()),
                MessageKind::Request,
                body.clone(),
                ts_ms,
            ))
        }
        SendSource::Envelope(path) => read_request_envelope(open_input(Some(path))?),
    }
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

/// Resolves the `--agent-output` selector (+ `--agent-result-key`) into an
/// [`OutputAdapter`]. `None`/`"raw"` ⇒ whole stdout; `"json"` ⇒ the final JSON
/// line's `result_key` field (default `result`). Any other selector is a usage
/// error.
fn build_output_adapter(
    selector: Option<&str>,
    result_key: Option<String>,
) -> Result<OutputAdapter> {
    match selector.unwrap_or("raw") {
        "raw" => Ok(OutputAdapter::Raw),
        "json" => Ok(OutputAdapter::Json {
            result_key: result_key.unwrap_or_else(|| "result".to_string()),
        }),
        other => Err(usage(&format!(
            "--agent-output must be 'raw' or 'json', got {other:?}"
        ))),
    }
}

/// Assembles the external agent's argument list: the first-class role-config
/// flags mapped to the reference agent's (Claude Code) spelling — `--agent-system
/// <path>` → `--append-system-prompt <contents>`, `--agent-mcp-config <path>` →
/// `--mcp-config <path>` — **prepended** to the operator's raw `--agent-arg`
/// values so a hand-supplied override still composes. Reads the system-prompt
/// file (an Io error on failure, matching [`participant_config`]).
fn build_agent_args(
    system_path: Option<&str>,
    mcp_config_path: Option<&str>,
    agent_args: Vec<String>,
) -> Result<Vec<String>> {
    let mut args = Vec::with_capacity(agent_args.len() + 4);
    if let Some(path) = system_path {
        let prompt = std::fs::read_to_string(path).map_err(|err| {
            BatonError::Io(format!(
                "could not read --agent-system file {path:?}: {err}"
            ))
        })?;
        args.push("--append-system-prompt".to_string());
        args.push(prompt);
    }
    if let Some(path) = mcp_config_path {
        args.push("--mcp-config".to_string());
        args.push(path.to_string());
    }
    args.extend(agent_args);
    Ok(args)
}

/// Builds the seed request envelope: participant A's opening message addressed
/// to B. Ids are derived from the emission time so a run needs no external id
/// source; `baton.message/v1` places no format constraint on them beyond
/// uniqueness.
fn build_seed_envelope(body: &str) -> MessageEnvelope {
    build_ring_seed_envelope(body, "agent-a", "agent-b")
}

/// Builds the seed request envelope for an N-party ring: `from`'s opening message
/// addressed to `to` (the first responder). Ids are derived from the emission
/// time, exactly as [`build_seed_envelope`]; the only difference is that the ring
/// names the two endpoints explicitly rather than defaulting to `agent-a`/`agent-b`.
fn build_ring_seed_envelope(body: &str, from: &str, to: &str) -> MessageEnvelope {
    let ts_ms = now_ms();
    let conversation_id = format!("conv-{ts_ms}");
    let message_id = format!("{conversation_id}-m0");
    MessageEnvelope::new(
        message_id,
        conversation_id,
        from,
        to,
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
    output: impl Write,
) -> Result<()> {
    // Mint one id for the whole run and open the session boundary on the trail.
    // Every turn's `request` carries this id; the matching `session_end` closes
    // it on a clean exit.
    let session_id = new_session_id();
    emit(sink, &ExchangeEvent::session_start(now_ms(), &session_id));
    run_session_repl(
        transport,
        sink,
        meta,
        input,
        output,
        session_id,
        Conversation::new(),
        0,
    )
}

/// Resumes a prior session from its rehydrated state and re-enters the REPL.
///
/// Unlike [`execute_session`], no fresh `session_start` is emitted: the original
/// run already opened this session's frame on the trail, and partitioning keys on
/// `session_id` (see [`crate::log::parse_sessions`]), so the resumed run reuses
/// that id and continues its `turn_index`. The preloaded [`Conversation`] means
/// the first new request already carries every prior user + assistant turn.
fn execute_session_resumed(
    transport: &impl Transport,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    input: impl BufRead,
    output: impl Write,
    resumed: ResumedSession,
) -> Result<()> {
    eprintln!(
        "baton session — resumed {} ({} prior turn(s)); type a message and press enter, Ctrl-D or {SESSION_EXIT_COMMAND} to quit",
        resumed.session_id,
        resumed.conversation.len() / 2,
    );
    run_session_repl(
        transport,
        sink,
        meta,
        input,
        output,
        resumed.session_id,
        resumed.conversation,
        resumed.next_turn_index,
    )
}

/// The shared REPL loop behind [`execute_session`] and [`execute_session_resumed`].
///
/// Each line read from `input` becomes a user turn appended to `conversation`;
/// the full accumulated history is sent on every request, so turn N carries all
/// prior user and assistant turns. The assistant reply is printed to `output`
/// (and appended as the next turn). Blank lines are ignored; EOF or a lone
/// [`SESSION_EXIT_COMMAND`] line ends the loop cleanly (the caller returns exit
/// code 0).
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
/// `session_id` frames every turn's `request`; `turn_index` is the next turn's
/// index (0 for a fresh session, the continuation for a resumed one) and counts
/// only turns whose `request` is emitted, so it is bumped after a turn is
/// dispatched, not per input line. The caller owns the opening `session_start`
/// (a fresh session emits one; a resumed session reuses the trail's existing
/// frame); this loop always closes with a `session_end` on a clean exit.
///
/// Parameterised over [`BufRead`]/[`Write`] so the whole loop — history
/// accumulation, exit conditions, and error rollback — is unit-testable with
/// in-memory buffers and a fake transport, without a terminal or a network.
#[allow(clippy::too_many_arguments)]
fn run_session_repl(
    transport: &impl Transport,
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    input: impl BufRead,
    mut output: impl Write,
    session_id: String,
    mut conversation: Conversation,
    mut turn_index: u64,
) -> Result<()> {
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
        let result = timed_exchange(sink, meta, &line, Some((&session_id, turn_index)), || {
            transport.send_conversation(conversation.messages())
        });
        turn_index += 1;

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

    // Clean exit (EOF / `/exit`): close the session boundary. A session killed
    // mid-run never reaches here, so its trail carries a `session_start` and
    // turns but no `session_end` — partitioning keys on `session_id`, not on a
    // matched pair (see `crate::log::parse_sessions`). On a *resumed* run this
    // appends a second `session_end` for a session that already had one (if the
    // prior run exited cleanly); that duplicate is intentional and harmless —
    // `parse_sessions` keys on `session_id` and takes the last `declared_turns`,
    // so read-back reflects the full resumed turn count.
    emit(
        sink,
        &ExchangeEvent::session_end(now_ms(), &session_id, turn_index),
    );

    Ok(())
}

/// A prior session rehydrated from its trail, ready to re-enter the REPL.
///
/// Built by [`select_and_rehydrate`]: the trail's `session_id` (reused so the
/// resumed run extends one coherent session), the [`Conversation`] reconstructed
/// from the recorded turns, and the next `turn_index` (continuing monotonically
/// past the last recorded turn).
#[derive(Debug)]
struct ResumedSession {
    /// The original session's id, reused for every resumed turn.
    session_id: String,
    /// History reconstructed from the trail's completed turns.
    conversation: Conversation,
    /// The `turn_index` the first resumed turn will carry.
    next_turn_index: u64,
}

/// Reads a session trail and rehydrates the target session for `--resume`.
///
/// Opens `file`, partitions it with [`crate::log::parse_sessions`] (torn-tail
/// tolerant), surfaces any parse warnings on stderr, then hands off to
/// [`select_and_rehydrate`]. All of this runs *before* the caller opens the
/// append sink, so a parse or selection failure exits non-zero having written
/// nothing — the same contract as the `--in`/parse-error path.
fn load_resume(file: &str, session_id: Option<&str>) -> Result<ResumedSession> {
    let handle = File::open(file)
        .map_err(|err| BatonError::Io(format!("failed to open --resume file {file:?}: {err}")))?;
    let report = log::parse_sessions(handle)?;
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }
    select_and_rehydrate(report.sessions, session_id)
}

/// Selects the target session and rehydrates it — the pure core of `--resume`.
///
/// With `session_id`, selects that id (a miss is a usage error). Without it, the
/// file must hold exactly one session: zero is a usage error ("no sessions"),
/// more than one is a usage error naming the available ids and requiring
/// `--session`. The selected session's turns are replayed in order into a fresh
/// [`Conversation`]: each turn whose outcome is `Ok` contributes a user + an
/// assistant turn. Turns with an `Error` or a torn (`None`) outcome contributed
/// no assistant reply to the original in-memory history (the live loop rolls a
/// failed user turn back out), so they are skipped — never leaving a dangling
/// user turn that would send two consecutive user turns on resume. The next
/// `turn_index` continues past the last recorded turn (torn or not), keeping the
/// resumed run's indices monotonic and non-colliding.
///
/// Pure over its inputs (no I/O), so selection and rehydration are unit-testable
/// without a trail file.
fn select_and_rehydrate(
    sessions: Vec<log::SessionRecord>,
    session_id: Option<&str>,
) -> Result<ResumedSession> {
    let record = match session_id {
        Some(wanted) => sessions
            .into_iter()
            .find(|s| s.session_id == wanted)
            .ok_or_else(|| usage(&format!("no session {wanted:?} in the --resume trail")))?,
        None => {
            let mut iter = sessions.into_iter();
            let first = iter
                .next()
                .ok_or_else(|| usage("the --resume trail holds no sessions"))?;
            if let Some(second) = iter.next() {
                let mut ids = vec![first.session_id, second.session_id];
                ids.extend(iter.map(|s| s.session_id));
                return Err(usage(&format!(
                    "the --resume trail holds {} sessions; select one with --session <id>: {}",
                    ids.len(),
                    ids.join(", "),
                )));
            }
            first
        }
    };

    let mut conversation = Conversation::new();
    for turn in &record.turns {
        if let Some(log::Outcome::Ok { reply, .. }) = &turn.outcome {
            conversation.push_user(turn.request.prompt.as_str());
            conversation.push_assistant(reply.as_str());
        }
    }

    // Continue past the last recorded turn — including a torn final turn whose
    // outcome never landed (it still counts as a recorded turn). Fall back to the
    // turn count if a `turn_index` is somehow absent.
    let next_turn_index = record
        .turns
        .last()
        .and_then(|t| t.request.turn_index)
        .map_or(record.turns.len() as u64, |i| i + 1);

    Ok(ResumedSession {
        session_id: record.session_id,
        conversation,
        next_turn_index,
    })
}

/// Mints a session id unique to this `baton session` process.
///
/// Derived from the process id and the start timestamp — dependency-free, in the
/// same spirit as the message-id derivation in [`crate::participant`]. One
/// `session` process runs one session, so `(pid, start-ms)` cannot collide with
/// another live session on the same host, and the value carries no format
/// constraint beyond uniqueness.
fn new_session_id() -> String {
    format!("sess-{}-{}", std::process::id(), now_ms())
}

/// Records the request event, times `call`, records the matching outcome event,
/// and returns the call's result.
///
/// Emits the `request` → `response_ok`/`response_error` event pair for the
/// `ask` and session paths, whose orchestration lives here. (`baton exchange`
/// does not route through this: it delegates the call to a [`Participant`] and
/// wires its own trail in [`execute_exchange`].) `event_prompt` is the user text
/// recorded on the `request` event (the turn's input). `session` carries the
/// run's `session_id` and this turn's `turn_index` on the session path, and is
/// `None` on the single-turn `ask` path (whose `request` line stays unframed). A
/// failed event write is downgraded to a stderr warning and never changes the
/// exchange result.
fn timed_exchange(
    sink: &mut dyn EventSink,
    meta: &ExchangeMeta,
    event_prompt: &str,
    session: Option<(&str, u64)>,
    call: impl FnOnce() -> Result<AssistantReply>,
) -> Result<AssistantReply> {
    let request = match session {
        Some((session_id, turn_index)) => {
            ExchangeEvent::session_request(now_ms(), meta, event_prompt, session_id, turn_index)
        }
        None => ExchangeEvent::request(now_ms(), meta, event_prompt),
    };
    emit(sink, &request);

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

/// Opens an append-mode event sink on an explicit trail file, for `--resume`.
///
/// Resuming writes new turns back to the trail it read from (not
/// [`EVENT_LOG_ENV`]), so the resumed run extends the same session file. The file
/// already exists — [`load_resume`] read it first — so a failure here is a real
/// I/O error worth surfacing rather than silently dropping.
fn open_append_sink(path: &str) -> Result<Box<dyn EventSink>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| BatonError::Io(format!("failed to open --resume file {path:?}: {err}")))?;
    Ok(Box::new(WriterSink::new(file)))
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
        "converse-ring" => parse_converse_ring(iter),
        "serve" => parse_serve(iter),
        "send" => parse_send(iter),
        "status" => parse_status(iter),
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
        "merge" => parse_log_merge(iter),
        other => Err(usage(&format!("unknown log subcommand {other:?}"))),
    }
}

/// Parses the arguments following `log merge`.
///
/// Requires `--conversation <id>` (non-blank) and at least one positional trail
/// path; every other token is taken as a trail path. The `--conversation=<id>`
/// form is accepted. A missing/blank conversation id, no trail paths, or a
/// `--conversation` without a value is a usage error.
fn parse_log_merge<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut conversation: Option<String> = None;
    let mut paths: Vec<String> = Vec::new();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--conversation" => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage("--conversation requires a value"))?;
                conversation = Some(value.clone());
            }
            other if other.starts_with("--conversation=") => {
                conversation = Some(other["--conversation=".len()..].to_string());
            }
            other if other.starts_with("--") => {
                return Err(usage(&format!("unexpected argument {other:?}")));
            }
            other => paths.push(other.to_string()),
        }
    }

    let conversation = match conversation {
        Some(id) if !id.trim().is_empty() => id,
        Some(_) => return Err(usage("--conversation <id> must not be empty")),
        None => return Err(usage("log merge requires --conversation <id>")),
    };
    if paths.is_empty() {
        return Err(usage("log merge requires at least one trail path"));
    }

    Ok(Command::LogMerge {
        paths,
        conversation,
    })
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
/// A bare `session` starts a fresh REPL. `--resume <file>` rehydrates a prior
/// session from that JSONL trail; the optional `--session <id>` selects one when
/// the file holds several. Both accept the `--flag=value` form. `--session`
/// without `--resume` — or any other token — is a usage error.
fn parse_session<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut file: Option<String> = None;
    let mut session_id: Option<String> = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--resume" => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage("--resume requires a value"))?;
                file = Some(value.clone());
            }
            other if other.starts_with("--resume=") => {
                file = Some(other["--resume=".len()..].to_string());
            }
            "--session" => {
                let value = iter
                    .next()
                    .ok_or_else(|| usage("--session requires a value"))?;
                session_id = Some(value.clone());
            }
            other if other.starts_with("--session=") => {
                session_id = Some(other["--session=".len()..].to_string());
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    match file {
        Some(file) => Ok(Command::Session {
            resume: Some(ResumeArgs { file, session_id }),
        }),
        None if session_id.is_some() => Err(usage("--session requires --resume <file>")),
        None => Ok(Command::Session { resume: None }),
    }
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
/// exactly one of `--seed <text>` or `--seed-file <path>` is required. Side B
/// may instead be mailbox-backed via `--b-mailbox` (which requires `--b-inbox`
/// and `--b-outbox`, and accepts `--b-await-ms`); the B-mailbox dirs / await are
/// valid only with `--b-mailbox`, and `--b-system`/`--b-model` are rejected
/// alongside it (a mailbox peer configures itself). Every flag also accepts the
/// `--flag=value` form. A flag without a value, both seed forms together, a
/// missing seed, an incoherent B-mailbox combination, or any other token is a
/// usage error.
fn parse_converse<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut a_system: Option<String> = None;
    let mut b_system: Option<String> = None;
    let mut a_model: Option<String> = None;
    let mut b_model: Option<String> = None;
    let mut b_mailbox = false;
    let mut b_inbox: Option<String> = None;
    let mut b_outbox: Option<String> = None;
    let mut b_await_ms: Option<u64> = None;
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
            "--b-mailbox" => b_mailbox = true,
            "--b-inbox" => b_inbox = Some(take("--b-inbox")?),
            other if other.starts_with("--b-inbox=") => {
                b_inbox = Some(other["--b-inbox=".len()..].to_string());
            }
            "--b-outbox" => b_outbox = Some(take("--b-outbox")?),
            other if other.starts_with("--b-outbox=") => {
                b_outbox = Some(other["--b-outbox=".len()..].to_string());
            }
            "--b-await-ms" => b_await_ms = Some(parse_timeout_ms(&take("--b-await-ms")?)?),
            other if other.starts_with("--b-await-ms=") => {
                b_await_ms = Some(parse_timeout_ms(&other["--b-await-ms=".len()..])?);
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

    // B-mailbox coherence: the dirs / await knob describe a mailbox-backed B and
    // are meaningless without it, and a mailbox peer configures itself, so the
    // in-process B overrides cannot apply to it.
    if b_mailbox {
        if b_system.is_some() || b_model.is_some() {
            return Err(usage(
                "--b-system/--b-model apply to an in-process B; --b-mailbox configures its own peer",
            ));
        }
        b_inbox = Some(require_dir(b_inbox, "--b-inbox")?);
        b_outbox = Some(require_dir(b_outbox, "--b-outbox")?);
    } else {
        if b_inbox.is_some() || b_outbox.is_some() {
            return Err(usage(
                "--b-inbox/--b-outbox are only valid with --b-mailbox",
            ));
        }
        if b_await_ms.is_some() {
            return Err(usage("--b-await-ms is only valid with --b-mailbox"));
        }
    }

    Ok(Command::Converse {
        a_system,
        b_system,
        a_model,
        b_model,
        b_mailbox,
        b_inbox,
        b_outbox,
        b_await_ms: b_await_ms.unwrap_or(DEFAULT_CONVERSE_AWAIT_MS),
        seed,
        out_path,
    })
}

/// Parses the arguments following the `converse-ring` subcommand.
///
/// Requires `--registry <path>` and `--roster <a,b,c>` (comma-separated, ≥2
/// members, trimmed, no blank and no duplicate names), plus exactly one of
/// `--seed <text>` / `--seed-file <path>`. Accepts an optional `--await-ms <n>`
/// (positive integer, default [`DEFAULT_CONVERSE_AWAIT_MS`]) and `--out <path>`.
/// Every flag also accepts the `--flag=value` form. A flag without a value, both
/// seed forms together, a missing required flag, a roster with fewer than two
/// distinct members, or any other token is a usage error.
fn parse_converse_ring<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut registry: Option<String> = None;
    let mut roster_raw: Option<String> = None;
    let mut await_ms: Option<u64> = None;
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
            "--registry" => registry = Some(take("--registry")?),
            other if other.starts_with("--registry=") => {
                registry = Some(other["--registry=".len()..].to_string());
            }
            "--roster" => roster_raw = Some(take("--roster")?),
            other if other.starts_with("--roster=") => {
                roster_raw = Some(other["--roster=".len()..].to_string());
            }
            "--await-ms" => await_ms = Some(parse_timeout_ms(&take("--await-ms")?)?),
            other if other.starts_with("--await-ms=") => {
                await_ms = Some(parse_timeout_ms(&other["--await-ms=".len()..])?);
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

    let registry = match registry {
        Some(path) if !path.trim().is_empty() => path,
        _ => return Err(usage("--registry <path> is required")),
    };
    let roster = parse_roster(roster_raw.as_deref())?;

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

    Ok(Command::ConverseRing {
        registry,
        roster,
        seed,
        await_ms: await_ms.unwrap_or(DEFAULT_CONVERSE_AWAIT_MS),
        out_path,
    })
}

/// Parses `--roster`: a comma-separated list of participant names in ring order.
///
/// Each name is trimmed; an empty name is rejected. The ring needs at least two
/// members to have a peer to converse with, and a duplicated name would give one
/// participant two ring positions, so both are usage errors.
fn parse_roster(raw: Option<&str>) -> Result<Vec<String>> {
    let raw = raw.ok_or_else(|| usage("--roster <a,b,c> is required"))?;
    let mut names = Vec::new();
    for part in raw.split(',') {
        let name = part.trim();
        if name.is_empty() {
            return Err(usage("--roster names must not be empty"));
        }
        if names.iter().any(|existing: &String| existing == name) {
            return Err(usage(&format!("--roster has a duplicate name: {name:?}")));
        }
        names.push(name.to_string());
    }
    if names.len() < 2 {
        return Err(usage("--roster needs at least two participants"));
    }
    Ok(names)
}

/// Parses the arguments following the `serve` subcommand.
///
/// The daemon form requires `--inbox <dir>` and `--outbox <dir>` (both
/// non-blank) and accepts an optional `--poll-ms <n>` (positive integer, default
/// [`DEFAULT_SERVE_POLL_MS`]) and the `--once` flag. The cooperative-stop form
/// (`--stop`) requires only `--inbox` and rejects the daemon-only flags
/// (`--outbox`, `--poll-ms`, `--once`). Every valued flag also accepts the
/// `--flag=value` form. A flag without a value, a blank/missing required dir, a
/// non-positive `--poll-ms`, or any other token is a usage error.
fn parse_serve<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut inbox: Option<String> = None;
    let mut outbox: Option<String> = None;
    let mut poll_ms: Option<u64> = None;
    let mut once = false;
    let mut stop = false;
    let mut agent_cmd: Option<String> = None;
    let mut agent_args: Vec<String> = Vec::new();
    let mut agent_cwd: Option<String> = None;
    let mut agent_timeout_ms: Option<u64> = None;
    let mut agent_output: Option<String> = None;
    let mut agent_result_key: Option<String> = None;
    let mut agent_system: Option<String> = None;
    let mut agent_mcp_config: Option<String> = None;

    while let Some(arg) = iter.next() {
        let mut take = |flag: &str| -> Result<String> {
            iter.next()
                .cloned()
                .ok_or_else(|| usage(&format!("{flag} requires a value")))
        };
        match arg.as_str() {
            "--inbox" => inbox = Some(take("--inbox")?),
            other if other.starts_with("--inbox=") => {
                inbox = Some(other["--inbox=".len()..].to_string());
            }
            "--outbox" => outbox = Some(take("--outbox")?),
            other if other.starts_with("--outbox=") => {
                outbox = Some(other["--outbox=".len()..].to_string());
            }
            "--poll-ms" => poll_ms = Some(parse_poll_ms(&take("--poll-ms")?)?),
            other if other.starts_with("--poll-ms=") => {
                poll_ms = Some(parse_poll_ms(&other["--poll-ms=".len()..])?);
            }
            "--once" => once = true,
            "--stop" => stop = true,
            "--agent-cmd" => agent_cmd = Some(take("--agent-cmd")?),
            other if other.starts_with("--agent-cmd=") => {
                agent_cmd = Some(other["--agent-cmd=".len()..].to_string());
            }
            "--agent-arg" => agent_args.push(take("--agent-arg")?),
            other if other.starts_with("--agent-arg=") => {
                agent_args.push(other["--agent-arg=".len()..].to_string());
            }
            "--agent-cwd" => agent_cwd = Some(take("--agent-cwd")?),
            other if other.starts_with("--agent-cwd=") => {
                agent_cwd = Some(other["--agent-cwd=".len()..].to_string());
            }
            "--agent-timeout-ms" => {
                agent_timeout_ms = Some(parse_positive_ms(
                    &take("--agent-timeout-ms")?,
                    "--agent-timeout-ms",
                )?)
            }
            other if other.starts_with("--agent-timeout-ms=") => {
                agent_timeout_ms = Some(parse_positive_ms(
                    &other["--agent-timeout-ms=".len()..],
                    "--agent-timeout-ms",
                )?);
            }
            "--agent-output" => agent_output = Some(take("--agent-output")?),
            other if other.starts_with("--agent-output=") => {
                agent_output = Some(other["--agent-output=".len()..].to_string());
            }
            "--agent-result-key" => agent_result_key = Some(take("--agent-result-key")?),
            other if other.starts_with("--agent-result-key=") => {
                agent_result_key = Some(other["--agent-result-key=".len()..].to_string());
            }
            "--agent-system" => agent_system = Some(take("--agent-system")?),
            other if other.starts_with("--agent-system=") => {
                agent_system = Some(other["--agent-system=".len()..].to_string());
            }
            "--agent-mcp-config" => agent_mcp_config = Some(take("--agent-mcp-config")?),
            other if other.starts_with("--agent-mcp-config=") => {
                agent_mcp_config = Some(other["--agent-mcp-config=".len()..].to_string());
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    // Cooperative-stop client: only `--inbox` is meaningful; the daemon-only
    // flags have no effect here, so reject them rather than silently ignore.
    if stop {
        if outbox.is_some()
            || poll_ms.is_some()
            || once
            || agent_cmd.is_some()
            || !agent_args.is_empty()
            || agent_cwd.is_some()
            || agent_timeout_ms.is_some()
            || agent_output.is_some()
            || agent_result_key.is_some()
            || agent_system.is_some()
            || agent_mcp_config.is_some()
        {
            return Err(usage(
                "--stop takes only --inbox (not --outbox/--poll-ms/--once/--agent-*)",
            ));
        }
        let inbox = require_dir(inbox, "--inbox")?;
        return Ok(Command::ServeStop { inbox });
    }

    // The agent-run flags only qualify `--agent-cmd`; without it they would be
    // silently ignored, so reject them rather than mislead.
    if agent_cmd.is_none()
        && (!agent_args.is_empty()
            || agent_cwd.is_some()
            || agent_timeout_ms.is_some()
            || agent_output.is_some()
            || agent_result_key.is_some()
            || agent_system.is_some()
            || agent_mcp_config.is_some())
    {
        return Err(usage(
            "--agent-arg/--agent-cwd/--agent-timeout-ms/--agent-output/--agent-result-key/--agent-system/--agent-mcp-config require --agent-cmd",
        ));
    }

    // `--agent-result-key` names a field the `json` adapter reads; under `raw`
    // (the default) it has no effect, so reject it rather than silently ignore.
    if agent_result_key.is_some() && agent_output.as_deref() != Some("json") {
        return Err(usage("--agent-result-key requires --agent-output json"));
    }

    let inbox = require_dir(inbox, "--inbox")?;
    let outbox = require_dir(outbox, "--outbox")?;
    Ok(Command::Serve {
        inbox,
        outbox,
        poll_ms: poll_ms.unwrap_or(DEFAULT_SERVE_POLL_MS),
        once,
        agent_cmd,
        agent_args,
        agent_cwd,
        agent_timeout_ms,
        agent_output,
        agent_result_key,
        agent_system,
        agent_mcp_config,
    })
}

/// Parses the arguments following the `send` subcommand.
///
/// Requires `--inbox <dir>` and exactly one message source (`--body <text>` xor
/// `--in <path>`). `--to`/`--from`/`--conversation` describe a `--body`-built
/// message and are rejected alongside `--in` (a full envelope carries its own).
/// `--await` requires `--outbox <dir>`; `--outbox` and `--timeout-ms` are valid
/// only with `--await`. Every valued flag also accepts the `--flag=value` form.
/// A blank body, a missing/blank required dir, a non-positive `--timeout-ms`, or
/// any other token is a usage error.
fn parse_send<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut inbox: Option<String> = None;
    let mut registry: Option<String> = None;
    let mut body: Option<String> = None;
    let mut in_path: Option<String> = None;
    let mut to: Option<String> = None;
    let mut from: Option<String> = None;
    let mut conversation: Option<String> = None;
    let mut await_reply = false;
    let mut outbox: Option<String> = None;
    let mut timeout_ms: Option<u64> = None;

    while let Some(arg) = iter.next() {
        let mut take = |flag: &str| -> Result<String> {
            iter.next()
                .cloned()
                .ok_or_else(|| usage(&format!("{flag} requires a value")))
        };
        match arg.as_str() {
            "--inbox" => inbox = Some(take("--inbox")?),
            other if other.starts_with("--inbox=") => {
                inbox = Some(other["--inbox=".len()..].to_string());
            }
            "--registry" => registry = Some(take("--registry")?),
            other if other.starts_with("--registry=") => {
                registry = Some(other["--registry=".len()..].to_string());
            }
            "--body" => body = Some(take("--body")?),
            other if other.starts_with("--body=") => {
                body = Some(other["--body=".len()..].to_string());
            }
            "--in" => in_path = Some(take("--in")?),
            other if other.starts_with("--in=") => {
                in_path = Some(other["--in=".len()..].to_string());
            }
            "--to" => to = Some(take("--to")?),
            other if other.starts_with("--to=") => {
                to = Some(other["--to=".len()..].to_string());
            }
            "--from" => from = Some(take("--from")?),
            other if other.starts_with("--from=") => {
                from = Some(other["--from=".len()..].to_string());
            }
            "--conversation" => conversation = Some(take("--conversation")?),
            other if other.starts_with("--conversation=") => {
                conversation = Some(other["--conversation=".len()..].to_string());
            }
            "--await" => await_reply = true,
            "--outbox" => outbox = Some(take("--outbox")?),
            other if other.starts_with("--outbox=") => {
                outbox = Some(other["--outbox=".len()..].to_string());
            }
            "--timeout-ms" => timeout_ms = Some(parse_timeout_ms(&take("--timeout-ms")?)?),
            other if other.starts_with("--timeout-ms=") => {
                timeout_ms = Some(parse_timeout_ms(&other["--timeout-ms=".len()..])?);
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    let source = match (body, in_path) {
        (Some(_), Some(_)) => return Err(usage("--body and --in are mutually exclusive")),
        (Some(body), None) => {
            if body.trim().is_empty() {
                return Err(usage("--body must not be empty"));
            }
            SendSource::Body(body)
        }
        (None, Some(path)) => {
            if to.is_some() || from.is_some() || conversation.is_some() {
                return Err(usage(
                    "--to/--from/--conversation apply to --body; --in supplies a complete envelope",
                ));
            }
            SendSource::Envelope(path)
        }
        (None, None) => {
            return Err(usage("missing message: pass --body <text> or --in <path>"));
        }
    };

    // Destination: exactly one of --inbox (a path) / --registry (role lookup).
    let inbox = match (inbox, &registry) {
        (Some(_), Some(_)) => return Err(usage("--inbox and --registry are mutually exclusive")),
        (Some(inbox), None) => Some(require_dir(Some(inbox), "--inbox")?),
        (None, Some(_)) => None,
        (None, None) => return Err(usage("--inbox <dir> or --registry <path> is required")),
    };
    let registry = match registry {
        Some(path) if path.trim().is_empty() => {
            return Err(usage("--registry <path> must not be empty"));
        }
        other => other,
    };
    // A --body send routes by --to (the addressee role); an --in send routes by
    // the envelope's own `to`, so --to is not required (and is rejected above).
    if registry.is_some() && matches!(source, SendSource::Body(_)) && to.is_none() {
        return Err(usage("--registry with --body requires --to <role>"));
    }

    // With --registry the outbox is resolved from the role, so --outbox is
    // rejected; --await then needs no explicit outbox.
    if registry.is_some() && outbox.is_some() {
        return Err(usage("--outbox is supplied by --registry; do not pass it"));
    }
    if await_reply && outbox.is_none() && registry.is_none() {
        return Err(usage(
            "--await requires --outbox <dir> (or --registry to resolve it)",
        ));
    }
    if !await_reply && outbox.is_some() {
        return Err(usage("--outbox is only valid with --await"));
    }
    if !await_reply && timeout_ms.is_some() {
        return Err(usage("--timeout-ms is only valid with --await"));
    }
    let outbox = match outbox {
        Some(dir) if dir.trim().is_empty() => {
            return Err(usage("--outbox <dir> must not be empty"));
        }
        other => other,
    };

    Ok(Command::Send {
        inbox,
        registry,
        source,
        to,
        from,
        conversation,
        await_reply,
        outbox,
        timeout_ms: timeout_ms.unwrap_or(DEFAULT_SEND_TIMEOUT_MS),
    })
}

/// Parses the arguments following the `status` command.
///
/// Accepts either `--mailbox <root>` or a `--registry <path> --role <role>`
/// lookup — the two forms are mutually exclusive, and neither being present is a
/// usage error. `--max-runtime-ms` is an optional positive-integer override of
/// the crashed-stale threshold. Every valued flag also accepts the `--flag=value`
/// form; any other token is a usage error. The `--mailbox`/`--registry`/`--role`
/// combination is validated in [`run`] where the registry is loaded.
fn parse_status<'a>(mut iter: impl Iterator<Item = &'a String>) -> Result<Command> {
    let mut mailbox: Option<String> = None;
    let mut registry: Option<String> = None;
    let mut role: Option<String> = None;
    let mut max_runtime_ms: Option<u64> = None;

    while let Some(arg) = iter.next() {
        let mut take = |flag: &str| -> Result<String> {
            iter.next()
                .cloned()
                .ok_or_else(|| usage(&format!("{flag} requires a value")))
        };
        match arg.as_str() {
            "--mailbox" => mailbox = Some(take("--mailbox")?),
            other if other.starts_with("--mailbox=") => {
                mailbox = Some(other["--mailbox=".len()..].to_string());
            }
            "--registry" => registry = Some(take("--registry")?),
            other if other.starts_with("--registry=") => {
                registry = Some(other["--registry=".len()..].to_string());
            }
            "--role" => role = Some(take("--role")?),
            other if other.starts_with("--role=") => {
                role = Some(other["--role=".len()..].to_string());
            }
            "--max-runtime-ms" => {
                max_runtime_ms = Some(parse_max_runtime_ms(&take("--max-runtime-ms")?)?);
            }
            other if other.starts_with("--max-runtime-ms=") => {
                max_runtime_ms = Some(parse_max_runtime_ms(&other["--max-runtime-ms=".len()..])?);
            }
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }

    // Shape check here; the full --mailbox vs --registry/--role resolution (and
    // registry load) happens in `run`.
    if mailbox.is_none() && registry.is_none() && role.is_none() {
        return Err(usage(
            "status needs --mailbox <root> or --registry <path> --role <role>",
        ));
    }
    if mailbox.is_some() && (registry.is_some() || role.is_some()) {
        return Err(usage(
            "--mailbox and --registry/--role are mutually exclusive",
        ));
    }
    if registry.is_some() != role.is_some() {
        return Err(usage("--registry and --role must be given together"));
    }

    Ok(Command::Status {
        mailbox,
        registry,
        role,
        max_runtime_ms,
    })
}

/// Parses `--max-runtime-ms`: a positive integer of milliseconds (zero is
/// rejected — a zero threshold would flag every live claim as crashed).
fn parse_max_runtime_ms(raw: &str) -> Result<u64> {
    parse_positive_ms(raw, "--max-runtime-ms")
}

/// Parses `--timeout-ms`: a positive integer of milliseconds (zero is rejected —
/// a zero-length await would time out before the first poll could observe a
/// reply).
fn parse_timeout_ms(raw: &str) -> Result<u64> {
    parse_positive_ms(raw, "--timeout-ms")
}

/// Parses a positive-integer millisecond value for `flag`, rejecting zero and
/// non-numeric input with a usage error naming the flag. Shared by every
/// `--*-ms` flag so the "positive integer" rule lives in one place.
fn parse_positive_ms(raw: &str, flag: &str) -> Result<u64> {
    raw.trim()
        .parse::<u64>()
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| usage(&format!("{flag} must be a positive integer, got {raw:?}")))
}

/// Requires a non-blank directory value for `flag`.
fn require_dir(value: Option<String>, flag: &str) -> Result<String> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(usage(&format!("{flag} <dir> is required"))),
    }
}

/// Parses `--poll-ms`: a positive integer of milliseconds (zero is rejected — a
/// zero interval would busy-spin the poll loop).
fn parse_poll_ms(raw: &str) -> Result<u64> {
    parse_positive_ms(raw, "--poll-ms")
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
    fn parses_log_merge_with_paths_and_conversation() {
        assert_eq!(
            parse_args(&argv(&[
                "log",
                "merge",
                "--conversation",
                "c-1",
                "/tmp/a.jsonl",
                "/tmp/b.jsonl"
            ]))
            .expect("parses"),
            Command::LogMerge {
                paths: vec!["/tmp/a.jsonl".to_string(), "/tmp/b.jsonl".to_string()],
                conversation: "c-1".to_string(),
            }
        );
        // `--conversation=<id>` form, and a path given before the flag.
        assert_eq!(
            parse_args(&argv(&[
                "log",
                "merge",
                "/tmp/a.jsonl",
                "--conversation=c-2"
            ]))
            .expect("parses"),
            Command::LogMerge {
                paths: vec!["/tmp/a.jsonl".to_string()],
                conversation: "c-2".to_string(),
            }
        );
    }

    #[test]
    fn log_merge_without_conversation_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log", "merge", "/tmp/a.jsonl"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn log_merge_without_paths_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log", "merge", "--conversation", "c-1"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn log_merge_unknown_flag_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["log", "merge", "--conversation", "c-1", "--bogus"])).unwrap_err(),
            BatonError::Usage(_)
        ));
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
                session_id: None,
                turn_index: None,
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
            Command::Session { resume: None }
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
    fn parses_session_resume_file() {
        assert_eq!(
            parse_args(&argv(&["session", "--resume", "/tmp/trail.jsonl"])).expect("should parse"),
            Command::Session {
                resume: Some(ResumeArgs {
                    file: "/tmp/trail.jsonl".to_string(),
                    session_id: None,
                }),
            }
        );
    }

    #[test]
    fn parses_session_resume_with_session_selector() {
        // Both the spaced and `--flag=value` forms resolve to the same command.
        let spaced = parse_args(&argv(&[
            "session",
            "--resume",
            "/tmp/trail.jsonl",
            "--session",
            "sess-A",
        ]))
        .expect("should parse");
        let joined = parse_args(&argv(&[
            "session",
            "--resume=/tmp/trail.jsonl",
            "--session=sess-A",
        ]))
        .expect("should parse");
        let expected = Command::Session {
            resume: Some(ResumeArgs {
                file: "/tmp/trail.jsonl".to_string(),
                session_id: Some("sess-A".to_string()),
            }),
        };
        assert_eq!(spaced, expected);
        assert_eq!(joined, expected);
    }

    #[test]
    fn session_selector_without_resume_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["session", "--session", "sess-A"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn session_resume_without_value_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["session", "--resume"])).unwrap_err(),
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

        // A session frames its turns with start/end markers: session_start,
        // then two turns × (request + outcome), then session_end.
        assert_eq!(
            sink.events.len(),
            6,
            "session_start + two turns × (request + outcome) + session_end"
        );
        let session_id = match &sink.events[0] {
            ExchangeEvent::SessionStart { session_id, .. } => session_id.clone(),
            other => panic!("first event must be session_start, got: {other:?}"),
        };
        // Each turn's request carries the run's session_id and its 0-based index.
        for (event_idx, expected_turn) in [(1usize, 0u64), (3, 1)] {
            match &sink.events[event_idx] {
                ExchangeEvent::Request {
                    session_id: sid,
                    turn_index,
                    ..
                } => {
                    assert_eq!(sid.as_deref(), Some(session_id.as_str()));
                    assert_eq!(*turn_index, Some(expected_turn));
                }
                other => panic!("event {event_idx} must be a request, got: {other:?}"),
            }
        }
        assert!(matches!(sink.events[2], ExchangeEvent::ResponseOk { .. }));
        assert!(matches!(sink.events[4], ExchangeEvent::ResponseOk { .. }));
        // The end marker closes the same session and reports the turn count.
        match &sink.events[5] {
            ExchangeEvent::SessionEnd {
                session_id: sid,
                turns,
                ..
            } => {
                assert_eq!(sid, &session_id);
                assert_eq!(*turns, 2);
            }
            other => panic!("last event must be session_end, got: {other:?}"),
        }

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

        // Framed as: session_start, turn 0 (request + error), turn 1 (request +
        // ok), session_end. A failed turn still emits its request and advances
        // the turn index, so the end marker counts it.
        assert_eq!(sink.events.len(), 6);
        assert!(matches!(sink.events[0], ExchangeEvent::SessionStart { .. }));
        assert!(matches!(
            &sink.events[1],
            ExchangeEvent::Request {
                turn_index: Some(0),
                ..
            }
        ));
        assert!(matches!(
            sink.events[2],
            ExchangeEvent::ResponseError { .. }
        ));
        assert!(matches!(
            &sink.events[3],
            ExchangeEvent::Request {
                turn_index: Some(1),
                ..
            }
        ));
        assert!(matches!(sink.events[4], ExchangeEvent::ResponseOk { .. }));
        assert!(matches!(
            sink.events[5],
            ExchangeEvent::SessionEnd { turns: 2, .. }
        ));
    }

    // -- baton session --resume --------------------------------------------

    /// Builds a `RequestRecord` for a session turn at `turn_index`.
    fn resume_request(session_id: &str, turn_index: u64, prompt: &str) -> log::RequestRecord {
        log::RequestRecord {
            ts_ms: 1,
            model: "m".to_string(),
            base_url: "u".to_string(),
            prompt: prompt.to_string(),
            session_id: Some(session_id.to_string()),
            turn_index: Some(turn_index),
        }
    }

    /// A completed session turn (`Ok` outcome carrying `reply`).
    fn ok_turn(session_id: &str, turn_index: u64, prompt: &str, reply: &str) -> log::SessionTurn {
        log::SessionTurn {
            request: resume_request(session_id, turn_index, prompt),
            outcome: Some(log::Outcome::Ok {
                ts_ms: 2,
                duration_ms: 1,
                reply: reply.to_string(),
                input_tokens: None,
                output_tokens: None,
            }),
        }
    }

    /// A session record wrapping the given turns (start marker seen, no end).
    fn session_record(session_id: &str, turns: Vec<log::SessionTurn>) -> log::SessionRecord {
        log::SessionRecord {
            session_id: session_id.to_string(),
            started: true,
            ended: false,
            declared_turns: None,
            turns,
        }
    }

    #[test]
    fn rehydrate_builds_user_assistant_pairs_and_continues_turn_index() {
        let record = session_record(
            "sess-A",
            vec![
                ok_turn("sess-A", 0, "hi", "hello"),
                ok_turn("sess-A", 1, "again", "yo"),
            ],
        );
        let resumed = select_and_rehydrate(vec![record], None).expect("rehydrates");
        assert_eq!(resumed.session_id, "sess-A");
        assert_eq!(
            resumed.conversation.messages(),
            &[
                Message::user("hi"),
                Message::assistant("hello"),
                Message::user("again"),
                Message::assistant("yo"),
            ]
        );
        // Two recorded turns (0, 1) → the next resumed turn is index 2.
        assert_eq!(resumed.next_turn_index, 2);
    }

    #[test]
    fn rehydrate_skips_torn_final_turn_but_advances_past_it() {
        // A torn final turn — its `request` landed (turn_index 1) but the outcome
        // never did — contributes no assistant reply, so it is not replayed into
        // the history; the next turn_index still advances past it (to 2).
        let torn = log::SessionTurn {
            request: resume_request("sess-A", 1, "unanswered"),
            outcome: None,
        };
        let record = session_record("sess-A", vec![ok_turn("sess-A", 0, "hi", "hello"), torn]);
        let resumed = select_and_rehydrate(vec![record], None).expect("rehydrates");
        assert_eq!(
            resumed.conversation.messages(),
            &[Message::user("hi"), Message::assistant("hello")],
            "the torn turn leaves no dangling user turn"
        );
        assert_eq!(
            resumed.next_turn_index, 2,
            "index advances past the torn turn"
        );
    }

    #[test]
    fn rehydrate_skips_errored_turn() {
        let errored = log::SessionTurn {
            request: resume_request("sess-A", 1, "boom"),
            outcome: Some(log::Outcome::Error {
                ts_ms: 2,
                duration_ms: 1,
                kind: "transport".to_string(),
                message: "network down".to_string(),
            }),
        };
        let record = session_record("sess-A", vec![ok_turn("sess-A", 0, "hi", "hello"), errored]);
        let resumed = select_and_rehydrate(vec![record], None).expect("rehydrates");
        assert_eq!(
            resumed.conversation.messages(),
            &[Message::user("hi"), Message::assistant("hello")],
            "an errored turn contributes no assistant reply"
        );
        assert_eq!(resumed.next_turn_index, 2);
    }

    #[test]
    fn rehydrate_selects_named_session_from_many() {
        let sessions = vec![
            session_record("sess-A", vec![ok_turn("sess-A", 0, "a", "ra")]),
            session_record("sess-B", vec![ok_turn("sess-B", 0, "b", "rb")]),
        ];
        let resumed = select_and_rehydrate(sessions, Some("sess-B")).expect("selects B");
        assert_eq!(resumed.session_id, "sess-B");
        assert_eq!(resumed.conversation.messages()[0], Message::user("b"));
    }

    #[test]
    fn rehydrate_unknown_session_id_is_usage_error() {
        let sessions = vec![session_record(
            "sess-A",
            vec![ok_turn("sess-A", 0, "a", "ra")],
        )];
        assert!(matches!(
            select_and_rehydrate(sessions, Some("sess-Z")).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn rehydrate_ambiguous_selection_is_usage_error() {
        let sessions = vec![
            session_record("sess-A", vec![ok_turn("sess-A", 0, "a", "ra")]),
            session_record("sess-B", vec![ok_turn("sess-B", 0, "b", "rb")]),
        ];
        match select_and_rehydrate(sessions, None).unwrap_err() {
            // The error names both ids so the caller knows what to pass.
            BatonError::Usage(msg) => {
                assert!(
                    msg.contains("sess-A") && msg.contains("sess-B"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Usage, got {other:?}"),
        }
    }

    #[test]
    fn rehydrate_empty_trail_is_usage_error() {
        assert!(matches!(
            select_and_rehydrate(vec![], None).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn resumed_session_first_request_carries_prior_history_and_continues_frame() {
        // A prior session with two completed turns, rehydrated and resumed.
        let record = session_record(
            "sess-A",
            vec![
                ok_turn("sess-A", 0, "who won 1998?", "France"),
                ok_turn("sess-A", 1, "the final score?", "3–0"),
            ],
        );
        let resumed = select_and_rehydrate(vec![record], None).expect("rehydrates");

        let transport = RecordingTransport::new();
        let mut sink = RecordingSink::new();
        let input = Cursor::new("and who did they beat?\n");
        let mut output: Vec<u8> = Vec::new();

        execute_session_resumed(
            &transport,
            &mut sink,
            &test_meta(),
            input,
            &mut output,
            resumed,
        )
        .expect("resume exits cleanly on EOF");

        // The first resumed request already carries every prior user+assistant
        // turn, then the new user line — the model sees the earlier context.
        let calls = transport.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            vec![
                Message::user("who won 1998?"),
                Message::assistant("France"),
                Message::user("the final score?"),
                Message::assistant("3–0"),
                Message::user("and who did they beat?"),
            ]
        );

        // No fresh session_start — the resumed run reuses the trail's frame. The
        // first event is the continuing turn's request at index 2 under sess-A;
        // the closing session_end reports the full resumed turn count (3).
        assert!(
            !sink
                .events
                .iter()
                .any(|e| matches!(e, ExchangeEvent::SessionStart { .. })),
            "resume must not open a second session frame"
        );
        match &sink.events[0] {
            ExchangeEvent::Request {
                session_id,
                turn_index,
                ..
            } => {
                assert_eq!(session_id.as_deref(), Some("sess-A"));
                assert_eq!(*turn_index, Some(2), "turn_index continues from the trail");
            }
            other => panic!("first resumed event must be a request, got: {other:?}"),
        }
        match sink.events.last().expect("has events") {
            ExchangeEvent::SessionEnd {
                session_id, turns, ..
            } => {
                assert_eq!(session_id, "sess-A");
                assert_eq!(*turns, 3, "one new turn on top of the two resumed");
            }
            other => panic!("last resumed event must be session_end, got: {other:?}"),
        }
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
                b_mailbox: false,
                b_inbox: None,
                b_outbox: None,
                b_await_ms: DEFAULT_CONVERSE_AWAIT_MS,
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
                b_mailbox: false,
                b_inbox: None,
                b_outbox: None,
                b_await_ms: DEFAULT_CONVERSE_AWAIT_MS,
                seed: SeedSource::File("/tmp/seed.txt".to_string()),
                out_path: Some("/tmp/trail.jsonl".to_string()),
            }
        );
    }

    #[test]
    fn parses_converse_with_b_mailbox() {
        assert_eq!(
            parse_args(&argv(&[
                "converse",
                "--seed",
                "hi",
                "--b-mailbox",
                "--b-inbox=/tmp/in",
                "--b-outbox",
                "/tmp/out",
                "--b-await-ms=90000",
            ]))
            .expect("parses"),
            Command::Converse {
                a_system: None,
                b_system: None,
                a_model: None,
                b_model: None,
                b_mailbox: true,
                b_inbox: Some("/tmp/in".to_string()),
                b_outbox: Some("/tmp/out".to_string()),
                b_await_ms: 90_000,
                seed: SeedSource::Text("hi".to_string()),
                out_path: None,
            }
        );
    }

    #[test]
    fn parses_converse_b_mailbox_defaults_await() {
        // `--b-await-ms` omitted ⇒ the generous default.
        match parse_args(&argv(&[
            "converse",
            "--seed",
            "hi",
            "--b-mailbox",
            "--b-inbox=/tmp/in",
            "--b-outbox=/tmp/out",
        ]))
        .expect("parses")
        {
            Command::Converse { b_await_ms, .. } => {
                assert_eq!(b_await_ms, DEFAULT_CONVERSE_AWAIT_MS)
            }
            other => panic!("expected Converse, got {other:?}"),
        }
    }

    #[test]
    fn converse_b_mailbox_incoherent_combinations_are_usage_errors() {
        let cases: &[&[&str]] = &[
            // --b-mailbox without the required dirs.
            &["converse", "--seed", "hi", "--b-mailbox"],
            &["converse", "--seed", "hi", "--b-mailbox", "--b-inbox=/in"],
            &["converse", "--seed", "hi", "--b-mailbox", "--b-outbox=/out"],
            // B-mailbox dirs / await without --b-mailbox.
            &["converse", "--seed", "hi", "--b-inbox=/in"],
            &["converse", "--seed", "hi", "--b-outbox=/out"],
            &["converse", "--seed", "hi", "--b-await-ms=5000"],
            // In-process B overrides alongside --b-mailbox.
            &[
                "converse",
                "--seed",
                "hi",
                "--b-mailbox",
                "--b-inbox=/in",
                "--b-outbox=/out",
                "--b-model=m",
            ],
            &[
                "converse",
                "--seed",
                "hi",
                "--b-mailbox",
                "--b-inbox=/in",
                "--b-outbox=/out",
                "--b-system=/s",
            ],
            // Non-positive await.
            &[
                "converse",
                "--seed",
                "hi",
                "--b-mailbox",
                "--b-inbox=/in",
                "--b-outbox=/out",
                "--b-await-ms=0",
            ],
        ];
        for case in cases {
            assert!(
                matches!(parse_args(&argv(case)).unwrap_err(), BatonError::Usage(_)),
                "expected usage error for {case:?}"
            );
        }
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

    // -- baton converse-ring -----------------------------------------------

    #[test]
    fn parses_converse_ring_with_defaults() {
        assert_eq!(
            parse_args(&argv(&[
                "converse-ring",
                "--registry",
                "/tmp/reg.json",
                "--roster",
                "alice,bob,carol",
                "--seed",
                "hello",
            ]))
            .expect("parses"),
            Command::ConverseRing {
                registry: "/tmp/reg.json".to_string(),
                roster: vec!["alice".to_string(), "bob".to_string(), "carol".to_string(),],
                seed: SeedSource::Text("hello".to_string()),
                await_ms: DEFAULT_CONVERSE_AWAIT_MS,
                out_path: None,
            }
        );
    }

    #[test]
    fn parses_converse_ring_with_all_flags_and_equals_forms() {
        assert_eq!(
            parse_args(&argv(&[
                "converse-ring",
                "--registry=/tmp/reg.json",
                "--roster= alice , bob ",
                "--await-ms=90000",
                "--seed-file",
                "/tmp/seed.txt",
                "--out=/tmp/trail.jsonl",
            ]))
            .expect("parses"),
            Command::ConverseRing {
                registry: "/tmp/reg.json".to_string(),
                roster: vec!["alice".to_string(), "bob".to_string()],
                seed: SeedSource::File("/tmp/seed.txt".to_string()),
                await_ms: 90_000,
                out_path: Some("/tmp/trail.jsonl".to_string()),
            }
        );
    }

    #[test]
    fn converse_ring_incoherent_combinations_are_usage_errors() {
        let cases: &[&[&str]] = &[
            // Missing --registry.
            &["converse-ring", "--roster", "a,b", "--seed", "hi"],
            // Blank --registry.
            &[
                "converse-ring",
                "--registry",
                "   ",
                "--roster",
                "a,b",
                "--seed",
                "hi",
            ],
            // Missing --roster.
            &["converse-ring", "--registry", "/r", "--seed", "hi"],
            // Roster with a single member.
            &[
                "converse-ring",
                "--registry",
                "/r",
                "--roster",
                "solo",
                "--seed",
                "hi",
            ],
            // Roster with a blank name.
            &[
                "converse-ring",
                "--registry",
                "/r",
                "--roster",
                "a,,b",
                "--seed",
                "hi",
            ],
            // Roster with a duplicate name.
            &[
                "converse-ring",
                "--registry",
                "/r",
                "--roster",
                "a,b,a",
                "--seed",
                "hi",
            ],
            // Missing seed.
            &["converse-ring", "--registry", "/r", "--roster", "a,b"],
            // Both seed forms.
            &[
                "converse-ring",
                "--registry",
                "/r",
                "--roster",
                "a,b",
                "--seed",
                "hi",
                "--seed-file",
                "/s",
            ],
            // Flag without a value.
            &["converse-ring", "--registry"],
            // Unknown token.
            &[
                "converse-ring",
                "--registry",
                "/r",
                "--roster",
                "a,b",
                "--seed",
                "hi",
                "--who",
            ],
        ];
        for case in cases {
            assert!(
                matches!(parse_args(&argv(case)).unwrap_err(), BatonError::Usage(_)),
                "expected usage error for {case:?}"
            );
        }
    }

    #[test]
    fn build_ring_seed_envelope_names_both_endpoints() {
        let seed = build_ring_seed_envelope("kick off", "alice", "bob");
        assert_eq!(seed.kind, MessageKind::Request);
        assert_eq!(seed.from, "alice");
        assert_eq!(seed.to, "bob");
        assert_eq!(seed.body, "kick off");
        assert!(seed.in_reply_to.is_none());
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

    // -- serve / parse_serve ------------------------------------------------

    #[test]
    fn parse_serve_requires_inbox_and_outbox() {
        assert_eq!(
            parse_args(&argv(&["serve", "--inbox=/tmp/in", "--outbox=/tmp/out"])).expect("parses"),
            Command::Serve {
                inbox: "/tmp/in".to_string(),
                outbox: "/tmp/out".to_string(),
                poll_ms: DEFAULT_SERVE_POLL_MS,
                once: false,
                agent_cmd: None,
                agent_args: vec![],
                agent_cwd: None,
                agent_timeout_ms: None,
                agent_output: None,
                agent_result_key: None,
                agent_system: None,
                agent_mcp_config: None,
            }
        );
    }

    #[test]
    fn parse_serve_stop_requires_only_inbox() {
        assert_eq!(
            parse_args(&argv(&["serve", "--stop", "--inbox=/tmp/in"])).expect("parses"),
            Command::ServeStop {
                inbox: "/tmp/in".to_string(),
            }
        );
    }

    #[test]
    fn parse_serve_stop_rejects_daemon_only_flags() {
        // --outbox, --poll-ms, and --once have no meaning for the stop client.
        assert!(matches!(
            parse_args(&argv(&[
                "serve",
                "--stop",
                "--inbox=/tmp/in",
                "--outbox=/tmp/out"
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
        assert!(matches!(
            parse_args(&argv(&["serve", "--stop", "--inbox=/tmp/in", "--once"])).unwrap_err(),
            BatonError::Usage(_)
        ));
        assert!(matches!(
            parse_args(&argv(&[
                "serve",
                "--stop",
                "--inbox=/tmp/in",
                "--poll-ms=50"
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
        // The agent-run flags are equally meaningless for the stop client.
        assert!(matches!(
            parse_args(&argv(&[
                "serve",
                "--stop",
                "--inbox=/tmp/in",
                "--agent-cmd=claude"
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_accepts_agent_flags() {
        assert_eq!(
            parse_args(&argv(&[
                "serve",
                "--inbox",
                "/tmp/in",
                "--outbox",
                "/tmp/out",
                "--agent-cmd",
                "claude",
                "--agent-arg",
                "-p",
                "--agent-arg",
                "--output-format=text",
                "--agent-cwd",
                "/tmp/work",
                "--agent-timeout-ms",
                "900000",
            ]))
            .expect("parses"),
            Command::Serve {
                inbox: "/tmp/in".to_string(),
                outbox: "/tmp/out".to_string(),
                poll_ms: DEFAULT_SERVE_POLL_MS,
                once: false,
                agent_cmd: Some("claude".to_string()),
                agent_args: vec!["-p".to_string(), "--output-format=text".to_string()],
                agent_cwd: Some("/tmp/work".to_string()),
                agent_timeout_ms: Some(900_000),
                agent_output: None,
                agent_result_key: None,
                agent_system: None,
                agent_mcp_config: None,
            }
        );
    }

    #[test]
    fn parse_serve_accepts_agent_flags_equals_form() {
        assert_eq!(
            parse_args(&argv(&[
                "serve",
                "--inbox=/tmp/in",
                "--outbox=/tmp/out",
                "--agent-cmd=claude",
                "--agent-arg=-p",
            ]))
            .expect("parses"),
            Command::Serve {
                inbox: "/tmp/in".to_string(),
                outbox: "/tmp/out".to_string(),
                poll_ms: DEFAULT_SERVE_POLL_MS,
                once: false,
                agent_cmd: Some("claude".to_string()),
                agent_args: vec!["-p".to_string()],
                agent_cwd: None,
                agent_timeout_ms: None,
                agent_output: None,
                agent_result_key: None,
                agent_system: None,
                agent_mcp_config: None,
            }
        );
    }

    #[test]
    fn parse_serve_accepts_role_config_flags() {
        assert_eq!(
            parse_args(&argv(&[
                "serve",
                "--inbox=/tmp/in",
                "--outbox=/tmp/out",
                "--agent-cmd=codex",
                "--agent-output=json",
                "--agent-result-key=message",
                "--agent-system=/tmp/role.txt",
                "--agent-mcp-config=/tmp/mcp.json",
            ]))
            .expect("parses"),
            Command::Serve {
                inbox: "/tmp/in".to_string(),
                outbox: "/tmp/out".to_string(),
                poll_ms: DEFAULT_SERVE_POLL_MS,
                once: false,
                agent_cmd: Some("codex".to_string()),
                agent_args: vec![],
                agent_cwd: None,
                agent_timeout_ms: None,
                agent_output: Some("json".to_string()),
                agent_result_key: Some("message".to_string()),
                agent_system: Some("/tmp/role.txt".to_string()),
                agent_mcp_config: Some("/tmp/mcp.json".to_string()),
            }
        );
    }

    #[test]
    fn parse_serve_result_key_requires_json_output() {
        // Under the default `raw` adapter, --agent-result-key has no effect.
        assert!(matches!(
            parse_args(&argv(&[
                "serve",
                "--inbox=/tmp/in",
                "--outbox=/tmp/out",
                "--agent-cmd=claude",
                "--agent-result-key=result",
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_rejects_unknown_agent_output() {
        assert!(matches!(
            build_output_adapter(Some("yaml"), None).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn build_output_adapter_maps_selectors() {
        assert_eq!(
            build_output_adapter(None, None).expect("raw default"),
            OutputAdapter::Raw
        );
        assert_eq!(
            build_output_adapter(Some("raw"), None).expect("raw"),
            OutputAdapter::Raw
        );
        assert_eq!(
            build_output_adapter(Some("json"), None).expect("json default key"),
            OutputAdapter::Json {
                result_key: "result".to_string()
            }
        );
        assert_eq!(
            build_output_adapter(Some("json"), Some("message".to_string())).expect("json key"),
            OutputAdapter::Json {
                result_key: "message".to_string()
            }
        );
    }

    #[test]
    fn build_agent_args_prepends_role_config_before_operator_args() {
        let root = TempRoot::new("agent-args");
        let role = root.path.join("role.txt");
        std::fs::write(&role, "You are the reviewer.").expect("write role file");

        let args = build_agent_args(
            Some(role.to_str().unwrap()),
            Some("/tmp/mcp.json"),
            vec![
                "-p".to_string(),
                "--dangerously-skip-permissions".to_string(),
            ],
        )
        .expect("assembles");

        assert_eq!(
            args,
            vec![
                "--append-system-prompt".to_string(),
                "You are the reviewer.".to_string(),
                "--mcp-config".to_string(),
                "/tmp/mcp.json".to_string(),
                "-p".to_string(),
                "--dangerously-skip-permissions".to_string(),
            ]
        );
    }

    #[test]
    fn build_agent_args_without_role_config_is_operator_args_verbatim() {
        let args = build_agent_args(None, None, vec!["-p".to_string()]).expect("assembles");
        assert_eq!(args, vec!["-p".to_string()]);
    }

    #[test]
    fn build_agent_args_missing_system_file_is_io_error() {
        assert!(matches!(
            build_agent_args(Some("/no/such/role/file.txt"), None, vec![]).unwrap_err(),
            BatonError::Io(_)
        ));
    }

    #[test]
    fn parse_serve_agent_run_flags_require_agent_cmd() {
        // The agent-run + role-config flags without --agent-cmd would be silently
        // ignored, so each is a usage error on its own.
        for flag in [
            "--agent-arg=-p",
            "--agent-cwd=/tmp/work",
            "--agent-timeout-ms=1000",
            "--agent-output=json",
            "--agent-system=/tmp/role.txt",
            "--agent-mcp-config=/tmp/mcp.json",
        ] {
            assert!(
                matches!(
                    parse_args(&argv(&[
                        "serve",
                        "--inbox=/tmp/in",
                        "--outbox=/tmp/out",
                        flag
                    ]))
                    .unwrap_err(),
                    BatonError::Usage(_)
                ),
                "{flag} without --agent-cmd should be a usage error"
            );
        }
    }

    #[test]
    fn parse_serve_non_positive_agent_timeout_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&[
                "serve",
                "--inbox=/tmp/in",
                "--outbox=/tmp/out",
                "--agent-cmd=claude",
                "--agent-timeout-ms=0",
            ]))
            .unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_stop_requires_inbox() {
        assert!(matches!(
            parse_args(&argv(&["serve", "--stop"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_accepts_poll_ms_and_once() {
        assert_eq!(
            parse_args(&argv(&[
                "serve",
                "--inbox",
                "/tmp/in",
                "--outbox",
                "/tmp/out",
                "--poll-ms",
                "50",
                "--once",
            ]))
            .expect("parses"),
            Command::Serve {
                inbox: "/tmp/in".to_string(),
                outbox: "/tmp/out".to_string(),
                poll_ms: 50,
                once: true,
                agent_cmd: None,
                agent_args: vec![],
                agent_cwd: None,
                agent_timeout_ms: None,
                agent_output: None,
                agent_result_key: None,
                agent_system: None,
                agent_mcp_config: None,
            }
        );
    }

    #[test]
    fn parse_serve_missing_required_dir_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["serve", "--outbox=/tmp/out"])).unwrap_err(),
            BatonError::Usage(_)
        ));
        assert!(matches!(
            parse_args(&argv(&["serve", "--inbox=/tmp/in"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_blank_dir_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["serve", "--inbox=  ", "--outbox=/tmp/out"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    #[test]
    fn parse_serve_non_positive_poll_ms_is_usage_error() {
        for bad in ["0", "-1", "abc"] {
            assert!(
                matches!(
                    parse_args(&argv(&[
                        "serve",
                        "--inbox=/tmp/in",
                        "--outbox=/tmp/out",
                        "--poll-ms",
                        bad
                    ]))
                    .unwrap_err(),
                    BatonError::Usage(_)
                ),
                "--poll-ms {bad:?} must be a usage error"
            );
        }
    }

    #[test]
    fn parse_serve_flag_without_value_is_usage_error() {
        assert!(matches!(
            parse_args(&argv(&["serve", "--inbox"])).unwrap_err(),
            BatonError::Usage(_)
        ));
    }

    /// A unique self-cleaning temp dir, mirroring the mailbox unit tests.
    struct TempRoot {
        path: std::path::PathBuf,
    }

    impl TempRoot {
        fn new(tag: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("baton-serve-{}-{}", std::process::id(), tag));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp serve dir");
            Self { path }
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn json_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.ends_with(".json"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// End-to-end drain over a real mailbox and the in-process participant: one
    /// request in `pending/` yields one correlated reply in the outbox keyed by
    /// the request id, moves the request to `done/`, and a second drain is a
    /// no-op (dedup). Network-free — `OkTransport` stands in for the provider.
    #[test]
    fn drain_mailbox_answers_and_dedups() {
        let root = TempRoot::new("drain");
        let inbox = root.path.join("inbox");
        let outbox = root.path.join("outbox");

        let mailbox = Mailbox::open(&inbox).expect("open mailbox");
        mailbox.deliver(&request_envelope()).expect("deliver");

        let participant = participant_over(OkTransport::new("four"));
        let mut sink = NoopSink;

        let drained =
            drain_mailbox(&mailbox, &outbox, &participant, &mut sink, &test_meta()).expect("drain");
        assert!(matches!(drained, Drain::Drained(1)), "one request drained");

        // The reply is keyed by the request id (m-req-1), not the fresh response id.
        assert_eq!(json_files(&outbox), vec!["m-req-1.json".to_string()]);
        assert_eq!(
            json_files(&inbox.join("done")).len(),
            1,
            "request completed"
        );
        assert!(json_files(&inbox.join("pending")).is_empty());

        // Re-running is a no-op: the id is in `done/`.
        let drained2 = drain_mailbox(&mailbox, &outbox, &participant, &mut sink, &test_meta())
            .expect("second drain");
        assert!(
            matches!(drained2, Drain::Drained(0)),
            "already-done id is not reprocessed"
        );
    }

    /// A stop sentinel dropped before a claim makes the drain pass return
    /// `Stopped` without processing the still-pending message — the between-
    /// messages cooperative-stop check.
    #[test]
    fn drain_mailbox_stops_on_sentinel_before_processing() {
        let root = TempRoot::new("drain-stop");
        let inbox = root.path.join("inbox");
        let outbox = root.path.join("outbox");

        let mailbox = Mailbox::open(&inbox).expect("open mailbox");
        mailbox.deliver(&request_envelope()).expect("deliver");
        // A cooperative stop arrives before the daemon claims the message.
        mailbox::request_stop(&inbox).expect("request stop");

        let participant = participant_over(OkTransport::new("four"));
        let mut sink = NoopSink;

        let drained =
            drain_mailbox(&mailbox, &outbox, &participant, &mut sink, &test_meta()).expect("drain");
        assert!(matches!(drained, Drain::Stopped), "sentinel ⇒ Stopped");
        assert_eq!(
            json_files(&inbox.join("pending")).len(),
            1,
            "the pending message is left unprocessed"
        );
        assert!(json_files(&outbox).is_empty(), "no reply written");
    }

    /// A reclaimed in-flight message re-drains to the *same* outbox filename —
    /// one file, overwritten, not two (the keyed-outbox guarantee end-to-end).
    #[test]
    fn drain_mailbox_reprocess_keeps_single_outbox_file() {
        let root = TempRoot::new("reprocess");
        let inbox = root.path.join("inbox");
        let outbox = root.path.join("outbox");

        let mailbox = Mailbox::open(&inbox).expect("open mailbox");
        let participant = participant_over(OkTransport::new("four"));
        let mut sink = NoopSink;

        // First delivery + drain writes one reply.
        mailbox.deliver(&request_envelope()).expect("deliver");
        drain_mailbox(&mailbox, &outbox, &participant, &mut sink, &test_meta()).expect("drain");
        assert_eq!(json_files(&outbox).len(), 1);

        // Simulate a reprocess: re-deliver the same request id and drain again.
        // (A real reclaim moves `claimed/ → pending/`; re-delivery is the same
        // effect on the outbox key.) It must overwrite, not append.
        std::fs::remove_file(inbox.join("done").join("m-req-1.json")).expect("clear done");
        mailbox.deliver(&request_envelope()).expect("re-deliver");
        drain_mailbox(&mailbox, &outbox, &participant, &mut sink, &test_meta()).expect("re-drain");
        assert_eq!(
            json_files(&outbox).len(),
            1,
            "keyed by request id ⇒ reprocess overwrites, single outbox file"
        );
    }

    // ---- `baton send` --------------------------------------------------------

    fn send_request(id: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            id,
            "conv-9",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "ping",
            1_700_000_000_000,
        )
    }

    fn reply_to(request_id: &str) -> MessageEnvelope {
        let mut r = MessageEnvelope::new(
            "r-1",
            "conv-9",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "pong",
            1_700_000_000_001,
        );
        r.in_reply_to = Some(request_id.to_string());
        r
    }

    /// Writes a reply into `outbox` keyed by `key`, as `serve`'s
    /// `deliver_response` would, so `try_claim_response` finds it.
    fn seed_reply(outbox: &Path, key: &str, reply: &MessageEnvelope) {
        std::fs::create_dir_all(outbox).expect("create outbox");
        let json = serde_json::to_string(reply).expect("serialize reply");
        std::fs::write(outbox.join(format!("{key}.json")), json).expect("seed reply");
    }

    #[test]
    fn parses_send_body_minimal() {
        assert_eq!(
            parse_args(&argv(&["send", "--inbox", "/tmp/mb", "--body", "hi"])).expect("parses"),
            Command::Send {
                inbox: Some("/tmp/mb".to_string()),
                registry: None,
                source: SendSource::Body("hi".to_string()),
                to: None,
                from: None,
                conversation: None,
                await_reply: false,
                outbox: None,
                timeout_ms: DEFAULT_SEND_TIMEOUT_MS,
            }
        );
    }

    #[test]
    fn parses_send_await_with_outbox_and_timeout() {
        assert_eq!(
            parse_args(&argv(&[
                "send",
                "--inbox=/tmp/mb",
                "--in=/tmp/env.json",
                "--await",
                "--outbox=/tmp/ob",
                "--timeout-ms=1500",
            ]))
            .expect("parses"),
            Command::Send {
                inbox: Some("/tmp/mb".to_string()),
                registry: None,
                source: SendSource::Envelope("/tmp/env.json".to_string()),
                to: None,
                from: None,
                conversation: None,
                await_reply: true,
                outbox: Some("/tmp/ob".to_string()),
                timeout_ms: 1500,
            }
        );
    }

    #[test]
    fn send_source_and_dependency_rules_are_usage_errors() {
        let cases: &[&[&str]] = &[
            &["send", "--body", "hi"],                        // missing --inbox
            &["send", "--inbox", "/tmp/mb"],                  // missing source
            &["send", "--inbox", "/tmp/mb", "--body", "   "], // blank body
            &["send", "--inbox", "/tmp/mb", "--body", "hi", "--in", "/p"], // both sources
            &["send", "--inbox", "/tmp/mb", "--in", "/p", "--to", "x"], // addressing with --in
            &["send", "--inbox", "/tmp/mb", "--body", "hi", "--await"], // --await sans --outbox
            &[
                "send", "--inbox", "/tmp/mb", "--body", "hi", "--outbox", "/ob",
            ], // --outbox sans --await
            &[
                "send",
                "--inbox",
                "/tmp/mb",
                "--body",
                "hi",
                "--timeout-ms",
                "10",
            ], // --timeout-ms sans --await
            &[
                "send",
                "--inbox",
                "/tmp/mb",
                "--body",
                "hi",
                "--await",
                "--outbox",
                "/ob",
                "--timeout-ms",
                "0",
            ], // zero timeout
        ];
        for case in cases {
            assert!(
                matches!(parse_args(&argv(case)).unwrap_err(), BatonError::Usage(_)),
                "expected usage error for {case:?}"
            );
        }
    }

    #[test]
    fn parses_send_registry_role_addressed() {
        assert_eq!(
            parse_args(&argv(&[
                "send",
                "--registry",
                "/tmp/reg.json",
                "--to",
                "reviewer",
                "--body",
                "hi",
            ]))
            .expect("parses"),
            Command::Send {
                inbox: None,
                registry: Some("/tmp/reg.json".to_string()),
                source: SendSource::Body("hi".to_string()),
                to: Some("reviewer".to_string()),
                from: None,
                conversation: None,
                await_reply: false,
                outbox: None,
                timeout_ms: DEFAULT_SEND_TIMEOUT_MS,
            }
        );
    }

    #[test]
    fn parses_send_registry_await_without_outbox() {
        // --registry supplies the outbox, so --await needs no --outbox.
        assert_eq!(
            parse_args(&argv(&[
                "send",
                "--registry=/tmp/reg.json",
                "--to=reviewer",
                "--body=hi",
                "--await",
            ]))
            .expect("parses"),
            Command::Send {
                inbox: None,
                registry: Some("/tmp/reg.json".to_string()),
                source: SendSource::Body("hi".to_string()),
                to: Some("reviewer".to_string()),
                from: None,
                conversation: None,
                await_reply: true,
                outbox: None,
                timeout_ms: DEFAULT_SEND_TIMEOUT_MS,
            }
        );
    }

    #[test]
    fn send_registry_rules_are_usage_errors() {
        let cases: &[&[&str]] = &[
            // --inbox and --registry are mutually exclusive.
            &[
                "send",
                "--inbox",
                "/mb",
                "--registry",
                "/reg",
                "--to",
                "r",
                "--body",
                "hi",
            ],
            // --registry with --body needs --to.
            &["send", "--registry", "/reg", "--body", "hi"],
            // --outbox is supplied by --registry.
            &[
                "send",
                "--registry",
                "/reg",
                "--to",
                "r",
                "--body",
                "hi",
                "--await",
                "--outbox",
                "/ob",
            ],
            // blank --registry.
            &["send", "--registry", "  ", "--to", "r", "--body", "hi"],
        ];
        for case in cases {
            assert!(
                matches!(parse_args(&argv(case)).unwrap_err(), BatonError::Usage(_)),
                "expected usage error for {case:?}"
            );
        }
    }

    #[test]
    fn parses_status_mailbox_and_registry_forms() {
        assert_eq!(
            parse_args(&argv(&["status", "--mailbox", "/tmp/mb"])).expect("parses"),
            Command::Status {
                mailbox: Some("/tmp/mb".to_string()),
                registry: None,
                role: None,
                max_runtime_ms: None,
            }
        );
        assert_eq!(
            parse_args(&argv(&[
                "status",
                "--registry=/tmp/reg.json",
                "--role=reviewer",
                "--max-runtime-ms=1200000",
            ]))
            .expect("parses"),
            Command::Status {
                mailbox: None,
                registry: Some("/tmp/reg.json".to_string()),
                role: Some("reviewer".to_string()),
                max_runtime_ms: Some(1_200_000),
            }
        );
    }

    #[test]
    fn status_argument_rules_are_usage_errors() {
        let cases: &[&[&str]] = &[
            &["status"],                                              // neither form
            &["status", "--mailbox", "/mb", "--role", "r"],           // mixed forms
            &["status", "--registry", "/reg"],                        // registry sans role
            &["status", "--role", "r"],                               // role sans registry
            &["status", "--mailbox", "/mb", "--max-runtime-ms", "0"], // zero threshold
        ];
        for case in cases {
            assert!(
                matches!(parse_args(&argv(case)).unwrap_err(), BatonError::Usage(_)),
                "expected usage error for {case:?}"
            );
        }
    }

    #[test]
    fn execute_status_renders_json_per_state() {
        let busy = MailboxStatus {
            state: MailboxState::Busy,
            queue_depth: 3,
            claim_age_ms: Some(4200),
        };
        let mut out = Vec::new();
        execute_status(&busy, 900_000, &mut out).expect("render");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"state\":\"busy\",\"queue_depth\":3,\"claim_age_ms\":4200,\"max_runtime_ms\":900000}\n"
        );

        let idle = MailboxStatus {
            state: MailboxState::IdleDone,
            queue_depth: 0,
            claim_age_ms: None,
        };
        let mut out = Vec::new();
        execute_status(&idle, 900_000, &mut out).expect("render");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"state\":\"idle-done\",\"queue_depth\":0,\"claim_age_ms\":null,\"max_runtime_ms\":900000}\n"
        );

        let stale = MailboxStatus {
            state: MailboxState::CrashedStale,
            queue_depth: 1,
            claim_age_ms: Some(999_999),
        };
        let mut out = Vec::new();
        execute_status(&stale, 60_000, &mut out).expect("render");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"state\":\"crashed-stale\",\"queue_depth\":1,\"claim_age_ms\":999999,\"max_runtime_ms\":60000}\n"
        );
    }

    #[test]
    fn build_send_envelope_from_body_applies_addressing_overrides() {
        let env = build_send_envelope(
            &SendSource::Body("hi".to_string()),
            Some("recipient".to_string()),
            Some("sender".to_string()),
            Some("c-1".to_string()),
        )
        .expect("builds");
        assert_eq!(env.to, "recipient");
        assert_eq!(env.from, "sender");
        assert_eq!(env.conversation_id, "c-1");
        assert_eq!(env.kind, MessageKind::Request);
        assert_eq!(env.body, "hi");
        assert!(
            env.message_id.starts_with("c-1-"),
            "id derived from conversation: {}",
            env.message_id
        );
    }

    #[test]
    fn build_send_envelope_reads_full_envelope_from_in() {
        let root = TempRoot::new("send-in");
        let path = root.path.join("env.json");
        std::fs::write(
            &path,
            serde_json::to_string(&send_request("m-in-1")).unwrap(),
        )
        .expect("write envelope");
        let env = build_send_envelope(
            &SendSource::Envelope(path.to_string_lossy().into_owned()),
            None,
            None,
            None,
        )
        .expect("reads");
        assert_eq!(env.message_id, "m-in-1");
        assert_eq!(env.body, "ping");
    }

    #[test]
    fn execute_send_delivers_and_prints_message_id() {
        let root = TempRoot::new("send-noawait");
        let env = send_request("m-send-1");
        let mut sink = RecordingSink::new();
        let mut out: Vec<u8> = Vec::new();

        execute_send(
            &root.path,
            None,
            &env,
            false,
            Duration::from_millis(0),
            Duration::from_millis(1),
            &mut sink,
            &mut out,
        )
        .expect("delivers");

        assert_eq!(
            json_files(&root.path.join("pending")),
            vec!["m-send-1.json".to_string()],
            "request landed in pending/"
        );
        assert_eq!(String::from_utf8(out).unwrap().trim(), "m-send-1");
        assert_eq!(sink.events.len(), 1, "only the send event");
        assert!(matches!(sink.events[0], ExchangeEvent::MessageSent { .. }));
    }

    #[test]
    fn execute_send_await_consumes_correlated_reply() {
        let root = TempRoot::new("send-await-ok");
        let outbox = root.path.join("outbox");
        let env = send_request("m-send-2");
        seed_reply(&outbox, "m-send-2", &reply_to("m-send-2"));

        let mut sink = RecordingSink::new();
        let mut out: Vec<u8> = Vec::new();
        execute_send(
            &root.path,
            Some(&outbox),
            &env,
            true,
            Duration::from_millis(500),
            Duration::from_millis(1),
            &mut sink,
            &mut out,
        )
        .expect("consumes reply");

        let printed = String::from_utf8(out).unwrap();
        let parsed: MessageEnvelope = serde_json::from_str(printed.trim()).expect("reply is json");
        assert_eq!(parsed.body, "pong");
        assert_eq!(parsed.in_reply_to.as_deref(), Some("m-send-2"));

        assert!(matches!(sink.events[0], ExchangeEvent::MessageSent { .. }));
        assert!(matches!(
            sink.events[1],
            ExchangeEvent::ReplyConsumed { .. }
        ));
        assert!(
            json_files(&outbox).is_empty(),
            "the claimed reply is renamed out of the outbox"
        );
    }

    #[test]
    fn execute_send_await_rejects_mismatched_reply() {
        let root = TempRoot::new("send-await-mismatch");
        let outbox = root.path.join("outbox");
        let env = send_request("m-send-3");
        // Reply is filed under the right key but answers a different request.
        seed_reply(&outbox, "m-send-3", &reply_to("some-other-id"));

        let mut sink = RecordingSink::new();
        let mut out: Vec<u8> = Vec::new();
        let err = execute_send(
            &root.path,
            Some(&outbox),
            &env,
            true,
            Duration::from_millis(500),
            Duration::from_millis(1),
            &mut sink,
            &mut out,
        )
        .expect_err("mismatch is a hard error");
        assert!(matches!(err, BatonError::Io(_)));
        // The send was recorded; the mismatched reply is not accepted.
        assert!(matches!(sink.events[0], ExchangeEvent::MessageSent { .. }));
        assert!(
            !sink
                .events
                .iter()
                .any(|e| matches!(e, ExchangeEvent::ReplyConsumed { .. })),
            "a mismatched reply is never recorded as consumed"
        );
    }

    #[test]
    fn execute_send_await_times_out_leaving_request_in_mailbox() {
        let root = TempRoot::new("send-timeout");
        let outbox = root.path.join("outbox");
        std::fs::create_dir_all(&outbox).expect("outbox");
        let env = send_request("m-send-4");

        let mut sink = NoopSink;
        let mut out: Vec<u8> = Vec::new();
        let err = execute_send(
            &root.path,
            Some(&outbox),
            &env,
            true,
            Duration::from_millis(10),
            Duration::from_millis(2),
            &mut sink,
            &mut out,
        )
        .expect_err("times out with no reply");
        assert!(matches!(err, BatonError::Io(_)));
        assert_eq!(
            json_files(&root.path.join("pending")),
            vec!["m-send-4.json".to_string()],
            "the request remains in the mailbox after a timeout"
        );
    }
}
