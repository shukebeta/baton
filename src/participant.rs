//! The participant seam: an envelope-in / envelope-out boundary.
//!
//! [`Participant`] is the A2A analog of [`crate::transport::Transport`]. Where a
//! `Transport` hides *which provider* answers a call, a `Participant` hides
//! *which participant* answers a `baton.message/v1` envelope — in-process here,
//! subprocess (M3b) or mailbox (M4) later. The boundary is envelope-only: a
//! participant holds no state shared with any other, so the M3c driver can hold
//! one abstractly and reach it the same way regardless of how it is realised.
//!
//! [`LocalParticipant`] is the first implementation: an in-process, LLM-backed
//! participant that is a system prompt + a [`Transport`]. It carries the same
//! request-envelope → response-envelope transformation the `baton exchange`
//! verb performs, so the two share one source of truth (the verb delegates
//! here); the CLI layers the `BATON_EVENT_LOG` side trail on top.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{BatonError, Result};
use crate::events::{ExchangeMeta, now_ms};
use crate::log::{Exchange, Outcome, RequestRecord};
use crate::mailbox;
use crate::message::{MessageEnvelope, MessageKind, WrappedExchange};
use crate::model::Prompt;
use crate::transport::Transport;

/// Answers a `baton.message/v1` request envelope with a response envelope.
///
/// Infallible by contract: a provider (or delivery) failure is a *delivered*
/// `kind: "error"` response, never a propagated `Err` — matching the
/// `baton exchange` delivered-error contract. Implementations share no mutable
/// state with one another; the envelope is the entire boundary.
pub trait Participant {
    /// Consumes a `request` envelope and returns the correlated response.
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope;
}

/// An in-process, LLM-backed participant: a system prompt + a [`Transport`].
///
/// The system prompt already lives in the transport's config (applied by the
/// Claude client), so a participant reply is exactly one provider exchange. The
/// response envelope preserves `conversation_id`, links `in_reply_to` to the
/// request, swaps addressing (`from`/`to`), and nests the `baton.exchange/v1`
/// record for the call it ran so the call — and its token usage — is observable
/// in-band. [`ExchangeMeta`] supplies the `model`/`base_url` stamped on that
/// nested record.
pub struct LocalParticipant<T: Transport> {
    transport: T,
    meta: ExchangeMeta,
}

impl<T: Transport> LocalParticipant<T> {
    /// Builds a participant over `transport`, stamping `meta` (`model` /
    /// `base_url`) onto the nested `baton.exchange/v1` record of each reply.
    pub fn new(transport: T, meta: ExchangeMeta) -> Self {
        Self { transport, meta }
    }
}

impl<T: Transport> Participant for LocalParticipant<T> {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        let request_ts = now_ms();
        let start = Instant::now();
        let result = self.transport.send(&Prompt::new(request.body.as_str()));
        let duration_ms = start.elapsed().as_millis() as u64;
        let outcome_ts = now_ms();

        let request_record = RequestRecord {
            ts_ms: request_ts,
            model: self.meta.model.clone(),
            base_url: self.meta.base_url.clone(),
            prompt: request.body.clone(),
        };

        let (kind, body, outcome) = match result {
            Ok(reply) => {
                let outcome = Outcome::Ok {
                    ts_ms: outcome_ts,
                    duration_ms,
                    reply: reply.text.clone(),
                    input_tokens: reply.usage.input_tokens,
                    output_tokens: reply.usage.output_tokens,
                };
                (MessageKind::Response, reply.text, outcome)
            }
            Err(err) => {
                let outcome = Outcome::Error {
                    ts_ms: outcome_ts,
                    duration_ms,
                    kind: err.kind().to_string(),
                    message: err.to_string(),
                };
                (MessageKind::Error, err.to_string(), outcome)
            }
        };

        // Addressing swaps: the reply is from the request's recipient, to its
        // sender.
        let mut response = MessageEnvelope::new(
            fresh_message_id(&request.conversation_id, outcome_ts),
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            body,
            outcome_ts,
        );
        response.in_reply_to = Some(request.message_id.clone());
        response.exchange = Some(WrappedExchange::new(Exchange {
            request: request_record,
            outcome,
        }));
        response
    }
}

/// A subprocess-backed participant: each reply is one `baton exchange` child.
///
/// Where [`LocalParticipant`] answers in-process, this impl reaches a *separate
/// OS process* — the honest "two independent agents, no shared state" boundary.
/// One [`respond`](Participant::respond) call spawns the program, writes the
/// request envelope to its stdin, reads one response envelope from its stdout,
/// and reaps it. The child is configured through its own environment (its own
/// `BATON_MODEL` / `BATON_SYSTEM_PROMPT` / credential vars), so it is a
/// genuinely independent Baton agent driven over the same envelope boundary.
///
/// The trait stays infallible. The delivered-error boundary (aligned with the
/// `baton exchange` verb) lives entirely in envelope terms:
///
/// - A child that **exits 0 with a well-formed envelope** is returned
///   *unchanged* — including a provider-failure `kind: "error"` envelope with
///   its nested `baton.exchange/v1` record, since that is exactly what the verb
///   emits on a delivered provider error.
/// - A child that **exits non-zero**, emits a **malformed or absent** envelope,
///   or **exceeds [`read_timeout`](Self::read_timeout)** is reconciled into a
///   *synthesized* delivered `kind: "error"` envelope with **no** nested record
///   — the parent observed no provider call it can vouch for (mirroring how
///   [`testing::ScriptedParticipant`] nests nothing when it ran no call).
pub struct SubprocessParticipant {
    program: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, String)>,
    read_timeout: Duration,
}

impl SubprocessParticipant {
    /// Builds a participant that spawns `program` with `args`, layering `envs`
    /// over the inherited environment, and waits at most `read_timeout` for the
    /// child's response envelope.
    ///
    /// `envs` are applied *on top of* the parent environment, so credentials
    /// flow through while `BATON_MODEL` / `BATON_SYSTEM_PROMPT` can differ — the
    /// layering that makes the child an independent agent rather than a clone.
    /// `read_timeout` must sit *above* the child's own `BATON_TIMEOUT_SECS` (the
    /// child's provider deadline); a shorter parent deadline would kill a
    /// slow-but-alive child and discard a real delivered error.
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        read_timeout: Duration,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            envs: envs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            read_timeout,
        }
    }

    /// Builds a participant that spawns *this* `baton` binary
    /// ([`std::env::current_exe`]) with the `exchange` verb — the production
    /// wiring. `envs` / `read_timeout` are as in [`new`](Self::new).
    pub fn for_current_exe(
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        read_timeout: Duration,
    ) -> Result<Self> {
        let program = std::env::current_exe().map_err(|err| {
            BatonError::Io(format!("could not resolve the current executable: {err}"))
        })?;
        Ok(Self::new(program, ["exchange"], envs, read_timeout))
    }

    /// Runs one child exchange, returning the parsed response envelope or an
    /// `Err` describing the machinery failure (non-zero exit, malformed/absent
    /// envelope, or read timeout). The infallible [`Participant::respond`]
    /// reconciles that `Err` into a delivered error envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<MessageEnvelope> {
        let payload = serde_json::to_string(request).map_err(|err| {
            BatonError::Io(format!("could not serialize request envelope: {err}"))
        })?;

        let stdout = capture_child_stdout(
            &self.program,
            &self.args,
            &self.envs,
            None,
            payload.as_bytes(),
            self.read_timeout,
        )?;

        if stdout.trim().is_empty() {
            return Err(BatonError::Decode(
                "child participant produced no response envelope".to_string(),
            ));
        }
        serde_json::from_str(&stdout).map_err(|err| {
            BatonError::Decode(format!(
                "child participant produced a malformed response envelope: {err}"
            ))
        })
    }
}

impl Participant for SubprocessParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(response) => response,
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }
}

/// A mailbox-backed participant: each reply is one round-trip over a file-mailbox.
///
/// Where [`SubprocessParticipant`] reaches an independent agent over pipes, this
/// impl reaches one over the *file-mailbox* (M4): a peer `baton serve` daemon.
/// One [`respond`](Participant::respond) call delivers the request into the
/// peer's inbox via the lock-free atomic path ([`mailbox::deliver_to`]) and then
/// polls the outbox for the correlated reply ([`mailbox::try_claim_response`],
/// keyed by the request id) until it appears or [`await_timeout`](Self::await_timeout)
/// elapses. It holds no lock — the peer daemon owns the single-instance lock —
/// so the driver is a *governed client* of a `serve` service, not a co-owner of
/// its mailbox.
///
/// The trait stays infallible; the delivered-error boundary is the same one
/// [`SubprocessParticipant`] draws, in envelope terms:
///
/// - A **peer-delivered reply** (whatever the outbox holds, correlated to the
///   request) is returned *unchanged* — including a peer `kind: "error"` whose
///   nested `baton.exchange/v1` record carries the peer's provider-call outcome,
///   since that is a delivered response the peer vouches for.
/// - A **machinery/transport failure** — delivery failed, no reply arrived
///   before the deadline, or the reply did not correlate — is reconciled into a
///   *synthesized* delivered `kind: "error"` envelope with **no** nested record
///   ([`synthesize_error_response`]): the driver obtained no peer provider-call
///   it can vouch for, mirroring how [`SubprocessParticipant`] synthesizes a
///   machinery failure.
///
/// This is what lets the `converse` trail distinguish "the peer answered with an
/// error" from "the driver stopped waiting": both are `kind: "error"`, but only
/// the former nests a `baton.exchange/v1` record. That predicate rests on the
/// peer nesting a record on every delivered reply — which holds for a `baton
/// serve` peer, whose in-process [`LocalParticipant`] always nests one. A future
/// peer that could deliver a recordless error would blur the two; the synthesized
/// timeout body naming the await-timeout is the tie-breaker for that case.
pub struct MailboxParticipant {
    /// Root of the peer's mailbox; the request is delivered to `<inbox>/pending/`.
    inbox: PathBuf,
    /// Directory the correlated reply is awaited from (the peer's outbox).
    outbox: PathBuf,
    /// Maximum time to await the correlated reply before synthesizing a timeout.
    await_timeout: Duration,
    /// Interval between outbox polls while awaiting the reply.
    poll_interval: Duration,
}

impl MailboxParticipant {
    /// Builds a participant that delivers requests to `<inbox>/pending/` and
    /// awaits their correlated replies from `outbox`, polling every
    /// `poll_interval` for at most `await_timeout` before synthesizing a
    /// transport-timeout error.
    ///
    /// `await_timeout` should be *generous* relative to a single `send --await`:
    /// each reply is a full provider turn run by the peer daemon, so a short
    /// deadline would synthesize a timeout while the peer is still answering.
    pub fn new(
        inbox: impl Into<PathBuf>,
        outbox: impl Into<PathBuf>,
        await_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            inbox: inbox.into(),
            outbox: outbox.into(),
            await_timeout,
            poll_interval,
        }
    }

    /// Delivers `request` and awaits its correlated reply, returning it, or an
    /// `Err` describing the machinery failure (delivery failed, await timed out,
    /// or the reply did not correlate). The infallible [`Participant::respond`]
    /// reconciles that `Err` into a synthesized delivered error envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<MessageEnvelope> {
        mailbox::deliver_to(&self.inbox, request)?;

        let deadline = Instant::now() + self.await_timeout;
        let reply = loop {
            if let Some(reply) = mailbox::try_claim_response(&self.outbox, &request.message_id)? {
                break reply;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(BatonError::Transport(format!(
                    "await timed out after {}ms without a correlated reply to {:?}",
                    self.await_timeout.as_millis(),
                    request.message_id
                )));
            }
            thread::sleep(self.poll_interval.min(remaining));
        };

        // The reply is keyed by the request id, but a mis-correlated envelope
        // filed under that key is a protocol error, not a reply to return.
        if reply.in_reply_to.as_deref() != Some(request.message_id.as_str()) {
            return Err(BatonError::Transport(format!(
                "reply {:?} has in_reply_to {:?}, expected {:?}",
                reply.message_id, reply.in_reply_to, request.message_id
            )));
        }
        Ok(reply)
    }
}

impl Participant for MailboxParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(response) => response,
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }
}

/// An external-agent-backed participant: each reply is one **headless run of a
/// full-tooled native agent CLI** (one that edits files and runs git/bash/MCP),
/// driven entirely through the mailbox with no tmux and no live TUI.
///
/// Where [`SubprocessParticipant`] reaches an independent *Baton* agent that
/// emits a complete `baton.message/v1` envelope on stdout, this impl reaches a
/// generic agent CLI that emits **free text** — its final result — which this
/// participant then **wraps** into a `kind: "response"` envelope (conversation
/// preserved, addressing swapped, `in_reply_to` linked). The agent is run with a
/// **git worktree as cwd** ([`cwd`](Self::cwd)); the request body is written to
/// its stdin, and its final stdout is captured as the reply body.
///
/// Cross-message state is the agent's own responsibility: it reconstructs
/// context across rounds from **durable artifacts** (the git branch/worktree it
/// shares run-to-run, the issue thread, prior mailbox history), not from an
/// in-memory session — headless-per-message is the model. This participant
/// guarantees only the substrate: the same `cwd` on every call, the request
/// delivered on stdin, and the final output captured.
///
/// The trait stays infallible; the delivered-error boundary is drawn in envelope
/// terms, mirroring the sibling impls:
///
/// - A run that **exits 0 with a non-empty extracted result** yields a
///   `kind: "response"` whose body is that result, with **no** nested
///   `baton.exchange/v1` record — an agent run is not a single provider call
///   Baton can vouch for, so it nests nothing (as [`testing::ScriptedParticipant`]
///   does when it runs no call). The result is isolated from the raw stdout by an
///   [`OutputAdapter`]: `Raw` takes the whole stdout, `Json` takes the final JSON
///   line's result field, so a streaming backend's tool/step chatter never leaks
///   into the reply body.
/// - A **machinery failure** — the agent could not be spawned, exited non-zero,
///   produced empty output, exceeded [`read_timeout`](Self::read_timeout), or (in
///   [`OutputAdapter::Json`] mode) emitted a final line the adapter could not
///   extract a string result from — is reconciled into a *synthesized* delivered
///   `kind: "error"` envelope ([`synthesize_error_response`]), its body naming the
///   failure.
pub struct ExternalAgentParticipant {
    /// The native agent CLI to run headless (e.g. `claude`).
    program: PathBuf,
    /// Fixed arguments passed on every run (headless/role flags), before stdin.
    args: Vec<String>,
    /// Environment layered over the inherited environment (the agent carries its
    /// own credentials / MCP config through here).
    envs: Vec<(String, String)>,
    /// Working directory for every run — the git worktree the agent acts in and
    /// reconstructs context from across rounds.
    cwd: PathBuf,
    /// How the reply body is isolated from the agent's raw stdout (whole stdout
    /// vs. the final JSON line's result field).
    output: OutputAdapter,
    /// Maximum time to await the agent's final output before synthesizing an
    /// error. Should be *generous*: a headless agent run is many tool calls, not
    /// one provider turn.
    read_timeout: Duration,
}

/// Isolates the agent's final *result* from its raw stdout.
///
/// A non-streaming backend (e.g. `claude -p`) prints only its final answer, so
/// the whole stdout *is* the result ([`Raw`](Self::Raw)). A streaming backend
/// (codex/copilot) interleaves tool/step chatter into stdout; run under its
/// `--output-format json`/`stream-json` convention its terminal line is a JSON
/// object carrying the result, which [`Json`](Self::Json) extracts by key so the
/// chatter above it is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputAdapter {
    /// The whole stdout is the reply body (the #68 default; correct when the
    /// backend prints only its final answer).
    Raw,
    /// The reply body is the string value at `result_key` in the final non-empty
    /// stdout line parsed as a JSON object. Anything else — no non-empty line, a
    /// line that is not a JSON object, an absent key, or a present-but-non-string
    /// value — is a machinery failure the caller reconciles into a delivered
    /// error, never a stringified-JSON surprise.
    Json { result_key: String },
}

impl OutputAdapter {
    /// Extracts the reply body from `stdout`, or an `Err` describing why no result
    /// could be isolated (only reachable in [`Json`](Self::Json) mode).
    fn extract(&self, stdout: &str) -> Result<String> {
        match self {
            OutputAdapter::Raw => Ok(stdout.to_string()),
            OutputAdapter::Json { result_key } => {
                let last = stdout.lines().rev().find(|line| !line.trim().is_empty());
                let Some(line) = last else {
                    return Err(BatonError::Decode(
                        "external agent produced no output line to extract a JSON result from"
                            .to_string(),
                    ));
                };
                let value: serde_json::Value = serde_json::from_str(line.trim()).map_err(|err| {
                    BatonError::Decode(format!(
                        "external agent's final output line is not a JSON object: {err}"
                    ))
                })?;
                match value.get(result_key) {
                    Some(serde_json::Value::String(s)) => Ok(s.clone()),
                    Some(_) => Err(BatonError::Decode(format!(
                        "external agent's JSON result field {result_key:?} is not a string"
                    ))),
                    None => Err(BatonError::Decode(format!(
                        "external agent's JSON output has no {result_key:?} result field"
                    ))),
                }
            }
        }
    }
}

impl ExternalAgentParticipant {
    /// Builds a participant that runs `program` with `args` (layering `envs` over
    /// the inherited environment) in `cwd`, feeding each request body on stdin,
    /// awaiting the agent's final stdout for at most `read_timeout`, and isolating
    /// the reply body from that stdout with `output`.
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        cwd: impl Into<PathBuf>,
        output: OutputAdapter,
        read_timeout: Duration,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            envs: envs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            cwd: cwd.into(),
            output,
            read_timeout,
        }
    }

    /// Runs one headless agent turn, returning the reply body (the agent's final
    /// result, isolated from raw stdout by the [`OutputAdapter`]) or an `Err`
    /// describing the machinery failure (spawn failure, non-zero exit, empty
    /// output, unextractable result, or read timeout). The infallible
    /// [`Participant::respond`] reconciles that `Err` into a delivered error
    /// envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<String> {
        let stdout = capture_child_stdout(
            &self.program,
            &self.args,
            &self.envs,
            Some(&self.cwd),
            request.body.as_bytes(),
            self.read_timeout,
        )?;

        if stdout.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced no output".to_string(),
            ));
        }
        let body = self.output.extract(&stdout)?;
        if body.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced an empty result".to_string(),
            ));
        }
        Ok(body)
    }
}

impl Participant for ExternalAgentParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(body) => {
                let ts_ms = now_ms();
                let mut response = MessageEnvelope::new(
                    fresh_message_id(&request.conversation_id, ts_ms),
                    request.conversation_id.clone(),
                    request.to.clone(),
                    request.from.clone(),
                    MessageKind::Response,
                    body,
                    ts_ms,
                );
                response.in_reply_to = Some(request.message_id.clone());
                response
            }
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }
}

/// Spawns `program` with `args`/`envs` (optionally in `cwd`), writes `payload`
/// to its stdin, and returns its stdout captured to EOF — the shared process
/// machinery behind [`SubprocessParticipant`] and [`ExternalAgentParticipant`].
///
/// stdout is drained on its own thread, started *before* the stdin write, so a
/// child that emits before consuming all its input cannot deadlock against a
/// full pipe buffer. A child that holds stdout open past `read_timeout` is
/// killed and reaped. Returns `Ok(stdout)` only when the child exits 0 (the
/// string may be empty — the caller decides what empty means); a spawn failure,
/// a non-zero exit (stderr folded into the message), a timeout, or an I/O error
/// is an `Err`.
fn capture_child_stdout(
    program: &Path,
    args: &[String],
    envs: &[(String, String)],
    cwd: Option<&Path>,
    payload: &[u8],
    read_timeout: Duration,
) -> Result<String> {
    let mut command = Command::new(program);
    command
        .args(args)
        .envs(envs.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let mut child = command.spawn().map_err(|err| {
        BatonError::Io(format!("could not spawn child process {program:?}: {err}"))
    })?;

    // Drain stdout on its own thread, started *before* writing stdin, so a child
    // that emits before consuming all its input cannot deadlock with us on a
    // full pipe buffer.
    let mut stdout = child.stdout.take().expect("child stdout is piped");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = String::new();
        let result = stdout.read_to_string(&mut buf).map(|_| buf);
        // A dropped receiver (the parent already timed out) is expected.
        let _ = tx.send(result);
    });

    // Write the payload, then drop stdin so the child sees EOF.
    {
        let mut stdin = child.stdin.take().expect("child stdin is piped");
        stdin
            .write_all(payload)
            .map_err(|err| BatonError::Io(format!("could not write to child stdin: {err}")))?;
    }

    match rx.recv_timeout(read_timeout) {
        Ok(read_result) => {
            let stdout = read_result
                .map_err(|err| BatonError::Io(format!("could not read child stdout: {err}")))?;
            let status = child
                .wait()
                .map_err(|err| BatonError::Io(format!("could not reap child process: {err}")))?;
            if !status.success() {
                let stderr = read_stderr(&mut child);
                let detail = if stderr.trim().is_empty() {
                    String::new()
                } else {
                    format!(": {}", stderr.trim())
                };
                return Err(BatonError::Transport(format!(
                    "child process exited with {status}{detail}"
                )));
            }
            Ok(stdout)
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            // The child is still holding stdout open past the deadline; kill and
            // reap it before surfacing the timeout.
            let _ = child.kill();
            let _ = child.wait();
            Err(BatonError::Transport(format!(
                "child process exceeded the {read_timeout:?} read timeout"
            )))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(BatonError::Transport(
                "child process stdout reader terminated unexpectedly".to_string(),
            ))
        }
    }
}

/// Reads the child's stderr to a string, best-effort, for enriching an
/// exit-failure message. Called only after the child has been reaped, so the
/// pipe is at EOF and the read is bounded.
fn read_stderr(child: &mut Child) -> String {
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf);
    }
    buf
}

/// Builds a delivered `kind: "error"` envelope for a machinery failure,
/// correlated to `request` (conversation preserved, addressing swapped,
/// `in_reply_to` linked) with **no** nested `baton.exchange/v1` record — the
/// parent ran no provider call of its own to record.
fn synthesize_error_response(request: &MessageEnvelope, message: &str) -> MessageEnvelope {
    let ts_ms = now_ms();
    let mut response = MessageEnvelope::new(
        fresh_message_id(&request.conversation_id, ts_ms),
        request.conversation_id.clone(),
        request.to.clone(),
        request.from.clone(),
        MessageKind::Error,
        message.to_string(),
        ts_ms,
    );
    response.in_reply_to = Some(request.message_id.clone());
    response
}

/// Builds a fresh `message_id` for a response without adding a dependency.
///
/// Derived from the conversation id and the response timestamp: an in-process
/// participant emits one response per request, so a collision is impossible, and
/// `baton.message/v1` places no format constraint on the id beyond uniqueness.
fn fresh_message_id(conversation_id: &str, ts_ms: u64) -> String {
    format!("{conversation_id}-r-{ts_ms}")
}

/// Test-only participant doubles, reusable across the crate's unit tests.
///
/// Lives here (not in a `#[cfg(test)] mod tests`) so a future driver module's
/// unit tests can reach [`ScriptedParticipant`] as
/// `crate::participant::testing::ScriptedParticipant`. Compiled only under
/// `cargo test`, so nothing ships in the release binary.
#[cfg(test)]
pub mod testing {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use super::Participant;
    use crate::log::{Exchange, Outcome, RequestRecord};
    use crate::message::{MessageEnvelope, MessageKind, WrappedExchange};

    /// Builds a reply correlated to `request` with a deterministic id/timestamp
    /// (so tests need no wall clock): preserved `conversation_id`, `in_reply_to`
    /// set, and addressing swapped — the reply is from the request's recipient,
    /// to its sender. Shared by every fake here so they correlate identically to
    /// [`super::LocalParticipant`].
    fn correlated_reply(
        request: &MessageEnvelope,
        kind: MessageKind,
        body: impl Into<String>,
    ) -> MessageEnvelope {
        let mut response = MessageEnvelope::new(
            format!("{}-r-{}", request.conversation_id, request.message_id),
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            body,
            request.ts_ms + 1,
        );
        response.in_reply_to = Some(request.message_id.clone());
        response
    }

    /// A [`Participant`] that replies from a scripted queue with no network.
    ///
    /// Each `respond` pops the next scripted body and wraps it in a
    /// `kind: "response"` envelope correlated to the request. Unlike
    /// [`super::LocalParticipant`] it nests no `baton.exchange/v1` record — it
    /// runs no provider call. An exhausted queue yields a `kind: "error"`
    /// envelope so a driver test sees a well-formed reply rather than a panic.
    pub struct ScriptedParticipant {
        replies: RefCell<VecDeque<String>>,
    }

    impl ScriptedParticipant {
        /// Builds a participant that answers with `replies`, in order.
        pub fn new(replies: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self {
                replies: RefCell::new(replies.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl Participant for ScriptedParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            match self.replies.borrow_mut().pop_front() {
                Some(body) => correlated_reply(request, MessageKind::Response, body),
                None => correlated_reply(request, MessageKind::Error, "no scripted reply"),
            }
        }
    }

    /// A [`Participant`] that always replies with the same body and never stops
    /// on its own — the shape a turn-cap guarantee test needs (only the cap can
    /// end it). Optionally carries nested token usage so a token-budget test can
    /// accumulate a running total.
    pub struct LoopingParticipant {
        body: String,
        usage: Option<(u64, u64)>,
    }

    impl LoopingParticipant {
        /// A looping participant whose replies nest no usage (contribute zero to
        /// a token budget).
        pub fn new(body: impl Into<String>) -> Self {
            Self {
                body: body.into(),
                usage: None,
            }
        }

        /// A looping participant whose replies nest `(input, output)` token
        /// usage on a `response_ok` record.
        pub fn with_usage(body: impl Into<String>, input: u64, output: u64) -> Self {
            Self {
                body: body.into(),
                usage: Some((input, output)),
            }
        }
    }

    impl Participant for LoopingParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            let mut reply = correlated_reply(request, MessageKind::Response, self.body.clone());
            if let Some((input, output)) = self.usage {
                reply.exchange = Some(WrappedExchange::new(Exchange {
                    request: RequestRecord {
                        ts_ms: request.ts_ms,
                        model: "fake-model".to_string(),
                        base_url: "fake-base-url".to_string(),
                        prompt: request.body.clone(),
                    },
                    outcome: Outcome::Ok {
                        ts_ms: request.ts_ms + 1,
                        duration_ms: 0,
                        reply: self.body.clone(),
                        input_tokens: Some(input),
                        output_tokens: Some(output),
                    },
                }));
            }
            reply
        }
    }

    /// A [`Participant`] that always emits a `kind: "done"` reply — the
    /// unilateral-completion terminal condition, unreachable from today's real
    /// participants (which emit only `response`/`error`).
    pub struct DoneParticipant;

    impl Participant for DoneParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            correlated_reply(request, MessageKind::Done, "done")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::ScriptedParticipant;
    use super::*;
    use crate::config::{BatonConfig, Credential, DEFAULT_MAX_TOKENS};
    use crate::error::Result;
    use crate::transport::claude::ClaudeClient;
    use crate::transport::http::{HttpClient, HttpResponse};
    use std::time::Duration;

    /// A fake [`HttpClient`] returning a canned status + body, so a
    /// [`ClaudeClient`] can be driven without a network — mirroring the fake in
    /// `transport::claude`'s own tests.
    struct FakeHttp {
        status: u16,
        body: String,
    }

    impl HttpClient for FakeHttp {
        fn post_json(
            &self,
            _url: &str,
            _headers: &[(&str, &str)],
            _body: &str,
        ) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }

    fn test_meta() -> ExchangeMeta {
        ExchangeMeta {
            model: "claude-test-model".to_string(),
            base_url: "https://api.anthropic.com".to_string(),
        }
    }

    fn test_config() -> BatonConfig {
        BatonConfig {
            credential: Credential::ApiKey("secret-key".to_string()),
            base_url: "https://api.anthropic.com".to_string(),
            model: "claude-test-model".to_string(),
            timeout: Duration::from_secs(60),
            max_tokens: DEFAULT_MAX_TOKENS,
            system_prompt: None,
        }
    }

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

    /// A `ClaudeClient`-backed participant (as production uses) turns a request
    /// envelope into a `kind: "response"` reply correlated to the request, with
    /// the provider call nested in-band.
    #[test]
    fn local_participant_builds_response_envelope_correlated_to_request() {
        let body = r#"{"content": [{"type": "text", "text": "four"}]}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 200,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());
        let request = request_envelope();

        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "four");
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps: reply is from the request's recipient, to its sender.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert_ne!(response.message_id, request.message_id);

        let wrapped = response
            .exchange
            .as_ref()
            .expect("wrapped exchange present");
        assert_eq!(wrapped.schema, crate::events::SCHEMA);
        match &wrapped.exchange.outcome {
            Outcome::Ok { reply, .. } => assert_eq!(reply, "four"),
            other => panic!("expected Ok outcome, got {other:?}"),
        }
        assert_eq!(wrapped.exchange.request.prompt, "what is 2+2?");
        assert_eq!(wrapped.exchange.request.model, "claude-test-model");
    }

    /// Reported token usage rides along on the nested `baton.exchange/v1` record.
    #[test]
    fn local_participant_wraps_reported_token_usage() {
        let body = r#"{"content": [{"type": "text", "text": "hi"}], "usage": {"input_tokens": 7, "output_tokens": 11}}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 200,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());

        let response = participant.respond(&request_envelope());

        match &response.exchange.expect("wrapped").exchange.outcome {
            Outcome::Ok {
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, Some(7));
                assert_eq!(*output_tokens, Some(11));
            }
            other => panic!("expected Ok outcome, got {other:?}"),
        }
    }

    /// A provider failure is a *delivered* `kind: "error"` envelope, never a
    /// propagated error — and the nested outcome carries the machine kind.
    #[test]
    fn local_participant_delivers_error_envelope_on_provider_failure() {
        let body = r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 401,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());

        let response = participant.respond(&request_envelope());

        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        assert_eq!(response.conversation_id, "conv-42");
        assert!(
            response.body.contains("invalid x-api-key"),
            "error body carries the failure description: {}",
            response.body
        );
        match &response
            .exchange
            .expect("wrapped failed exchange")
            .exchange
            .outcome
        {
            Outcome::Error { kind, .. } => assert_eq!(kind, "auth"),
            other => panic!("expected Error outcome, got {other:?}"),
        }
    }

    /// The scripted fake answers a driver's requests in order, correlated to each
    /// request, with no provider call (no nested exchange) — the shape M3c's
    /// driver tests consume.
    #[test]
    fn scripted_participant_answers_in_order_correlated_to_each_request() {
        let participant = ScriptedParticipant::new(["first", "second"]);

        let req1 = request_envelope();
        let resp1 = participant.respond(&req1);
        assert_eq!(resp1.kind, MessageKind::Response);
        assert_eq!(resp1.body, "first");
        assert_eq!(resp1.in_reply_to.as_deref(), Some("m-req-1"));
        assert_eq!(resp1.from, "agent-b");
        assert_eq!(resp1.to, "agent-a");
        assert!(
            resp1.exchange.is_none(),
            "scripted fake runs no provider call"
        );

        let mut req2 = request_envelope();
        req2.message_id = "m-req-2".to_string();
        let resp2 = participant.respond(&req2);
        assert_eq!(resp2.body, "second");
        assert_eq!(resp2.in_reply_to.as_deref(), Some("m-req-2"));

        // Queue exhausted → a well-formed delivered error, not a panic.
        let resp3 = participant.respond(&request_envelope());
        assert_eq!(resp3.kind, MessageKind::Error);
    }

    // -- SubprocessParticipant --------------------------------------------
    //
    // These drive the impl against `sh -c` stub programs — no live provider,
    // no `baton` binary — so each delivered-response / machinery-failure path
    // is exercised deterministically. `cat >/dev/null` in each stub consumes
    // the request from stdin so the child never dies on a broken pipe.

    /// Builds a subprocess participant that runs `script` under `sh -c`, passing
    /// `STUB_OUT` through as an env override the script can echo.
    fn stub(script: &str, stub_out: &str, read_timeout: Duration) -> SubprocessParticipant {
        SubprocessParticipant::new("sh", ["-c", script], [("STUB_OUT", stub_out)], read_timeout)
    }

    /// A child that exits 0 emitting a well-formed envelope has that envelope
    /// returned unchanged.
    #[test]
    fn subprocess_returns_child_envelope_unchanged_on_success() {
        let mut child_reply = MessageEnvelope::new(
            "child-resp-1",
            "conv-42",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "four",
            1_700_000_000_001,
        );
        child_reply.in_reply_to = Some("m-req-1".to_string());
        let json = serde_json::to_string(&child_reply).expect("serializes");

        let participant = stub(
            "cat >/dev/null; printf %s \"$STUB_OUT\"",
            &json,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());

        // Returned verbatim — the child, not the parent, owns correlation here.
        assert_eq!(response, child_reply);
    }

    /// A child that exits 0 with a `kind: "error"` envelope (a delivered
    /// provider failure) is passed through unchanged, nested record and all —
    /// it is a delivered response, not a machinery failure.
    #[test]
    fn subprocess_passes_through_delivered_error_envelope() {
        let mut child_error = MessageEnvelope::new(
            "child-err-1",
            "conv-42",
            "agent-b",
            "agent-a",
            MessageKind::Error,
            "invalid x-api-key",
            1_700_000_000_002,
        );
        child_error.in_reply_to = Some("m-req-1".to_string());
        child_error.exchange = Some(WrappedExchange::new(Exchange {
            request: RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-test-model".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "what is 2+2?".to_string(),
            },
            outcome: Outcome::Error {
                ts_ms: 1_700_000_000_002,
                duration_ms: 2,
                kind: "auth".to_string(),
                message: "invalid x-api-key".to_string(),
            },
        }));
        let json = serde_json::to_string(&child_error).expect("serializes");

        let participant = stub(
            "cat >/dev/null; printf %s \"$STUB_OUT\"",
            &json,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());

        // Unchanged: still an error envelope carrying the child's nested record.
        assert_eq!(response, child_error);
        assert_eq!(response.kind, MessageKind::Error);
        assert!(response.exchange.is_some(), "nested record preserved");
    }

    /// Asserts a synthesized machinery-failure envelope: a `kind: "error"`
    /// correlated to the request, with **no** nested provider record.
    fn assert_synthesized_error(response: &MessageEnvelope) {
        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps, just like a delivered reply.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "a machinery failure nests no provider record"
        );
    }

    /// A child that exits non-zero yields a synthesized delivered error.
    #[test]
    fn subprocess_synthesizes_error_on_nonzero_exit() {
        let participant = stub(
            "cat >/dev/null; echo boom >&2; exit 3",
            "",
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("boom") || response.body.contains("exit"),
            "body describes the child failure: {}",
            response.body
        );
    }

    /// A child that exits 0 but emits non-JSON yields a synthesized error.
    #[test]
    fn subprocess_synthesizes_error_on_malformed_stdout() {
        let participant = stub(
            "cat >/dev/null; printf 'not an envelope'",
            "",
            Duration::from_secs(5),
        );
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    /// A child that exits 0 with empty stdout yields a synthesized error.
    #[test]
    fn subprocess_synthesizes_error_on_absent_envelope() {
        let participant = stub("cat >/dev/null", "", Duration::from_secs(5));
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    /// A child that holds stdout open past the read timeout is killed and
    /// yields a synthesized error, without hanging the parent.
    #[test]
    fn subprocess_synthesizes_error_on_read_timeout() {
        // `sleep 30` keeps stdout open; the 150ms parent deadline fires first.
        let participant = stub("cat >/dev/null; sleep 30", "", Duration::from_millis(150));
        let response = participant.respond(&request_envelope());
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("timeout"),
            "body names the timeout: {}",
            response.body
        );
    }

    /// A program that cannot be spawned at all yields a synthesized error, not
    /// a panic.
    #[test]
    fn subprocess_synthesizes_error_when_program_missing() {
        let participant = SubprocessParticipant::new(
            "baton-no-such-program-xyz",
            std::iter::empty::<String>(),
            std::iter::empty::<(String, String)>(),
            Duration::from_secs(5),
        );
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    // -- ExternalAgentParticipant -----------------------------------------
    //
    // These drive the impl against `sh -c` stub programs in a tempdir cwd — no
    // real agent, no network, no API key. Each stub `cat`s its stdin (the
    // request body) so it never dies on a broken pipe, then acts in cwd and/or
    // prints its free-text "final result". This proves the machinery: stdin
    // delivery, the cwd side effect, the free-text→envelope wrap, the two-round
    // continuity substrate (shared cwd), and every synthesized-error path. The
    // real cross-context proof (an agent reconstructing from a git branch + the
    // issue thread) is `scripts/external-agent-proof.sh`, run manually.

    /// Builds an external-agent participant running `script` under `sh -c` in
    /// `cwd`, with no env overrides and the `Raw` output adapter (whole stdout).
    fn external_agent(
        script: &str,
        cwd: &std::path::Path,
        read_timeout: Duration,
    ) -> ExternalAgentParticipant {
        external_agent_with_output(script, cwd, OutputAdapter::Raw, read_timeout)
    }

    /// Builds an external-agent participant running `script` under `sh -c` in
    /// `cwd`, with the chosen `output` adapter — for exercising streaming-result
    /// extraction against a stub that emits chatter + a final result.
    fn external_agent_with_output(
        script: &str,
        cwd: &std::path::Path,
        output: OutputAdapter,
        read_timeout: Duration,
    ) -> ExternalAgentParticipant {
        ExternalAgentParticipant::new(
            "sh",
            ["-c", script],
            std::iter::empty::<(String, String)>(),
            cwd,
            output,
            read_timeout,
        )
    }

    /// A request envelope with a chosen id/body (agent-a → agent-b), so a
    /// two-round test can address distinct payloads.
    fn request_with_body(id: &str, body: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            id,
            "conv-42",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            body,
            1_700_000_000_000,
        )
    }

    /// A headless run that exits 0 with free-text stdout has that text wrapped
    /// into a `kind: "response"` correlated to the request (no nested record),
    /// the request body arrives on the agent's stdin, and the agent's cwd side
    /// effect lands in the worktree.
    #[test]
    fn external_agent_wraps_stdout_and_produces_cwd_side_effect() {
        let dir = TempDir::new("ext-ok");
        // The stub records its stdin to a file in cwd (proving stdin delivery +
        // an observable side effect), then prints its free-text result.
        let participant = external_agent(
            "cat > round1.txt; printf 'edited round1.txt and committed'",
            &dir.path,
            Duration::from_secs(5),
        );
        let request = request_with_body("m-req-1", "please edit round1.txt");

        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "edited round1.txt and committed");
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps: reply is from the request's recipient, to its sender.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "an agent run is not one provider call, so it nests no record"
        );
        // The request body was delivered on the agent's stdin...
        let stdin_seen = std::fs::read_to_string(dir.path.join("round1.txt")).expect("side effect");
        assert_eq!(stdin_seen, "please edit round1.txt");
    }

    /// Two sequential runs over the *same cwd*: the second run observes the
    /// first's durable artifact — the continuity substrate a headless-per-message
    /// agent relies on to reconstruct context across rounds.
    #[test]
    fn external_agent_two_rounds_share_cwd_for_continuity() {
        let dir = TempDir::new("ext-continuity");

        // Round 1 appends its payload to a durable ledger in the worktree.
        let round1 = external_agent(
            "cat >> ledger.txt; printf 'r1 done'",
            &dir.path,
            Duration::from_secs(5),
        );
        let resp1 = round1.respond(&request_with_body("m-req-1", "ROUND-ONE-PAYLOAD"));
        assert_eq!(resp1.body, "r1 done");

        // Round 2 (fresh headless process, same cwd) reads the ledger back —
        // seeing round 1's artifact proves cross-round continuity via durable
        // state, not an in-memory session.
        let round2 = external_agent(
            "cat >> ledger.txt; cat ledger.txt",
            &dir.path,
            Duration::from_secs(5),
        );
        let resp2 = round2.respond(&request_with_body("m-req-2", "ROUND-TWO-PAYLOAD"));

        assert_eq!(resp2.kind, MessageKind::Response);
        assert_eq!(resp2.in_reply_to.as_deref(), Some("m-req-2"));
        assert!(
            resp2.body.contains("ROUND-ONE-PAYLOAD"),
            "round 2 reconstructed round 1's durable artifact: {}",
            resp2.body
        );
        assert!(resp2.body.contains("ROUND-TWO-PAYLOAD"));
    }

    /// An agent that exits non-zero yields a synthesized delivered error naming
    /// the failure.
    #[test]
    fn external_agent_synthesizes_error_on_nonzero_exit() {
        let dir = TempDir::new("ext-nonzero");
        let participant = external_agent(
            "cat >/dev/null; echo boom >&2; exit 3",
            &dir.path,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("boom") || response.body.contains("exit"),
            "body describes the agent failure: {}",
            response.body
        );
    }

    /// An agent that exits 0 with empty stdout yields a synthesized error —
    /// there is no final result to deliver.
    #[test]
    fn external_agent_synthesizes_error_on_empty_output() {
        let dir = TempDir::new("ext-empty");
        let participant = external_agent("cat >/dev/null", &dir.path, Duration::from_secs(5));
        assert_synthesized_error(&participant.respond(&request_with_body("m-req-1", "go")));
    }

    /// An agent that holds stdout open past the read timeout is killed and
    /// yields a synthesized error naming the timeout, without hanging the parent.
    #[test]
    fn external_agent_synthesizes_error_on_read_timeout() {
        let dir = TempDir::new("ext-timeout");
        let participant = external_agent(
            "cat >/dev/null; sleep 30",
            &dir.path,
            Duration::from_millis(150),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("timeout"),
            "body names the timeout: {}",
            response.body
        );
    }

    /// A **streaming** backend that interleaves tool/step chatter on stdout and
    /// prints its final answer as a terminal JSON line yields a reply whose body
    /// is **only** that answer — the chatter is excluded by the `Json` adapter.
    #[test]
    fn external_agent_json_adapter_excludes_streaming_chatter() {
        let dir = TempDir::new("ext-json");
        // The stub mimics a streaming agent: tool/step chatter lines, then the
        // final result as a JSON object on the last line (the `--output-format
        // json` convention). Only the `result` field must reach the reply body.
        let participant = external_agent_with_output(
            "cat >/dev/null; \
             printf '[tool] reading files\\n'; \
             printf '[step] editing notes.md\\n'; \
             printf '{\"type\":\"result\",\"result\":\"edited notes.md and committed\"}\\n'",
            &dir.path,
            OutputAdapter::Json {
                result_key: "result".to_string(),
            },
            Duration::from_secs(5),
        );

        let response = participant.respond(&request_with_body("m-req-1", "please edit notes.md"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "edited notes.md and committed");
        assert!(
            !response.body.contains("[tool]") && !response.body.contains("[step]"),
            "streaming chatter must be excluded from the reply body: {}",
            response.body
        );
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        assert!(
            response.exchange.is_none(),
            "an agent run is not one provider call, so it nests no record"
        );
    }

    /// In `Json` mode a final line whose result field is missing or non-string is
    /// a machinery failure — a synthesized delivered error, never a stringified
    /// JSON body.
    #[test]
    fn external_agent_json_adapter_synthesizes_error_on_unextractable_result() {
        let dir = TempDir::new("ext-json-bad");
        // The result field is a nested object, not a string — must not be
        // stringified into the body.
        let participant = external_agent_with_output(
            "cat >/dev/null; printf '{\"result\":{\"nested\":true}}\\n'",
            &dir.path,
            OutputAdapter::Json {
                result_key: "result".to_string(),
            },
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("not a string"),
            "body names the extraction failure: {}",
            response.body
        );
    }

    // -- MailboxParticipant -----------------------------------------------
    //
    // These drive the impl against a tempdir mailbox — no live `serve`, no
    // network. A reply is seeded into the outbox exactly as `serve`'s
    // `deliver_response` would key it (by the request id), so the deliver +
    // await round-trip is exercised deterministically.

    use std::path::PathBuf;

    /// A unique self-cleaning temp directory, mirroring the idiom in
    /// `mailbox`'s own tests.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "baton-mailbox-participant-{}-{}-{tag}",
                std::process::id(),
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Seeds `reply` into `outbox` keyed by `request_id`, as `serve`'s
    /// `deliver_response` would, so `try_claim_response` finds it.
    fn seed_reply(outbox: &std::path::Path, request_id: &str, reply: &MessageEnvelope) {
        std::fs::create_dir_all(outbox).expect("create outbox");
        let json = serde_json::to_string(reply).expect("serialize reply");
        std::fs::write(outbox.join(format!("{request_id}.json")), json).expect("seed reply");
    }

    /// Builds a peer reply correlated to `request` (addressing swapped,
    /// `in_reply_to` linked), optionally nesting a provider-call record — the
    /// shape a `baton serve` peer's `LocalParticipant` delivers.
    fn peer_reply(request: &MessageEnvelope, kind: MessageKind, nested: bool) -> MessageEnvelope {
        let mut reply = MessageEnvelope::new(
            "peer-reply-id",
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            "pong",
            request.ts_ms + 1,
        );
        reply.in_reply_to = Some(request.message_id.clone());
        if nested {
            reply.exchange = Some(WrappedExchange::new(Exchange {
                request: RequestRecord {
                    ts_ms: request.ts_ms,
                    model: "peer-model".to_string(),
                    base_url: "https://peer".to_string(),
                    prompt: request.body.clone(),
                },
                outcome: Outcome::Ok {
                    ts_ms: request.ts_ms + 1,
                    duration_ms: 1,
                    reply: "pong".to_string(),
                    input_tokens: Some(3),
                    output_tokens: Some(5),
                },
            }));
        }
        reply
    }

    /// A seeded, correlated reply is delivered unchanged, and the request lands
    /// in the peer's `pending/` — the deliver + await round-trip.
    #[test]
    fn mailbox_returns_correlated_reply_and_delivers_request() {
        let dir = TempDir::new("ok");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let reply = peer_reply(&request, MessageKind::Response, true);
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        // Returned verbatim — the peer, not the driver, owns correlation.
        assert_eq!(response, reply);
        // The request was delivered to the peer's inbox.
        assert!(
            inbox.join("pending").join("m-req-1.json").exists(),
            "request delivered to <inbox>/pending/"
        );
    }

    /// A peer-delivered `kind: "error"` (carrying the peer's nested record) is
    /// passed through unchanged — a delivered response, not a machinery failure.
    #[test]
    fn mailbox_passes_through_peer_delivered_error() {
        let dir = TempDir::new("peer-err");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let reply = peer_reply(&request, MessageKind::Error, true);
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert!(
            response.exchange.is_some(),
            "a peer-delivered error nests the peer's record"
        );
    }

    /// No reply before the deadline yields a synthesized `kind: "error"` with no
    /// nested record and a body naming the await-timeout — the "driver stopped
    /// waiting" terminal, distinct from a peer-delivered error.
    #[test]
    fn mailbox_synthesizes_timeout_error_when_no_reply() {
        let dir = TempDir::new("timeout");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(10),
            Duration::from_millis(2),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps, like a delivered reply.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "a machinery/transport failure nests no record"
        );
        assert!(
            response.body.contains("timed out"),
            "body names the await-timeout: {}",
            response.body
        );
        // The request is left in the peer's inbox for a later drain.
        assert!(inbox.join("pending").join("m-req-1.json").exists());
    }

    /// A reply filed under the request key but answering a *different* request
    /// is rejected as a machinery failure, never returned as the correlated
    /// reply.
    #[test]
    fn mailbox_synthesizes_error_on_mis_correlated_reply() {
        let dir = TempDir::new("mismatch");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let mut reply = peer_reply(&request, MessageKind::Response, true);
        reply.in_reply_to = Some("some-other-id".to_string());
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert!(
            response.exchange.is_none(),
            "a mis-correlated reply is a machinery failure, nesting no record"
        );
    }
}
