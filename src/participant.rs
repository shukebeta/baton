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
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{BatonError, Result};
use crate::events::{ExchangeMeta, now_ms};
use crate::log::{Exchange, Outcome, RequestRecord};
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

        let mut child = Command::new(&self.program)
            .args(&self.args)
            .envs(self.envs.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| {
                BatonError::Io(format!(
                    "could not spawn child participant {:?}: {err}",
                    self.program
                ))
            })?;

        // Drain stdout on its own thread, started *before* writing stdin, so a
        // child that emits before consuming all its input cannot deadlock with
        // us on a full pipe buffer.
        let mut stdout = child.stdout.take().expect("child stdout is piped");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = String::new();
            let result = stdout.read_to_string(&mut buf).map(|_| buf);
            // A dropped receiver (the parent already timed out) is expected.
            let _ = tx.send(result);
        });

        // Write the request, then drop stdin so the child sees EOF.
        {
            let mut stdin = child.stdin.take().expect("child stdin is piped");
            stdin
                .write_all(payload.as_bytes())
                .map_err(|err| BatonError::Io(format!("could not write to child stdin: {err}")))?;
        }

        match rx.recv_timeout(self.read_timeout) {
            Ok(read_result) => {
                let stdout = read_result
                    .map_err(|err| BatonError::Io(format!("could not read child stdout: {err}")))?;
                let status = child.wait().map_err(|err| {
                    BatonError::Io(format!("could not reap child participant: {err}"))
                })?;
                if !status.success() {
                    let stderr = read_stderr(&mut child);
                    let detail = if stderr.trim().is_empty() {
                        String::new()
                    } else {
                        format!(": {}", stderr.trim())
                    };
                    return Err(BatonError::Transport(format!(
                        "child participant exited with {status}{detail}"
                    )));
                }
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
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The child is still holding stdout open past the deadline; kill
                // and reap it before surfacing the timeout.
                let _ = child.kill();
                let _ = child.wait();
                Err(BatonError::Transport(format!(
                    "child participant exceeded the {:?} read timeout",
                    self.read_timeout
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(BatonError::Transport(
                    "child participant stdout reader terminated unexpectedly".to_string(),
                ))
            }
        }
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
    use crate::message::{MessageEnvelope, MessageKind};

    /// A [`Participant`] that replies from a scripted queue with no network.
    ///
    /// Each `respond` pops the next scripted body and wraps it in a
    /// `kind: "response"` envelope correlated to the request (preserved
    /// `conversation_id`, `in_reply_to` set, addressing swapped). Unlike
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
            let (kind, body) = match self.replies.borrow_mut().pop_front() {
                Some(body) => (MessageKind::Response, body),
                None => (MessageKind::Error, "no scripted reply".to_string()),
            };
            // Deterministic id/timestamp derived from the request, so tests need
            // no wall clock.
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
}
