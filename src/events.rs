//! Structured, machine-readable exchange events for the single-turn path.
//!
//! Baton records each `ask` exchange as JSONL — one JSON object per line — so a
//! single request/response can be programmatically inspected or replayed, and
//! so failures are captured explicitly rather than lost in human-oriented
//! stdout/stderr output.
//!
//! The model is deliberately scoped to one user prompt and one assistant reply
//! (the epic's non-goals exclude multi-turn sessions). Three event kinds cover
//! the lifecycle: a [`ExchangeEvent::Request`] emitted before the call (it
//! carries everything needed to replay the exchange), and exactly one terminal
//! outcome — [`ExchangeEvent::ResponseOk`] or [`ExchangeEvent::ResponseError`].
//!
//! Recording is wired through an [`EventSink`]: [`NoopSink`] when disabled and
//! [`WriterSink`] when a sink is configured. Both the event types and the
//! writer are pure / `Write`-backed so they are unit-testable without a file or
//! the network — mirroring the testable seams elsewhere in the crate.

use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::BatonError;
use crate::model::TokenUsage;

/// Schema discriminator stamped on every event line.
///
/// Bump the version suffix if the shape changes incompatibly so downstream
/// consumers can branch on it.
pub const SCHEMA: &str = "baton.exchange/v1";

/// Replay-relevant metadata about an exchange, known before the call is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeMeta {
    /// Model id the request targets.
    pub model: String,
    /// Base URL the request is sent to.
    pub base_url: String,
}

/// One resolved identity field recorded on a session's opening marker: the
/// effective per-role config value and the layer that supplied it (the #80
/// reproducibility note). `value` is always a plain string — for the credential
/// entry it is the **reference** form (`oauth (env ALICE_TOKEN)`), never the
/// secret; [`crate::roles::Identity::to_fields`] passes it through as-is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityField {
    /// The friendly config key (`model`, `base_url`, `credential`, …).
    pub key: String,
    /// The resolved value, or `None` when unset with no built-in default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// The layer that supplied it (`env` / `role` / `defaults` / `default`).
    pub source: String,
}

/// A single lifecycle event for one `ask` exchange.
///
/// Serialized as JSONL: the `event` tag selects the kind and `schema` carries
/// [`SCHEMA`], so each line is self-describing when read in isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ExchangeEvent {
    /// Emitted before the provider call. Carries enough to replay the exchange.
    Request {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Model id the request targets.
        model: String,
        /// Base URL the request is sent to.
        base_url: String,
        /// The user prompt text.
        prompt: String,
        /// Session this turn belongs to, when emitted from `baton session`.
        ///
        /// Omitted on the single-turn `ask` path (that line stays byte-identical
        /// to before this field existed). Present, and equal to the run's
        /// [`SessionStart::session_id`](ExchangeEvent::SessionStart), on every
        /// session turn — the key that partitions a shared trail back into
        /// whole sessions.
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// Monotonic turn number within the session, starting at 0. Present on
        /// every human↔agent session turn alongside `session_id`; omitted on the
        /// `ask` path and on A2A seat turns (which order by file position, since
        /// [`crate::log::parse_sessions`] ignores `turn_index` for placement).
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_index: Option<u64>,
        /// A2A speaker (the peer who sent this turn's request). Present only on a
        /// `serve --role` seat turn; omitted on the `ask` / human↔agent paths.
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
        /// A2A recipient (the recording role's own address). Present only on a
        /// seat turn.
        #[serde(skip_serializing_if = "Option::is_none")]
        to: Option<String>,
        /// Conversation this seat turn belongs to. On the A2A path this equals
        /// `session_id` (the seat file is keyed on it), so the seat trail
        /// partitions into one session; omitted off the A2A path.
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation_id: Option<String>,
        /// Id of the request message this seat turn answered. Present only on a
        /// seat turn.
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
        /// The message this request replies to, correlating turns across the
        /// conversation. Present only on a seat turn that carried one.
        #[serde(skip_serializing_if = "Option::is_none")]
        in_reply_to: Option<String>,
    },
    /// Emitted when the provider call succeeds.
    ResponseOk {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Time spent in the provider call, milliseconds.
        duration_ms: u64,
        /// The assistant reply text.
        reply: String,
        /// Provider-reported input (prompt) tokens; omitted when unknown.
        #[serde(skip_serializing_if = "Option::is_none")]
        input_tokens: Option<u64>,
        /// Provider-reported output (completion) tokens; omitted when unknown.
        #[serde(skip_serializing_if = "Option::is_none")]
        output_tokens: Option<u64>,
    },
    /// Emitted by `baton send` when a request is delivered into a mailbox.
    ///
    /// A mailbox producer runs no provider call, so this records the *delivery*
    /// (addressing + ids) rather than a request/outcome pair. It rides the same
    /// [`SCHEMA`] trail as every other event; the read path
    /// ([`crate::log::parse_jsonl`]) skips its unknown tag, so `log show`/`replay`
    /// are unaffected.
    MessageSent {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Id of the delivered message.
        message_id: String,
        /// Conversation the message belongs to.
        conversation_id: String,
        /// Sender address.
        from: String,
        /// Recipient address.
        to: String,
    },
    /// Emitted by `baton send --await` when a correlated reply is consumed
    /// (renamed out of the outbox). Records the reply's ids for correlation.
    ReplyConsumed {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Id of the consumed reply message.
        message_id: String,
        /// The request `message_id` this reply answers.
        #[serde(skip_serializing_if = "Option::is_none")]
        in_reply_to: Option<String>,
        /// Conversation the reply belongs to.
        conversation_id: String,
    },
    /// Emitted when the provider call fails. The failure is recorded explicitly
    /// rather than surfaced only on stderr.
    ResponseError {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Time spent before the failure resolved, milliseconds.
        duration_ms: u64,
        /// Stable machine-readable error class (see [`BatonError::kind`]).
        ///
        /// Owned (not `&'static str`) so this event can be rebuilt from a
        /// recorded [`crate::log::Outcome`] via [`ExchangeEvent::from_outcome`],
        /// whose kind is an owned `String`; the wire value is identical.
        kind: String,
        /// Human-readable error description.
        message: String,
    },
    /// Emitted once by `baton session` at the start of a run, before any turn.
    ///
    /// Marks the opening boundary of a session on the trail and stamps the
    /// `session_id` every turn of the run carries, so a shared append log is
    /// unambiguously partitionable back into whole sessions. Rides the same
    /// [`SCHEMA`] trail; the read path ([`crate::log::parse_jsonl`]) skips its
    /// unknown tag, so `log show`/`replay` are unaffected.
    SessionStart {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Stable id for this session run, carried by every turn's `request`.
        session_id: String,
        /// The role whose home this session was recorded under, when framed from
        /// a `--role` context; omitted for a plain `BATON_EVENT_LOG` session.
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        /// The role's effective identity at session open (values + source), for
        /// reproducibility (#80). Omitted when no role framed the session.
        #[serde(skip_serializing_if = "Option::is_none")]
        identity: Option<Vec<IdentityField>>,
    },
    /// Emitted once by `baton session` on a clean exit (EOF / `/exit`), after the
    /// last turn. Marks the closing boundary of a session.
    ///
    /// A session killed mid-run never emits this — partitioning therefore keys on
    /// `session_id`, not on a matched start/end pair (see
    /// [`crate::log::parse_sessions`]).
    SessionEnd {
        /// Schema discriminator ([`SCHEMA`]).
        schema: &'static str,
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// The session this closes; equals the matching `SessionStart.session_id`.
        session_id: String,
        /// Count of turns whose `request` was emitted in this session.
        turns: u64,
    },
}

impl ExchangeEvent {
    /// Builds the request event from the exchange metadata and prompt.
    ///
    /// Carries no session framing — used by the single-turn `ask` path, whose
    /// line stays byte-identical to before session framing existed. Session
    /// turns use [`ExchangeEvent::session_request`].
    pub fn request(ts_ms: u64, meta: &ExchangeMeta, prompt: &str) -> Self {
        ExchangeEvent::Request {
            schema: SCHEMA,
            ts_ms,
            model: meta.model.clone(),
            base_url: meta.base_url.clone(),
            prompt: prompt.to_string(),
            session_id: None,
            turn_index: None,
            from: None,
            to: None,
            conversation_id: None,
            message_id: None,
            in_reply_to: None,
        }
    }

    /// Builds a session turn's request event, stamped with the run's
    /// `session_id` and this turn's `turn_index`.
    pub fn session_request(
        ts_ms: u64,
        meta: &ExchangeMeta,
        prompt: &str,
        session_id: &str,
        turn_index: u64,
    ) -> Self {
        ExchangeEvent::Request {
            schema: SCHEMA,
            ts_ms,
            model: meta.model.clone(),
            base_url: meta.base_url.clone(),
            prompt: prompt.to_string(),
            session_id: Some(session_id.to_string()),
            turn_index: Some(turn_index),
            from: None,
            to: None,
            conversation_id: None,
            message_id: None,
            in_reply_to: None,
        }
    }

    /// Builds an A2A **seat turn** request for a `serve --role` session file: the
    /// peer's request (`prompt` = the utterance received) stamped with the seat's
    /// addressing and correlation. `session_id` is set to `conversation_id` so the
    /// seat trail partitions into exactly one session under
    /// [`crate::log::parse_sessions`] (a `None` here would be read as a sessionless
    /// `ask` line and skipped). `turn_index` is left `None` — the seat trail orders
    /// by file position, which the reader honours.
    #[allow(clippy::too_many_arguments)]
    pub fn a2a_turn_request(
        ts_ms: u64,
        meta: &ExchangeMeta,
        prompt: &str,
        conversation_id: &str,
        from: &str,
        to: &str,
        message_id: &str,
        in_reply_to: Option<&str>,
    ) -> Self {
        ExchangeEvent::Request {
            schema: SCHEMA,
            ts_ms,
            model: meta.model.clone(),
            base_url: meta.base_url.clone(),
            prompt: prompt.to_string(),
            session_id: Some(conversation_id.to_string()),
            turn_index: None,
            from: Some(from.to_string()),
            to: Some(to.to_string()),
            conversation_id: Some(conversation_id.to_string()),
            message_id: Some(message_id.to_string()),
            in_reply_to: in_reply_to.map(str::to_string),
        }
    }

    /// Builds the session-start marker stamping the run's `session_id`, with no
    /// role framing (the plain `BATON_EVENT_LOG` session path).
    pub fn session_start(ts_ms: u64, session_id: &str) -> Self {
        ExchangeEvent::SessionStart {
            schema: SCHEMA,
            ts_ms,
            session_id: session_id.to_string(),
            role: None,
            identity: None,
        }
    }

    /// Builds the session-start marker for a `--role`-framed session, recording
    /// the role name and its effective identity (values + source) for
    /// reproducibility (#80).
    pub fn session_start_with_identity(
        ts_ms: u64,
        session_id: &str,
        role: &str,
        identity: Vec<IdentityField>,
    ) -> Self {
        ExchangeEvent::SessionStart {
            schema: SCHEMA,
            ts_ms,
            session_id: session_id.to_string(),
            role: Some(role.to_string()),
            identity: Some(identity),
        }
    }

    /// Builds the session-end marker, recording the run's `session_id` and the
    /// number of turns emitted.
    pub fn session_end(ts_ms: u64, session_id: &str, turns: u64) -> Self {
        ExchangeEvent::SessionEnd {
            schema: SCHEMA,
            ts_ms,
            session_id: session_id.to_string(),
            turns,
        }
    }

    /// Builds the success outcome event, carrying any reported token usage.
    pub fn response_ok(ts_ms: u64, duration_ms: u64, reply: &str, usage: &TokenUsage) -> Self {
        ExchangeEvent::ResponseOk {
            schema: SCHEMA,
            ts_ms,
            duration_ms,
            reply: reply.to_string(),
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        }
    }

    /// Builds the delivery event for a message posted by `baton send`.
    pub fn message_sent(ts_ms: u64, envelope: &crate::message::MessageEnvelope) -> Self {
        ExchangeEvent::MessageSent {
            schema: SCHEMA,
            ts_ms,
            message_id: envelope.message_id.clone(),
            conversation_id: envelope.conversation_id.clone(),
            from: envelope.from.clone(),
            to: envelope.to.clone(),
        }
    }

    /// Builds the consume event for a reply claimed by `baton send --await`.
    pub fn reply_consumed(ts_ms: u64, reply: &crate::message::MessageEnvelope) -> Self {
        ExchangeEvent::ReplyConsumed {
            schema: SCHEMA,
            ts_ms,
            message_id: reply.message_id.clone(),
            in_reply_to: reply.in_reply_to.clone(),
            conversation_id: reply.conversation_id.clone(),
        }
    }

    /// Builds the failure outcome event from a [`BatonError`].
    pub fn response_error(ts_ms: u64, duration_ms: u64, err: &BatonError) -> Self {
        ExchangeEvent::ResponseError {
            schema: SCHEMA,
            ts_ms,
            duration_ms,
            kind: err.kind().to_string(),
            message: err.to_string(),
        }
    }

    /// Builds the terminal outcome event from an already-recorded
    /// [`Outcome`](crate::log::Outcome).
    ///
    /// Lets a caller that already holds a completed `baton.exchange/v1` record —
    /// e.g. the nested record on an A2A response envelope — mirror it into the
    /// event trail without re-timing or re-deriving the call, so the trail and
    /// the in-band record stay one source of truth.
    pub fn from_outcome(outcome: &crate::log::Outcome) -> Self {
        match outcome {
            crate::log::Outcome::Ok {
                ts_ms,
                duration_ms,
                reply,
                input_tokens,
                output_tokens,
            } => ExchangeEvent::ResponseOk {
                schema: SCHEMA,
                ts_ms: *ts_ms,
                duration_ms: *duration_ms,
                reply: reply.clone(),
                input_tokens: *input_tokens,
                output_tokens: *output_tokens,
            },
            crate::log::Outcome::Error {
                ts_ms,
                duration_ms,
                kind,
                message,
            } => ExchangeEvent::ResponseError {
                schema: SCHEMA,
                ts_ms: *ts_ms,
                duration_ms: *duration_ms,
                kind: kind.clone(),
                message: message.clone(),
            },
        }
    }
}

/// Current wall-clock time as Unix epoch milliseconds.
///
/// Returns `0` for the (practically impossible) case of a clock before the
/// epoch rather than panicking — event recording must never abort a command.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Sink for exchange events.
///
/// Implementations persist or discard events; the orchestration code is written
/// against this trait so recording can be toggled without branching the
/// exchange logic.
pub trait EventSink {
    /// Records a single event. Returns an error only if persistence failed; the
    /// caller decides whether that is fatal (it is not, for the `ask` path).
    fn record(&mut self, event: &ExchangeEvent) -> io::Result<()>;
}

/// An [`EventSink`] that discards everything. Used when recording is disabled.
pub struct NoopSink;

impl EventSink for NoopSink {
    fn record(&mut self, _event: &ExchangeEvent) -> io::Result<()> {
        Ok(())
    }
}

/// An [`EventSink`] that writes one JSON object per line to a [`Write`].
///
/// Each event is flushed immediately so a consumer tailing the sink sees the
/// request line before the (possibly slow) response line.
pub struct WriterSink<W: Write> {
    writer: W,
}

impl<W: Write> WriterSink<W> {
    /// Creates a sink that writes JSONL to `writer`.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write> EventSink for WriterSink<W> {
    fn record(&mut self, event: &ExchangeEvent) -> io::Result<()> {
        let line = serde_json::to_string(event).map_err(io::Error::other)?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn meta() -> ExchangeMeta {
        ExchangeMeta {
            model: "claude-test-model".to_string(),
            base_url: "https://api.anthropic.com".to_string(),
        }
    }

    #[test]
    fn request_event_serializes_with_schema_and_replay_fields() {
        let event = ExchangeEvent::request(1_700_000_000_000, &meta(), "hello");
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "request");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["ts_ms"], 1_700_000_000_000u64);
        assert_eq!(value["model"], "claude-test-model");
        assert_eq!(value["base_url"], "https://api.anthropic.com");
        assert_eq!(value["prompt"], "hello");
    }

    #[test]
    fn response_ok_event_serializes_with_reply_duration_and_tokens() {
        let usage = TokenUsage {
            input_tokens: Some(12),
            output_tokens: Some(34),
        };
        let event = ExchangeEvent::response_ok(1_700_000_000_001, 42, "hi there", &usage);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "response_ok");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["duration_ms"], 42);
        assert_eq!(value["reply"], "hi there");
        assert_eq!(value["input_tokens"], 12);
        assert_eq!(value["output_tokens"], 34);
    }

    #[test]
    fn response_ok_event_omits_token_fields_when_usage_absent() {
        let event = ExchangeEvent::response_ok(1_700_000_000_001, 42, "hi", &TokenUsage::default());
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert!(
            value.get("input_tokens").is_none(),
            "absent usage must omit input_tokens, got: {value}"
        );
        assert!(
            value.get("output_tokens").is_none(),
            "absent usage must omit output_tokens, got: {value}"
        );
    }

    #[test]
    fn response_error_event_carries_machine_kind_and_message() {
        let err = BatonError::Auth("invalid x-api-key".to_string());
        let event = ExchangeEvent::response_error(1_700_000_000_002, 7, &err);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "response_error");
        assert_eq!(value["kind"], "auth");
        assert_eq!(value["duration_ms"], 7);
        assert_eq!(value["message"], err.to_string());
    }

    #[test]
    fn message_sent_event_serializes_with_addressing_and_ids() {
        use crate::message::{MessageEnvelope, MessageKind};
        let env = MessageEnvelope::new(
            "m-1",
            "conv-1",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "hi",
            1_700_000_000_000,
        );
        let event = ExchangeEvent::message_sent(1_700_000_000_000, &env);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "message_sent");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["message_id"], "m-1");
        assert_eq!(value["conversation_id"], "conv-1");
        assert_eq!(value["from"], "agent-a");
        assert_eq!(value["to"], "agent-b");
    }

    #[test]
    fn reply_consumed_event_serializes_with_correlation_ids() {
        use crate::message::{MessageEnvelope, MessageKind};
        let mut reply = MessageEnvelope::new(
            "r-1",
            "conv-1",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "hello",
            1_700_000_000_001,
        );
        reply.in_reply_to = Some("m-1".to_string());
        let event = ExchangeEvent::reply_consumed(1_700_000_000_001, &reply);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "reply_consumed");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["message_id"], "r-1");
        assert_eq!(value["in_reply_to"], "m-1");
        assert_eq!(value["conversation_id"], "conv-1");
    }

    #[test]
    fn writer_sink_emits_one_flushed_line_per_event() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut sink = WriterSink::new(&mut buf);
            sink.record(&ExchangeEvent::request(1, &meta(), "q"))
                .expect("records request");
            sink.record(&ExchangeEvent::response_ok(
                2,
                3,
                "a",
                &TokenUsage::default(),
            ))
            .expect("records response");
        }
        let text = String::from_utf8(buf).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "one line per event");

        // Each line is independently parseable JSON carrying the schema.
        for line in &lines {
            let value: Value = serde_json::from_str(line).expect("line is json");
            assert_eq!(value["schema"], SCHEMA);
        }
        assert_eq!(
            serde_json::from_str::<Value>(lines[0]).unwrap()["event"],
            "request"
        );
        assert_eq!(
            serde_json::from_str::<Value>(lines[1]).unwrap()["event"],
            "response_ok"
        );
    }

    #[test]
    fn request_event_omits_session_fields_on_ask_path() {
        let event = ExchangeEvent::request(1, &meta(), "q");
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert!(
            value.get("session_id").is_none(),
            "ask request must omit session_id, got: {value}"
        );
        assert!(
            value.get("turn_index").is_none(),
            "ask request must omit turn_index, got: {value}"
        );
    }

    #[test]
    fn session_request_event_carries_session_id_and_turn_index() {
        let event = ExchangeEvent::session_request(5, &meta(), "hi", "sess-1", 2);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "request");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["prompt"], "hi");
        assert_eq!(value["session_id"], "sess-1");
        assert_eq!(value["turn_index"], 2);
    }

    #[test]
    fn session_start_event_serializes_with_id() {
        let event = ExchangeEvent::session_start(7, "sess-1");
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "session_start");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["ts_ms"], 7);
        assert_eq!(value["session_id"], "sess-1");
    }

    #[test]
    fn session_end_event_serializes_with_id_and_turn_count() {
        let event = ExchangeEvent::session_end(9, "sess-1", 3);
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "session_end");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["ts_ms"], 9);
        assert_eq!(value["session_id"], "sess-1");
        assert_eq!(value["turns"], 3);
    }

    #[test]
    fn noop_sink_records_without_error() {
        let mut sink = NoopSink;
        sink.record(&ExchangeEvent::request(1, &meta(), "q"))
            .expect("noop never fails");
    }

    #[test]
    fn now_ms_is_after_a_known_recent_epoch() {
        // 2023-01-01T00:00:00Z in ms; the clock must be well past it.
        assert!(now_ms() > 1_672_531_200_000);
    }

    #[test]
    fn a2a_turn_request_sets_session_id_to_conversation_and_omits_turn_index() {
        // The linchpin (#82): the seat turn's `session_id` must equal
        // `conversation_id` so `parse_sessions` partitions the seat trail into one
        // session; a `None` session_id would be read as a sessionless `ask` line.
        let event = ExchangeEvent::a2a_turn_request(
            5,
            &meta(),
            "hi",
            "conv-1",
            "alice",
            "bob",
            "m-1",
            Some("m-0"),
        );
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "request");
        assert_eq!(value["session_id"], "conv-1");
        assert_eq!(value["conversation_id"], "conv-1");
        assert_eq!(value["from"], "alice");
        assert_eq!(value["to"], "bob");
        assert_eq!(value["message_id"], "m-1");
        assert_eq!(value["in_reply_to"], "m-0");
        assert!(
            value.get("turn_index").is_none(),
            "A2A seat turns order by file position, not turn_index: {value}"
        );
    }

    #[test]
    fn a2a_turn_request_omits_in_reply_to_when_absent() {
        let event = ExchangeEvent::a2a_turn_request(
            5,
            &meta(),
            "hi",
            "conv-1",
            "alice",
            "bob",
            "m-1",
            None,
        );
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert!(
            value.get("in_reply_to").is_none(),
            "absent in_reply_to must be omitted, got: {value}"
        );
    }

    #[test]
    fn session_start_with_identity_serializes_role_and_fields() {
        let event = ExchangeEvent::session_start_with_identity(
            7,
            "conv-1",
            "bob",
            vec![IdentityField {
                key: "model".to_string(),
                value: Some("claude-x".to_string()),
                source: "role".to_string(),
            }],
        );
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "session_start");
        assert_eq!(value["session_id"], "conv-1");
        assert_eq!(value["role"], "bob");
        assert_eq!(value["identity"][0]["key"], "model");
        assert_eq!(value["identity"][0]["value"], "claude-x");
        assert_eq!(value["identity"][0]["source"], "role");
    }

    #[test]
    fn bare_session_start_omits_role_and_identity() {
        let event = ExchangeEvent::session_start(7, "sess-1");
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert!(
            value.get("role").is_none() && value.get("identity").is_none(),
            "a plain session marker carries no role framing, got: {value}"
        );
    }

    #[test]
    fn bare_and_session_request_omit_a2a_correlation_fields() {
        // The `ask` / human↔agent lines stay byte-identical: the new A2A fields
        // are omitted entirely.
        for event in [
            ExchangeEvent::request(1, &meta(), "q"),
            ExchangeEvent::session_request(1, &meta(), "q", "sess-1", 0),
        ] {
            let value: Value = serde_json::to_value(&event).expect("serializes");
            for field in ["from", "to", "conversation_id", "message_id", "in_reply_to"] {
                assert!(
                    value.get(field).is_none(),
                    "{field} must be omitted off the A2A path, got: {value}"
                );
            }
        }
    }
}
