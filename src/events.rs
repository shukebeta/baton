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

use serde::Serialize;

use crate::error::BatonError;

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
        kind: &'static str,
        /// Human-readable error description.
        message: String,
    },
}

impl ExchangeEvent {
    /// Builds the request event from the exchange metadata and prompt.
    pub fn request(ts_ms: u64, meta: &ExchangeMeta, prompt: &str) -> Self {
        ExchangeEvent::Request {
            schema: SCHEMA,
            ts_ms,
            model: meta.model.clone(),
            base_url: meta.base_url.clone(),
            prompt: prompt.to_string(),
        }
    }

    /// Builds the success outcome event.
    pub fn response_ok(ts_ms: u64, duration_ms: u64, reply: &str) -> Self {
        ExchangeEvent::ResponseOk {
            schema: SCHEMA,
            ts_ms,
            duration_ms,
            reply: reply.to_string(),
        }
    }

    /// Builds the failure outcome event from a [`BatonError`].
    pub fn response_error(ts_ms: u64, duration_ms: u64, err: &BatonError) -> Self {
        ExchangeEvent::ResponseError {
            schema: SCHEMA,
            ts_ms,
            duration_ms,
            kind: err.kind(),
            message: err.to_string(),
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
    fn response_ok_event_serializes_with_reply_and_duration() {
        let event = ExchangeEvent::response_ok(1_700_000_000_001, 42, "hi there");
        let value: Value = serde_json::to_value(&event).expect("serializes");
        assert_eq!(value["event"], "response_ok");
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["duration_ms"], 42);
        assert_eq!(value["reply"], "hi there");
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
    fn writer_sink_emits_one_flushed_line_per_event() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut sink = WriterSink::new(&mut buf);
            sink.record(&ExchangeEvent::request(1, &meta(), "q"))
                .expect("records request");
            sink.record(&ExchangeEvent::response_ok(2, 3, "a"))
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
}
