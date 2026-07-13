//! The `baton.message/v1` peer-message envelope — the A2A lingua franca.
//!
//! [`crate::events`] / [`crate::log`] describe a single provider *call* (the
//! `baton.exchange/v1` trail). This module adds the distinct contract for a
//! *peer message*: who it is from/to, which conversation and turn it belongs
//! to, what kind of message it is, and its body. The exchange verb (M2) and the
//! driver (M3) share this envelope; this slice is the **contract only** — no
//! transport, no I/O verb, no driver, no mailbox addressing.
//!
//! ## Nesting over `baton.exchange/v1`
//!
//! An envelope is nested *over* the exchange trail: one peer message may wrap
//! zero-or-one provider-call record ([`MessageEnvelope::exchange`]). A message
//! that triggered an LLM call carries the resulting exchange as a
//! [`WrappedExchange`], which pairs the `baton.exchange/v1` schema discriminator
//! with the owned [`crate::log::Exchange`] value (its `request` + terminal
//! `outcome`). A message that triggered no call leaves the field `None`.
//!
//! ## Forward-compatibility
//!
//! Reads skip unknown fields (serde's default — these types deliberately do
//! **not** set `deny_unknown_fields`), matching the exchange trail. That is what
//! lets a later slice add fields — or a `kind` such as `notify`, intentionally
//! omitted here — without a schema break.

use serde::{Deserialize, Serialize};

use crate::log::Exchange;

/// Schema discriminator stamped on every envelope.
///
/// Bump the version suffix if the shape changes incompatibly so downstream
/// consumers can branch on it.
pub const SCHEMA: &str = "baton.message/v1";

/// What kind of peer message this envelope carries.
///
/// Serializes to the snake_case wire values `request` / `response` / `done` /
/// `error`. `notify` is intentionally omitted this slice (no producer or
/// consumer yet); the unknown-field/variant skip keeps adding it later a
/// non-breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// A message asking the peer to act (e.g. a prompt to relay).
    Request,
    /// A message answering a prior [`MessageKind::Request`].
    Response,
    /// A terminal marker: the conversation turn is complete.
    Done,
    /// A terminal marker: the turn failed.
    Error,
}

/// The provider-call record a message wraps, self-describing via its schema.
///
/// Pairs the `baton.exchange/v1` discriminator with the owned
/// [`Exchange`](crate::log::Exchange) value so the nesting is explicit on the
/// wire: `{"schema":"baton.exchange/v1","exchange":{"request":…,"outcome":…}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedExchange {
    /// Schema discriminator of the wrapped record ([`crate::events::SCHEMA`],
    /// `baton.exchange/v1`).
    pub schema: String,
    /// The wrapped provider call: its request paired with its terminal outcome.
    pub exchange: Exchange,
}

impl WrappedExchange {
    /// Wraps an [`Exchange`], stamping the `baton.exchange/v1` discriminator
    /// (owned copy of [`crate::events::SCHEMA`], the write-path constant).
    pub fn new(exchange: Exchange) -> Self {
        Self {
            schema: crate::events::SCHEMA.to_string(),
            exchange,
        }
    }
}

/// A single `baton.message/v1` peer message.
///
/// Constructed with [`MessageEnvelope::new`] for the common case (schema stamped,
/// no reply link, no wrapped exchange); the remaining fields are public so a
/// caller can set [`in_reply_to`](Self::in_reply_to) and
/// [`exchange`](Self::exchange) directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// Schema discriminator ([`SCHEMA`]).
    pub schema: String,
    /// Unique id of this message.
    pub message_id: String,
    /// Id of the conversation this message belongs to.
    pub conversation_id: String,
    /// Sender address.
    pub from: String,
    /// Recipient address.
    pub to: String,
    /// The `message_id` this message replies to, if any.
    pub in_reply_to: Option<String>,
    /// What kind of message this is.
    pub kind: MessageKind,
    /// The message body.
    pub body: String,
    /// Wall-clock emission time, Unix epoch milliseconds.
    pub ts_ms: u64,
    /// The provider call this message wrapped, if any (zero-or-one).
    pub exchange: Option<WrappedExchange>,
}

impl MessageEnvelope {
    /// Builds an envelope with the schema stamped, no reply link, and no wrapped
    /// exchange. Set [`in_reply_to`](Self::in_reply_to) /
    /// [`exchange`](Self::exchange) on the returned value when needed.
    pub fn new(
        message_id: impl Into<String>,
        conversation_id: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        kind: MessageKind,
        body: impl Into<String>,
        ts_ms: u64,
    ) -> Self {
        Self {
            schema: SCHEMA.to_string(),
            message_id: message_id.into(),
            conversation_id: conversation_id.into(),
            from: from.into(),
            to: to.into(),
            in_reply_to: None,
            kind,
            body: body.into(),
            ts_ms,
            exchange: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{Outcome, RequestRecord};
    use serde_json::Value;

    fn base() -> MessageEnvelope {
        MessageEnvelope::new(
            "m-1",
            "c-1",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "hello",
            1_700_000_000_000,
        )
    }

    fn wrapped() -> WrappedExchange {
        WrappedExchange::new(Exchange {
            request: RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-sonnet-4-6".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "hello".to_string(),
            },
            outcome: Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "hi there".to_string(),
            },
        })
    }

    /// Serialize → parse → equal, with no reply link and no wrapped exchange.
    #[test]
    fn round_trips_minimal_envelope() {
        let msg = base();
        let json = serde_json::to_string(&msg).expect("serializes");
        let back: MessageEnvelope = serde_json::from_str(&json).expect("parses");
        assert_eq!(msg, back);
    }

    /// The nullable `in_reply_to` round-trips in both its `Some` and `None`
    /// states.
    #[test]
    fn round_trips_in_reply_to_none_and_some() {
        let none = base();
        assert_eq!(none.in_reply_to, None);
        let back: MessageEnvelope =
            serde_json::from_str(&serde_json::to_string(&none).unwrap()).unwrap();
        assert_eq!(none, back);

        let mut some = base();
        some.in_reply_to = Some("m-0".to_string());
        let back: MessageEnvelope =
            serde_json::from_str(&serde_json::to_string(&some).unwrap()).unwrap();
        assert_eq!(some, back);
        assert_eq!(back.in_reply_to.as_deref(), Some("m-0"));
    }

    /// Every `kind` variant round-trips and carries its snake_case wire value.
    #[test]
    fn round_trips_every_kind_variant() {
        for (kind, wire) in [
            (MessageKind::Request, "request"),
            (MessageKind::Response, "response"),
            (MessageKind::Done, "done"),
            (MessageKind::Error, "error"),
        ] {
            let mut msg = base();
            msg.kind = kind;
            let json = serde_json::to_string(&msg).expect("serializes");
            let value: Value = serde_json::from_str(&json).expect("json");
            assert_eq!(value["kind"], wire, "wire value for {kind:?}");
            let back: MessageEnvelope = serde_json::from_str(&json).expect("parses");
            assert_eq!(msg, back);
        }
    }

    /// A wrapped exchange round-trips and the nested object carries the
    /// `baton.exchange/v1` discriminator plus the trail's outcome tag.
    #[test]
    fn round_trips_wrapped_exchange_with_schema_discriminator() {
        let mut msg = base();
        msg.exchange = Some(wrapped());
        let json = serde_json::to_string(&msg).expect("serializes");
        let value: Value = serde_json::from_str(&json).expect("json");
        assert_eq!(value["exchange"]["schema"], crate::events::SCHEMA);
        assert_eq!(value["schema"], SCHEMA);
        // The nested outcome tag matches the on-disk exchange trail.
        assert_eq!(
            value["exchange"]["exchange"]["outcome"]["event"],
            "response_ok"
        );

        let back: MessageEnvelope = serde_json::from_str(&json).expect("parses");
        assert_eq!(msg, back);
    }

    /// A message with no wrapped exchange round-trips with `exchange: null`.
    #[test]
    fn round_trips_without_wrapped_exchange() {
        let msg = base();
        assert_eq!(msg.exchange, None);
        let json = serde_json::to_string(&msg).expect("serializes");
        let back: MessageEnvelope = serde_json::from_str(&json).expect("parses");
        assert_eq!(msg, back);
    }

    /// Unknown top-level fields are ignored on read, not errors — the
    /// forward-compatibility guarantee (no `deny_unknown_fields`).
    #[test]
    fn unknown_fields_are_ignored_on_read() {
        let json = r#"{
            "schema": "baton.message/v1",
            "message_id": "m-1",
            "conversation_id": "c-1",
            "from": "agent-a",
            "to": "agent-b",
            "in_reply_to": null,
            "kind": "request",
            "body": "hello",
            "ts_ms": 1700000000000,
            "exchange": null,
            "future_field": {"added": "by a newer baton"},
            "another_unknown": 42
        }"#;
        let back: MessageEnvelope = serde_json::from_str(json).expect("ignores unknown fields");
        assert_eq!(back, base());
    }
}
