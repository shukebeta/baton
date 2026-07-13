//! The conversation driver: alternate two participants to a terminal condition.
//!
//! Where [`crate::participant`] answers *one* envelope, this module sustains a
//! *bounded, governed* exchange between two [`Participant`]s. Given a seed
//! request, [`converse`] alternates turns — each reply's body becomes the next
//! participant's request — recording every turn as a `baton.message/v1`
//! [`MessageEnvelope`] until the first terminal condition trips.
//!
//! Termination is guaranteed: the turn-cap is always enforced, so even two
//! participants that would loop forever stop. The other conditions
//! ([`TerminalReason`]) end the run earlier when they apply.
//!
//! The driver depends only on the [`Participant`] trait, so the same code drives
//! in-process participants (the `baton converse` verb) or independent OS
//! processes (the vertical-proof test) without change.

use crate::error::{BatonError, Result};
use crate::log::Outcome;
use crate::message::{MessageEnvelope, MessageKind};
use crate::participant::Participant;

/// Default hard turn-cap when `BATON_MAX_TURNS` is unset.
pub const DEFAULT_MAX_TURNS: usize = 8;

/// Why a conversation ended. The trail's final turn is the one that tripped it
/// (except [`TurnCap`](Self::TurnCap), where the cap fires after the final
/// recorded reply).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalReason {
    /// A participant emitted a `kind: "done"` reply — unilateral completion.
    Done,
    /// A participant emitted a `kind: "error"` reply — a delivered failure,
    /// recorded as the terminal turn.
    Error,
    /// Accumulated provider usage exceeded [`Governance::token_budget`].
    TokenBudget,
    /// The [`Governance::max_turns`] hard cap was reached.
    TurnCap,
}

/// The governance knobs that bound a run. `max_turns` is the always-enforced
/// guarantee; `token_budget` is an optional additional ceiling (`None` disables
/// the arm).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Governance {
    /// Hard cap on the number of reply turns (the seed request is turn 0 and is
    /// never counted). At most this many [`Participant::respond`] calls run.
    pub max_turns: usize,
    /// Optional cumulative token budget across all replies' nested usage; the
    /// run ends once the running total exceeds it. `None` disables the arm.
    pub token_budget: Option<u64>,
}

impl Governance {
    /// Loads governance from an arbitrary key lookup (the testable core behind
    /// the `converse` verb's env reads), mirroring
    /// [`BatonConfig::from_lookup`](crate::config::BatonConfig::from_lookup).
    ///
    /// `BATON_MAX_TURNS` defaults to [`DEFAULT_MAX_TURNS`] and must be a positive
    /// integer (zero is rejected — a cap of zero would record no turns).
    /// `BATON_TOKEN_BUDGET` is optional; when present it must be a positive
    /// integer, and when absent the budget arm is disabled.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let max_turns = match non_empty(lookup("BATON_MAX_TURNS")) {
            Some(raw) => {
                let parsed = raw.parse::<usize>().map_err(|_| {
                    BatonError::Config(format!(
                        "BATON_MAX_TURNS must be a positive integer, got {raw:?}"
                    ))
                })?;
                if parsed == 0 {
                    return Err(BatonError::Config(
                        "BATON_MAX_TURNS must be greater than zero".to_string(),
                    ));
                }
                parsed
            }
            None => DEFAULT_MAX_TURNS,
        };

        let token_budget = match non_empty(lookup("BATON_TOKEN_BUDGET")) {
            Some(raw) => {
                let parsed = raw.parse::<u64>().map_err(|_| {
                    BatonError::Config(format!(
                        "BATON_TOKEN_BUDGET must be a positive integer, got {raw:?}"
                    ))
                })?;
                if parsed == 0 {
                    return Err(BatonError::Config(
                        "BATON_TOKEN_BUDGET must be greater than zero".to_string(),
                    ));
                }
                Some(parsed)
            }
            None => None,
        };

        Ok(Self {
            max_turns,
            token_budget,
        })
    }
}

/// The result of a run: the ordered `baton.message/v1` trail (the seed followed
/// by every reply) and the reason it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transcript {
    /// Every message in turn order: the seed request first, then each reply.
    pub trail: Vec<MessageEnvelope>,
    /// Why the conversation ended.
    pub reason: TerminalReason,
}

/// Alternates `a` and `b` from `seed` until the first terminal condition trips.
///
/// `seed` is participant A's opening request, addressed to B, so B replies
/// first, then A, alternating. Each reply is appended to the trail; the next
/// request reuses the reply's `from`/`to` **verbatim** (the participant already
/// swapped addressing on its reply, so the driver must not swap again — a double
/// swap would mislabel each reply's speaker) and only flips the kind to
/// `request`.
///
/// Terminal checks, per reply, in order: a `done` reply ends the run; an `error`
/// reply is recorded and ends it; the accumulated nested token usage ending it
/// once it exceeds `governance.token_budget`; otherwise the run ends when the
/// recorded reply count reaches `governance.max_turns`. A reply carrying no
/// nested usage (a fake, or a synthesized machinery error) contributes zero, so
/// with usage absent the run still terminates on the turn-cap.
pub fn converse(
    a: &dyn Participant,
    b: &dyn Participant,
    seed: MessageEnvelope,
    governance: &Governance,
) -> Transcript {
    let mut trail = vec![seed.clone()];
    let mut request = seed;
    // The seed is addressed to B, so B is the first responder; the two then
    // alternate.
    let mut responder_is_b = true;
    let mut turns = 0usize;
    let mut token_total: u64 = 0;

    let reason = loop {
        if turns >= governance.max_turns {
            break TerminalReason::TurnCap;
        }

        let responder: &dyn Participant = if responder_is_b { b } else { a };
        let reply = responder.respond(&request);
        turns += 1;
        trail.push(reply.clone());

        match reply.kind {
            MessageKind::Done => break TerminalReason::Done,
            MessageKind::Error => break TerminalReason::Error,
            MessageKind::Request | MessageKind::Response => {}
        }

        if let Some(budget) = governance.token_budget {
            token_total = token_total.saturating_add(reply_tokens(&reply));
            if token_total > budget {
                break TerminalReason::TokenBudget;
            }
        }

        request = next_request(&reply);
        responder_is_b = !responder_is_b;
    };

    Transcript { trail, reason }
}

/// Sums a reply's nested provider usage (input + output tokens), treating an
/// absent nested record or absent counts as zero.
fn reply_tokens(reply: &MessageEnvelope) -> u64 {
    match reply.exchange.as_ref() {
        Some(wrapped) => match &wrapped.exchange.outcome {
            Outcome::Ok {
                input_tokens,
                output_tokens,
                ..
            } => input_tokens
                .unwrap_or(0)
                .saturating_add(output_tokens.unwrap_or(0)),
            Outcome::Error { .. } => 0,
        },
        None => 0,
    }
}

/// Builds the next request from a reply: same conversation, addressing reused
/// as-is (the participant already swapped it), `in_reply_to` linked, and the
/// kind flipped to `request` so the peer answers it.
fn next_request(reply: &MessageEnvelope) -> MessageEnvelope {
    let mut request = MessageEnvelope::new(
        format!("{}-q-{}", reply.conversation_id, reply.message_id),
        reply.conversation_id.clone(),
        reply.from.clone(),
        reply.to.clone(),
        MessageKind::Request,
        reply.body.clone(),
        reply.ts_ms + 1,
    );
    request.in_reply_to = Some(reply.message_id.clone());
    request
}

/// Treats a present-but-blank value as absent, matching the config env reads.
fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::participant::testing::{DoneParticipant, LoopingParticipant, ScriptedParticipant};
    use std::collections::HashMap;

    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    fn seed() -> MessageEnvelope {
        MessageEnvelope::new(
            "conv-1-m0",
            "conv-1",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "hello",
            1_700_000_000_000,
        )
    }

    fn gov(max_turns: usize, token_budget: Option<u64>) -> Governance {
        Governance {
            max_turns,
            token_budget,
        }
    }

    /// The turn-cap is a hard guarantee: two participants that never stop on
    /// their own are cut off at exactly `max_turns` replies.
    #[test]
    fn turn_cap_terminates_looping_participants() {
        let a = LoopingParticipant::new("a-says");
        let b = LoopingParticipant::new("b-says");

        let transcript = converse(&a, &b, seed(), &gov(5, None));

        assert_eq!(transcript.reason, TerminalReason::TurnCap);
        // Seed + exactly 5 replies.
        assert_eq!(transcript.trail.len(), 6);
    }

    /// Each reply's `from` names the actual speaker, alternating B, A, B — the
    /// coherence that a double addressing-swap would break.
    #[test]
    fn trail_addressing_alternates_coherently() {
        let a = LoopingParticipant::new("a-says");
        let b = LoopingParticipant::new("b-says");

        let transcript = converse(&a, &b, seed(), &gov(3, None));

        // trail[0] is the seed: A asks B.
        assert_eq!(transcript.trail[0].from, "agent-a");
        assert_eq!(transcript.trail[0].to, "agent-b");
        // Replies alternate speaker B, A, B and address the peer.
        assert_eq!(transcript.trail[1].from, "agent-b");
        assert_eq!(transcript.trail[1].to, "agent-a");
        assert_eq!(transcript.trail[1].body, "b-says");
        assert_eq!(transcript.trail[2].from, "agent-a");
        assert_eq!(transcript.trail[2].to, "agent-b");
        assert_eq!(transcript.trail[2].body, "a-says");
        assert_eq!(transcript.trail[3].from, "agent-b");
        assert_eq!(transcript.trail[3].to, "agent-a");
        // Each reply links to the request it answered.
        assert!(transcript.trail[1].in_reply_to.is_some());
    }

    /// A `done` reply ends the run before the caps, recorded as the terminal
    /// turn.
    #[test]
    fn done_reply_ends_conversation() {
        // B loops, A emits done. Turn 1 (B) is a normal reply; turn 2 (A) is
        // done.
        let a = DoneParticipant;
        let b = LoopingParticipant::new("b-says");

        let transcript = converse(&a, &b, seed(), &gov(8, None));

        assert_eq!(transcript.reason, TerminalReason::Done);
        // Seed, B's reply, A's done.
        assert_eq!(transcript.trail.len(), 3);
        assert_eq!(transcript.trail.last().unwrap().kind, MessageKind::Done);
    }

    /// An `error` reply ends the run, recorded as the terminal turn. An
    /// exhausted `ScriptedParticipant` yields a delivered error, which the driver
    /// treats as terminal.
    #[test]
    fn error_reply_ends_conversation() {
        // B has one scripted reply then errors on the next request; A loops. So
        // turn 1 (B) responds, turn 2 (A) responds, turn 3 (B) errors.
        let a = LoopingParticipant::new("a-says");
        let b = ScriptedParticipant::new(["b-first"]);

        let transcript = converse(&a, &b, seed(), &gov(8, None));

        assert_eq!(transcript.reason, TerminalReason::Error);
        assert_eq!(transcript.trail.last().unwrap().kind, MessageKind::Error);
    }

    /// The token-budget arm ends the run once accumulated nested usage exceeds
    /// the budget, before the turn-cap.
    #[test]
    fn token_budget_terminates_before_turn_cap() {
        // Each reply reports 10 input + 10 output = 20 tokens. Budget 25 is
        // exceeded after the second reply (40 > 25), well before the cap.
        let a = LoopingParticipant::with_usage("a-says", 10, 10);
        let b = LoopingParticipant::with_usage("b-says", 10, 10);

        let transcript = converse(&a, &b, seed(), &gov(100, Some(25)));

        assert_eq!(transcript.reason, TerminalReason::TokenBudget);
        // Seed + 2 replies (20 then 40 cumulative).
        assert_eq!(transcript.trail.len(), 3);
    }

    /// With usage absent, the budget arm never fires and the run still
    /// terminates on the turn-cap.
    #[test]
    fn absent_usage_falls_back_to_turn_cap() {
        // Looping participants nest no usage; even with a budget set, only the
        // cap can stop them.
        let a = LoopingParticipant::new("a-says");
        let b = LoopingParticipant::new("b-says");

        let transcript = converse(&a, &b, seed(), &gov(4, Some(1)));

        assert_eq!(transcript.reason, TerminalReason::TurnCap);
        assert_eq!(transcript.trail.len(), 5);
    }

    #[test]
    fn governance_defaults_max_turns_and_disables_budget() {
        let gov = Governance::from_lookup(lookup_from(&[])).expect("loads");
        assert_eq!(gov.max_turns, DEFAULT_MAX_TURNS);
        assert_eq!(gov.token_budget, None);
    }

    #[test]
    fn governance_reads_both_knobs() {
        let gov = Governance::from_lookup(lookup_from(&[
            ("BATON_MAX_TURNS", "3"),
            ("BATON_TOKEN_BUDGET", "500"),
        ]))
        .expect("loads");
        assert_eq!(gov.max_turns, 3);
        assert_eq!(gov.token_budget, Some(500));
    }

    #[test]
    fn governance_rejects_zero_and_non_numeric() {
        assert!(Governance::from_lookup(lookup_from(&[("BATON_MAX_TURNS", "0")])).is_err());
        assert!(Governance::from_lookup(lookup_from(&[("BATON_MAX_TURNS", "x")])).is_err());
        assert!(Governance::from_lookup(lookup_from(&[("BATON_TOKEN_BUDGET", "0")])).is_err());
    }
}
