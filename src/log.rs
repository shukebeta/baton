//! Reading and rendering the JSONL exchange-event trail.
//!
//! [`crate::events`] owns the write path: each `ask`/`session` exchange emits a
//! `request` line followed by exactly one outcome line. This module owns the
//! read path — turning that trail back into typed, paired [`Exchange`] values so
//! the log becomes a first-class artefact: inspectable (`baton log show`) and
//! replayable (`baton log replay`).
//!
//! The read path deliberately does **not** reuse the write-path
//! [`ExchangeEvent`](crate::events::ExchangeEvent): its `schema` field is a
//! `&'static str`, which cannot be deserialized into. Instead, dedicated owned
//! `Deserialize` records mirror the on-disk shape, and [`parse_jsonl`] accepts
//! any [`Read`] so it is unit-testable without touching a file.
//!
//! Unknown `event` tags are skipped (forward-compatibility with a newer
//! writer), but a line that is not valid JSON is a hard parse error: a corrupt
//! trail should be surfaced, not silently dropped.

use std::io::{BufRead, BufReader, Read};

use serde::Deserialize;
use serde_json::Value;

use crate::error::{BatonError, Result};

/// One request paired with its single outcome — the unit `baton log` operates on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exchange {
    /// The recorded request (carries everything needed to replay it).
    pub request: RequestRecord,
    /// The recorded terminal outcome (success reply or failure).
    pub outcome: Outcome,
}

/// The replay-relevant fields of a `request` event, read back from the log.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RequestRecord {
    /// Wall-clock emission time, Unix epoch milliseconds.
    pub ts_ms: u64,
    /// Model id the request targeted.
    pub model: String,
    /// Base URL the request was sent to.
    pub base_url: String,
    /// The user prompt text.
    pub prompt: String,
}

/// The terminal outcome of an exchange, read back from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The call succeeded.
    Ok {
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Time spent in the provider call, milliseconds.
        duration_ms: u64,
        /// The assistant reply text.
        reply: String,
    },
    /// The call failed; `kind` is the stable machine class.
    Error {
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Time spent before the failure resolved, milliseconds.
        duration_ms: u64,
        /// Stable machine-readable error class (mirrors [`BatonError::kind`]).
        kind: String,
        /// Human-readable error description.
        message: String,
    },
}

/// Deserialization mirror of a `response_ok` line.
#[derive(Deserialize)]
struct OkRecord {
    ts_ms: u64,
    duration_ms: u64,
    reply: String,
}

/// Deserialization mirror of a `response_error` line.
#[derive(Deserialize)]
struct ErrRecord {
    ts_ms: u64,
    duration_ms: u64,
    kind: String,
    message: String,
}

/// Parses a JSONL exchange trail into paired [`Exchange`] values.
///
/// Each non-blank line is parsed as a standalone JSON object and dispatched on
/// its `event` tag. A `request` opens a pending exchange; the next outcome line
/// (`response_ok` / `response_error`) closes it. Behaviour at the edges:
///
/// - **Unknown `event` tag** (or a line with no `event`): skipped without error,
///   so a log written by a newer Baton still parses.
/// - **Malformed JSON line**, or a known event missing required fields: a hard
///   [`BatonError::Log`] naming the 1-based line number.
/// - **Dangling outcome** (no preceding request) or a **trailing request** with
///   no outcome: not yielded — only complete pairs become an [`Exchange`].
pub fn parse_jsonl<R: Read>(reader: R) -> Result<Vec<Exchange>> {
    let buffered = BufReader::new(reader);
    let mut exchanges = Vec::new();
    let mut pending: Option<RequestRecord> = None;

    for (idx, line) in buffered.lines().enumerate() {
        let line_no = idx + 1;
        let line =
            line.map_err(|err| BatonError::Io(format!("reading log line {line_no}: {err}")))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_str(trimmed)
            .map_err(|err| BatonError::Log(format!("line {line_no}: invalid JSON: {err}")))?;

        match value.get("event").and_then(Value::as_str) {
            Some("request") => {
                let record: RequestRecord = from_value(value, line_no, "request")?;
                pending = Some(record);
            }
            Some("response_ok") => {
                let ok: OkRecord = from_value(value, line_no, "response_ok")?;
                if let Some(request) = pending.take() {
                    exchanges.push(Exchange {
                        request,
                        outcome: Outcome::Ok {
                            ts_ms: ok.ts_ms,
                            duration_ms: ok.duration_ms,
                            reply: ok.reply,
                        },
                    });
                }
            }
            Some("response_error") => {
                let err: ErrRecord = from_value(value, line_no, "response_error")?;
                if let Some(request) = pending.take() {
                    exchanges.push(Exchange {
                        request,
                        outcome: Outcome::Error {
                            ts_ms: err.ts_ms,
                            duration_ms: err.duration_ms,
                            kind: err.kind,
                            message: err.message,
                        },
                    });
                }
            }
            // Unknown or absent event tag: skip, staying forward-compatible.
            _ => {}
        }
    }

    Ok(exchanges)
}

/// Deserializes a known event into `T`, mapping a shape mismatch onto a
/// [`BatonError::Log`] that names the line and event so a corrupt trail points
/// at the offending entry.
fn from_value<T: serde::de::DeserializeOwned>(
    value: Value,
    line_no: usize,
    event: &str,
) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|err| BatonError::Log(format!("line {line_no}: malformed {event} event: {err}")))
}

/// Renders one exchange as a human-readable multi-line block for `baton log show`.
///
/// `n` is the 1-based position shown to the user. The block carries the
/// timestamp, model, and call duration on its header line, then a truncated
/// prompt and either a truncated reply or the failure (`kind: message`).
pub fn format_exchange(n: usize, exchange: &Exchange) -> String {
    const MAX: usize = 120;
    let request = &exchange.request;
    let mut out = match &exchange.outcome {
        Outcome::Ok {
            duration_ms, reply, ..
        } => format!(
            "#{n}  {}  {}  ({duration_ms}ms)\n    prompt: {}\n    reply:  {}",
            format_ts(request.ts_ms),
            request.model,
            excerpt(&request.prompt, MAX),
            excerpt(reply, MAX),
        ),
        Outcome::Error {
            duration_ms,
            kind,
            message,
            ..
        } => format!(
            "#{n}  {}  {}  ({duration_ms}ms)\n    prompt: {}\n    error:  {kind}: {}",
            format_ts(request.ts_ms),
            request.model,
            excerpt(&request.prompt, MAX),
            excerpt(message, MAX),
        ),
    };
    out.push('\n');
    out
}

/// Collapses newlines to spaces and truncates `s` to at most `max` characters,
/// appending `…` when truncation occurred.
///
/// Truncation is on `char` boundaries so a multibyte character is never split.
fn excerpt(s: &str, max: usize) -> String {
    let flattened: String = s
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if flattened.chars().count() <= max {
        return flattened;
    }
    let mut truncated: String = flattened.chars().take(max).collect();
    truncated.push('…');
    truncated
}

/// Formats Unix epoch milliseconds as `YYYY-MM-DDTHH:MM:SSZ` (UTC).
///
/// Uses Howard Hinnant's civil-from-days algorithm so no date dependency is
/// pulled into the crate. Sub-second precision is dropped; the trail's `ts_ms`
/// stays available for machine consumers.
pub fn format_ts(ts_ms: u64) -> String {
    let secs = ts_ms / 1000;
    let days = (secs / 86_400) as i64;
    let tod = secs % 86_400;
    let (hour, minute, second) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Converts a count of days since the Unix epoch into a `(year, month, day)`
/// civil date, via Howard Hinnant's well-known `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A valid two-line exchange parses into one request/outcome pair.
    #[test]
    fn parses_valid_two_line_exchange() {
        let log = concat!(
            r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"hello"}"#,
            "\n",
            r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"hi there"}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses");
        assert_eq!(exchanges.len(), 1);
        assert_eq!(
            exchanges[0].request,
            RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-sonnet-4-6".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "hello".to_string(),
            }
        );
        assert_eq!(
            exchanges[0].outcome,
            Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "hi there".to_string(),
            }
        );
    }

    /// A `response_error` outcome is paired and carries kind + message.
    #[test]
    fn parses_error_outcome() {
        let log = concat!(
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"p"}"#,
            "\n",
            r#"{"event":"response_error","ts_ms":2,"duration_ms":7,"kind":"auth","message":"bad api key"}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses");
        assert_eq!(exchanges.len(), 1);
        assert_eq!(
            exchanges[0].outcome,
            Outcome::Error {
                ts_ms: 2,
                duration_ms: 7,
                kind: "auth".to_string(),
                message: "bad api key".to_string(),
            }
        );
    }

    /// Unknown `event` tags are skipped without error; the surrounding valid
    /// exchange still parses.
    #[test]
    fn unknown_event_tags_are_skipped() {
        let log = concat!(
            r#"{"event":"heartbeat","ts_ms":1}"#,
            "\n",
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"p"}"#,
            "\n",
            r#"{"event":"telemetry","foo":42}"#,
            "\n",
            r#"{"event":"response_ok","ts_ms":2,"duration_ms":1,"reply":"r"}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses");
        assert_eq!(exchanges.len(), 1, "only the request/ok pair is yielded");
        assert_eq!(exchanges[0].request.prompt, "p");
    }

    /// A line with no `event` field at all is also skipped (not an error).
    #[test]
    fn line_without_event_field_is_skipped() {
        let log = concat!(
            r#"{"note":"not an event"}"#,
            "\n",
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"p"}"#,
            "\n",
            r#"{"event":"response_ok","ts_ms":2,"duration_ms":1,"reply":"r"}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses");
        assert_eq!(exchanges.len(), 1);
    }

    /// A malformed JSON line surfaces as a `Log` parse error naming the line.
    #[test]
    fn malformed_json_line_is_a_parse_error() {
        let log = concat!(
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"p"}"#,
            "\n",
            "<<<not json at all>>>\n",
        );
        match parse_jsonl(Cursor::new(log)).unwrap_err() {
            BatonError::Log(msg) => assert!(msg.contains("line 2"), "got: {msg}"),
            other => panic!("expected Log, got {other:?}"),
        }
    }

    /// A known event missing required fields is a parse error, not a skip.
    #[test]
    fn malformed_request_event_is_a_parse_error() {
        // `request` with no `prompt` cannot deserialize into RequestRecord.
        let log = concat!(
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u"}"#,
            "\n"
        );
        match parse_jsonl(Cursor::new(log)).unwrap_err() {
            BatonError::Log(msg) => {
                assert!(
                    msg.contains("line 1") && msg.contains("request"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Log, got {other:?}"),
        }
    }

    /// Blank lines are ignored and a trailing request with no outcome is not
    /// yielded (only complete pairs become an Exchange).
    #[test]
    fn blank_lines_skipped_and_trailing_request_not_yielded() {
        let log = concat!(
            "\n",
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"first"}"#,
            "\n",
            r#"{"event":"response_ok","ts_ms":2,"duration_ms":1,"reply":"r"}"#,
            "\n",
            "   \n",
            r#"{"event":"request","ts_ms":3,"model":"m","base_url":"u","prompt":"dangling"}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses");
        assert_eq!(
            exchanges.len(),
            1,
            "the trailing unpaired request is dropped"
        );
        assert_eq!(exchanges[0].request.prompt, "first");
    }

    #[test]
    fn format_ts_renders_known_epoch_in_utc() {
        // 1700000000 s = 2023-11-14T22:13:20Z.
        assert_eq!(format_ts(1_700_000_000_000), "2023-11-14T22:13:20Z");
        // The Unix epoch itself.
        assert_eq!(format_ts(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn excerpt_collapses_newlines_and_truncates_on_char_boundaries() {
        assert_eq!(excerpt("a\nb\rc", 10), "a b c");
        // Multibyte chars: 4 'é' under a max of 3 → 3 kept + ellipsis.
        assert_eq!(excerpt("éééé", 3), "ééé…");
        // Exactly at the limit is not truncated.
        assert_eq!(excerpt("abc", 3), "abc");
    }

    #[test]
    fn format_exchange_includes_timestamp_model_prompt_and_reply() {
        let ex = Exchange {
            request: RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-sonnet-4-6".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "the question".to_string(),
            },
            outcome: Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "the answer".to_string(),
            },
        };
        let rendered = format_exchange(1, &ex);
        assert!(rendered.contains("#1"));
        assert!(rendered.contains("2023-11-14T22:13:20Z"));
        assert!(rendered.contains("claude-sonnet-4-6"));
        assert!(rendered.contains("the question"));
        assert!(rendered.contains("the answer"));
        assert!(rendered.contains("418ms"));
    }

    #[test]
    fn format_exchange_renders_error_kind_for_failure() {
        let ex = Exchange {
            request: RequestRecord {
                ts_ms: 0,
                model: "m".to_string(),
                base_url: "u".to_string(),
                prompt: "p".to_string(),
            },
            outcome: Outcome::Error {
                ts_ms: 0,
                duration_ms: 5,
                kind: "auth".to_string(),
                message: "bad api key".to_string(),
            },
        };
        let rendered = format_exchange(2, &ex);
        assert!(rendered.contains("#2"));
        assert!(rendered.contains("error:"));
        assert!(rendered.contains("auth"));
        assert!(rendered.contains("bad api key"));
    }
}
