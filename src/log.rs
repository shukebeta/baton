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
//! writer). A line that is not valid JSON is a hard parse error — except a
//! trailing partial line, one with no terminating newline left behind when a
//! `baton ask`/`session` process is killed mid-write: that is tolerated and
//! reported as a warning, so an unclean shutdown never bricks the whole trail.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{BatonError, Result};
use crate::message::{MessageEnvelope, SCHEMA as MESSAGE_SCHEMA};

/// One request paired with its single outcome — the unit `baton log` operates on.
///
/// Also serves as the owned exchange value nested inside a `baton.message/v1`
/// envelope (see [`crate::message::WrappedExchange`]): the `Serialize` /
/// `Deserialize` derives exist for that embedding. The on-disk JSONL trail is
/// written by [`crate::events`] as two separate lines and read back via the
/// dedicated `OkRecord`/`ErrRecord` mirrors below — it does not use these
/// derives, so their tags are free to describe the *nested* object shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exchange {
    /// The recorded request (carries everything needed to replay it).
    pub request: RequestRecord,
    /// The recorded terminal outcome (success reply or failure).
    pub outcome: Outcome,
}

/// The replay-relevant fields of a `request` event, read back from the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRecord {
    /// Wall-clock emission time, Unix epoch milliseconds.
    pub ts_ms: u64,
    /// Model id the request targeted.
    pub model: String,
    /// Base URL the request was sent to.
    pub base_url: String,
    /// The user prompt text.
    pub prompt: String,
    /// Session this turn belonged to; absent on the single-turn `ask` path. On
    /// an A2A seat turn this equals `conversation_id`, so the seat trail
    /// partitions into one session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Monotonic turn number within the session; absent on the `ask` path and on
    /// A2A seat turns (which order by file position).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_index: Option<u64>,
}

/// The terminal outcome of an exchange, read back from the log.
///
/// When serialized (only as part of a nested [`Exchange`] inside a
/// `baton.message/v1` envelope), the `event` tag reads `response_ok` /
/// `response_error`, matching the on-disk trail's outcome tags in
/// [`crate::events`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum Outcome {
    /// The call succeeded.
    #[serde(rename = "response_ok")]
    Ok {
        /// Wall-clock emission time, Unix epoch milliseconds.
        ts_ms: u64,
        /// Time spent in the provider call, milliseconds.
        duration_ms: u64,
        /// The assistant reply text.
        reply: String,
        /// Provider-reported input (prompt) tokens; omitted when unknown.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_tokens: Option<u64>,
        /// Provider-reported output (completion) tokens; omitted when unknown.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_tokens: Option<u64>,
    },
    /// The call failed; `kind` is the stable machine class.
    #[serde(rename = "response_error")]
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
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

/// Deserialization mirror of a `response_error` line.
#[derive(Deserialize)]
struct ErrRecord {
    ts_ms: u64,
    duration_ms: u64,
    kind: String,
    message: String,
}

/// The outcome of parsing an exchange trail: the complete [`Exchange`] pairs and
/// any non-fatal diagnostics collected along the way (e.g. a tolerated trailing
/// partial line). [`parse_jsonl`] is pure over its reader — it returns warnings
/// here rather than printing them, leaving stderr emission to the caller.
#[derive(Debug, Default)]
pub struct ParseReport {
    /// Complete request/outcome pairs, in file order.
    pub exchanges: Vec<Exchange>,
    /// Non-fatal diagnostics, in the order they were encountered.
    pub warnings: Vec<String>,
}

/// Parses a JSONL exchange trail into a [`ParseReport`] of paired [`Exchange`]
/// values plus any non-fatal warnings.
///
/// Each non-blank line is parsed as a standalone JSON object and dispatched on
/// its `event` tag. A `request` opens a pending exchange; the next outcome line
/// (`response_ok` / `response_error`) closes it. Behaviour at the edges:
///
/// - **Unknown `event` tag** (or a line with no `event`): skipped without error,
///   so a log written by a newer Baton still parses.
/// - **Malformed JSON line**, or a known event missing required fields: a hard
///   [`BatonError::Log`] naming the 1-based line number — *unless* the offending
///   line is the final one and was read without a terminating newline (see
///   below).
/// - **Trailing partial line**: the final line of the file with no terminating
///   `\n` is the signature of an unclean shutdown — a `baton ask`/`session`
///   process killed mid-`write_all`. A UTF-8 or JSON-syntax failure there is not
///   fatal: the line is skipped and recorded in [`ParseReport::warnings`] so the
///   caller can surface it, and the exchanges already parsed are still yielded.
///   The same failure on any newline-terminated line is genuine corruption and
///   stays a hard error.
/// - **Dangling outcome** (no preceding request) or a **trailing request** with
///   no outcome: not yielded — only complete pairs become an [`Exchange`].
///
/// The function is pure over its [`Read`] argument: warnings are returned in the
/// [`ParseReport`] rather than printed, so callers (and unit tests) decide how
/// to surface them.
pub fn parse_jsonl<R: Read>(reader: R) -> Result<ParseReport> {
    let mut buffered = BufReader::new(reader);
    let mut report = ParseReport::default();
    let mut pending: Option<RequestRecord> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut line_no = 0usize;

    loop {
        buf.clear();
        let read = buffered
            .read_until(b'\n', &mut buf)
            .map_err(|err| BatonError::Io(format!("reading log line {}: {err}", line_no + 1)))?;
        if read == 0 {
            // EOF: a final '\n' leaves read_until returning 0 (not a zero-length
            // line), so the counter is not bumped and never trips the check below.
            break;
        }
        line_no += 1;

        // The only byte-level signal kept: whether this line was terminated. A
        // line with no trailing '\n' can only be the final line, and is what an
        // unclean shutdown (a kill mid-`write_all`) leaves behind. The `str::trim`
        // inside `parse_line_value` reproduces `BufRead::lines()`'s `\n` / `\r\n`
        // handling, so no byte-level stripping is needed beyond this flag.
        let terminated = buf.last() == Some(&b'\n');
        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        let value = match parse_line_value(&buf) {
            Ok(value) => value,
            Err(detail) if !terminated => {
                report.warnings.push(format!(
                    "skipped partial trailing line {line_no} of the event log \
                     (no terminating newline — likely an unclean shutdown): {detail}"
                ));
                continue;
            }
            Err(detail) => return Err(BatonError::Log(format!("line {line_no}: {detail}"))),
        };

        match value.get("event").and_then(Value::as_str) {
            Some("request") => {
                let record: RequestRecord = from_value(value, line_no, "request")?;
                pending = Some(record);
            }
            Some("response_ok") => {
                let ok: OkRecord = from_value(value, line_no, "response_ok")?;
                if let Some(request) = pending.take() {
                    report.exchanges.push(Exchange {
                        request,
                        outcome: Outcome::Ok {
                            ts_ms: ok.ts_ms,
                            duration_ms: ok.duration_ms,
                            reply: ok.reply,
                            input_tokens: ok.input_tokens,
                            output_tokens: ok.output_tokens,
                        },
                    });
                }
            }
            Some("response_error") => {
                let err: ErrRecord = from_value(value, line_no, "response_error")?;
                if let Some(request) = pending.take() {
                    report.exchanges.push(Exchange {
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

    Ok(report)
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

/// Parses one raw log line into a JSON [`Value`], returning a short detail
/// string (e.g. `"invalid JSON: …"`) on failure rather than a full
/// [`BatonError`].
///
/// The two callers in [`parse_jsonl`] — the tolerate-trailing-partial path and
/// the hard-error path — frame that detail differently (one prefixes the line
/// number for a `BatonError::Log`, the other folds it into a warning), so
/// returning the bare detail avoids duplicating "line N" or "log error:" in the
/// warning text. The bytes are trimmed before parsing, reproducing
/// `BufRead::lines()`'s `\n` / `\r\n` handling without stripping them at the
/// byte level.
fn parse_line_value(bytes: &[u8]) -> std::result::Result<Value, String> {
    let s = std::str::from_utf8(bytes).map_err(|err| format!("invalid UTF-8: {err}"))?;
    serde_json::from_str(s.trim()).map_err(|err| format!("invalid JSON: {err}"))
}

/// One turn of a session, read back from the trail: the turn's `request` (which
/// carries `session_id` and `turn_index`) paired with its terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTurn {
    /// The request that opened this turn.
    pub request: RequestRecord,
    /// The turn's outcome, or `None` when the run was killed after the request
    /// line but before its outcome landed (a torn tail) — the request still
    /// counts as a turn; its answer just never arrived.
    pub outcome: Option<Outcome>,
}

/// A whole session reconstructed from the trail, keyed on `session_id`.
///
/// Partitioning keys on `session_id` alone, not on a matched start/end pair, so
/// a session killed mid-run (a `session_start` and turns but no `session_end`)
/// still forms one complete [`SessionRecord`] with `ended == false`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    /// The id shared by this session's start marker and every turn's request.
    pub session_id: String,
    /// Whether a `session_start` marker for this id was seen on the trail.
    pub started: bool,
    /// Whether a `session_end` marker for this id was seen — `false` for a
    /// session killed mid-run.
    pub ended: bool,
    /// The turn count declared on the `session_end` marker, or `None` for a
    /// session with no end marker. Compare against `turns.len()` to detect a
    /// trail truncated before its turns were fully written.
    pub declared_turns: Option<u64>,
    /// The session's turns, in file (== `turn_index`) order.
    pub turns: Vec<SessionTurn>,
}

/// The outcome of partitioning a trail into sessions: the [`SessionRecord`]s in
/// first-seen order plus any non-fatal diagnostics (a tolerated trailing partial
/// line), mirroring [`ParseReport`]'s shape.
#[derive(Debug, Default)]
pub struct SessionParseReport {
    /// Reconstructed sessions, in the order their `session_id` was first seen.
    pub sessions: Vec<SessionRecord>,
    /// Non-fatal diagnostics, in encounter order.
    pub warnings: Vec<String>,
}

/// Deserialization mirror of a `session_start` line.
#[derive(Deserialize)]
struct SessionStartRecord {
    session_id: String,
}

/// Deserialization mirror of a `session_end` line.
#[derive(Deserialize)]
struct SessionEndRecord {
    session_id: String,
    turns: u64,
}

/// Partitions a JSONL trail into whole sessions, keyed on `session_id`.
///
/// Reads the same trail as [`parse_jsonl`], but groups by session framing rather
/// than pairing bare exchanges: `session_start` / `session_end` markers bound a
/// session, each session turn's `request` carries the `session_id` + `turn_index`
/// that place it, and the following outcome line closes that turn. Behaviour at
/// the edges mirrors [`parse_jsonl`]:
///
/// - **Sessionless lines** — an `ask` `request`/outcome pair, `baton.message/v1`
///   envelopes, unknown tags — are skipped: they belong to no session.
/// - **Partitioning keys on `session_id`, not on a start/end pair.** A session
///   killed mid-run yields a [`SessionRecord`] with `ended == false` and its
///   turns intact; two sequential sessions in one file separate cleanly by id.
/// - **Trailing partial line**: the final unterminated line (an unclean
///   shutdown's signature) that fails to parse is skipped-with-warning, exactly
///   as in [`parse_jsonl`]; any newline-terminated malformed line stays a hard
///   error. A torn final line after a turn's `request` leaves that turn with
///   `outcome == None`.
pub fn parse_sessions<R: Read>(reader: R) -> Result<SessionParseReport> {
    let mut buffered = BufReader::new(reader);
    let mut report = SessionParseReport::default();
    // First-seen order is preserved by pushing to `report.sessions`; the map
    // routes later lines (turns, end markers) back to the right record.
    let mut index: HashMap<String, usize> = HashMap::new();
    // The session turn awaiting its outcome, as (session index, turn index).
    // Cleared by a sessionless request so a stray outcome is never misattributed.
    let mut pending: Option<(usize, usize)> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut line_no = 0usize;

    loop {
        buf.clear();
        let read = buffered
            .read_until(b'\n', &mut buf)
            .map_err(|err| BatonError::Io(format!("reading log line {}: {err}", line_no + 1)))?;
        if read == 0 {
            break;
        }
        line_no += 1;

        let terminated = buf.last() == Some(&b'\n');
        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        let value = match parse_line_value(&buf) {
            Ok(value) => value,
            Err(detail) if !terminated => {
                report.warnings.push(format!(
                    "skipped partial trailing line {line_no} of the event log \
                     (no terminating newline — likely an unclean shutdown): {detail}"
                ));
                continue;
            }
            Err(detail) => return Err(BatonError::Log(format!("line {line_no}: {detail}"))),
        };

        match value.get("event").and_then(Value::as_str) {
            Some("session_start") => {
                let start: SessionStartRecord = from_value(value, line_no, "session_start")?;
                let idx = session_index(&mut report.sessions, &mut index, &start.session_id);
                report.sessions[idx].started = true;
                pending = None;
            }
            Some("session_end") => {
                let end: SessionEndRecord = from_value(value, line_no, "session_end")?;
                let idx = session_index(&mut report.sessions, &mut index, &end.session_id);
                report.sessions[idx].ended = true;
                report.sessions[idx].declared_turns = Some(end.turns);
                pending = None;
            }
            Some("request") => {
                let record: RequestRecord = from_value(value, line_no, "request")?;
                match record.session_id.clone() {
                    Some(session_id) => {
                        let idx = session_index(&mut report.sessions, &mut index, &session_id);
                        let turn_idx = report.sessions[idx].turns.len();
                        report.sessions[idx].turns.push(SessionTurn {
                            request: record,
                            outcome: None,
                        });
                        pending = Some((idx, turn_idx));
                    }
                    // Sessionless (`ask`) request: not part of any session, and
                    // its outcome must not attach to the previous session turn.
                    None => pending = None,
                }
            }
            Some("response_ok") => {
                let ok: OkRecord = from_value(value, line_no, "response_ok")?;
                if let Some((idx, turn_idx)) = pending.take() {
                    report.sessions[idx].turns[turn_idx].outcome = Some(Outcome::Ok {
                        ts_ms: ok.ts_ms,
                        duration_ms: ok.duration_ms,
                        reply: ok.reply,
                        input_tokens: ok.input_tokens,
                        output_tokens: ok.output_tokens,
                    });
                }
            }
            Some("response_error") => {
                let err: ErrRecord = from_value(value, line_no, "response_error")?;
                if let Some((idx, turn_idx)) = pending.take() {
                    report.sessions[idx].turns[turn_idx].outcome = Some(Outcome::Error {
                        ts_ms: err.ts_ms,
                        duration_ms: err.duration_ms,
                        kind: err.kind,
                        message: err.message,
                    });
                }
            }
            // Unknown or absent event tag (e.g. a `baton.message/v1` envelope):
            // skip, staying forward-compatible.
            _ => {}
        }
    }

    Ok(report)
}

/// Returns the index of the [`SessionRecord`] for `session_id`, creating an
/// empty one (in first-seen order) on first sighting. Shared by the marker and
/// turn arms of [`parse_sessions`] so every reference to an id lands on one
/// record regardless of which line kind mentions it first.
fn session_index(
    sessions: &mut Vec<SessionRecord>,
    index: &mut HashMap<String, usize>,
    session_id: &str,
) -> usize {
    if let Some(&idx) = index.get(session_id) {
        return idx;
    }
    let idx = sessions.len();
    sessions.push(SessionRecord {
        session_id: session_id.to_string(),
        started: false,
        ended: false,
        declared_turns: None,
        turns: Vec::new(),
    });
    index.insert(session_id.to_string(), idx);
    idx
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
            duration_ms,
            reply,
            input_tokens,
            output_tokens,
            ..
        } => format!(
            "#{n}  {}  {}  ({duration_ms}ms)\n    prompt: {}\n    reply:  {}\n    tokens: {}",
            format_ts(request.ts_ms),
            request.model,
            excerpt(&request.prompt, MAX),
            excerpt(reply, MAX),
            format_tokens(*input_tokens, *output_tokens),
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

/// Formats the reported token counts for a `response_ok` block.
///
/// Each count renders as its number, or `?` when the provider did not report
/// it; a fully absent usage block renders as `unknown` so the line never
/// silently implies a zero-token call.
fn format_tokens(input_tokens: Option<u64>, output_tokens: Option<u64>) -> String {
    match (input_tokens, output_tokens) {
        (None, None) => "unknown".to_string(),
        (input, output) => {
            let fmt = |t: Option<u64>| t.map_or_else(|| "?".to_string(), |n| n.to_string());
            format!("{} in, {} out", fmt(input), fmt(output))
        }
    }
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

/// The outcome of parsing a `baton.message/v1` trail for the cross-trail merge:
/// every envelope found plus any non-fatal diagnostics.
///
/// Unlike [`ParseReport`] (the `baton.exchange/v1` read path), this report is
/// tolerant of *any* bad line — not just a trailing partial one. A merge spans
/// several independently-written trails, so a single corrupt or truncated line
/// in one of them is skipped-with-warning rather than aborting the whole merge.
#[derive(Debug, Default)]
pub struct MessageParseReport {
    /// Every `baton.message/v1` envelope found, in file order.
    pub envelopes: Vec<MessageEnvelope>,
    /// Non-fatal diagnostics (skipped malformed lines), in encounter order.
    pub warnings: Vec<String>,
}

/// Parses one trail file, collecting every `baton.message/v1` envelope it holds.
///
/// A trail may interleave schemas — a file can carry `baton.exchange/v1` event
/// lines beside `baton.message/v1` envelopes — so each line is first parsed as a
/// bare JSON object and dispatched on its `schema`:
///
/// - `schema == "baton.message/v1"` → deserialized into a [`MessageEnvelope`].
///   A shape mismatch here is skipped-with-warning (see below), not a hard error.
/// - **Any other (or absent) `schema`** → silently skipped: it belongs to a
///   different trail and is simply not this mode's concern.
/// - **A malformed line** (invalid UTF-8 / not JSON), or a `baton.message/v1`
///   line that fails to deserialize → skipped and recorded in
///   [`MessageParseReport::warnings`]. This is the robustness contract for the
///   merge: one bad line in one trail must never brick the unified view. (This
///   is deliberately more tolerant than [`parse_jsonl`], which hard-errors on a
///   mid-file malformed line — a single-file inspector can surface corruption,
///   but a cross-trail merge should degrade rather than abort.)
///
/// Pure over its [`Read`]: warnings are returned, not printed, leaving stderr
/// emission to the caller.
pub fn parse_message_trail<R: Read>(reader: R) -> Result<MessageParseReport> {
    let mut buffered = BufReader::new(reader);
    let mut report = MessageParseReport::default();
    let mut buf: Vec<u8> = Vec::new();
    let mut line_no = 0usize;

    loop {
        buf.clear();
        let read = buffered
            .read_until(b'\n', &mut buf)
            .map_err(|err| BatonError::Io(format!("reading trail line {}: {err}", line_no + 1)))?;
        if read == 0 {
            break;
        }
        line_no += 1;

        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        let value = match parse_line_value(&buf) {
            Ok(value) => value,
            Err(detail) => {
                report
                    .warnings
                    .push(format!("skipped malformed trail line {line_no}: {detail}"));
                continue;
            }
        };

        // Only `baton.message/v1` envelopes are this mode's concern; every other
        // schema (e.g. a `baton.exchange/v1` event) is skipped without comment.
        if value.get("schema").and_then(Value::as_str) != Some(MESSAGE_SCHEMA) {
            continue;
        }

        match serde_json::from_value::<MessageEnvelope>(value) {
            Ok(envelope) => report.envelopes.push(envelope),
            Err(err) => report.warnings.push(format!(
                "skipped malformed trail line {line_no}: invalid {MESSAGE_SCHEMA} envelope: {err}"
            )),
        }
    }

    Ok(report)
}

/// Merges the envelopes of one conversation into a single causal-chain–ordered
/// view.
///
/// `envelopes` is the concatenation of every source trail; this filters to
/// `conversation_id`, deduplicates, and orders the result. The ordering rules
/// (from the cross-trail merge contract):
///
/// - **`in_reply_to` is authoritative.** Ordering follows the reply chain, not
///   the clock — across trails from different hosts `ts_ms` is subject to clock
///   skew, so it is never trusted for cross-trail order.
/// - **`ts_ms` is a cosmetic tie-break only**, used to order sibling replies to
///   the same parent (and multiple roots) within a single logical step; ties on
///   `ts_ms` fall back to `message_id` for a deterministic result.
/// - **Duplicates are collapsed by `message_id`.** At-least-once delivery means
///   the same envelope can appear in more than one trail; the first occurrence
///   wins.
/// - A **root** is an envelope whose `in_reply_to` is absent or points to a
///   `message_id` not present in the filtered set (a dangling parent — e.g. the
///   other side of a partially-collected exchange).
/// - A cyclic `in_reply_to` cannot wedge the traversal (a `visited` set guards
///   it); any envelope left unreached is appended, ordered by `(ts_ms,
///   message_id)`, so nothing is silently dropped.
pub fn merge_conversation(
    envelopes: Vec<MessageEnvelope>,
    conversation_id: &str,
) -> Vec<MessageEnvelope> {
    // Filter to the target conversation, collapsing duplicate message_ids
    // (at-least-once delivery) to the first occurrence.
    let mut seen: HashSet<String> = HashSet::new();
    let items: Vec<MessageEnvelope> = envelopes
        .into_iter()
        .filter(|e| e.conversation_id == conversation_id)
        .filter(|e| seen.insert(e.message_id.clone()))
        .collect();

    // Index by message_id so `in_reply_to` links can be resolved and roots
    // (dangling parent) distinguished from replies within the set.
    let index: HashMap<&str, usize> = items
        .iter()
        .enumerate()
        .map(|(i, e)| (e.message_id.as_str(), i))
        .collect();

    // children[parent] = its replies; roots = no parent in the set.
    let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for (i, e) in items.iter().enumerate() {
        match e.in_reply_to.as_deref().and_then(|p| index.get(p)) {
            Some(&parent) => children.entry(parent).or_default().push(i),
            None => roots.push(i),
        }
    }

    // Order roots and each sibling group by the (ts_ms, message_id) tie-break.
    let by_tiebreak = |a: &usize, b: &usize| {
        let (ea, eb) = (&items[*a], &items[*b]);
        ea.ts_ms
            .cmp(&eb.ts_ms)
            .then_with(|| ea.message_id.cmp(&eb.message_id))
    };
    roots.sort_by(by_tiebreak);
    for group in children.values_mut() {
        group.sort_by(by_tiebreak);
    }

    // Pre-order DFS along the reply chain, guarding against a cyclic link.
    let mut ordered: Vec<usize> = Vec::with_capacity(items.len());
    let mut visited: HashSet<usize> = HashSet::new();
    let mut stack: Vec<usize> = roots.into_iter().rev().collect();
    while let Some(i) = stack.pop() {
        if !visited.insert(i) {
            continue;
        }
        ordered.push(i);
        if let Some(group) = children.get(&i) {
            stack.extend(group.iter().rev().copied());
        }
    }

    // Any envelope unreached (only possible under a cycle) is appended so the
    // merge never silently drops a line.
    let mut leftover: Vec<usize> = (0..items.len()).filter(|i| !visited.contains(i)).collect();
    leftover.sort_by(by_tiebreak);
    ordered.extend(leftover);

    // Materialize the ordering. `items` is consumed via index, so swap each out.
    let mut items: Vec<Option<MessageEnvelope>> = items.into_iter().map(Some).collect();
    ordered
        .into_iter()
        .map(|i| items[i].take().expect("each index visited once"))
        .collect()
}

/// Renders one merged message as a human-readable block for `baton log merge`.
///
/// `n` is the 1-based position in the merged view. The header carries the
/// timestamp, `from → to` addressing, the message kind, and the `in_reply_to`
/// link (or `—` for a root); the body follows, excerpted like an exchange block.
pub fn format_message(n: usize, envelope: &MessageEnvelope) -> String {
    const MAX: usize = 120;
    let reply_to = envelope.in_reply_to.as_deref().unwrap_or("—");
    format!(
        "#{n}  {}  {} → {}  {:?}\n    in_reply_to: {reply_to}\n    body: {}\n",
        format_ts(envelope.ts_ms),
        envelope.from,
        envelope.to,
        envelope.kind,
        excerpt(&envelope.body, MAX),
    )
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
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
        assert_eq!(exchanges.len(), 1);
        assert_eq!(
            exchanges[0].request,
            RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-sonnet-4-6".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "hello".to_string(),
                session_id: None,
                turn_index: None,
            }
        );
        assert_eq!(
            exchanges[0].outcome,
            Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "hi there".to_string(),
                input_tokens: None,
                output_tokens: None,
            }
        );
    }

    /// A `response_ok` line carrying a usage block parses the token counts back.
    #[test]
    fn parses_response_ok_token_usage() {
        let log = concat!(
            r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"m","base_url":"u","prompt":"hello"}"#,
            "\n",
            r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"hi","input_tokens":12,"output_tokens":34}"#,
            "\n",
        );
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
        assert_eq!(
            exchanges[0].outcome,
            Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "hi".to_string(),
                input_tokens: Some(12),
                output_tokens: Some(34),
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
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
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
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
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
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
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

    /// A trailing partial line (no terminating newline — the unclean-shutdown
    /// artefact) is tolerated: the complete exchange before it is still yielded
    /// and a warning naming the line is recorded, rather than the whole trail
    /// hard-erroring.
    #[test]
    fn trailing_partial_line_is_tolerated() {
        // One valid exchange, then a truncated `request` with no trailing newline
        // (exactly what a killed mid-write process leaves behind).
        let log = concat!(
            r#"{"event":"request","ts_ms":1,"model":"m","base_url":"u","prompt":"p"}"#,
            "\n",
            r#"{"event":"response_ok","ts_ms":2,"duration_ms":1,"reply":"r"}"#,
            "\n",
            r#"{"event":"request","ts_ms":3,"model":"m","base_url":"u","prom"#,
        );
        let report = parse_jsonl(Cursor::new(log)).expect("tolerates trailing partial");
        assert_eq!(
            report.exchanges.len(),
            1,
            "the complete exchange is yielded"
        );
        assert_eq!(report.warnings.len(), 1, "the skipped line is warned about");
        assert!(
            report.warnings[0].contains("line 3"),
            "warning names the skipped line: {}",
            report.warnings[0]
        );
    }

    /// A trailing partial line truncated mid-multibyte (invalid UTF-8) is also
    /// tolerated, not surfaced as a hard error.
    #[test]
    fn trailing_partial_invalid_utf8_is_tolerated() {
        // One valid exchange, then trailing bytes ending mid-UTF-8-sequence with
        // no newline: 0xe6 starts a 3-byte sequence that is never completed.
        let mut log: Vec<u8> = Vec::new();
        log.extend_from_slice(
            b"{\"event\":\"request\",\"ts_ms\":1,\"model\":\"m\",\"base_url\":\"u\",\"prompt\":\"p\"}\n",
        );
        log.extend_from_slice(
            b"{\"event\":\"response_ok\",\"ts_ms\":2,\"duration_ms\":1,\"reply\":\"r\"}\n",
        );
        log.extend_from_slice(b"{\"event\":\"request\",\"ts_ms\":3,\"prompt\":\"");
        log.push(0xe6); // truncated start of a 3-byte UTF-8 sequence

        let report =
            parse_jsonl(Cursor::new(log)).expect("tolerates invalid-utf8 trailing partial");
        assert_eq!(report.exchanges.len(), 1);
        assert_eq!(report.warnings.len(), 1);
        assert!(
            report.warnings[0].contains("line 3"),
            "warning names the skipped line: {}",
            report.warnings[0]
        );
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
        let exchanges = parse_jsonl(Cursor::new(log)).expect("parses").exchanges;
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
                session_id: None,
                turn_index: None,
            },
            outcome: Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "the answer".to_string(),
                input_tokens: Some(12),
                output_tokens: Some(34),
            },
        };
        let rendered = format_exchange(1, &ex);
        assert!(rendered.contains("#1"));
        assert!(rendered.contains("2023-11-14T22:13:20Z"));
        assert!(rendered.contains("claude-sonnet-4-6"));
        assert!(rendered.contains("the question"));
        assert!(rendered.contains("the answer"));
        assert!(rendered.contains("418ms"));
        assert!(rendered.contains("12 in, 34 out"), "got: {rendered}");
    }

    #[test]
    fn format_exchange_renders_unknown_tokens_when_usage_absent() {
        let ex = Exchange {
            request: RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-sonnet-4-6".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "q".to_string(),
                session_id: None,
                turn_index: None,
            },
            outcome: Outcome::Ok {
                ts_ms: 1_700_000_000_420,
                duration_ms: 418,
                reply: "a".to_string(),
                input_tokens: None,
                output_tokens: None,
            },
        };
        let rendered = format_exchange(1, &ex);
        assert!(rendered.contains("tokens: unknown"), "got: {rendered}");
    }

    #[test]
    fn format_exchange_renders_error_kind_for_failure() {
        let ex = Exchange {
            request: RequestRecord {
                ts_ms: 0,
                model: "m".to_string(),
                base_url: "u".to_string(),
                prompt: "p".to_string(),
                session_id: None,
                turn_index: None,
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

    // ---- cross-trail merge (`baton log merge`) -------------------------------

    use crate::message::MessageKind;

    /// Builds a `baton.message/v1` envelope JSONL line with an explicit
    /// `in_reply_to` (or `null`) for the merge tests.
    fn msg_line(
        message_id: &str,
        conversation_id: &str,
        in_reply_to: Option<&str>,
        ts_ms: u64,
        body: &str,
    ) -> String {
        let mut env = MessageEnvelope::new(
            message_id,
            conversation_id,
            "agent-a",
            "agent-b",
            MessageKind::Request,
            body,
            ts_ms,
        );
        env.in_reply_to = in_reply_to.map(str::to_string);
        serde_json::to_string(&env).expect("serializes")
    }

    /// Only `baton.message/v1` lines are collected; a `baton.exchange/v1` event
    /// line interleaved in the same trail is skipped without a warning.
    #[test]
    fn parse_message_trail_collects_only_message_envelopes() {
        let trail = format!(
            "{}\n{}\n{}\n",
            msg_line("m-1", "c-1", None, 1, "hello"),
            r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":2,"model":"m","base_url":"u","prompt":"p"}"#,
            msg_line("m-2", "c-1", Some("m-1"), 3, "hi"),
        );
        let report = parse_message_trail(Cursor::new(trail)).expect("parses");
        assert_eq!(report.envelopes.len(), 2);
        assert!(report.warnings.is_empty(), "exchange line skipped silently");
        assert_eq!(report.envelopes[0].message_id, "m-1");
        assert_eq!(report.envelopes[1].message_id, "m-2");
    }

    /// A malformed line — including a mid-file one — is skipped-with-warning and
    /// does not drop the valid envelopes around it (the merge robustness contract).
    #[test]
    fn parse_message_trail_tolerates_malformed_line() {
        let trail = format!(
            "{}\n<<<not json>>>\n{}\n",
            msg_line("m-1", "c-1", None, 1, "a"),
            msg_line("m-2", "c-1", Some("m-1"), 2, "b"),
        );
        let report = parse_message_trail(Cursor::new(trail)).expect("tolerates malformed");
        assert_eq!(report.envelopes.len(), 2, "valid envelopes survive");
        assert_eq!(report.warnings.len(), 1);
        assert!(
            report.warnings[0].contains("line 2"),
            "{}",
            report.warnings[0]
        );
    }

    /// A `baton.message/v1` line that cannot deserialize into an envelope is a
    /// skipped-with-warning line, not a hard error.
    #[test]
    fn parse_message_trail_warns_on_bad_message_envelope() {
        let trail = concat!(r#"{"schema":"baton.message/v1","message_id":"m-1"}"#, "\n",);
        let report = parse_message_trail(Cursor::new(trail)).expect("does not hard-error");
        assert!(report.envelopes.is_empty());
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("baton.message/v1"));
    }

    /// Envelopes split across two trails interleave into one chain-ordered view,
    /// following `in_reply_to` rather than the order the trails were concatenated.
    #[test]
    fn merge_interleaves_trails_in_causal_order() {
        // Trail order deliberately shuffles the causal chain m0→m1→m2→m3.
        let env = |id: &str, parent: Option<&str>, ts: u64| {
            let mut e = MessageEnvelope::new(id, "c-1", "a", "b", MessageKind::Request, "x", ts);
            e.in_reply_to = parent.map(|p| p.to_string());
            e
        };
        let envelopes = vec![
            env("m-2", Some("m-1"), 30),
            env("m-0", None, 10),
            env("m-3", Some("m-2"), 40),
            env("m-1", Some("m-0"), 20),
        ];
        let merged = merge_conversation(envelopes, "c-1");
        let ids: Vec<&str> = merged.iter().map(|e| e.message_id.as_str()).collect();
        assert_eq!(ids, ["m-0", "m-1", "m-2", "m-3"]);
    }

    /// Only the selected conversation is included; other conversations drop out.
    #[test]
    fn merge_filters_by_conversation_id() {
        let mut a = MessageEnvelope::new("m-1", "c-1", "a", "b", MessageKind::Request, "x", 1);
        let mut b = MessageEnvelope::new("m-2", "c-2", "a", "b", MessageKind::Request, "y", 2);
        a.in_reply_to = None;
        b.in_reply_to = None;
        let merged = merge_conversation(vec![a, b], "c-1");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].conversation_id, "c-1");
    }

    /// `ts_ms` breaks ties only among siblings sharing a parent — two replies to
    /// the same message order by timestamp, earliest first.
    #[test]
    fn merge_uses_ts_ms_only_as_sibling_tiebreak() {
        let root = MessageEnvelope::new("m-0", "c-1", "a", "b", MessageKind::Request, "root", 100);
        let mut late =
            MessageEnvelope::new("m-2", "c-1", "b", "a", MessageKind::Response, "late", 300);
        let mut early =
            MessageEnvelope::new("m-1", "c-1", "b", "a", MessageKind::Response, "early", 200);
        late.in_reply_to = Some("m-0".to_string());
        early.in_reply_to = Some("m-0".to_string());
        // Feed the later sibling first; the tie-break must still order early→late.
        let merged = merge_conversation(vec![root, late, early], "c-1");
        let ids: Vec<&str> = merged.iter().map(|e| e.message_id.as_str()).collect();
        assert_eq!(ids, ["m-0", "m-1", "m-2"]);
    }

    /// A `message_id` repeated across trails (at-least-once delivery) is
    /// collapsed to its first occurrence.
    #[test]
    fn merge_deduplicates_repeated_message_id() {
        let one = MessageEnvelope::new("m-1", "c-1", "a", "b", MessageKind::Request, "first", 1);
        let dup = MessageEnvelope::new("m-1", "c-1", "a", "b", MessageKind::Request, "dup", 1);
        let merged = merge_conversation(vec![one, dup], "c-1");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].body, "first");
    }

    /// An `in_reply_to` pointing outside the collected set (a dangling parent —
    /// e.g. the other side of a half-collected exchange) is treated as a root,
    /// not dropped.
    #[test]
    fn merge_treats_dangling_parent_as_root() {
        let mut e =
            MessageEnvelope::new("m-1", "c-1", "a", "b", MessageKind::Response, "orphan", 5);
        e.in_reply_to = Some("m-absent".to_string());
        let merged = merge_conversation(vec![e], "c-1");
        assert_eq!(merged.len(), 1, "dangling reply still surfaces");
        assert_eq!(merged[0].message_id, "m-1");
    }

    #[test]
    fn format_message_includes_addressing_kind_and_reply_link() {
        let mut e = MessageEnvelope::new(
            "m-2",
            "c-1",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "the answer",
            1_700_000_000_000,
        );
        e.in_reply_to = Some("m-1".to_string());
        let rendered = format_message(2, &e);
        assert!(rendered.contains("#2"));
        assert!(rendered.contains("2023-11-14T22:13:20Z"));
        assert!(rendered.contains("agent-b → agent-a"));
        assert!(rendered.contains("Response"));
        assert!(rendered.contains("in_reply_to: m-1"));
        assert!(rendered.contains("the answer"));
    }

    #[test]
    fn format_message_renders_dash_for_root_reply_link() {
        let e = MessageEnvelope::new("m-0", "c-1", "a", "b", MessageKind::Request, "seed", 0);
        let rendered = format_message(1, &e);
        assert!(rendered.contains("in_reply_to: —"), "got: {rendered}");
    }

    // -- parse_sessions ----------------------------------------------------

    use crate::events::{ExchangeEvent, ExchangeMeta};
    use crate::model::TokenUsage;

    /// Serializes an event to its exact on-disk JSONL line (no trailing newline),
    /// so these tests exercise the same bytes `WriterSink` writes.
    fn line(event: &ExchangeEvent) -> String {
        serde_json::to_string(event).expect("event serializes")
    }

    /// A full session frame (start, one turn, end) partitions into one session
    /// carrying its id, the turn's index/prompt/reply, and both markers.
    #[test]
    fn parse_sessions_reconstructs_one_framed_session() {
        let meta = ExchangeMeta {
            model: "m".to_string(),
            base_url: "u".to_string(),
        };
        let trail = [
            line(&ExchangeEvent::session_start(1, "sess-A")),
            line(&ExchangeEvent::session_request(2, &meta, "hi", "sess-A", 0)),
            line(&ExchangeEvent::response_ok(
                3,
                10,
                "hello",
                &TokenUsage::default(),
            )),
            line(&ExchangeEvent::session_end(4, "sess-A", 1)),
        ]
        .join("\n")
            + "\n";

        let report = parse_sessions(Cursor::new(trail)).expect("parses");
        assert_eq!(report.sessions.len(), 1);
        let s = &report.sessions[0];
        assert_eq!(s.session_id, "sess-A");
        assert!(s.started && s.ended);
        assert_eq!(s.declared_turns, Some(1));
        assert_eq!(s.turns.len(), 1);
        assert_eq!(s.turns[0].request.turn_index, Some(0));
        assert_eq!(s.turns[0].request.prompt, "hi");
        assert_eq!(
            s.turns[0].outcome,
            Some(Outcome::Ok {
                ts_ms: 3,
                duration_ms: 10,
                reply: "hello".to_string(),
                input_tokens: None,
                output_tokens: None,
            })
        );
    }

    /// Two sessions appended to one file separate unambiguously by `session_id`
    /// and their start/end markers — no reliance on line ordering.
    #[test]
    fn parse_sessions_separates_two_sequential_sessions() {
        let meta = ExchangeMeta {
            model: "m".to_string(),
            base_url: "u".to_string(),
        };
        let trail = [
            line(&ExchangeEvent::session_start(1, "sess-A")),
            line(&ExchangeEvent::session_request(2, &meta, "a0", "sess-A", 0)),
            line(&ExchangeEvent::response_ok(
                3,
                1,
                "ra0",
                &TokenUsage::default(),
            )),
            line(&ExchangeEvent::session_end(4, "sess-A", 1)),
            line(&ExchangeEvent::session_start(5, "sess-B")),
            line(&ExchangeEvent::session_request(6, &meta, "b0", "sess-B", 0)),
            line(&ExchangeEvent::response_ok(
                7,
                1,
                "rb0",
                &TokenUsage::default(),
            )),
            line(&ExchangeEvent::session_request(8, &meta, "b1", "sess-B", 1)),
            line(&ExchangeEvent::response_ok(
                9,
                1,
                "rb1",
                &TokenUsage::default(),
            )),
            line(&ExchangeEvent::session_end(10, "sess-B", 2)),
        ]
        .join("\n")
            + "\n";

        let report = parse_sessions(Cursor::new(trail)).expect("parses");
        assert_eq!(report.sessions.len(), 2, "two distinct session_ids");
        assert_eq!(report.sessions[0].session_id, "sess-A");
        assert_eq!(report.sessions[0].turns.len(), 1);
        assert_eq!(report.sessions[1].session_id, "sess-B");
        assert_eq!(report.sessions[1].turns.len(), 2);
        assert_eq!(report.sessions[1].turns[1].request.prompt, "b1");
        assert_eq!(report.sessions[1].declared_turns, Some(2));
    }

    /// A session killed mid-run — a `session_start` and turns but no
    /// `session_end` — still partitions into one whole session with
    /// `ended == false`. Partitioning keys on `session_id`, not a matched pair.
    #[test]
    fn parse_sessions_keeps_killed_session_without_end_marker() {
        let meta = ExchangeMeta {
            model: "m".to_string(),
            base_url: "u".to_string(),
        };
        let trail = [
            line(&ExchangeEvent::session_start(1, "sess-A")),
            line(&ExchangeEvent::session_request(2, &meta, "hi", "sess-A", 0)),
            line(&ExchangeEvent::response_ok(
                3,
                1,
                "hello",
                &TokenUsage::default(),
            )),
        ]
        .join("\n")
            + "\n";

        let report = parse_sessions(Cursor::new(trail)).expect("parses");
        assert_eq!(report.sessions.len(), 1);
        let s = &report.sessions[0];
        assert!(s.started, "start marker was present");
        assert!(!s.ended, "no end marker for a killed session");
        assert_eq!(s.declared_turns, None);
        assert_eq!(s.turns.len(), 1, "the completed turn is still captured");
    }

    /// A torn final `session_end` line (killed mid-`write_all`, no terminating
    /// newline) is skipped-with-warning: the session's turns survive and the
    /// trail stays valid, proving flush-per-line partial-trail tolerance end to
    /// end.
    #[test]
    fn parse_sessions_tolerates_torn_trailing_session_end() {
        let meta = ExchangeMeta {
            model: "m".to_string(),
            base_url: "u".to_string(),
        };
        let full_end = line(&ExchangeEvent::session_end(4, "sess-A", 1));
        // Keep the leading `{` so the line is JSON-shaped but truncated, and drop
        // the terminating newline — the signature of an unclean shutdown.
        let torn_end = &full_end[..full_end.len() / 2];
        let trail = [
            line(&ExchangeEvent::session_start(1, "sess-A")),
            line(&ExchangeEvent::session_request(2, &meta, "hi", "sess-A", 0)),
            line(&ExchangeEvent::response_ok(
                3,
                1,
                "hello",
                &TokenUsage::default(),
            )),
        ]
        .join("\n")
            + "\n"
            + torn_end; // no trailing newline

        let report = parse_sessions(Cursor::new(trail)).expect("torn tail is tolerated");
        assert_eq!(report.warnings.len(), 1, "the torn line is reported");
        assert_eq!(report.sessions.len(), 1);
        let s = &report.sessions[0];
        assert!(s.started && !s.ended, "the end marker never landed");
        assert_eq!(s.turns.len(), 1);
        assert_eq!(
            s.turns[0].outcome.as_ref().map(|_| ()),
            Some(()),
            "the completed turn's outcome survives the torn tail"
        );
    }

    /// Sessionless `ask` lines carry no `session_id`, so `parse_sessions` skips
    /// them: they belong to no session and their outcome never attaches to one.
    #[test]
    fn parse_sessions_ignores_sessionless_ask_lines() {
        let ask = concat!(
            r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1,"model":"m","base_url":"u","prompt":"ask"}"#,
            "\n",
            r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":2,"duration_ms":1,"reply":"a"}"#,
            "\n",
        );
        let report = parse_sessions(Cursor::new(ask)).expect("parses");
        assert!(
            report.sessions.is_empty(),
            "ask lines form no session, got: {:?}",
            report.sessions
        );
    }
}
