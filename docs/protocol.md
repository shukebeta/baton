# Baton wire protocol & schema reference

This is the authoritative reference for Baton's versioned **wire contracts** —
the schemas other tools serialize against. Each is version-suffixed and changes
under compatibility pressure, independent of CLI ergonomics; a consumer
implementing against them should not have to read the getting-started
walkthrough in [`README.md`](../README.md) to find a field. It is written to
stand on its own.

Two schemas and their trail:

- `baton.exchange/v1` — one provider *call* (request + outcome), recorded as a
  JSONL trail.
- `baton.message/v1` — one agent-to-agent *peer message* envelope, which nests
  a `baton.exchange/v1` record.

## Structured exchange events

Baton can record each exchange as a machine-readable trail so a single
request/response can be programmatically inspected or replayed, and so failures
are captured explicitly rather than lost in human-oriented output. Both `ask`
and `session` record; a session emits one `request`/outcome pair per turn.

Recording is **opt-in**: set `BATON_EVENT_LOG` to a file path. Each run appends
its events to that file, so successive runs accumulate one trail.

```bash
export ANTHROPIC_API_KEY=sk-...
export BATON_EVENT_LOG=baton-events.jsonl
cargo run -- ask -p "hello"
cat baton-events.jsonl
```

The format is [JSONL](https://jsonlines.org/): one JSON object per line. Each
line carries a `schema` discriminator (`baton.exchange/v1`), an `event` tag, and
a `ts_ms` wall-clock timestamp (Unix epoch milliseconds). One exchange emits a
`request` line followed by exactly one outcome line (`response_ok` or
`response_error`):

```jsonl
{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"hello"}
{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"Hi there!","input_tokens":9,"output_tokens":3}
```

| `event`          | Fields beyond `schema` / `ts_ms`                            | Meaning                                              |
| ---------------- | ----------------------------------------------------------- | ---------------------------------------------------- |
| `request`        | `model`, `base_url`, `prompt`, `session_id?`, `turn_index?` | Emitted before the call; carries enough to replay it. `session_id` / `turn_index` are present only on a `session` turn (see below). |
| `response_ok`    | `duration_ms`, `reply`, `input_tokens`, `output_tokens`     | The call succeeded.                                  |
| `response_error` | `duration_ms`, `kind`, `message`                            | The call failed; `kind` is the stable machine class. |
| `session_start`  | `session_id`                                                | Emitted once at the start of a `baton session` run.  |
| `session_end`    | `session_id`, `turns`                                       | Emitted once on a clean session exit (EOF / `/exit`); `turns` is the turn count. |

`input_tokens` / `output_tokens` are the provider-reported token counts for the
call. They are **optional**: a `2xx` response that omits the `usage` block (or a
field within it) still succeeds, and the missing count is simply left off the
`response_ok` line rather than failing the exchange — so a consumer must treat
either field as possibly absent.

A `response_error` event's `kind` is one of the six classes an exchange can
actually fail with (`transport`, `auth`, `rate_limited`, `server`, `api`,
`decode`), so consumers can branch on the failure class without parsing the
human-readable `message`. The other `BatonError` kinds (`config`, `usage`, `io`,
`log`) arise only outside an exchange — at startup, CLI parsing, REPL
stdin/stdout I/O, or event-log parsing — where no `response_error` event is
emitted, so a trail consumer will never see them in `kind`.

**Consumption model.** Read the file line by line; parse each line as a
standalone JSON object. A trailing partial line — one with no terminating
newline, left behind when a `baton ask`/`session` process is killed mid-write —
can be skipped; `baton log` itself does this (emitting a stderr warning naming
the line), so an unclean shutdown never bricks the whole trail. The event trail
is auxiliary observability — it is written to the configured file
only, never to stdout, and a failed log write degrades to a stderr warning
rather than failing the command. The schema is per-exchange: each line is one
request or one outcome, and a session turn's `request` carries that turn's user
input as `prompt` (the full accumulated history is not aggregated into a single
schema object).

### Session trail

A `baton session` run frames its turns so a single file — or a shared append log
holding several runs — is unambiguously partitionable back into whole sessions,
without guessing from line ordering:

- One `session_start` line opens the run and stamps a `session_id` unique to that
  process.
- Each turn's `request` carries that same `session_id` plus a monotonic
  `turn_index` (starting at 0), so every turn is placed within its session. A
  failed turn still emits its `request` and advances the index.
- One `session_end` line closes the run on a clean exit (EOF / `/exit`), carrying
  the `session_id` and the total `turns` count.

```jsonl
{"event":"session_start","schema":"baton.exchange/v1","ts_ms":1700000000000,"session_id":"sess-4171-1700000000000"}
{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000001,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"hello","session_id":"sess-4171-1700000000000","turn_index":0}
{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"Hi there!","input_tokens":9,"output_tokens":3}
{"event":"session_end","schema":"baton.exchange/v1","ts_ms":1700000000500,"session_id":"sess-4171-1700000000000","turns":1}
```

Partitioning keys on `session_id`, **not** on a matched start/end pair: a session
killed mid-run leaves a `session_start` and its turns but no `session_end`, and is
still recovered as one whole session (its final line may be torn — the same
partial-trail tolerance above covers it). The `ask` path is unframed: its
`request` line omits `session_id` / `turn_index` and belongs to no session. The
`session_start` / `session_end` markers ride the same `baton.exchange/v1` trail;
`baton log show` / `replay` skip them, so they are unaffected.

## Inspecting and replaying the trail

`baton log` makes the recorded trail a first-class artefact — no `jq` or
hand-rolled parser required. Both subcommands read the JSONL file named by
`--file <path>`, falling back to `BATON_EVENT_LOG` when `--file` is absent; with
neither set there is nothing to read and the command exits with a usage error.

**`baton log show`** pretty-prints each exchange — a `request` paired with its
single outcome — as a human-readable block: a 1-based index, a UTC timestamp,
the model, the call duration, and a truncated prompt with either a truncated
reply plus its token counts or the failure (`kind: message`).

```bash
export BATON_EVENT_LOG=baton-events.jsonl
baton log show
```

```text
#1  2023-11-14T22:13:20Z  claude-sonnet-4-6  (418ms)
    prompt: who won the 1998 world cup?
    reply:  France.
    tokens: 9 in, 3 out
```

The `tokens:` line shows the reported input/output counts; a call whose response
carried no usage renders `tokens: unknown`.

**`baton log replay [--index <N>]`** re-runs a recorded exchange. It reads the
`model`, `base_url`, and `prompt` from the chosen `request` event and sends a
fresh single-turn request with the **current** credential (and timeout /
`max_tokens` / system prompt from the environment) — so a replay re-runs with
today's auth, not a credential that was never recorded. `N` is 1-based and
defaults to the last exchange; an out-of-range `N` errors and names the valid
range. The reply is printed to stdout (same contract as `baton ask`), and the
replay itself is appended to `BATON_EVENT_LOG` as a new exchange.

```bash
baton log replay              # re-run the last recorded exchange
baton log replay --index 1    # re-run the first
```

Unknown `event` tags are skipped when reading (forward-compatibility with a
newer writer). A line that is not valid JSON is a hard parse error naming the
offending line — except a trailing partial line (no terminating newline, the sign
of a `baton ask`/`session` process killed mid-write), which is skipped with a
stderr warning so one unclean shutdown can't brick the whole trail. Diffing,
filtering, and non-JSONL export remain out of scope.

### Cross-trail merge (`baton log merge`)

The moment a conversation goes async it is split across ≥2 trails — the
initiator's and each `baton serve` daemon's, possibly on different hosts. `baton
log merge` reunites them: it reads the `baton.message/v1` envelopes (written by
`converse`, `exchange`, and `send`) out of several trail files and presents every
one sharing a `--conversation <id>` as a single turn-ordered view.

```bash
baton log merge --conversation c-1 initiator-trail.jsonl peer-trail.jsonl
baton log merge --conversation c-1 ./trails/     # a directory expands to its files
```

```text
#1  2023-11-14T22:13:20Z  agent-a → agent-b  Request
    in_reply_to: —
    body: who won the 1998 world cup?
#2  2023-11-14T22:13:21Z  agent-b → agent-a  Response
    in_reply_to: m-0
    body: France
```

Ordering follows the **`in_reply_to` causal chain** as the authoritative order:
each message is placed after the one it replies to, so envelopes from different
source trails interleave into the one true sequence. Across trails from different
hosts `ts_ms` is subject to clock skew and is therefore **never** trusted for
cross-trail order — it is only a cosmetic tie-break between sibling replies to the
same parent (falling back to `message_id`). A message whose `in_reply_to` points
outside the collected set is treated as a root, and a `message_id` that appears in
more than one trail (at-least-once delivery) is collapsed to its first occurrence.

Each argument is a trail file, or a directory whose files are each read. Only
`baton.message/v1` lines are considered; a line of any other schema (e.g. a
`baton.exchange/v1` event) is ignored. Unlike `show`/`replay`, **any** malformed
line — not just a trailing partial one — is skipped with a stderr warning rather
than aborting, so one corrupt line in one trail never bricks the merge. Live
tailing and cross-host trail fetch remain out of scope.

## A2A message envelope (`baton.message/v1`)

Where `baton.exchange/v1` (above) describes a single provider *call*,
`baton.message/v1` describes an agent-to-agent *peer message* — the lingua franca
the exchange verb and driver share. It is a contract only: the envelope types
carry no transport, I/O, or addressing semantics.

An envelope carries a `schema` discriminator (`baton.message/v1`), a
`message_id`, the `conversation_id` it belongs to, `from` / `to` addresses, a
nullable `in_reply_to` linking it to the message it answers, a `kind`, a `body`,
and a `ts_ms` wall-clock timestamp (Unix epoch milliseconds).

| Field           | Type              | Meaning                                                        |
| --------------- | ----------------- | -------------------------------------------------------------- |
| `schema`        | string            | Discriminator, `baton.message/v1`.                             |
| `message_id`    | string            | Unique id of this message.                                     |
| `conversation_id` | string          | The conversation this message belongs to.                      |
| `from` / `to`   | string            | Sender / recipient address.                                    |
| `in_reply_to`   | string \| null    | The `message_id` this replies to, or `null`.                   |
| `kind`          | string            | One of `request`, `response`, `done`, `error` (see below).     |
| `body`          | string            | The message body.                                              |
| `ts_ms`         | number            | Emission time, Unix epoch milliseconds.                        |
| `exchange`      | object \| null    | The wrapped provider call, or `null` (see nesting below).      |

`kind` is one of four variants: `request` (asks the peer to act), `response`
(answers a prior `request`), and the terminal markers `done` (turn complete) and
`error` (turn failed). (`notify` is intentionally not part of this slice; the
unknown-field skip below lets it be added later without a schema break.)

### Nesting over `baton.exchange/v1`

The envelope is nested **over** the exchange trail: one peer message may wrap
zero-or-one provider-call record. A message that triggered an LLM call carries
the resulting exchange under `exchange`, a self-describing object pairing the
`baton.exchange/v1` discriminator with that call's `request` and terminal
`outcome`; a message that triggered no call leaves `exchange` as `null`.

```json
{
  "schema": "baton.message/v1",
  "message_id": "m-1",
  "conversation_id": "c-1",
  "from": "agent-a",
  "to": "agent-b",
  "in_reply_to": null,
  "kind": "response",
  "body": "France.",
  "ts_ms": 1700000000420,
  "exchange": {
    "schema": "baton.exchange/v1",
    "exchange": {
      "request": {
        "ts_ms": 1700000000000,
        "model": "claude-sonnet-4-6",
        "base_url": "https://api.anthropic.com",
        "prompt": "who won the 1998 world cup?"
      },
      "outcome": {
        "event": "response_ok",
        "ts_ms": 1700000000420,
        "duration_ms": 418,
        "reply": "France.",
        "input_tokens": 9,
        "output_tokens": 3
      }
    }
  }
}
```

The nested `outcome` uses the same `event` tags as the exchange trail
(`response_ok` / `response_error`). As with that trail, unknown/extra fields are
skipped on read (forward-compatibility) rather than treated as errors.
