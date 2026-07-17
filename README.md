# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

Early scaffolding. The crate establishes the module layout and typed runtime
shape around a non-streaming Claude-compatible Messages client
(`transport::claude::ClaudeClient`). Its commands wire it to the command line:
`baton ask` for a single-turn first-prompt / first-reply, `baton session` for an
interactive multi-turn REPL that accumulates conversation history across turns,
`baton exchange` for one structured `baton.message/v1` request/reply round-trip,
`baton converse` for a governed two-participant conversation driven to a terminal
condition, and `baton log` for inspecting and replaying the recorded exchange
trail.

## Configuration

Baton reads its runtime configuration from environment variables:

| Variable                     | Required | Default                     | Purpose                                              |
| ---------------------------- | -------- | --------------------------- | ---------------------------------------------------- |
| `ANTHROPIC_API_KEY`          | one of three | —                       | Provider API key. Must be set and non-empty.         |
| `ANTHROPIC_AUTH_TOKEN`       | one of three | —                       | OAuth bearer token. Must be set and non-empty.       |
| `CLAUDE_CODE_OAUTH_TOKEN`    | one of three | —                       | OAuth bearer token (Claude Code subscription).       |
| `ANTHROPIC_BASE_URL`         | no       | `https://api.anthropic.com` | Base URL for the Claude-compatible Messages API.     |
| `BATON_MODEL`                | no       | `claude-sonnet-4-6`         | Model id to request.                                 |
| `BATON_TIMEOUT_SECS`         | no       | `60`                        | Per-request timeout in seconds (positive integer; zero is rejected). |
| `BATON_MAX_TOKENS`           | no       | `1024`                      | Maximum output tokens to request per reply (positive integer; zero is rejected). |
| `BATON_SYSTEM_PROMPT`        | no       | — (no system prompt)        | Path to a markdown file whose content is sent as the request's `system` field. Missing/unreadable file is a startup error. |
| `BATON_MAX_TURNS`            | no       | `8`                         | `baton converse` hard turn-cap: the maximum number of reply turns before the run ends (positive integer; zero is rejected). |
| `BATON_TOKEN_BUDGET`         | no       | — (disabled)                | `baton converse` cumulative token budget across all replies' reported usage; the run ends once it is exceeded (positive integer; zero is rejected). Unset disables the arm. |
| `BATON_EVENT_LOG`            | no       | — (disabled)                | File path for the JSONL exchange-event trail (see below). |

Exactly one credential variable is required. The first one that is set (in
precedence `ANTHROPIC_API_KEY` > `ANTHROPIC_AUTH_TOKEN` > `CLAUDE_CODE_OAUTH_TOKEN`)
wins; the others are ignored. A credential variable that is exported but blank
or whitespace-only is an error, even if a later candidate is valid — exporting
an empty value is almost always a misconfiguration rather than an explicit
"skip me" signal.

Missing or invalid values are surfaced as explicit configuration errors at
startup rather than failing later.

`BATON_SYSTEM_PROMPT` gives an agent an identity, role constraints, or
output-format instructions. It is a **file path**, not a raw string — system
prompts are usually multi-paragraph documents better kept under version control
than squeezed into an environment variable. The file is read at startup; its
content becomes the request's `system` field. Unset or blank means no system
field is sent (the prior behaviour). A path to a missing or unreadable file is a
configuration error that fails before any network call.

## First reply

`baton ask` sends a single prompt and prints the assistant's reply.

With an Anthropic API key:

```bash
export ANTHROPIC_API_KEY=sk-...
cargo run -- ask -p "hello"
```

With an OAuth bearer token (Claude Code subscription or `ANTHROPIC_AUTH_TOKEN`):

```bash
export CLAUDE_CODE_OAUTH_TOKEN=...
cargo run -- ask -p "hello"
```

- One prompt in, one reply out — no REPL, conversation state, streaming, or
  tool execution.
- On success, **stdout contains only the assistant text** (followed by a single
  newline). The prompt is taken from `-p` / `--prompt` (the `--prompt=<text>`
  form is also accepted).
- On failure (bad arguments, missing configuration, or a provider/transport
  error) Baton prints the error to **stderr** and exits with a non-zero status;
  stdout stays empty.

## Multi-turn session

`baton session` is an interactive REPL that keeps a conversation in memory and
resends the full history on every request, so the assistant has the context of
all prior turns.

```bash
export ANTHROPIC_API_KEY=sk-...
cargo run -- session
```

```text
baton session — type a message and press enter; Ctrl-D or /exit to quit
who won the 1998 world cup?
France.
and who did they beat in the final?
Brazil, 3–0.
/exit
```

- Each line you enter is appended to the history as a `user` turn; the
  assistant's reply is printed and appended as an `assistant` turn. Turn N's
  request carries every prior user and assistant turn.
- `BATON_SYSTEM_PROMPT` (if set) is sent as the `system` field on **every**
  request, same as the `ask` path.
- A blank line is ignored. The session ends — cleanly, with exit code 0 — on
  EOF (`Ctrl-D`) or a lone `/exit` line.
- A turn that fails (rate limit, transport error, …) is **not** fatal: the
  error is printed to stderr, the failed turn is dropped from the history, and
  the REPL continues so you can retry.
- History lives only in memory: it is not persisted across process restarts,
  and there are no named sessions or session IDs yet.

## Provider transport

`transport::claude::ClaudeClient` implements the `Transport` trait against a
Claude-compatible non-streaming `POST /v1/messages` endpoint:

- Authenticates with the `x-api-key` header when the resolved credential is
  an API key, or with `Authorization: Bearer <token>` when the resolved
  credential is an OAuth token. Pins the `anthropic-version: 2023-06-01`
  header in either case.
- Sends to `{ANTHROPIC_BASE_URL}/v1/messages`, requesting the configured
  `BATON_MODEL`.
- Requests up to `BATON_MAX_TOKENS` output tokens per reply (default 1024) and
  extracts the assistant's text from the response's `content` blocks.

Failures are surfaced as explicit `BatonError` variants rather than silent
fallbacks:

| Condition                         | Error                          |
| --------------------------------- | ------------------------------ |
| Connection / TLS / timeout        | `Transport`                    |
| 401 Unauthorized                  | `Auth`                         |
| 429 Too Many Requests             | `RateLimited`                  |
| 5xx server failure                | `Server { status, .. }`        |
| Other non-2xx (e.g. 400)          | `Api { status, .. }`           |
| Malformed or text-less 2xx body   | `Decode`                       |

The client sends one or more conversation turns and decodes one reply;
`baton session` builds the multi-turn history on top of it. Streaming and tool
calling remain out of scope for this client.

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

| `event`          | Fields beyond `schema` / `ts_ms`                          | Meaning                                              |
| ---------------- | --------------------------------------------------------- | ---------------------------------------------------- |
| `request`        | `model`, `base_url`, `prompt`                             | Emitted before the call; carries enough to replay it. |
| `response_ok`    | `duration_ms`, `reply`, `input_tokens`, `output_tokens`   | The call succeeded.                                  |
| `response_error` | `duration_ms`, `kind`, `message`                          | The call failed; `kind` is the stable machine class. |

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

## Exchanging envelopes (`baton exchange`)

`baton exchange` is the structured request/reply verb: it reads exactly one
`baton.message/v1` request envelope, runs the provider call for its `body`, and
writes exactly one response envelope. Unlike `ask` (prose on stdout), both sides
of `exchange` are machine-readable envelopes — this is the primitive one Baton
process uses to reach another over pipes, with no tmux and no daemon.

```
baton exchange [--in <path>] [--out <path>]
```

The request is read from `--in <path>` when given, else stdin; the response is
written to `--out <path>` when given, else stdout. `BATON_SYSTEM_PROMPT` applies
exactly as on `ask`, so a spawned `baton exchange` is an independently-configured
participant.

```bash
echo '{"schema":"baton.message/v1","message_id":"m-1","conversation_id":"c-1","from":"agent-a","to":"agent-b","in_reply_to":null,"kind":"request","body":"who won the 1998 world cup?","ts_ms":1700000000000,"exchange":null}' \
  | baton exchange
```

The response envelope:

- is `kind: "response"` on success (its `body` is the assistant reply) or
  `kind: "error"` when the provider call fails (its `body` is the error
  description);
- preserves the request's `conversation_id`, sets `in_reply_to` to the request's
  `message_id`, and carries a fresh `message_id`;
- **swaps addressing** — the reply's `from` is the request's `to`, and its `to`
  is the request's `from`;
- wraps the provider call it ran under `exchange` (the `baton.exchange/v1` record
  with its token usage), so the call is observable in-band, not only in the
  `BATON_EVENT_LOG` trail (which still records the same request→outcome pair as
  `ask`).

### Delivered-error exit semantics

A provider failure is a *delivered response*, not a process failure: a
well-formed request whose provider call fails writes a `kind: "error"` response
envelope to stdout and **exits 0**. The caller reads the outcome from the
envelope, not from the exit code. Only a malformed or unreadable request
envelope — or a usage/CLI error — exits **non-zero**, with a stderr diagnostic
and nothing on stdout.

`exchange` is the synchronous round-trip only; for asynchronous, addressable
mailbox delivery see [`baton serve`](#serving-a-mailbox-baton-serve).

## Conversing (`baton converse`)

`baton converse` is the governed two-participant driver: given a seed message it
alternates two participants — each participant's reply becomes the next
participant's request — recording every turn as a `baton.message/v1` envelope,
until the first terminal condition trips. Where `exchange` is one round-trip,
`converse` is a *sustained, bounded* conversation with termination guaranteed.

```
baton converse [--a-system <path>] [--b-system <path>] [--a-model <id>] [--b-model <id>] [--b-mailbox --b-inbox <dir> --b-outbox <dir> [--b-await-ms <n>]] (--seed <text> | --seed-file <path>) [--out <path>]
```

Each side is an in-process participant built from the shared environment
configuration (one credential, one `ANTHROPIC_BASE_URL`), differing only by its
identity and model:

- `--a-system <path>` / `--b-system <path>` — each side's system-prompt file (its
  identity); omitted, a side falls back to `BATON_SYSTEM_PROMPT`.
- `--a-model <id>` / `--b-model <id>` — each side's model, overriding
  `BATON_MODEL` for that side only.
- `--seed <text>` or `--seed-file <path>` (exactly one) — the opening message.
  Participant A sends it to B first.
- `--out <path>` — where the trail is written; stdout when omitted.

The full trail is written as **JSONL**, one `baton.message/v1` envelope per line
in turn order: the seed request first, then each reply. Each reply preserves the
`conversation_id`, links `in_reply_to`, swaps addressing (so a reply's `from`
names its speaker), and wraps the provider call it ran under `exchange` — so per
turn token usage is observable in-band. The terminal reason is printed to stderr.

### Terminal conditions

Whichever trips first ends the run:

- **turn-cap** — `BATON_MAX_TURNS` (default `8`): the hard, always-enforced
  guarantee. Even two participants that would loop forever stop here.
- **token-budget** — `BATON_TOKEN_BUDGET` (optional): ends the run once the
  accumulated reported usage exceeds the budget. When usage is unavailable the
  run still terminates on the turn-cap.
- **unilateral `done`** — a participant emitting a `kind: "done"` reply ends the
  conversation before the caps. (Today's LLM-backed participants emit only
  `response`/`error`; `done` is honored if a participant returns it.)
- **delivered error** — a `kind: "error"` reply is recorded as the terminal turn
  and ends the run.

```bash
baton converse \
  --a-system prompts/interviewer.md \
  --b-system prompts/candidate.md \
  --seed "Introduce yourself in one sentence." \
  --out /tmp/trail.jsonl
```

Because the driver depends only on the participant boundary, the same driver can
be pointed at two independent `baton exchange` **processes** rather than
in-process participants — the vertical proof in `tests/integration_test.rs`
(`converse_drives_two_independent_processes_to_turn_cap`) drives two spawned
children against loopback mock servers, no external network.

### Async: side B over a mailbox (`--b-mailbox`)

The same boundary lets side B be a **live [`baton serve`](#serving-a-mailbox-baton-serve)
daemon** reached over the file-mailbox instead of an in-process participant.
`baton converse` becomes a *governed client* of that service: A is still driven
in-process, but each of B's turns is delivered to the peer's inbox over the
atomic mailbox path and its reply awaited from the outbox.

```bash
# Peer B, a long-lived responder:
baton serve --inbox /tmp/mb --outbox /tmp/ob --poll-ms 20 &

# Local governed driver, with B mailbox-backed:
baton converse \
  --seed "Introduce yourself in one sentence." \
  --b-mailbox --b-inbox /tmp/mb --b-outbox /tmp/ob --b-await-ms 60000
```

- `--b-mailbox` — make side B mailbox-backed. Requires `--b-inbox` and
  `--b-outbox`; mutually exclusive with `--b-system`/`--b-model` (the peer daemon
  configures its own identity and model).
- `--b-inbox <dir>` / `--b-outbox <dir>` — the peer `serve`'s `--inbox` /
  `--outbox`. Each request lands in `<b-inbox>/pending/`; each reply is claimed
  from `<b-outbox>`, keyed by the request id.
- `--b-await-ms <n>` — how long a B turn waits for its reply before giving up
  (positive integer; default `60000`). Generous by default: every B turn is a
  full provider turn run by the peer, so a short deadline would give up mid-answer.

**Topology.** This is a *local governed driver ↔ one remote responder over one
mailbox* — a governed client of a `serve` service, **not** autonomous
peer-daemon↔peer-daemon conversation (there is still a single central driver).
The driver and its governance (turn-cap, token-budget) are unchanged: a
mailbox-backed B is just another participant, so `BATON_MAX_TURNS` /
`BATON_TOKEN_BUDGET` bound the run exactly as in-process.

**Terminal semantics — "peer errored" vs "driver stopped waiting".** A B turn
that times out (or fails to deliver, or gets a mis-correlated reply) is recorded
as a terminal `kind: "error"` turn — but one with **no** nested
`baton.exchange/v1` record and a body naming the await-timeout. A peer-*delivered*
error is also `kind: "error"`, but carries the peer's nested provider-call record.
So the trail distinguishes the two by that record: `error` **with** a nested
record means the peer answered with an error; `error` **without** one means the
driver stopped waiting. This distinction assumes the peer nests a record on every
delivered reply, which holds for a `baton serve` peer (its in-process participant
always does); a future peer that could deliver a recordless error would rely on
the timeout-naming body as the tie-breaker. Mapping the first await-timeout
straight to a terminal is a deliberate v1 simplification — retry/backoff within
an await is a named follow-on.

## Serving a mailbox (`baton serve`)

Where `exchange` is a synchronous round-trip over pipes, `baton serve` gives that
exchange an **asynchronous, addressable** home on disk: a sender drops a
`baton.message/v1` request file into an inbox, and a long-lived `serve` process
picks it up later, answers it through the same participant seam, and writes the
reply to an outbox. Everything is a file — the reach is the filesystem, not a
socket.

```
baton serve --inbox <dir> --outbox <dir> [--poll-ms <n>] [--once]
baton serve --stop --inbox <dir>
```

- `--inbox <dir>` — the mailbox root. `serve` manages `pending/`, `claimed/`, and
  `done/` subdirectories under it.
- `--outbox <dir>` — where response envelopes are written.
- `--poll-ms <n>` — inbox poll interval in milliseconds (default `500`).
- `--once` — drain everything currently pending, then exit (cron-friendly);
  omitted, `serve` polls the inbox until terminated.
- `--stop` — cooperatively stop the `serve` running on `--inbox` (see
  [Shutdown](#shutdown-cooperative-graceful-stop)); takes only `--inbox`.

Each side configures the answering participant exactly as `exchange`/`ask` do
(`BATON_MODEL`, `BATON_SYSTEM_PROMPT`, the credential env, `BATON_EVENT_LOG`), so
a served message runs the identical exchange and records the same trail.

### Delivery: atomic, addressable, crash-safe

A sender delivers by writing a temp file and `rename(2)`-ing it into the inbox,
so `serve` never observes a partial envelope. Each message then moves through one
atomic rename per state: `pending → claimed → done`. A crash mid-answer leaves
the message in `claimed/`; the next start **reclaims** it back to `pending/`, so
no in-flight message is lost. The response is written to
`<outbox>/<request message_id>.json` — keyed by the *request* id (the reply's
`in_reply_to`), so a reprocessed message overwrites its own not-yet-consumed
reply instead of leaving a second file.

### Single instance

`serve` takes an exclusive advisory lock (std `File::try_lock`, stable since Rust
1.89) on the mailbox root at startup; a second `serve` on the same root exits
non-zero rather than running concurrently. This is what makes reclaim safe —
reclaim runs only in the one live instance, so it can never move a `claimed/`
message another daemon is mid-answer on. The lock is advisory and per-host:
reliable on a local filesystem, **not** across NFS/network filesystems (a mailbox
shared between hosts reintroduces the race and is out of scope).

### At-least-once semantics

Processing is **at-least-once**, not exactly-once. An abrupt kill (SIGKILL / OOM
/ power loss) between answering and marking `done` is safe for *delivery* — the
message is reclaimed and reprocessed — but that reprocess is a repeat provider
call and may emit a **second** response envelope. Consumers must therefore
correlate/dedup on `in_reply_to` / `conversation_id`. Keyed outbox writes shrink
the common (unconsumed) case to a single file; they do not make it exactly-once.

### Shutdown (cooperative graceful stop)

A raw `SIGTERM`/`SIGKILL` mid-answer is *safe for delivery* — the message is
reclaimed and reprocessed on the next start — but that reprocess is a repeat
provider call and may emit a second response envelope. To avoid that redundant
reprocess on an *expected* stop (systemd stop, `docker stop`, deploy), stop the
daemon cooperatively instead:

```
baton serve --stop --inbox <dir>
```

`--stop` drops a stop sentinel at the mailbox root; the running daemon consumes
it **between messages** and exits `0`, so an in-flight `respond()` is never
interrupted mid-call. It detects a live daemon by probing the single-instance
lock: if no daemon holds the lock it writes nothing (so a stale sentinel can
never kill a later fresh `serve`) and reports that nothing is running — still
exiting `0`, since a cooperative stop is idempotent. Wire it as systemd
`ExecStop=baton serve --stop --inbox <dir>`.

`--stop` is the **only** graceful path: `serve` installs no signal handler and
does not react to a raw `SIGTERM` (Option A semantics — the crash-safe FSM,
without signal reaction, is the shipped default). Graceful completion is bounded
by the supervisor's stop timeout (systemd `TimeoutStopSec`): if the in-flight
message does not finish before it expires, the supervisor signals the daemon
anyway and delivery falls back to the reclaim-and-reprocess path above.

There is also no zero-downtime handover: a restart has a brief window where the
second `serve` is refused until the first exits.

The client side of this mailbox — posting a request and reading the correlated
reply — is [`baton send`](#posting-to-a-mailbox-baton-send).

## Posting to a mailbox (`baton send`)

`baton send` is the producer for a mailbox: it drops a `baton.message/v1` request
into `<inbox>/pending/` over the same atomic temp-file + `rename(2)` path `serve`
consumes, and with `--await` reads back the correlated reply. It is the reference
client for `serve`'s at-least-once contract. Unlike `serve` it takes **no**
single-instance lock, so it posts to an inbox a live `serve` already owns; and it
runs no provider call, so it needs no credential.

```
baton send --inbox <dir> (--body <text> | --in <path>) [--to <id>] [--from <id>] [--conversation <id>] [--await --outbox <dir> [--timeout-ms <n>]]
```

- `--inbox <dir>` — the mailbox root; the request is written to its `pending/`.
- `--body <text>` — build a request envelope around this body. `--to`/`--from`/
  `--conversation` override its addressing (defaults `agent-b`/`agent-a` and a
  time-derived conversation id); the `message_id` is derived so no external id
  source is needed.
- `--in <path>` — read a complete envelope from a file instead (mutually
  exclusive with `--body`; the addressing flags do not apply — the envelope
  carries its own).
- `--await` — after delivering, wait for the reply and print it to stdout.
  Requires `--outbox <dir>`.
- `--outbox <dir>` — where `serve` writes replies (`<outbox>/<message_id>.json`).
- `--timeout-ms <n>` — how long `--await` waits before giving up (default
  `30000`).

Without `--await`, `send` prints the sent `message_id` to stdout and exits. With
`--await`, the `message_id` confirmation goes to stderr and **stdout carries only
the reply envelope** (one JSON line), so a consumer can pipe it straight into a
parser.

### Await: claim, correlate, and the at-least-once caveat

`--await` polls `<outbox>/<message_id>.json`; on appearance it **atomically
renames the reply out of the outbox to claim ownership**, then reads it. The
rename is the claim: it prevents a concurrent `--await` — or a reappearing
reclaim-driven second response — from double-consuming the same file. (It is not
about partial reads; the atomic write already rules those out.) The consumed
reply's `in_reply_to` must equal the sent `message_id`, or `send` errors rather
than accept an uncorrelated reply.

The await is bounded to the single invocation: on timeout `send` exits non-zero,
the request is left in the mailbox, and it does **not** re-await across runs.
Because a claimed reply is renamed away, a later reclaim-driven **second**
response (the at-least-once tail described under `serve`) reappears as a fresh
outbox file and would be handed to a *subsequent* `--await` — so consumers dedup
on `in_reply_to` / `conversation_id`, exactly as they must for `serve`.

Both the send and the consumed reply are recorded to `BATON_EVENT_LOG` (as
`message_sent` / `reply_consumed` lines on the same trail), when it is set.

## Development

CI runs the following gates; run them locally before opening a PR:

- `cargo fmt --all -- --check` — formatting must be clean.
- `cargo clippy --all-targets -- -D warnings` — no lints allowed.
- `cargo build --verbose` — workspace must build.
- `cargo test --verbose` — runs unit and integration tests; the integration
  tests in `tests/integration_test.rs` spin up an in-process mock HTTP server
  on `127.0.0.1` and need no network access or API credentials.
