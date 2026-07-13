# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

Early scaffolding. The crate establishes the module layout and typed runtime
shape around a non-streaming Claude-compatible Messages client
(`transport::claude::ClaudeClient`). Three commands wire it to the command line:
`baton ask` for a single-turn first-prompt / first-reply, `baton session` for an
interactive multi-turn REPL that accumulates conversation history across turns,
and `baton log` for inspecting and replaying the recorded exchange trail.

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

## Development

CI runs the following gates; run them locally before opening a PR:

- `cargo fmt --all -- --check` — formatting must be clean.
- `cargo clippy --all-targets -- -D warnings` — no lints allowed.
- `cargo build --verbose` — workspace must build.
- `cargo test --verbose` — runs unit and integration tests; the integration
  tests in `tests/integration_test.rs` spin up an in-process mock HTTP server
  on `127.0.0.1` and need no network access or API credentials.
