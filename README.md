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
| `BATON_TIMEOUT_SECS`         | no       | `60`                        | Per-request timeout in seconds (non-negative integer). |
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
{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"Hi there!"}
```

| `event`          | Fields beyond `schema` / `ts_ms`            | Meaning                                              |
| ---------------- | ------------------------------------------- | ---------------------------------------------------- |
| `request`        | `model`, `base_url`, `prompt`               | Emitted before the call; carries enough to replay it. |
| `response_ok`    | `duration_ms`, `reply`                      | The call succeeded.                                  |
| `response_error` | `duration_ms`, `kind`, `message`            | The call failed; `kind` is the stable machine class. |

`kind` mirrors the `BatonError` variants (`transport`, `auth`, `rate_limited`,
`server`, `api`, `decode`, `io`, `config`, `usage`), so consumers can branch on
the failure class without parsing the human-readable `message`.

**Consumption model.** Read the file line by line; parse each line as a
standalone JSON object (a partial trailing line, if any, can be skipped). The
event trail is auxiliary observability — it is written to the configured file
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
reply or the failure (`kind: message`).

```bash
export BATON_EVENT_LOG=baton-events.jsonl
baton log show
```

```text
#1  2023-11-14T22:13:20Z  claude-sonnet-4-6  (418ms)
    prompt: who won the 1998 world cup?
    reply:  France.
```

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
newer writer), but a line that is not valid JSON is a hard parse error naming
the offending line. Diffing, filtering, and non-JSONL export remain out of
scope.

## Development

CI runs the following gates; run them locally before opening a PR:

- `cargo fmt --all -- --check` — formatting must be clean.
- `cargo clippy --all-targets -- -D warnings` — no lints allowed.
- `cargo build --verbose` — workspace must build.
- `cargo test --verbose` — runs unit and integration tests; the integration
  tests in `tests/integration_test.rs` spin up an in-process mock HTTP server
  on `127.0.0.1` and need no network access or API credentials.
