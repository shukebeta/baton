# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

Early scaffolding. The crate establishes the module layout and typed runtime
shape for a single-turn first-prompt / first-reply path, plus a non-streaming
Claude-compatible Messages client (`transport::claude::ClaudeClient`) that can
send one prompt and decode one reply. The `baton ask` command wires that client
to the command line for the first-reply bootstrap flow.

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

Streaming, tool calling, and multi-turn conversations are out of scope for this
client.

## Structured exchange events

Baton can record each `ask` exchange as a machine-readable trail so a single
request/response can be programmatically inspected or replayed, and so failures
are captured explicitly rather than lost in human-oriented output.

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
rather than failing the command. Scope is single-turn: there is no session or
multi-turn state in the schema.

## Development

CI runs the following gates; run them locally before opening a PR:

- `cargo fmt --all -- --check` — formatting must be clean.
- `cargo clippy --all-targets -- -D warnings` — no lints allowed.
- `cargo build --verbose` — workspace must build.
- `cargo test --verbose` — runs unit and integration tests; the integration
  tests in `tests/integration_test.rs` spin up an in-process mock HTTP server
  on `127.0.0.1` and need no network access or API credentials.
