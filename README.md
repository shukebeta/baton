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

Exactly one credential variable is required. The first one that is set (in
precedence `ANTHROPIC_API_KEY` > `ANTHROPIC_AUTH_TOKEN` > `CLAUDE_CODE_OAUTH_TOKEN`)
wins; the others are ignored. A credential variable that is exported but blank
or whitespace-only is an error, even if a later candidate is valid — exporting
an empty value is almost always a misconfiguration rather than an explicit
"skip me" signal.

Missing or invalid values are surfaced as explicit configuration errors at
startup rather than failing later.

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
- Requests up to 1024 output tokens per reply (fixed for now) and extracts the
  assistant's text from the response's `content` blocks.

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
