# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

Early scaffolding. The crate establishes the module layout and typed runtime
shape for a single-turn first-prompt / first-reply path, plus a non-streaming
Claude-compatible Messages client (`transport::claude::ClaudeClient`) that can
send one prompt and decode one reply. Wiring that client into a user-facing
`ask` command lands in a later ticket; for now it is a library surface.

## Configuration

Baton reads its runtime configuration from environment variables:

| Variable               | Required | Default                     | Purpose                                              |
| ---------------------- | -------- | --------------------------- | ---------------------------------------------------- |
| `ANTHROPIC_API_KEY`    | yes      | —                           | Provider API key. Must be set and non-empty.         |
| `ANTHROPIC_BASE_URL`   | no       | `https://api.anthropic.com` | Base URL for the Claude-compatible Messages API.     |
| `BATON_MODEL`          | no       | `claude-sonnet-4-6`         | Model id to request.                                 |
| `BATON_TIMEOUT_SECS`   | no       | `60`                        | Per-request timeout in seconds (non-negative integer). |

Missing or invalid values are surfaced as explicit configuration errors at
startup rather than failing later.

## Bootstrap

```bash
export ANTHROPIC_API_KEY=sk-...
cargo run
```

The bare invocation loads configuration and reports that the runtime is ready.
This is a placeholder for the `ask` command added in a later ticket; it exists
so configuration errors surface today.

## Provider transport

`transport::claude::ClaudeClient` implements the `Transport` trait against a
Claude-compatible non-streaming `POST /v1/messages` endpoint:

- Authenticates with the `x-api-key` header from `ANTHROPIC_API_KEY` and pins
  the `anthropic-version: 2023-06-01` header.
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
