# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

Early scaffolding. The crate currently establishes the module layout and typed
runtime shape for a single-turn first-prompt / first-reply path. Sending a real
prompt (the Messages transport and the `ask` command) lands in later tickets.

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
