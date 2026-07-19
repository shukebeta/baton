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
condition, `baton converse-ring` for the N-party round-robin generalisation over a
static routing registry, `baton serve` for answering `baton.message/v1` requests
from a file mailbox, `baton send` for posting a request into a mailbox (by path or
by **role name** via the registry) and consuming the correlated reply, `baton
status` for reporting a mailbox's liveness (`idle-done` / `busy` / `crashed-stale`
plus queue depth), and `baton log` for inspecting and replaying the recorded
exchange trail.

## Install

Install the `baton` binary from a pinned git tag with a Rust toolchain (≥ 1.89):

```bash
cargo install --git https://github.com/shukebeta/baton --tag v0.1.0 --locked
```

This puts `baton` on your PATH — the form a consumer that invokes baton as a
binary uses. The `--locked` flag is **required**: without it `cargo install
--git` ignores the tracked `Cargo.lock` and resolves fresh dependency versions,
losing the reproducibility the lockfile exists to guarantee. `--tag <tag>` pins
the build to a blessed commit; `cargo install --git … --rev <sha> --locked` pins
just as immutably if you prefer a raw SHA — the tag is the human-memorable name
and GitHub releases anchor over it.

Consumers stay frozen by pinning a tag, and upgrade by re-pinning a newer tag
deliberately. Pinning is the churn-control mechanism.

**Stability is an explicit non-goal at 0.1.0.** Neither the Rust library API nor
the CLI flag surface is promised stable; the CLI is only the *intended*
integration surface, and pinning a tag is how a consumer insulates itself from
change. This release ships no crates.io publish, no prebuilt or cross-platform
binaries (no homebrew / apt), and no supported library-dependency recipe — baton
compiles as lib+bin, but crate consumption is unsupported at 0.1.0 because the
module layout is intentionally thin and will be reworked.

## Quickstart

To see the whole A2A loop end-to-end — reproducibly, with no API key and no
external network — run:

```bash
./scripts/quickstart.sh
```

It launches a loopback mock provider (`examples/mock_provider.rs`), points baton
at it via `ANTHROPIC_BASE_URL`, and drives both A2A surfaces:

1. **`baton converse`** — a governed two-agent conversation between the example
   identities in `prompts/interviewer.md` and `prompts/candidate.md`, driven to
   the turn-cap.
2. **`baton serve` + `baton send --await`** — an asynchronous mailbox
   round-trip: `serve` answers a request dropped into an inbox, and `send`
   consumes the correlated reply.

The resulting JSONL trails are written under `target/quickstart/`
(`converse-trail.jsonl` and `serve-send-reply.jsonl`); the script prints each
path and exits 0. It needs only a Rust toolchain — the mock stands in for the
provider, so no credential is read and nothing leaves `127.0.0.1`.

### Mock vs. a real provider

The mock run proves **plumbing and reproducibility**: that the commands wire
together and terminate deterministically. It is *not* a demonstration — every
reply is the same canned line, so a mock-vs-mock exchange is no substitute for
the real artifact.

To **demonstrate** baton to a human, run the same two commands against a real
provider: set a real credential (`ANTHROPIC_API_KEY`), leave `ANTHROPIC_BASE_URL`
at its default (or point it at your gateway), and keep the two distinct system
prompts so the agents hold a genuine conversation with real replies:

```bash
export ANTHROPIC_API_KEY=sk-...          # a real credential
unset ANTHROPIC_BASE_URL                  # use the real Messages API

baton converse \
  --a-system prompts/interviewer.md \
  --b-system prompts/candidate.md \
  --seed "Introduce yourself in one sentence." \
  --out /tmp/trail.jsonl

# In one shell: a long-lived responder.
baton serve --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox
# In another: post a request and read the correlated reply.
baton send --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox \
  --await --body "Ping over the mailbox."
```

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
| `BATON_EVENT_LOG`            | no       | — (disabled)                | File path for the JSONL exchange-event trail, opened in append mode. Also carries the `baton session` [session trail](#session-trail) (session start/end markers + per-turn `session_id` / `turn_index`). See below. |
| `BATON_HOME`                 | no       | `$HOME/.baton`              | Root of the [role homes](#role-homes-roles-name) (`roles/<name>/`, `defaults.json`). Not required to exist; created lazily. |

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

### Role homes (`roles/<name>/`)

A multi-party conversation's parties have **distinct identities** — each needs
its own system prompt, model, credential, working directory, and MCP config.
Rather than hand-assemble those env vars per process *and* a routing entry, Baton
makes a role's identity a **per-role home directory** under the baton home root
(`BATON_HOME`, else `$HOME/.baton`), analogous to `~/.claude` with one
subdirectory per role:

```text
$BATON_HOME/                    # BATON_HOME, else $HOME/.baton
  defaults.json                 # base config inherited by every role
  roles/
    alice/
      config.json               # alice's identity overrides
      system.md                 # optional; the default system prompt
      sessions/                 # recorded sessions alice took part in (#82)
        <session_id>.jsonl      # one file per session, both sides' turns
```

> **Behaviour change.** This is a deliberate departure from Baton's prior
> no-hidden-state, env-only stance: a role home is state on disk. It is opt-in —
> nothing reads the home until you pass `--role`, and every existing env-only
> invocation is unchanged.

Adding a role is creating a `roles/<name>/` directory; removing it is deleting
the directory. A broken `config.json` breaks only that role, not the roster.

**`config.json`** — every field optional; an absent field inherits `defaults.json`,
then the built-in default:

```json
{
  "model": "claude-opus-4-8",
  "base_url": "https://api.anthropic.com",
  "system_prompt": "system.md",
  "credential": { "kind": "oauth", "env": "ALICE_TOKEN" },
  "cwd": "/work/alice",
  "mcp_config": "mcp.json",
  "timeout_secs": 60,
  "max_tokens": 1024
}
```

| Field           | Maps to                     | Notes                                                                 |
| --------------- | --------------------------- | -------------------------------------------------------------------- |
| `model`         | `BATON_MODEL`               |                                                                      |
| `base_url`      | `ANTHROPIC_BASE_URL`        |                                                                      |
| `system_prompt` | `BATON_SYSTEM_PROMPT`       | File path; relative resolves against the role dir. Defaults to `system.md` in the role dir when present (the "inline" ergonomics). |
| `credential`    | credential env var          | A **reference**, never the secret: `{ "kind": "api_key"\|"oauth", "env": "<VAR>" }` names the env var holding the secret. |
| `cwd`           | `serve --agent-cwd`         | External-agent working directory; relative resolves against the role dir. |
| `mcp_config`    | `serve --agent-mcp-config`  | MCP config path; relative resolves against the role dir.              |
| `timeout_secs`  | `BATON_TIMEOUT_SECS`        |                                                                      |
| `max_tokens`    | `BATON_MAX_TOKENS`          |                                                                      |

**`defaults.json`** uses the same schema; its relative paths resolve against the
home root. Every role inherits it, so common settings are written once.

**Resolution order** is `flag > env > role config > defaults > built-in default`
— standard aws/docker precedence, where **env overrides the config file**. The
command-line env override (`BATON_MODEL=… baton …`) is the escape hatch for when
editing config is inconvenient; config-over-env would weld it shut. A credential
is a special case: any directly-set credential env var wins wholesale over a
role's `credential` reference.

**Roster commands** give the single-glance overview centralization would
otherwise provide:

```bash
baton roles                 # list the role names under roles/
baton role show alice       # print alice's effective identity + each value's source
```

`baton role show` prints, per field, the resolved value and the layer it came
from (`env` / `role` / `defaults` / `default`). The credential line shows only
the reference (`oauth (env ALICE_TOKEN)` or `env ANTHROPIC_API_KEY`), never the
secret.

A role's home is consumed by [`baton serve --role <name>`](#serving-a-mailbox-baton-serve):
each party in an N-party ring is its own `serve --role` daemon, so identity lands
there while the [routing registry](#routing-registry-name--mailbox) stays pure
routing and references roles by name only.

### Provider configuration — recorded decision

This records *why* provider access is configured the way it is above, and the
explicit conditions under which that changes. It is a decision record, not a
new mechanism — nothing here adds a config file type, field, or precedence tier.

**1. Identity is inlined by reference.** A role reaches a provider through a
`base_url` plus a **credential reference** — the name of the env var holding the
secret, never the secret itself (`{ "kind": "api_key"|"oauth", "env": "<VAR>" }`).
These resolve through the layered chain `env > role config.json > defaults.json >
built-in`. `defaults.json` is the single shared bucket every role inherits, so one
shared account is written there once and referenced by every role that uses it.

**2. No backend entity now.** Baton does not add a named `backends/<name>.json`
record, a role `backend:` reference, or the resulting per-field 6-tier precedence
chain. The only thing that would drive such an entity is **≥2 distinct** shared
provider groups — which the single `defaults.json` bucket cannot express — and
that need is not present in Baton's own `roles/` usage. Deferring costs nothing
compounding: adding `backend: Option<String>` to `RoleConfig` later is
non-breaking (config fields are `#[serde(default)] Option<_>`, and
`deny_unknown_fields` rejects only *unknown* keys, not newly-added ones), and
retrofitting existing roles onto a shared record is linear whenever it is done.
The full shape to build when that day comes is preserved in issue #84.

**3. No dialect-dispatch seam now.** There is one `Transport` implementation
(`ClaudeClient`) and one wire dialect. A protocol-keyed dispatch with a single
registered arm is dead scaffolding, so no `protocol`/`kind`/`dialect` field or
dispatch registry is added until a second dialect is real work. When it is, a
plain field named `protocol` reads distinctly from the harness-level `kind` and
from `CredentialRef.kind`.

**4. Per-worker tuning stays on the worker.** A role carries not only *which
account it reaches* but *how it runs against it* — today `model`, and plausibly a
future `effort`/reasoning-level knob. (On this transport path `effort` would map
to the Messages API `thinking: { budget_tokens }` param, which the transport does
not yet send — so it is a transport feature, not a free field, and is likewise
not built now.) Two roles on the *same* provider and token legitimately differ in
model and effort. This tuning is a separate axis from the shared
`base_url`+`credential` a backend entity would hold: when that entity lands,
tuning stays on the worker, never on the backend.

**When to revisit.** Reopen this decision when an operator configures **a second
distinct non-default provider group** — a shared `base_url`/`credential.env` pair
across ≥2 roles that the single `defaults.json` bucket cannot hold — **or** when a
second wire dialect is needed. A single shared non-default pair is *not* the
trigger: it hoists into `defaults.json`. The near-term archetype is roles split
across **Anthropic + z.ai (GLM) + MiniMax** — all the same `claude` wire dialect
(so not a dialect trigger), but each a distinct `base_url` + `credential.env`.
`defaults.json` can hold one as the default; the second and third distinct groups
are exactly what one bucket cannot express, and are the trigger.

**Designated first step when the trigger fires (recorded, not built).** The first
thing to build then is a cross-role **coupling view** extending `baton roles`
(today a name-only lister): resolve every role's effective identity and report,
grouped by shared value, the roles sharing the same non-default `base_url` and/or
`credential.env` — credentials shown in reference form only (`kind (env NAME)`),
never a resolved secret — distinguishing `defaults.json`-inherited pairs (already
one-edit atomic) from per-role inliners. It gives operators an auditable
pre-migration checklist and sizes the eventual backend entity. Its value is empty
until the trigger fires, so its shape is recorded here only so nothing is
re-derived.

### Per-role session recording

A role's home also holds its **history**. The unit is the *session* — the whole
back-and-forth the role took part in, both sides' turns, not the role's own
utterances in isolation. Each session the role participates in is one file:

```text
roles/<name>/sessions/<session_id>.jsonl
```

Written by two paths, both reusing the flush-per-line JSONL writer (a killed
process leaves a valid partial session — the same torn-tail tolerance
`baton log` already has):

- **`baton session --role <name>`** (human↔agent) speaks as the role's identity
  and records the #76-shaped trail — every user *and* assistant turn — under the
  role's home. The `<session_id>` is the minted `sess-…` id. (`--role` cannot be
  combined with `--resume`, which already fixes its own trail file.)
- **`baton serve --role <name>`** (A2A) records each answered exchange as one
  **seat turn** — the request it received *and* the reply it sent — keyed on the
  message's `conversation_id`, so turns of one conversation land in one file.

**Schema** — baton's own (`baton.exchange/v1`, extending the #76 session events;
*not* Claude Code's format). One JSON object per line:

- `session_start` — opens the file, carrying `session_id`, the recording `role`,
  and its effective `identity` (each config value + the layer it came from, for
  reproducibility; the credential is the reference form, never the secret).
- Per turn, a `request` line (the received/sent prompt; on a seat turn it also
  carries `from` / `to` / `conversation_id` / `message_id` / `in_reply_to`, and
  its `session_id` equals `conversation_id`) followed by a `response_ok` /
  `response_error` outcome line — the two together are **both sides** of the turn.
- `session_end` — closes a cleanly-exited `baton session` (a long-lived `serve`
  daemon does not emit one; the reader tolerates its absence).

Read one back with `baton log show --file roles/<name>/sessions/<id>.jsonl`.

**N-party role views are seat-scoped.** A single `serve` sees only its own
request/reply pairs, so for the common 2-party shapes (human↔agent, agent↔agent)
the seat view *is* the complete session; in an N-party ring each role's file is
its own seat's view. The full-ring transcript is the conversation driver's
`--out` trail, or assemble it across trails with
[`baton log merge`](#cross-trail-merge-baton-log-merge).

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
- History lives only in memory: it is not persisted across process restarts.
  Setting `BATON_EVENT_LOG` records a real-time, self-delimiting JSONL **session
  trail** — a `session_id`, per-turn `turn_index`, and session start/end markers —
  that a single file (or a shared append log) is unambiguously partitionable back
  into whole sessions (see [Session trail](#session-trail)).
- `baton session --role <name>` speaks as that role's identity and records the
  same trail under the role's home
  ([per-role session recording](#per-role-session-recording)), stamping the
  role's effective identity on the opening marker, instead of `BATON_EVENT_LOG`.

### Resuming a session

`baton session --resume <file>` rehydrates a prior session from its trail: it
reads the session-scoped JSONL, replays the recorded turns in `turn_index` order
into a fresh conversation, and enters the REPL with that history preloaded — so
the first new request already carries every prior turn.

```bash
# continue where a previous `baton session` left off
cargo run -- session --resume ./session.jsonl
```

- New turns continue the **same** session: they append to `<file>` with the
  original `session_id` and a `turn_index` continuing monotonically from the last
  recorded turn, so the resumed run extends one coherent session rather than
  forking a new one. (New turns are written to the resume file itself, not to
  `BATON_EVENT_LOG`.)
- Only completed turns (a recorded assistant reply) are replayed into the
  history; a turn that errored or was cut off by an unclean shutdown contributes
  no reply and is skipped, so the resumed history never holds a dangling user
  turn.
- **Selection.** When `<file>` is a shared append log holding several sessions,
  pass `--session <id>` to choose one. With a single-session file the selector is
  optional; a missing `--session` against a multi-session file names the
  available ids and exits with a usage error. Selecting a non-existent
  `session_id`, or an empty / malformed trail, is a usage error that exits
  non-zero having written nothing.
- A trail whose final line is torn (an unclean prior shutdown) resumes from the
  last complete turn — the incomplete trailing record is dropped with a warning,
  matching the trail's [torn-tail handling](#session-trail).

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

## N-party ring (`baton converse-ring`)

`baton converse` drives two participants; `baton converse-ring` generalises that
to an **N-party (N ≥ 2) round-robin ring** whose members are all live
mailbox-backed peers. The driver takes turns around a fixed ring — `roster[1]`
answers the seed, then `roster[2]`, … wrapping past `roster[0]` — recording every
turn as a `baton.message/v1` envelope, bounded by the same governance
(`BATON_MAX_TURNS` / `BATON_TOKEN_BUDGET`) as `converse`. The recipient of each
turn is chosen purely by **ring position**, never by a reply's `to`; the registry
below only resolves a name to a mailbox, it does not route.

```
baton converse-ring --registry <path> --roster <a,b,c> (--seed <text> | --seed-file <path>) [--await-ms <n>] [--out <path>]
```

- `--registry <path>` — the routing registry (JSON, format below), loaded once at
  startup.
- `--roster <a,b,c>` — the ring order, a comma-separated list of participant names
  (≥ 2, no blanks, no duplicates). Every name must exist in the registry; an
  unknown name is a **startup error** before any turn runs.
- `--seed <text>` / `--seed-file <path>` (exactly one) — the opening message,
  addressed from `roster[0]` to `roster[1]`.
- `--await-ms <n>` — how long each turn waits for its peer's reply (positive
  integer; default `60000`), as for `converse --b-await-ms`.
- `--out <path>` — where the JSONL trail is written; stdout when omitted.

### Routing registry (name → mailbox)

The registry is a **static** JSON file mapping each participant name to its
`{inbox, outbox}` mailbox **pair** — each peer is its own [`baton serve`](#serving-a-mailbox-baton-serve)
daemon with its own inbox and outbox. It is pure lookup: it holds **no**
governance (the driver remains the sole governance authority) and performs no
routing beyond name resolution. Names are validated as safe mailbox keys, so a
name cannot escape the mailbox root via path components.

```json
{
  "participants": {
    "alice": { "inbox": "/tmp/alice/inbox", "outbox": "/tmp/alice/outbox" },
    "bob":   { "inbox": "/tmp/bob/inbox",   "outbox": "/tmp/bob/outbox" },
    "carol": { "inbox": "/tmp/carol/inbox", "outbox": "/tmp/carol/outbox" }
  }
}
```

| Field                    | Meaning                                                                 |
|--------------------------|------------------------------------------------------------------------|
| `participants`           | Object mapping each participant name to its mailbox pair.               |
| `participants.<name>.inbox`  | The peer `serve`'s `--inbox`; requests land in `<inbox>/pending/`.  |
| `participants.<name>.outbox` | The peer `serve`'s `--outbox`; replies are claimed keyed by request id. |

The registry answers only *where* a name's messages go, never *who* that name
is. A party's **identity** — system prompt, model, credential, cwd, MCP config —
lives in its [role home](#role-homes-roles-name) (`roles/<name>/`), and each ring
member is its own `baton serve --role <name>` daemon that loads it. So the two
surfaces stay cleanly split: the registry is pure routing, the role home is pure
identity, and standing up a party is "add a `roles/<name>/` directory + a
registry entry", not hand-assembling per-process env.

### Worked example — three peers

```bash
# Three long-lived responders, one per ring member — each loads its own
# identity from $BATON_HOME/roles/<name>/ via --role:
baton serve --inbox /tmp/alice/inbox --outbox /tmp/alice/outbox --role alice --poll-ms 20 &
baton serve --inbox /tmp/bob/inbox   --outbox /tmp/bob/outbox   --role bob   --poll-ms 20 &
baton serve --inbox /tmp/carol/inbox --outbox /tmp/carol/outbox --role carol --poll-ms 20 &

# Drive the ring from the registry above (saved as /tmp/roster.json):
baton converse-ring \
  --registry /tmp/roster.json \
  --roster alice,bob,carol \
  --seed "Introduce yourself in one sentence." \
  --await-ms 10000 \
  --out /tmp/ring-trail.jsonl
```

The trail's replies advance by ring position — `bob`, `carol`, then `alice` on
the wrap — each carrying its peer's nested `baton.exchange/v1` provider call. The
end-to-end proof is `converse_ring_drives_three_live_serve_peers` in
`tests/integration_test.rs`, which drives three independent daemons against
loopback mock servers with no external network.

**Non-goals (v1).** The registry is deliberately minimal:

- **No convention-derived paths** (`<root>/<name>/…`) — a possible later
  zero-config layer over the explicit registry.
- **No dynamic discovery** (register / heartbeat / liveness / join-leave) — the
  roster is fixed for the run.
- **No `to`-based routing** — the driver picks the next recipient by ring order;
  the registry only resolves names to mailboxes.

## Serving a mailbox (`baton serve`)

Where `exchange` is a synchronous round-trip over pipes, `baton serve` gives that
exchange an **asynchronous, addressable** home on disk: a sender drops a
`baton.message/v1` request file into an inbox, and a long-lived `serve` process
picks it up later, answers it through the same participant seam, and writes the
reply to an outbox. Everything is a file — the reach is the filesystem, not a
socket.

```
baton serve --inbox <dir> --outbox <dir> [--poll-ms <n>] [--once]
            [--agent-cmd <program> [--agent-arg <arg>]... [--agent-cwd <dir>] [--agent-timeout-ms <n>]
             [--agent-output raw|json [--agent-result-key <key>]] [--agent-system <path>] [--agent-mcp-config <path>]]
            [--role <name>]
baton serve --stop --inbox <dir>
```

- `--inbox <dir>` — the mailbox root. `serve` manages `pending/`, `claimed/`, and
  `done/` subdirectories under it.
- `--outbox <dir>` — where response envelopes are written.
- `--poll-ms <n>` — inbox poll interval in milliseconds (default `500`).
- `--once` — drain everything currently pending, then exit (cron-friendly);
  omitted, `serve` polls the inbox until terminated.
- `--agent-cmd <program>` — host the role with an **external agent** instead of an
  in-process provider call (see [External-agent role](#external-agent-role---agent-cmd)).
- `--role <name>` — resolve the answering identity from the role's
  [home directory](#role-homes-roles-name) (`roles/<name>/`), so a party is stood
  up by name instead of hand-assembled env vars. In-process mode feeds the role's
  layered config (model, base URL, credential, system prompt, timeouts) to the
  provider call; agent mode fills `--agent-cwd` / `--agent-system` /
  `--agent-mcp-config` from the role's `cwd` / `system_prompt` / `mcp_config` when
  the flag is not passed. **Explicit flags and env always override the role**
  (`flag > env > role config > defaults > default`). With `--role`, each answered
  exchange is also recorded as a per-role
  [session](#per-role-session-recording) under
  `roles/<name>/sessions/<conversation_id>.jsonl`.
- `--stop` — cooperatively stop the `serve` running on `--inbox` (see
  [Shutdown](#shutdown-cooperative-graceful-stop)); takes only `--inbox`.

Without `--agent-cmd`, each side configures the answering participant exactly as
`exchange`/`ask` do (`BATON_MODEL`, `BATON_SYSTEM_PROMPT`, the credential env,
`BATON_EVENT_LOG`), so a served message runs the identical exchange and records
the same trail. A `--role` supplies these same values from the role's home when
the env leaves them unset.

### External-agent role (`--agent-cmd`)

By default a served reply is a single Messages-API call. `--agent-cmd` instead
backs the role with a **full-tooled native agent CLI run headless** — one that
edits files and runs git/bash/MCP — driven entirely through the mailbox, with
**no tmux and no live TUI**. This is the tmux-free launch leaf for a non-tmux
team role: `baton serve --agent-cmd …` has no `TMAT_PANE` / `tmux` / pane-title
dependency anywhere.

```
baton serve --inbox <dir> --outbox <dir> \
  --agent-cmd claude --agent-cwd /path/to/worktree \
  --agent-system /path/to/role-identity.txt \
  --agent-arg -p --agent-arg --dangerously-skip-permissions
```

- `--agent-cmd <program>` — the agent CLI to run once per message.
- `--agent-arg <arg>` — a fixed argument passed on every run (repeatable), e.g.
  headless/role flags. The request body is delivered on the agent's **stdin**;
  the agent's final **stdout** becomes the reply body (see the output adapter
  below).
- `--agent-cwd <dir>` — the working directory (a git worktree) for every run;
  defaults to the `serve` process's own cwd.
- `--agent-timeout-ms <n>` — read timeout for one agent run (default `600000`).
  Generous by design: an agent run is many tool calls, not one provider turn.

#### Reply shape: the output adapter

By default the **whole** stdout is the reply body — correct for a backend that
prints only its final answer (e.g. `claude -p`). A *streaming* backend
(codex/copilot) interleaves tool/step chatter into stdout, which would leak into
the reply. `--agent-output` isolates the final result:

- `--agent-output raw` (default) — the whole stdout is the reply body.
- `--agent-output json` — the reply body is the string value at a result field
  in the agent's **final non-empty stdout line, parsed as a JSON object** — the
  `--output-format json`/`stream-json` convention. Chatter lines above that final
  line are dropped. Pair it with the backend's own structured-output flag via
  `--agent-arg` (e.g. `--agent-arg --output-format --agent-arg json`).
  - `--agent-result-key <key>` — the field to read (default `result`, matching
    `claude -p --output-format json`; set it to your backend's field, e.g.
    `message`). Valid only with `--agent-output json`.
  - If the final line is absent, is not a JSON object, lacks the key, or the
    key's value is not a string, the run becomes a synthesized delivered
    `kind: "error"` (never a stringified-JSON body).

#### Per-role identity and MCP config

Role identity and MCP configuration are first-class, so a served role is
configured *by role* rather than by a hand-assembled `--agent-arg` list:

- `--agent-system <path>` — a role system-prompt/identity **file**, injected as
  the agent's `--append-system-prompt <contents>`.
- `--agent-mcp-config <path>` — an MCP config file, injected as the agent's
  `--mcp-config <path>`.

Both are **mapped to Claude Code's flag spelling** (baton's reference backend)
and prepended to your `--agent-arg` values, so a raw override still composes. For
a non-claude backend whose flags differ, pass identity/MCP through raw
`--agent-arg` instead.

In this mode `serve` loads **no `BatonConfig` and needs no API key** — the agent
carries its own credentials and MCP config (layer them through the inherited
environment, or via `--agent-mcp-config`). Cross-message state is the agent's own
job: it reconstructs context across rounds from **durable artifacts** (the git
branch/worktree it shares run-to-run, the issue thread, prior mailbox history),
not an in-memory session — headless-per-message is the model. An agent run that
exits 0 with a non-empty extracted result is wrapped into a `kind: "response"`
(it nests no `baton.exchange/v1` record, since a multi-tool run is not one
provider call baton can vouch for); a spawn failure, non-zero exit, empty output,
an unextractable JSON result, or a timeout becomes a synthesized delivered
`kind: "error"`.

`scripts/external-agent-proof.sh` is the runnable end-to-end proof (real agent +
real credentials, so **not** part of baton's no-API-key CI): it drives two
addressed rounds against a throwaway git worktree and asserts an observable side
effect (a commit) plus round-2 continuity on a durable artifact (a further
commit extending round 1's file). The hermetic machinery is covered by the
`ExternalAgentParticipant` unit tests.

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
baton send (--inbox <dir> | --registry <path>) (--body <text> [--to <role>] | --in <path>) [--from <id>] [--conversation <id>] [--await [--outbox <dir>] [--timeout-ms <n>]]
```

- `--inbox <dir>` — the mailbox root; the request is written to its `pending/`.
  Mutually exclusive with `--registry`.
- `--registry <path>` — resolve the destination by **role name** instead of a
  path (same registry format as `converse-ring`). The addressee role — the
  `--body` `--to <role>`, or the `--in` envelope's own `to` — is looked up to its
  `{inbox, outbox}` pair; an unknown role fails fast. The registry supplies the
  `--await` outbox, so `--outbox` is not passed with `--registry`.
- `--body <text>` — build a request envelope around this body. `--to`/`--from`/
  `--conversation` override its addressing (defaults `agent-b`/`agent-a` and a
  time-derived conversation id); the `message_id` is derived so no external id
  source is needed. With `--registry`, `--to <role>` is required (it is both the
  routing key and the envelope `to`).
- `--in <path>` — read a complete envelope from a file instead (mutually
  exclusive with `--body`; the addressing flags do not apply — the envelope
  carries its own; with `--registry` its `to` is the routing role).
- `--await` — after delivering, wait for the reply and print it to stdout. Needs
  `--outbox <dir>` unless `--registry` resolves it.
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

## Mailbox liveness (`baton status`)

`baton status` reports whether a mailbox's worker is idle, actively running, or
crashed — the signal a team's gate-check reads before starting a cycle. The naive
test `idle = pending empty AND claimed empty` cannot tell a legitimately long run
from a crash: both leave a `claimed/` entry (this is why
[reclaim](#at-least-once-semantics) exists). `status` splits that ambiguity by
**claim age** against a max-runtime threshold — there is **no heartbeat protocol**.

```
baton status (--mailbox <root> | --registry <path> --role <role>) [--max-runtime-ms <n>]
```

- `--mailbox <root>` — probe this mailbox root directly.
- `--registry <path> --role <role>` — resolve the mailbox by role name (same
  registry as `send`/`converse-ring`); an unknown role fails fast.
- `--max-runtime-ms <n>` — the crashed-stale threshold, in milliseconds. Precedence:
  this flag > the role's `max_runtime_ms` in the registry (see below) > a built-in
  default. It **must sit above the worst-case legitimate agent run**, or a
  slow-but-alive worker is misread as crashed.

It prints one JSON line and exits 0:

```json
{"state":"busy","queue_depth":2,"claim_age_ms":4200,"max_runtime_ms":900000}
```

- `state` — `idle-done` (no claim), `busy` (a claim younger than the threshold),
  or `crashed-stale` (a claim older than the threshold).
- `queue_depth` — the number of requests waiting in `pending/`.
- `claim_age_ms` — the oldest claim's age in milliseconds, or `null` when idle.

The probe is **lock-free**: it reads `pending/` and `claimed/` without taking the
single-instance lock, so it safely inspects a mailbox a live `serve` owns. A
claim's age is measured from **when it was claimed** — `claim_next` stamps the
claim time onto the file — so a request that waited in `pending/` is not misread as
instantly stale.

**Reclaim hazard (documented boundary).** A `crashed-stale` claim is recovered by
`serve`'s [at-least-once reclaim](#at-least-once-semantics) on the next start,
which re-runs the abandoned message — possibly re-running a side-effecting agent (a
double commit / PR). Two mitigations are required: (a) the threshold above sits
above the worst-case legitimate run, so a live worker is never falsely reclaimed;
and (b) correctness on re-run relies on the agent's **idempotency via durable
artifacts** — on re-run it observes its own prior branch/commit and adapts.

### Per-role threshold in the registry

A registry entry may carry an optional `max_runtime_ms`, used by `status
--registry --role` when no `--max-runtime-ms` override is given:

```json
{
  "participants": {
    "reviewer": { "inbox": "/tmp/reviewer/inbox", "outbox": "/tmp/reviewer/outbox", "max_runtime_ms": 1200000 }
  }
}
```

The field is optional and back-compatible: existing registries without it parse
unchanged and fall back to the `status` default.

## Development

CI runs the following gates; run them locally before opening a PR:

- `cargo fmt --all -- --check` — formatting must be clean.
- `cargo clippy --all-targets -- -D warnings` — no lints allowed.
- `cargo build --verbose` — workspace must build.
- `cargo test --verbose` — runs unit and integration tests; the integration
  tests in `tests/integration_test.rs` spin up an in-process mock HTTP server
  on `127.0.0.1` and need no network access or API credentials.
