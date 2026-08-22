# Configuration and role identity

How a Baton process is configured: the environment variables every verb reads,
the per-role home directories that give a party its own identity, the recorded
decision behind that shape, per-role session recording, and the provider
transport those settings drive.

## Environment variables

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
| `BATON_EVENT_LOG`            | no       | — (disabled)                | File path for the JSONL exchange-event trail, opened in append mode. Also carries the `baton session` [session trail](protocol.md#session-trail) (session start/end markers + per-turn `session_id` / `turn_index`). Schema: [Structured exchange events](protocol.md#structured-exchange-events). |
| `BATON_HOME`                 | no       | `$HOME/.baton`              | Root of the [role homes](#role-homes-rolesname) (`roles/<name>/`, `defaults.json`). Not required to exist; created lazily. |

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

A multi-party conversation's parties have **distinct identities** — each can have
its own system prompt, model, credential, and working directory. External-agent
MCP configuration belongs to the caller's `--agent-cmd` wrapper or
`--agent-arg` passthrough, not the role home.
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
| `timeout_secs`  | `BATON_TIMEOUT_SECS`        |                                                                      |
| `max_tokens`    | `BATON_MAX_TOKENS`          |                                                                      |

`mcp_config` is not a supported `RoleConfig` field after #115. Existing role
configurations that set it must move those agent MCP settings to the caller's
`--agent-cmd` wrapper or `--agent-arg` passthrough; see [System prompt and MCP:
the caller's job](external-agent.md#system-prompt-and-mcp-the-callers-job).

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

A role's home is consumed by [`baton serve --role <name>`](mailbox.md#serving-a-mailbox-baton-serve):
each party in an N-party ring is its own `serve --role` daemon, so identity lands
there while the [routing registry](mailbox.md#routing-registry-name--mailbox) stays pure
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
  Human↔agent session outcomes repeat the request's `session_id` and
  `turn_index` so overlapping sessions in one append log can be matched
  directly. A2A seat outcomes mirrored from nested exchanges omit those fields
  and use the parser's file-order fallback.
- `session_end` — closes a cleanly-exited `baton session` (a long-lived `serve`
  daemon does not emit one; the reader tolerates its absence).

Read one back with `baton log show --file roles/<name>/sessions/<id>.jsonl`.

**N-party role views are seat-scoped.** A single `serve` sees only its own
request/reply pairs, so for the common 2-party shapes (human↔agent, agent↔agent)
the seat view *is* the complete session; in an N-party ring each role's file is
its own seat's view. The full-ring transcript is the conversation driver's
`--out` trail, or assemble it across trails with
[`baton log merge`](protocol.md#cross-trail-merge-baton-log-merge).

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
