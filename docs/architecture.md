# Baton architecture & onboarding

This is the conceptual map the CLI reference pages do not
give: what Baton is, the two ways a participant can answer a message, how the
crate is laid out, and how each CLI verb maps to the agent-to-agent (A2A) model.
It is written to stand on its own — you do not need to have read the source to
follow it.

## What Baton is

Baton is an **A2A-first harness**: its subject is one agent sending another a
structured message and getting a correlated, recorded reply — not a human
chatting with a model. The chat-style single-turn verbs (`ask`, `session`)
exist, but they are the shallow end; the centre of gravity is the
`baton.message/v1` envelope (see [protocol.md §A2A message
envelope](protocol.md#a2a-message-envelope-batonmessagev1)) flowing between
independent participants, with every provider call recorded in-band as a nested
`baton.exchange/v1` record so a conversation is observable and replayable after
the fact.

Two facts follow from that stance and frame everything below:

1. A **participant** is an envelope-in / envelope-out boundary
   (`src/participant.rs`). Whatever answers — an in-process client, a
   subprocess, a mailbox peer, a full external agent — is reached the same way:
   hand it a request envelope, get back a correlated response envelope. The
   trait is *infallible by contract*: a provider or delivery failure comes back
   as a **delivered** `kind: "error"` envelope, never a propagated `Err`.
2. Whether Baton **owns a provider** at all depends on which participant path
   you run — the next section.

## The two participant paths

This is the central architectural fact, and it is the frame for the
provider-config work (#84/#86): **Baton has two participant paths, and only one
of them makes Baton own an LLM client.**

### Phase 1 — external-agent path (`ExternalAgentParticipant`)

Baton is a **thin wrapper**. Each reply is one headless run of a full-tooled
native agent CLI (e.g. `claude`) — an agent that edits files and runs
git/bash/MCP on its own. Baton delivers the request body on the agent's stdin,
runs it in a fixed git-worktree cwd, and captures its final stdout as the reply
body (isolated from streaming chatter by an `OutputAdapter`: `Raw` takes the
whole stdout, `Json` takes the final JSON line's result field).

The external agent **owns its own provider** — its own model, key, and
credentials, configured through its own environment. Baton supplies *no*
Messages-API client here; it owns only the substrate (same cwd each round,
stdin delivery, output capture). Cross-round memory is the agent's job,
reconstructed from durable artifacts (the shared git branch/worktree, the issue
thread, prior mailbox history) — headless-per-message, no in-memory session.

Wired at the CLI by `baton serve --agent-cmd` (see
[external-agent.md](external-agent.md#external-agent-role---agent-cmd)).

### Phase 2 — local path (`LocalParticipant` + `Transport`)

Baton **owns the Messages-API client**. A `LocalParticipant` is a system prompt
plus a `Transport` (`src/transport/`), and the production `Transport` is
`ClaudeClient` — a non-streaming Claude-compatible Messages client. One reply is
exactly one provider exchange, whose token usage is stamped into the nested
`baton.exchange/v1` record. Here Baton *does* need a provider: a model, a
base URL, and a credential (`src/config.rs`), which is what the #84/#86
provider-config decision is about.

Two more participants are **variants of this same local path**, not a third
kind — they move the same envelope boundary across a process line:

- `SubprocessParticipant` — each reply is a separate `baton exchange` OS
  process (an independent Baton agent over pipes; still a `LocalParticipant`
  inside that child).
- `MailboxParticipant` — each reply is a round-trip over the file-mailbox to a
  peer `baton serve` daemon (whose in-process `LocalParticipant` answers).

The distinction that matters across all of them: a **delivered** error nests a
provider record (the peer/child ran a call it vouches for); a **machinery**
failure (spawn failure, non-zero exit, malformed/absent envelope, timeout, no
correlated reply) is a *synthesized* error envelope with **no** nested record —
the driver observed no provider call it can vouch for.

## Module layout

The crate root (`src/lib.rs`) keeps each module intentionally thin. Grouped by
concern:

| Concern | Modules |
|---|---|
| **Participant seam** | `participant` — the envelope-in/out boundary and its four impls (local, subprocess, mailbox, external-agent) |
| **Provider transport** | `transport` (boundary + `claude` Messages client + `http` execution seam), `model` (typed prompt/reply/session), `config` (env-backed runtime config: credential, base URL, model, timeout) |
| **A2A envelope & driver** | `message` (the `baton.message/v1` envelope), `converse` (the governed N≥2 participant conversation driver), `registry` (static name → mailbox routing) |
| **Mailbox** | `mailbox` — the addressable, crash-safe on-disk queue backing `baton serve` |
| **Session ownership** | `service` — the host-owned supervisor spawning `baton serve` sessions as its own children (see [`docs/service.md`](service.md)) |
| **Trail** | `events` (structured JSONL recording of each exchange), `log` (reading/rendering/replaying the trail) |
| **Identity** | `roles` — per-role home directories and layered env>config identity resolution |
| **Surface & plumbing** | `cli` (command entry surface), `error` (shared error/result types) |

## CLI verb → A2A model map

Each verb is a projection of the A2A model onto the command line.
[conversations.md](conversations.md), [mailbox.md](mailbox.md), and
[external-agent.md](external-agent.md) document each verb's flags in full; this
is the map from verb to concept.

| Verb | A2A concept |
|---|---|
| `ask` | **Single-turn** — one first-prompt → first-reply against the provider. The shallow end. |
| `session` | **Multi-turn** — an interactive REPL accumulating conversation history across turns; resumable. |
| `exchange` | **Structured exchange** — one `baton.message/v1` request → correlated response round-trip, with the provider call nested in-band. The unit the subprocess path spawns. |
| `converse` | **Governed conversation** — drive two participants to a terminal condition (turn cap, token budget, unilateral `done`). Side B may be local or mailbox-backed. |
| `converse-ring` | **N-party ring** — the round-robin generalisation of `converse` over a static routing registry (name → mailbox). |
| `serve` | **Mailbox responder** — a long-lived daemon answering `baton.message/v1` requests from a file-mailbox; `--agent-cmd` selects the external-agent path. |
| `send` | **Mailbox client** — post a request into a mailbox (by path or by role name via the registry) and consume the correlated reply. |
| `status` | **Liveness** — report a mailbox's state (`idle-done` / `busy` / `crashed-stale`) plus queue depth. |
| `log` | **Trail** — inspect, merge, and replay the recorded `baton.exchange/v1` / `baton.message/v1` trail. |

## For contributors

A companion to [development.md](development.md) — where the code lives when
you go to change it:

- **Adding a way to answer a message** → `src/participant.rs`. Implement
  `Participant`; keep the trait infallible (reconcile any machinery failure into
  a synthesized `kind: "error"` envelope via `synthesize_error_response`, nesting
  no record). Test doubles live in `participant::testing`, compiled only under
  `cargo test`.
- **Adding/altering a provider** → `src/transport/`. `Transport` is the
  boundary; `claude.rs` is the concrete client; `http.rs` is the injectable HTTP
  seam that lets client tests run without a network (`HttpClient` fakes).
- **Changing the envelope or its recording** → `src/message.rs` (the wire shape)
  and `src/events.rs` + `src/log.rs` (the JSONL trail and its rendering).
- **Conversation control flow** (turn caps, budgets, terminal conditions) →
  `src/converse.rs`.
- **Mailbox on-disk format / delivery semantics** → `src/mailbox.rs`.
- **Per-role identity resolution** → `src/roles.rs` and `src/config.rs`.

A full `CONTRIBUTING.md` is intentionally out of scope for now; this section is
the orientation, and the module doc-comments carry the detail.
