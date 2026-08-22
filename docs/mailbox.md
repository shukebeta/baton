# Mailbox transport: `serve`, `send`, `status`

The asynchronous, file-backed transport: a long-lived responder, the client that
posts to it and consumes the correlated reply, the liveness probe over the same
mailbox, and the routing registry that resolves a role name to a mailbox pair.

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
             [--agent-output raw|json [--agent-result-key <key>]]]
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
  in-process provider call (see [External-agent role](external-agent.md#external-agent-role---agent-cmd)).
- `--role <name>` — resolve the answering identity from the role's
  [home directory](configuration.md#role-homes-rolesname) (`roles/<name>/`), so a party is stood
  up by name instead of hand-assembled env vars. In-process mode feeds the role's
  layered config (model, base URL, credential, system prompt, timeouts) to the
  provider call; agent mode fills `--agent-cwd` from the role's `cwd` when the
  flag is not passed — baton stops there, since it carries no other
  agent-specific knowledge. **Explicit flags and env always override the role**
  (`flag > env > role config > defaults > default`). With `--role`, each answered
  exchange is also recorded as a per-role
  [session](configuration.md#per-role-session-recording) under
  `roles/<name>/sessions/<conversation_id>.jsonl`.
- `--stop` — cooperatively stop the `serve` running on `--inbox` (see
  [Shutdown](#shutdown-cooperative-graceful-stop)); takes only `--inbox`.

Without `--agent-cmd`, each side configures the answering participant exactly as
`exchange`/`ask` do (`BATON_MODEL`, `BATON_SYSTEM_PROMPT`, the credential env,
`BATON_EVENT_LOG`), so a served message runs the identical exchange and records
the same trail. A `--role` supplies these same values from the role's home when
the env leaves them unset.

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

## Routing registry (name → mailbox)

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
is. A party's **role-owned identity** — system prompt, model, credential, and
cwd — lives in its [role home](configuration.md#role-homes-rolesname) (`roles/<name>/`), and
each ring member is its own `baton serve --role <name>` daemon that loads it.
External-agent MCP configuration remains caller-owned and must be supplied
through the `--agent-cmd` wrapper or `--agent-arg` passthrough. So the two
surfaces stay cleanly split: the registry is pure routing, the role home stores
Baton-supported identity, and standing up a party is "add a `roles/<name>/`
directory + a registry entry", not hand-assembling per-process env.

**Non-goals (v1).** The registry is deliberately minimal:

- **No convention-derived paths** (`<root>/<name>/…`) — a possible later
  zero-config layer over the explicit registry.
- **No dynamic discovery** (register / heartbeat / liveness / join-leave) — the
  roster is fixed for the run.
- **No `to`-based routing** — the driver picks the next recipient by ring order;
  the registry only resolves names to mailboxes.
