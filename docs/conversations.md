# Conversations: `ask`, `session`, `exchange`, `converse`, `converse-ring`

The conversation surface, from a single prompt to an N-party ring: one-shot
replies, the resumable multi-turn REPL, the structured `baton.message/v1`
round-trip, and the two governed drivers. The envelope and trail schemas these
verbs write are in [protocol.md](protocol.md); the mailbox they reach peers over
is in [mailbox.md](mailbox.md).

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
  into whole sessions (see [Session trail](protocol.md#session-trail)).
- `baton session --role <name>` speaks as that role's identity and records the
  same trail under the role's home
  ([per-role session recording](configuration.md#per-role-session-recording)), stamping the
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
- In a shared append log, outcomes carrying both `session_id` and `turn_index`
  close the exact matching request, so interleaved sessions resume with their
  own histories. Outcomes lacking both fields retain the file-order fallback for
  older sequential trails and A2A seat trails.
- A trail whose final line is torn (an unclean prior shutdown) resumes from the
  last complete turn — the incomplete trailing record is dropped with a warning,
  matching the trail's [torn-tail handling](protocol.md#session-trail).

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
mailbox delivery see [`baton serve`](mailbox.md#serving-a-mailbox-baton-serve).

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

The same boundary lets side B be a **live [`baton serve`](mailbox.md#serving-a-mailbox-baton-serve)
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
only resolves a name to a mailbox, it does not route.

```
baton converse-ring --registry <path> --roster <a,b,c> (--seed <text> | --seed-file <path>) [--await-ms <n>] [--out <path>]
```

- `--registry <path>` — the [routing registry](mailbox.md#routing-registry-name--mailbox)
  (JSON), loaded once at startup.
- `--roster <a,b,c>` — the ring order, a comma-separated list of participant names
  (≥ 2, no blanks, no duplicates). Every name must exist in the registry; an
  unknown name is a **startup error** before any turn runs.
- `--seed <text>` / `--seed-file <path>` (exactly one) — the opening message,
  addressed from `roster[0]` to `roster[1]`.
- `--await-ms <n>` — how long each turn waits for its peer's reply (positive
  integer; default `60000`), as for `converse --b-await-ms`.
- `--out <path>` — where the JSONL trail is written; stdout when omitted.

### Routing registry (name → mailbox)

The `--registry` file maps each participant name to its `{inbox, outbox}` mailbox
pair. It is shared with `baton send` and `baton status`, and its format, guarantees,
and non-goals are documented once in
[mailbox.md → Routing registry](mailbox.md#routing-registry-name--mailbox).

### Worked example — three peers

```bash
# Three long-lived responders, one per ring member — each loads its own
# identity from $BATON_HOME/roles/<name>/ via --role:
baton serve --inbox /tmp/alice/inbox --outbox /tmp/alice/outbox --role alice --poll-ms 20 &
baton serve --inbox /tmp/bob/inbox   --outbox /tmp/bob/outbox   --role bob   --poll-ms 20 &
baton serve --inbox /tmp/carol/inbox --outbox /tmp/carol/outbox --role carol --poll-ms 20 &

# Drive the ring from a registry (see mailbox.md) saved as /tmp/roster.json:
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
