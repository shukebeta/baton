# `baton service`: a host-owned supervisor for `baton serve` sessions

`baton serve --agent-cmd` is already a resident, single-instance-locked mailbox
daemon (see [`src/mailbox.rs`](../src/mailbox.rs)), but nothing durable *owns*
it: launch it directly from a client or tool-runner process and the daemon
inherits that process's own tree as its parent. `setsid`/`disown` only detach
a process *group* — an external agent/tool runner that reaps its process tree
still takes the daemon down with it. `baton service run --control <dir>` is
the missing owner: a long-lived process, meant to be kept alive by an OS
service manager, that spawns each `baton serve` session as its own direct
child so a short-lived client can start, inspect, stop, or tear one down
without ever sharing a process tree with it.

## Ownership contract

- **Every session is a direct child of `service run`, never of the client
  that requested it.** A submitting client's own process tree can die the
  moment `service start` returns; the session keeps running.
- **Only `service run` holds the child handle.** It reaps exited children
  non-blockingly as its loop ticks, so it never accumulates zombies while it
  stays alive. If `service run` itself exits or crashes, the kernel reparents
  its still-running children to init, which reaps them on their own eventual
  exit — a restart (e.g. systemd `Restart=on-failure`) never orphans a
  zombie either.
- **Each session is its own process-group leader.** `service start` spawns
  `baton serve` with `process_group(0)` (safe, stable since Rust 1.64 —
  deliberately not `pre_exec(setsid)`, which would need `unsafe`), so a
  `kill -<pid>` from `service stop`/`teardown` reaches both the `serve`
  process and any in-flight `--agent-cmd` grandchild it spawned.
- **`baton service` carries no systemd-specific assumption.** The unit under
  `packaging/systemd/` is an external liveness wrapper only — start/stop/
  restart-on-death — layered on top of a control surface that works
  identically whether or not that unit is ever installed.
- **Unix only (Linux and macOS).** Process groups and the `kill` escalation
  this module relies on have no equivalent in this crate's dependency-free
  design on Windows; `baton service` fails clearly there instead of silently
  falling back to a weaker guarantee. A Windows host-service integration is
  tracked separately.

## Control surface

`--control <dir>` holds:

- `service.lock` — the exclusive single-instance advisory lock, held by
  `service run` for as long as it runs (mirrors `serve`'s own `serve.lock`).
  A second `service run` against the same `--control` dir is refused.
- `service.stop` — the cooperative-stop sentinel `service teardown` drops for
  a live `service run` to observe between polls (mirrors `serve.stop`).
- `requests/` / `processing/` / `responses/` — the atomic-rename request
  protocol `service start` uses to reach the live `run` loop: a session-spec
  request is delivered into `requests/`, claimed into `processing/` by `run`,
  and answered into `responses/` keyed by the request id. This is the only
  operation that must execute *inside* the long-lived process, since spawning
  the child there is the entire point.
- `sessions/<id>.json` — one durable session record per session (its
  effective spec, real PID, and recorded start time). `status`/`stop`/
  `teardown` read this directly and act on the OS process by PID — none of
  them need `run` to be alive, so a session started by a since-crashed `run`
  can still be inspected, stopped, or torn down.

## Lifecycle contract

```
baton service run --control <dir>
baton service start --control <dir> --inbox <dir> --outbox <dir> [--poll-ms <n>]
                    [--agent-cmd <program> [--agent-arg <arg>]... [--agent-cwd <dir>]
                     [--agent-timeout-ms <n>] [--agent-output raw|json [--agent-result-key <key>]]]
                    [--role <name>]
baton service status --control <dir> [--session <id>]
baton service stop --control <dir> --session <id>
baton service teardown --control <dir>
```

- **`service run`** acquires the control lock and blocks, spawning and
  reaping sessions until it observes a cooperative stop. It exits cleanly on
  `teardown`; a single malformed or failing start request logs a warning and
  never crashes the loop for the sessions it already owns.
- **`service start`** takes exactly the flags `baton serve` itself takes (they
  become the session's `SessionSpec`, reconstructed into an equivalent
  `baton serve` argv by `run`). It submits the spec and returns a session id
  as soon as `run` has spawned the child and persisted its record — it never
  waits on a served turn. Fails fast, with a clear error, if no `service run`
  is currently live on `--control`, rather than hanging on a request no one
  will ever answer.
- **`service status`** reports the service's own liveness plus every session's
  (or just `--session <id>`'s). Per-session liveness checks the recorded PID
  against `/proc/<pid>` on Linux — alive, not a zombie, and its start time
  still matches the record — so a PID recycled after a restart is reported as
  crashed rather than (incorrectly) live; on non-Linux Unix hosts this
  degrades to an existence-only `kill -0` check, which cannot detect PID
  reuse.
- **`service stop --session <id>`** tries `serve`'s own cooperative stop
  against the session's inbox first, then escalates to a bounded
  `SIGTERM`/`SIGKILL` on the session's process group if it is still alive.
  Idempotent — stopping an already-gone session is a no-op success.
- **`service teardown`** applies `stop` to every known session, then requests
  `run`'s own cooperative stop. Idempotent, and safe to call whether or not
  `run` is currently alive (e.g. to reap stale records left by a `run` that
  already crashed).

Standalone `baton serve` (run directly, without `baton service`) is
unaffected by any of this — `service` only ever spawns the same `serve`
binary as a subprocess with the same flags a caller would pass by hand.

## Task lifecycle (`baton task`)

`baton task` extends the same control plane with a generic, service-owned
asynchronous job: a command that keeps running (and reports back) after the
submitting turn/process has already exited. It exists because a headless
`baton serve --agent-cmd` turn runs one cold agent invocation per mailbox
message and has no resident place to wait on a slow command; a task is that
missing place, owned by the same `service run` process that owns sessions.

```
baton task start --control <dir> --session <id> --command <program>
                 [--arg <arg>]... [--cwd <dir>] [--env KEY=VALUE]...
                 [--milestone-ms <n>]... --max-duration-ms <n>
                 --callback-inbox <dir> [--callback-role <name>]
baton task status --control <dir> [--task <id>]
baton task cancel --control <dir> --task <id>
```

- **`task start`** submits a `TaskSpec` (schema `baton.task-spec/v1`) and
  returns a stable task id as soon as `run` has spawned the command and
  persisted its record — like `service start`, it never waits on the command
  to finish, and fails fast if no `service run` is live on `--control`.
- **`--session <id>` is the ownership tag, not a routing target.** A task is
  owned and reaped by whichever `baton service` session names it here,
  independent of where its events are delivered — see "Ownership vs. callback"
  below.
- **`--milestone-ms <n>` is opaque to baton.** The core carries no default
  duration set and no cadence semantics; a caller supplies whichever
  durations (elapsed ms since spawn) it wants an event at, or none. The same
  timer that enforces `--max-duration-ms` also fires milestones, so there is
  no separate scheduler subsystem and no consumer-side polling requirement.
- **`--callback-inbox <dir>` / `--callback-role <name>`** address a mailbox
  root exactly like every other `baton` surface (`SessionSpec.inbox`,
  `send --to`, …): `inbox` is where events land, `role` is an optional
  identity tag carried on the delivered envelope for the recipient's own
  framing. Neither is used to resolve delivery beyond that.
- **`task status`** reports one task (`--task <id>`) or every known task:
  `running`/`completed`/`failed`/`timeout`/`cancelled` state, exit code,
  elapsed milliseconds, command identity, and the captured stdout/stderr log
  paths. Reads the durable `TaskRecord` directly by PID, so it works whether
  or not `run` is currently alive.
- **`task cancel`** is idempotent: kills the task's whole process group if
  still running (cooperative cancel sentinel consumed by the next tick, so
  the resulting terminal event reads `cancelled` rather than `failed`), and a
  no-op success if the task is already gone.

### Event delivery and dedup contract

Each configured milestone and the eventual terminal outcome produces exactly
one event, delivered via the same atomic mailbox write every other `baton`
surface uses (`mailbox::deliver_to`, see
[`src/mailbox.rs`](../src/mailbox.rs)) into the callback inbox's `pending/` as
a `baton.message/v1` envelope with `kind: "notify"` (see
[`src/message.rs`](../src/message.rs)) and a JSON `TaskEventBody` (`task_id`,
`kind: "milestone"|"terminal"`, and — depending on kind — `milestone_index`,
or `state`/`exit_code`/`elapsed_ms`) as its `body`.

Every event's id is **deterministic and content-addressable**: composed only
from the task id, the event kind, and — for a milestone — that milestone's
configured index (e.g. `<task-id>-milestone-0`, `<task-id>-terminal`; see
`task_event_id` in [`src/task.rs`](../src/task.rs)). No wall-clock or
fire-time input feeds the id, so a crash-restart replay or any other
redelivery regenerates the exact same id every time. Delivery is
at-least-once, matching every other `baton` mailbox path; a consumer dedups a
redelivered event for free via the mailbox's own `done/`-membership check
(`Mailbox::claim_next` drops an id it has already claimed-and-completed) — no
resident store or polling loop needed between turns.

### Ownership vs. callback

A task's ownership and reaping are scoped strictly to the `--session <id>` it
names, never to its callback target. `service stop --session <id>` and
`service teardown` reap every task owned by that session — killing its
process group and removing its record — even if that task's
`--callback-inbox` points somewhere entirely outside the owning session's own
mailbox. The callback is a delivery target only.

### Injectable clock

The task loop's milestone/max-duration timing decisions are driven by an
injectable `Clock` trait (`SystemClock` in the real `run_service` loop, a
manually-advanced `FakeClock` in tests; see [`src/task.rs`](../src/task.rs)),
so a contract test can submit
a task and drive it through milestone and terminal delivery without any real
`sleep` — see `service::imp::tests::tick_one_task_delivers_milestone_via_fake_clock_no_real_sleep`
in [`src/service.rs`](../src/service.rs). A separate live-subprocess
regression, `service_task_survives_submitting_client_and_is_owned_by_run` in
[`tests/integration_test.rs`](../tests/integration_test.rs), proves the
ownership/survival contract against a real `baton service run`, with trivial
real thresholds and no `sleep()` of its own beyond bounded polling for
delivery.

### `bg-run` integration boundary

`baton task` is generic and provider-neutral: it owns process and mailbox
lifecycle only, never an agent's context or model conversation, and carries
no `bg-run`-specific naming or default cadence. The my-ai-team `bg-run`
command is the product-facing adapter: it translates its own options into a
`baton task start` call, supplies its own milestone-duration defaults (baton
core has none), maps delivered task events onto its existing `[relay]`
wake/result-file contract, and returns the task id immediately. `bg-run` is
not, and will not become, a `baton` command name — Baton core exposes only
the generic `task` namespace.

## systemd setup

`packaging/systemd/baton.service` is an example per-user unit:

```bash
mkdir -p ~/.config/systemd/user
cp packaging/systemd/baton.service ~/.config/systemd/user/
# edit ExecStart's --control path for your host
systemctl --user daemon-reload
systemctl --user enable --now baton.service
```

`ExecStart` runs `service run`; `ExecStop` runs `service teardown` so a
`systemctl --user stop`/`restart` tears every managed session down instead of
orphaning them. `Restart=on-failure` restarts `run` itself if it dies — any
sessions it had already spawned are, by then, children of init, which reaps
them on their own eventual exit; a restarted `run` does not re-adopt them,
so `service status` will report them as no-longer-visible processes needing a
fresh `service start`. Run `loginctl enable-linger <user>` if the service
should also survive the user's last session logging out.
