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
- **Each session has an OS ownership boundary.** Unix uses a dedicated process
  group. Windows uses a non-inheritable Job Object adopted by `baton serve`
  before it can spawn an `--agent-cmd` child. Both boundaries reach the
  session's descendants without making the submitting client their owner.
- **`baton service` carries no systemd-specific assumption.** The unit under
  `packaging/systemd/` and the LaunchAgent under `packaging/launchd/` are
  external liveness wrappers only — start/stop/restart-on-death — layered on
  top of a control surface that works identically whether or not either is
  installed.
- **Linux, macOS, and Windows.** Linux liveness uses `/proc/<pid>/stat`; macOS
  uses `ps` process state plus the second-granular `lstart` value; Windows uses
  `GetProcessTimes` plus the recorded Job Object name. A probe reports `live`,
  `dead`, or `unresolved`; the last state means baton cannot prove the PID and
  ownership identity and is never treated as dead. Windows Job Objects are
  not configured with kill-on-close, and their handles are retained until
  `ActiveProcesses == 0`.

## Control surface

`--control <dir>` holds:

- `service.lock` — the exclusive single-instance advisory lock, held by
  `service run` for as long as it runs (mirrors `serve`'s own `serve.lock`).
  A second `service run` against the same `--control` dir is refused.
- `service.admission.lock` — a short-lived advisory lock shared by task
  admission and session cleanup. It is separate from `service.lock`, which
  the long-lived `service run` holds for its entire lifetime.
- `service.stop` — the cooperative-stop sentinel `service teardown` drops for
  a live `service run` to observe between polls (mirrors `serve.stop`).
- `requests/` / `processing/` / `responses/` — the atomic-rename request
  protocol `service start` uses to reach the live `run` loop: a session-spec
  request is delivered into `requests/`, claimed into `processing/` by `run`,
  and answered into `responses/` keyed by the request id. This is the only
  operation that must execute *inside* the long-lived process, since spawning
  the child there is the entire point.
- `sessions/<id>.json` — one durable session record per session (its
  effective spec, real PID, canonical `started_at` string, and on Windows its
  optional Job Object name). `status`/`stop`/`teardown` read
  this directly and act on the OS process by PID — none of them need `run` to
  be alive, so a session started by a since-crashed `run` can still be
  inspected, stopped, or torn down.

## Lifecycle contract

```
baton service run --control <dir>
baton service start --control <dir> --inbox <dir> --outbox <dir> [--poll-ms <n>]
                    [--agent-cmd <program> [--agent-arg <arg>]... [--agent-cwd <dir>]
                     [--agent-timeout-ms <n>] [--agent-output raw|json [--agent-result-key <key>]]]
                    [--role <name>]
baton service status --control <dir> [--session <id>]
baton service stop --control <dir> --session <id> [--force]
baton service teardown --control <dir> [--force]
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
  will ever answer. Relative `--inbox`, `--outbox`, and `--agent-cwd` values
  are resolved against the submitting client's current working directory and
  persisted as absolute paths before the request is sent; resolution is
  lexical and does not canonicalize or require the target to exist at
  submission time. Absolute values are preserved unchanged.
- **`service status`** reports the service's own liveness plus every session's
  (or just `--session <id>`'s). Each record retains the compatibility boolean
  `live` (`true` only for `liveness: "live"`) and exposes the full
  `liveness` state. Per-session liveness checks the recorded PID against
  `/proc/<pid>` on Linux — alive, not a zombie, and its start time still
  matches the record — so a PID recycled after a restart is `dead`. On macOS,
  records with `start_epoch_secs` compare that canonical epoch directly and
  do not invoke a corroborator; older records use
  `ps -ww -p <pid> -o state=,lstart=,command=` and fall through from an absent
  or mismatched `started_at` to the argv/instant corroborators below. Each
  macOS probe fixes `LC_ALL` and `LC_TIME` to `C` and `TZ` to `UTC` before
  invoking `ps`, so `lstart` remains comparable when the supervisor and
  client inherit different locale or time-zone settings. `lstart` is
  second-granular and is a BSD/procps extension rather than a POSIX interface.
- **`service stop --session <id>`** tries `serve`'s own cooperative stop
  against the session's inbox first. Unix then escalates with bounded
  `SIGTERM`/`SIGKILL` process-group signals; Windows escalates with
  `TerminateJobObject`. Without `--force`, an unresolved session or owned task
  remains durable and the command exits non-zero after printing its id, PID,
  liveness, and recorded argv to stderr. `--force` asserts the operator's
  identity claim. If a Windows Job Object cannot be resolved, it terminates
  only the recorded PID, removes the record, and warns that descendants may
  survive; it never claims tree reach in that case.
  It serializes that cleanup with task admission, so a task is either
  admitted before the stop and reaped with the session or rejected after the
  session is no longer a live owner. Idempotent — stopping an already-gone
  session is a no-op success.
- **`service teardown`** first requests `run`'s cooperative stop, then waits
  for `run` to release the control lock before taking its session snapshot and
  applying `stop` to every record. The released lock is the admission barrier:
  a concurrent `service start` either was handled before the barrier and is in
  the snapshot, or fails because no live service remains (an already-written
  request may remain unprocessed); it cannot spawn a session outside the
  snapshot. Teardown also takes the short-lived admission lock while it
  drains the snapshot, covering task cleanup as well. Without `--force`, it
  keeps unresolved records, prints their identity details to stderr, and exits
  non-zero; an initially unresolved task is retained immediately without a
  per-task grace delay. `--force` applies the same platform-specific rule and
  removes them, including task admission files, on the operator's assertion.
  Idempotent, and safe to call whether or not `run` is currently alive (e.g.
  to reap stale records left by a `run` that already crashed).

### Liveness resolution and safe cleanup

Status exposes three identity outcomes for a running record:

- `live` means the process is present and its PID identity is corroborated.
- `dead` means the PID is absent, a zombie, or positively belongs to a
  different process. Dead records may be removed.
- `unresolved` means the PID exists but the available identity evidence is
  unreadable or insufficient. It is fail-closed: no signal, removal, or failed
  terminal state is inferred from it.

The resolution ladder is platform-specific:

- On Linux, an unreadable `/proc/<pid>/stat` is `unresolved`; a missing PID or
  zombie is `dead`; a matching start-time tick is `live`; and a mismatched
  recorded tick is `dead`. A legacy session with no start key may use the
  exact NUL-separated `/proc/<pid>/cmdline` argv as a corroborator. A legacy
  task may use matching argv to confirm `live`, but a mismatch is only
  `unresolved`, because a task such as `bash -c ...` can exec-replace its argv
  while retaining the same PID. Linux has no epoch instant leg: `/proc` start
  ticks are relative to boot, and `/proc/<pid>` directory mtime is not a safe
  substitute because procfs can restamp it on lookup.
- On macOS, a record with `start_epoch_secs` compares the probe's parsed
  canonical UTC epoch: equal is `live`, unequal is `dead`, and an unparseable
  probe is `unresolved`. A legacy record with no `start_epoch_secs` uses a
  mismatched or absent `started_at` key as the trigger for a session argv
  suffix match or a task start-instant comparison. The probe is pinned to
  `LC_ALL=C LC_TIME=C TZ=UTC` and parses `lstart` as a proleptic Gregorian UTC
  epoch. For a task, `Δ = started_ms - lstart_epoch_seconds * 1000`: a negative
  delta is `dead`, `0 <= Δ < 6000` ms is `live`, and a larger delta is
  `unresolved`. The 6000 ms bound allows one second of `lstart` truncation plus
  5000 ms of spawn-to-record latency; overestimating it can only preserve an
  unresolved record, never condemn a genuine task. Task argv can confirm only
  when the durable instant is unavailable and never condemns a mismatch.

- On Windows, `GetProcessTimes` supplies the creation-time start key. A live
  PID with a matching key is `live` regardless of whether its recorded Job
  Object name resolves; a missing, inaccessible, or mismatched key is
  `unresolved`. The Job Object is a
  signaling and descendant-reachability preference, not part of process
  identity. Task Job Object handles are inherited by the task process so the
  named object normally remains available for startup re-adoption while the
  task tree drains. If a name still cannot be opened after a restart, normal
  cancellation, timeout, stop, and cleanup signal only the corroborated
  recorded PID and warn that descendants may survive. A PID whose identity
  cannot be corroborated remains fail-closed and is never signalled.

New macOS records retain `started_at` as the operator-readable UTC string and
also persist `start_epoch_secs`; the field's presence is the format-version
marker. A legacy record that is rescued as live by a lock-holding cleanup or
admission-reconciliation path is rewritten once with the epoch marker. The
read-only `service status` and the supervisor's lock-free task tick never
rewrite records, so an unresolved legacy task remains safe to retry until a
cleanup path can upgrade it.

`service stop` and `service teardown` always try the identity-free cooperative
mailbox stop first. They then signal only `live` records. On Windows, a live
record uses `TerminateJobObject` when its name resolves and otherwise falls
back to the corroborated recorded PID with a descendants-may-survive warning.
Dead records are
removed, terminal task records are removed without probing or signalling their
recorded PID, and initially unresolved records are retained immediately
without a per-task grace delay. A retained record's id, PID, liveness, and
recorded argv are printed to stderr and the command exits non-zero. `--force`
is the explicit operator assertion of identity: Unix sends process-group
signals; Windows follows the same Job Object/PID fallback and removes the
record even when identity remains unresolved.
`task cancel` has no force flag; it leaves its cooperative cancel sentinel for
the supervisor, signals a corroborated live PID even when its Job Object name
is gone, and never escalates an unresolved identity. If that controlled PID
has exited, the supervisor can finalize the cancellation even when an
unresolvable descendant might still exist.

`TaskRecord.started_ms` and `Clock::now_ms()` are Unix epoch milliseconds.
`TaskRecord.start_epoch_secs`, when present on macOS, is the canonical
second-granular process-start epoch parsed from `ps lstart` and is the task's
version marker and fast-path identity key.
`FakeClock::at` initializes that same unit for deterministic instant-leg tests.

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
  to finish, and fails fast if no `service run` is live on `--control`. The
  `--session` value must name a managed session whose durable record exists
  and whose recorded process is currently live. A missing record or a stale
  record whose process is no longer live is rejected with a non-zero owner
  error before any task process, log directory, or `TaskRecord` is created.
  While waiting for the response, the submitting client probes the control
  lock. If `run` releases that lock before answering, the client takes the
  short-lived admission lock, re-checks for a response, writes a durable
  rollback marker, then removes the still-pending request from both
  `task-requests/` and `task-processing/` and exits non-zero with a `task start
  request was not admitted` error. Startup reconciliation or the request loop
  clears the marker only after it has removed any associated task record and
  process, so a supervisor crash during cleanup leaves the request suppressed
  on the next restart. A response
  already written before the lock is released wins this race and remains a
  successful task start.
- **`--session <id>` is the ownership tag, not a routing target.** A task is
  owned and reaped by whichever `baton service` session names it here,
  independent of where its events are delivered — see "Ownership vs. callback"
  below.
- **`--cwd <dir>`** sets the task command's working directory. A relative path
  is resolved lexically against the submitting `task start` client's current
  directory; an absolute path is preserved unchanged. The resolved path is
  persisted in the task record before the service spawns the command. If the
  flag is omitted, the command inherits the service's working directory.
- **`--milestone-ms <n>` is opaque to baton.** The core carries no default
  duration set and no cadence semantics; a caller supplies whichever
  durations (elapsed ms since spawn) it wants an event at, or none. The same
  timer that enforces `--max-duration-ms` also fires milestones, so there is
  no separate scheduler subsystem and no consumer-side polling requirement.
- **`--callback-inbox <dir>` / `--callback-role <name>`** address a mailbox
  root exactly like every other `baton` surface (`SessionSpec.inbox`,
  `send --to`, …): `inbox` is where events land, `role` is an optional
  identity tag carried on the delivered envelope for the recipient's own
  framing. A relative inbox path is resolved lexically against the submitting
  `task start` client's current directory and persisted as an absolute path;
  an absolute path is preserved unchanged. Neither the inbox nor role is used
  to resolve delivery beyond that.
- **`task status`** reports one task (`--task <id>`) or every known task:
  `running`/`completed`/`failed`/`timeout`/`cancelled` state, exit code,
  elapsed milliseconds, `live` plus the distinct `liveness` identity state,
  and the following task identity/output fields:
  `command` is the effective executable identity, while `stdout_path` and
  `stderr_path` are the paths to the captured stdout and stderr logs.
  These fields are present for both running and terminal tasks. Reads the
  durable `TaskRecord` directly by PID, so it works whether or not `run` is
  currently alive.
- **`task cancel`** is idempotent: after the cooperative cancel sentinel it
  terminates the task's Job Object on Windows or process group on Unix, so the
  resulting terminal event reads `cancelled` rather than `failed`. It is a
  no-op success if the task is already gone.
- **Unix task draining** treats the task's process group as the ownership and
  reaping boundary. If the direct command exits while a same-group descendant
  remains, the durable task record stays `running`; status, cancellation, and
  max-duration enforcement continue to track and signal that group. The task
  becomes terminal only after the group drains, matching Windows' Job Object
  active-process-count behavior. A descendant that detaches into another
  process group is outside this boundary.

### Supervisor restart reconciliation

`service run` scans durable task records before it accepts new requests. Each
new running record stores the task's spawn time as Unix epoch milliseconds, so
milestone and max-duration decisions continue from the original task start
after a restart. Linux requires a recorded `/proc/<pid>/stat` start key to
match the current process; a missing key uses the fail-closed argv fallback,
while a mismatched key is `dead` and is never adopted or signalled. On macOS,
an absent or mismatched `lstart` key uses the task instant corroborator
described above, so a live exec-replaced task remains tracked instead of being
finalized as failed.

The restarted supervisor cannot recover a `std::process::Child` handle for a
task reparented to init. It therefore tracks a corroborated PID directly and,
on Unix, the recorded process group as well: milestones and timeout signals
continue while either the direct PID or a same-group descendant is live. A
rehydrated Unix task whose direct PID is gone but whose group still has a
member remains `running`; an unresolved group probe is retained and never
treated as drained. Once the group is drained, no exit status can be
recovered through this path, so the task is recorded as `failed` with
`exit_code: null`; a timeout or cancellation remains `timeout` or `cancelled`
when the supervisor initiated that outcome. On Windows, the equivalent Job
Object active-process count continues to define the drain boundary. A missing
Job Object no longer changes a matching PID to `unresolved`; it only changes
termination from tree-wide to PID-only, with a descendants-may-survive
warning. State is persisted before the deterministic terminal callback is
delivered, and a delivery failure leaves the tracker in place to retry the
same event id. A terminal record is replayed once on the next startup for the
same reason; the mailbox's done ledger drops it if delivery already
completed.

Prepared admissions are not active tasks: startup reconciliation owns their
cleanup, and `rehydrate_tasks` excludes them from the in-memory tracker. If a
prepared task's PID identity is unresolved, its durable record and rollback
marker remain in place while pending request locations are discarded. The
service does not finalize the task or deliver a terminal callback. A later
startup removes the record and its admission files after the PID is positively
dead. `service teardown --force` is the explicit operator path for removing
residue whose identity remains unresolved.

Task admission is a durable transaction keyed by the task-start request id.
After spawning and persisting a `TaskRecord`, `run` records the `prepared`
phase, then durably changes it to `committed` before publishing the success
response, and finally records `responded` while holding the admission lock.
The task-start client takes that same lock, atomically renames the response to
a private claim, parses it, writes `task-start-ack/<request-id>.json`, and
removes the claim only after the acknowledgement is durable. A claim without
an acknowledgement is restored to the response on the next startup.

Startup reconciliation removes a prepared task only after its process is
positively dead (or its record is already terminal), including its process and
task record. An unresolved prepared task remains durable and is excluded from
rehydration until a later probe resolves its identity. For rollback, the
marker is cleared only after the task record, response/claim files, and both
pending request locations have been removed; repeated startup passes are
therefore safe if the supervisor crashes during cleanup. A committed task with
an acknowledgement is finalized as `responded` without recreating its
response. A committed task with an existing response or recoverable claim is
also finalized as `responded`; one with neither gets one response before the
phase is finalized. Response publication and phase persistence failures leave
the record `committed` for a later retry, while the task remains tracked and
is never spawned again. A responded task is retained without recreating a
response already consumed by the client. This ordering makes the task record
and its response boundary recoverable without replaying a spawned command.

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
`service teardown` reap every task owned by that session: tasks with a
corroborated live identity have their process group stopped on Unix or Job
Object terminated on Windows and their records removed, initially unresolved
tasks are retained for a later cleanup attempt, and terminal task records are
removed without signalling their recorded PID.
This applies even if a task's `--callback-inbox` points somewhere entirely
outside the owning session's own mailbox. Task admission and those cleanup
paths share the short-lived `service.admission.lock`, so cleanup cannot leave a
task admitted after its owner record has been removed. The callback is a
delivery target only.

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

## macOS launchd setup

`packaging/launchd/baton.plist` is an example per-user LaunchAgent. It uses
`KeepAlive` with `SuccessfulExit=false`, so a crash restarts `service run` but
the clean exit caused by `baton service teardown` does not fight the stop path.
The example contains `/Users/YOUR_USER` placeholders because LaunchAgent
`ProgramArguments` entries do not expand `~` or `$HOME`.

Install it for the current login session:

```bash
AGENT_LABEL=com.shukebeta.baton.service
AGENT_PATH="$HOME/Library/LaunchAgents/${AGENT_LABEL}.plist"
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/.baton"
cp packaging/launchd/baton.plist "$AGENT_PATH"
# Replace every /Users/YOUR_USER placeholder in "$AGENT_PATH" first.
plutil -lint "$AGENT_PATH"
launchctl bootstrap "gui/$(id -u)" "$AGENT_PATH"
launchctl print "gui/$(id -u)/$AGENT_LABEL"
```

Check the loaded job with `launchctl print`. For a clean stop, tear down
managed sessions before unloading the LaunchAgent:

```bash
AGENT_LABEL=com.shukebeta.baton.service
baton service teardown --control "$HOME/.baton/service"
launchctl bootout "gui/$(id -u)/$AGENT_LABEL"
```

`launchctl bootout` alone is not the clean-stop operation: managed sessions
are separate process-group leaders and can outlive the LaunchAgent. The
explicit `service teardown` first stops those sessions and the supervisor;
`bootout` then removes the stopped job. To uninstall the integration after the
clean stop, remove the per-user plist:

```bash
rm -f "$HOME/Library/LaunchAgents/com.shukebeta.baton.service.plist"
```

This LaunchAgent is login-scoped and is intentionally not a root-owned
LaunchDaemon; it does not promise survival across logout.
