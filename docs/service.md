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
