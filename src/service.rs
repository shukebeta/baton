//! `baton service`: a host-owned supervisor for `baton serve` sessions.
//!
//! `baton serve --agent-cmd` is already a resident, single-instance-locked
//! mailbox daemon (see [`crate::mailbox`]), but nothing durable *owns* it: an
//! integration that launches it directly inherits the daemon as a child of its
//! own process tree, and `setsid`/`disown` only detach a process group — an
//! external agent/tool runner that reaps that tree takes the daemon with it.
//! `baton service run --control <dir>` is the missing owner: a long-lived
//! foreground process (meant to be kept alive by an OS service manager, e.g.
//! the systemd user-service unit under `packaging/systemd/`) that spawns each
//! `baton serve` session as its own direct child, detached into its own
//! process group, and tracks it durably so a short-lived client can start,
//! inspect, stop, or tear one down without ever sharing a process tree with it.
//!
//! ## Control surface
//!
//! `--control <dir>` holds:
//! - `service.lock` — the exclusive single-instance advisory lock, taken by
//!   [`ServiceCommand::Run`] for as long as it runs. Mirrors
//!   [`mailbox::Mailbox`]'s `serve.lock`.
//! - `service.admission.lock` — a short-lived advisory lock shared by task
//!   admission and session cleanup. It is separate from `service.lock`,
//!   which the long-lived `Run` process holds for its entire lifetime.
//! - `service.probe.lock` — a short-lived advisory lock serializing
//!   `service.lock`'s acquisition against the liveness probes behind
//!   `Status`/`Stop`/`Teardown`/`Start`. A probe answers "not running" by
//!   *taking* `service.lock`, so without this a probe landing on a starting
//!   `Run` would refuse it as a duplicate instance.
//! - `service.stop` — the cooperative-stop sentinel `Teardown` drops for a live
//!   `Run` to observe between polls. Mirrors `serve.stop`.
//! - `requests/` / `processing/` / `responses/` — the atomic-rename request
//!   protocol `Start` uses to reach the live `Run` loop (the only operation
//!   that must run *in* the long-lived process, since spawning there is the
//!   entire point). A session-spec request is delivered into `requests/`,
//!   claimed into `processing/` by `Run`, and answered into `responses/` keyed
//!   by the request id — the same temp-file-then-`rename` idiom as
//!   [`mailbox::deliver_to`]/[`mailbox::atomic_write`], reused directly.
//! - `task-start-ack/` — durable markers written by a task-start client after
//!   it claims a response, allowing restart reconciliation to distinguish a
//!   consumed response from one that was never published.
//! - `sessions/<id>.json` — one durable [`SessionRecord`] per session, holding
//!   its effective spec, real PID, and (on Unix) its stderr log path. The
//!   corresponding `sessions/<id>/stderr.log` keeps daemon warnings durable.
//!   `Status`/`Stop`/`Teardown` read this directly and act on the OS process by
//!   PID; none of them need the `Run` loop to be alive, so a session started by
//!   a since-crashed `Run` can still be inspected, stopped, or torn down.
//!
//! ## Ownership boundary
//!
//! A spawned `baton serve` child is never `wait()`-ed by its short-lived
//! submitter — only `Run` holds the [`std::process::Child`] handle, reaping it
//! (via non-blocking [`std::process::Child::try_wait`]) as its loop ticks. On
//! Unix, cleanup targets the child's process group. On Windows, each session
//! and task has a Job Object retained until its active-process count reaches
//! zero, so an exited `serve` or task parent does not release ownership of a
//! surviving descendant.
//!
//! ## Host support
//!
//! Linux and macOS use process groups, `/proc`/`ps` corroboration, and Unix
//! signal escalation. Windows uses `windows-sys` Job Objects and
//! `GetProcessTimes` in the separate `cfg(windows)` implementation. The
//! systemd user-service integration (`packaging/systemd/`) and macOS
//! LaunchAgent integration (`packaging/launchd/`) are external to this binary;
//! choosing a Windows host-service manager remains a packaging concern.

use std::io::Write;
#[cfg(any(unix, windows))]
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BatonError, Result};
use crate::task::TaskCommand;

/// Schema tag for a [`SessionSpec`] request, stamped for forward-compatible
/// parsing (unchecked today — there is only one version).
pub const SESSION_SPEC_SCHEMA: &str = "baton.session-spec/v1";

/// A versioned specification for one `baton serve` session, submitted to
/// `baton service start`. Mirrors `baton serve`'s own flags exactly — see
/// [`crate::cli`]'s `Command::Serve` — since `Run` reconstructs an equivalent
/// `baton serve` argv from it (see `imp::serve_argv`) rather than translating
/// through a second flag surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSpec {
    /// [`SESSION_SPEC_SCHEMA`].
    pub schema: String,
    /// `baton serve --inbox`.
    pub inbox: String,
    /// `baton serve --outbox`.
    pub outbox: String,
    /// `baton serve --poll-ms`; `None` ⇒ the child's own default.
    pub poll_ms: Option<u64>,
    /// `baton serve --agent-cmd`; `None` ⇒ the in-process participant.
    pub agent_cmd: Option<String>,
    /// `baton serve --agent-arg` (repeatable).
    #[serde(default)]
    pub agent_args: Vec<String>,
    /// `baton serve --agent-cwd`.
    pub agent_cwd: Option<String>,
    /// `baton serve --agent-timeout-ms`.
    pub agent_timeout_ms: Option<u64>,
    /// `baton serve --agent-output`.
    pub agent_output: Option<String>,
    /// `baton serve --agent-result-key`.
    pub agent_result_key: Option<String>,
    /// `baton serve --role`.
    pub role: Option<String>,
}

/// A parsed `baton service` invocation.
#[derive(Debug, PartialEq, Eq)]
pub enum ServiceCommand {
    /// Run the long-lived supervisor loop, holding the control lock until a
    /// cooperative stop (see `Teardown`).
    Run {
        /// The `--control <dir>` root.
        control: String,
    },
    /// Submit a session spec to a live `Run` and return its session id.
    Start {
        /// The `--control <dir>` root.
        control: String,
        /// The session to start. Boxed: `SessionSpec` is by far the largest
        /// field of any `ServiceCommand` variant, and boxing it keeps the
        /// enum itself small regardless of how many optional agent flags it
        /// carries.
        spec: Box<SessionSpec>,
    },
    /// Report the service's own liveness plus every managed session's (or
    /// just `session`'s, when given).
    Status {
        /// The `--control <dir>` root.
        control: String,
        /// `--session <id>`; `None` reports every known session.
        session: Option<String>,
    },
    /// Stop one session: cooperative `serve --stop` first, then a bounded
    /// process-group escalation. Idempotent. `force` permits cleanup when
    /// process identity cannot be corroborated.
    Stop {
        /// The `--control <dir>` root.
        control: String,
        /// The session id to stop.
        session: String,
        /// Signal and remove a record whose identity is unresolved.
        force: bool,
    },
    /// Stop every managed session, then request `Run`'s own cooperative stop.
    /// Idempotent, and independent of whether `Run` is currently alive.
    Teardown {
        /// The `--control <dir>` root.
        control: String,
        /// Signal and remove records whose identities are unresolved.
        force: bool,
    },
}

/// Runs `cmd` to completion, writing any human-readable output to `out`.
#[cfg(any(unix, windows))]
pub fn execute_service(cmd: ServiceCommand, out: impl Write) -> Result<()> {
    imp::dispatch(cmd, out)
}

/// `baton service` has no supported implementation on this host: process
/// ownership and the platform-specific escalation this module relies on have
/// no implementation on this host. Fails clearly rather than silently
/// degrading the ownership guarantee.
#[cfg(not(any(unix, windows)))]
pub fn execute_service(cmd: ServiceCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton service requires a supported host (Linux, macOS, or Windows)".to_string(),
    ))
}

/// Runs `cmd` to completion, writing any human-readable output to `out`.
///
/// A `baton task` is owned and reaped by the same control-plane `Run` loop as
/// a `baton service` session (see the module doc) — `Start` reaches it
/// through the identical atomic-rename request protocol, while `Status` and
/// `Cancel` act directly on the durable [`crate::task::TaskRecord`], just as
/// `Status`/`Stop` do for a [`SessionRecord`](imp), so both keep working even
/// when `Run` itself is not currently alive.
#[cfg(any(unix, windows))]
pub fn execute_task(cmd: TaskCommand, out: impl Write) -> Result<()> {
    imp::dispatch_task(cmd, out)
}

/// `baton task` has no supported implementation on this host; see
/// [`execute_service`]'s non-Unix stub for why.
#[cfg(not(any(unix, windows)))]
pub fn execute_task(cmd: TaskCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton task requires a supported host (Linux, macOS, or Windows)".to_string(),
    ))
}

#[cfg(unix)]
mod imp {
    use super::*;
    #[cfg(all(test, not(target_os = "linux")))]
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::fs::{self, File, OpenOptions, TryLockError};
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use crate::mailbox;
    use crate::message::{MessageEnvelope, MessageKind};
    use crate::task::{
        Clock, SystemClock, TaskAdmissionPhase, TaskEventBody, TaskEventKind, TaskRecord, TaskSpec,
        TaskState, max_duration_exceeded, milestones_due, task_event_id,
    };
    #[cfg(test)]
    use crate::task::{FakeClock, TaskCallback};

    /// Name of the control-plane lockfile at the control root, mirroring
    /// [`mailbox`]'s `serve.lock`.
    const CONTROL_LOCK_FILE: &str = "service.lock";
    /// Short-lived lock serializing task admission with session cleanup.
    /// Separate from [`CONTROL_LOCK_FILE`], which `Run` holds for its whole
    /// lifetime and therefore cannot be used by `service stop`.
    const ADMISSION_LOCK_FILE: &str = "service.admission.lock";
    /// Serializes taking the control lock against probing it.
    ///
    /// A probe can only answer "is a `Run` live?" by *trying to take*
    /// [`CONTROL_LOCK_FILE`] — so while it holds that lock it is
    /// indistinguishable from a live supervisor. Without this guard, a
    /// client polling `service status` during a supervisor's startup makes
    /// [`acquire_control_lock`]'s single non-blocking attempt fail with a
    /// spurious "another baton service already holds the control lock", and
    /// the supervisor exits. Held only across the control lock's
    /// open-and-try — never while a session or task is spawned — so blocking
    /// on it is bounded by a handful of syscalls.
    const CONTROL_PROBE_LOCK_FILE: &str = "service.probe.lock";
    /// Name of the cooperative-stop sentinel at the control root, mirroring
    /// [`mailbox`]'s `serve.stop`.
    const CONTROL_STOP_FILE: &str = "service.stop";
    /// Interval between `Run`'s request-directory scans, and between polls of
    /// a pending await (start response, stop/kill grace).
    const POLL_INTERVAL_MS: u64 = 100;
    /// Minimum interval between liveness probes for one rehydrated task. The
    /// supervisor still ticks every 100 ms, but a non-Linux `ps` probe is
    /// allowed at most twice per second in the steady state.
    #[cfg(not(target_os = "linux"))]
    const REHYDRATED_LIVENESS_CACHE_MS: u64 = 500;
    /// Initial delay before retrying a failed terminal callback delivery.
    const TERMINAL_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
    /// Longest delay between terminal callback delivery attempts.
    const TERMINAL_RETRY_MAX_DELAY_MS: u64 = 60_000;
    /// Total terminal callback delivery attempts before the task is dropped
    /// from the in-memory tracker.
    const MAX_TERMINAL_DELIVERY_ATTEMPTS: u32 = 10;
    /// Bound on how long `Start` waits for a live `Run` to answer.
    const START_AWAIT_MS: u64 = 10_000;
    /// Bound on the cooperative `serve --stop` grace before escalating to
    /// `SIGTERM`.
    const STOP_GRACE_MS: u64 = 5_000;
    /// Bound on the `SIGTERM`/`SIGKILL` escalation grace.
    const KILL_GRACE_MS: u64 = 2_000;

    /// Process-local sequence, making request/session ids unique even across
    /// several calls within the same millisecond.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    #[cfg(all(test, not(target_os = "linux")))]
    thread_local! {
        static PROCESS_PROBE_COUNT: Cell<u64> = const { Cell::new(0) };
    }

    #[cfg(all(test, not(target_os = "linux")))]
    fn note_process_probe() {
        PROCESS_PROBE_COUNT.with(|count| count.set(count.get() + 1));
    }

    #[cfg(all(test, not(target_os = "linux")))]
    fn reset_process_probe_count() {
        PROCESS_PROBE_COUNT.with(|count| count.set(0));
    }

    #[cfg(all(test, not(target_os = "linux")))]
    fn process_probe_count() -> u64 {
        PROCESS_PROBE_COUNT.with(Cell::get)
    }

    /// Opens any service lock with close-on-exec set. `Run` holds the control
    /// lock while it spawns sessions and tasks; descendants must not retain
    /// that lock after the supervisor is killed, or a restart is blocked by
    /// the descendant's lifetime.
    fn service_lock_options() -> OpenOptions {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_CLOEXEC);
        options
    }

    /// A durable on-disk record of one session `Run` has spawned: enough to
    /// find, corroborate, and signal the real OS process from any later,
    /// independent `Status`/`Stop`/`Teardown` invocation.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SessionRecord {
        id: String,
        spec: SessionSpec,
        pid: u32,
        /// Linux `/proc/<pid>/stat` starttime or non-Linux Unix `ps` `lstart`,
        /// corroborating `pid` against reuse after a `Run` restart; `None`
        /// where the platform probe could not be read or for a legacy record
        /// that must use the platform fallback ladder.
        started_at: Option<String>,
        /// Canonical Unix epoch seconds parsed from macOS `ps lstart`; its
        /// presence marks a post-upgrade record that can use the epoch fast
        /// path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_epoch_secs: Option<i64>,
        /// Path to the session daemon's durable stderr log. Empty for a
        /// legacy record written before session stderr capture was added.
        #[serde(default)]
        stderr_path: String,
    }

    /// The `Start` response body, keyed by request id in `responses/`.
    /// An admitted request carries `session_id`; an admission failure the
    /// supervisor can name carries `error` and no session id. Both fields
    /// default, so a response persisted before `error` existed still parses.
    #[derive(Debug, Serialize, Deserialize)]
    struct StartResponse {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }

    /// Whether a live `Run` holds the control lock.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ControlLiveness {
        Live,
        NotRunning,
    }

    /// Result of corroborating a durable PID against the process currently
    /// occupying it. `Unresolved` is deliberately distinct from `Dead`: the
    /// former means the PID exists but baton cannot prove its identity, so it
    /// is never signalled or removed without an explicit operator override.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    enum Liveness {
        Live,
        Dead,
        Unresolved,
    }

    impl Liveness {
        fn is_live(self) -> bool {
            self == Self::Live
        }
    }

    /// A process probe can positively report absence, fail to read the
    /// process, or return a sample. The distinction is the safety boundary of
    /// the liveness ladder.
    #[derive(Debug, PartialEq, Eq)]
    enum ProbeResult<T> {
        Gone,
        Unreadable,
        Present(T),
    }

    /// Dispatches one parsed [`ServiceCommand`].
    pub(super) fn dispatch(cmd: ServiceCommand, mut out: impl Write) -> Result<()> {
        match cmd {
            ServiceCommand::Run { control } => run_service(Path::new(&control), out),
            ServiceCommand::Start { control, spec } => {
                let session_id = submit_start_request(Path::new(&control), &spec)?;
                writeln!(out, "{session_id}").map_err(io_err)
            }
            ServiceCommand::Status { control, session } => {
                execute_status(Path::new(&control), session.as_deref(), out)
            }
            ServiceCommand::Stop {
                control,
                session,
                force,
            } => execute_stop(Path::new(&control), &session, force, out),
            ServiceCommand::Teardown { control, force } => {
                execute_teardown(Path::new(&control), force, out)
            }
        }
    }

    /// Dispatches one parsed [`TaskCommand`].
    pub(super) fn dispatch_task(cmd: TaskCommand, mut out: impl Write) -> Result<()> {
        match cmd {
            TaskCommand::Start { control, spec } => {
                let task_id = submit_task_start_request(Path::new(&control), &spec)?;
                writeln!(out, "{task_id}").map_err(io_err)
            }
            TaskCommand::Status { control, task } => {
                execute_task_status(Path::new(&control), task.as_deref(), out)
            }
            TaskCommand::Cancel { control, task } => {
                execute_task_cancel(Path::new(&control), &task, out)
            }
        }
    }

    /// Maps an [`std::io::Error`] encountered writing to `out` to a
    /// [`BatonError::Io`].
    fn io_err(err: std::io::Error) -> BatonError {
        BatonError::Io(format!("could not write service output: {err}"))
    }

    // -- Run ------------------------------------------------------------

    /// Runs the supervisor loop: holds the control lock, drains `requests/`
    /// into spawned sessions, reaps exited children, and exits cooperatively
    /// once `Teardown` drops the stop sentinel.
    fn run_service(control: &Path, mut out: impl Write) -> Result<()> {
        fs::create_dir_all(control).map_err(|err| {
            BatonError::Io(format!(
                "could not create control directory {control:?}: {err}"
            ))
        })?;
        let lock = acquire_control_lock(control)?;
        // Discard any stale sentinel a prior instance left, so a fresh start
        // is never killed by a stop meant for an earlier run.
        let _ = fs::remove_file(control.join(CONTROL_STOP_FILE));
        // Reconcile task admissions while the startup instance is the only
        // possible request processor. The short-lived lock also serializes
        // this pass with a submitting client writing a rollback marker after
        // observing the previous supervisor disappear.
        let _admission = acquire_admission_lock(control)?;
        reconcile_task_admissions(control)?;
        // A request left mid-`processing/` by a crash between claim and
        // response is returned to `requests/`, mirroring
        // `Mailbox::reclaim_stale` — reprocessed harmlessly under a fresh
        // session id on this restart: the reprocessed spec spawns a *second*
        // `baton serve` on the same inbox/outbox, but `serve`'s own
        // single-instance mailbox lock refuses the duplicate immediately, so
        // it exits at once, leaving only a transient stale session record
        // behind (reaped the next time it's inspected).
        reclaim_stale_requests(control)?;
        reclaim_stale_task_requests(control)?;
        drop(_admission);
        let clock = SystemClock;
        let mut tasks = rehydrate_tasks(control, &clock)?;
        writeln!(out, "baton service running on {}", control.display()).map_err(io_err)?;

        let mut children: HashMap<String, Child> = HashMap::new();
        loop {
            // A failure to check the stop sentinel (as opposed to a clean
            // present/absent read) must not crash the loop either — the same
            // "one bad thing can't wedge the daemon" posture as the request
            // arms below.
            match consume_stop_sentinel(control) {
                Ok(true) => break,
                Ok(false) => {}
                Err(err) => {
                    eprintln!("warning: baton service failed to check its stop sentinel: {err}");
                }
            }
            reap_exited(&mut children);
            tick_tasks(control, &mut tasks, &clock);

            // One request's failure (a malformed spec, a transient spawn
            // error) must not crash the loop out from under every other
            // session/task this instance already owns — warn and keep
            // polling, the same "one bad message can't wedge the daemon"
            // posture `Mailbox::claim_next` takes for a malformed mailbox
            // entry.
            let mut did_work = false;
            match process_one_request(control) {
                Ok(Some((session_id, child))) => {
                    children.insert(session_id, child);
                    did_work = true;
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!(
                        "warning: baton service failed to process a session-start request: {err}"
                    );
                }
            }
            match process_one_task_request(control, &clock) {
                Ok(Some((task_id, running))) => {
                    tasks.insert(task_id, running);
                    did_work = true;
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!(
                        "warning: baton service failed to process a task-start request: {err}"
                    );
                }
            }
            if !did_work {
                std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
            }
        }

        // `service teardown` waits for this lock to be released before it
        // snapshots and stops session records. Waiting for children here would
        // delay that admission barrier while the sessions are still live; the
        // teardown client owns their PID-based drain after this lock is dropped.
        drop(lock);
        Ok(())
    }

    /// Removes every child whose exit status is already available, reaping
    /// it. A still-running child (`Ok(None)`) is left in place.
    fn reap_exited(children: &mut HashMap<String, Child>) {
        children.retain(|_, child| !matches!(child.try_wait(), Ok(Some(_)) | Err(_)));
    }

    /// Blocks until this process may touch `control`'s control lock, so an
    /// acquisition and a liveness probe never overlap. See
    /// [`CONTROL_PROBE_LOCK_FILE`].
    ///
    /// Always taken *before* the control lock and released as soon as that
    /// attempt is done, so it can never participate in a cycle with the
    /// control or admission locks.
    fn acquire_control_probe_guard(control: &Path) -> Result<File> {
        let lock_path = control.join(CONTROL_PROBE_LOCK_FILE);
        let lock = service_lock_options().open(&lock_path).map_err(|err| {
            BatonError::Io(format!(
                "could not open service probe guard {lock_path:?}: {err}"
            ))
        })?;
        lock.lock().map_err(|err| {
            BatonError::Io(format!(
                "could not lock service probe guard {control:?}: {err}"
            ))
        })?;
        Ok(lock)
    }

    /// Takes the exclusive control-plane lock, refusing a second live `Run`
    /// on the same `control`.
    ///
    /// A `WouldBlock` here means a *supervisor* holds the lock, never a
    /// passing probe: [`acquire_control_probe_guard`] keeps the two apart.
    fn acquire_control_lock(control: &Path) -> Result<File> {
        let _guard = acquire_control_probe_guard(control)?;
        let lock_path = control.join(CONTROL_LOCK_FILE);
        let lock = service_lock_options().open(&lock_path).map_err(|err| {
            BatonError::Io(format!("could not open service lock {lock_path:?}: {err}"))
        })?;
        match lock.try_lock() {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(BatonError::Io(format!(
                "another baton service already holds the control lock at {control:?}"
            ))),
            Err(TryLockError::Error(err)) => Err(BatonError::Io(format!(
                "could not lock service control {control:?}: {err}"
            ))),
        }
    }

    /// Takes the short-lived lock shared by task admission and session
    /// cleanup. This must remain distinct from [`acquire_control_lock`]: the
    /// long-lived `Run` process owns the control lock while `service stop`
    /// still needs to run concurrently with it.
    fn acquire_admission_lock(control: &Path) -> Result<File> {
        fs::create_dir_all(control).map_err(|err| {
            BatonError::Io(format!(
                "could not create service control directory {control:?}: {err}"
            ))
        })?;
        let lock_path = control.join(ADMISSION_LOCK_FILE);
        let lock = service_lock_options().open(&lock_path).map_err(|err| {
            BatonError::Io(format!(
                "could not open service admission lock {lock_path:?}: {err}"
            ))
        })?;
        lock.lock().map_err(|err| {
            BatonError::Io(format!(
                "could not lock service admission {control:?}: {err}"
            ))
        })?;
        Ok(lock)
    }

    /// Owns the admission lock for a cleanup pass, and can lend it back for
    /// the duration of a wall-clock grace wait.
    ///
    /// The discipline is: every record read, signal, and record mutation runs
    /// under the lock; only the sleeps of [`wait_while_alive`] /
    /// [`wait_while_task_alive`] run outside it, and liveness is re-probed
    /// after each re-acquisition. Holding the lock across those waits froze
    /// the supervisor's request admission and every task-start client for up
    /// to the sum of the grace windows.
    ///
    /// Releasing it mid-cleanup is safe because of two facts:
    ///
    /// 1. Task admission is gated on a *live* owner, and the whole request —
    ///    owner check, spawn, record write — runs inside one hold of this
    ///    lock. So while we hold it and observe the session `Dead`, no
    ///    admission can be part-way through: a racer either finished its
    ///    record write before we took the lock, or runs its owner check after
    ///    we release and is rejected. `Dead` is stable, since the record pins
    ///    a start identity rather than a bare PID.
    /// 2. The session escalation ladder runs *before* task reaping, so on the
    ///    success path every task wait happens after the session was observed
    ///    `Dead` — a state in which nothing new can be admitted for it.
    ///
    /// A racing record can therefore only appear on a path that already
    /// fails, and [`rescan_owned_tasks`] accounts for it before the session
    /// record is removed.
    struct AdmissionGuard<'a> {
        control: &'a Path,
        /// `Some` whenever the lock is held; `None` only inside
        /// [`AdmissionGuard::unlocked_wait`]. Dropping the `File` unlocks.
        lock: Option<File>,
    }

    impl<'a> AdmissionGuard<'a> {
        fn acquire(control: &'a Path) -> Result<Self> {
            Ok(Self {
                control,
                lock: Some(acquire_admission_lock(control)?),
            })
        }

        fn control(&self) -> &'a Path {
            self.control
        }

        /// Runs `wait` with the admission lock released, then re-acquires it
        /// before returning. Callers must re-probe any liveness they decided
        /// on before the wait.
        fn unlocked_wait<T>(&mut self, wait: impl FnOnce() -> T) -> Result<T> {
            self.lock = None;
            let value = wait();
            self.lock = Some(acquire_admission_lock(self.control)?);
            Ok(value)
        }
    }

    /// The durable "a stop owns this session" marker, written under the
    /// admission lock before [`stop_session_record_with_wait`] first releases
    /// it and removed when that stop finishes.
    ///
    /// It exists because releasing the lock across a grace wait leaves a
    /// window in which the owner still probes `Live`. The cooperative
    /// `serve.stop` sentinel cannot serve here: `poll_stop` consumes it as
    /// soon as the daemon observes it, well before the process exits, so a
    /// start landing in between would see neither a sentinel nor a dead
    /// owner. This marker spans the whole cleanup instead.
    ///
    /// It records the stopping process's own identity so a marker orphaned by
    /// a killed `service stop` cannot wedge admission forever: a reader whose
    /// identity no longer matches treats it as stale and removes it.
    #[derive(Serialize, Deserialize)]
    struct SessionStopMarker {
        pid: u32,
        #[serde(default)]
        started_at: Option<String>,
        #[serde(default)]
        start_epoch_secs: Option<i64>,
    }

    fn session_stop_markers_dir(control: &Path) -> std::path::PathBuf {
        control.join("session-stopping")
    }

    fn session_stop_marker_path(control: &Path, id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(id) {
            return Err(BatonError::Io(format!(
                "session id is not usable as a filename: {id:?}"
            )));
        }
        Ok(session_stop_markers_dir(control).join(mailbox::file_name(id)))
    }

    /// Whether some live `service stop`/`teardown` currently owns `id`'s
    /// cleanup. Removes a stale marker as a side effect, so one orphaned by a
    /// killed stop costs at most one rejected start.
    fn session_stop_in_progress(control: &Path, id: &str) -> Result<bool> {
        let path = session_stop_marker_path(control, id)?;
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
        };
        // A malformed marker is not evidence of a live stop, and leaving it
        // would wedge admission for good.
        let Ok(marker) = serde_json::from_str::<SessionStopMarker>(&data) else {
            let _ = fs::remove_file(&path);
            return Ok(false);
        };
        let (started_at, start_epoch_secs) = recorded_start_identity(marker.pid);
        if started_at == marker.started_at && start_epoch_secs == marker.start_epoch_secs {
            return Ok(true);
        }
        let _ = fs::remove_file(&path);
        Ok(false)
    }

    /// Owns a [`SessionStopMarker`] for the length of one session's cleanup,
    /// removing it on every exit path including an early `?`.
    struct SessionStopGuard {
        path: std::path::PathBuf,
        pid: u32,
    }

    impl SessionStopGuard {
        fn claim(control: &Path, id: &str) -> Result<Self> {
            let pid = std::process::id();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            let marker = SessionStopMarker {
                pid,
                started_at,
                start_epoch_secs,
            };
            let json = serde_json::to_string(&marker).map_err(|err| {
                BatonError::Io(format!("could not serialize session stop marker: {err}"))
            })?;
            let dir = session_stop_markers_dir(control);
            fs::create_dir_all(&dir)
                .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
            mailbox::atomic_write(&dir, &mailbox::file_name(id), &json)?;
            Ok(Self {
                path: session_stop_marker_path(control, id)?,
                pid,
            })
        }
    }

    impl Drop for SessionStopGuard {
        fn drop(&mut self) {
            // Only clear our own claim: a second stop of the same session
            // overwrites the marker, and it still needs it after we finish.
            let Ok(data) = fs::read_to_string(&self.path) else {
                return;
            };
            if serde_json::from_str::<SessionStopMarker>(&data)
                .is_ok_and(|marker| marker.pid == self.pid)
            {
                let _ = fs::remove_file(&self.path);
            }
        }
    }

    /// Checks for and consumes the cooperative-stop sentinel in one atomic
    /// step. Mirrors [`mailbox::Mailbox::poll_stop`] exactly.
    fn consume_stop_sentinel(control: &Path) -> Result<bool> {
        match fs::remove_file(control.join(CONTROL_STOP_FILE)) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(BatonError::Io(format!(
                "could not consume service stop sentinel: {err}"
            ))),
        }
    }

    /// Probes whether a live `Run` holds `control`'s lock, leaving the
    /// control plane untouched (`signal = false`) or, when `signal`,
    /// dropping the stop sentinel for it to observe. Mirrors
    /// [`mailbox::request_stop`].
    ///
    /// The probe is a read only in its *result*: answering "not running"
    /// requires actually taking the exclusive lock. That hold is invisible
    /// to callers but not to a supervisor starting at the same instant, so
    /// the whole attempt runs under
    /// [`acquire_control_probe_guard`] — see [`CONTROL_PROBE_LOCK_FILE`].
    fn probe_or_signal_control(control: &Path, signal: bool) -> Result<ControlLiveness> {
        let lock_path = control.join(CONTROL_LOCK_FILE);
        let lock = match service_lock_options().open(&lock_path) {
            Ok(lock) => lock,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ControlLiveness::NotRunning);
            }
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not open service lock {lock_path:?}: {err}"
                )));
            }
        };
        let _guard = acquire_control_probe_guard(control)?;
        let outcome = lock.try_lock();
        // Release the probe's own hold *before* the guard is dropped at the
        // end of this function, so no acquirer can ever observe it.
        drop(lock);
        match outcome {
            Ok(()) => Ok(ControlLiveness::NotRunning),
            Err(TryLockError::WouldBlock) => {
                if signal {
                    mailbox::atomic_write(control, CONTROL_STOP_FILE, "")?;
                }
                Ok(ControlLiveness::Live)
            }
            Err(TryLockError::Error(err)) => Err(BatonError::Io(format!(
                "could not probe service lock {control:?}: {err}"
            ))),
        }
    }

    fn probe_control(control: &Path) -> Result<ControlLiveness> {
        probe_or_signal_control(control, false)
    }

    /// Requests a cooperative stop of a live `Run`; a no-op success when none
    /// is running (idempotent, like [`mailbox::request_stop`]).
    fn request_control_stop(control: &Path) -> Result<ControlLiveness> {
        probe_or_signal_control(control, true)
    }

    // -- Start / control-plane request protocol --------------------------

    fn requests_dir(control: &Path) -> std::path::PathBuf {
        control.join("requests")
    }

    fn processing_dir(control: &Path) -> std::path::PathBuf {
        control.join("processing")
    }

    fn responses_dir(control: &Path) -> std::path::PathBuf {
        control.join("responses")
    }

    fn fresh_request_id() -> String {
        format!(
            "req-{}-{}-{}",
            std::process::id(),
            crate::events::now_ms(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn fresh_session_id() -> String {
        format!(
            "svc-{}-{}-{}",
            std::process::id(),
            crate::events::now_ms(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Submits `spec` to a live `Run` and awaits its session id.
    ///
    /// Fails fast (before writing anything) when no `Run` holds the control
    /// lock, rather than waiting out the full await bound against a service
    /// that was never started.
    fn submit_start_request(control: &Path, spec: &SessionSpec) -> Result<String> {
        if probe_control(control)? == ControlLiveness::NotRunning {
            return Err(BatonError::Io(format!(
                "no live baton service on {control:?}; start one with `baton service run --control <dir>` first"
            )));
        }
        let request_id = fresh_request_id();
        let json = serde_json::to_string(spec)
            .map_err(|err| BatonError::Io(format!("could not serialize session spec: {err}")))?;
        mailbox::atomic_write(
            &requests_dir(control),
            &mailbox::file_name(&request_id),
            &json,
        )?;
        await_start_response(control, &request_id)
    }

    fn await_start_response(control: &Path, request_id: &str) -> Result<String> {
        let path = responses_dir(control).join(mailbox::file_name(request_id));
        let deadline = Instant::now() + Duration::from_millis(START_AWAIT_MS);
        loop {
            if let Ok(data) = fs::read_to_string(&path) {
                let _ = fs::remove_file(&path);
                let resp: StartResponse = serde_json::from_str(&data).map_err(|err| {
                    BatonError::Decode(format!("malformed service response {path:?}: {err}"))
                })?;
                if let Some(error) = resp.error {
                    return Err(BatonError::Io(error));
                }
                return resp.session_id.ok_or_else(|| {
                    BatonError::Decode(format!(
                        "service response {path:?} contained neither a session id nor an error"
                    ))
                });
            }
            if probe_control(control)? == ControlLiveness::NotRunning {
                return Err(BatonError::Io(format!(
                    "no live baton service on {control:?}; start request was not admitted"
                )));
            }
            if Instant::now() >= deadline {
                return Err(BatonError::Io(format!(
                    "timed out waiting for baton service to start the session ({request_id})"
                )));
            }
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Returns any `processing/` entry a crash left mid-request to
    /// `requests/`, safe only because the caller already holds the control
    /// lock (mirrors [`mailbox::Mailbox::reclaim_stale`]).
    fn reclaim_stale_requests(control: &Path) -> Result<()> {
        let processing = processing_dir(control);
        let entries = match fs::read_dir(&processing) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {processing:?}: {err}"
                )));
            }
        };
        let requests = requests_dir(control);
        fs::create_dir_all(&requests)
            .map_err(|err| BatonError::Io(format!("could not create {requests:?}: {err}")))?;
        for entry in entries {
            let path = mailbox::dir_entry(entry, &processing)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            let dest = requests.join(mailbox::file_name(&key));
            fs::rename(&path, &dest)
                .map_err(|err| BatonError::Io(format!("could not reclaim {path:?}: {err}")))?;
        }
        Ok(())
    }

    /// Claims and handles the next pending start request, if any.
    fn process_one_request(control: &Path) -> Result<Option<(String, Child)>> {
        let dir = requests_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(BatonError::Io(format!("could not read {dir:?}: {err}"))),
        };
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            let processing = processing_dir(control);
            fs::create_dir_all(&processing)
                .map_err(|err| BatonError::Io(format!("could not create {processing:?}: {err}")))?;
            let claimed_path = processing.join(mailbox::file_name(&key));
            match fs::rename(&path, &claimed_path) {
                Ok(()) => {
                    let outcome = handle_start_request(control, &key, &claimed_path);
                    let _ = fs::remove_file(&claimed_path);
                    let Some((record, child)) = outcome? else {
                        return Ok(None);
                    };
                    return Ok(Some((record.id, child)));
                }
                // Lost a claim race (shouldn't happen — `Run` is the sole
                // reader — but harmless if it ever did): move on.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(BatonError::Io(format!("could not claim {path:?}: {err}"))),
            }
        }
        Ok(None)
    }

    /// Renders `err` for a start-response `error` field. The client re-wraps
    /// that text in [`BatonError::Io`], so the rendered form must not repeat
    /// the kind prefix `Display` already adds.
    fn admission_error_text(err: &BatonError) -> String {
        match err {
            BatonError::Io(msg) => msg.clone(),
            other => other.to_string(),
        }
    }

    /// Writes a start response body keyed by `request_id`.
    fn write_start_response(
        control: &Path,
        request_id: &str,
        response: &StartResponse,
    ) -> Result<()> {
        let json = serde_json::to_string(response)
            .map_err(|err| BatonError::Io(format!("could not serialize start response: {err}")))?;
        let responses = responses_dir(control);
        fs::create_dir_all(&responses)
            .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
        mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
    }

    /// Answers a claimed start request with an admission failure the
    /// supervisor can name, so the client fails immediately with the real
    /// reason instead of waiting out [`START_AWAIT_MS`]. Only the response
    /// write itself can still fail the request loop.
    fn reject_start_request(
        control: &Path,
        request_id: &str,
        error: String,
    ) -> Result<Option<(SessionRecord, Child)>> {
        write_start_response(
            control,
            request_id,
            &StartResponse {
                session_id: None,
                error: Some(error),
            },
        )?;
        Ok(None)
    }

    /// Spawns the requested session, persists its [`SessionRecord`], and
    /// answers the request with its session id.
    ///
    /// An admission failure after the request is claimed — a spawn failure, a
    /// post-spawn corroboration failure, a record-write failure — is answered
    /// as an error response and reported as `Ok(None)`; only a failure to
    /// deliver a response at all is propagated as `Err`.
    fn handle_start_request(
        control: &Path,
        request_id: &str,
        spec_path: &Path,
    ) -> Result<Option<(SessionRecord, Child)>> {
        let data = fs::read_to_string(spec_path)
            .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
        let spec: SessionSpec = serde_json::from_str(&data).map_err(|err| {
            BatonError::Decode(format!("malformed session spec {spec_path:?}: {err}"))
        })?;
        let session_id = fresh_session_id();
        let log_dir = session_logs_dir(control, &session_id);
        if let Err(err) = fs::create_dir_all(&log_dir) {
            return reject_start_request(
                control,
                request_id,
                format!("could not create {log_dir:?}: {err}"),
            );
        }
        let stderr_path = log_dir.join("stderr.log");
        let mut child = match spawn_serve_child(&spec, &stderr_path) {
            Ok(child) => child,
            Err(err) => {
                let _ = fs::remove_dir_all(&log_dir);
                return reject_start_request(control, request_id, admission_error_text(&err));
            }
        };
        let pid = child.id();
        let (started_at, start_epoch_secs) = recorded_start_identity(pid);
        // Everything below this point must kill+reap `child` before
        // returning: once this function stops tracking it, nothing else ever
        // does (it isn't inserted into `Run`'s `children` map, and `Drop` for
        // `std::process::Child` does not kill), so leaving it running here
        // would leak a live, unrecorded, unreapable `serve` process.
        if !spawn_start_key_ok(&started_at, &start_epoch_secs) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = fs::remove_dir_all(&log_dir);
            return reject_start_request(
                control,
                request_id,
                format!(
                    "baton serve (pid {pid}) could not be corroborated right after spawn; treating as a spawn failure"
                ),
            );
        }
        let record = SessionRecord {
            id: session_id,
            spec,
            pid,
            started_at,
            start_epoch_secs,
            stderr_path: stderr_path.display().to_string(),
        };
        if let Err(err) = write_session_record(control, &record) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = fs::remove_dir_all(&log_dir);
            return reject_start_request(control, request_id, admission_error_text(&err));
        }
        let respond = write_start_response(
            control,
            request_id,
            &StartResponse {
                session_id: Some(record.id.clone()),
                error: None,
            },
        );
        if let Err(err) = respond {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = remove_session_record(control, &record.id);
            let _ = fs::remove_dir_all(&log_dir);
            return Err(err);
        }
        Ok(Some((record, child)))
    }

    /// Resolves the currently-running `baton` binary, so `Run` spawns
    /// `serve` sessions via the same executable rather than trusting `PATH`.
    fn current_baton_exe() -> Result<std::path::PathBuf> {
        std::env::current_exe().map_err(|err| {
            BatonError::Io(format!(
                "could not resolve the running baton executable: {err}"
            ))
        })
    }

    /// Builds the `baton serve` argv equivalent to `spec`.
    fn serve_argv(spec: &SessionSpec) -> Vec<String> {
        let mut argv = vec![
            "serve".to_string(),
            "--inbox".to_string(),
            spec.inbox.clone(),
            "--outbox".to_string(),
            spec.outbox.clone(),
        ];
        if let Some(poll_ms) = spec.poll_ms {
            argv.push("--poll-ms".to_string());
            argv.push(poll_ms.to_string());
        }
        if let Some(agent_cmd) = &spec.agent_cmd {
            argv.push("--agent-cmd".to_string());
            argv.push(agent_cmd.clone());
            for arg in &spec.agent_args {
                argv.push("--agent-arg".to_string());
                argv.push(arg.clone());
            }
            if let Some(cwd) = &spec.agent_cwd {
                argv.push("--agent-cwd".to_string());
                argv.push(cwd.clone());
            }
            if let Some(timeout_ms) = spec.agent_timeout_ms {
                argv.push("--agent-timeout-ms".to_string());
                argv.push(timeout_ms.to_string());
            }
            if let Some(output) = &spec.agent_output {
                argv.push("--agent-output".to_string());
                argv.push(output.clone());
                if let Some(key) = &spec.agent_result_key {
                    argv.push("--agent-result-key".to_string());
                    argv.push(key.clone());
                }
            }
        }
        if let Some(role) = &spec.role {
            argv.push("--role".to_string());
            argv.push(role.clone());
        }
        argv
    }

    /// Spawns `baton serve` for `spec` as its own process-group leader
    /// (`pgid == pid`), detached from this process's stdio except for durable
    /// stderr capture, and returns the live [`Child`] without waiting on it —
    /// `Run`'s loop reaps it later.
    fn spawn_serve_child(spec: &SessionSpec, stderr_path: &Path) -> Result<Child> {
        let exe = current_baton_exe()?;
        let mut command = Command::new(&exe);
        command.args(serve_argv(spec));
        command.stdin(Stdio::null());
        command.stdout(Stdio::null());
        let stderr_file = File::create(stderr_path)
            .map_err(|err| BatonError::Io(format!("could not create {stderr_path:?}: {err}")))?;
        command.stderr(Stdio::from(stderr_file));
        // A fresh process group (not this service's own) so a later
        // `kill -- -<pid>` escalation reaches exactly this session's `serve`
        // process and its `agent-cmd` grandchild, nothing else this service
        // manages. Safe and stable — deliberately not `pre_exec(setsid)`,
        // which would require `unsafe`.
        command.process_group(0);
        command
            .spawn()
            .map_err(|err| BatonError::Io(format!("could not spawn baton serve: {err}")))
    }

    // -- Task control-plane request protocol -------------------------------
    //
    // A `baton task` reaches the live `Run` loop through the identical
    // atomic-rename request protocol as a session `Start` (its own
    // `task-requests/`/`task-processing/`/`task-responses/` directories, so
    // the two schemas — `SessionSpec` and `TaskSpec` — are never comingled in
    // the same request file). `Run`'s own tick is the sole writer of a
    // task's terminal state and the sole deliverer of its events; `Cancel`
    // (below, alongside `Status`) instead acts directly on the durable
    // `TaskRecord`'s PID, exactly like `Stop` does for a session, so both
    // keep working even when `Run` itself is not currently alive.

    fn task_requests_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-requests")
    }

    fn task_processing_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-processing")
    }

    fn task_responses_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-responses")
    }

    fn task_start_ack_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-start-ack")
    }

    fn tasks_dir(control: &Path) -> std::path::PathBuf {
        control.join("tasks")
    }

    fn task_logs_dir(control: &Path, task_id: &str) -> std::path::PathBuf {
        control.join("task-logs").join(task_id)
    }

    fn task_start_rollback_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-start-rollback")
    }

    fn task_cancel_dir(control: &Path) -> std::path::PathBuf {
        control.join("task-cancel")
    }

    fn fresh_task_id() -> String {
        format!(
            "task-{}-{}-{}",
            std::process::id(),
            crate::events::now_ms(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// The `Start` response body, keyed by request id in `task-responses/`.
    /// An admitted request carries `task_id`; an owner rejection carries
    /// `error` and no task id.
    #[derive(Debug, Serialize, Deserialize)]
    struct TaskStartResponse {
        #[serde(default)]
        task_id: Option<String>,
        #[serde(default)]
        error: Option<String>,
    }

    fn task_start_response_path(control: &Path, request_id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(request_id) {
            return Err(BatonError::Io(format!(
                "task start request id is not usable as a filename: {request_id:?}"
            )));
        }
        Ok(task_responses_dir(control).join(mailbox::file_name(request_id)))
    }

    fn task_start_response_claim_path(
        control: &Path,
        request_id: &str,
    ) -> Result<std::path::PathBuf> {
        let response = task_start_response_path(control, request_id)?;
        let file_name = response
            .file_name()
            .expect("task-start response path has a filename")
            .to_string_lossy();
        Ok(response.with_file_name(format!(".{file_name}.claimed")))
    }

    fn task_start_ack_path(control: &Path, request_id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(request_id) {
            return Err(BatonError::Io(format!(
                "task start request id is not usable as a filename: {request_id:?}"
            )));
        }
        Ok(task_start_ack_dir(control).join(mailbox::file_name(request_id)))
    }

    /// Submits `spec` to a live `Run` and awaits its task id. Mirrors
    /// [`submit_start_request`] exactly, against the task request
    /// directories instead.
    fn submit_task_start_request(control: &Path, spec: &TaskSpec) -> Result<String> {
        if probe_control(control)? == ControlLiveness::NotRunning {
            return Err(BatonError::Io(format!(
                "no live baton service on {control:?}; start one with `baton service run --control <dir>` first"
            )));
        }
        let request_id = fresh_request_id();
        let json = serde_json::to_string(spec)
            .map_err(|err| BatonError::Io(format!("could not serialize task spec: {err}")))?;
        mailbox::atomic_write(
            &task_requests_dir(control),
            &mailbox::file_name(&request_id),
            &json,
        )?;
        await_task_start_response(control, &request_id)
    }

    fn await_task_start_response(control: &Path, request_id: &str) -> Result<String> {
        let deadline = Instant::now() + Duration::from_millis(START_AWAIT_MS);
        loop {
            if let Some(response) = take_task_start_response(control, request_id)? {
                return task_start_response_id(control, request_id, response);
            }
            if probe_control(control)? == ControlLiveness::NotRunning {
                // The supervisor can write its response just before dropping
                // the control lock. Re-check the response after observing the
                // released lock so a successful admission wins this race. The
                // admission lock keeps a newly-started supervisor from
                // processing the request between this check and the rollback
                // marker.
                let _admission = acquire_admission_lock(control)?;
                if let Some(response) = take_task_start_response_locked(control, request_id)? {
                    return task_start_response_id(control, request_id, response);
                }
                if task_start_ack_exists(control, request_id)? {
                    return Err(BatonError::Io(format!(
                        "task start response for {request_id} was already consumed"
                    )));
                }
                mark_task_start_rollback(control, request_id)?;
                discard_pending_task_start_request(control, request_id)?;
                return Err(BatonError::Io(format!(
                    "no live baton service on {control:?}; task start request was not admitted"
                )));
            }
            if Instant::now() >= deadline {
                return Err(BatonError::Io(format!(
                    "timed out waiting for baton service to start the task ({request_id})"
                )));
            }
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Takes a task-start response if the supervisor has written one.
    ///
    /// The admission lock serializes the claim with response publication,
    /// phase persistence, and startup reconciliation. The acknowledgement is
    /// durable before the private claim is removed.
    fn take_task_start_response(
        control: &Path,
        request_id: &str,
    ) -> Result<Option<TaskStartResponse>> {
        if !task_start_response_boundary_exists(control, request_id)? {
            return Ok(None);
        }
        let _admission = acquire_admission_lock(control)?;
        take_task_start_response_locked(control, request_id)
    }

    /// Lock-free hint used by the polling client. The result can change
    /// immediately after this check; the admission lock is acquired before
    /// any claim, acknowledgement, or cleanup operation.
    fn task_start_response_boundary_exists(control: &Path, request_id: &str) -> Result<bool> {
        Ok(task_start_response_path(control, request_id)?.is_file()
            || task_start_response_claim_path(control, request_id)?.is_file()
            || task_start_ack_path(control, request_id)?.is_file())
    }

    fn take_task_start_response_locked(
        control: &Path,
        request_id: &str,
    ) -> Result<Option<TaskStartResponse>> {
        let response_path = task_start_response_path(control, request_id)?;
        let claim_path = task_start_response_claim_path(control, request_id)?;

        if task_start_ack_exists(control, request_id)? {
            remove_task_start_response_files(control, request_id)?;
            return Ok(None);
        }
        restore_task_start_response_claim(control, request_id)?;
        match fs::rename(&response_path, &claim_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not claim task response {response_path:?}: {err}"
                )));
            }
        }
        let data = match fs::read_to_string(&claim_path) {
            Ok(data) => data,
            Err(err) => {
                restore_task_start_response_claim(control, request_id)?;
                return Err(BatonError::Io(format!(
                    "could not read claimed task response {claim_path:?}: {err}"
                )));
            }
        };
        let response = match serde_json::from_str(&data) {
            Ok(response) => response,
            Err(err) => {
                restore_task_start_response_claim(control, request_id)?;
                return Err(BatonError::Decode(format!(
                    "malformed task response {response_path:?}: {err}"
                )));
            }
        };
        mark_task_start_ack(control, request_id)?;
        wait_for_test_task_start_ack_barrier();
        let _ = fs::remove_file(&claim_path);
        Ok(Some(response))
    }

    fn restore_task_start_response_claim(control: &Path, request_id: &str) -> Result<()> {
        let response_path = task_start_response_path(control, request_id)?;
        let claim_path = task_start_response_claim_path(control, request_id)?;
        if !claim_path.is_file() {
            return Ok(());
        }
        if response_path.is_file() {
            return remove_file_if_present(&claim_path, "task response claim");
        }
        match fs::rename(&claim_path, &response_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!(
                "could not restore task response claim {claim_path:?}: {err}"
            ))),
        }
    }

    fn remove_task_start_response_files(control: &Path, request_id: &str) -> Result<()> {
        let response_path = task_start_response_path(control, request_id)?;
        let claim_path = task_start_response_claim_path(control, request_id)?;
        remove_file_if_present(&response_path, "task response")?;
        remove_file_if_present(&claim_path, "task response claim")
    }

    fn remove_file_if_present(path: &Path, description: &str) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!(
                "could not remove {description} {path:?}: {err}"
            ))),
        }
    }

    fn mark_task_start_ack(control: &Path, request_id: &str) -> Result<()> {
        let dir = task_start_ack_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let path = mailbox::file_name(request_id);
        mailbox::atomic_write(&dir, &path, "")
    }

    fn task_start_ack_exists(control: &Path, request_id: &str) -> Result<bool> {
        Ok(task_start_ack_path(control, request_id)?.is_file())
    }

    fn remove_task_start_ack(control: &Path, request_id: &str) -> Result<()> {
        let path = task_start_ack_path(control, request_id)?;
        remove_file_if_present(&path, "task-start acknowledgement")
    }

    /// Removes every durable file belonging to a task-start transaction after
    /// the task record has reached a safe cleanup boundary. Explicit
    /// ownership teardown uses this after asserting the process identity;
    /// leaving an admission marker behind after a forced removal would make
    /// the next supervisor treat an already-removed task as rollback residue.
    fn remove_task_start_transaction(control: &Path, record: &TaskRecord) -> Result<()> {
        let Some(request_id) = record.request_id.as_deref() else {
            return Ok(());
        };
        discard_pending_task_start_request(control, request_id)?;
        remove_task_start_response_files(control, request_id)?;
        remove_task_start_ack(control, request_id)?;
        remove_task_start_rollback(control, request_id)
    }

    fn list_task_start_acks(control: &Path) -> Result<Vec<String>> {
        let dir = task_start_ack_dir(control);
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read task-start acknowledgement directory {dir:?}: {err}"
                )));
            }
        };
        let mut request_ids = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            request_ids.push(key);
        }
        Ok(request_ids)
    }

    fn list_task_start_response_claims(control: &Path) -> Result<Vec<String>> {
        let dir = task_responses_dir(control);
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read task response directory {dir:?}: {err}"
                )));
            }
        };
        let mut request_ids = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(response_name) = name
                .strip_prefix('.')
                .and_then(|name| name.strip_suffix(".claimed"))
            else {
                continue;
            };
            let response_path = dir.join(response_name);
            if let Some(request_id) = mailbox::json_key(&response_path) {
                request_ids.push(request_id);
            }
        }
        Ok(request_ids)
    }

    fn task_start_response_id(
        control: &Path,
        request_id: &str,
        response: TaskStartResponse,
    ) -> Result<String> {
        if let Some(error) = response.error {
            return Err(BatonError::Io(error));
        }
        response.task_id.ok_or_else(|| {
            let path = task_responses_dir(control).join(mailbox::file_name(request_id));
            BatonError::Decode(format!(
                "task response {path:?} contained neither a task id nor an error"
            ))
        })
    }

    /// Discards a task-start request that has not been answered after the
    /// supervisor released the control lock. It may still be waiting in
    /// `task-requests/` or already claimed in `task-processing/`; removing
    /// both locations prevents a restarted supervisor from replaying a
    /// request whose client has been told admission failed.
    fn discard_pending_task_start_request(control: &Path, request_id: &str) -> Result<()> {
        let file_name = mailbox::file_name(request_id);
        for dir in [task_requests_dir(control), task_processing_dir(control)] {
            let path = dir.join(&file_name);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(BatonError::Io(format!(
                        "could not discard pending task start request {path:?}: {err}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Records that the submitting client observed admission loss. The marker
    /// is durable before request files are removed, so a restart can reconcile
    /// a task record that was written before the response boundary.
    fn mark_task_start_rollback(control: &Path, request_id: &str) -> Result<()> {
        let dir = task_start_rollback_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        mailbox::atomic_write(&dir, &mailbox::file_name(request_id), "")
    }

    fn task_start_rollback_path(control: &Path, request_id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(request_id) {
            return Err(BatonError::Io(format!(
                "task start request id is not usable as a filename: {request_id:?}"
            )));
        }
        Ok(task_start_rollback_dir(control).join(mailbox::file_name(request_id)))
    }

    fn task_start_rollback_exists(control: &Path, request_id: &str) -> Result<bool> {
        Ok(task_start_rollback_path(control, request_id)?.is_file())
    }

    fn remove_task_start_rollback(control: &Path, request_id: &str) -> Result<()> {
        let path = task_start_rollback_path(control, request_id)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!(
                "could not remove task start rollback marker {path:?}: {err}"
            ))),
        }
    }

    fn list_task_start_rollbacks(control: &Path) -> Result<Vec<String>> {
        let dir = task_start_rollback_dir(control);
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read task start rollback directory {dir:?}: {err}"
                )));
            }
        };
        let mut request_ids = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            request_ids.push(key);
        }
        Ok(request_ids)
    }

    /// Reconciles task-start transactions before the request loop can accept
    /// new work. The caller holds the admission lock for the whole pass.
    /// Prepared records are never safe to rehydrate; a rollback marker also
    /// wins over a committed or responded record because the client observed
    /// no response. A durable acknowledgement wins over any response file and
    /// upgrades a committed record to responded. A committed record with a
    /// response or a recoverable claim is finalized as responded, while a
    /// committed record with neither is given one response before that phase
    /// is persisted. Rollback, claim, and acknowledgement cleanup is
    /// idempotent across interrupted startup passes. An unresolved prepared
    /// record remains durable cleanup residue, including its rollback marker,
    /// until a later liveness probe can prove that its process is dead.
    fn reconcile_task_admissions(control: &Path) -> Result<()> {
        let rollback_ids = list_task_start_rollbacks(control)?;
        let ack_ids = list_task_start_acks(control)?;
        let claim_ids = list_task_start_response_claims(control)?;
        let records = list_task_records(control)?;
        let mut seen_rollbacks = std::collections::HashSet::new();
        let mut retained_rollbacks = std::collections::HashSet::new();
        let mut seen_acks = std::collections::HashSet::new();

        for record in records {
            if record.admission == TaskAdmissionPhase::Prepared {
                let request_id = record.request_id.as_deref();
                let rollback = request_id
                    .map(|id| rollback_ids.iter().any(|rollback_id| rollback_id == id))
                    .unwrap_or(false);
                if rollback && let Some(request_id) = request_id {
                    seen_rollbacks.insert(request_id.to_string());
                }
                if let Some(request_id) = request_id {
                    discard_pending_task_start_request(control, request_id)?;
                }
                if !abort_task_admission(control, &record)? {
                    if rollback && let Some(request_id) = request_id {
                        retained_rollbacks.insert(request_id.to_string());
                    }
                    eprintln!(
                        "warning: task {} admission remains unresolved; preserving its record",
                        record.id
                    );
                    continue;
                }
                if let Some(request_id) = request_id {
                    remove_task_start_response_files(control, request_id)?;
                    remove_task_start_ack(control, request_id)?;
                    if rollback {
                        wait_for_test_task_rollback_cleanup_barrier(
                            TEST_TASK_ROLLBACK_RECONCILE_BARRIER,
                        );
                    }
                    remove_task_start_rollback(control, request_id)?;
                }
                continue;
            }
            let Some(request_id) = record.request_id.as_deref() else {
                continue;
            };
            let rollback = rollback_ids.iter().any(|id| id == request_id);
            if rollback {
                seen_rollbacks.insert(request_id.to_string());
            }
            if rollback {
                if !abort_task_admission(control, &record)? {
                    retained_rollbacks.insert(request_id.to_string());
                    eprintln!(
                        "warning: task {} rollback remains unresolved; preserving its record",
                        record.id
                    );
                    continue;
                }
                discard_pending_task_start_request(control, request_id)?;
                remove_task_start_response_files(control, request_id)?;
                remove_task_start_ack(control, request_id)?;
                wait_for_test_task_rollback_cleanup_barrier(TEST_TASK_ROLLBACK_RECONCILE_BARRIER);
                remove_task_start_rollback(control, request_id)?;
                continue;
            }

            discard_pending_task_start_request(control, request_id)?;
            if task_start_ack_exists(control, request_id)? {
                seen_acks.insert(request_id.to_string());
                remove_task_start_response_files(control, request_id)?;
                if record.admission == TaskAdmissionPhase::Committed {
                    let mut responded = record.clone();
                    responded.admission = TaskAdmissionPhase::Responded;
                    if let Err(err) = write_task_record(control, &responded) {
                        eprintln!(
                            "warning: task {} acknowledgement was durable but responded phase could not be persisted: {err}",
                            record.id
                        );
                        continue;
                    }
                }
                remove_task_start_ack(control, request_id)?;
                continue;
            }

            restore_task_start_response_claim(control, request_id)?;
            if record.admission == TaskAdmissionPhase::Committed {
                let response_path = task_start_response_path(control, request_id)?;
                if !response_path.is_file()
                    && let Err(err) = write_task_start_response(
                        control,
                        request_id,
                        &TaskStartResponse {
                            task_id: Some(record.id.clone()),
                            error: None,
                        },
                    )
                {
                    eprintln!(
                        "warning: task {} response restoration failed; retaining committed admission: {err}",
                        record.id
                    );
                    continue;
                }
                let mut responded = record.clone();
                responded.admission = TaskAdmissionPhase::Responded;
                if let Err(err) = write_task_record(control, &responded) {
                    eprintln!(
                        "warning: task {} restored response was written but responded phase could not be persisted: {err}",
                        record.id
                    );
                }
            }
        }

        for request_id in ack_ids {
            if !seen_acks.contains(&request_id) {
                discard_pending_task_start_request(control, &request_id)?;
                remove_task_start_response_files(control, &request_id)?;
                remove_task_start_ack(control, &request_id)?;
            }
        }

        for request_id in claim_ids {
            let claim_path = task_start_response_claim_path(control, &request_id)?;
            if !claim_path.is_file() {
                continue;
            }
            if task_start_ack_exists(control, &request_id)? {
                remove_task_start_response_files(control, &request_id)?;
                remove_task_start_ack(control, &request_id)?;
            } else {
                restore_task_start_response_claim(control, &request_id)?;
            }
        }

        for request_id in rollback_ids {
            if !seen_rollbacks.contains(&request_id) {
                discard_pending_task_start_request(control, &request_id)?;
                remove_task_start_response_files(control, &request_id)?;
                remove_task_start_ack(control, &request_id)?;
                wait_for_test_task_rollback_cleanup_barrier(TEST_TASK_ROLLBACK_RECONCILE_BARRIER);
            }
            if !retained_rollbacks.contains(&request_id) {
                remove_task_start_rollback(control, &request_id)?;
            }
        }
        Ok(())
    }

    fn abort_task_admission(control: &Path, record: &TaskRecord) -> Result<bool> {
        if record.state == TaskState::Running {
            let mut record = record.clone();
            upgrade_legacy_task_record(control, &mut record)?;
            let mut liveness = task_execution_liveness_after_retry(&record, KILL_GRACE_MS);
            if liveness == Liveness::Unresolved {
                return Ok(false);
            }
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                wait_while_task_alive(&record, KILL_GRACE_MS);
                liveness = task_execution_liveness_after_retry(&record, KILL_GRACE_MS);
                if liveness == Liveness::Live {
                    let _ = signal_group(record.pid, "-KILL");
                    wait_while_task_alive(&record, KILL_GRACE_MS);
                    liveness = task_execution_liveness_after_retry(&record, KILL_GRACE_MS);
                }
            }
            if liveness != Liveness::Dead {
                return Ok(false);
            }
        }
        remove_task_record(control, &record.id).map(|()| true)
    }

    /// Returns any `task-processing/` entry a crash left mid-request to
    /// `task-requests/`. Mirrors [`reclaim_stale_requests`].
    fn reclaim_stale_task_requests(control: &Path) -> Result<()> {
        let processing = task_processing_dir(control);
        let entries = match fs::read_dir(&processing) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {processing:?}: {err}"
                )));
            }
        };
        let requests = task_requests_dir(control);
        fs::create_dir_all(&requests)
            .map_err(|err| BatonError::Io(format!("could not create {requests:?}: {err}")))?;
        for entry in entries {
            let path = mailbox::dir_entry(entry, &processing)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            let dest = requests.join(mailbox::file_name(&key));
            fs::rename(&path, &dest)
                .map_err(|err| BatonError::Io(format!("could not reclaim {path:?}: {err}")))?;
        }
        Ok(())
    }

    /// One task the `Run` loop is currently tracking: its durable
    /// [`TaskRecord`] (kept in sync as milestones fire and it goes terminal),
    /// either the live [`Child`] handle owned by this supervisor or a
    /// rehydrated PID identity, and the injected-clock timestamps driving
    /// milestone/max-duration decisions.
    struct RunningTask {
        record: TaskRecord,
        /// `Some` while this `Run` instance owns the child handle. A task
        /// restored after a supervisor restart has already been reparented to
        /// init, so it is represented by `None` and polled by corroborated
        /// PID liveness instead of `Child::try_wait`.
        child: Option<Child>,
        /// Most recent non-Linux liveness sample for a rehydrated task. The
        /// sample is intentionally in-memory: a restart must corroborate the
        /// durable PID again before making any decision about it.
        #[cfg(not(target_os = "linux"))]
        rehydrated_liveness: Option<(u64, Liveness)>,
        started_ms: u64,
        /// Set once this task's max duration has been exceeded and `SIGTERM`
        /// sent, so a later tick knows to escalate to `SIGKILL` after
        /// `KILL_GRACE_MS`, and a successful reap after this is set is
        /// attributed to `timeout`, not `completed`/`failed`.
        term_sent_at_ms: Option<u64>,
        /// Set once `SIGKILL` has been sent, so it is only ever sent once.
        kill_sent: bool,
        /// Number of failed terminal callback delivery attempts. These are
        /// deliberately in-memory: a terminal record is replayed once after
        /// restart, then follows the same bounded retry policy.
        terminal_delivery_attempts: u32,
        /// Clock deadline at which the next terminal callback delivery may be
        /// attempted.
        next_terminal_retry_ms: Option<u64>,
        /// Delay used for the most recent failed terminal delivery, so the
        /// next failure can double it without a wall-clock dependency.
        terminal_retry_delay_ms: u64,
    }

    /// Restores every durable task before the request loop accepts new work.
    /// Running records are rehydrated for PID-based tracking; terminal
    /// records are retained for one deterministic callback replay in case the
    /// previous supervisor persisted state immediately before it exited. The
    /// child process was reparented when the previous supervisor exited, so
    /// the new tracker deliberately carries no `Child` handle and lets
    /// `tick_one_task` use the record's corroborated PID identity instead.
    /// Prepared admission records are intentionally excluded: they have not
    /// crossed the durable admission boundary, so an unresolved one is
    /// retained for reconciliation rather than treated as active work.
    fn rehydrate_tasks(control: &Path, clock: &dyn Clock) -> Result<HashMap<String, RunningTask>> {
        let mut tasks = HashMap::new();
        for mut record in list_task_records(control)? {
            if record.admission == TaskAdmissionPhase::Prepared {
                continue;
            }
            let started_ms = match record.started_ms {
                Some(started_ms) => started_ms,
                None if record.state == TaskState::Running => {
                    // Older records have no durable wall-clock origin. They
                    // cannot be assigned a trustworthy historical elapsed
                    // time, so preserve the task and start timing from this
                    // restart while upgrading the record for future restarts.
                    let started_ms = clock.now_ms();
                    record.started_ms = Some(started_ms);
                    write_task_record(control, &record)?;
                    started_ms
                }
                None => clock.now_ms(),
            };
            let id = record.id.clone();
            tasks.insert(
                id,
                RunningTask {
                    record,
                    child: None,
                    #[cfg(not(target_os = "linux"))]
                    rehydrated_liveness: None,
                    started_ms,
                    term_sent_at_ms: None,
                    kill_sent: false,
                    terminal_delivery_attempts: 0,
                    next_terminal_retry_ms: None,
                    terminal_retry_delay_ms: 0,
                },
            );
        }
        Ok(tasks)
    }

    /// Outcome of one [`tick_one_task`] call.
    #[derive(Debug)]
    enum TaskTick {
        StillRunning,
        Finished,
        TerminalDeliveryRetry {
            error: String,
            attempt: u32,
            delay_ms: u64,
        },
        TerminalDeliveryDropped {
            error: String,
            attempts: u32,
        },
    }

    /// Claims and handles the next pending task-start request, if any.
    /// Mirrors [`process_one_request`].
    fn process_one_task_request(
        control: &Path,
        clock: &dyn Clock,
    ) -> Result<Option<(String, RunningTask)>> {
        let dir = task_requests_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(BatonError::Io(format!("could not read {dir:?}: {err}"))),
        };
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            let processing = task_processing_dir(control);
            fs::create_dir_all(&processing)
                .map_err(|err| BatonError::Io(format!("could not create {processing:?}: {err}")))?;
            let claimed_path = processing.join(mailbox::file_name(&key));
            match fs::rename(&path, &claimed_path) {
                Ok(()) => {
                    // The lock is intentionally acquired after the request is
                    // claimed but before owner validation and spawn. If
                    // session cleanup wins the race, validation observes the
                    // removed/dead owner; if admission wins, cleanup waits
                    // and reaps the newly recorded task.
                    let outcome = acquire_admission_lock(control).and_then(|_admission| {
                        if task_start_rollback_exists(control, &key)? {
                            discard_pending_task_start_request(control, &key)?;
                            wait_for_test_task_rollback_cleanup_barrier(
                                TEST_TASK_ROLLBACK_REQUEST_BARRIER,
                            );
                            remove_task_start_rollback(control, &key)?;
                            return Ok(None);
                        }
                        handle_task_start_request(control, &key, &claimed_path, clock)
                    });
                    let _ = fs::remove_file(&claimed_path);
                    let Some((record, child, started_ms)) = outcome? else {
                        return Ok(None);
                    };
                    let id = record.id.clone();
                    let running = RunningTask {
                        record,
                        child: Some(child),
                        #[cfg(not(target_os = "linux"))]
                        rehydrated_liveness: None,
                        started_ms,
                        term_sent_at_ms: None,
                        kill_sent: false,
                        terminal_delivery_attempts: 0,
                        next_terminal_retry_ms: None,
                        terminal_retry_delay_ms: 0,
                    };
                    return Ok(Some((id, running)));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(BatonError::Io(format!("could not claim {path:?}: {err}"))),
            }
        }
        Ok(None)
    }

    /// Answers a claimed task-start request with an admission failure the
    /// supervisor can name, mirroring [`reject_start_request`].
    fn reject_task_start_request(
        control: &Path,
        request_id: &str,
        error: String,
    ) -> Result<Option<(TaskRecord, Child, u64)>> {
        write_task_start_response(
            control,
            request_id,
            &TaskStartResponse {
                task_id: None,
                error: Some(error),
            },
        )?;
        Ok(None)
    }

    /// Validates the requested owner, then spawns the task, persists its
    /// [`TaskRecord`], and answers the request with its task id. Mirrors
    /// [`handle_start_request`]'s
    /// kill-and-unwind-on-any-later-failure discipline until the committed
    /// record is durable. After that point the task remains tracked when
    /// response delivery or phase persistence fails, so restart reconciliation
    /// can retry the response boundary without spawning again.
    ///
    /// Every admission failure before that point — owner rejection, log-dir
    /// creation, spawn, post-spawn corroboration, record write — is answered
    /// as an error response and reported as `Ok(None)`, so the client fails
    /// immediately with the real reason instead of waiting out
    /// [`START_AWAIT_MS`]. Only a failure to deliver a response at all is
    /// propagated as `Err`.
    fn handle_task_start_request(
        control: &Path,
        request_id: &str,
        spec_path: &Path,
        clock: &dyn Clock,
    ) -> Result<Option<(TaskRecord, Child, u64)>> {
        let data = fs::read_to_string(spec_path)
            .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
        let spec: TaskSpec = serde_json::from_str(&data).map_err(|err| {
            BatonError::Decode(format!("malformed task spec {spec_path:?}: {err}"))
        })?;
        // A session being stopped is not an admissible owner, even while its
        // process is still live. `service stop` releases the admission lock
        // across its grace windows (so it never freezes this loop), which
        // leaves a window where the owner still probes `Live`; without this
        // gate a start racing that window would be answered with a task id
        // for a process the very same stop is about to kill.
        let owner_live = if mailbox::is_safe_key(&spec.session) {
            read_session_record(control, &spec.session)?
                .map(|record| is_session_alive(&record) == Liveness::Live)
                .unwrap_or(false)
                && !session_stop_in_progress(control, &spec.session)?
        } else {
            false
        };
        if !owner_live {
            let error = format!(
                "task start rejected: --session {:?} does not name a live managed session on {:?} (the session record is absent, its process is no longer live, or it is draining a stop request)",
                spec.session, control
            );
            return reject_task_start_request(control, request_id, error);
        }
        let task_id = fresh_task_id();
        let log_dir = task_logs_dir(control, &task_id);
        if let Err(err) = fs::create_dir_all(&log_dir) {
            return reject_task_start_request(
                control,
                request_id,
                format!("could not create {log_dir:?}: {err}"),
            );
        }
        let stdout_path = log_dir.join("stdout.log");
        let stderr_path = log_dir.join("stderr.log");
        let mut child = match spawn_task_child(&spec, &stdout_path, &stderr_path) {
            Ok(child) => child,
            Err(err) => {
                // Nothing ever ran under this id, so its just-created log
                // directory holds two empty files and no record refers to
                // it: drop it rather than leaking one per failed start.
                let _ = fs::remove_dir_all(&log_dir);
                return reject_task_start_request(control, request_id, admission_error_text(&err));
            }
        };
        let pid = child.id();
        let (started_at, start_epoch_secs) = recorded_start_identity(pid);
        if !spawn_start_key_ok(&started_at, &start_epoch_secs) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            return reject_task_start_request(
                control,
                request_id,
                format!(
                    "task command (pid {pid}) could not be corroborated right after spawn; treating as a spawn failure"
                ),
            );
        }
        let started_ms = clock.now_ms();
        let mut record = TaskRecord {
            id: task_id,
            request_id: Some(request_id.to_string()),
            admission: TaskAdmissionPhase::Prepared,
            spec,
            pid,
            started_at,
            start_epoch_secs,
            job: None,
            started_ms: Some(started_ms),
            state: TaskState::Running,
            exit_code: None,
            elapsed_ms: None,
            stdout_path: stdout_path.display().to_string(),
            stderr_path: stderr_path.display().to_string(),
            delivered_milestones: 0,
        };
        if let Err(err) = write_task_record(control, &record) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            return reject_task_start_request(control, request_id, admission_error_text(&err));
        }
        wait_for_test_task_admission_barrier();
        record.admission = TaskAdmissionPhase::Committed;
        if let Err(err) = write_task_record(control, &record) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = remove_task_record(control, &record.id);
            return reject_task_start_request(control, request_id, admission_error_text(&err));
        }
        let respond = write_task_start_response(
            control,
            request_id,
            &TaskStartResponse {
                task_id: Some(record.id.clone()),
                error: None,
            },
        );
        if let Err(err) = respond {
            eprintln!(
                "warning: task {id} admission response could not be written; retaining committed admission: {err}",
                id = record.id
            );
            return Ok(Some((record, child, started_ms)));
        }
        wait_for_test_task_response_phase_barrier();
        record.admission = TaskAdmissionPhase::Responded;
        if let Err(err) = write_task_record(control, &record) {
            eprintln!(
                "warning: task {id} response was written but its responded phase could not be persisted: {err}",
                id = record.id
            );
            record.admission = TaskAdmissionPhase::Committed;
        }
        Ok(Some((record, child, started_ms)))
    }

    /// Test-only synchronization seam for the post-record/pre-response crash
    /// regression. A service launched with this environment variable waits
    /// after persisting the prepared record until the named path disappears;
    /// production callers never set it. This helper is compiled only with
    /// debug assertions; release builds use the no-op fallback below so the
    /// test seam cannot affect a shipped service.
    #[cfg(debug_assertions)]
    fn wait_for_test_task_admission_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_ADMISSION_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    #[cfg(not(debug_assertions))]
    fn wait_for_test_task_admission_barrier() {}

    /// Test-only synchronization seam for the response/phase boundary. A
    /// service launched with this environment variable waits after publishing
    /// the response while still holding the admission lock; production callers
    /// never set it. This helper is compiled only with debug assertions; the
    /// release fallback is a no-op.
    #[cfg(debug_assertions)]
    fn wait_for_test_task_response_phase_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_RESPONSE_PHASE_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    #[cfg(not(debug_assertions))]
    fn wait_for_test_task_response_phase_barrier() {}

    /// Test-only synchronization seam for the response claim/ack boundary. A
    /// task-start client waits after persisting its acknowledgement and before
    /// removing the private claim; production callers never set it. This helper
    /// is compiled only with debug assertions; the release fallback is a
    /// no-op.
    #[cfg(debug_assertions)]
    fn wait_for_test_task_start_ack_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_START_ACK_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    #[cfg(not(debug_assertions))]
    fn wait_for_test_task_start_ack_barrier() {}

    #[cfg(debug_assertions)]
    const TEST_TASK_ROLLBACK_RECONCILE_BARRIER: &str = "BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER";
    #[cfg(not(debug_assertions))]
    const TEST_TASK_ROLLBACK_RECONCILE_BARRIER: &str = "";

    #[cfg(debug_assertions)]
    const TEST_TASK_ROLLBACK_REQUEST_BARRIER: &str = "BATON_TEST_TASK_ROLLBACK_REQUEST_BARRIER";
    #[cfg(not(debug_assertions))]
    const TEST_TASK_ROLLBACK_REQUEST_BARRIER: &str = "";

    /// Test-only synchronization seam for rollback cleanup ordering. A
    /// service launched with one of the named environment variables waits
    /// after request/record cleanup and before removing the rollback marker;
    /// production callers never set it. This helper is compiled only with
    /// debug assertions; the release fallback is a no-op.
    #[cfg(debug_assertions)]
    fn wait_for_test_task_rollback_cleanup_barrier(variable: &str) {
        let Some(path) = std::env::var_os(variable) else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    #[cfg(not(debug_assertions))]
    fn wait_for_test_task_rollback_cleanup_barrier(_variable: &str) {}

    fn write_task_start_response(
        control: &Path,
        request_id: &str,
        response: &TaskStartResponse,
    ) -> Result<()> {
        // This failure injection is needed by integration tests, whose
        // test-built binary has debug assertions enabled. Keep the release
        // binary free of the test environment seam.
        #[cfg(debug_assertions)]
        if let Some(path) = std::env::var_os("BATON_TEST_TASK_START_RESPONSE_WRITE_FAILURE") {
            let path = std::path::PathBuf::from(path);
            match fs::remove_file(&path) {
                Ok(()) => {
                    return Err(BatonError::Io(format!(
                        "test-injected task-start response write failure at {path:?}"
                    )));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(BatonError::Io(format!(
                        "could not consume task-start response failure marker {path:?}: {err}"
                    )));
                }
            }
        }
        let json = serde_json::to_string(response).map_err(|err| {
            BatonError::Io(format!("could not serialize task start response: {err}"))
        })?;
        let responses = task_responses_dir(control);
        fs::create_dir_all(&responses)
            .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
        mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
    }

    /// Spawns `spec`'s command as its own process-group leader, stdout/stderr
    /// redirected to durable log files (unlike [`spawn_serve_child`], which
    /// discards its child's stdio entirely — a task's output is part of its
    /// durable record), and returns the live [`Child`] without waiting on
    /// it — `Run`'s loop reaps it later via [`tick_one_task`].
    fn spawn_task_child(spec: &TaskSpec, stdout_path: &Path, stderr_path: &Path) -> Result<Child> {
        let mut command = Command::new(&spec.command);
        command.args(&spec.args);
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        command.envs(spec.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        command.stdin(Stdio::null());
        let stdout_file = File::create(stdout_path)
            .map_err(|err| BatonError::Io(format!("could not create {stdout_path:?}: {err}")))?;
        let stderr_file = File::create(stderr_path)
            .map_err(|err| BatonError::Io(format!("could not create {stderr_path:?}: {err}")))?;
        command.stdout(Stdio::from(stdout_file));
        command.stderr(Stdio::from(stderr_file));
        // Own process-group leader, like `spawn_serve_child`, so a later
        // `kill -- -<pid>` (max-duration enforcement or `baton task cancel`)
        // reaches the task's whole subtree, not just this direct child.
        command.process_group(0);
        command.spawn().map_err(|err| {
            BatonError::Io(format!(
                "could not spawn task command {:?}: {err}",
                spec.command
            ))
        })
    }

    /// Returns one liveness sample for a rehydrated task. Linux deliberately
    /// keeps its existing `/proc` cadence; non-Linux hosts cache the sample.
    #[cfg(target_os = "linux")]
    fn rehydrated_task_liveness(
        running: &mut RunningTask,
        _now_ms: u64,
        _force_refresh: bool,
    ) -> Liveness {
        task_execution_liveness(&running.record)
    }

    /// Returns one cached liveness sample for a rehydrated task. A forced
    /// refresh is used immediately before timeout escalation so a stale live
    /// sample can never authorize signalling a reused PID. The caller must
    /// thread the returned value through the rest of the tick.
    #[cfg(not(target_os = "linux"))]
    fn rehydrated_task_liveness(
        running: &mut RunningTask,
        now_ms: u64,
        force_refresh: bool,
    ) -> Liveness {
        let refresh = force_refresh
            || running
                .rehydrated_liveness
                .map(|(checked_ms, _)| {
                    now_ms.saturating_sub(checked_ms) >= REHYDRATED_LIVENESS_CACHE_MS
                })
                .unwrap_or(true);
        if refresh {
            running.rehydrated_liveness = Some((now_ms, task_execution_liveness(&running.record)));
        }
        running
            .rehydrated_liveness
            .expect("rehydrated liveness cache populated")
            .1
    }

    /// Advances one tracked task by one loop tick: delivers any
    /// newly-due milestone events, escalates `SIGTERM`→`SIGKILL` past
    /// `max_duration_ms`, and — once the process has actually exited —
    /// persists its terminal state and delivers its terminal event.
    ///
    /// Pure with respect to wall-clock time: every timing decision reads
    /// `clock.now_ms()`, so a test can drive milestone/max-duration/terminal
    /// behavior deterministically with a `FakeClock` and no real sleep.
    fn tick_one_task(
        control: &Path,
        id: &str,
        running: &mut RunningTask,
        clock: &dyn Clock,
    ) -> Result<TaskTick> {
        // A terminal record can remain in the tracker when callback delivery
        // failed after state persistence. Retry the deterministic event before
        // dropping it, including after startup reconciliation.
        if read_task_record(control, id)?.is_none() {
            return Ok(TaskTick::Finished);
        }
        if running.record.state != TaskState::Running {
            return deliver_terminal_event(running, clock);
        }

        let now_ms = clock.now_ms();
        let elapsed_ms = now_ms.saturating_sub(running.started_ms);

        for index in milestones_due(
            elapsed_ms,
            &running.record.spec.milestones_ms,
            running.record.delivered_milestones,
        ) {
            deliver_task_event(&running.record, TaskEventKind::Milestone { index })?;
            running.record.delivered_milestones = index + 1;
            if let Err(err) = write_task_record(control, &running.record) {
                running.record.delivered_milestones = index;
                return Err(err);
            }
        }

        // A rehydrated task has no Child handle. Check its identity before
        // any timeout signal so a gone or PID-reused process is never
        // accidentally signalled as this task. An unresolved identity is
        // retained and retried on a later tick.
        let rehydrated_liveness = if running.child.is_none() {
            let signal_due = (running.term_sent_at_ms.is_none()
                && max_duration_exceeded(elapsed_ms, running.record.spec.max_duration_ms))
                || (running.term_sent_at_ms.is_some()
                    && !running.kill_sent
                    && now_ms.saturating_sub(running.term_sent_at_ms.unwrap()) >= KILL_GRACE_MS);
            let liveness = rehydrated_task_liveness(running, now_ms, signal_due);
            match liveness {
                Liveness::Dead => {
                    let cancelled = consume_task_cancel_sentinel(control, id)?;
                    let state = if cancelled {
                        TaskState::Cancelled
                    } else if running.term_sent_at_ms.is_some() {
                        TaskState::Timeout
                    } else {
                        TaskState::Failed
                    };
                    return finalize_task(control, running, state, None, elapsed_ms, clock);
                }
                Liveness::Live => {}
                Liveness::Unresolved => return Ok(TaskTick::StillRunning),
            }
            Some(liveness)
        } else {
            None
        };

        if running.term_sent_at_ms.is_none()
            && max_duration_exceeded(elapsed_ms, running.record.spec.max_duration_ms)
        {
            let liveness =
                rehydrated_liveness.unwrap_or_else(|| task_execution_liveness(&running.record));
            match liveness {
                Liveness::Unresolved => return Ok(TaskTick::StillRunning),
                Liveness::Live => {
                    let _ = signal_group(running.record.pid, "-TERM");
                    running.term_sent_at_ms = Some(clock.now_ms());
                }
                Liveness::Dead => {}
            }
        } else if let Some(term_at) = running.term_sent_at_ms
            && !running.kill_sent
            && clock.now_ms().saturating_sub(term_at) >= KILL_GRACE_MS
            && rehydrated_liveness != Some(Liveness::Dead)
        {
            let _ = signal_group(running.record.pid, "-KILL");
            running.kill_sent = true;
        }

        match running.child.as_mut() {
            None => match rehydrated_liveness.expect("rehydrated liveness cached above") {
                Liveness::Live | Liveness::Unresolved => Ok(TaskTick::StillRunning),
                Liveness::Dead => {
                    let cancelled = consume_task_cancel_sentinel(control, id)?;
                    let state = if cancelled {
                        TaskState::Cancelled
                    } else if running.term_sent_at_ms.is_some() {
                        TaskState::Timeout
                    } else {
                        TaskState::Failed
                    };
                    finalize_task(control, running, state, None, elapsed_ms, clock)
                }
            },
            Some(child) => match child.try_wait() {
                Ok(Some(status)) => {
                    // Reaping the direct leader does not end a Unix task while a
                    // descendant still occupies its process group. Keep the
                    // Child handle so its exit status remains available for
                    // finalization after the group drains. A mismatched PID is
                    // never treated as this task's group.
                    match task_execution_liveness(&running.record) {
                        Liveness::Live | Liveness::Unresolved => {
                            return Ok(TaskTick::StillRunning);
                        }
                        Liveness::Dead => {}
                    }
                    // An external `Stop`/`Teardown`/`Cancel` may already have
                    // finalized and removed this task's record (they act
                    // directly on the durable PID, independent of this `Run`
                    // loop being alive) — if so, this reap is a no-op besides
                    // dropping our own in-memory tracking below, so a race with
                    // an external reaper never resurrects a torn-down record.
                    if read_task_record(control, id)?.is_none() {
                        return Ok(TaskTick::Finished);
                    }
                    let cancelled = consume_task_cancel_sentinel(control, id)?;
                    let state = if cancelled {
                        TaskState::Cancelled
                    } else if running.term_sent_at_ms.is_some() {
                        TaskState::Timeout
                    } else if status.success() {
                        TaskState::Completed
                    } else {
                        TaskState::Failed
                    };
                    finalize_task(control, running, state, status.code(), elapsed_ms, clock)
                }
                Ok(None) => Ok(TaskTick::StillRunning),
                Err(err) => Err(BatonError::Io(format!("could not poll task {id}: {err}"))),
            },
        }
    }

    /// Delivers a terminal event, applying bounded exponential backoff when
    /// the callback inbox is unavailable.
    fn deliver_terminal_event(running: &mut RunningTask, clock: &dyn Clock) -> Result<TaskTick> {
        let now_ms = clock.now_ms();
        if let Some(next_retry_ms) = running.next_terminal_retry_ms
            && now_ms < next_retry_ms
        {
            return Ok(TaskTick::StillRunning);
        }

        match deliver_task_event(&running.record, TaskEventKind::Terminal) {
            Ok(()) => Ok(TaskTick::Finished),
            Err(err) => {
                let attempt = running.terminal_delivery_attempts.saturating_add(1);
                running.terminal_delivery_attempts = attempt;
                let error = err.to_string();
                if attempt >= MAX_TERMINAL_DELIVERY_ATTEMPTS {
                    return Ok(TaskTick::TerminalDeliveryDropped {
                        error,
                        attempts: attempt,
                    });
                }

                let delay_ms = if attempt == 1 {
                    TERMINAL_RETRY_INITIAL_DELAY_MS
                } else {
                    running
                        .terminal_retry_delay_ms
                        .saturating_mul(2)
                        .min(TERMINAL_RETRY_MAX_DELAY_MS)
                };
                running.terminal_retry_delay_ms = delay_ms;
                running.next_terminal_retry_ms = Some(now_ms.saturating_add(delay_ms));
                Ok(TaskTick::TerminalDeliveryRetry {
                    error,
                    attempt,
                    delay_ms,
                })
            }
        }
    }

    /// Persists a terminal task state before delivering its deterministic
    /// terminal event. If delivery fails, the terminal record remains in the
    /// tracker and the next tick retries the same event id.
    fn finalize_task(
        control: &Path,
        running: &mut RunningTask,
        state: TaskState,
        exit_code: Option<i32>,
        elapsed_ms: u64,
        clock: &dyn Clock,
    ) -> Result<TaskTick> {
        let previous = running.record.clone();
        {
            // External Stop/Teardown may remove a durable task while this
            // supervisor still has its in-memory tracker. Serialize the
            // existence check and terminal write with that cleanup so a
            // concurrent tick cannot resurrect the removed record.
            let _admission = acquire_admission_lock(control)?;
            if read_task_record(control, &running.record.id)?.is_none() {
                return Ok(TaskTick::Finished);
            }
            running.record.state = state;
            running.record.exit_code = exit_code;
            running.record.elapsed_ms = Some(elapsed_ms);
            if let Err(err) = write_task_record(control, &running.record) {
                running.record = previous;
                return Err(err);
            }
        }
        deliver_terminal_event(running, clock)
    }

    /// Ticks every tracked task once, dropping any that finished. One task's
    /// tick failure is warned and leaves it tracked for the next tick — the
    /// same "one bad thing can't wedge the daemon" posture the rest of
    /// `Run`'s loop takes.
    fn tick_tasks(control: &Path, tasks: &mut HashMap<String, RunningTask>, clock: &dyn Clock) {
        let mut finished = Vec::new();
        for (id, running) in tasks.iter_mut() {
            match tick_one_task(control, id, running, clock) {
                Ok(TaskTick::Finished) => finished.push(id.clone()),
                Ok(TaskTick::StillRunning) => {}
                Ok(TaskTick::TerminalDeliveryRetry {
                    error,
                    attempt,
                    delay_ms,
                }) => {
                    eprintln!(
                        "warning: baton service failed to deliver terminal event for task {id} to callback inbox {:?} (attempt {attempt}/{MAX_TERMINAL_DELIVERY_ATTEMPTS}; retrying in {delay_ms} ms): {error}",
                        running.record.spec.callback.inbox
                    );
                }
                Ok(TaskTick::TerminalDeliveryDropped { error, attempts }) => {
                    eprintln!(
                        "warning: baton service dropped task {id} after {attempts} failed terminal-event deliveries to callback inbox {:?}: {error}",
                        running.record.spec.callback.inbox
                    );
                    finished.push(id.clone());
                }
                Err(err) => {
                    eprintln!("warning: baton service failed to tick task {id}: {err}");
                }
            }
        }
        for id in finished {
            tasks.remove(&id);
        }
    }

    /// Delivers one task lifecycle event to `record.spec.callback.inbox`,
    /// keyed by its deterministic [`task_event_id`] so the mailbox's own
    /// `done/`-membership dedup recognizes an exact redelivery.
    fn deliver_task_event(record: &TaskRecord, kind: TaskEventKind) -> Result<()> {
        let event_id = task_event_id(&record.id, kind);
        let body = match kind {
            TaskEventKind::Milestone { index } => TaskEventBody::milestone(&record.id, index),
            TaskEventKind::Terminal => TaskEventBody::terminal(
                &record.id,
                record.state,
                record.exit_code,
                record.elapsed_ms.unwrap_or(0),
            ),
        };
        let body_json = serde_json::to_string(&body)
            .map_err(|err| BatonError::Io(format!("could not serialize task event body: {err}")))?;
        // `to` is a delivery-target-agnostic identity tag only — the actual
        // routing is `record.spec.callback.inbox`, a mailbox root, exactly
        // like `SessionSpec::role` never resolves anything by itself.
        let to = record
            .spec
            .callback
            .role
            .clone()
            .unwrap_or_else(|| record.id.clone());
        let envelope = MessageEnvelope::new(
            event_id,
            record.id.clone(),
            "baton-task",
            to,
            MessageKind::Notify,
            body_json,
            crate::events::now_ms(),
        );
        mailbox::deliver_to(&record.spec.callback.inbox, &envelope)
    }

    // -- Task records -------------------------------------------------------

    fn task_record_path(control: &Path, id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(id) {
            return Err(BatonError::Io(format!(
                "task id is not usable as a filename: {id:?}"
            )));
        }
        Ok(tasks_dir(control).join(mailbox::file_name(id)))
    }

    fn write_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
        let dir = tasks_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let json = serde_json::to_string(record)
            .map_err(|err| BatonError::Io(format!("could not serialize task record: {err}")))?;
        mailbox::atomic_write(&dir, &mailbox::file_name(&record.id), &json)
    }

    fn read_task_record(control: &Path, id: &str) -> Result<Option<TaskRecord>> {
        let path = task_record_path(control, id)?;
        match fs::read_to_string(&path) {
            Ok(data) => serde_json::from_str(&data).map(Some).map_err(|err| {
                BatonError::Decode(format!("malformed task record {path:?}: {err}"))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
        }
    }

    fn remove_task_record(control: &Path, id: &str) -> Result<()> {
        let path = task_record_path(control, id)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!("could not remove {path:?}: {err}"))),
        }
    }

    fn list_task_records(control: &Path) -> Result<Vec<TaskRecord>> {
        let dir = tasks_dir(control);
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(BatonError::Io(format!("could not read {dir:?}: {err}"))),
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            if let Some(record) = read_task_record(control, &key)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    #[derive(Debug, PartialEq, Eq)]
    struct CleanupResidue {
        kind: &'static str,
        id: String,
        pid: u32,
        liveness: Liveness,
        argv: String,
    }

    fn task_recorded_argv(record: &TaskRecord) -> String {
        std::iter::once(record.spec.command.as_str())
            .chain(record.spec.args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn session_recorded_argv(record: &SessionRecord) -> String {
        serve_argv(&record.spec).join(" ")
    }

    /// Removes a task record that cleanup is done with, together with every
    /// admission artifact that refers to it. Shared by the reaper's two
    /// removal branches (terminal state, and corroborated-dead process) and
    /// by [`rescan_owned_tasks`], so the three cannot drift apart.
    fn remove_reaped_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
        remove_task_start_transaction(control, record)?;
        remove_task_record(control, &record.id)?;
        let _ = fs::remove_file(task_cancel_sentinel_path(control, &record.id));
        Ok(())
    }

    fn task_residue(record: &TaskRecord, liveness: Liveness) -> CleanupResidue {
        CleanupResidue {
            kind: "task",
            id: record.id.clone(),
            pid: record.pid,
            liveness,
            argv: task_recorded_argv(record),
        }
    }

    /// Cancels and reaps every task owned by `session_id`, regardless of
    /// each task's own callback target — the callback mailbox/role is a
    /// delivery target only, never the ownership or reaping boundary. Called
    /// from [`stop_session_record_with_wait`], so this runs on both
    /// `Stop <session>` and `Teardown` (which stops every session).
    /// Unresolved records survive unless `force` is set.
    ///
    /// `wait` is the grace-window sleep, injected so tests can drive the
    /// escalation ladder without the wall clock. It runs through
    /// [`AdmissionGuard::unlocked_wait`], so admission stays available for its
    /// duration; every mutation around it holds the lock.
    fn reap_session_tasks_with_wait(
        admission: &mut AdmissionGuard,
        session_id: &str,
        force: bool,
        wait: impl Fn(&TaskRecord, u64),
    ) -> Result<Vec<CleanupResidue>> {
        let control = admission.control();
        let mut residue = Vec::new();
        for record in list_task_records(control)? {
            if record.spec.session != session_id {
                continue;
            }
            if record.state != TaskState::Running {
                remove_reaped_task_record(control, &record)?;
                continue;
            }
            let mut record = record;
            upgrade_legacy_task_record(control, &mut record)?;
            let mut liveness = task_execution_liveness(&record);
            if force {
                if liveness != Liveness::Dead {
                    let _ = signal_group(record.pid, "-TERM");
                    let _ = signal_group(record.pid, "-KILL");
                }
                remove_reaped_task_record(control, &record)?;
                continue;
            }
            if liveness == Liveness::Unresolved {
                residue.push(task_residue(&record, liveness));
                continue;
            }
            let mut term_sent = false;
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                term_sent = true;
            }
            if liveness != Liveness::Dead {
                liveness = admission.unlocked_wait(|| {
                    wait(&record, KILL_GRACE_MS);
                    task_execution_liveness_after_retry(&record, KILL_GRACE_MS)
                })?;
            }
            if liveness == Liveness::Live && !term_sent {
                let _ = signal_group(record.pid, "-TERM");
                liveness = admission.unlocked_wait(|| {
                    wait(&record, KILL_GRACE_MS);
                    task_execution_liveness_after_retry(&record, KILL_GRACE_MS)
                })?;
            }
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-KILL");
                liveness = admission.unlocked_wait(|| {
                    wait(&record, KILL_GRACE_MS);
                    task_execution_liveness_after_retry(&record, KILL_GRACE_MS)
                })?;
            }
            if liveness == Liveness::Dead {
                remove_reaped_task_record(control, &record)?;
            } else {
                residue.push(task_residue(&record, liveness));
            }
        }
        Ok(residue)
    }

    /// Accounts for every task record owned by `session_id` that the reaper's
    /// snapshot could have missed, without ever releasing the admission lock.
    ///
    /// A task start admitted while [`AdmissionGuard::unlocked_wait`] had the
    /// lock released lands outside [`reap_session_tasks_with_wait`]'s
    /// listing. This pass re-lists under the still-held lock and closes that
    /// gap for every state
    /// such a record can be in by now — including a terminal one, since the
    /// supervisor can tick a racing task to `Completed`/`Failed`/`Cancelled`/
    /// `Timeout` before we look.
    ///
    /// It performs no waits, so nothing can be admitted while it runs and one
    /// pass suffices. A record still `Running` and `Live`/`Unresolved` is
    /// reported rather than put through the grace ladder: granting it grace
    /// would mean releasing the lock again and reopening the race. That costs
    /// nothing, because such a record can only exist on a path that already
    /// fails (see [`AdmissionGuard`]), so the stop exits non-zero, the session
    /// record is retained, and the next stop applies the full ladder. Under
    /// `force` it mirrors the reaper's force branch instead, so `--force`
    /// still leaves nothing behind.
    fn rescan_owned_tasks(
        admission: &AdmissionGuard,
        session_id: &str,
        force: bool,
        residue: &mut Vec<CleanupResidue>,
    ) -> Result<()> {
        let control = admission.control();
        for record in list_task_records(control)? {
            if record.spec.session != session_id {
                continue;
            }
            if residue.iter().any(|entry| entry.id == record.id) {
                continue;
            }
            if record.state != TaskState::Running {
                remove_reaped_task_record(control, &record)?;
                continue;
            }
            let mut record = record;
            upgrade_legacy_task_record(control, &mut record)?;
            // The same probe the reaper's first pass uses, so a record that
            // only this pass sees is judged identically. The non-retrying
            // form: this pass must never sleep.
            let liveness = task_execution_liveness(&record);
            if force {
                if liveness != Liveness::Dead {
                    let _ = signal_group(record.pid, "-TERM");
                    let _ = signal_group(record.pid, "-KILL");
                }
                remove_reaped_task_record(control, &record)?;
                continue;
            }
            if liveness == Liveness::Dead {
                remove_reaped_task_record(control, &record)?;
            } else {
                residue.push(task_residue(&record, liveness));
            }
        }
        Ok(())
    }

    // -- Task cancel sentinel -----------------------------------------------
    //
    // Mirrors `service.stop`/`serve.stop`: a per-task cooperative sentinel
    // `Cancel` drops before signalling, so `Run`'s own tick — the sole
    // writer of terminal state — can attribute the reap it later observes to
    // `cancelled` rather than misreading a `SIGTERM` exit as `failed`.

    fn task_cancel_sentinel_path(control: &Path, task_id: &str) -> std::path::PathBuf {
        task_cancel_dir(control).join(mailbox::file_name(task_id))
    }

    fn request_task_cancel_sentinel(control: &Path, task_id: &str) -> Result<()> {
        let dir = task_cancel_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        mailbox::atomic_write(&dir, &mailbox::file_name(task_id), "")
    }

    fn consume_task_cancel_sentinel(control: &Path, task_id: &str) -> Result<bool> {
        match fs::remove_file(task_cancel_sentinel_path(control, task_id)) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(BatonError::Io(format!(
                "could not consume task cancel sentinel: {err}"
            ))),
        }
    }

    // -- Liveness ---------------------------------------------------------

    /// Converts a canonical UTC `ps lstart` value into Unix epoch seconds.
    /// The weekday is intentionally ignored: `ps` supplies it for humans, but
    /// the calendar date and time are the identity-bearing fields.
    #[cfg(any(not(target_os = "linux"), test))]
    fn parse_lstart_epoch_secs(start_key: &str) -> Option<i64> {
        let fields: Vec<&str> = start_key.split_whitespace().collect();
        if fields.len() != 5 {
            return None;
        }
        let month = match fields[1] {
            "Jan" => 1,
            "Feb" => 2,
            "Mar" => 3,
            "Apr" => 4,
            "May" => 5,
            "Jun" => 6,
            "Jul" => 7,
            "Aug" => 8,
            "Sep" => 9,
            "Oct" => 10,
            "Nov" => 11,
            "Dec" => 12,
            _ => return None,
        };
        let day = fields[2].parse::<i64>().ok()?;
        let mut time = fields[3].split(':');
        let hour = time.next()?.parse::<i64>().ok()?;
        let minute = time.next()?.parse::<i64>().ok()?;
        let second = time.next()?.parse::<i64>().ok()?;
        if time.next().is_some() || !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
            return None;
        }
        if !(0..=59).contains(&second) {
            return None;
        }
        let year = fields[4].parse::<i64>().ok()?;
        let days = days_from_civil(year, month, day)?;
        days.checked_mul(86_400)?.checked_add(
            hour.checked_mul(3_600)?
                .checked_add(minute.checked_mul(60)?)?
                .checked_add(second)?,
        )
    }

    /// Returns the number of days from 1970-01-01 to a proleptic Gregorian
    /// date. The arithmetic is the civil-calendar conversion used by the
    /// parser rather than a platform date API, so it behaves identically in
    /// unit tests and on macOS.
    #[cfg(any(not(target_os = "linux"), test))]
    fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days_in_month = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => return None,
        };
        if !(1..=days_in_month).contains(&day) {
            return None;
        }
        let adjusted_year = year - if month <= 2 { 1 } else { 0 };
        let era = if adjusted_year >= 0 {
            adjusted_year / 400
        } else {
            (adjusted_year - 399) / 400
        };
        let year_of_era = adjusted_year - era * 400;
        let adjusted_month = month + if month > 2 { -3 } else { 9 };
        let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        Some(era * 146_097 + day_of_era - 719_468)
    }

    /// Parses the platform process-state token used by both the Linux
    /// `/proc` and macOS `ps` probes. Unknown states are incomplete evidence,
    /// not proof that a process is live or drained.
    fn parse_process_state(state: &str) -> Option<bool> {
        let bytes = state.as_bytes();
        let first = *bytes.first()?;
        #[cfg(target_os = "linux")]
        {
            if !matches!(
                first,
                b'R' | b'S'
                    | b'D'
                    | b'T'
                    | b't'
                    | b'Z'
                    | b'X'
                    | b'x'
                    | b'K'
                    | b'W'
                    | b'P'
                    | b'I'
                    | b'U'
            ) || bytes.len() != 1
            {
                return None;
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            if !matches!(first, b'R' | b'S' | b'I' | b'U' | b'T' | b'W' | b'Z')
                || bytes[1..]
                    .iter()
                    .any(|byte| !matches!(byte, b'<' | b'>' | b'N' | b'L' | b's' | b'l' | b'+'))
            {
                return None;
            }
        }
        Some(first == b'Z')
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, PartialEq, Eq)]
    struct ProcessProbe {
        state: String,
        start_key: String,
    }

    #[cfg(target_os = "linux")]
    impl ProcessProbe {
        fn is_zombie(&self) -> bool {
            self.state.starts_with('Z')
        }
    }

    /// Parses `/proc/<pid>/stat`; the executable name is `(comm)` and may
    /// itself contain `)` or whitespace, so fields are counted from the last
    /// `)` rather than split naively.
    #[cfg(target_os = "linux")]
    fn parse_linux_process_probe(stat: &str) -> Option<ProcessProbe> {
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        // `fields[0]` is field 3 (state) overall; starttime is field 22
        // overall, i.e. `fields[19]`.
        let state = fields.first()?;
        parse_process_state(state)?;
        Some(ProcessProbe {
            state: state.to_string(),
            start_key: fields.get(19)?.to_string(),
        })
    }

    #[cfg(target_os = "linux")]
    fn process_probe(pid: u32) -> ProbeResult<ProcessProbe> {
        if pid <= 1 {
            return ProbeResult::Gone;
        }
        #[cfg(all(test, not(target_os = "linux")))]
        note_process_probe();
        match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => parse_linux_process_probe(&stat)
                .map(ProbeResult::Present)
                .unwrap_or(ProbeResult::Unreadable),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => ProbeResult::Gone,
            Err(_) => ProbeResult::Unreadable,
        }
    }

    #[cfg(target_os = "linux")]
    fn process_argv(pid: u32) -> ProbeResult<Vec<String>> {
        match fs::read(format!("/proc/{pid}/cmdline")) {
            Ok(bytes) if !bytes.is_empty() => {
                let mut argv = Vec::new();
                for value in bytes
                    .split(|byte| *byte == 0)
                    .filter(|value| !value.is_empty())
                {
                    let Ok(value) = std::str::from_utf8(value) else {
                        return ProbeResult::Unreadable;
                    };
                    argv.push(value.to_string());
                }
                if argv.is_empty() {
                    ProbeResult::Unreadable
                } else {
                    ProbeResult::Present(argv)
                }
            }
            Ok(_) => ProbeResult::Unreadable,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => ProbeResult::Gone,
            Err(_) => ProbeResult::Unreadable,
        }
    }

    #[cfg(target_os = "linux")]
    fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
        match process_probe(pid) {
            ProbeResult::Present(probe) if !probe.is_zombie() => (Some(probe.start_key), None),
            _ => (None, None),
        }
    }

    /// Whether a freshly-spawned child's start key is trustworthy enough to
    /// persist. A missing key means the child was already gone or a zombie
    /// microseconds after `spawn()` — fail closed as a spawn failure.
    #[cfg(target_os = "linux")]
    fn spawn_start_key_ok(started_at: &Option<String>, _start_epoch_secs: &Option<i64>) -> bool {
        started_at.is_some()
    }

    #[cfg(target_os = "linux")]
    fn linux_session_argv_matches(actual: &[String], spec: &SessionSpec) -> bool {
        let expected = serve_argv(spec);
        actual.len() >= expected.len() && actual.ends_with(&expected)
    }

    #[cfg(target_os = "linux")]
    fn linux_task_argv_matches(actual: &[String], record: &TaskRecord) -> bool {
        let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
        expected.push(record.spec.command.clone());
        expected.extend(record.spec.args.iter().cloned());
        actual == expected
    }

    #[cfg(target_os = "linux")]
    fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
        match process_probe(record.pid) {
            ProbeResult::Gone => (Liveness::Dead, None),
            ProbeResult::Unreadable => (Liveness::Unresolved, None),
            ProbeResult::Present(probe) if probe.is_zombie() => (Liveness::Dead, None),
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => (Liveness::Live, None),
                Some(_) => (Liveness::Dead, None),
                None => match process_argv(record.pid) {
                    ProbeResult::Gone => (Liveness::Dead, None),
                    ProbeResult::Unreadable => (Liveness::Unresolved, None),
                    ProbeResult::Present(actual)
                        if linux_session_argv_matches(&actual, &record.spec) =>
                    {
                        (Liveness::Live, None)
                    }
                    ProbeResult::Present(_) => (Liveness::Dead, None),
                },
            },
        }
    }

    #[cfg(target_os = "linux")]
    fn is_session_alive(record: &SessionRecord) -> Liveness {
        session_liveness(record).0
    }

    #[cfg(target_os = "linux")]
    fn task_liveness(record: &TaskRecord) -> (Liveness, Option<i64>) {
        match process_probe(record.pid) {
            ProbeResult::Gone => (Liveness::Dead, None),
            ProbeResult::Unreadable => (Liveness::Unresolved, None),
            ProbeResult::Present(probe) if probe.is_zombie() => (Liveness::Dead, None),
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => (Liveness::Live, None),
                Some(_) => (Liveness::Dead, None),
                None => match process_argv(record.pid) {
                    ProbeResult::Gone => (Liveness::Dead, None),
                    ProbeResult::Unreadable => (Liveness::Unresolved, None),
                    ProbeResult::Present(actual) if linux_task_argv_matches(&actual, record) => {
                        (Liveness::Live, None)
                    }
                    ProbeResult::Present(_) => (Liveness::Unresolved, None),
                },
            },
        }
    }

    #[cfg(target_os = "linux")]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        task_liveness(record).0
    }

    /// A non-Linux Unix `ps` sample. macOS has no `/proc`, so `state`,
    /// `lstart`, and the untruncated command line are the available process
    /// corroborators. Every probe pins the locale and time zone so the
    /// canonical key is independent of the supervisor/client environment.
    #[cfg(not(target_os = "linux"))]
    #[derive(Debug, PartialEq, Eq)]
    struct ProcessProbe {
        state: String,
        start_key: String,
        start_epoch_secs: Option<i64>,
        command: String,
    }

    #[cfg(not(target_os = "linux"))]
    impl ProcessProbe {
        fn is_zombie(&self) -> bool {
            self.state.starts_with('Z')
        }
    }

    /// Parses one `ps -p <pid> -o state=,lstart=,command=` row. The five
    /// lstart fields are fixed by the C locale; the remaining text is the
    /// command line and is whitespace-normalized for comparison.
    #[cfg(not(target_os = "linux"))]
    fn parse_process_probe(output: &str) -> Option<ProcessProbe> {
        let fields: Vec<&str> = output.split_whitespace().collect();
        if fields.len() < 6 {
            return None;
        }
        let state = fields[0].to_string();
        parse_process_state(&state)?;
        let start_key = fields[1..6].join(" ");
        Some(ProcessProbe {
            state,
            start_epoch_secs: parse_lstart_epoch_secs(&start_key),
            start_key,
            command: fields[6..].join(" "),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn process_probe(pid: u32) -> ProbeResult<ProcessProbe> {
        if pid <= 1 {
            return ProbeResult::Gone;
        }
        #[cfg(all(test, not(target_os = "linux")))]
        note_process_probe();
        let output = match Command::new("ps")
            .args([
                "-ww",
                "-p",
                &pid.to_string(),
                "-o",
                "state=,lstart=,command=",
            ])
            // macOS formats `lstart` through the caller's locale and time
            // zone. Keep the durable process key independent of whether the
            // probe runs inside `service run` or a later CLI invocation.
            .env("LC_ALL", "C")
            .env("LC_TIME", "C")
            .env("TZ", "UTC")
            .output()
        {
            Ok(output) => output,
            Err(_) => return ProbeResult::Unreadable,
        };
        if !output.status.success() {
            return if output.stdout.is_empty() {
                ProbeResult::Gone
            } else {
                ProbeResult::Unreadable
            };
        }
        match std::str::from_utf8(&output.stdout)
            .ok()
            .and_then(parse_process_probe)
        {
            Some(probe) => ProbeResult::Present(probe),
            None => ProbeResult::Unreadable,
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn start_identity_from_probe(probe: &ProcessProbe) -> (Option<String>, Option<i64>) {
        if probe.is_zombie() {
            (None, None)
        } else {
            (Some(probe.start_key.clone()), probe.start_epoch_secs)
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
        match process_probe(pid) {
            ProbeResult::Present(probe) => start_identity_from_probe(&probe),
            _ => (None, None),
        }
    }

    /// A missing start key after spawn means the process was already gone or
    /// a zombie, so fail closed rather than persisting an uncorroborated PID.
    #[cfg(not(target_os = "linux"))]
    fn spawn_start_key_ok(started_at: &Option<String>, start_epoch_secs: &Option<i64>) -> bool {
        started_at.is_some() && start_epoch_secs.is_some()
    }

    #[cfg(not(target_os = "linux"))]
    fn normalize_process_text(value: &str) -> String {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[cfg(not(target_os = "linux"))]
    fn session_argv_matches(command: &str, spec: &SessionSpec) -> bool {
        let command = normalize_process_text(command);
        let expected = serve_argv(spec).join(" ");
        if expected.is_empty() {
            return false;
        }
        command == expected || command.ends_with(&format!(" {expected}"))
    }

    #[cfg(not(target_os = "linux"))]
    fn task_argv_matches(command: &str, record: &TaskRecord) -> bool {
        let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
        expected.push(record.spec.command.as_str());
        expected.extend(record.spec.args.iter().map(String::as_str));
        normalize_process_text(command) == normalize_process_text(&expected.join(" "))
    }

    #[cfg(not(target_os = "linux"))]
    fn task_instant_liveness(probe: &ProcessProbe, record: &TaskRecord) -> Liveness {
        let Some(started_ms) = record.started_ms else {
            return if task_argv_matches(&probe.command, record) {
                Liveness::Live
            } else {
                Liveness::Unresolved
            };
        };
        let Some(start_epoch_secs) = probe.start_epoch_secs else {
            return Liveness::Unresolved;
        };
        let delta = i128::from(started_ms) - i128::from(start_epoch_secs) * 1_000;
        const TASK_START_LATENCY_MS: i128 = 5_000;
        if delta < 0 {
            Liveness::Dead
        } else if delta < 1_000 + TASK_START_LATENCY_MS {
            Liveness::Live
        } else {
            Liveness::Unresolved
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
        match process_probe(record.pid) {
            ProbeResult::Gone => (Liveness::Dead, None),
            ProbeResult::Unreadable => (Liveness::Unresolved, None),
            ProbeResult::Present(probe) if probe.is_zombie() => (Liveness::Dead, None),
            ProbeResult::Present(probe) => match record.start_epoch_secs {
                Some(recorded) => match probe.start_epoch_secs {
                    Some(current) if current == recorded => (Liveness::Live, Some(current)),
                    Some(_) => (Liveness::Dead, None),
                    None => (Liveness::Unresolved, None),
                },
                None => match &record.started_at {
                    Some(recorded) if recorded == &probe.start_key => {
                        (Liveness::Live, probe.start_epoch_secs)
                    }
                    _ if probe.command.is_empty() => (Liveness::Unresolved, None),
                    _ if session_argv_matches(&probe.command, &record.spec) => {
                        (Liveness::Live, probe.start_epoch_secs)
                    }
                    _ => (Liveness::Dead, None),
                },
            },
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn is_session_alive(record: &SessionRecord) -> Liveness {
        session_liveness(record).0
    }

    #[cfg(not(target_os = "linux"))]
    fn task_liveness(record: &TaskRecord) -> (Liveness, Option<i64>) {
        match process_probe(record.pid) {
            ProbeResult::Gone => (Liveness::Dead, None),
            ProbeResult::Unreadable => (Liveness::Unresolved, None),
            ProbeResult::Present(probe) if probe.is_zombie() => (Liveness::Dead, None),
            ProbeResult::Present(probe) => match record.start_epoch_secs {
                Some(recorded) => match probe.start_epoch_secs {
                    Some(current) if current == recorded => (Liveness::Live, Some(current)),
                    Some(_) => (Liveness::Dead, None),
                    None => (Liveness::Unresolved, None),
                },
                None => match &record.started_at {
                    Some(recorded) if recorded == &probe.start_key => {
                        (Liveness::Live, probe.start_epoch_secs)
                    }
                    _ => {
                        let liveness = task_instant_liveness(&probe, record);
                        let epoch = (liveness == Liveness::Live)
                            .then_some(probe.start_epoch_secs)
                            .flatten();
                        (liveness, epoch)
                    }
                },
            },
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        task_liveness(record).0
    }

    /// Probes whether the task's process group still has any members. The
    /// group is the Unix ownership boundary, so a reaped leader does not mean
    /// the task is gone while a descendant remains in that group. `EPERM`
    /// still proves that a member exists; every other unexpected probe error
    /// is unresolved and therefore fails closed.
    fn task_group_liveness(pid: u32) -> Liveness {
        if pid <= 1 || pid > i32::MAX as u32 {
            return Liveness::Dead;
        }
        let result = unsafe { libc::kill(-(pid as libc::pid_t), 0) };
        match result {
            0 => group_scan_with_absence_recheck(pid),
            _ => match std::io::Error::last_os_error().raw_os_error() {
                Some(libc::ESRCH) => Liveness::Dead,
                Some(libc::EPERM) => group_scan_with_absence_recheck(pid),
                _ => Liveness::Unresolved,
            },
        }
    }

    /// A process can disappear while a strict group scan is reading the
    /// system process table. If a second kernel-level probe now proves that
    /// the group itself is gone, that is complete absence evidence; otherwise
    /// preserve `Unresolved` rather than turning an incomplete scan into
    /// `Dead`.
    fn group_scan_with_absence_recheck(pgid: u32) -> Liveness {
        let liveness = process_group_member_liveness(pgid);
        if liveness == Liveness::Unresolved {
            let result = unsafe { libc::kill(-(pgid as libc::pid_t), 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return Liveness::Dead;
            }
        }
        liveness
    }

    /// Confirms whether a process-group probe that succeeded with `kill(0)`
    /// has a non-zombie member. `kill(0)` also succeeds for zombie-only groups,
    /// which otherwise leaves an already-reaped direct child looking live
    /// forever during cleanup.
    #[cfg(target_os = "linux")]
    fn process_group_member_liveness(pgid: u32) -> Liveness {
        let entries = match fs::read_dir("/proc") {
            Ok(entries) => entries,
            Err(_) => return Liveness::Unresolved,
        };
        let mut found_member = false;
        let mut found_live_member = false;
        for entry in entries {
            let Ok(entry) = entry else {
                return Liveness::Unresolved;
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Liveness::Unresolved;
            };
            if !name.bytes().all(|byte| byte.is_ascii_digit()) {
                continue;
            }
            let Ok(pid) = name.parse::<u32>() else {
                return Liveness::Unresolved;
            };
            let stat = match fs::read_to_string(entry.path().join("stat")) {
                Ok(stat) => stat,
                Err(_) => return Liveness::Unresolved,
            };
            let Some((stat_pid, is_zombie, current_pgid)) = parse_linux_process_group_member(&stat)
            else {
                return Liveness::Unresolved;
            };
            if stat_pid != pid {
                return Liveness::Unresolved;
            }
            if current_pgid != pgid {
                continue;
            }
            found_member = true;
            if !is_zombie {
                found_live_member = true;
            }
        }
        if found_live_member {
            Liveness::Live
        } else if found_member {
            Liveness::Dead
        } else {
            // The group existed for the kill(0) sample but no member was
            // observable in the scan; retry rather than finalizing on a race.
            Liveness::Unresolved
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn process_group_member_liveness(pgid: u32) -> Liveness {
        let output = match Command::new("ps")
            .args(["-ww", "-axo", "pid=,pgid=,state="])
            .env("LC_ALL", "C")
            .env("LC_TIME", "C")
            .env("TZ", "UTC")
            .output()
        {
            Ok(output) if output.status.success() => output,
            _ => return Liveness::Unresolved,
        };
        if !output.stderr.is_empty() {
            return Liveness::Unresolved;
        }
        let Ok(output) = std::str::from_utf8(&output.stdout) else {
            return Liveness::Unresolved;
        };
        let mut found_member = false;
        let mut found_live_member = false;
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() != 3 {
                return Liveness::Unresolved;
            }
            let Some(pid) = fields[0].parse::<u32>().ok() else {
                return Liveness::Unresolved;
            };
            let Some(current_pgid) = fields[1].parse::<u32>().ok() else {
                return Liveness::Unresolved;
            };
            if pid == 0 || current_pgid == 0 || fields[2].is_empty() {
                return Liveness::Unresolved;
            }
            let Some(is_zombie) = parse_process_state(fields[2]) else {
                return Liveness::Unresolved;
            };
            if current_pgid != pgid {
                continue;
            }
            found_member = true;
            if !is_zombie {
                found_live_member = true;
            }
        }
        if found_live_member {
            Liveness::Live
        } else if found_member {
            Liveness::Dead
        } else {
            Liveness::Unresolved
        }
    }

    #[cfg(target_os = "linux")]
    fn parse_linux_process_group_member(stat: &str) -> Option<(u32, bool, u32)> {
        let pid = stat.split_once(' ')?.0.parse::<u32>().ok()?;
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        let state = fields.first()?;
        let is_zombie = parse_process_state(state)?;
        let pgid = fields.get(2)?.parse::<u32>().ok()?;
        Some((pid, is_zombie, pgid))
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TaskLeaderExit {
        Gone,
        MatchingZombie,
        Mismatched,
        NotExited,
        Unresolved,
    }

    #[cfg(target_os = "linux")]
    fn zombie_identity_matches(record: &TaskRecord, probe: &ProcessProbe) -> Option<bool> {
        record
            .started_at
            .as_deref()
            .map(|recorded| recorded == probe.start_key)
    }

    #[cfg(not(target_os = "linux"))]
    fn zombie_identity_matches(record: &TaskRecord, probe: &ProcessProbe) -> Option<bool> {
        if let Some(recorded) = record.start_epoch_secs {
            probe.start_epoch_secs.map(|current| current == recorded)
        } else {
            record
                .started_at
                .as_deref()
                .map(|recorded| recorded == probe.start_key)
        }
    }

    /// Returns whether the recorded direct PID is definitely gone (or is a
    /// zombie that cannot be the live task). A zombie may fall through to a
    /// process-group probe only after its start identity matches the durable
    /// record. A mismatched or legacy identity is never allowed to use the
    /// numeric PID as a group id.
    fn task_leader_exited(record: &TaskRecord) -> TaskLeaderExit {
        match process_probe(record.pid) {
            ProbeResult::Gone => TaskLeaderExit::Gone,
            ProbeResult::Present(probe) if probe.is_zombie() => {
                match zombie_identity_matches(record, &probe) {
                    Some(true) => TaskLeaderExit::MatchingZombie,
                    Some(false) => TaskLeaderExit::Mismatched,
                    None => TaskLeaderExit::Unresolved,
                }
            }
            ProbeResult::Present(_) => TaskLeaderExit::NotExited,
            ProbeResult::Unreadable => TaskLeaderExit::Unresolved,
        }
    }

    /// Combines the direct-PID identity with the Unix process-group boundary.
    /// A rehydrated task has no `Child` handle and its direct leader may have
    /// exited while descendants remain; that group is still live work. An
    /// unresolved group probe is retained rather than finalized or signalled.
    fn task_execution_liveness(record: &TaskRecord) -> Liveness {
        match is_task_alive(record) {
            Liveness::Dead => match task_leader_exited(record) {
                TaskLeaderExit::Gone | TaskLeaderExit::MatchingZombie => {
                    task_group_liveness(record.pid)
                }
                TaskLeaderExit::Mismatched | TaskLeaderExit::Unresolved => Liveness::Unresolved,
                TaskLeaderExit::NotExited => Liveness::Dead,
            },
            liveness => liveness,
        }
    }

    /// Persists the canonical epoch after a legacy macOS record is rescued by
    /// the fallback ladder. Callers must hold the admission lock; status and
    /// supervisor tick paths intentionally remain read-only.
    #[cfg(target_os = "macos")]
    fn upgrade_legacy_session_record(control: &Path, record: &mut SessionRecord) -> Result<()> {
        if record.start_epoch_secs.is_some() {
            return Ok(());
        }
        let (liveness, start_epoch_secs) = session_liveness(record);
        if liveness == Liveness::Live
            && let Some(start_epoch_secs) = start_epoch_secs
        {
            record.start_epoch_secs = Some(start_epoch_secs);
            write_session_record(control, record)?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    fn upgrade_legacy_session_record(_control: &Path, _record: &mut SessionRecord) -> Result<()> {
        Ok(())
    }

    /// Persists the canonical epoch after a legacy macOS task record is
    /// rescued by the fallback ladder. Callers must hold the admission lock;
    /// the supervisor's rehydration/tick paths deliberately do not rewrite.
    #[cfg(target_os = "macos")]
    fn upgrade_legacy_task_record(control: &Path, record: &mut TaskRecord) -> Result<()> {
        if record.start_epoch_secs.is_some() {
            return Ok(());
        }
        let (liveness, start_epoch_secs) = task_liveness(record);
        if liveness == Liveness::Live
            && let Some(start_epoch_secs) = start_epoch_secs
        {
            record.start_epoch_secs = Some(start_epoch_secs);
            write_task_record(control, record)?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    fn upgrade_legacy_task_record(_control: &Path, _record: &mut TaskRecord) -> Result<()> {
        Ok(())
    }

    /// Builds the argv for sending `sig` (e.g. `"-TERM"`) to the process
    /// **group** led by `pid`. The `--` is required: procps-ng otherwise parses
    /// the negative process-group id as another option and can turn
    /// `kill -TERM -<pid>` into `kill(-1, SIGTERM)`, signalling every process
    /// the invoking user owns.
    fn signal_group_arguments(pid: u32, sig: &str) -> Option<[String; 3]> {
        if pid <= 1 {
            return None;
        }
        Some([sig.to_string(), "--".to_string(), format!("-{pid}")])
    }

    /// Sends `sig` to the process **group** led by `pid`. A failure (the group
    /// is already gone) is not surfaced — only a failure to run `kill` itself
    /// is.
    fn signal_group(pid: u32, sig: &str) -> Result<()> {
        let Some(args) = signal_group_arguments(pid, sig) else {
            return Ok(());
        };
        Command::new("kill")
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|_| ())
            .map_err(|err| BatonError::Io(format!("could not run kill {sig} -{pid}: {err}")))
    }

    fn wait_while_alive(record: &SessionRecord, grace_ms: u64) {
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        while is_session_alive(record) != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    fn wait_while_task_alive(record: &TaskRecord, grace_ms: u64) {
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        while task_execution_liveness(record) != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Retries an incomplete Unix process-group scan for one bounded grace
    /// period. An unresolved result is never treated as permission to signal;
    /// this only gives a transient `/proc` or `ps` snapshot a chance to become
    /// complete before a cancellation or escalation decision is made.
    fn task_execution_liveness_after_retry(record: &TaskRecord, grace_ms: u64) -> Liveness {
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        loop {
            let liveness = task_execution_liveness(record);
            if liveness != Liveness::Unresolved || Instant::now() >= deadline {
                return liveness;
            }
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Stops one session. The caller passes the held admission lock, which
    /// this releases across each grace wait and re-acquires afterwards:
    /// cooperative `serve --stop` on its inbox first,
    /// bounded wait, then `SIGTERM`/`SIGKILL` process-group escalation if
    /// still alive, then reaps every task this session owns
    /// ([`reap_session_tasks_with_wait`]) and removes the session's own
    /// durable record. Idempotent — a session already gone just gets its (possibly
    /// already-absent) record, and its tasks', cleaned up. Returns any
    /// records retained because their identity remained unresolved.
    fn stop_session_record(
        admission: &mut AdmissionGuard,
        record: &SessionRecord,
        force: bool,
    ) -> Result<Vec<CleanupResidue>> {
        stop_session_record_with_wait(
            admission,
            record,
            force,
            wait_while_alive,
            wait_while_task_alive,
        )
    }

    /// [`stop_session_record`] with both grace waits injectable, so tests can
    /// drive the racing-admission paths deterministically instead of against
    /// the wall clock. Production goes through the wrapper above.
    fn stop_session_record_with_wait(
        admission: &mut AdmissionGuard,
        record: &SessionRecord,
        force: bool,
        session_wait: impl Fn(&SessionRecord, u64),
        task_wait: impl Fn(&TaskRecord, u64),
    ) -> Result<Vec<CleanupResidue>> {
        let control = admission.control();
        let mut record = record.clone();
        upgrade_legacy_session_record(control, &mut record)?;
        // Claimed before the first `unlocked_wait`, so admission can tell a
        // still-live owner that is nonetheless committed to stopping.
        let _stopping = SessionStopGuard::claim(control, &record.id)?;
        let _ = mailbox::request_stop(&record.spec.inbox);
        let mut liveness = is_session_alive(&record);
        if force {
            if liveness != Liveness::Dead {
                let _ = signal_group(record.pid, "-TERM");
                let _ = signal_group(record.pid, "-KILL");
            }
            liveness = Liveness::Dead;
        } else {
            admission.unlocked_wait(|| session_wait(&record, STOP_GRACE_MS))?;
            liveness = is_session_alive(&record);
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                admission.unlocked_wait(|| session_wait(&record, KILL_GRACE_MS))?;
                liveness = is_session_alive(&record);
                if liveness == Liveness::Live {
                    let _ = signal_group(record.pid, "-KILL");
                    admission.unlocked_wait(|| session_wait(&record, KILL_GRACE_MS))?;
                    liveness = is_session_alive(&record);
                }
            }
        }
        let mut residue = reap_session_tasks_with_wait(admission, &record.id, force, task_wait)?;
        // From here to the session-record decision the admission lock is held
        // without interruption, so nothing can be admitted between the rescan
        // and `remove_session_record`.
        rescan_owned_tasks(admission, &record.id, force, &mut residue)?;
        if liveness == Liveness::Dead && residue.is_empty() {
            remove_session_record(control, &record.id)?;
        } else if liveness != Liveness::Dead {
            residue.push(CleanupResidue {
                kind: "session",
                id: record.id.clone(),
                pid: record.pid,
                liveness,
                argv: session_recorded_argv(&record),
            });
        }
        Ok(residue)
    }

    // -- Session records ---------------------------------------------------

    fn sessions_dir(control: &Path) -> std::path::PathBuf {
        control.join("sessions")
    }

    fn session_logs_dir(control: &Path, session_id: &str) -> std::path::PathBuf {
        sessions_dir(control).join(session_id)
    }

    fn session_record_path(control: &Path, id: &str) -> Result<std::path::PathBuf> {
        if !mailbox::is_safe_key(id) {
            return Err(BatonError::Io(format!(
                "session id is not usable as a filename: {id:?}"
            )));
        }
        Ok(sessions_dir(control).join(mailbox::file_name(id)))
    }

    fn write_session_record(control: &Path, record: &SessionRecord) -> Result<()> {
        let dir = sessions_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let json = serde_json::to_string(record)
            .map_err(|err| BatonError::Io(format!("could not serialize session record: {err}")))?;
        mailbox::atomic_write(&dir, &mailbox::file_name(&record.id), &json)
    }

    fn read_session_record(control: &Path, id: &str) -> Result<Option<SessionRecord>> {
        let path = session_record_path(control, id)?;
        match fs::read_to_string(&path) {
            Ok(data) => serde_json::from_str(&data).map(Some).map_err(|err| {
                BatonError::Decode(format!("malformed session record {path:?}: {err}"))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
        }
    }

    fn remove_session_record(control: &Path, id: &str) -> Result<()> {
        let path = session_record_path(control, id)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!("could not remove {path:?}: {err}"))),
        }
    }

    fn list_session_records(control: &Path) -> Result<Vec<SessionRecord>> {
        let dir = sessions_dir(control);
        let entries = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(BatonError::Io(format!("could not read {dir:?}: {err}"))),
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &dir)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            if let Some(record) = read_session_record(control, &key)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    // -- CLI-facing operations ---------------------------------------------

    #[derive(Serialize)]
    struct SessionStatusView<'a> {
        id: &'a str,
        pid: u32,
        live: bool,
        liveness: Liveness,
        inbox: &'a str,
        stderr_path: &'a str,
    }

    #[derive(Serialize)]
    struct ServiceStatusView<'a> {
        service_running: bool,
        control: String,
        sessions: Vec<SessionStatusView<'a>>,
    }

    fn execute_status(control: &Path, session: Option<&str>, mut out: impl Write) -> Result<()> {
        let service_running = probe_control(control)? == ControlLiveness::Live;
        let records = match session {
            Some(id) => read_session_record(control, id)?.into_iter().collect(),
            None => list_session_records(control)?,
        };
        let sessions = records
            .iter()
            .map(|record| {
                let liveness = is_session_alive(record);
                SessionStatusView {
                    id: &record.id,
                    pid: record.pid,
                    live: liveness.is_live(),
                    liveness,
                    inbox: &record.spec.inbox,
                    stderr_path: &record.stderr_path,
                }
            })
            .collect();
        let view = ServiceStatusView {
            service_running,
            control: control.display().to_string(),
            sessions,
        };
        let json = serde_json::to_string(&view)
            .map_err(|err| BatonError::Io(format!("could not serialize service status: {err}")))?;
        writeln!(out, "{json}").map_err(io_err)
    }

    fn report_cleanup_residue(residue: &[CleanupResidue]) {
        for item in residue {
            let liveness = match item.liveness {
                Liveness::Live => "live",
                Liveness::Dead => "dead",
                Liveness::Unresolved => "unresolved",
            };
            eprintln!(
                "baton cleanup kept {}: id={} pid={} liveness={} recorded_argv={:?}",
                item.kind, item.id, item.pid, liveness, item.argv
            );
        }
    }

    fn execute_stop(control: &Path, session: &str, force: bool, mut out: impl Write) -> Result<()> {
        let mut admission = AdmissionGuard::acquire(control)?;
        match read_session_record(control, session)? {
            Some(record) => {
                let residue = stop_session_record(&mut admission, &record, force)?;
                if !residue.is_empty() {
                    report_cleanup_residue(&residue);
                    return Err(BatonError::Io(format!(
                        "session {session} remains live or unresolved; use --force to assert its identity"
                    )));
                }
                writeln!(out, "stopped session {session}").map_err(io_err)
            }
            None => writeln!(
                out,
                "no session {session:?} on {}; nothing to stop",
                control.display()
            )
            .map_err(io_err),
        }
    }

    #[derive(Serialize)]
    struct TaskStatusView<'a> {
        id: &'a str,
        session: &'a str,
        pid: u32,
        state: TaskState,
        live: bool,
        liveness: Liveness,
        exit_code: Option<i32>,
        elapsed_ms: Option<u64>,
        command: &'a str,
        stdout_path: &'a str,
        stderr_path: &'a str,
    }

    #[derive(Serialize)]
    struct TaskStatusReport<'a> {
        control: String,
        tasks: Vec<TaskStatusView<'a>>,
    }

    fn execute_task_status(control: &Path, task: Option<&str>, mut out: impl Write) -> Result<()> {
        let records: Vec<TaskRecord> = match task {
            Some(id) => read_task_record(control, id)?.into_iter().collect(),
            None => list_task_records(control)?,
        };
        let tasks = records
            .iter()
            .map(|record| {
                let liveness = if record.state == TaskState::Running {
                    task_execution_liveness(record)
                } else {
                    Liveness::Dead
                };
                TaskStatusView {
                    id: &record.id,
                    session: &record.spec.session,
                    pid: record.pid,
                    state: record.state,
                    live: liveness.is_live(),
                    liveness,
                    exit_code: record.exit_code,
                    elapsed_ms: record.elapsed_ms,
                    command: &record.spec.command,
                    stdout_path: &record.stdout_path,
                    stderr_path: &record.stderr_path,
                }
            })
            .collect();
        let report = TaskStatusReport {
            control: control.display().to_string(),
            tasks,
        };
        let json = serde_json::to_string(&report)
            .map_err(|err| BatonError::Io(format!("could not serialize task status: {err}")))?;
        writeln!(out, "{json}").map_err(io_err)
    }

    /// Cancels one task: idempotent — a task already terminal (or unknown)
    /// is a no-op success. Acts directly on the durable [`TaskRecord`]'s
    /// PID, exactly like [`stop_session_record`] does for a session, so this
    /// works even when `Run` is not currently alive; `Run`'s own tick (if
    /// alive) still performs the actual terminal-state write and event
    /// delivery once it observes the exit.
    fn cancel_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
        if record.state != TaskState::Running {
            return Ok(());
        }
        request_task_cancel_sentinel(control, &record.id)?;
        let mut liveness = task_execution_liveness_after_retry(record, KILL_GRACE_MS);
        if liveness == Liveness::Live {
            let _ = signal_group(record.pid, "-TERM");
            wait_while_task_alive(record, KILL_GRACE_MS);
            liveness = task_execution_liveness_after_retry(record, KILL_GRACE_MS);
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-KILL");
                wait_while_task_alive(record, KILL_GRACE_MS);
            }
        }
        Ok(())
    }

    fn execute_task_cancel(control: &Path, task: &str, mut out: impl Write) -> Result<()> {
        match read_task_record(control, task)? {
            Some(record) => {
                cancel_task_record(control, &record)?;
                writeln!(out, "cancelled task {task}").map_err(io_err)
            }
            None => writeln!(
                out,
                "no task {task:?} on {}; nothing to cancel",
                control.display()
            )
            .map_err(io_err),
        }
    }

    fn execute_teardown(control: &Path, force: bool, mut out: impl Write) -> Result<()> {
        let service_liveness = request_control_stop(control)?;
        if service_liveness == ControlLiveness::Live {
            wait_for_control_release(control)?;
        }
        let mut admission = AdmissionGuard::acquire(control)?;
        let mut residue = Vec::new();
        for record in list_session_records(control)? {
            residue.extend(stop_session_record(&mut admission, &record, force)?);
        }
        let result = match service_liveness {
            ControlLiveness::Live => writeln!(
                out,
                "requested teardown of baton service on {}",
                control.display()
            ),
            ControlLiveness::NotRunning => writeln!(
                out,
                "no running baton service on {}; sessions reaped",
                control.display()
            ),
        }
        .map_err(io_err);
        if !residue.is_empty() {
            report_cleanup_residue(&residue);
            return Err(BatonError::Io(format!(
                "{} managed record(s) remain live or unresolved; use --force to assert their identities",
                residue.len()
            )));
        }
        result
    }

    /// Waits until the supervisor has observed its stop request and released
    /// the control lock. A released lock is the admission barrier: every
    /// request handled by the supervisor was committed to a session record
    /// before this point, and later `service start` calls fail fast.
    fn wait_for_control_release(control: &Path) -> Result<()> {
        while probe_control(control)? == ControlLiveness::Live {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // Serializes every test in this module that either holds the
        // control-plane flock directly or forks a real child process
        // (`spawn_task_child`/`Mailbox::open`'s own lock) against the rest of
        // the crate's forks. The guard is crate-wide rather than module-local
        // because the fd table it protects is process-wide: a spawn from any
        // other module's tests is just as capable of pinning this module's
        // locks open. See `crate::test_support`.
        //
        // "Forks a real child" includes the *indirect* forks a liveness check
        // performs off Linux: `process_probe` shells out to `ps` and
        // `signal_group` to `kill`, so `execute_status`/`execute_stop`/
        // `execute_teardown`/`reconcile_task_admissions` and friends fork on
        // macOS even when the test itself never spawns anything. Every test
        // that can reach one of those takes the guard.
        use crate::test_support::serialize_forks_and_locks;

        struct TempDir {
            path: std::path::PathBuf,
        }

        impl TempDir {
            fn new(tag: &str) -> Self {
                let path = std::env::temp_dir().join(format!(
                    "baton-service-{}-{}-{}",
                    std::process::id(),
                    SEQ.fetch_add(1, Ordering::Relaxed),
                    tag
                ));
                let _ = fs::remove_dir_all(&path);
                fs::create_dir_all(&path).expect("create temp control dir");
                Self { path }
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.path);
            }
        }

        fn spec(inbox: &str, outbox: &str) -> SessionSpec {
            SessionSpec {
                schema: SESSION_SPEC_SCHEMA.to_string(),
                inbox: inbox.to_string(),
                outbox: outbox.to_string(),
                poll_ms: None,
                agent_cmd: None,
                agent_args: Vec::new(),
                agent_cwd: None,
                agent_timeout_ms: None,
                agent_output: None,
                agent_result_key: None,
                role: None,
            }
        }

        fn task_spec(
            session: &str,
            command: &str,
            args: Vec<String>,
            milestones_ms: Vec<u64>,
            max_duration_ms: u64,
            callback_inbox: &str,
        ) -> TaskSpec {
            TaskSpec {
                schema: crate::task::TASK_SPEC_SCHEMA.to_string(),
                session: session.to_string(),
                command: command.to_string(),
                args,
                cwd: None,
                env: Vec::new(),
                milestones_ms,
                max_duration_ms,
                callback: TaskCallback {
                    inbox: callback_inbox.to_string(),
                    role: None,
                },
            }
        }

        /// Spawns `spec` under `dir` and wraps it as a durably-recorded
        /// [`RunningTask`], mirroring what `handle_task_start_request` does
        /// inside the real request protocol — but callable directly, so a
        /// test can drive [`tick_one_task`] without going through the
        /// request-file dance or the infinite `run_service` loop.
        fn spawn_running_task(
            dir: &Path,
            id: &str,
            spec: TaskSpec,
            clock: &dyn Clock,
        ) -> RunningTask {
            let log_dir = task_logs_dir(dir, id);
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let child = spawn_task_child(&spec, &stdout_path, &stderr_path).expect("spawn task");
            let pid = child.id();
            let started_ms = clock.now_ms();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            let record = TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid,
                started_at,
                start_epoch_secs,
                job: None,
                started_ms: Some(started_ms),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            write_task_record(dir, &record).expect("write task record");
            RunningTask {
                record,
                child: Some(child),
                #[cfg(not(target_os = "linux"))]
                rehydrated_liveness: None,
                started_ms,
                term_sent_at_ms: None,
                kill_sent: false,
                terminal_delivery_attempts: 0,
                next_terminal_retry_ms: None,
                terminal_retry_delay_ms: 0,
            }
        }

        /// Builds a terminal task without a live child so callback delivery
        /// retry behavior can be driven entirely by [`FakeClock`].
        fn terminal_running_task(
            dir: &Path,
            id: &str,
            callback_inbox: &Path,
            clock: &dyn Clock,
        ) -> RunningTask {
            let spec = task_spec(
                "svc-1",
                "true",
                vec![],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let record = TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid: 0,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: Some(clock.now_ms()),
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(10),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(dir, &record).expect("write terminal task record");
            RunningTask {
                record,
                child: None,
                #[cfg(not(target_os = "linux"))]
                rehydrated_liveness: None,
                started_ms: clock.now_ms(),
                term_sent_at_ms: None,
                kill_sent: false,
                terminal_delivery_attempts: 0,
                next_terminal_retry_ms: None,
                terminal_retry_delay_ms: 0,
            }
        }

        /// A child spawned while `Run` owns the control lock must not retain
        /// that lock after the supervisor exits. Otherwise a killed
        /// supervisor cannot be restarted until every descendant happens to
        /// exit, which breaks crash recovery on macOS.
        #[test]
        fn spawned_task_does_not_retain_control_lock_after_owner_exits() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("lock-inheritance");
            let owner_lock = acquire_control_lock(&dir.path).expect("owner lock");

            let log_dir = task_logs_dir(&dir.path, "lock-child");
            fs::create_dir_all(&log_dir).expect("create child log dir");
            let mut child = spawn_task_child(
                &task_spec(
                    "session",
                    "sh",
                    vec!["-c".to_string(), "sleep 30".to_string()],
                    Vec::new(),
                    60_000,
                    "/tmp/callback",
                ),
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn lock inheritance probe child");

            drop(owner_lock);
            let deadline = Instant::now() + Duration::from_secs(10);
            let replacement_lock = loop {
                match acquire_control_lock(&dir.path) {
                    Ok(lock) => break lock,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(err) => {
                        panic!("descendant must not retain the owner control lock: {err:?}")
                    }
                }
            };
            drop(replacement_lock);

            let _ = child.kill();
            let _ = child.wait();
        }

        /// A session record round-trips through the atomic-write file
        /// protocol byte-for-byte.
        #[test]
        fn session_record_round_trips() {
            let dir = TempDir::new("record");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: 4242,
                started_at: Some("123456".to_string()),
                start_epoch_secs: Some(123456),
                stderr_path: "/tmp/stderr.log".to_string(),
            };
            write_session_record(&dir.path, &record).expect("write");
            let read = read_session_record(&dir.path, "svc-1")
                .expect("read")
                .expect("present");
            assert_eq!(read, record);
        }

        /// A record written before session stderr capture was added remains
        /// readable and reports an empty path rather than failing status or
        /// cleanup.
        #[test]
        fn legacy_session_record_defaults_stderr_path() {
            let dir = TempDir::new("legacy-record");
            let record = SessionRecord {
                id: "svc-legacy".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: 4242,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            let mut json = serde_json::to_value(record).expect("serialize record");
            json.as_object_mut()
                .expect("record object")
                .remove("stderr_path");
            fs::create_dir_all(sessions_dir(&dir.path)).expect("create sessions dir");
            fs::write(
                session_record_path(&dir.path, "svc-legacy").expect("record path"),
                serde_json::to_vec(&json).expect("serialize legacy record"),
            )
            .expect("write legacy record");

            let read = read_session_record(&dir.path, "svc-legacy")
                .expect("read legacy record")
                .expect("legacy record exists");
            assert!(read.stderr_path.is_empty());
        }

        /// A legacy macOS task rescued by its spawn instant is upgraded once
        /// on a lock-holding cleanup path. Session argv rescue is covered by
        /// the real-binary integration fixture.
        #[cfg(target_os = "macos")]
        #[test]
        fn legacy_macos_task_record_upgrades_after_instant_rescue() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("legacy-macos-upgrade");

            let task_specification = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                Vec::new(),
                60_000,
                &dir.path.join("callback").display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-legacy");
            fs::create_dir_all(&log_dir).expect("create task logs");
            let mut task_child = spawn_task_child(
                &task_specification,
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn task child");
            let task_record = TaskRecord {
                id: "task-legacy".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Responded,
                spec: task_specification,
                pid: task_child.id(),
                started_at: Some("legacy-lstart".to_string()),
                start_epoch_secs: None,
                job: None,
                started_ms: Some(SystemClock.now_ms()),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: log_dir.join("stdout.log").display().to_string(),
                stderr_path: log_dir.join("stderr.log").display().to_string(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &task_record).expect("write legacy task");
            let mut upgraded_task = task_record.clone();
            upgrade_legacy_task_record(&dir.path, &mut upgraded_task).expect("upgrade legacy task");
            assert!(
                upgraded_task.start_epoch_secs.is_some(),
                "instant rescue populates the canonical task epoch"
            );
            assert_eq!(
                read_task_record(&dir.path, "task-legacy")
                    .expect("read upgraded task")
                    .expect("upgraded task exists")
                    .start_epoch_secs,
                upgraded_task.start_epoch_secs
            );

            signal_group(task_child.id(), "-KILL").expect("kill task child");
            task_child.wait().expect("wait for task child");
        }

        /// A session id absent from `sessions/` reads as `None`, not an error.
        #[test]
        fn read_session_record_absent_is_none() {
            let dir = TempDir::new("absent");
            assert!(
                read_session_record(&dir.path, "nope")
                    .expect("read")
                    .is_none()
            );
        }

        /// Missing and stale session owners are rejected before task log or
        /// process creation, and the submitting client receives the owner
        /// error through the task-start response.
        #[test]
        fn task_start_rejects_missing_or_dead_session_before_spawn() {
            let _guard = serialize_forks_and_locks();
            for (tag, session, session_record) in [
                ("owner-absent", "svc-missing", None),
                ("owner-unsafe", "../svc-unsafe", None),
                (
                    "owner-dead",
                    "svc-dead",
                    Some(SessionRecord {
                        id: "svc-dead".to_string(),
                        spec: spec("/tmp/in", "/tmp/out"),
                        pid: u32::MAX - 1,
                        started_at: Some("not-current".to_string()),
                        start_epoch_secs: None,
                        stderr_path: String::new(),
                    }),
                ),
            ] {
                let dir = TempDir::new(tag);
                if let Some(record) = session_record {
                    write_session_record(&dir.path, &record).expect("write stale session");
                }
                let spec_path = dir.path.join("task-request.json");
                let task_spec = task_spec(session, "true", vec![], vec![], 1_000, "/tmp/callback");
                fs::write(
                    &spec_path,
                    serde_json::to_string(&task_spec).expect("serialize task spec"),
                )
                .expect("write task spec");

                let outcome = handle_task_start_request(
                    &dir.path,
                    "reject-request",
                    &spec_path,
                    &FakeClock::new(),
                )
                .expect("owner rejection is a handled response");
                assert!(outcome.is_none(), "rejected owner must not return a task");
                assert!(
                    !dir.path.join("tasks").exists(),
                    "rejected owner must not create a task record directory"
                );
                assert!(
                    !dir.path.join("task-logs").exists(),
                    "rejected owner must not create task logs"
                );

                let response_path =
                    task_responses_dir(&dir.path).join(mailbox::file_name("reject-request"));
                let response: TaskStartResponse = serde_json::from_str(
                    &fs::read_to_string(response_path).expect("owner rejection response"),
                )
                .expect("decode owner rejection response");
                assert!(response.task_id.is_none());
                assert!(response
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("does not name a live managed session")));
            }
        }

        /// A task command that cannot be spawned is answered through the
        /// task-start response, so the submitting client fails with the
        /// spawn reason instead of waiting out `START_AWAIT_MS`.
        #[test]
        fn task_start_spawn_failure_answers_with_error_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-spawn-failure");
            let pid = std::process::id();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            write_session_record(
                &dir.path,
                &SessionRecord {
                    id: "svc-live".to_string(),
                    spec: spec("/tmp/in", "/tmp/out"),
                    pid,
                    started_at,
                    start_epoch_secs,
                    stderr_path: String::new(),
                },
            )
            .expect("write live session record");

            let unspawnable = dir.path.join("no-such-binary").display().to_string();
            let spec_path = dir.path.join("task-request.json");
            let task_spec = task_spec(
                "svc-live",
                &unspawnable,
                vec![],
                vec![],
                1_000,
                "/tmp/callback",
            );
            fs::write(
                &spec_path,
                serde_json::to_string(&task_spec).expect("serialize task spec"),
            )
            .expect("write task spec");

            let outcome =
                handle_task_start_request(&dir.path, "spawn-fail", &spec_path, &FakeClock::new())
                    .expect("spawn failure is a handled response, not a request-loop error");
            assert!(outcome.is_none(), "a failed spawn returns no task");
            assert!(
                !dir.path.join("tasks").exists(),
                "a failed spawn leaves no task record"
            );
            assert!(
                fs::read_dir(dir.path.join("task-logs"))
                    .map(|entries| entries.count() == 0)
                    .unwrap_or(true),
                "a failed spawn leaves no orphan task log directory"
            );

            let response_path =
                task_responses_dir(&dir.path).join(mailbox::file_name("spawn-fail"));
            let response: TaskStartResponse =
                serde_json::from_str(&fs::read_to_string(response_path).expect("spawn response"))
                    .expect("decode spawn response");
            assert!(response.task_id.is_none());
            let error = response.error.expect("spawn failure carries an error");
            assert!(
                error.contains("could not spawn task command") && error.contains(&unspawnable),
                "the response names the spawn failure: {error}"
            );
        }

        /// A session admission failure after the request is claimed is
        /// answered through the start response rather than propagated to the
        /// request loop, where it would strand the client on the await bound.
        #[test]
        fn start_request_admission_failure_answers_with_error_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("session-admission-failure");
            // A file where `sessions/` must be a directory, so the record
            // write fails — the last admission step before the success
            // response. Which named failure the handler reaches first is
            // platform-dependent: where the post-spawn corroborator cannot
            // read a just-spawned child's start key (observed on macOS), the
            // earlier corroboration step rejects instead. Both are named
            // post-claim admission failures, and the contract under test is
            // the same for either — an error response, not a propagated
            // `Err` the client would only see as a timeout.
            fs::write(sessions_dir(&dir.path), b"not a directory").expect("block sessions dir");

            let spec_path = dir.path.join("session-request.json");
            fs::write(
                &spec_path,
                serde_json::to_string(&spec(
                    &dir.path.join("in").display().to_string(),
                    &dir.path.join("out").display().to_string(),
                ))
                .expect("serialize session spec"),
            )
            .expect("write session spec");

            let outcome = handle_start_request(&dir.path, "session-fail", &spec_path)
                .expect("admission failure is a handled response, not a request-loop error");
            assert!(outcome.is_none(), "a failed admission returns no session");

            let response_path = responses_dir(&dir.path).join(mailbox::file_name("session-fail"));
            let response: StartResponse =
                serde_json::from_str(&fs::read_to_string(response_path).expect("start response"))
                    .expect("decode start response");
            assert!(response.session_id.is_none());
            let error = response.error.expect("the response carries an error");
            assert!(
                error.contains("sessions") || error.contains("could not be corroborated"),
                "the response names a post-claim admission failure: {error}"
            );
            assert!(
                list_session_records(&dir.path)
                    .unwrap_or_default()
                    .is_empty(),
                "a failed admission persists no session record"
            );
        }

        /// The start-response client surfaces an error response immediately,
        /// and still reads a response persisted before `error` existed.
        #[test]
        fn await_start_response_surfaces_error_and_legacy_bodies() {
            let dir = TempDir::new("start-response-shapes");
            let responses = responses_dir(&dir.path);
            fs::create_dir_all(&responses).expect("create responses dir");

            fs::write(
                responses.join(mailbox::file_name("failed")),
                serde_json::to_string(&StartResponse {
                    session_id: None,
                    error: Some("could not spawn baton serve: nope".to_string()),
                })
                .expect("serialize error response"),
            )
            .expect("write error response");
            let err = await_start_response(&dir.path, "failed").expect_err("error response fails");
            assert!(
                err.to_string()
                    .contains("could not spawn baton serve: nope"),
                "the client error names the admission failure: {err}"
            );

            fs::write(
                responses.join(mailbox::file_name("legacy")),
                r#"{"session_id":"svc-legacy"}"#,
            )
            .expect("write legacy response");
            assert_eq!(
                await_start_response(&dir.path, "legacy").expect("legacy response parses"),
                "svc-legacy"
            );
        }

        /// An unsafe session id is rejected rather than escaping the control
        /// root, on both the read and remove paths.
        #[test]
        fn unsafe_session_id_is_rejected() {
            let dir = TempDir::new("unsafe");
            assert!(read_session_record(&dir.path, "../escape").is_err());
            assert!(remove_session_record(&dir.path, "../escape").is_err());
        }

        /// `list_session_records` on a control root with no `sessions/` yet
        /// is empty, not an error.
        #[test]
        fn list_session_records_on_absent_dir_is_empty() {
            let dir = TempDir::new("list-absent");
            assert!(list_session_records(&dir.path).expect("list").is_empty());
        }

        /// `list_session_records` returns every written record.
        #[test]
        fn list_session_records_returns_all() {
            let dir = TempDir::new("list");
            for i in 0..3 {
                let record = SessionRecord {
                    id: format!("svc-{i}"),
                    spec: spec("/tmp/in", "/tmp/out"),
                    pid: 1000 + i,
                    started_at: None,
                    start_epoch_secs: None,
                    stderr_path: String::new(),
                };
                write_session_record(&dir.path, &record).expect("write");
            }
            let mut ids: Vec<String> = list_session_records(&dir.path)
                .expect("list")
                .into_iter()
                .map(|r| r.id)
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["svc-0", "svc-1", "svc-2"]);
        }

        /// A removed record is gone from a subsequent list/read, and removing
        /// an already-absent record is a no-op success (idempotent).
        #[test]
        fn remove_session_record_is_idempotent() {
            let dir = TempDir::new("remove");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &record).expect("write");
            remove_session_record(&dir.path, "svc-1").expect("remove");
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none()
            );
            // Second remove: still `Ok`.
            remove_session_record(&dir.path, "svc-1").expect("remove again");
        }

        /// A second `Run` on the same control root is refused while the first
        /// holds the lock (single-instance guarantee, mirroring
        /// `mailbox::second_open_on_locked_root_fails`).
        #[test]
        fn second_control_lock_is_refused() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("lock");
            let _held = acquire_control_lock(&dir.path).expect("first lock");
            assert!(acquire_control_lock(&dir.path).is_err());
        }

        /// The short-lived admission lock is independent from the
        /// long-lived control lock, so cleanup can take it while `Run` owns
        /// the control lock.
        #[test]
        fn admission_lock_is_independent_from_control_lock() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("admission-lock");
            let _control = acquire_control_lock(&dir.path).expect("control lock");
            let _admission = acquire_admission_lock(&dir.path).expect("admission lock");
        }

        /// `probe_control` reports `Live` while a lock is held and
        /// `NotRunning` once released, without ever writing the stop
        /// sentinel (a pure read).
        #[test]
        fn probe_control_reflects_lock_without_signalling() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("probe");
            {
                let _held = acquire_control_lock(&dir.path).expect("lock");
                assert_eq!(
                    probe_control(&dir.path).expect("probe"),
                    ControlLiveness::Live
                );
            }
            assert_eq!(
                probe_control(&dir.path).expect("probe after release"),
                ControlLiveness::NotRunning
            );
            assert!(
                !dir.path.join(CONTROL_STOP_FILE).exists(),
                "a read-only probe never drops the stop sentinel"
            );
        }

        /// A concurrent liveness probe must never defeat a starting
        /// supervisor.
        ///
        /// `probe_or_signal_control` decides "not running" by *taking* the
        /// exclusive control lock, so its hold — however brief — is
        /// indistinguishable from a live `Run` to
        /// [`acquire_control_lock`]'s single non-blocking attempt. Every
        /// client that polls `service status` while a supervisor starts
        /// (exactly what the restart integration test does) can therefore
        /// kill it with a spurious "another baton service already holds the
        /// control lock". The two must be serialized against each other.
        #[test]
        fn probe_never_defeats_a_concurrent_control_lock_acquisition() {
            use std::sync::Arc;
            use std::sync::atomic::AtomicBool;

            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("probe-vs-acquire");
            // Create the lock file up front so the probers spend their time
            // contending for it rather than creating it.
            drop(acquire_control_lock(&dir.path).expect("seed the lock file"));

            let stop = Arc::new(AtomicBool::new(false));
            let probers: Vec<_> = (0..2)
                .map(|_| {
                    let path = dir.path.clone();
                    let stop = Arc::clone(&stop);
                    std::thread::spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            probe_control(&path).expect("probe");
                        }
                    })
                })
                .collect();

            let mut lost = None;
            for attempt in 0..500 {
                match acquire_control_lock(&dir.path) {
                    Ok(lock) => drop(lock),
                    Err(err) => {
                        lost = Some((attempt, err));
                        break;
                    }
                }
            }

            stop.store(true, Ordering::Relaxed);
            for prober in probers {
                prober.join().expect("prober thread");
            }
            assert!(
                lost.is_none(),
                "a concurrent probe defeated the control-lock acquisition: {lost:?}"
            );
        }

        /// `request_control_stop` drops the sentinel for a live holder and
        /// reports `NotRunning` (without creating one) when the lock is free.
        #[test]
        fn request_control_stop_signals_only_when_live() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("signal");
            let held = acquire_control_lock(&dir.path).expect("lock");
            assert_eq!(
                request_control_stop(&dir.path).expect("stop"),
                ControlLiveness::Live
            );
            assert!(dir.path.join(CONTROL_STOP_FILE).exists());
            drop(held);

            let _ = fs::remove_file(dir.path.join(CONTROL_STOP_FILE));
            assert_eq!(
                request_control_stop(&dir.path).expect("stop again"),
                ControlLiveness::NotRunning
            );
            assert!(!dir.path.join(CONTROL_STOP_FILE).exists());
        }

        /// A negative process-group id must follow `--`; without the option
        /// terminator procps-ng interprets it as an option and can broadcast
        /// the signal with `kill(-1, ...)`.
        #[test]
        fn signal_group_arguments_terminate_options_before_negative_pgid() {
            assert_eq!(
                signal_group_arguments(1_072_950, "-TERM"),
                Some([
                    "-TERM".to_string(),
                    "--".to_string(),
                    "-1072950".to_string(),
                ])
            );
            assert_eq!(signal_group_arguments(1, "-TERM"), None);
        }

        /// `serve_argv` reconstructs a plain daemon invocation with only
        /// `--inbox`/`--outbox`, and folds in the agent flags (in order) only
        /// when `agent_cmd` is set.
        #[test]
        fn serve_argv_reflects_spec() {
            let plain = spec("/tmp/in", "/tmp/out");
            assert_eq!(
                serve_argv(&plain),
                vec!["serve", "--inbox", "/tmp/in", "--outbox", "/tmp/out"]
            );

            let mut agent = spec("/tmp/in", "/tmp/out");
            agent.agent_cmd = Some("claude".to_string());
            agent.agent_args = vec!["--print".to_string()];
            agent.agent_cwd = Some("/work".to_string());
            agent.agent_output = Some("json".to_string());
            agent.agent_result_key = Some("result".to_string());
            agent.role = Some("alice".to_string());
            assert_eq!(
                serve_argv(&agent),
                vec![
                    "serve",
                    "--inbox",
                    "/tmp/in",
                    "--outbox",
                    "/tmp/out",
                    "--agent-cmd",
                    "claude",
                    "--agent-arg",
                    "--print",
                    "--agent-cwd",
                    "/work",
                    "--agent-output",
                    "json",
                    "--agent-result-key",
                    "result",
                    "--role",
                    "alice",
                ]
            );
        }

        /// `execute_status` on a control root with no `Run` and no sessions
        /// reports `service_running: false` and an empty session list.
        #[test]
        fn execute_status_on_fresh_control_is_empty() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("status-fresh");
            let mut out = Vec::new();
            execute_status(&dir.path, None, &mut out).expect("status");
            let json: serde_json::Value = serde_json::from_slice(&out).expect("json");
            assert_eq!(json["service_running"], false);
            assert_eq!(json["sessions"].as_array().unwrap().len(), 0);
        }

        /// `execute_status` reports `service_running: true` while a lock is
        /// held, and lists a written session record's liveness.
        #[test]
        fn execute_status_reports_live_service_and_dead_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("status-live");
            let _held = acquire_control_lock(&dir.path).expect("lock");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                // A PID astronomically unlikely to be a live process on the
                // test host, so the recorded session reads as not-alive.
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &record).expect("write");

            let mut out = Vec::new();
            execute_status(&dir.path, None, &mut out).expect("status");
            let json: serde_json::Value = serde_json::from_slice(&out).expect("json");
            assert_eq!(json["service_running"], true);
            let sessions = json["sessions"].as_array().unwrap();
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0]["id"], "svc-1");
            assert_eq!(sessions[0]["live"], false);
            assert_eq!(sessions[0]["liveness"], "dead");
        }

        /// `execute_stop` on an unknown session id is a no-op success
        /// (idempotent), leaving nothing behind.
        #[test]
        fn execute_stop_unknown_session_is_idempotent_success() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-unknown");
            let mut out = Vec::new();
            execute_stop(&dir.path, "nope", false, &mut out).expect("stop");
            assert!(String::from_utf8(out).unwrap().contains("nothing to stop"));
        }

        /// `execute_teardown` reaps a dead session's stale record and reports
        /// no running service when none holds the lock — idempotent even
        /// when `Run` was never (or is no longer) alive.
        #[test]
        fn execute_teardown_reaps_stale_record_without_live_service() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-stale");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &record).expect("write");

            let mut out = Vec::new();
            execute_teardown(&dir.path, false, &mut out).expect("teardown");
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none(),
                "the stale record is reaped"
            );
            assert!(
                String::from_utf8(out)
                    .unwrap()
                    .contains("no running baton service")
            );
        }

        /// Teardown removes a terminal legacy task record without probing or
        /// signalling a live process whose PID and argv happen to match it.
        #[cfg(target_os = "linux")]
        #[test]
        fn execute_teardown_does_not_signal_terminal_legacy_task() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-terminal-task");
            let task_id = "task-terminal-legacy";
            let callback_inbox = dir.path.join("callback");
            let task_specification = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                Vec::new(),
                60_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, task_id);
            fs::create_dir_all(&log_dir).expect("create task log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                .expect("spawn unrelated process");
            let task_record = TaskRecord {
                id: task_id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Responded,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(1),
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &task_record).expect("write terminal task record");
            request_task_cancel_sentinel(&dir.path, task_id).expect("write cancel sentinel");
            let deadline = Instant::now() + Duration::from_secs(10);
            let liveness = loop {
                let liveness = is_task_alive(&task_record);
                if liveness == Liveness::Live || Instant::now() >= deadline {
                    break liveness;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            assert_eq!(
                liveness,
                Liveness::Live,
                "fixture must match the live process by PID and argv within 10 seconds"
            );

            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session record");

            execute_teardown(&dir.path, false, &mut Vec::new()).expect("teardown");

            assert!(
                child.try_wait().expect("poll unrelated process").is_none(),
                "teardown leaves the matching unrelated process alive"
            );
            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read removed terminal task")
                    .is_none(),
                "teardown removes the terminal task record"
            );
            assert!(
                !task_cancel_sentinel_path(&dir.path, task_id).is_file(),
                "teardown removes the terminal task cancel sentinel"
            );

            child.kill().expect("clean up unrelated process");
            child.wait().expect("wait for unrelated process");
        }

        // -- Task records -----------------------------------------------

        /// A task record round-trips through the atomic-write file protocol
        /// byte-for-byte.
        #[test]
        fn task_record_round_trips() {
            let dir = TempDir::new("task-record");
            let record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: 4242,
                started_at: Some("123456".to_string()),
                start_epoch_secs: Some(123456),
                job: None,
                started_ms: Some(42),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: "/tmp/out.log".to_string(),
                stderr_path: "/tmp/err.log".to_string(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &record).expect("write");
            let read = read_task_record(&dir.path, "task-1")
                .expect("read")
                .expect("present");
            assert_eq!(read, record);
        }

        /// A task id absent from `tasks/` reads as `None`, not an error.
        #[test]
        fn read_task_record_absent_is_none() {
            let dir = TempDir::new("task-absent");
            assert!(read_task_record(&dir.path, "nope").expect("read").is_none());
        }

        /// An unsafe task id is rejected rather than escaping the control
        /// root.
        #[test]
        fn unsafe_task_id_is_rejected() {
            let dir = TempDir::new("task-unsafe");
            assert!(read_task_record(&dir.path, "../escape").is_err());
            assert!(remove_task_record(&dir.path, "../escape").is_err());
        }

        /// `list_task_records` returns every written record.
        #[test]
        fn list_task_records_returns_all() {
            let dir = TempDir::new("task-list");
            for i in 0..3 {
                let record = TaskRecord {
                    id: format!("task-{i}"),
                    request_id: None,
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                    pid: 1000 + i,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Running,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout_path: String::new(),
                    stderr_path: String::new(),
                    delivered_milestones: 0,
                };
                write_task_record(&dir.path, &record).expect("write");
            }
            let mut ids: Vec<String> = list_task_records(&dir.path)
                .expect("list")
                .into_iter()
                .map(|r| r.id)
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["task-0", "task-1", "task-2"]);
        }

        /// An absent response returns without waiting on the admission lock,
        /// so a polling client cannot hold that lock across the wait interval.
        #[test]
        fn absent_task_start_response_poll_is_lock_free() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-response-poll");
            let _admission = acquire_admission_lock(&dir.path).expect("hold admission lock");
            assert!(
                take_task_start_response(&dir.path, "poll-request")
                    .expect("poll response")
                    .is_none()
            );
        }

        /// A response claim writes its acknowledgement before cleanup, and a
        /// repeated consumer cannot read the response again.
        #[test]
        fn task_start_response_claim_records_ack_idempotently() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-response-ack");
            let request_id = "response-ack-request";
            let response = TaskStartResponse {
                task_id: Some("task-1".to_string()),
                error: None,
            };
            write_task_start_response(&dir.path, request_id, &response).expect("write response");

            let consumed = take_task_start_response(&dir.path, request_id)
                .expect("take response")
                .expect("response is present");
            assert_eq!(consumed.task_id, response.task_id);
            assert!(task_start_ack_exists(&dir.path, request_id).expect("ack exists"));
            assert!(
                !task_start_response_path(&dir.path, request_id)
                    .expect("response path")
                    .exists()
            );
            assert!(
                !task_start_response_claim_path(&dir.path, request_id)
                    .expect("claim path")
                    .exists()
            );
            assert!(
                take_task_start_response(&dir.path, request_id)
                    .expect("repeat take")
                    .is_none()
            );
            reconcile_task_admissions(&dir.path).expect("reconcile orphan acknowledgement");
            assert!(!task_start_ack_exists(&dir.path, request_id).expect("ack cleanup"));
        }

        /// A private claim left by a client before acknowledgement is
        /// restored even when its response has no task record, such as an
        /// owner-rejection response.
        #[test]
        fn reconcile_orphan_task_response_claim_restores_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("orphan-task-response-claim");
            let request_id = "orphan-response-claim";
            let response = TaskStartResponse {
                task_id: None,
                error: Some("owner rejected".to_string()),
            };
            write_task_start_response(&dir.path, request_id, &response)
                .expect("write owner rejection response");
            fs::rename(
                task_start_response_path(&dir.path, request_id).expect("response path"),
                task_start_response_claim_path(&dir.path, request_id).expect("claim path"),
            )
            .expect("claim owner rejection response");

            reconcile_task_admissions(&dir.path).expect("reconcile orphan claim");

            assert!(
                task_start_response_path(&dir.path, request_id)
                    .expect("response path")
                    .is_file()
            );
            assert!(
                !task_start_response_claim_path(&dir.path, request_id)
                    .expect("claim path")
                    .exists()
            );
        }

        /// Startup reconciliation finalizes a committed record for an
        /// acknowledgement, a response, or a recoverable private claim, and
        /// remains safe when repeated.
        #[test]
        fn reconcile_task_admission_finalizes_response_boundaries() {
            let _guard = serialize_forks_and_locks();
            for (index, boundary) in ["ack", "response", "claim", "missing"]
                .into_iter()
                .enumerate()
            {
                let dir = TempDir::new(&format!("task-response-boundary-{boundary}"));
                let request_id = format!("response-boundary-request-{index}");
                let task_id = format!("response-boundary-task-{index}");
                let record = TaskRecord {
                    id: task_id.clone(),
                    request_id: Some(request_id.clone()),
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/callback"),
                    pid: 0,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Completed,
                    exit_code: Some(0),
                    elapsed_ms: Some(1),
                    stdout_path: String::new(),
                    stderr_path: String::new(),
                    delivered_milestones: 0,
                };
                write_task_record(&dir.path, &record).expect("write task record");
                let response = TaskStartResponse {
                    task_id: Some(task_id.clone()),
                    error: None,
                };
                if boundary != "missing" {
                    write_task_start_response(&dir.path, &request_id, &response)
                        .expect("write task response");
                }
                if boundary == "ack" {
                    take_task_start_response(&dir.path, &request_id)
                        .expect("take response")
                        .expect("response is present");
                } else if boundary == "claim" {
                    fs::rename(
                        task_start_response_path(&dir.path, &request_id).expect("response path"),
                        task_start_response_claim_path(&dir.path, &request_id).expect("claim path"),
                    )
                    .expect("claim response");
                }

                reconcile_task_admissions(&dir.path).expect("reconcile response boundary");
                reconcile_task_admissions(&dir.path).expect("repeat reconciliation");

                let reconciled = read_task_record(&dir.path, &task_id)
                    .expect("read reconciled task")
                    .expect("reconciled task exists");
                assert_eq!(reconciled.admission, TaskAdmissionPhase::Responded);
                assert_eq!(
                    task_start_response_path(&dir.path, &request_id)
                        .expect("response path")
                        .is_file(),
                    boundary != "ack"
                );
                assert!(
                    !task_start_response_claim_path(&dir.path, &request_id)
                        .expect("claim path")
                        .exists()
                );
                assert!(
                    !task_start_ack_path(&dir.path, &request_id)
                        .expect("ack path")
                        .exists()
                );
            }
        }

        /// A rollback marker removes every admission phase's task record and
        /// both request locations before the marker is cleared. Repeating
        /// reconciliation is harmless once the first pass has completed.
        #[test]
        fn reconcile_task_admission_rollback_is_idempotent_across_phases() {
            let _guard = serialize_forks_and_locks();
            for (index, admission) in [
                TaskAdmissionPhase::Prepared,
                TaskAdmissionPhase::Committed,
                TaskAdmissionPhase::Responded,
            ]
            .into_iter()
            .enumerate()
            {
                let dir = TempDir::new(&format!("task-rollback-phase-{index}"));
                let request_id = format!("request-{index}");
                let task_id = format!("task-{index}");
                let record = TaskRecord {
                    id: task_id.clone(),
                    request_id: Some(request_id.clone()),
                    admission,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/callback"),
                    pid: 0,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Completed,
                    exit_code: Some(0),
                    elapsed_ms: Some(1),
                    stdout_path: String::new(),
                    stderr_path: String::new(),
                    delivered_milestones: 0,
                };
                write_task_record(&dir.path, &record).expect("write task record");

                for path in [
                    task_requests_dir(&dir.path),
                    task_processing_dir(&dir.path),
                    task_start_rollback_dir(&dir.path),
                ] {
                    fs::create_dir_all(&path).expect("create task admission directory");
                }
                let file_name = mailbox::file_name(&request_id);
                fs::write(task_requests_dir(&dir.path).join(&file_name), "{}")
                    .expect("write request");
                fs::write(task_processing_dir(&dir.path).join(&file_name), "{}")
                    .expect("write claimed request");
                fs::write(task_start_rollback_dir(&dir.path).join(&file_name), "")
                    .expect("write rollback marker");

                reconcile_task_admissions(&dir.path).expect("reconcile rollback");
                reconcile_task_admissions(&dir.path).expect("repeat reconciliation");

                assert!(
                    read_task_record(&dir.path, &task_id)
                        .expect("read task record")
                        .is_none(),
                    "{admission:?} task record is removed"
                );
                assert!(
                    !task_requests_dir(&dir.path).join(&file_name).exists(),
                    "pending request is removed"
                );
                assert!(
                    !task_processing_dir(&dir.path).join(&file_name).exists(),
                    "claimed request is removed"
                );
                assert!(
                    !task_start_rollback_dir(&dir.path).join(&file_name).exists(),
                    "rollback marker is removed last"
                );
            }
        }

        /// An unresolved prepared admission is retained as cleanup residue,
        /// not rehydrated as active work. Once its PID is positively dead,
        /// reconciliation removes the record and every admission artifact.
        #[cfg(target_os = "linux")]
        #[test]
        fn unresolved_prepared_admission_retains_marker_until_dead_cleanup() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("prepared-unresolved");
            let request_id = "prepared-unresolved-request";
            let task_id = "prepared-unresolved-task";
            let spec = task_spec(
                "svc-1",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let log_dir = task_logs_dir(&dir.path, task_id);
            fs::create_dir_all(&log_dir).expect("create task logs");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child =
                spawn_task_child(&spec, &stdout_path, &stderr_path).expect("spawn unresolved task");
            let record = TaskRecord {
                id: task_id.to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Prepared,
                spec,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: Some(1),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &record).expect("write prepared task");
            write_task_start_response(
                &dir.path,
                request_id,
                &TaskStartResponse {
                    task_id: Some(task_id.to_string()),
                    error: None,
                },
            )
            .expect("write task response");
            mark_task_start_ack(&dir.path, request_id).expect("write task acknowledgement");
            mark_task_start_rollback(&dir.path, request_id).expect("write rollback marker");

            let deadline = Instant::now() + Duration::from_secs(10);
            while is_task_alive(&record) != Liveness::Unresolved && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(
                is_task_alive(&record),
                Liveness::Unresolved,
                "fixture reaches the unresolved identity state"
            );

            reconcile_task_admissions(&dir.path).expect("retain unresolved admission");
            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read retained task")
                    .is_some(),
                "unresolved prepared record remains durable"
            );
            assert!(
                task_start_rollback_exists(&dir.path, request_id).expect("rollback marker exists"),
                "rollback marker remains until cleanup succeeds"
            );
            assert!(
                rehydrate_tasks(&dir.path, &FakeClock::new())
                    .expect("rehydrate tasks")
                    .is_empty(),
                "unresolved prepared record is not active work"
            );

            signal_group(record.pid, "-KILL").expect("kill unresolved task");
            child.wait().expect("wait for unresolved task");
            reconcile_task_admissions(&dir.path).expect("remove dead admission");

            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read removed task")
                    .is_none(),
                "dead prepared record is removed"
            );
            assert!(
                !task_start_rollback_exists(&dir.path, request_id)
                    .expect("rollback marker cleanup"),
                "dead prepared rollback marker is removed"
            );
            assert!(
                !task_start_response_boundary_exists(&dir.path, request_id)
                    .expect("response boundary cleanup"),
                "dead prepared response boundary is removed"
            );
        }

        /// A PID whose current start key does not match the durable record is
        /// finalized as gone and is never signalled as the task.
        #[test]
        fn rehydrate_rejects_pid_reuse_without_signalling() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-pid-reuse");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["5".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-reused");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child =
                spawn_task_child(&spec, &stdout_path, &stderr_path).expect("spawn task");
            let record = TaskRecord {
                id: "task-reused".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid: child.id(),
                started_at: Some("not-the-current-start-key".to_string()),
                start_epoch_secs: None,
                job: None,
                started_ms: Some(clock.now_ms()),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &record).expect("write task record");

            let mut tasks = rehydrate_tasks(&dir.path, &clock).expect("rehydrate task");
            let mut running = tasks.remove("task-reused").expect("rehydrated task");
            assert!(
                running.child.is_none(),
                "rehydrated task has no Child handle"
            );
            assert!(
                matches!(
                    tick_one_task(&dir.path, "task-reused", &mut running, &clock),
                    Ok(TaskTick::Finished)
                ),
                "PID reuse is finalized without adoption"
            );
            assert_eq!(running.record.state, TaskState::Failed);
            assert_eq!(running.record.exit_code, None);
            assert!(
                child.try_wait().expect("poll unrelated process").is_none(),
                "a mismatched PID is not signalled"
            );

            let _ = signal_group(child.id(), "-KILL");
            let _ = child.wait();
        }

        /// A reused PID can still be a zombie while the original task's
        /// descendant keeps the old process group alive. The zombie's
        /// mismatched identity must remain unresolved rather than making that
        /// unrelated group look like this task.
        #[cfg(target_os = "linux")]
        #[test]
        fn task_rejects_reused_zombie_pid_with_live_group() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-zombie-pid-reuse");
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-zombie-pid-reuse");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&spec, &stdout_path, &stderr_path)
                .expect("spawn task with descendant");
            let pid = child.id();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            assert!(started_at.is_some(), "fixture has a start identity");
            let record = TaskRecord {
                id: "task-zombie-pid-reuse".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid,
                started_at: Some("not-the-zombie-start-key".to_string()),
                start_epoch_secs,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };

            wait_for_zombie_group_descendant(&child);
            write_task_record(&dir.path, &record).expect("write zombie task record");
            assert_eq!(
                task_execution_liveness(&record),
                Liveness::Unresolved,
                "a mismatched zombie remains unresolved"
            );
            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue =
                reap_session_tasks_with_wait(&mut admission, "svc-1", false, wait_while_task_alive)
                    .expect("retain task");
            drop(admission);
            assert_eq!(residue.len(), 1);
            assert_eq!(residue[0].liveness, Liveness::Unresolved);
            assert!(
                read_task_record(&dir.path, &record.id)
                    .expect("read retained zombie task")
                    .is_some(),
                "unresolved zombie task record is retained"
            );
            let mut legacy = record.clone();
            legacy.started_at = None;
            assert_eq!(
                task_execution_liveness(&legacy),
                Liveness::Unresolved,
                "a zombie without durable identity remains unresolved"
            );
            assert_eq!(
                unsafe { libc::kill(-(pid as libc::pid_t), 0) },
                0,
                "the descendant's process group remains alive after the rejected probes"
            );

            signal_group(pid, "-KILL").expect("kill fixture group");
            let _ = child.wait();
        }

        /// The lstart parser accepts a leap-day in a divisible-by-four year.
        #[test]
        fn lstart_parser_leap_year_case() {
            assert_eq!(
                parse_lstart_epoch_secs("Thu Feb 29 00:00:00 2024"),
                Some(1_709_164_800)
            );
        }

        /// The lstart parser applies Gregorian century rules: 1900 is not a
        /// leap year, while 2000 is.
        #[test]
        fn lstart_parser_century_boundary_case() {
            assert_eq!(parse_lstart_epoch_secs("Thu Feb 29 00:00:00 1900"), None);
            assert_eq!(
                parse_lstart_epoch_secs("Tue Feb 29 00:00:00 2000"),
                Some(951_782_400)
            );
        }

        /// The lstart parser carries a month boundary by exactly one day.
        #[test]
        fn lstart_parser_month_boundary_case() {
            let january =
                parse_lstart_epoch_secs("Wed Jan 31 23:59:59 2024").expect("January 31 is valid");
            let february =
                parse_lstart_epoch_secs("Thu Feb 1 00:00:00 2024").expect("February 1 is valid");
            assert_eq!(february - january, 1);
        }

        /// The lstart parser maps the Unix epoch itself to zero.
        #[test]
        fn lstart_parser_unix_epoch_case() {
            assert_eq!(parse_lstart_epoch_secs("Thu Jan 1 00:00:00 1970"), Some(0));
        }

        /// Linux must not condemn a task whose durable start key is absent
        /// merely because bash has exec-replaced its command line. The
        /// command-line corroborator may confirm a task, but a mismatch is
        /// unresolved because the same PID can legitimately change argv.
        #[cfg(target_os = "linux")]
        #[test]
        fn linux_exec_replaced_task_without_start_key_is_unresolved() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("linux-exec-unresolved");
            let callback_inbox = dir.path.join("callback");
            let task_specification = task_spec(
                "svc-1",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-exec-unresolved");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                .expect("spawn task");
            let record = TaskRecord {
                id: "task-exec-unresolved".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            let deadline = Instant::now() + Duration::from_secs(10);
            let liveness = loop {
                let liveness = is_task_alive(&record);
                if liveness == Liveness::Unresolved || Instant::now() >= deadline {
                    break liveness;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            assert_eq!(liveness, Liveness::Unresolved);
            assert!(
                child.try_wait().expect("poll exec-replaced task").is_none(),
                "an unresolved task is not signalled"
            );
            let _ = signal_group(child.id(), "-KILL");
            let _ = child.wait();
        }

        /// A zombie sample is never accepted as a live, corroborated task
        /// even when its reported start time matches the durable record.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn process_probe_rejects_zombie_state() {
            let probe = parse_process_probe("Z Mon Aug 3 12:34:56 2026\n")
                .expect("parse zombie process sample");
            assert!(probe.is_zombie());
            let (started_at, start_epoch_secs) = start_identity_from_probe(&probe);
            assert!(started_at.is_none());
            assert!(start_epoch_secs.is_none());
        }

        /// macOS/BSD `ps` appends metadata flags to the leading process
        /// state; those flags do not change whether the process is a zombie.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn process_state_accepts_bsd_ps_flags() {
            assert_eq!(parse_process_state("Ssl+"), Some(false));
            assert_eq!(parse_process_state("Zs+"), Some(true));
            assert_eq!(parse_process_state("Q"), None);
            assert_eq!(parse_process_state("S?"), None);
        }

        /// Removing an already-absent task record is a no-op success
        /// (idempotent).
        #[test]
        fn remove_task_record_is_idempotent() {
            let dir = TempDir::new("task-remove");
            assert!(remove_task_record(&dir.path, "nope").is_ok());
        }

        // -- Deterministic event ids + mailbox delivery ------------------

        /// A redelivered event id — already consumed (`claimed → done`) by a
        /// prior delivery — is dropped by the mailbox's own dedup, not
        /// reprocessed, so a crash-restart replay never creates a second
        /// visible event.
        #[test]
        fn task_event_redelivery_is_deduped_by_mailbox_done_ledger() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("event-dedup");
            let callback_inbox = dir.path.join("callback");
            let record = TaskRecord {
                id: "task-x".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec(
                    "svc-1",
                    "true",
                    vec![],
                    vec![],
                    1_000,
                    &callback_inbox.display().to_string(),
                ),
                pid: 0,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(10),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };

            deliver_task_event(&record, TaskEventKind::Terminal).expect("deliver");
            {
                let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
                let claimed = mailbox.claim_next().expect("claim").expect("event present");
                assert_eq!(claimed.key, "task-x-terminal");
                mailbox.complete(claimed).expect("complete");
            }

            // Redeliver the identical event (same deterministic id).
            deliver_task_event(&record, TaskEventKind::Terminal).expect("redeliver");
            {
                let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
                assert!(
                    mailbox.claim_next().expect("claim").is_none(),
                    "an id already in done/ must be dropped, not reprocessed"
                );
            }
        }

        // -- Task loop: FakeClock-driven milestone/max-duration/terminal -

        /// A milestone fires exactly once it is due, per the injected clock
        /// — not real wall-clock time — and is delivered to the callback
        /// mailbox under its deterministic id.
        #[test]
        fn tick_one_task_delivers_milestone_via_fake_clock_no_real_sleep() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-milestone");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["0.3".to_string()],
                vec![50],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-m", spec, &clock);

            clock.advance(60);
            let tick = tick_one_task(&dir.path, "task-m", &mut running, &clock).expect("tick");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_eq!(running.record.delivered_milestones, 1);

            let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
            let claimed = mailbox
                .claim_next()
                .expect("claim")
                .expect("milestone event present");
            assert_eq!(claimed.key, "task-m-milestone-0");

            // Ticking again at the same elapsed time must not re-fire it.
            let tick = tick_one_task(&dir.path, "task-m", &mut running, &clock).expect("tick");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_eq!(running.record.delivered_milestones, 1);

            let _ = signal_group(running.record.pid, "-KILL");
            let _ = running.child.as_mut().expect("owned child").wait();
        }

        /// Exceeding `max_duration_ms` (per the injected clock) escalates
        /// `SIGTERM`→`SIGKILL` when the child remains alive, while an exit
        /// observed immediately after `SIGTERM` is also attributed to
        /// `timeout`. Both valid Unix reap outcomes deliver the same terminal
        /// event.
        #[test]
        fn tick_one_task_enforces_max_duration_and_marks_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-timeout");
            let clock = FakeClock::new();
            let helper = task_timeout_helper_path();

            let immediate_callback = dir.path.join("callback-immediate");
            let immediate_ready = dir.path.join("immediate.ready");
            let immediate_spec = task_spec(
                "svc-1",
                &helper.display().to_string(),
                vec![
                    "--mode".to_string(),
                    "exit-on-term".to_string(),
                    "--ready-file".to_string(),
                    immediate_ready.display().to_string(),
                ],
                vec![],
                100,
                &immediate_callback.display().to_string(),
            );
            let mut immediate =
                spawn_running_task(&dir.path, "task-t-immediate", immediate_spec, &clock);
            wait_for_task_helper(&mut immediate, &immediate_ready);

            clock.advance(150);
            let tick =
                tick_one_task(&dir.path, "task-t-immediate", &mut immediate, &clock).expect("tick");
            assert!(
                immediate.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM"
            );
            reap_task_until_finished(&dir.path, "task-t-immediate", &mut immediate, &clock, tick);
            assert!(
                !immediate.kill_sent,
                "immediate TERM exit must not need SIGKILL"
            );
            assert_eq!(immediate.record.state, TaskState::Timeout);
            assert_terminal_task_event(&immediate_callback, "task-t-immediate");

            let kill_callback = dir.path.join("callback-kill");
            let kill_ready = dir.path.join("kill.ready");
            let kill_spec = task_spec(
                "svc-1",
                &helper.display().to_string(),
                vec![
                    "--mode".to_string(),
                    "ignore-term".to_string(),
                    "--ready-file".to_string(),
                    kill_ready.display().to_string(),
                ],
                vec![],
                100,
                &kill_callback.display().to_string(),
            );
            let mut kill_task = spawn_running_task(&dir.path, "task-t-kill", kill_spec, &clock);
            wait_for_task_helper(&mut kill_task, &kill_ready);

            clock.advance(150);
            let tick =
                tick_one_task(&dir.path, "task-t-kill", &mut kill_task, &clock).expect("tick");
            assert!(
                matches!(tick, TaskTick::StillRunning),
                "TERM-ignoring helper must remain alive for KILL escalation"
            );
            assert!(
                kill_task.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM"
            );

            clock.advance(KILL_GRACE_MS);
            let tick =
                tick_one_task(&dir.path, "task-t-kill", &mut kill_task, &clock).expect("tick");
            assert!(
                kill_task.kill_sent,
                "SIGTERM grace expiry must send SIGKILL"
            );
            reap_task_until_finished(&dir.path, "task-t-kill", &mut kill_task, &clock, tick);
            assert_eq!(kill_task.record.state, TaskState::Timeout);

            assert_terminal_task_event(&kill_callback, "task-t-kill");
        }

        /// Builds the explicit-signal child used by the timeout test. The
        /// helper is an example that runs as a real process-group leader.
        fn task_timeout_helper_path() -> std::path::PathBuf {
            let cargo = option_env!("CARGO").unwrap_or("cargo");
            let built = Command::new(cargo)
                .args(["build", "--quiet", "--example", "task_timeout_helper"])
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .status()
                .expect("build task_timeout_helper");
            assert!(built.success(), "task_timeout_helper builds");

            let test_bin = std::env::current_exe().expect("resolve service test executable");
            let profile_dir = test_bin
                .parent()
                .and_then(std::path::Path::parent)
                .expect("service test executable has a profile directory");
            let helper = profile_dir.join("examples").join("task_timeout_helper");
            assert!(
                helper.is_file(),
                "task_timeout_helper at {}",
                helper.display()
            );
            helper
        }

        /// Waits for the helper's signal handler to be installed before the
        /// fake clock drives the timeout. A readiness file avoids a startup
        /// race where TERM could arrive before the helper has configured its
        /// explicit handler.
        fn wait_for_task_helper(running: &mut RunningTask, ready: &Path) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if ready.is_file() {
                    return;
                }
                if let Some(child) = running.child.as_mut()
                    && child.try_wait().expect("poll task helper").is_some()
                {
                    panic!("task timeout helper exited before becoming ready");
                }
                if Instant::now() >= deadline {
                    let _ = signal_group(running.record.pid, "-KILL");
                    let _ = running.child.as_mut().expect("owned helper").wait();
                    panic!("task timeout helper did not become ready within the test bound");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// Reaps a task after the fake-clock decision has been made. Process
        /// exit and `Child::try_wait` are real-time OS observations, so keep
        /// polling at the same fake time under a finite wall-clock bound.
        fn reap_task_until_finished(
            control: &Path,
            id: &str,
            running: &mut RunningTask,
            clock: &dyn Clock,
            first_tick: TaskTick,
        ) {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut tick = first_tick;
            loop {
                if matches!(tick, TaskTick::Finished) {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task {id} did not exit within the bounded real-time reap loop"
                );
                std::thread::sleep(Duration::from_millis(20));
                tick = tick_one_task(control, id, running, clock).expect("tick");
            }
        }

        fn assert_terminal_task_event(callback_inbox: &Path, task_id: &str) {
            let mailbox = mailbox::Mailbox::open(callback_inbox).expect("open");
            let claimed = mailbox
                .claim_next()
                .expect("claim")
                .expect("terminal event present");
            assert_eq!(claimed.key, format!("{task_id}-terminal"));
        }

        #[cfg(target_os = "linux")]
        fn wait_for_zombie_group_descendant(child: &Child) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let zombie = matches!(
                    process_probe(child.id()),
                    ProbeResult::Present(probe) if probe.is_zombie()
                );
                if zombie && task_group_liveness(child.id()) == Liveness::Live {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task leader did not become a zombie with a live group descendant"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// Waits until the direct shell exits while its background child
        /// keeps the task's process group alive. The bounded wait avoids
        /// making the process-group regressions depend on scheduler timing.
        fn wait_for_group_descendant(running: &mut RunningTask) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let direct_exited = running
                    .child
                    .as_mut()
                    .expect("owned task child")
                    .try_wait()
                    .expect("poll task child")
                    .is_some();
                if direct_exited && task_group_liveness(running.record.pid) == Liveness::Live {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task direct child did not exit with a live group descendant"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        fn assert_durable_task_state(control: &Path, id: &str, state: TaskState) -> TaskRecord {
            let record = read_task_record(control, id)
                .expect("read durable task")
                .expect("durable task record");
            assert_eq!(record.state, state);
            record
        }

        /// Failed terminal callback delivery backs off from one second and
        /// still redelivers the deterministic event when the inbox recovers.
        #[test]
        fn terminal_delivery_uses_fake_clock_backoff_and_recovers() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("terminal-delivery-retry");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let mut running =
                terminal_running_task(&dir.path, "task-terminal-retry", &callback_inbox, &clock);

            match tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                .expect("first terminal delivery attempt")
            {
                TaskTick::TerminalDeliveryRetry {
                    attempt, delay_ms, ..
                } => {
                    assert_eq!(attempt, 1);
                    assert_eq!(delay_ms, TERMINAL_RETRY_INITIAL_DELAY_MS);
                }
                other => panic!("expected a scheduled retry, got {other:?}"),
            }
            assert_eq!(running.terminal_delivery_attempts, 1);
            assert_eq!(running.next_terminal_retry_ms, Some(1_000));

            clock.advance(TERMINAL_RETRY_INITIAL_DELAY_MS - 1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                    .expect("early terminal retry tick"),
                TaskTick::StillRunning
            ));
            fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");

            clock.advance(1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                    .expect("recovered terminal delivery"),
                TaskTick::Finished
            ));
            assert_terminal_task_event(&callback_inbox, "task-terminal-retry");
        }

        /// Persistent terminal callback failure is retried at most the
        /// configured bound, with no delay growing beyond one minute, then
        /// returns `Finished` so `tick_tasks` drops the tracker entry.
        #[test]
        fn terminal_delivery_drops_after_bounded_backoff_attempts() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("terminal-delivery-drop");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let mut running =
                terminal_running_task(&dir.path, "task-terminal-drop", &callback_inbox, &clock);
            let mut expected_delay = TERMINAL_RETRY_INITIAL_DELAY_MS;

            for attempt in 1..=MAX_TERMINAL_DELIVERY_ATTEMPTS {
                if attempt > 1 {
                    clock.advance(running.terminal_retry_delay_ms);
                }
                let tick = tick_one_task(&dir.path, "task-terminal-drop", &mut running, &clock)
                    .expect("terminal delivery attempt");
                if attempt < MAX_TERMINAL_DELIVERY_ATTEMPTS {
                    match tick {
                        TaskTick::TerminalDeliveryRetry {
                            attempt: reported_attempt,
                            delay_ms,
                            ..
                        } => {
                            assert_eq!(reported_attempt, attempt);
                            assert_eq!(delay_ms, expected_delay);
                            assert!(delay_ms <= TERMINAL_RETRY_MAX_DELAY_MS);
                            expected_delay = expected_delay
                                .saturating_mul(2)
                                .min(TERMINAL_RETRY_MAX_DELAY_MS);
                        }
                        other => panic!("expected another retry, got {other:?}"),
                    }
                } else {
                    assert!(matches!(
                        tick,
                        TaskTick::TerminalDeliveryDropped {
                            attempts: MAX_TERMINAL_DELIVERY_ATTEMPTS,
                            ..
                        }
                    ));
                }
            }
            assert_eq!(
                running.terminal_delivery_attempts,
                MAX_TERMINAL_DELIVERY_ATTEMPTS
            );
            assert!(
                callback_inbox.is_file(),
                "failed callback remains unavailable"
            );
        }

        /// A supervisor tick that races external cleanup must not recreate a
        /// durable task record after the cleanup has removed it.
        #[test]
        fn finalize_task_does_not_resurrect_removed_record() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("finalize-removed-task");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running =
                terminal_running_task(&dir.path, "task-finalize-removed", &callback_inbox, &clock);
            remove_task_record(&dir.path, "task-finalize-removed").expect("remove task record");

            assert!(matches!(
                finalize_task(&dir.path, &mut running, TaskState::Failed, None, 10, &clock,)
                    .expect("finalize removed task"),
                TaskTick::Finished
            ));
            assert!(
                read_task_record(&dir.path, "task-finalize-removed")
                    .expect("read removed task")
                    .is_none(),
                "finalizing an externally removed task does not resurrect its record"
            );
        }

        /// A task that exits zero on its own, before any max-duration
        /// breach, is reaped as `completed`.
        #[test]
        fn tick_one_task_reaps_a_clean_exit_as_completed() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-completed");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "true",
                vec![],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-c", spec, &clock);

            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match tick_one_task(&dir.path, "task-c", &mut running, &clock).expect("tick") {
                    TaskTick::Finished => break,
                    TaskTick::StillRunning => {
                        assert!(Instant::now() < deadline, "task did not exit in time");
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    other => panic!("unexpected terminal delivery outcome: {other:?}"),
                }
            }
            assert_eq!(running.record.state, TaskState::Completed);
            assert_eq!(running.record.exit_code, Some(0));
        }

        /// A direct shell exit does not finalize a task while its same-group
        /// background descendant remains alive. The retained Child handle
        /// keeps the direct exit status available for completion after drain.
        #[test]
        fn tick_one_task_waits_for_same_group_descendant_before_completion() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-group-drain", spec, &clock);

            wait_for_group_descendant(&mut running);
            let tick = tick_one_task(&dir.path, "task-group-drain", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-group-drain", TaskState::Running);

            let mut status = Vec::new();
            execute_task_status(&dir.path, Some("task-group-drain"), &mut status)
                .expect("status while group remains");
            let status: serde_json::Value = serde_json::from_slice(&status).expect("status JSON");
            assert_eq!(status["tasks"][0]["state"], "running");
            assert!(
                matches!(
                    status["tasks"][0]["liveness"].as_str(),
                    Some("live") | Some("unresolved")
                ),
                "an incomplete group scan remains safe: {status}"
            );

            signal_group(running.record.pid, "-KILL").expect("kill group descendant");
            reap_task_until_finished(&dir.path, "task-group-drain", &mut running, &clock, tick);
            let record =
                assert_durable_task_state(&dir.path, "task-group-drain", TaskState::Completed);
            assert_eq!(record.exit_code, Some(0));
            assert_terminal_task_event(&callback_inbox, "task-group-drain");
        }

        /// Cancellation after the direct leader exits still reaches a live
        /// same-group descendant and records `cancelled` only after drain.
        #[test]
        fn task_cancel_reaches_same_group_descendant_after_leader_exit() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("cancel-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-cancel-group", spec, &clock);

            wait_for_group_descendant(&mut running);
            let tick = tick_one_task(&dir.path, "task-cancel-group", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-cancel-group", TaskState::Running);

            execute_task_cancel(&dir.path, "task-cancel-group", &mut Vec::new())
                .expect("cancel task");
            reap_task_until_finished(&dir.path, "task-cancel-group", &mut running, &clock, tick);
            assert_eq!(running.record.state, TaskState::Cancelled);
            assert_durable_task_state(&dir.path, "task-cancel-group", TaskState::Cancelled);
            assert_eq!(task_group_liveness(running.record.pid), Liveness::Dead);
            assert_terminal_task_event(&callback_inbox, "task-cancel-group");
        }

        /// Timeout escalation never signals an owned task while its process
        /// identity is unresolved, even when the duration has elapsed.
        #[cfg(target_os = "linux")]
        #[test]
        fn task_timeout_does_not_signal_unresolved_owned_task() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("timeout-unresolved-owned");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running =
                spawn_running_task(&dir.path, "task-timeout-unresolved", spec, &clock);
            running.record.started_at = None;
            running.record.spec.command = "not-the-running-command".to_string();

            clock.advance(100);
            let first_tick =
                tick_one_task(&dir.path, "task-timeout-unresolved", &mut running, &clock)
                    .expect("unresolved timeout tick");
            assert!(matches!(first_tick, TaskTick::StillRunning));
            assert_eq!(running.term_sent_at_ms, None);
            assert!(!running.kill_sent);

            running.term_sent_at_ms = Some(clock.now_ms().saturating_sub(KILL_GRACE_MS));
            let second_tick =
                tick_one_task(&dir.path, "task-timeout-unresolved", &mut running, &clock)
                    .expect("unresolved timeout escalation tick");
            assert!(matches!(second_tick, TaskTick::StillRunning));
            assert!(!running.kill_sent);

            let mut child = running.child.take().expect("unresolved task child");
            assert!(child.try_wait().expect("poll unresolved task").is_none());
            signal_group(running.record.pid, "-KILL").expect("clean up unresolved task");
            child.wait().expect("wait for unresolved task");
        }

        /// Max-duration escalation after the direct leader exits still
        /// reaches a TERM-ignoring same-group descendant, then records
        /// `timeout` after SIGKILL drains the group.
        #[test]
        fn task_timeout_reaches_same_group_descendant_after_leader_exit() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("timeout-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-timeout-group", spec, &clock);

            wait_for_group_descendant(&mut running);
            let first_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(first_tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-timeout-group", TaskState::Running);

            clock.advance(100);
            let term_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("timeout TERM tick");
            assert!(matches!(term_tick, TaskTick::StillRunning));
            assert!(running.term_sent_at_ms.is_some());
            assert_durable_task_state(&dir.path, "task-timeout-group", TaskState::Running);
            assert_eq!(
                unsafe { libc::kill(-(running.record.pid as libc::pid_t), 0) },
                0,
                "the TERM-ignoring descendant keeps the process group alive"
            );

            clock.advance(KILL_GRACE_MS);
            let kill_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("timeout KILL tick");
            assert!(running.kill_sent);
            reap_task_until_finished(
                &dir.path,
                "task-timeout-group",
                &mut running,
                &clock,
                kill_tick,
            );
            assert_eq!(running.record.state, TaskState::Timeout);
            assert_durable_task_state(&dir.path, "task-timeout-group", TaskState::Timeout);
            assert_eq!(task_group_liveness(running.record.pid), Liveness::Dead);
            assert_terminal_task_event(&callback_inbox, "task-timeout-group");
        }

        /// Rehydrated liveness is sampled once per tick and cached between
        /// ticks. The fake clock makes the 500 ms cadence deterministic while
        /// the process-probe seam proves that the supervisor does not repeat
        /// the same sample at each decision point.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn rehydrated_task_liveness_is_deduplicated_and_rate_limited() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-liveness-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                60_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-rehydrated-cache", spec, &clock);
            let mut child = running.child.take().expect("rehydrated task child");

            reset_process_probe_count();
            assert!(matches!(
                tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock,),
                Ok(TaskTick::StillRunning)
            ));
            assert_eq!(process_probe_count(), 1);

            clock.advance(100);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("cached liveness tick");
            assert_eq!(process_probe_count(), 1);

            clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 101);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("cached liveness tick before refresh");
            assert_eq!(process_probe_count(), 1);

            clock.advance(1);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("refresh liveness tick");
            assert_eq!(process_probe_count(), 2);

            signal_group(running.record.pid, "-KILL").expect("clean up rehydrated task");
            child.wait().expect("wait for rehydrated task");
        }

        /// A restarted supervisor has no Child handle, but still waits for a
        /// same-group descendant after the recorded direct PID disappears.
        /// Once the group drains, the rehydrated path records failed/null
        /// because no direct exit status can be recovered.
        #[test]
        fn rehydrated_task_waits_for_same_group_descendant_before_failure() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut owned = spawn_running_task(&dir.path, "task-rehydrated-group", spec, &clock);

            wait_for_group_descendant(&mut owned);
            owned
                .child
                .take()
                .expect("owned task child")
                .wait()
                .expect("wait for direct leader");
            let mut rehydrated = RunningTask {
                record: owned.record.clone(),
                child: None,
                #[cfg(not(target_os = "linux"))]
                rehydrated_liveness: None,
                started_ms: owned.started_ms,
                term_sent_at_ms: None,
                kill_sent: false,
                terminal_delivery_attempts: 0,
                next_terminal_retry_ms: None,
                terminal_retry_delay_ms: 0,
            };

            let tick = tick_one_task(&dir.path, "task-rehydrated-group", &mut rehydrated, &clock)
                .expect("rehydrated tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-rehydrated-group", TaskState::Running);

            signal_group(rehydrated.record.pid, "-KILL").expect("kill group descendant");
            reap_task_until_finished(
                &dir.path,
                "task-rehydrated-group",
                &mut rehydrated,
                &clock,
                tick,
            );
            let record =
                assert_durable_task_state(&dir.path, "task-rehydrated-group", TaskState::Failed);
            assert_eq!(record.exit_code, None);
            assert_terminal_task_event(&callback_inbox, "task-rehydrated-group");
        }

        // -- Session-scoped task reaping ----------------------------------

        /// `reap_session_tasks` reaps only the records owned by the given
        /// session, leaving another session's task record untouched.
        #[test]
        fn reap_session_tasks_is_scoped_to_the_owning_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("reap-scoped");
            let owned = TaskRecord {
                id: "task-owned".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb-a"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            let other = TaskRecord {
                id: "task-other".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-2", "true", vec![], vec![], 1_000, "/tmp/cb-b"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &owned).expect("write owned");
            write_task_record(&dir.path, &other).expect("write other");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            reap_session_tasks_with_wait(&mut admission, "svc-1", false, wait_while_task_alive)
                .expect("reap");

            assert!(
                read_task_record(&dir.path, "task-owned")
                    .expect("read")
                    .is_none(),
                "the owning session's task record is reaped"
            );
            assert!(
                read_task_record(&dir.path, "task-other")
                    .expect("read")
                    .is_some(),
                "a different session's task record is untouched"
            );
        }

        /// `execute_teardown` reaps every session's owned task records too,
        /// regardless of each task's own callback target — the callback is
        /// a delivery target only, never the ownership/reaping boundary.
        #[test]
        fn execute_teardown_reaps_stale_task_records_owned_by_the_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-tasks");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            let task_record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec(
                    "svc-1",
                    "true",
                    vec![],
                    vec![],
                    1_000,
                    // A callback role/inbox outside the owning session's own
                    // mailbox — still reaped, since ownership is by session,
                    // not by callback target.
                    "/tmp/some-other-roles-inbox",
                ),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &task_record).expect("write task");

            let mut out = Vec::new();
            execute_teardown(&dir.path, false, &mut out).expect("teardown");

            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none()
            );
            assert!(
                read_task_record(&dir.path, "task-1")
                    .expect("read")
                    .is_none(),
                "the session's task record is reaped too"
            );
        }

        /// Spawns a real long-lived child to stand in for a live task, and
        /// returns it. `SIGTERM` ends it, so a reap ladder run against its
        /// record terminates within the first escalation round.
        #[cfg(target_os = "linux")]
        fn spawn_live_task_child(control: &Path, task_id: &str) -> Child {
            let log_dir = task_logs_dir(control, task_id);
            fs::create_dir_all(&log_dir).expect("create task log dir");
            spawn_task_child(
                &live_task_spec("svc-1"),
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn live task child")
        }

        #[cfg(target_os = "linux")]
        fn live_task_spec(session: &str) -> TaskSpec {
            task_spec(
                session,
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                Vec::new(),
                60_000,
                "/tmp/cb",
            )
        }

        /// A committed `Running` record pinned to `pid`'s corroborated start
        /// identity, so `is_task_alive` reports it `Live`.
        #[cfg(target_os = "linux")]
        fn live_task_record(session: &str, task_id: &str, pid: u32) -> TaskRecord {
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            TaskRecord {
                id: task_id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: live_task_spec(session),
                pid,
                started_at,
                start_epoch_secs,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            }
        }

        /// A session whose cleanup a live stop owns is not an admissible task
        /// owner, even while its process still probes live **and its
        /// cooperative `serve.stop` sentinel has already been consumed**.
        /// That interleaving is the whole reason the marker is durable:
        /// `poll_stop` removes the sentinel as soon as the daemon observes
        /// it, long before the process exits, so a start landing in between
        /// would otherwise see neither a sentinel nor a dead owner and be
        /// handed a task id for a process the stop is about to kill.
        ///
        /// The rejection is driven from inside the stop's own released grace
        /// window, so the interleaving is exercised as it really occurs
        /// rather than simulated.
        #[cfg(target_os = "linux")]
        #[test]
        fn task_start_is_rejected_while_a_stop_owns_the_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("admit-stopping");
            let inbox = dir.path.join("inbox");
            fs::create_dir_all(&inbox).expect("create inbox");

            // A real child stands in for the session, so admission's own
            // liveness check keeps saying `Live` right up until the stop's
            // escalation ladder ends it.
            let mut child = spawn_live_task_child(&dir.path, "svc-stopping");
            let (started_at, start_epoch_secs) = recorded_start_identity(child.id());
            let session_record = SessionRecord {
                id: "svc-stopping".to_string(),
                spec: spec(
                    &inbox.display().to_string(),
                    &dir.path.join("outbox").display().to_string(),
                ),
                pid: child.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };
            assert_eq!(
                is_session_alive(&session_record),
                Liveness::Live,
                "fixture session is live before the stop begins"
            );
            write_session_record(&dir.path, &session_record).expect("write session");

            let spec_path = dir.path.join("racing-spec.json");
            fs::write(
                &spec_path,
                serde_json::to_string(&task_spec(
                    "svc-stopping",
                    "true",
                    Vec::new(),
                    Vec::new(),
                    1_000,
                    &dir.path.join("callback").display().to_string(),
                ))
                .expect("serialize spec"),
            )
            .expect("write spec");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let racing_outcome = std::cell::RefCell::new(None);
            let _ = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {
                    // Only the first grace window: later escalation rounds
                    // run after `SIGTERM`, when the owner is no longer live
                    // and the plain liveness check would reject on its own.
                    if racing_outcome.borrow().is_some() {
                        return;
                    }
                    // Inside the released grace window. Consume the sentinel
                    // exactly as the daemon's `poll_stop` would, then let a
                    // start race: neither a sentinel nor a dead owner is
                    // observable at this instant.
                    let _ = fs::remove_file(inbox.join("serve.stop"));
                    assert!(
                        !inbox.join("serve.stop").is_file(),
                        "the cooperative sentinel is consumed, as poll_stop leaves it"
                    );
                    assert_eq!(
                        is_session_alive(&session_record),
                        Liveness::Live,
                        "the owner process is still live at this instant"
                    );
                    *racing_outcome.borrow_mut() = Some(
                        handle_task_start_request(
                            &dir.path,
                            "racing-request",
                            &spec_path,
                            &SystemClock,
                        )
                        .expect("handle racing start"),
                    );
                },
                |_, _| {},
            );
            drop(admission);

            assert!(
                racing_outcome
                    .into_inner()
                    .expect("the racing start ran inside the grace window")
                    .is_none(),
                "the racing start is rejected, not spawned"
            );
            let response: TaskStartResponse = serde_json::from_str(
                &fs::read_to_string(
                    task_start_response_path(&dir.path, "racing-request").expect("response path"),
                )
                .expect("task start response"),
            )
            .expect("decode task start response");
            assert!(response.task_id.is_none(), "no task id is handed out");
            assert!(
                response
                    .error
                    .as_deref()
                    .unwrap_or_default()
                    .contains("does not name a live managed session"),
                "the rejection names the owner failure: {:?}",
                response.error
            );
            assert!(
                !session_stop_in_progress(&dir.path, "svc-stopping").expect("probe"),
                "the finished stop released its marker"
            );
            child.wait().expect("reap session stand-in");
        }

        /// The stop marker is released when the stop finishes, and a marker
        /// orphaned by a killed `service stop` cannot wedge admission: a
        /// reader whose recorded identity no longer resolves discards it.
        #[test]
        fn a_stale_session_stop_marker_does_not_wedge_admission() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stale-stop-marker");

            {
                let _claim = SessionStopGuard::claim(&dir.path, "svc-1").expect("claim");
                assert!(
                    session_stop_in_progress(&dir.path, "svc-1").expect("probe"),
                    "a live stop owns the session"
                );
            }
            assert!(
                !session_stop_in_progress(&dir.path, "svc-1").expect("probe"),
                "finishing the stop releases the marker"
            );

            let dir_path = session_stop_markers_dir(&dir.path);
            fs::create_dir_all(&dir_path).expect("create marker dir");
            let orphan = serde_json::to_string(&SessionStopMarker {
                pid: u32::MAX - 1,
                started_at: Some("never-resolves".to_string()),
                start_epoch_secs: Some(1),
            })
            .expect("serialize orphan");
            mailbox::atomic_write(&dir_path, &mailbox::file_name("svc-1"), &orphan)
                .expect("write orphan marker");
            assert!(
                !session_stop_in_progress(&dir.path, "svc-1").expect("probe"),
                "a marker whose owner no longer resolves is stale"
            );
            assert!(
                !session_stop_marker_path(&dir.path, "svc-1")
                    .expect("marker path")
                    .exists(),
                "and is cleared, so it costs at most one rejected start"
            );

            mailbox::atomic_write(&dir_path, &mailbox::file_name("svc-1"), "not json")
                .expect("write malformed marker");
            assert!(
                !session_stop_in_progress(&dir.path, "svc-1").expect("probe"),
                "a malformed marker is not evidence of a live stop"
            );
        }

        /// The session grace window must not hold the admission lock: while a
        /// non-force stop is waiting out `STOP_GRACE_MS`, another party can
        /// still take `service.admission.lock`. Before #195 the lock was held
        /// for the whole stop, freezing the supervisor's request admission and
        /// every task-start client for up to the sum of the grace windows.
        #[cfg(target_os = "linux")]
        #[test]
        fn stop_grace_does_not_hold_the_admission_lock() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-grace-unlocked");
            let inbox = dir.path.join("inbox");
            fs::create_dir_all(&inbox).expect("create session inbox");

            // Hold the session mailbox lock, so the stop's cooperative
            // `mailbox::request_stop` takes its "a daemon is live" branch and
            // writes the `serve.stop` sentinel. That write happens under the
            // admission lock, immediately before the grace wait — it is the
            // barrier proving the stop already holds the lock.
            let mailbox_lock = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(inbox.join("serve.lock"))
                .expect("open mailbox lock");
            mailbox_lock.lock().expect("hold mailbox lock");

            let task_specification = task_spec(
                "svc-grace",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                Vec::new(),
                60_000,
                "/tmp/callback",
            );
            let log_dir = task_logs_dir(&dir.path, "grace-session");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let mut child = spawn_task_child(
                &task_specification,
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn session stand-in");
            let (started_at, start_epoch_secs) = recorded_start_identity(child.id());
            let session_record = SessionRecord {
                id: "svc-grace".to_string(),
                spec: spec(
                    &inbox.display().to_string(),
                    &dir.path.join("outbox").display().to_string(),
                ),
                pid: child.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };
            assert_eq!(
                is_session_alive(&session_record),
                Liveness::Live,
                "fixture session is live, so the stop enters its grace window"
            );
            write_session_record(&dir.path, &session_record).expect("write session record");

            let control = dir.path.clone();
            let stopper = std::thread::spawn(move || {
                let mut out = Vec::new();
                execute_stop(&control, "svc-grace", false, &mut out)
            });

            let sentinel = inbox.join("serve.stop");
            let barrier = Instant::now() + Duration::from_secs(10);
            while !sentinel.exists() && Instant::now() < barrier {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                sentinel.exists(),
                "the stop reached its cooperative request, so it holds the admission lock"
            );

            // Non-blocking on purpose: a blocking acquire would simply wait
            // the grace out and pass against the old implementation too.
            let probe = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.path.join(ADMISSION_LOCK_FILE))
                .expect("open admission lock");
            let deadline = Instant::now() + Duration::from_millis(STOP_GRACE_MS / 2);
            let mut acquired = false;
            while Instant::now() < deadline {
                if probe.try_lock().is_ok() {
                    acquired = true;
                    probe.unlock().expect("release probe lock");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                acquired,
                "admission stays available while the stop waits out its grace window"
            );

            stopper.join().expect("join stopper").expect("stop session");
            assert!(
                read_session_record(&dir.path, "svc-grace")
                    .expect("read")
                    .is_none(),
                "the session is still stopped and its record removed"
            );
            child.wait().expect("reap session stand-in");
            drop(mailbox_lock);
        }

        /// A task start admitted while a grace wait had the admission lock
        /// released lands outside the reaper's snapshot. The final locked
        /// rescan reports a still-live racer as residue instead of dropping
        /// it, and the session record is retained.
        #[cfg(target_os = "linux")]
        #[test]
        fn rescan_reports_a_task_admitted_during_a_released_grace_wait() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rescan-live-racer");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            // A live record the reaper does see, so it reaches its grace
            // wait — the point at which the racing admission lands. `SIGTERM`
            // ends this one, so it leaves no residue of its own.
            let mut reaped_child = spawn_live_task_child(&dir.path, "task-reaped");
            let reaped = live_task_record("svc-1", "task-reaped", reaped_child.id());
            write_task_record(&dir.path, &reaped).expect("write reaped task");

            let mut child = spawn_live_task_child(&dir.path, "task-racer");
            let racer = live_task_record("svc-1", "task-racer", child.id());

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                // Stands in for a task start admitted while this wait had the
                // admission lock released.
                |_, _| {
                    write_task_record(&dir.path, &racer).expect("admit racing task");
                },
            )
            .expect("stop session");
            drop(admission);

            assert!(
                residue.iter().any(|entry| entry.id == "task-racer"),
                "the racing task is reported, not silently dropped: {residue:?}"
            );
            assert!(
                read_task_record(&dir.path, "task-racer")
                    .expect("read")
                    .is_some(),
                "the racing task record is retained for a later cleanup attempt"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_some(),
                "the session record is not removed while an owned task is outstanding"
            );

            signal_group(child.id(), "-KILL").expect("kill racer");
            child.wait().expect("reap racer");
            reaped_child
                .wait()
                .expect("reap the terminated fixture task");
        }

        /// The supervisor can tick a racing task to a terminal state before
        /// the rescan looks. Such a record gets the same cleanup the reaper
        /// applies to any terminal record — and, with nothing else
        /// outstanding, the session record is still removed, so the rescan
        /// does not block the success path.
        #[cfg(target_os = "linux")]
        #[test]
        fn rescan_cleans_up_a_terminal_task_admitted_during_a_released_grace_wait() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rescan-terminal-racer");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            // A live record the reaper does see, so it reaches its grace
            // wait — the point at which the racing admission lands. `SIGTERM`
            // ends this one, so it leaves no residue of its own.
            let mut reaped_child = spawn_live_task_child(&dir.path, "task-reaped");
            let reaped = live_task_record("svc-1", "task-reaped", reaped_child.id());
            write_task_record(&dir.path, &reaped).expect("write reaped task");

            let request_id = "racer-request";
            let racer = TaskRecord {
                id: "task-racer".to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(1),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                |_, _| {
                    write_task_record(&dir.path, &racer).expect("admit racing task");
                    mark_task_start_ack(&dir.path, request_id).expect("write acknowledgement");
                    request_task_cancel_sentinel(&dir.path, &racer.id).expect("write sentinel");
                },
            )
            .expect("stop session");
            drop(admission);

            assert!(residue.is_empty(), "nothing is outstanding: {residue:?}");
            assert!(
                read_task_record(&dir.path, "task-racer")
                    .expect("read")
                    .is_none(),
                "the terminal racing record is removed"
            );
            assert!(
                !task_start_ack_exists(&dir.path, request_id).expect("probe acknowledgement"),
                "its start transaction is removed too"
            );
            assert!(
                !task_cancel_sentinel_path(&dir.path, "task-racer").exists(),
                "its cancel sentinel is removed too"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none(),
                "the rescan does not block the success path"
            );
            reaped_child
                .wait()
                .expect("reap the terminated fixture task");
        }

        /// Multiple unresolved task records are retained without entering a
        /// grace wait or signalling their uncorroborated process groups.
        #[cfg(target_os = "linux")]
        #[test]
        fn reap_session_tasks_retains_multiple_unresolved_without_waiting() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("reap-unresolved-fast");
            let mut children = Vec::new();
            let mut task_ids = Vec::new();

            for index in 0..3 {
                let task_id = format!("task-unresolved-{index}");
                let task_specification = task_spec(
                    "svc-1",
                    "bash",
                    vec!["-c".to_string(), "exec sleep 30".to_string()],
                    vec![],
                    60_000,
                    "/tmp/callback",
                );
                let log_dir = task_logs_dir(&dir.path, &task_id);
                fs::create_dir_all(&log_dir).expect("create task log dir");
                let stdout_path = log_dir.join("stdout.log");
                let stderr_path = log_dir.join("stderr.log");
                let child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                    .expect("spawn task");
                let task_record = TaskRecord {
                    id: task_id.clone(),
                    request_id: None,
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_specification,
                    pid: child.id(),
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Running,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout_path: stdout_path.display().to_string(),
                    stderr_path: stderr_path.display().to_string(),
                    delivered_milestones: 0,
                };
                write_task_record(&dir.path, &task_record).expect("write unresolved task");

                // Wait only for bash to exec-replace its argv so the fixture
                // reaches the intended unresolved identity state.
                let deadline = Instant::now() + Duration::from_secs(10);
                while is_task_alive(&task_record) != Liveness::Unresolved
                    && Instant::now() < deadline
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                assert_eq!(
                    is_task_alive(&task_record),
                    Liveness::Unresolved,
                    "fixture reaches the unresolved identity state"
                );

                task_ids.push(task_id);
                children.push(child);
            }

            let wait_calls = std::cell::Cell::new(0);
            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = reap_session_tasks_with_wait(&mut admission, "svc-1", false, |_, _| {
                wait_calls.set(wait_calls.get() + 1)
            })
            .expect("reap unresolved tasks");

            assert_eq!(wait_calls.get(), 0, "unresolved tasks skip grace waits");
            assert_eq!(residue.len(), task_ids.len());
            assert!(
                residue
                    .iter()
                    .all(|item| { item.kind == "task" && item.liveness == Liveness::Unresolved })
            );
            for (task_id, child) in task_ids.iter().zip(children.iter_mut()) {
                assert!(
                    read_task_record(&dir.path, task_id)
                        .expect("read retained task")
                        .is_some(),
                    "unresolved task record remains durable"
                );
                assert!(
                    child.try_wait().expect("poll unresolved task").is_none(),
                    "unresolved task process remains unsignalled"
                );
            }

            for mut child in children {
                signal_group(child.id(), "-KILL").expect("kill unresolved task");
                child.wait().expect("wait for unresolved task");
            }
        }

        /// Ordinary teardown keeps an unresolved rehydrated task and its
        /// owning session record so a later forced teardown still has a
        /// durable cleanup boundary. `--force` then removes the record and
        /// signals the asserted PID group.
        #[cfg(target_os = "linux")]
        #[test]
        fn teardown_preserves_unresolved_task_until_force() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-unresolved");
            let request_id = "task-unresolved-request";
            let task_specification = task_spec(
                "svc-1",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let log_dir = task_logs_dir(&dir.path, "task-unresolved");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                .expect("spawn task");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            let task_record = TaskRecord {
                id: "task-unresolved".to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Prepared,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            write_task_record(&dir.path, &task_record).expect("write unresolved task");
            write_task_start_response(
                &dir.path,
                request_id,
                &TaskStartResponse {
                    task_id: Some(task_record.id.clone()),
                    error: None,
                },
            )
            .expect("write unresolved task response");
            mark_task_start_ack(&dir.path, request_id).expect("write unresolved task ack");
            mark_task_start_rollback(&dir.path, request_id)
                .expect("write unresolved task rollback");

            let deadline = Instant::now() + Duration::from_secs(10);
            while is_task_alive(&task_record) != Liveness::Unresolved && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(is_task_alive(&task_record), Liveness::Unresolved);

            let mut status = Vec::new();
            execute_task_status(&dir.path, Some("task-unresolved"), &mut status)
                .expect("status unresolved task");
            let status: serde_json::Value = serde_json::from_slice(&status).expect("status JSON");
            assert_eq!(status["tasks"][0]["live"], false);
            assert_eq!(status["tasks"][0]["liveness"], "unresolved");

            let mut out = Vec::new();
            assert!(
                execute_teardown(&dir.path, false, &mut out).is_err(),
                "ordinary teardown reports unresolved residue"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read preserved session")
                    .is_some()
            );
            assert!(
                read_task_record(&dir.path, "task-unresolved")
                    .expect("read preserved task")
                    .is_some()
            );
            assert!(
                child.try_wait().expect("poll unresolved task").is_none(),
                "ordinary teardown does not signal unresolved identity"
            );

            execute_teardown(&dir.path, true, &mut Vec::new()).expect("forced teardown");
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read forced session")
                    .is_none()
            );
            assert!(
                read_task_record(&dir.path, "task-unresolved")
                    .expect("read forced task")
                    .is_none()
            );
            assert!(
                !task_start_rollback_exists(&dir.path, request_id).expect("read forced rollback"),
                "forced teardown removes the rollback marker"
            );
            assert!(
                !task_start_response_boundary_exists(&dir.path, request_id)
                    .expect("read forced response boundary"),
                "forced teardown removes the response boundary"
            );
            let _ = child.wait();
        }

        // -- Task CLI-facing operations -----------------------------------

        /// `execute_task_status` reports a written task record's liveness
        /// and fields.
        #[test]
        fn execute_task_status_reports_task_fields() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-status");
            let record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(42),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &record).expect("write");

            let mut out = Vec::new();
            execute_task_status(&dir.path, None, &mut out).expect("status");
            let json: serde_json::Value = serde_json::from_slice(&out).expect("json");
            let tasks = json["tasks"].as_array().unwrap();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0]["id"], "task-1");
            assert_eq!(tasks[0]["session"], "svc-1");
            assert_eq!(tasks[0]["state"], "completed");
            assert_eq!(tasks[0]["liveness"], "dead");
            assert_eq!(tasks[0]["exit_code"], 0);
            assert_eq!(tasks[0]["elapsed_ms"], 42);
        }

        /// `execute_task_cancel` on an unknown task id is a no-op success
        /// (idempotent).
        #[test]
        fn execute_task_cancel_unknown_task_is_idempotent_success() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("cancel-unknown");
            let mut out = Vec::new();
            execute_task_cancel(&dir.path, "nope", &mut out).expect("cancel");
            assert!(
                String::from_utf8(out)
                    .unwrap()
                    .contains("nothing to cancel")
            );
        }

        /// Cancelling a running task writes the cooperative cancel sentinel
        /// and kills its process group; a subsequent reap (driven directly
        /// via `tick_one_task`, standing in for `Run`'s own tick) attributes
        /// the exit to `cancelled`, not `failed`.
        #[test]
        fn cancel_then_tick_marks_task_cancelled() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("cancel-then-tick");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["5".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-cancel", spec, &clock);

            let mut out = Vec::new();
            execute_task_cancel(&dir.path, "task-cancel", &mut out).expect("cancel");

            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match tick_one_task(&dir.path, "task-cancel", &mut running, &clock).expect("tick") {
                    TaskTick::Finished => break,
                    TaskTick::StillRunning => {
                        assert!(Instant::now() < deadline, "task did not exit in time");
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    other => panic!("unexpected terminal delivery outcome: {other:?}"),
                }
            }
            assert_eq!(running.record.state, TaskState::Cancelled);
        }
    }
}

#[cfg(windows)]
#[path = "service_windows.rs"]
mod imp;

#[cfg(windows)]
pub fn adopt_windows_service_job() -> Result<()> {
    imp::adopt_service_job()
}
