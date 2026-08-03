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
//!   its effective spec and real PID. `Status`/`Stop`/`Teardown` read this
//!   directly and act on the OS process by PID; none of them need the `Run`
//!   loop to be alive, so a session started by a since-crashed `Run` can still
//!   be inspected, stopped, or torn down.
//!
//! ## Ownership boundary
//!
//! A spawned `baton serve` child is never `wait()`-ed by its short-lived
//! submitter — only `Run` holds the [`std::process::Child`] handle, reaping it
//! (via non-blocking [`std::process::Child::try_wait`]) as its loop ticks, so
//! it never leaks a zombie while it stays alive. If `Run` itself exits or
//! crashes, the kernel reparents its still-running children to init, which
//! reaps them on their own eventual exit — so a `Run` restart (e.g. systemd
//! `Restart=on-failure`) never orphans a zombie either. Killing a session
//! (`Stop`/`Teardown`) targets its **process group**: [`spawn_serve_child`]
//! makes each `baton serve` child its own group leader
//! (`std::os::unix::process::CommandExt::process_group(0)`, safe and stable —
//! deliberately not `pre_exec(setsid)`, which would require `unsafe`), so one
//! `kill -- -<pid>` reaches both the `serve` process and its in-flight
//! `agent-cmd` grandchild.
//!
//! ## Host support
//!
//! Unix only (Linux and macOS): process groups and the `kill` escalation this
//! module relies on have no equivalent in this crate's dependency-free design
//! on Windows. [`execute_service`] fails clearly there rather than silently
//! degrading. The systemd user-service integration (`packaging/systemd/`) and
//! macOS LaunchAgent integration (`packaging/launchd/`) are external to this
//! binary; a Windows host-service integration is tracked separately.

use std::io::Write;
#[cfg(unix)]
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
#[cfg(unix)]
pub fn execute_service(cmd: ServiceCommand, out: impl Write) -> Result<()> {
    imp::dispatch(cmd, out)
}

/// `baton service` has no supported implementation on this host: process
/// groups and the `kill`-based escalation this module relies on have no
/// equivalent in this crate's dependency-free design on Windows. Fails
/// clearly rather than silently falling back to an ownership guarantee (e.g.
/// bare `setsid`/`disown`) this host cannot actually provide.
#[cfg(not(unix))]
pub fn execute_service(cmd: ServiceCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton service requires a Unix host (Linux or macOS); Windows support is tracked in a follow-up issue".to_string(),
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
#[cfg(unix)]
pub fn execute_task(cmd: TaskCommand, out: impl Write) -> Result<()> {
    imp::dispatch_task(cmd, out)
}

/// `baton task` has no supported implementation on this host; see
/// [`execute_service`]'s non-Unix stub for why.
#[cfg(not(unix))]
pub fn execute_task(cmd: TaskCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton task requires a Unix host (Linux or macOS); Windows support is tracked in a follow-up issue".to_string(),
    ))
}

#[cfg(unix)]
mod imp {
    use super::*;
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
    }

    /// The `Start` response body, keyed by request id in `responses/`.
    #[derive(Debug, Serialize, Deserialize)]
    struct StartResponse {
        session_id: String,
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
                return Ok(resp.session_id);
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
                    let (record, child) = outcome?;
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

    /// Spawns the requested session, persists its [`SessionRecord`], and
    /// answers the request with its session id.
    fn handle_start_request(
        control: &Path,
        request_id: &str,
        spec_path: &Path,
    ) -> Result<(SessionRecord, Child)> {
        let data = fs::read_to_string(spec_path)
            .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
        let spec: SessionSpec = serde_json::from_str(&data).map_err(|err| {
            BatonError::Decode(format!("malformed session spec {spec_path:?}: {err}"))
        })?;
        let mut child = spawn_serve_child(&spec)?;
        let pid = child.id();
        let started_at = recorded_start_key(pid);
        // Everything below this point must kill+reap `child` before
        // returning `Err`: once this function returns an error, nothing else
        // ever tracks this `Child` (it isn't inserted into `Run`'s
        // `children` map, and `Drop` for `std::process::Child` does not
        // kill), so leaving it running here would leak a live, unrecorded,
        // unreapable `serve` process.
        if !spawn_start_key_ok(&started_at) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            return Err(BatonError::Io(format!(
                "baton serve (pid {pid}) could not be corroborated right after spawn; treating as a spawn failure"
            )));
        }
        let record = SessionRecord {
            id: fresh_session_id(),
            spec,
            pid,
            started_at,
        };
        if let Err(err) = write_session_record(control, &record) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            return Err(err);
        }
        let response = StartResponse {
            session_id: record.id.clone(),
        };
        let respond = serde_json::to_string(&response)
            .map_err(|err| BatonError::Io(format!("could not serialize start response: {err}")))
            .and_then(|json| {
                let responses = responses_dir(control);
                fs::create_dir_all(&responses).map_err(|err| {
                    BatonError::Io(format!("could not create {responses:?}: {err}"))
                })?;
                mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
            });
        if let Err(err) = respond {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = remove_session_record(control, &record.id);
            return Err(err);
        }
        Ok((record, child))
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
    /// (`pgid == pid`), detached from this process's stdio, and returns the
    /// live [`Child`] without waiting on it — `Run`'s loop reaps it later.
    fn spawn_serve_child(spec: &SessionSpec) -> Result<Child> {
        let exe = current_baton_exe()?;
        let mut command = Command::new(&exe);
        command.args(serve_argv(spec));
        command.stdin(Stdio::null());
        command.stdout(Stdio::null());
        command.stderr(Stdio::null());
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
                            "BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER",
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
                wait_for_test_task_rollback_cleanup_barrier(
                    "BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER",
                );
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
                wait_for_test_task_rollback_cleanup_barrier(
                    "BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER",
                );
            }
            if !retained_rollbacks.contains(&request_id) {
                remove_task_start_rollback(control, &request_id)?;
            }
        }
        Ok(())
    }

    fn abort_task_admission(control: &Path, record: &TaskRecord) -> Result<bool> {
        if record.state == TaskState::Running {
            let mut liveness = is_task_alive(record);
            if liveness == Liveness::Unresolved {
                return Ok(false);
            }
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                wait_while_task_alive(record, KILL_GRACE_MS);
                liveness = is_task_alive(record);
                if liveness == Liveness::Live {
                    let _ = signal_group(record.pid, "-KILL");
                    wait_while_task_alive(record, KILL_GRACE_MS);
                    liveness = is_task_alive(record);
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
        started_ms: u64,
        /// Set once this task's max duration has been exceeded and `SIGTERM`
        /// sent, so a later tick knows to escalate to `SIGKILL` after
        /// `KILL_GRACE_MS`, and a successful reap after this is set is
        /// attributed to `timeout`, not `completed`/`failed`.
        term_sent_at_ms: Option<u64>,
        /// Set once `SIGKILL` has been sent, so it is only ever sent once.
        kill_sent: bool,
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
                    started_ms,
                    term_sent_at_ms: None,
                    kill_sent: false,
                },
            );
        }
        Ok(tasks)
    }

    /// Outcome of one [`tick_one_task`] call.
    enum TaskTick {
        StillRunning,
        Finished,
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
                                "BATON_TEST_TASK_ROLLBACK_REQUEST_BARRIER",
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
                        started_ms,
                        term_sent_at_ms: None,
                        kill_sent: false,
                    };
                    return Ok(Some((id, running)));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(BatonError::Io(format!("could not claim {path:?}: {err}"))),
            }
        }
        Ok(None)
    }

    /// Validates the requested owner, then spawns the task, persists its
    /// [`TaskRecord`], and answers the request with its task id. Mirrors
    /// [`handle_start_request`]'s
    /// kill-and-unwind-on-any-later-failure discipline until the committed
    /// record is durable. After that point the task remains tracked when
    /// response delivery or phase persistence fails, so restart reconciliation
    /// can retry the response boundary without spawning again.
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
        let owner_live = if mailbox::is_safe_key(&spec.session) {
            read_session_record(control, &spec.session)?
                .map(|record| is_session_alive(&record) == Liveness::Live)
                .unwrap_or(false)
        } else {
            false
        };
        if !owner_live {
            let error = format!(
                "task start rejected: --session {:?} does not name a live managed session on {:?} (the session record is absent or its process is no longer live)",
                spec.session, control
            );
            write_task_start_response(
                control,
                request_id,
                &TaskStartResponse {
                    task_id: None,
                    error: Some(error),
                },
            )?;
            return Ok(None);
        }
        let task_id = fresh_task_id();
        let log_dir = task_logs_dir(control, &task_id);
        fs::create_dir_all(&log_dir)
            .map_err(|err| BatonError::Io(format!("could not create {log_dir:?}: {err}")))?;
        let stdout_path = log_dir.join("stdout.log");
        let stderr_path = log_dir.join("stderr.log");
        let mut child = spawn_task_child(&spec, &stdout_path, &stderr_path)?;
        let pid = child.id();
        let started_at = recorded_start_key(pid);
        if !spawn_start_key_ok(&started_at) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            return Err(BatonError::Io(format!(
                "task command (pid {pid}) could not be corroborated right after spawn; treating as a spawn failure"
            )));
        }
        let started_ms = clock.now_ms();
        let mut record = TaskRecord {
            id: task_id,
            request_id: Some(request_id.to_string()),
            admission: TaskAdmissionPhase::Prepared,
            spec,
            pid,
            started_at,
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
            return Err(err);
        }
        wait_for_test_task_admission_barrier();
        record.admission = TaskAdmissionPhase::Committed;
        if let Err(err) = write_task_record(control, &record) {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = remove_task_record(control, &record.id);
            return Err(err);
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
    /// production callers never set it.
    fn wait_for_test_task_admission_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_ADMISSION_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Test-only synchronization seam for the response/phase boundary. A
    /// service launched with this environment variable waits after publishing
    /// the response while still holding the admission lock; production callers
    /// never set it.
    fn wait_for_test_task_response_phase_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_RESPONSE_PHASE_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Test-only synchronization seam for the response claim/ack boundary. A
    /// task-start client waits after persisting its acknowledgement and before
    /// removing the private claim; production callers never set it.
    fn wait_for_test_task_start_ack_barrier() {
        let Some(path) = std::env::var_os("BATON_TEST_TASK_START_ACK_BARRIER") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Test-only synchronization seam for rollback cleanup ordering. A
    /// service launched with one of the named environment variables waits
    /// after request/record cleanup and before removing the rollback marker;
    /// production callers never set it.
    fn wait_for_test_task_rollback_cleanup_barrier(variable: &str) {
        let Some(path) = std::env::var_os(variable) else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        while path.exists() {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    fn write_task_start_response(
        control: &Path,
        request_id: &str,
        response: &TaskStartResponse,
    ) -> Result<()> {
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
            deliver_task_event(&running.record, TaskEventKind::Terminal)?;
            return Ok(TaskTick::Finished);
        }

        let elapsed_ms = clock.now_ms().saturating_sub(running.started_ms);

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
        if running.child.is_none() {
            match is_task_alive(&running.record) {
                Liveness::Dead => {
                    let cancelled = consume_task_cancel_sentinel(control, id)?;
                    let state = if cancelled {
                        TaskState::Cancelled
                    } else if running.term_sent_at_ms.is_some() {
                        TaskState::Timeout
                    } else {
                        TaskState::Failed
                    };
                    return finalize_task(control, running, state, None, elapsed_ms);
                }
                Liveness::Live => {}
                Liveness::Unresolved => return Ok(TaskTick::StillRunning),
            }
        }

        if running.term_sent_at_ms.is_none()
            && max_duration_exceeded(elapsed_ms, running.record.spec.max_duration_ms)
        {
            let _ = signal_group(running.record.pid, "-TERM");
            running.term_sent_at_ms = Some(clock.now_ms());
        } else if let Some(term_at) = running.term_sent_at_ms
            && !running.kill_sent
            && clock.now_ms().saturating_sub(term_at) >= KILL_GRACE_MS
        {
            if running.child.is_none() {
                match is_task_alive(&running.record) {
                    Liveness::Dead => {
                        let cancelled = consume_task_cancel_sentinel(control, id)?;
                        let state = if cancelled {
                            TaskState::Cancelled
                        } else {
                            TaskState::Timeout
                        };
                        return finalize_task(control, running, state, None, elapsed_ms);
                    }
                    Liveness::Live => {}
                    Liveness::Unresolved => return Ok(TaskTick::StillRunning),
                }
            }
            let _ = signal_group(running.record.pid, "-KILL");
            running.kill_sent = true;
        }

        match running.child.as_mut() {
            None => match is_task_alive(&running.record) {
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
                    finalize_task(control, running, state, None, elapsed_ms)
                }
            },
            Some(child) => match child.try_wait() {
                Ok(Some(status)) => {
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
                    finalize_task(control, running, state, status.code(), elapsed_ms)
                }
                Ok(None) => Ok(TaskTick::StillRunning),
                Err(err) => Err(BatonError::Io(format!("could not poll task {id}: {err}"))),
            },
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
    ) -> Result<TaskTick> {
        let previous = running.record.clone();
        running.record.state = state;
        running.record.exit_code = exit_code;
        running.record.elapsed_ms = Some(elapsed_ms);
        if let Err(err) = write_task_record(control, &running.record) {
            running.record = previous;
            return Err(err);
        }
        deliver_task_event(&running.record, TaskEventKind::Terminal)?;
        Ok(TaskTick::Finished)
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

    /// Cancels and reaps every task owned by `session_id`, regardless of
    /// each task's own callback target — the callback mailbox/role is a
    /// delivery target only, never the ownership or reaping boundary. Called
    /// from [`stop_session_record`], so this runs on both `Stop <session>`
    /// and `Teardown` (which stops every session). Unresolved records survive
    /// unless `force` is set.
    fn reap_session_tasks(
        control: &Path,
        session_id: &str,
        force: bool,
    ) -> Result<Vec<CleanupResidue>> {
        reap_session_tasks_with_wait(control, session_id, force, wait_while_task_alive)
    }

    fn reap_session_tasks_with_wait(
        control: &Path,
        session_id: &str,
        force: bool,
        wait: impl Fn(&TaskRecord, u64),
    ) -> Result<Vec<CleanupResidue>> {
        let mut residue = Vec::new();
        for record in list_task_records(control)? {
            if record.spec.session != session_id {
                continue;
            }
            if record.state != TaskState::Running {
                remove_task_start_transaction(control, &record)?;
                remove_task_record(control, &record.id)?;
                let _ = fs::remove_file(task_cancel_sentinel_path(control, &record.id));
                continue;
            }
            let mut liveness = is_task_alive(&record);
            if force {
                if liveness != Liveness::Dead {
                    let _ = signal_group(record.pid, "-TERM");
                    let _ = signal_group(record.pid, "-KILL");
                }
                remove_task_start_transaction(control, &record)?;
                remove_task_record(control, &record.id)?;
                let _ = fs::remove_file(task_cancel_sentinel_path(control, &record.id));
                continue;
            }
            if liveness == Liveness::Unresolved {
                residue.push(CleanupResidue {
                    kind: "task",
                    id: record.id.clone(),
                    pid: record.pid,
                    liveness,
                    argv: task_recorded_argv(&record),
                });
                continue;
            }
            let mut term_sent = false;
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                term_sent = true;
            }
            if liveness != Liveness::Dead {
                wait(&record, KILL_GRACE_MS);
                liveness = is_task_alive(&record);
            }
            if liveness == Liveness::Live && !term_sent {
                let _ = signal_group(record.pid, "-TERM");
                wait(&record, KILL_GRACE_MS);
                liveness = is_task_alive(&record);
            }
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-KILL");
                wait(&record, KILL_GRACE_MS);
                liveness = is_task_alive(&record);
            }
            if liveness == Liveness::Dead {
                remove_task_start_transaction(control, &record)?;
                remove_task_record(control, &record.id)?;
                let _ = fs::remove_file(task_cancel_sentinel_path(control, &record.id));
            } else {
                residue.push(CleanupResidue {
                    kind: "task",
                    id: record.id.clone(),
                    pid: record.pid,
                    liveness,
                    argv: task_recorded_argv(&record),
                });
            }
        }
        Ok(residue)
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
        Some(ProcessProbe {
            state: fields.first()?.to_string(),
            start_key: fields.get(19)?.to_string(),
        })
    }

    #[cfg(target_os = "linux")]
    fn process_probe(pid: u32) -> ProbeResult<ProcessProbe> {
        if pid <= 1 {
            return ProbeResult::Gone;
        }
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
    fn process_start_key(pid: u32) -> Option<String> {
        match process_probe(pid) {
            ProbeResult::Present(probe) if !probe.is_zombie() => Some(probe.start_key),
            _ => None,
        }
    }

    #[cfg(target_os = "linux")]
    fn recorded_start_key(pid: u32) -> Option<String> {
        process_start_key(pid)
    }

    /// Whether a freshly-spawned child's start key is trustworthy enough to
    /// persist. A missing key means the child was already gone or a zombie
    /// microseconds after `spawn()` — fail closed as a spawn failure.
    #[cfg(target_os = "linux")]
    fn spawn_start_key_ok(started_at: &Option<String>) -> bool {
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
    fn is_session_alive(record: &SessionRecord) -> Liveness {
        match process_probe(record.pid) {
            ProbeResult::Gone => Liveness::Dead,
            ProbeResult::Unreadable => Liveness::Unresolved,
            ProbeResult::Present(probe) if probe.is_zombie() => Liveness::Dead,
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => Liveness::Live,
                Some(_) => Liveness::Dead,
                None => match process_argv(record.pid) {
                    ProbeResult::Gone => Liveness::Dead,
                    ProbeResult::Unreadable => Liveness::Unresolved,
                    ProbeResult::Present(actual)
                        if linux_session_argv_matches(&actual, &record.spec) =>
                    {
                        Liveness::Live
                    }
                    ProbeResult::Present(_) => Liveness::Dead,
                },
            },
        }
    }

    #[cfg(target_os = "linux")]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        match process_probe(record.pid) {
            ProbeResult::Gone => Liveness::Dead,
            ProbeResult::Unreadable => Liveness::Unresolved,
            ProbeResult::Present(probe) if probe.is_zombie() => Liveness::Dead,
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => Liveness::Live,
                Some(_) => Liveness::Dead,
                None => match process_argv(record.pid) {
                    ProbeResult::Gone => Liveness::Dead,
                    ProbeResult::Unreadable => Liveness::Unresolved,
                    ProbeResult::Present(actual) if linux_task_argv_matches(&actual, record) => {
                        Liveness::Live
                    }
                    ProbeResult::Present(_) => Liveness::Unresolved,
                },
            },
        }
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
    fn process_start_key_from_probe(probe: &ProcessProbe) -> Option<String> {
        (!probe.is_zombie()).then(|| probe.start_key.clone())
    }

    #[cfg(not(target_os = "linux"))]
    fn process_start_key(pid: u32) -> Option<String> {
        match process_probe(pid) {
            ProbeResult::Present(probe) => process_start_key_from_probe(&probe),
            _ => None,
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn recorded_start_key(pid: u32) -> Option<String> {
        process_start_key(pid)
    }

    /// A missing start key after spawn means the process was already gone or
    /// a zombie, so fail closed rather than persisting an uncorroborated PID.
    #[cfg(not(target_os = "linux"))]
    fn spawn_start_key_ok(started_at: &Option<String>) -> bool {
        started_at.is_some()
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
    fn is_session_alive(record: &SessionRecord) -> Liveness {
        match process_probe(record.pid) {
            ProbeResult::Gone => Liveness::Dead,
            ProbeResult::Unreadable => Liveness::Unresolved,
            ProbeResult::Present(probe) if probe.is_zombie() => Liveness::Dead,
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => Liveness::Live,
                _ if probe.command.is_empty() => Liveness::Unresolved,
                _ if session_argv_matches(&probe.command, &record.spec) => Liveness::Live,
                _ => Liveness::Dead,
            },
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        match process_probe(record.pid) {
            ProbeResult::Gone => Liveness::Dead,
            ProbeResult::Unreadable => Liveness::Unresolved,
            ProbeResult::Present(probe) if probe.is_zombie() => Liveness::Dead,
            ProbeResult::Present(probe) => match &record.started_at {
                Some(recorded) if recorded == &probe.start_key => Liveness::Live,
                _ => task_instant_liveness(&probe, record),
            },
        }
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
        while is_task_alive(record) != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Stops one session. The caller must hold the admission lock:
    /// cooperative `serve --stop` on its inbox first,
    /// bounded wait, then `SIGTERM`/`SIGKILL` process-group escalation if
    /// still alive, then reaps every task this session owns
    /// ([`reap_session_tasks`]) and removes the session's own durable
    /// record. Idempotent — a session already gone just gets its (possibly
    /// already-absent) record, and its tasks', cleaned up. Returns any
    /// records retained because their identity remained unresolved.
    fn stop_session_record(
        control: &Path,
        record: &SessionRecord,
        force: bool,
    ) -> Result<Vec<CleanupResidue>> {
        let _ = mailbox::request_stop(&record.spec.inbox);
        let mut liveness = is_session_alive(record);
        if force {
            if liveness != Liveness::Dead {
                let _ = signal_group(record.pid, "-TERM");
                let _ = signal_group(record.pid, "-KILL");
            }
            liveness = Liveness::Dead;
        } else {
            wait_while_alive(record, STOP_GRACE_MS);
            liveness = is_session_alive(record);
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, "-TERM");
                wait_while_alive(record, KILL_GRACE_MS);
                liveness = is_session_alive(record);
                if liveness == Liveness::Live {
                    let _ = signal_group(record.pid, "-KILL");
                    wait_while_alive(record, KILL_GRACE_MS);
                    liveness = is_session_alive(record);
                }
            }
        }
        let mut residue = reap_session_tasks(control, &record.id, force)?;
        if liveness == Liveness::Dead && residue.is_empty() {
            remove_session_record(control, &record.id)?;
        } else if liveness != Liveness::Dead {
            residue.push(CleanupResidue {
                kind: "session",
                id: record.id.clone(),
                pid: record.pid,
                liveness,
                argv: session_recorded_argv(record),
            });
        }
        Ok(residue)
    }

    // -- Session records ---------------------------------------------------

    fn sessions_dir(control: &Path) -> std::path::PathBuf {
        control.join("sessions")
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
        let _admission = acquire_admission_lock(control)?;
        match read_session_record(control, session)? {
            Some(record) => {
                let residue = stop_session_record(control, &record, force)?;
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
                    is_task_alive(record)
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
        if is_task_alive(record) == Liveness::Live {
            let _ = signal_group(record.pid, "-TERM");
            wait_while_task_alive(record, KILL_GRACE_MS);
            if is_task_alive(record) == Liveness::Live {
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
        let _admission = acquire_admission_lock(control)?;
        let mut residue = Vec::new();
        for record in list_session_records(control)? {
            residue.extend(stop_session_record(control, &record, force)?);
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
        use std::sync::{Mutex, MutexGuard};

        /// Serializes every test in this module that either holds the
        /// control-plane flock directly or forks a real child process
        /// (`spawn_task_child`/`Mailbox::open`'s own lock).
        ///
        /// `fork(2)` duplicates the *whole process's* fd table across every
        /// thread, not just the caller's — so a flock another `cargo test`
        /// thread holds at that instant is briefly visible (as still held) to
        /// the forked child, and vice versa, until the child's `execve`
        /// closes its `O_CLOEXEC`-marked fds. `cargo test`'s default thread
        /// parallelism runs this module's flock-assertion tests concurrently
        /// with tests that spawn real processes, so without this guard the
        /// two occasionally race (observed empirically: a lock reads back as
        /// still held, or a fresh mailbox open is transiently refused). This
        /// never happens in production — a real `baton service run` process
        /// never shares an address space with unrelated flock-holding code —
        /// it is purely a same-process test-parallelism artifact.
        static FORK_LOCK_SERIALIZE: Mutex<()> = Mutex::new(());

        fn serialize_forks_and_locks() -> MutexGuard<'static, ()> {
            FORK_LOCK_SERIALIZE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

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
            let record = TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid,
                started_at: recorded_start_key(pid),
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
                started_ms,
                term_sent_at_ms: None,
                kill_sent: false,
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
            let replacement_lock = acquire_control_lock(&dir.path)
                .expect("descendant must not retain the owner control lock");
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
            };
            write_session_record(&dir.path, &record).expect("write");
            let read = read_session_record(&dir.path, "svc-1")
                .expect("read")
                .expect("present");
            assert_eq!(read, record);
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
            let dir = TempDir::new("teardown-stale");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
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
            assert_eq!(
                is_task_alive(&task_record),
                Liveness::Live,
                "fixture must match the live process by PID and argv"
            );

            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
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

            let deadline = Instant::now() + Duration::from_secs(2);
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
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
            };
            let deadline = Instant::now() + Duration::from_secs(2);
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
            assert!(process_start_key_from_probe(&probe).is_none());
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
                }
            }
            assert_eq!(running.record.state, TaskState::Completed);
            assert_eq!(running.record.exit_code, Some(0));
        }

        // -- Session-scoped task reaping ----------------------------------

        /// `reap_session_tasks` reaps only the records owned by the given
        /// session, leaving another session's task record untouched.
        #[test]
        fn reap_session_tasks_is_scoped_to_the_owning_session() {
            let dir = TempDir::new("reap-scoped");
            let owned = TaskRecord {
                id: "task-owned".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb-a"),
                pid: u32::MAX - 1,
                started_at: None,
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

            reap_session_tasks(&dir.path, "svc-1", false).expect("reap");

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
            let dir = TempDir::new("teardown-tasks");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
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
                let deadline = Instant::now() + Duration::from_secs(2);
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
            let residue = reap_session_tasks_with_wait(&dir.path, "svc-1", false, |_, _| {
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
            };
            let task_record = TaskRecord {
                id: "task-unresolved".to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Prepared,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
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

            let deadline = Instant::now() + Duration::from_secs(2);
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
            let dir = TempDir::new("task-status");
            let record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: u32::MAX - 1,
                started_at: None,
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
                }
            }
            assert_eq!(running.record.state, TaskState::Cancelled);
        }
    }
}
