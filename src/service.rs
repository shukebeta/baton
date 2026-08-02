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
//! - `service.stop` — the cooperative-stop sentinel `Teardown` drops for a live
//!   `Run` to observe between polls. Mirrors `serve.stop`.
//! - `requests/` / `processing/` / `responses/` — the atomic-rename request
//!   protocol `Start` uses to reach the live `Run` loop (the only operation
//!   that must run *in* the long-lived process, since spawning there is the
//!   entire point). A session-spec request is delivered into `requests/`,
//!   claimed into `processing/` by `Run`, and answered into `responses/` keyed
//!   by the request id — the same temp-file-then-`rename` idiom as
//!   [`mailbox::deliver_to`]/[`mailbox::atomic_write`], reused directly.
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
//! degrading. The systemd user-service integration (`packaging/systemd/`) is
//! Linux-specific and external to this binary; launchd (macOS) and a Windows
//! host-service integration are tracked separately.

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
    /// process-group escalation. Idempotent.
    Stop {
        /// The `--control <dir>` root.
        control: String,
        /// The session id to stop.
        session: String,
    },
    /// Stop every managed session, then request `Run`'s own cooperative stop.
    /// Idempotent, and independent of whether `Run` is currently alive.
    Teardown {
        /// The `--control <dir>` root.
        control: String,
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
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use crate::mailbox;
    use crate::message::{MessageEnvelope, MessageKind};
    use crate::task::{
        Clock, SystemClock, TaskEventBody, TaskEventKind, TaskRecord, TaskSpec, TaskState,
        max_duration_exceeded, milestones_due, task_event_id,
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

    /// A durable on-disk record of one session `Run` has spawned: enough to
    /// find, corroborate, and signal the real OS process from any later,
    /// independent `Status`/`Stop`/`Teardown` invocation.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SessionRecord {
        id: String,
        spec: SessionSpec,
        pid: u32,
        /// Linux `/proc/<pid>/stat` starttime field, corroborating `pid`
        /// against reuse after a `Run` restart; `None` where it could not be
        /// read (non-Linux Unix, or the field was unavailable).
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
            ServiceCommand::Stop { control, session } => {
                execute_stop(Path::new(&control), &session, out)
            }
            ServiceCommand::Teardown { control } => execute_teardown(Path::new(&control), out),
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
        writeln!(out, "baton service running on {}", control.display()).map_err(io_err)?;

        let clock = SystemClock;
        let mut children: HashMap<String, Child> = HashMap::new();
        let mut tasks: HashMap<String, RunningTask> = HashMap::new();
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

    /// Takes the exclusive control-plane lock, refusing a second live `Run`
    /// on the same `control`.
    fn acquire_control_lock(control: &Path) -> Result<File> {
        let lock_path = control.join(CONTROL_LOCK_FILE);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| {
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
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| {
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

    /// Probes whether a live `Run` holds `control`'s lock, without side
    /// effects (`signal = false`) or, when `signal`, dropping the stop
    /// sentinel for it to observe. Mirrors [`mailbox::request_stop`].
    fn probe_or_signal_control(control: &Path, signal: bool) -> Result<ControlLiveness> {
        let lock_path = control.join(CONTROL_LOCK_FILE);
        let lock = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
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
        match lock.try_lock() {
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

    fn tasks_dir(control: &Path) -> std::path::PathBuf {
        control.join("tasks")
    }

    fn task_logs_dir(control: &Path, task_id: &str) -> std::path::PathBuf {
        control.join("task-logs").join(task_id)
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
        let path = task_responses_dir(control).join(mailbox::file_name(request_id));
        let deadline = Instant::now() + Duration::from_millis(START_AWAIT_MS);
        loop {
            if let Ok(data) = fs::read_to_string(&path) {
                let _ = fs::remove_file(&path);
                let resp: TaskStartResponse = serde_json::from_str(&data).map_err(|err| {
                    BatonError::Decode(format!("malformed task response {path:?}: {err}"))
                })?;
                if let Some(error) = resp.error {
                    return Err(BatonError::Io(error));
                }
                return resp.task_id.ok_or_else(|| {
                    BatonError::Decode(format!(
                        "task response {path:?} contained neither a task id nor an error"
                    ))
                });
            }
            if Instant::now() >= deadline {
                return Err(BatonError::Io(format!(
                    "timed out waiting for baton service to start the task ({request_id})"
                )));
            }
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
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

    /// One task the `Run` loop is currently tracking: the live [`Child`]
    /// handle (so a non-blocking `try_wait` can reap it), its durable
    /// [`TaskRecord`] (kept in sync as milestones fire and it goes
    /// terminal), and the injected-clock timestamps driving milestone/
    /// max-duration decisions.
    struct RunningTask {
        record: TaskRecord,
        child: Child,
        started_ms: u64,
        /// Set once this task's max duration has been exceeded and `SIGTERM`
        /// sent, so a later tick knows to escalate to `SIGKILL` after
        /// `KILL_GRACE_MS`, and a successful reap after this is set is
        /// attributed to `timeout`, not `completed`/`failed`.
        term_sent_at_ms: Option<u64>,
        /// Set once `SIGKILL` has been sent, so it is only ever sent once.
        kill_sent: bool,
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
                        handle_task_start_request(control, &key, &claimed_path, clock)
                    });
                    let _ = fs::remove_file(&claimed_path);
                    let Some((record, child, started_ms)) = outcome? else {
                        return Ok(None);
                    };
                    let id = record.id.clone();
                    let running = RunningTask {
                        record,
                        child,
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
    /// kill-and-unwind-on-any-later-failure discipline: once spawned, every
    /// early return below this point kills and reaps `child` first, so a
    /// failure here never leaves an unrecorded, unreapable process behind.
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
                .map(|record| is_session_alive(&record))
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
        let record = TaskRecord {
            id: task_id,
            spec,
            pid,
            started_at,
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
        let respond = write_task_start_response(
            control,
            request_id,
            &TaskStartResponse {
                task_id: Some(record.id.clone()),
                error: None,
            },
        );
        if let Err(err) = respond {
            let _ = signal_group(pid, "-KILL");
            let _ = child.wait();
            let _ = remove_task_record(control, &record.id);
            return Err(err);
        }
        Ok(Some((record, child, clock.now_ms())))
    }

    fn write_task_start_response(
        control: &Path,
        request_id: &str,
        response: &TaskStartResponse,
    ) -> Result<()> {
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
        let elapsed_ms = clock.now_ms().saturating_sub(running.started_ms);

        for index in milestones_due(
            elapsed_ms,
            &running.record.spec.milestones_ms,
            running.record.delivered_milestones,
        ) {
            deliver_task_event(&running.record, TaskEventKind::Milestone { index })?;
            running.record.delivered_milestones = index + 1;
            write_task_record(control, &running.record)?;
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
            let _ = signal_group(running.record.pid, "-KILL");
            running.kill_sent = true;
        }

        match running.child.try_wait() {
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
                running.record.state = state;
                running.record.exit_code = status.code();
                running.record.elapsed_ms = Some(elapsed_ms);
                write_task_record(control, &running.record)?;
                deliver_task_event(&running.record, TaskEventKind::Terminal)?;
                Ok(TaskTick::Finished)
            }
            Ok(None) => Ok(TaskTick::StillRunning),
            Err(err) => Err(BatonError::Io(format!("could not poll task {id}: {err}"))),
        }
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

    /// Cancels and reaps every task owned by `session_id`, regardless of
    /// each task's own callback target — the callback mailbox/role is a
    /// delivery target only, never the ownership or reaping boundary. Called
    /// from [`stop_session_record`], so this runs on both `Stop <session>`
    /// and `Teardown` (which stops every session).
    fn reap_session_tasks(control: &Path, session_id: &str) -> Result<()> {
        for record in list_task_records(control)? {
            if record.spec.session != session_id {
                continue;
            }
            if is_task_alive(&record) {
                let _ = signal_group(record.pid, "-TERM");
                wait_while_task_alive(&record, KILL_GRACE_MS);
                if is_task_alive(&record) {
                    let _ = signal_group(record.pid, "-KILL");
                    wait_while_task_alive(&record, KILL_GRACE_MS);
                }
            }
            remove_task_record(control, &record.id)?;
            let _ = fs::remove_file(task_cancel_sentinel_path(control, &record.id));
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

    /// Reads `/proc/<pid>/stat`'s state and starttime fields: `None` for a
    /// gone or zombie process, `Some(starttime)` (clock ticks since boot) for
    /// a live one — a value stable across the process's lifetime, so it
    /// corroborates `pid` against reuse.
    #[cfg(target_os = "linux")]
    fn process_start_key(pid: u32) -> Option<String> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // The executable name is `(comm)` and may itself contain `)` or
        // whitespace, so fields are counted from the *last* `)`, not split
        // naively — the same care `ps`/`procps` takes.
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        // `fields[0]` is field 3 (state) overall; starttime is field 22
        // overall, i.e. `fields[19]`.
        if fields.first() == Some(&"Z") {
            return None;
        }
        fields.get(19).map(|s| s.to_string())
    }

    #[cfg(target_os = "linux")]
    fn recorded_start_key(pid: u32) -> Option<String> {
        process_start_key(pid)
    }

    /// Whether a freshly-spawned child's start key is trustworthy enough to
    /// persist. On Linux, a `None` here means the child was already gone or a
    /// zombie microseconds after `spawn()` — fail closed and treat that as a
    /// spawn failure rather than persisting an uncorroborated record.
    #[cfg(target_os = "linux")]
    fn spawn_start_key_ok(started_at: &Option<String>) -> bool {
        started_at.is_some()
    }

    /// Shared corroboration check behind [`is_session_alive`] and
    /// [`is_task_alive`]: a recorded start key must match the pid's current
    /// one. No corroborating start key on record (should not happen for a
    /// record this module wrote — see `spawn_start_key_ok` — but a
    /// hand-edited or pre-upgrade record could lack one) fails closed rather
    /// than risk reporting a reused pid as this session/task, alive.
    #[cfg(target_os = "linux")]
    fn corroborated_alive(pid: u32, started_at: &Option<String>) -> bool {
        match (started_at, process_start_key(pid)) {
            (Some(recorded), Some(current)) => *recorded == current,
            _ => false,
        }
    }

    #[cfg(target_os = "linux")]
    fn is_session_alive(record: &SessionRecord) -> bool {
        corroborated_alive(record.pid, &record.started_at)
    }

    #[cfg(target_os = "linux")]
    fn is_task_alive(record: &TaskRecord) -> bool {
        corroborated_alive(record.pid, &record.started_at)
    }

    /// macOS has no `/proc`; existence-only fallback (`kill -0`). This cannot
    /// corroborate a PID against reuse across a `Run` restart the way the
    /// Linux path does — acceptable here because macOS host-service
    /// ownership (launchd) is explicitly out of scope for this module (see
    /// the module doc); this fallback exists so `baton service` still
    /// compiles, spawns, and tears down correctly on macOS in the foreground/
    /// test/diagnostic mode this module always supports on Unix.
    #[cfg(not(target_os = "linux"))]
    fn recorded_start_key(_pid: u32) -> Option<String> {
        None
    }

    /// No spawn-time corroboration is possible on this host (see
    /// [`recorded_start_key`]'s doc), so a `None` start key is expected, not a
    /// spawn failure.
    #[cfg(not(target_os = "linux"))]
    fn spawn_start_key_ok(_started_at: &Option<String>) -> bool {
        true
    }

    #[cfg(not(target_os = "linux"))]
    fn corroborated_alive(pid: u32) -> bool {
        if pid <= 1 {
            return false;
        }
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    #[cfg(not(target_os = "linux"))]
    fn is_session_alive(record: &SessionRecord) -> bool {
        corroborated_alive(record.pid)
    }

    #[cfg(not(target_os = "linux"))]
    fn is_task_alive(record: &TaskRecord) -> bool {
        corroborated_alive(record.pid)
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
        while is_session_alive(record) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    fn wait_while_task_alive(record: &TaskRecord, grace_ms: u64) {
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        while is_task_alive(record) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Stops one session. The caller must hold the admission lock:
    /// cooperative `serve --stop` on its inbox first,
    /// bounded wait, then `SIGTERM`/`SIGKILL` process-group escalation if
    /// still alive, then reaps every task this session owns
    /// ([`reap_session_tasks`]) and removes the session's own durable
    /// record. Idempotent — a session already gone just gets its (possibly
    /// already-absent) record, and its tasks', cleaned up.
    fn stop_session_record(control: &Path, record: &SessionRecord) -> Result<()> {
        let _ = mailbox::request_stop(&record.spec.inbox);
        wait_while_alive(record, STOP_GRACE_MS);
        if is_session_alive(record) {
            let _ = signal_group(record.pid, "-TERM");
            wait_while_alive(record, KILL_GRACE_MS);
            if is_session_alive(record) {
                let _ = signal_group(record.pid, "-KILL");
                wait_while_alive(record, KILL_GRACE_MS);
            }
        }
        reap_session_tasks(control, &record.id)?;
        remove_session_record(control, &record.id)
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
            .map(|record| SessionStatusView {
                id: &record.id,
                pid: record.pid,
                live: is_session_alive(record),
                inbox: &record.spec.inbox,
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

    fn execute_stop(control: &Path, session: &str, mut out: impl Write) -> Result<()> {
        let _admission = acquire_admission_lock(control)?;
        match read_session_record(control, session)? {
            Some(record) => {
                stop_session_record(control, &record)?;
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
            .map(|record| TaskStatusView {
                id: &record.id,
                session: &record.spec.session,
                pid: record.pid,
                state: record.state,
                live: record.state == TaskState::Running && is_task_alive(record),
                exit_code: record.exit_code,
                elapsed_ms: record.elapsed_ms,
                command: &record.spec.command,
                stdout_path: &record.stdout_path,
                stderr_path: &record.stderr_path,
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
        if is_task_alive(record) {
            let _ = signal_group(record.pid, "-TERM");
            wait_while_task_alive(record, KILL_GRACE_MS);
            if is_task_alive(record) {
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

    fn execute_teardown(control: &Path, mut out: impl Write) -> Result<()> {
        let service_liveness = request_control_stop(control)?;
        if service_liveness == ControlLiveness::Live {
            wait_for_control_release(control)?;
        }
        let _admission = acquire_admission_lock(control)?;
        for record in list_session_records(control)? {
            stop_session_record(control, &record)?;
        }
        match service_liveness {
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
        .map_err(io_err)
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
            let record = TaskRecord {
                id: id.to_string(),
                spec,
                pid,
                started_at: recorded_start_key(pid),
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
                child,
                started_ms: clock.now_ms(),
                term_sent_at_ms: None,
                kill_sent: false,
            }
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
        }

        /// `execute_stop` on an unknown session id is a no-op success
        /// (idempotent), leaving nothing behind.
        #[test]
        fn execute_stop_unknown_session_is_idempotent_success() {
            let dir = TempDir::new("stop-unknown");
            let mut out = Vec::new();
            execute_stop(&dir.path, "nope", &mut out).expect("stop");
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
            execute_teardown(&dir.path, &mut out).expect("teardown");
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

        // -- Task records -----------------------------------------------

        /// A task record round-trips through the atomic-write file protocol
        /// byte-for-byte.
        #[test]
        fn task_record_round_trips() {
            let dir = TempDir::new("task-record");
            let record = TaskRecord {
                id: "task-1".to_string(),
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: 4242,
                started_at: Some("123456".to_string()),
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
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                    pid: 1000 + i,
                    started_at: None,
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
            let _ = running.child.wait();
        }

        /// Exceeding `max_duration_ms` (per the injected clock) escalates
        /// `SIGTERM`→`SIGKILL` and the eventual reap is attributed to
        /// `timeout`, not `completed`/`failed`.
        #[test]
        fn tick_one_task_enforces_max_duration_and_marks_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-timeout");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "trap '' TERM; sleep 5".to_string()],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-t", spec, &clock);

            clock.advance(150);
            let tick = tick_one_task(&dir.path, "task-t", &mut running, &clock).expect("tick");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert!(
                running.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM"
            );

            clock.advance(KILL_GRACE_MS);
            let _tick = tick_one_task(&dir.path, "task-t", &mut running, &clock).expect("tick");
            assert!(running.kill_sent, "SIGTERM grace expiry must send SIGKILL");

            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match tick_one_task(&dir.path, "task-t", &mut running, &clock).expect("tick") {
                    TaskTick::Finished => break,
                    TaskTick::StillRunning => {
                        assert!(
                            Instant::now() < deadline,
                            "task did not exit after SIGTERM within the test bound"
                        );
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
            assert_eq!(running.record.state, TaskState::Timeout);

            let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
            let claimed = mailbox
                .claim_next()
                .expect("claim")
                .expect("terminal event present");
            assert_eq!(claimed.key, "task-t-terminal");
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
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb-a"),
                pid: u32::MAX - 1,
                started_at: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            let other = TaskRecord {
                id: "task-other".to_string(),
                spec: task_spec("svc-2", "true", vec![], vec![], 1_000, "/tmp/cb-b"),
                pid: u32::MAX - 1,
                started_at: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &owned).expect("write owned");
            write_task_record(&dir.path, &other).expect("write other");

            reap_session_tasks(&dir.path, "svc-1").expect("reap");

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
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
            };
            write_task_record(&dir.path, &task_record).expect("write task");

            let mut out = Vec::new();
            execute_teardown(&dir.path, &mut out).expect("teardown");

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

        // -- Task CLI-facing operations -----------------------------------

        /// `execute_task_status` reports a written task record's liveness
        /// and fields.
        #[test]
        fn execute_task_status_reports_task_fields() {
            let dir = TempDir::new("task-status");
            let record = TaskRecord {
                id: "task-1".to_string(),
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: u32::MAX - 1,
                started_at: None,
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
