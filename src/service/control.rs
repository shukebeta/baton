//! The client-facing control plane shared by `Run`'s supervisor loop and
//! every CLI-facing operation (`Start`, `Status`, `Stop`, `Teardown`, and
//! their task equivalents).
//!
//! Everything here was, before this extraction, duplicated byte-for-byte (or
//! near enough) between the Unix and Windows `imp` modules. Platform-specific
//! liveness/signal primitives (process probing, PID/Job-Object termination,
//! process spawning) remain in each platform's own `imp` module and are
//! imported here by name; this module holds the request protocol, admission
//! bookkeeping, and CLI dispatch targets built on top of them.

use super::admission;
#[cfg(unix)]
use super::records::sessions_dir;
use super::records::{
    AwaitConfig, SessionRecord, StartResponse, TaskStartResponse,
    discard_pending_task_start_request, fresh_request_id, fresh_session_id, list_session_records,
    list_task_record_ids, list_task_records, mark_task_start_rollback, read_session_record,
    read_task_record, reclaim_stale_requests, remove_session_record, responses_dir, start_channel,
    take_task_start_response_locked, task_cancel_dir, task_channel, task_start_ack_exists,
    task_start_response_boundary_exists, task_start_response_id, write_session_record,
    write_start_response,
};
use super::task_tick::{self, Liveness, ServicePlatform, remove_reaped_task_record};
use super::*;

#[cfg(windows)]
use super::imp::{
    JobHandle, active_job_processes, assign_job_to_child, cleanup_liveness_after_pid_signal,
    create_job, force_terminate_record_job, fresh_job_name, is_task_alive, open_job,
    record_job_available, recorded_start_identity, resume_initial_thread, session_liveness,
    spawn_serve_child, spawn_start_key_ok, terminate_job, terminate_record_job_or_pid,
    upgrade_legacy_session_record, upgrade_legacy_task_record, wait_while_alive,
    wait_while_task_alive,
};
#[cfg(unix)]
use super::imp::{
    recorded_start_identity, session_liveness, signal_group, spawn_serve_child, spawn_start_key_ok,
    task_execution_liveness, task_execution_liveness_after_retry, upgrade_legacy_session_record,
    upgrade_legacy_task_record, wait_while_alive, wait_while_task_alive,
};

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::process::Child;
use std::time::{Duration, Instant};

use crate::mailbox;
use crate::task::{SystemClock, TaskRecord, TaskSpec, TaskState};

/// Name of the control-plane lockfile at the control root, mirroring
/// [`mailbox`]'s `serve.lock`.
pub(super) const CONTROL_LOCK_FILE: &str = "service.lock";
/// Short-lived lock serializing task admission with session cleanup.
/// Separate from [`CONTROL_LOCK_FILE`], which `Run` holds for its whole
/// lifetime and therefore cannot be used by `service stop`.
pub(super) const ADMISSION_LOCK_FILE: &str = "service.admission.lock";
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
pub(super) const CONTROL_PROBE_LOCK_FILE: &str = "service.probe.lock";
/// Name of the cooperative-stop sentinel at the control root, mirroring
/// [`mailbox`]'s `serve.stop`.
pub(super) const CONTROL_STOP_FILE: &str = "service.stop";
/// Interval between `Run`'s request-directory scans, and between polls of
/// a pending await (start response, stop/kill grace).
pub(super) const POLL_INTERVAL_MS: u64 = 100;
/// Bound on how long `Start` waits for a live `Run` to answer.
pub(super) const START_AWAIT_MS: u64 = 10_000;
/// Bound on the cooperative `serve --stop` grace before escalating to the
/// platform's forceful termination (`SIGTERM`/`SIGKILL` on Unix,
/// `TerminateJobObject` on Windows).
pub(super) const STOP_GRACE_MS: u64 = 5_000;
/// Bound on the escalation grace after the first forceful termination
/// attempt.
pub(super) const KILL_GRACE_MS: u64 = 2_000;
/// Bound on how long teardown waits for `Run` to release the control
/// lock before continuing with record cleanup.
pub(super) const CONTROL_RELEASE_TIMEOUT_MS: u64 = 10_000;

/// A live session's platform-specific process handle: a bare
/// [`std::process::Child`] on Unix (a signal reaches the whole process group
/// via its `pid`), or a [`RunningSession`] on Windows (paired with its Job
/// Object so descendant termination remains possible even after `Child` is
/// dropped).
#[cfg(unix)]
pub(super) type SessionHandle = std::process::Child;
#[cfg(windows)]
pub(super) type SessionHandle = RunningSession;

/// Opens any service lock with close-on-exec set on Unix (`Run` holds the
/// control lock while it spawns sessions and tasks; descendants must not
/// retain that lock after the supervisor is killed, or a restart is blocked
/// by the descendant's lifetime). Windows process handles are not inherited
/// by `std::process::Command` unless explicitly requested, so no equivalent
/// flag is needed there.
#[cfg(unix)]
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

#[cfg(windows)]
fn service_lock_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    options
}

/// Whether a live `Run` holds the control lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ControlLiveness {
    Live,
    NotRunning,
}

/// Maps an [`std::io::Error`] encountered writing to `out` to a
/// [`BatonError::Io`].
pub(super) fn io_err(err: std::io::Error) -> BatonError {
    BatonError::Io(format!("could not write service output: {err}"))
}

// -- Run ------------------------------------------------------------

/// Runs the supervisor loop: holds the control lock, drains `requests/`
/// into spawned sessions, reaps exited children, and exits cooperatively
/// once `Teardown` drops the stop sentinel.
pub(super) fn run_service<P: ServicePlatform>(
    control: &Path,
    task_retention_ms: u64,
    mut out: impl Write,
) -> Result<()> {
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
    let (reconciled_records, reconcile_mutated) =
        admission::reconcile_task_admissions::<P>(control)?;
    // A request left mid-`processing/` by a crash between claim and
    // response is returned to `requests/`, mirroring
    // `Mailbox::reclaim_stale` — reprocessed harmlessly under a fresh
    // session id on this restart: the reprocessed spec spawns a *second*
    // `baton serve` on the same inbox/outbox, but `serve`'s own
    // single-instance mailbox lock refuses the duplicate immediately, so
    // it exits at once, leaving only a transient stale session record
    // behind (reaped the next time it's inspected).
    reclaim_stale_requests(control)?;
    admission::reclaim_stale_task_requests(control)?;
    drop(_admission);
    let clock = SystemClock;
    #[cfg(windows)]
    let mut sessions = rehydrate_sessions(control)?;
    #[cfg(unix)]
    let mut sessions: HashMap<String, SessionHandle> = HashMap::new();
    // Reuse reconciliation's own `tasks/` listing when it changed
    // nothing, so the common (nothing-to-reconcile) restart parses each
    // record exactly once instead of walking `tasks/` twice.
    let records = if reconcile_mutated {
        None
    } else {
        Some(reconciled_records)
    };
    let mut tasks = task_tick::rehydrate_tasks::<P>(control, &clock, task_retention_ms, records)?;
    writeln!(out, "baton service running on {}", control.display()).map_err(io_err)?;

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
        reap_exited(&mut sessions);
        task_tick::tick_tasks::<P>(control, &mut tasks, &clock);

        // One request's failure (a malformed spec, a transient spawn
        // error) must not crash the loop out from under every other
        // session/task this instance already owns — warn and keep
        // polling, the same "one bad message can't wedge the daemon"
        // posture `Mailbox::claim_next` takes for a malformed mailbox
        // entry.
        let mut did_work = false;
        match process_one_request(control) {
            Ok(Some((session_id, session))) => {
                sessions.insert(session_id, session);
                did_work = true;
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!(
                    "warning: baton service failed to process a session-start request: {err}"
                );
            }
        }
        match admission::process_one_task_request::<P>(control, &clock) {
            Ok(Some((task_id, running))) => {
                tasks.insert(task_id, running.with_retention_ms(task_retention_ms));
                did_work = true;
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("warning: baton service failed to process a task-start request: {err}");
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

/// Pairs a session's direct child handle with the Job Object it was
/// assigned to, so a still-live grandchild (`baton serve`'s own
/// `agent-cmd`) remains reachable through the job after `Child` alone
/// would no longer see it.
#[cfg(windows)]
pub(super) struct RunningSession {
    child: Option<Child>,
    pub(super) job: Option<JobHandle>,
}

#[cfg(windows)]
pub(super) fn rehydrate_sessions(control: &Path) -> Result<HashMap<String, RunningSession>> {
    let mut sessions = HashMap::new();
    for record in list_session_records(control)? {
        let job = record.job.as_deref().map(open_job).transpose()?.flatten();
        sessions.insert(record.id, RunningSession { child: None, job });
    }
    Ok(sessions)
}

/// Removes every child whose exit status is already available, reaping
/// it. A still-running child (`Ok(None)`) is left in place.
#[cfg(unix)]
fn reap_exited(children: &mut HashMap<String, SessionHandle>) {
    children.retain(|_, child| !matches!(child.try_wait(), Ok(Some(_)) | Err(_)));
}

/// Reaps direct children, but retains each Job Object handle until its
/// active-process count reaches zero so a serve-exited grandchild remains
/// reachable through the same job.
#[cfg(windows)]
fn reap_exited(sessions: &mut HashMap<String, SessionHandle>) {
    sessions.retain(|_, running| {
        if let Some(child) = running.child.as_mut() {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => running.child = None,
                Ok(None) => {}
            }
        }
        match running.job.as_ref() {
            Some(job) => match active_job_processes(job) {
                Ok(0) => running.child.is_some(),
                Ok(_) | Err(_) => true,
            },
            None => running.child.is_some(),
        }
    });
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
pub(super) fn acquire_control_lock(control: &Path) -> Result<File> {
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
pub(super) fn acquire_admission_lock(control: &Path) -> Result<File> {
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
pub(super) struct AdmissionGuard<'a> {
    control: &'a Path,
    /// `Some` whenever the lock is held; `None` only inside
    /// [`AdmissionGuard::unlocked_wait`]. Dropping the `File` unlocks.
    lock: Option<File>,
    /// Every task record classified so far, keyed by id via
    /// `task_index`. Seeded once by [`AdmissionGuard::task_ids_for_session`]
    /// and grown only by [`AdmissionGuard::refresh_new_task_ids`] — an
    /// id already present here is never re-parsed.
    task_records: Vec<TaskRecord>,
    task_index: std::collections::HashMap<String, usize>,
    task_records_loaded: bool,
}

#[cfg(test)]
pub(super) static TASK_FULL_LISTINGS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
pub(super) static TASK_NEW_ID_PARSES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
/// Counts direct `read_task_record` confirmations of a cached `Running`
/// entry inside [`reap_session_tasks_with_wait`] and
/// [`wait_then_recheck_terminal`] only. `rescan_owned_tasks` performs the
/// same kind of confirmation read for its own cached-`Running` entries
/// but deliberately does not add to this counter: it exists to bound the
/// reap pass's confirmation cost, and rescan only ever runs against ids
/// the reap pass has not already handled (a genuinely rare path — a
/// stop-owned rescan sees an untouched `Running` id only when
/// `refresh_new_task_ids` admitted it after the reap pass's own loop
/// already read past it).
#[cfg(test)]
pub(super) static TASK_CONFIRM_READS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl<'a> AdmissionGuard<'a> {
    pub(super) fn acquire(control: &'a Path) -> Result<Self> {
        Ok(Self {
            control,
            lock: Some(acquire_admission_lock(control)?),
            task_records: Vec::new(),
            task_index: std::collections::HashMap::new(),
            task_records_loaded: false,
        })
    }

    fn control(&self) -> &'a Path {
        self.control
    }

    /// Runs `wait` with the admission lock released, then re-acquires it
    /// before returning. Callers must re-probe any liveness they decided
    /// on before the wait. Also the sole point that refreshes the task
    /// cache for newly admitted ids: this is the only place the lock is
    /// ever released, so it is the only place a new admission could have
    /// landed.
    fn unlocked_wait<T>(&mut self, wait: impl FnOnce() -> T) -> Result<T> {
        self.lock = None;
        let value = wait();
        self.lock = Some(acquire_admission_lock(self.control)?);
        self.refresh_new_task_ids()?;
        Ok(value)
    }

    /// Picks up any task admitted while the lock was released: one cheap
    /// id-only directory scan (no JSON decode), then one parse per id not
    /// already cached. A no-op before the first `task_ids_for_session`
    /// call, since there is nothing to refresh yet — that call performs
    /// the one full listing this cache is seeded from, and nothing can be
    /// admitted before this guard's own first lock acquisition anyway.
    fn refresh_new_task_ids(&mut self) -> Result<()> {
        if !self.task_records_loaded {
            return Ok(());
        }
        for id in list_task_record_ids(self.control)? {
            if !self.task_index.contains_key(&id)
                && let Some(record) = read_task_record(self.control, &id)?
            {
                self.task_index.insert(id, self.task_records.len());
                self.task_records.push(record);
                #[cfg(test)]
                TASK_NEW_ID_PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Ids currently known to belong to `session_id`. The first call
    /// across this guard's lifetime performs the one full `tasks/`
    /// listing+parse the whole cache is seeded from; every later call is
    /// a plain in-memory read — freshness for anything admitted since is
    /// already maintained by `unlocked_wait`, not by this accessor. An id
    /// already cached is never re-parsed here: a task's
    /// `spec`/`spec.session` never changes after creation
    /// (`upgrade_legacy_task_record` only ever touches
    /// `start_epoch_secs`), so the classification this returns is always
    /// correct. The lifecycle fields (`state`/`exit_code`/...) a cached
    /// entry carries can go stale if the daemon's own supervisor tick
    /// (`task_tick::finalize_task`) terminalizes it later — callers must
    /// treat [`AdmissionGuard::cached_task`] accordingly (see its doc
    /// comment).
    fn task_ids_for_session(&mut self, session_id: &str) -> Result<Vec<String>> {
        if !self.task_records_loaded {
            self.task_records = list_task_records(self.control)?;
            self.task_index = self
                .task_records
                .iter()
                .enumerate()
                .map(|(i, r)| (r.id.clone(), i))
                .collect();
            self.task_records_loaded = true;
            #[cfg(test)]
            TASK_FULL_LISTINGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(self
            .task_records
            .iter()
            .filter(|r| r.spec.session == session_id)
            .map(|r| r.id.clone())
            .collect())
    }

    /// The last-classified copy of `id`, if known. **Trust it directly
    /// only when it is already terminal** (a task's terminal state is
    /// final, so a cached terminal read is never stale in a way that
    /// changes a cleanup decision). Never use a cached `Running` copy to
    /// decide whether to probe or signal a process — confirm it first
    /// with a direct `read_task_record`, since the supervisor can
    /// terminalize it at any time this guard is not the one holding the
    /// lock exclusively.
    fn cached_task(&self, id: &str) -> Option<&TaskRecord> {
        self.task_index.get(id).map(|&i| &self.task_records[i])
    }
}

/// A session being stopped is not an admissible task owner, even while its
/// process is still live. `service stop` releases the admission lock across
/// its grace windows (so it never freezes this loop), which leaves a window
/// where the owner still probes `Live`; without this marker a start racing
/// that window would be answered with a task id for a process the very same
/// stop is about to kill.
///
/// This is distinct from the cooperative `serve.stop` sentinel, which the
/// daemon consumes as soon as it observes it, long before the process exits
/// — a start could otherwise land in the gap between sentinel consumption
/// and process exit and see neither a sentinel nor a dead owner. This marker
/// spans the whole cleanup instead.
///
/// It records the stopping process's own identity so a marker orphaned by
/// a killed `service stop` cannot wedge admission forever: a reader whose
/// identity no longer matches treats it as stale and removes it.
#[derive(Serialize, Deserialize)]
pub(super) struct SessionStopMarker {
    pub(super) pid: u32,
    #[serde(default)]
    pub(super) started_at: Option<String>,
    #[serde(default)]
    pub(super) start_epoch_secs: Option<i64>,
}

/// Owns a [`SessionStopMarker`] for the length of one session's cleanup,
/// removing it on every exit path including an early `?`.
pub(super) struct SessionStopGuard {
    path: std::path::PathBuf,
    pid: u32,
}

impl SessionStopGuard {
    pub(super) fn claim(control: &Path, id: &str) -> Result<Self> {
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
        let dir = admission::session_stop_markers_dir(control);
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        mailbox::atomic_write(&dir, &mailbox::file_name(id), &json)?;
        Ok(Self {
            path: admission::session_stop_marker_path(control, id)?,
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
pub(super) fn probe_or_signal_control(control: &Path, signal: bool) -> Result<ControlLiveness> {
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

pub(super) fn probe_control(control: &Path) -> Result<ControlLiveness> {
    probe_or_signal_control(control, false)
}

/// Requests a cooperative stop of a live `Run`; a no-op success when none
/// is running (idempotent, like [`mailbox::request_stop`]).
pub(super) fn request_control_stop(control: &Path) -> Result<ControlLiveness> {
    probe_or_signal_control(control, true)
}

// -- Start / control-plane request protocol --------------------------

/// Submits `spec` to a live `Run` and awaits its session id.
///
/// Fails fast (before writing anything) when no `Run` holds the control
/// lock, rather than waiting out the full await bound against a service
/// that was never started.
pub(super) fn submit_start_request(control: &Path, spec: &SessionSpec) -> Result<String> {
    let request_id = fresh_request_id();
    start_channel(control).submit(
        &request_id,
        spec,
        |control| Ok(probe_control(control)? == ControlLiveness::Live),
        |control| {
            format!(
                "no live baton service on {control:?}; start one with `baton service run [--control <dir>]` first"
            )
        },
        "session spec",
        || await_start_response(control, &request_id),
    )
}

pub(super) fn await_start_response(control: &Path, request_id: &str) -> Result<String> {
    let path = responses_dir(control).join(mailbox::file_name(request_id));
    start_channel(control).await_response(
        request_id,
        AwaitConfig::new(
            START_AWAIT_MS,
            POLL_INTERVAL_MS,
            format!("no live baton service on {control:?}; start request was not admitted"),
            "session",
        ),
        || {
            if let Ok(data) = fs::read_to_string(&path) {
                let _ = fs::remove_file(&path);
                let resp: StartResponse = serde_json::from_str(&data).map_err(|err| {
                    BatonError::Decode(format!("malformed service response {path:?}: {err}"))
                })?;
                if let Some(error) = resp.error {
                    return Err(BatonError::Io(error));
                }
                return resp
                    .session_id
                    .ok_or_else(|| {
                        BatonError::Decode(format!(
                            "service response {path:?} contained neither a session id nor an error"
                        ))
                    })
                    .map(Some);
            }
            Ok(None)
        },
        |control| Ok(probe_control(control)? == ControlLiveness::Live),
        || Ok(None),
    )
}

/// Claims and handles the next pending start request, if any.
fn process_one_request(control: &Path) -> Result<Option<(String, SessionHandle)>> {
    start_channel(control).process_one(|request_id, claimed_path| {
        let outcome = handle_start_request(control, request_id, claimed_path)?;
        let Some((record, session)) = outcome else {
            return Ok(None);
        };
        Ok(Some((record.id, session)))
    })
}

/// Answers a claimed start request with an admission failure the
/// supervisor can name, so the client fails immediately with the real
/// reason instead of waiting out [`START_AWAIT_MS`]. Only the response
/// write itself can still fail the request loop.
fn reject_start_request(
    control: &Path,
    request_id: &str,
    error: String,
) -> Result<Option<(SessionRecord, SessionHandle)>> {
    start_channel(control).reject(
        request_id,
        &StartResponse {
            session_id: None,
            error: Some(error),
        },
        "start response",
    )
}

/// Spawns the requested session, persists its [`SessionRecord`], and
/// answers the request with its session id.
///
/// An admission failure after the request is claimed — a spawn failure, a
/// post-spawn corroboration failure, a record-write failure — is answered
/// as an error response and reported as `Ok(None)`; only a failure to
/// deliver a response at all is propagated as `Err`.
#[cfg(unix)]
pub(super) fn handle_start_request(
    control: &Path,
    request_id: &str,
    spec_path: &Path,
) -> Result<Option<(SessionRecord, SessionHandle)>> {
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
            return reject_start_request(
                control,
                request_id,
                admission::admission_error_text(&err),
            );
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
        let _ = signal_group(pid, libc::SIGKILL);
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
        let _ = signal_group(pid, libc::SIGKILL);
        let _ = child.wait();
        let _ = fs::remove_dir_all(&log_dir);
        return reject_start_request(control, request_id, admission::admission_error_text(&err));
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
        let _ = signal_group(pid, libc::SIGKILL);
        let _ = child.wait();
        let _ = remove_session_record(control, &record.id);
        let _ = fs::remove_dir_all(&log_dir);
        return Err(err);
    }
    Ok(Some((record, child)))
}

/// Spawns the requested session, persists its [`SessionRecord`], and
/// answers the request with its session id.
///
/// An admission failure after the request is claimed — a job/spawn failure, a
/// post-spawn corroboration failure, a record-write failure — is answered as
/// an error response and reported as `Ok(None)`; only a failure to deliver a
/// response at all is propagated as `Err`.
#[cfg(windows)]
pub(super) fn handle_start_request(
    control: &Path,
    request_id: &str,
    spec_path: &Path,
) -> Result<Option<(SessionRecord, SessionHandle)>> {
    let data = fs::read_to_string(spec_path)
        .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
    let spec: SessionSpec = serde_json::from_str(&data).map_err(|err| {
        BatonError::Decode(format!("malformed session spec {spec_path:?}: {err}"))
    })?;
    let job_name = fresh_job_name("session");
    let job = match create_job(&job_name, false) {
        Ok(job) => job,
        Err(err) => {
            return reject_start_request(
                control,
                request_id,
                admission::admission_error_text(&err),
            );
        }
    };
    let mut child = match spawn_serve_child(&spec, &job_name) {
        Ok(child) => child,
        Err(err) => {
            drop(job);
            return reject_start_request(
                control,
                request_id,
                admission::admission_error_text(&err),
            );
        }
    };
    if let Err(err) =
        assign_job_to_child(&job, &child).and_then(|_| resume_initial_thread(child.id()))
    {
        let _ = terminate_job(&job);
        let _ = child.wait();
        return reject_start_request(control, request_id, admission::admission_error_text(&err));
    }
    let pid = child.id();
    let (started_at, start_epoch_secs) = recorded_start_identity(pid);
    // Everything below this point must kill+reap `child` before
    // returning: once this function stops tracking it, nothing else ever
    // does (it isn't inserted into `Run`'s `children` map, and `Drop` for
    // `std::process::Child` does not kill), so leaving it running here
    // would leak a live, unrecorded, unreapable `serve` process.
    if !spawn_start_key_ok(&started_at, &start_epoch_secs) {
        let _ = terminate_job(&job);
        let _ = child.wait();
        return reject_start_request(
            control,
            request_id,
            format!(
                "baton serve (pid {pid}) could not be corroborated right after spawn; treating as a spawn failure"
            ),
        );
    }
    let record = SessionRecord {
        id: fresh_session_id(),
        spec,
        pid,
        started_at,
        start_epoch_secs,
        job: Some(job_name),
    };
    if let Err(err) = write_session_record(control, &record) {
        let _ = terminate_job(&job);
        let _ = child.wait();
        return reject_start_request(control, request_id, admission::admission_error_text(&err));
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
        let _ = terminate_job(&job);
        let _ = child.wait();
        let _ = remove_session_record(control, &record.id);
        return Err(err);
    }
    Ok(Some((
        record,
        RunningSession {
            child: Some(child),
            job: Some(job),
        },
    )))
}

/// Resolves the currently-running `baton` binary, so `Run` spawns
/// `serve` sessions via the same executable rather than trusting `PATH`.
pub(super) fn current_baton_exe() -> Result<std::path::PathBuf> {
    std::env::current_exe().map_err(|err| {
        BatonError::Io(format!(
            "could not resolve the running baton executable: {err}"
        ))
    })
}

/// Builds the `baton serve` argv equivalent to `spec`.
pub(super) fn serve_argv(spec: &SessionSpec) -> Vec<String> {
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

/// Submits `spec` through the shared request channel and awaits its task
/// id. The task response claim and rollback transaction remain specific
/// to this caller.
pub(super) fn submit_task_start_request(control: &Path, spec: &TaskSpec) -> Result<String> {
    let request_id = fresh_request_id();
    task_channel(control).submit(
        &request_id,
        spec,
        |control| Ok(probe_control(control)? == ControlLiveness::Live),
        |control| {
            format!(
                "no live baton service on {control:?}; start one with `baton service run [--control <dir>]` first"
            )
        },
        "task spec",
        || await_task_start_response(control, &request_id),
    )
}

fn await_task_start_response(control: &Path, request_id: &str) -> Result<String> {
    task_channel(control).await_response(
        request_id,
        AwaitConfig::new(
            START_AWAIT_MS,
            POLL_INTERVAL_MS,
            format!("no live baton service on {control:?}; task start request was not admitted"),
            "task",
        ),
        || {
            take_task_start_response(control, request_id).and_then(|response| {
                response.map_or(Ok(None), |response| {
                    task_start_response_id(control, request_id, response).map(Some)
                })
            })
        },
        |control| Ok(probe_control(control)? == ControlLiveness::Live),
        || {
            // The supervisor can write its response just before dropping
            // the control lock. Re-check the response after observing the
            // released lock so a successful admission wins this race. The
            // admission lock keeps a newly-started supervisor from
            // processing the request between this check and the rollback
            // marker.
            let _admission = acquire_admission_lock(control)?;
            if let Some(response) = take_task_start_response_locked(control, request_id)? {
                return task_start_response_id(control, request_id, response).map(Some);
            }
            if task_start_ack_exists(control, request_id)? {
                return Err(BatonError::Io(format!(
                    "task start response for {request_id} was already consumed"
                )));
            }
            mark_task_start_rollback(control, request_id)?;
            discard_pending_task_start_request(control, request_id)?;
            Err(BatonError::Io(format!(
                "no live baton service on {control:?}; task start request was not admitted"
            )))
        },
    )
}

/// Takes a task-start response if the supervisor has written one.
///
/// The admission lock serializes the claim with response publication,
/// phase persistence, and startup reconciliation. The acknowledgement is
/// durable before the private claim is removed.
pub(super) fn take_task_start_response(
    control: &Path,
    request_id: &str,
) -> Result<Option<TaskStartResponse>> {
    if !task_start_response_boundary_exists(control, request_id)? {
        return Ok(None);
    }
    let _admission = acquire_admission_lock(control)?;
    take_task_start_response_locked(control, request_id)
}

// -- Task records -------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub(super) struct CleanupResidue {
    kind: &'static str,
    pub(super) id: String,
    pid: u32,
    pub(super) liveness: Liveness,
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
#[cfg(unix)]
pub(super) fn reap_session_tasks_with_wait(
    admission: &mut AdmissionGuard,
    session_id: &str,
    force: bool,
    wait: impl Fn(&TaskRecord, u64),
) -> Result<(Vec<CleanupResidue>, std::collections::HashSet<String>)> {
    let control = admission.control();
    let ids = admission.task_ids_for_session(session_id)?;
    let mut handled = std::collections::HashSet::new();
    let mut residue = Vec::new();
    for id in ids {
        handled.insert(id.clone());
        let Some(mut record) = admission.cached_task(&id).cloned() else {
            continue;
        };
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        // The cached copy says Running; confirm before acting on it —
        // the supervisor's own tick can have terminalized it since this
        // guard classified it.
        match read_task_record(control, &id)? {
            Some(fresh) => record = fresh,
            None => continue,
        }
        #[cfg(test)]
        TASK_CONFIRM_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        upgrade_legacy_task_record(control, &mut record)?;
        let mut liveness = task_execution_liveness(&record);
        if force {
            if liveness != Liveness::Dead {
                let _ = signal_group(record.pid, libc::SIGTERM);
                let _ = signal_group(record.pid, libc::SIGKILL);
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
            let _ = signal_group(record.pid, libc::SIGTERM);
            term_sent = true;
        }
        if liveness != Liveness::Dead
            && let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
        {
            remove_reaped_task_record(control, &terminal)?;
            continue;
        }
        if liveness == Liveness::Live && !term_sent {
            let _ = signal_group(record.pid, libc::SIGTERM);
            if let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
            {
                remove_reaped_task_record(control, &terminal)?;
                continue;
            }
        }
        if liveness == Liveness::Live {
            let _ = signal_group(record.pid, libc::SIGKILL);
            if let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
            {
                remove_reaped_task_record(control, &terminal)?;
                continue;
            }
        }
        if liveness == Liveness::Dead {
            remove_reaped_task_record(control, &record)?;
        } else {
            residue.push(task_residue(&record, liveness));
        }
    }
    Ok((residue, handled))
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
#[cfg(windows)]
pub(super) fn reap_session_tasks_with_wait(
    admission: &mut AdmissionGuard,
    session_id: &str,
    force: bool,
    wait: impl Fn(&TaskRecord, u64),
) -> Result<(Vec<CleanupResidue>, std::collections::HashSet<String>)> {
    let control = admission.control();
    let ids = admission.task_ids_for_session(session_id)?;
    let mut handled = std::collections::HashSet::new();
    let mut residue = Vec::new();
    for id in ids {
        handled.insert(id.clone());
        let Some(mut record) = admission.cached_task(&id).cloned() else {
            continue;
        };
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        // The cached copy says Running; confirm before acting on it — the
        // supervisor's own tick can have terminalized it since this guard
        // classified it.
        match read_task_record(control, &id)? {
            Some(fresh) => record = fresh,
            None => continue,
        }
        #[cfg(test)]
        TASK_CONFIRM_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        upgrade_legacy_task_record(control, &mut record)?;
        let mut liveness = is_task_alive(&record);
        if force {
            if liveness != Liveness::Dead {
                if !record_job_available(record.job.as_deref()) {
                    eprintln!(
                        "warning: forced Windows cleanup of task {} reaches only recorded pid {}; descendants may survive",
                        record.id, record.pid
                    );
                }
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            term_sent = true;
        }
        if liveness != Liveness::Dead
            && let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
        {
            remove_reaped_task_record(control, &terminal)?;
            continue;
        }
        if liveness == Liveness::Live && !term_sent {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            if let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
            {
                remove_reaped_task_record(control, &terminal)?;
                continue;
            }
        }
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
            if let Some(terminal) =
                wait_then_recheck_terminal(admission, &record, &mut liveness, KILL_GRACE_MS, &wait)?
            {
                remove_reaped_task_record(control, &terminal)?;
                continue;
            }
        }
        if liveness == Liveness::Dead {
            remove_reaped_task_record(control, &record)?;
        } else {
            residue.push(task_residue(&record, liveness));
        }
    }
    Ok((residue, handled))
}

/// Runs one grace wait like [`AdmissionGuard::unlocked_wait`], then
/// re-reads this one record directly: the daemon's own supervisor tick
/// can persist a terminal state for it while the lock was released
/// (`task_tick::finalize_task`), and a definitive terminal record beats
/// continuing the probe-based ladder on a now-stale `Running` copy,
/// whose pid could since have been reused by an unrelated process.
/// Returns the terminal record to remove without further signaling, or
/// `None` when the record is still genuinely `Running` and the wait's
/// own probe verdict (written back into `liveness`) should govern.
#[cfg(unix)]
fn wait_then_recheck_terminal(
    admission: &mut AdmissionGuard,
    record: &TaskRecord,
    liveness: &mut Liveness,
    grace_ms: u64,
    wait: &impl Fn(&TaskRecord, u64),
) -> Result<Option<TaskRecord>> {
    *liveness = admission.unlocked_wait(|| {
        wait(record, grace_ms);
        task_execution_liveness_after_retry(record, grace_ms)
    })?;
    #[cfg(test)]
    TASK_CONFIRM_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match read_task_record(admission.control(), &record.id)? {
        Some(fresh) if fresh.state != TaskState::Running => Ok(Some(fresh)),
        _ => Ok(None),
    }
}

/// Runs one grace wait like [`AdmissionGuard::unlocked_wait`], then
/// re-reads this one record directly: the daemon's own supervisor tick can
/// persist a terminal state for it while the lock was released
/// (`task_tick::finalize_task`), and a definitive terminal record beats
/// continuing the probe-based ladder on a now-stale `Running` copy, whose
/// pid could since have been reused by an unrelated process. Returns the
/// terminal record to remove without further signaling, or `None` when the
/// record is still genuinely `Running` and the wait's own probe verdict
/// (written back into `liveness`) should govern.
#[cfg(windows)]
fn wait_then_recheck_terminal(
    admission: &mut AdmissionGuard,
    record: &TaskRecord,
    liveness: &mut Liveness,
    grace_ms: u64,
    wait: &impl Fn(&TaskRecord, u64),
) -> Result<Option<TaskRecord>> {
    *liveness = admission.unlocked_wait(|| {
        wait(record, grace_ms);
        cleanup_liveness_after_pid_signal(is_task_alive(record), record.pid)
    })?;
    #[cfg(test)]
    TASK_CONFIRM_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match read_task_record(admission.control(), &record.id)? {
        Some(fresh) if fresh.state != TaskState::Running => Ok(Some(fresh)),
        _ => Ok(None),
    }
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
#[cfg(unix)]
pub(super) fn rescan_owned_tasks(
    admission: &mut AdmissionGuard,
    session_id: &str,
    force: bool,
    handled: &std::collections::HashSet<String>,
    residue: &mut Vec<CleanupResidue>,
) -> Result<()> {
    let control = admission.control();
    let ids = admission.task_ids_for_session(session_id)?;
    for id in ids {
        if handled.contains(&id) {
            continue;
        }
        let Some(cached) = admission.cached_task(&id).cloned() else {
            continue;
        };
        // A cached terminal entry is trusted directly, same rule as the
        // reaper; a cached Running entry was never handled by reap's own
        // pass, so it still needs one direct confirmation before this
        // pass decides anything from it.
        let mut record = if cached.state != TaskState::Running {
            cached
        } else {
            match read_task_record(control, &id)? {
                Some(fresh) => fresh,
                None => continue,
            }
        };
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        upgrade_legacy_task_record(control, &mut record)?;
        // The same probe the reaper's first pass uses, so a record that
        // only this pass sees is judged identically. The non-retrying
        // form: this pass must never sleep.
        let liveness = task_execution_liveness(&record);
        if force {
            if liveness != Liveness::Dead {
                let _ = signal_group(record.pid, libc::SIGTERM);
                let _ = signal_group(record.pid, libc::SIGKILL);
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

/// Accounts for every task record owned by `session_id` that the reaper's
/// snapshot could have missed, without ever releasing the admission lock.
///
/// A task start admitted while [`AdmissionGuard::unlocked_wait`] had the
/// lock released lands outside [`reap_session_tasks_with_wait`]'s listing.
/// This pass re-lists under the still-held lock and closes that gap for
/// every state such a record can be in by now — including a terminal one,
/// since the supervisor can tick a racing task to
/// `Completed`/`Failed`/`Cancelled`/`Timeout` before we look.
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
#[cfg(windows)]
pub(super) fn rescan_owned_tasks(
    admission: &mut AdmissionGuard,
    session_id: &str,
    force: bool,
    handled: &std::collections::HashSet<String>,
    residue: &mut Vec<CleanupResidue>,
) -> Result<()> {
    let control = admission.control();
    let ids = admission.task_ids_for_session(session_id)?;
    for id in ids {
        if handled.contains(&id) {
            continue;
        }
        let Some(cached) = admission.cached_task(&id).cloned() else {
            continue;
        };
        // A cached terminal entry is trusted directly, same rule as the
        // reaper; a cached Running entry was never handled by reap's own
        // pass, so it still needs one direct confirmation before this pass
        // decides anything from it.
        let mut record = if cached.state != TaskState::Running {
            cached
        } else {
            match read_task_record(control, &id)? {
                Some(fresh) => fresh,
                None => continue,
            }
        };
        if record.state != TaskState::Running {
            remove_reaped_task_record(control, &record)?;
            continue;
        }
        upgrade_legacy_task_record(control, &mut record)?;
        // The same probe the reaper's first pass uses, so a record that
        // only this pass sees is judged identically. The non-retrying
        // form: this pass must never sleep.
        let liveness = is_task_alive(&record);
        if force {
            if liveness != Liveness::Dead {
                if !record_job_available(record.job.as_deref()) {
                    eprintln!(
                        "warning: forced Windows cleanup of task {} reaches only recorded pid {}; descendants may survive",
                        record.id, record.pid
                    );
                }
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
// `cancelled` rather than misreading a forced exit as `failed`.

pub(super) fn request_task_cancel_sentinel(control: &Path, task_id: &str) -> Result<()> {
    let dir = task_cancel_dir(control);
    fs::create_dir_all(&dir)
        .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
    mailbox::atomic_write(&dir, &mailbox::file_name(task_id), "")
}

// -- Liveness -----------------------------------------------------------

#[cfg(all(unix, target_os = "linux"))]
pub(super) fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(super) fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

#[cfg(windows)]
pub(super) fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

// -- Session records ------------------------------------------------------

#[cfg(unix)]
pub(super) fn session_logs_dir(control: &Path, session_id: &str) -> std::path::PathBuf {
    sessions_dir(control).join(session_id)
}

// -- CLI-facing operations ---------------------------------------------

#[derive(Serialize)]
struct SessionStatusView<'a> {
    id: &'a str,
    pid: u32,
    live: bool,
    liveness: Liveness,
    inbox: &'a str,
    #[cfg(unix)]
    stderr_path: &'a str,
}

#[derive(Serialize)]
struct ServiceStatusView<'a> {
    service_running: bool,
    control: String,
    sessions: Vec<SessionStatusView<'a>>,
}

pub(super) fn execute_status(
    control: &Path,
    session: Option<&str>,
    mut out: impl Write,
) -> Result<()> {
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
                #[cfg(unix)]
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

pub(super) fn execute_stop(
    control: &Path,
    session: &str,
    force: bool,
    mut out: impl Write,
) -> Result<()> {
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

pub(super) fn execute_task_status<P: ServicePlatform>(
    control: &Path,
    task: Option<&str>,
    mut out: impl Write,
) -> Result<()> {
    let records: Vec<TaskRecord> = match task {
        Some(id) => read_task_record(control, id)?.into_iter().collect(),
        None => list_task_records(control)?,
    };
    let tasks = records
        .iter()
        .map(|record| {
            let liveness = if record.state == TaskState::Running {
                P::task_liveness(record)
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
#[cfg(unix)]
fn cancel_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
    if record.state != TaskState::Running {
        return Ok(());
    }
    request_task_cancel_sentinel(control, &record.id)?;
    let mut liveness = task_execution_liveness_after_retry(record, KILL_GRACE_MS);
    if liveness == Liveness::Live {
        let _ = signal_group(record.pid, libc::SIGTERM);
        wait_while_task_alive(record, KILL_GRACE_MS);
        liveness = task_execution_liveness_after_retry(record, KILL_GRACE_MS);
        if liveness == Liveness::Live {
            let _ = signal_group(record.pid, libc::SIGKILL);
            wait_while_task_alive(record, KILL_GRACE_MS);
        }
    }
    Ok(())
}

/// Cancels one task: idempotent — a task already terminal (or unknown)
/// is a no-op success. Acts directly on the durable [`TaskRecord`]'s
/// PID, exactly like [`stop_session_record`] does for a session, so this
/// works even when `Run` is not currently alive; `Run`'s own tick (if
/// alive) still performs the actual terminal-state write and event
/// delivery once it observes the exit.
#[cfg(windows)]
fn cancel_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
    if record.state != TaskState::Running {
        return Ok(());
    }
    request_task_cancel_sentinel(control, &record.id)?;
    if is_task_alive(record) == Liveness::Live {
        let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
        wait_while_task_alive(record, KILL_GRACE_MS);
        if is_task_alive(record) == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
            wait_while_task_alive(record, KILL_GRACE_MS);
        }
    }
    Ok(())
}

pub(super) fn execute_task_cancel(control: &Path, task: &str, mut out: impl Write) -> Result<()> {
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

/// Stops one session: cooperative `serve --stop` on its inbox first,
/// bounded wait, then forceful termination if still alive, then reaps
/// every task this session owns ([`reap_session_tasks_with_wait`]) and
/// removes the session's own durable record. Idempotent — a session
/// already gone just gets its (possibly already-absent) record, and its
/// tasks', cleaned up. Returns any records retained because their
/// identity remained unresolved.
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
#[cfg(unix)]
pub(super) fn stop_session_record_with_wait(
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
            let _ = signal_group(record.pid, libc::SIGTERM);
            let _ = signal_group(record.pid, libc::SIGKILL);
        }
        liveness = Liveness::Dead;
    } else {
        admission.unlocked_wait(|| session_wait(&record, STOP_GRACE_MS))?;
        liveness = is_session_alive(&record);
        if liveness == Liveness::Live {
            let _ = signal_group(record.pid, libc::SIGTERM);
            admission.unlocked_wait(|| session_wait(&record, KILL_GRACE_MS))?;
            liveness = is_session_alive(&record);
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, libc::SIGKILL);
                admission.unlocked_wait(|| session_wait(&record, KILL_GRACE_MS))?;
                liveness = is_session_alive(&record);
            }
        }
    }
    let (mut residue, handled) =
        reap_session_tasks_with_wait(admission, &record.id, force, task_wait)?;
    // From here to the session-record decision the admission lock is held
    // without interruption, so nothing can be admitted between the rescan
    // and `remove_session_record`.
    rescan_owned_tasks(admission, &record.id, force, &handled, &mut residue)?;
    if liveness == Liveness::Dead && residue.is_empty() {
        remove_session_record(control, &record.id)?;
        // The record was the only pointer to this session's captured
        // stderr, so the log tree is reclaimed with it. A session left as
        // residue keeps both, so the operator can still read why.
        let _ = fs::remove_dir_all(session_logs_dir(control, &record.id));
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

/// [`stop_session_record`] with both grace waits injectable, so tests can
/// drive the racing-admission paths deterministically instead of against
/// the wall clock. Production goes through the wrapper above.
#[cfg(windows)]
pub(super) fn stop_session_record_with_wait(
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
            if !record_job_available(record.job.as_deref()) {
                eprintln!(
                    "warning: forced Windows cleanup of session {} reaches only recorded pid {}; descendants may survive",
                    record.id, record.pid
                );
            }
            let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
        }
        liveness = Liveness::Dead;
    } else {
        admission.unlocked_wait(|| session_wait(&record, STOP_GRACE_MS))?;
        liveness = is_session_alive(&record);
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            liveness = admission.unlocked_wait(|| {
                session_wait(&record, KILL_GRACE_MS);
                cleanup_liveness_after_pid_signal(is_session_alive(&record), record.pid)
            })?;
            if liveness == Liveness::Live {
                let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
                liveness = admission.unlocked_wait(|| {
                    session_wait(&record, KILL_GRACE_MS);
                    cleanup_liveness_after_pid_signal(is_session_alive(&record), record.pid)
                })?;
            }
        }
    }
    let (mut residue, handled) =
        reap_session_tasks_with_wait(admission, &record.id, force, task_wait)?;
    // From here to the session-record decision the admission lock is held
    // without interruption, so nothing can be admitted between the rescan
    // and `remove_session_record`.
    rescan_owned_tasks(admission, &record.id, force, &handled, &mut residue)?;
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

pub(super) fn execute_teardown(control: &Path, force: bool, out: impl Write) -> Result<()> {
    let mut stderr = std::io::stderr();
    execute_teardown_with_timeout(
        control,
        force,
        out,
        Duration::from_millis(CONTROL_RELEASE_TIMEOUT_MS),
        &mut stderr,
    )
}

pub(super) fn execute_teardown_with_timeout(
    control: &Path,
    force: bool,
    mut out: impl Write,
    control_release_timeout: Duration,
    mut warning: impl Write,
) -> Result<()> {
    let service_liveness = request_control_stop(control)?;
    if service_liveness == ControlLiveness::Live {
        wait_for_control_release_with_timeout(control, control_release_timeout, &mut warning)?;
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

/// Waits for the supervisor's control lock with a bounded deadline. A
/// timeout is deliberately non-fatal: the admission lock is independent
/// from `service.lock`, so teardown can still drain the durable records.
pub(super) fn wait_for_control_release_with_timeout(
    control: &Path,
    timeout: Duration,
    mut warning: impl Write,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if probe_control(control)? == ControlLiveness::NotRunning {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            writeln!(
                warning,
                "warning: baton service supervisor did not release the control lock for {} within {}ms; continuing teardown. The supervisor may still hold {}; identify and terminate it before reusing the control directory",
                control.display(),
                timeout.as_millis(),
                control.join(CONTROL_LOCK_FILE).display(),
            )
            .map_err(io_err)?;
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS).min(remaining));
    }
}
