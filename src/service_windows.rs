use super::*;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::os::windows::io::AsRawHandle;
use std::os::windows::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::mailbox;
use crate::message::{MessageEnvelope, MessageKind};
use crate::task::{
    Clock, SystemClock, TaskAdmissionPhase, TaskEventBody, TaskEventKind, TaskRecord, TaskSpec,
    TaskState, max_duration_exceeded, milestones_due, task_event_id,
};
use windows_sys::Win32::Foundation::{
    CloseHandle, FALSE, FILETIME, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JobObjectBasicAccountingInformation, OpenJobObjectW, QueryInformationJobObject,
    TerminateJobObject,
};
use windows_sys::Win32::System::SystemServices::{JOB_OBJECT_QUERY, JOB_OBJECT_TERMINATE};
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, GetCurrentProcess, GetProcessTimes, OpenProcess, OpenThread,
    PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, ResumeThread,
    THREAD_SUSPEND_RESUME, TerminateProcess, WaitForSingleObject,
};

/// A non-inheritable Windows Job Object handle owned by the supervisor.
/// The handle stays open while a managed process tree remains active and
/// closes only after `ActiveProcesses == 0`.
struct JobHandle(HANDLE);

impl JobHandle {
    fn raw(&self) -> HANDLE {
        self.0
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        if self.0 != 0 {
            // SAFETY: This wrapper owns the handle and drops it exactly once.
            unsafe { CloseHandle(self.0) };
        }
    }
}

static SERVICE_JOB: OnceLock<JobHandle> = OnceLock::new();

fn wide_null(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn fresh_job_name(kind: &str) -> String {
    format!(
        r"Local\baton-{kind}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn create_job(name: &str) -> Result<JobHandle> {
    let name = wide_null(name);
    // SAFETY: `name` is a valid, NUL-terminated UTF-16 string and a null
    // security descriptor requests the default, non-inheritable handle.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), name.as_ptr()) };
    if handle == 0 {
        return Err(BatonError::Io(format!(
            "could not create Windows Job Object {name:?}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(JobHandle(handle))
}

fn open_job(name: &str) -> Result<Option<JobHandle>> {
    let name_wide = wide_null(name);
    // SAFETY: `name_wide` is a valid, NUL-terminated UTF-16 string; FALSE
    // deliberately requests a non-inheritable returned handle.
    let handle = unsafe {
        OpenJobObjectW(
            JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE,
            FALSE,
            name_wide.as_ptr(),
        )
    };
    if handle != 0 {
        return Ok(Some(JobHandle(handle)));
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(2) || error.raw_os_error() == Some(3) {
        Ok(None)
    } else {
        Err(BatonError::Io(format!(
            "could not open Windows Job Object {name:?}: {error}"
        )))
    }
}

fn assign_job_to_process(job: &JobHandle, process: HANDLE) -> Result<()> {
    // SAFETY: `job` owns a valid job handle and `process` is a live process
    // handle obtained from `Child` or `GetCurrentProcess`.
    if unsafe { AssignProcessToJobObject(job.raw(), process) } == FALSE {
        return Err(BatonError::Io(format!(
            "could not assign process to Windows Job Object: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn assign_job_to_child(job: &JobHandle, child: &Child) -> Result<()> {
    assign_job_to_process(job, child.as_raw_handle() as HANDLE)
}

fn active_job_processes(job: &JobHandle) -> Result<u32> {
    let mut info = std::mem::MaybeUninit::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>::zeroed();
    // SAFETY: `info` points to writable storage of the exact structure
    // requested by `JobObjectBasicAccountingInformation`.
    let ok = unsafe {
        QueryInformationJobObject(
            job.raw(),
            JobObjectBasicAccountingInformation,
            info.as_mut_ptr().cast(),
            std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if ok == FALSE {
        return Err(BatonError::Io(format!(
            "could not query Windows Job Object accounting: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: the successful query initialized the complete structure.
    Ok(unsafe { info.assume_init() }.ActiveProcesses)
}

fn terminate_job(job: &JobHandle) -> Result<()> {
    // SAFETY: `job` owns a valid handle and the exit code is arbitrary but
    // valid for TerminateJobObject.
    if unsafe { TerminateJobObject(job.raw(), 1) } == FALSE {
        return Err(BatonError::Io(format!(
            "could not terminate Windows Job Object: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// `baton serve` calls this before constructing its participant. The job
/// handle is retained for the process lifetime so the job name remains
/// available while an agent descendant is still active.
pub(super) fn adopt_service_job() -> Result<()> {
    let Some(name) = std::env::var_os("BATON_SERVICE_JOB") else {
        return Ok(());
    };
    let name = name.to_string_lossy();
    let job = open_job(&name)?.ok_or_else(|| {
        BatonError::Io(format!(
            "baton serve could not resolve its Windows Job Object {name:?}"
        ))
    })?;
    // SAFETY: GetCurrentProcess returns the current process pseudo-handle,
    // which is valid for AssignProcessToJobObject and must not be closed.
    let process = unsafe { GetCurrentProcess() };
    assign_job_to_process(&job, process)?;
    SERVICE_JOB
        .set(job)
        .map_err(|_| BatonError::Io("baton serve Windows Job Object was adopted twice".to_string()))
}

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

/// Opens any service lock. Windows process handles are not inherited by
/// `std::process::Command` unless explicitly requested, so the lock file
/// does not need a Unix close-on-exec flag here.
fn service_lock_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
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
    /// Named Job Object owned by the session's supervisor and serve child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job: Option<String>,
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
    let mut sessions = rehydrate_sessions(control)?;
    let mut tasks = rehydrate_tasks(control, &clock)?;
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
        tick_tasks(control, &mut tasks, &clock);

        // One request's failure (a malformed spec, a transient spawn
        // error) must not crash the loop out from under every other
        // session/task this instance already owns — warn and keep
        // polling, the same "one bad message can't wedge the daemon"
        // posture `Mailbox::claim_next` takes for a malformed mailbox
        // entry.
        let mut did_work = false;
        match process_one_request(control) {
            Ok(Some((session_id, running))) => {
                sessions.insert(session_id, running);
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

/// Removes every child whose exit status is already available, reaping
/// it. A still-running child (`Ok(None)`) is left in place.
struct RunningSession {
    child: Option<Child>,
    job: Option<JobHandle>,
}

fn rehydrate_sessions(control: &Path) -> Result<HashMap<String, RunningSession>> {
    let mut sessions = HashMap::new();
    for record in list_session_records(control)? {
        let job = record.job.as_deref().map(open_job).transpose()?.flatten();
        sessions.insert(record.id, RunningSession { child: None, job });
    }
    Ok(sessions)
}

/// Reaps direct children, but retains each Job Object handle until its
/// active-process count reaches zero so a serve-exited grandchild remains
/// reachable through the same job.
fn reap_exited(sessions: &mut HashMap<String, RunningSession>) {
    sessions.retain(|_, running| {
        if let Some(child) = running.child.as_mut() {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => running.child = None,
                Ok(None) => {}
            }
        }
        running
            .job
            .as_ref()
            .map(|job| active_job_processes(job).map_or(true, |count| count != 0))
            .unwrap_or_else(|| running.child.is_some())
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
fn process_one_request(control: &Path) -> Result<Option<(String, RunningSession)>> {
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
                let (record, running) = outcome?;
                return Ok(Some((record.id, running)));
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
) -> Result<(SessionRecord, RunningSession)> {
    let data = fs::read_to_string(spec_path)
        .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
    let spec: SessionSpec = serde_json::from_str(&data).map_err(|err| {
        BatonError::Decode(format!("malformed session spec {spec_path:?}: {err}"))
    })?;
    let job_name = fresh_job_name("session");
    let job = create_job(&job_name)?;
    let mut child = match spawn_serve_child(&spec, &job_name) {
        Ok(child) => child,
        Err(err) => {
            drop(job);
            return Err(err);
        }
    };
    let pid = child.id();
    let (started_at, start_epoch_secs) = recorded_start_identity(pid);
    // Everything below this point must kill+reap `child` before
    // returning `Err`: once this function returns an error, nothing else
    // ever tracks this `Child` (it isn't inserted into `Run`'s
    // `children` map, and `Drop` for `std::process::Child` does not
    // kill), so leaving it running here would leak a live, unrecorded,
    // unreapable `serve` process.
    if !spawn_start_key_ok(&started_at, &start_epoch_secs) {
        let _ = terminate_job(&job);
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
        start_epoch_secs,
        job: Some(job_name),
    };
    if let Err(err) = write_session_record(control, &record) {
        let _ = terminate_job(&job);
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
            fs::create_dir_all(&responses)
                .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
            mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
        });
    if let Err(err) = respond {
        let _ = terminate_job(&job);
        let _ = child.wait();
        let _ = remove_session_record(control, &record.id);
        return Err(err);
    }
    Ok((
        record,
        RunningSession {
            child: Some(child),
            job: Some(job),
        },
    ))
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

/// Spawns `baton serve` detached from this process's stdio. The child
/// adopts the named Job Object before it can create its participant.
fn spawn_serve_child(spec: &SessionSpec, job_name: &str) -> Result<Child> {
    let exe = current_baton_exe()?;
    let mut command = Command::new(&exe);
    command.args(serve_argv(spec));
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command.env("BATON_SERVICE_JOB", job_name);
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

fn task_start_response_claim_path(control: &Path, request_id: &str) -> Result<std::path::PathBuf> {
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
fn take_task_start_response(control: &Path, request_id: &str) -> Result<Option<TaskStartResponse>> {
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
        let mut record = record.clone();
        upgrade_legacy_task_record(control, &mut record)?;
        let mut liveness = is_task_alive(&record);
        if liveness == Liveness::Unresolved {
            return Ok(false);
        }
        if liveness == Liveness::Live {
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            wait_while_task_alive(&record, KILL_GRACE_MS);
            liveness = is_task_alive(&record);
            if liveness == Liveness::Live {
                let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
                wait_while_task_alive(&record, KILL_GRACE_MS);
                liveness = is_task_alive(&record);
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
    /// Handle-only Job Object re-adoption. A missing handle remains an
    /// unresolved identity and is never replaced by a PID tree walk.
    job: Option<JobHandle>,
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
        let job = record.job.as_deref().map(open_job).transpose()?.flatten();
        tasks.insert(
            id,
            RunningTask {
                record,
                child: None,
                job,
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
                let Some((record, child, job, started_ms)) = outcome? else {
                    return Ok(None);
                };
                let id = record.id.clone();
                let running = RunningTask {
                    record,
                    child: Some(child),
                    job: Some(job),
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
) -> Result<Option<(TaskRecord, Child, JobHandle, u64)>> {
    let data = fs::read_to_string(spec_path)
        .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
    let spec: TaskSpec = serde_json::from_str(&data)
        .map_err(|err| BatonError::Decode(format!("malformed task spec {spec_path:?}: {err}")))?;
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
    let (mut child, job, job_name) = spawn_task_child(&spec, &stdout_path, &stderr_path)?;
    let pid = child.id();
    let (started_at, start_epoch_secs) = recorded_start_identity(pid);
    if !spawn_start_key_ok(&started_at, &start_epoch_secs) {
        let _ = terminate_job(&job);
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
        start_epoch_secs,
        job: Some(job_name),
        started_ms: Some(started_ms),
        state: TaskState::Running,
        exit_code: None,
        elapsed_ms: None,
        stdout_path: stdout_path.display().to_string(),
        stderr_path: stderr_path.display().to_string(),
        delivered_milestones: 0,
    };
    if let Err(err) = write_task_record(control, &record) {
        let _ = terminate_job(&job);
        let _ = child.wait();
        return Err(err);
    }
    wait_for_test_task_admission_barrier();
    record.admission = TaskAdmissionPhase::Committed;
    if let Err(err) = write_task_record(control, &record) {
        let _ = terminate_job(&job);
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
        return Ok(Some((record, child, job, started_ms)));
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
    Ok(Some((record, child, job, started_ms)))
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
    let json = serde_json::to_string(response)
        .map_err(|err| BatonError::Io(format!("could not serialize task start response: {err}")))?;
    let responses = task_responses_dir(control);
    fs::create_dir_all(&responses)
        .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
    mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
}

fn resume_initial_thread(pid: u32) -> Result<()> {
    // SAFETY: the snapshot flags and process id are valid; the returned
    // snapshot is closed on every exit path below.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == -1 {
        return Err(BatonError::Io(format!(
            "could not enumerate Windows threads for pid {pid}: {}",
            std::io::Error::last_os_error()
        )));
    }
    let result = (|| {
        // SAFETY: `entry` is writable storage with the size required by
        // Thread32First/Thread32Next.
        let mut entry = unsafe { std::mem::zeroed::<THREADENTRY32>() };
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        // SAFETY: `snapshot` is valid and `entry` has its required size.
        let mut found = unsafe { Thread32First(snapshot, &mut entry) } != FALSE;
        while found {
            if entry.th32OwnerProcessID == pid {
                // SAFETY: the thread id came from the system snapshot and
                // the requested access is limited to suspend/resume.
                let thread =
                    unsafe { OpenThread(THREAD_SUSPEND_RESUME, FALSE, entry.th32ThreadID) };
                if thread == 0 {
                    return Err(BatonError::Io(format!(
                        "could not open initial thread for pid {pid}: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                // SAFETY: `thread` is a valid handle and the process was
                // created with exactly one suspended initial thread.
                let resumed = unsafe { ResumeThread(thread) } != u32::MAX;
                // SAFETY: `thread` is owned by this function.
                unsafe { CloseHandle(thread) };
                if !resumed {
                    return Err(BatonError::Io(format!(
                        "could not resume initial thread for pid {pid}: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                return Ok(());
            }
            entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
            // SAFETY: `snapshot` is valid and `entry` remains writable.
            found = unsafe { Thread32Next(snapshot, &mut entry) } != FALSE;
        }
        Err(BatonError::Io(format!(
            "could not find initial thread for suspended pid {pid}"
        )))
    })();
    // SAFETY: `snapshot` is the owned snapshot handle opened above.
    unsafe { CloseHandle(snapshot) };
    result
}

/// Spawns `spec` suspended, assigns it to a private Job Object before any
/// user code can run, then resumes its one initial thread.
fn spawn_task_child(
    spec: &TaskSpec,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<(Child, JobHandle, String)> {
    let job_name = fresh_job_name("task");
    let job = create_job(&job_name)?;
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
    command.creation_flags(CREATE_SUSPENDED);
    let child = command.spawn().map_err(|err| {
        BatonError::Io(format!(
            "could not spawn task command {:?}: {err}",
            spec.command
        ))
    })?;
    if let Err(err) =
        assign_job_to_child(&job, &child).and_then(|_| resume_initial_thread(child.id()))
    {
        let _ = terminate_job(&job);
        let mut child = child;
        let _ = child.wait();
        return Err(err);
    }
    Ok((child, job, job_name))
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
        let _ = terminate_running_task(running, "-TERM");
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
        let _ = terminate_running_task(running, "-KILL");
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
        Ok(data) => serde_json::from_str(&data)
            .map(Some)
            .map_err(|err| BatonError::Decode(format!("malformed task record {path:?}: {err}"))),
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
        let mut record = record;
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
                let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
                let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            term_sent = true;
        }
        if liveness != Liveness::Dead {
            wait(&record, KILL_GRACE_MS);
            liveness = is_task_alive(&record);
        }
        if liveness == Liveness::Live && !term_sent {
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            wait(&record, KILL_GRACE_MS);
            liveness = is_task_alive(&record);
        }
        if liveness == Liveness::Live {
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
#[cfg(all(any(not(target_os = "linux"), test), not(windows)))]
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
#[cfg(all(any(not(target_os = "linux"), test), not(windows)))]
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

#[cfg(not(windows))]
#[derive(Debug, PartialEq, Eq)]
struct ProcessProbe {
    state: String,
    start_key: String,
}

#[cfg(not(windows))]
impl ProcessProbe {
    fn is_zombie(&self) -> bool {
        self.state.starts_with('Z')
    }
}

/// Parses `/proc/<pid>/stat`; the executable name is `(comm)` and may
/// itself contain `)` or whitespace, so fields are counted from the last
/// `)` rather than split naively.
#[cfg(not(windows))]
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

#[cfg(not(windows))]
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

#[cfg(not(windows))]
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

#[cfg(not(windows))]
fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
    match process_probe(pid) {
        ProbeResult::Present(probe) if !probe.is_zombie() => (Some(probe.start_key), None),
        _ => (None, None),
    }
}

/// Whether a freshly-spawned child's start key is trustworthy enough to
/// persist. A missing key means the child was already gone or a zombie
/// microseconds after `spawn()` — fail closed as a spawn failure.
#[cfg(not(windows))]
fn spawn_start_key_ok(started_at: &Option<String>, _start_epoch_secs: &Option<i64>) -> bool {
    started_at.is_some()
}

fn job_name_resolves(name: Option<&str>) -> bool {
    match name {
        None => true,
        Some(name) => matches!(open_job(name), Ok(Some(_))),
    }
}

fn record_job_available(name: Option<&str>) -> bool {
    name.is_some() && job_name_resolves(name)
}

#[cfg(not(windows))]
fn linux_session_argv_matches(actual: &[String], spec: &SessionSpec) -> bool {
    let expected = serve_argv(spec);
    actual.len() >= expected.len() && actual.ends_with(&expected)
}

#[cfg(not(windows))]
fn linux_task_argv_matches(actual: &[String], record: &TaskRecord) -> bool {
    let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
    expected.push(record.spec.command.clone());
    expected.extend(record.spec.args.iter().cloned());
    actual == expected
}

#[cfg(not(windows))]
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

#[cfg(not(windows))]
fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

#[cfg(not(windows))]
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

#[cfg(not(windows))]
fn is_task_alive(record: &TaskRecord) -> Liveness {
    task_liveness(record).0
}

/// A non-Linux Unix `ps` sample. macOS has no `/proc`, so `state`,
/// `lstart`, and the untruncated command line are the available process
/// corroborators. Every probe pins the locale and time zone so the
/// canonical key is independent of the supervisor/client environment.
#[cfg(all(not(target_os = "linux"), not(windows)))]
#[derive(Debug, PartialEq, Eq)]
struct ProcessProbe {
    state: String,
    start_key: String,
    start_epoch_secs: Option<i64>,
    command: String,
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
impl ProcessProbe {
    fn is_zombie(&self) -> bool {
        self.state.starts_with('Z')
    }
}

/// Parses one `ps -p <pid> -o state=,lstart=,command=` row. The five
/// lstart fields are fixed by the C locale; the remaining text is the
/// command line and is whitespace-normalized for comparison.
#[cfg(all(not(target_os = "linux"), not(windows)))]
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn start_identity_from_probe(probe: &ProcessProbe) -> (Option<String>, Option<i64>) {
    if probe.is_zombie() {
        (None, None)
    } else {
        (Some(probe.start_key.clone()), probe.start_epoch_secs)
    }
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
    match process_probe(pid) {
        ProbeResult::Present(probe) => start_identity_from_probe(&probe),
        _ => (None, None),
    }
}

/// A missing start key after spawn means the process was already gone or
/// a zombie, so fail closed rather than persisting an uncorroborated PID.
#[cfg(all(not(target_os = "linux"), not(windows)))]
fn spawn_start_key_ok(started_at: &Option<String>, start_epoch_secs: &Option<i64>) -> bool {
    started_at.is_some() && start_epoch_secs.is_some()
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn normalize_process_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn session_argv_matches(command: &str, spec: &SessionSpec) -> bool {
    let command = normalize_process_text(command);
    let expected = serve_argv(spec).join(" ");
    if expected.is_empty() {
        return false;
    }
    command == expected || command.ends_with(&format!(" {expected}"))
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn task_argv_matches(command: &str, record: &TaskRecord) -> bool {
    let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
    expected.push(record.spec.command.as_str());
    expected.extend(record.spec.args.iter().map(String::as_str));
    normalize_process_text(command) == normalize_process_text(&expected.join(" "))
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

#[cfg(all(not(target_os = "linux"), not(windows)))]
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

#[cfg(all(not(target_os = "linux"), not(windows)))]
fn is_task_alive(record: &TaskRecord) -> Liveness {
    task_liveness(record).0
}

/// Returns a Windows process creation-time key only while the PID still
/// names a live process. An inaccessible process is unresolved; it is
/// never treated as a safe kill target.
fn process_probe(pid: u32) -> ProbeResult<String> {
    if pid <= 1 {
        return ProbeResult::Gone;
    }
    // SAFETY: the access mask is limited to querying, synchronizing, and
    // reading process creation time for the supplied PID.
    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            FALSE,
            pid,
        )
    };
    if process == 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(2 | 3 | 87) => ProbeResult::Gone,
            _ => ProbeResult::Unreadable,
        };
    }
    // SAFETY: `process` is a valid process handle owned by this function.
    let wait = unsafe { WaitForSingleObject(process, 0) };
    if wait == WAIT_OBJECT_0 {
        // SAFETY: `process` is the handle opened above.
        unsafe { CloseHandle(process) };
        return ProbeResult::Gone;
    }
    if wait != WAIT_TIMEOUT {
        // SAFETY: `process` is the handle opened above.
        unsafe { CloseHandle(process) };
        return ProbeResult::Unreadable;
    }
    let mut created = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exited = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut kernel = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut user = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    // SAFETY: all FILETIME pointers are writable and `process` is valid.
    let ok = unsafe { GetProcessTimes(process, &mut created, &mut exited, &mut kernel, &mut user) };
    // SAFETY: `process` is the handle opened above.
    unsafe { CloseHandle(process) };
    if ok == FALSE {
        return ProbeResult::Unreadable;
    }
    ProbeResult::Present(format!(
        "{:08x}{:08x}",
        created.dwHighDateTime, created.dwLowDateTime
    ))
}

fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
    match process_probe(pid) {
        ProbeResult::Present(start_key) => (Some(start_key), None),
        _ => (None, None),
    }
}

fn spawn_start_key_ok(started_at: &Option<String>, _start_epoch_secs: &Option<i64>) -> bool {
    started_at.is_some()
}

fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
    match process_probe(record.pid) {
        ProbeResult::Gone if record.job.is_some() && !job_name_resolves(record.job.as_deref()) => {
            (Liveness::Unresolved, None)
        }
        ProbeResult::Gone => (Liveness::Dead, None),
        ProbeResult::Unreadable => (Liveness::Unresolved, None),
        ProbeResult::Present(current) => match &record.started_at {
            Some(expected) if expected == &current && job_name_resolves(record.job.as_deref()) => {
                (Liveness::Live, None)
            }
            Some(expected) if expected == &current => (Liveness::Unresolved, None),
            Some(_) => (Liveness::Dead, None),
            None => (Liveness::Unresolved, None),
        },
    }
}

fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

fn task_liveness(record: &TaskRecord) -> (Liveness, Option<i64>) {
    match process_probe(record.pid) {
        ProbeResult::Gone if record.job.is_some() && !job_name_resolves(record.job.as_deref()) => {
            (Liveness::Unresolved, None)
        }
        ProbeResult::Gone => (Liveness::Dead, None),
        ProbeResult::Unreadable => (Liveness::Unresolved, None),
        ProbeResult::Present(current) => match &record.started_at {
            Some(expected) if expected == &current && job_name_resolves(record.job.as_deref()) => {
                (Liveness::Live, None)
            }
            Some(expected) if expected == &current => (Liveness::Unresolved, None),
            Some(_) => (Liveness::Dead, None),
            None => (Liveness::Unresolved, None),
        },
    }
}

fn is_task_alive(record: &TaskRecord) -> Liveness {
    task_liveness(record).0
}

/// Persists the canonical epoch after a legacy macOS record is rescued by
/// the fallback ladder. Callers must hold the admission lock; status and
/// supervisor tick paths intentionally remain read-only.
#[cfg(all(target_os = "macos", not(windows)))]
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
#[cfg(all(target_os = "macos", not(windows)))]
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

/// Terminates only the recorded PID. This is the explicit force fallback
/// after a named Job Object can no longer be resolved; it makes no claim
/// about descendants.
fn signal_group(pid: u32, sig: &str) -> Result<()> {
    if pid <= 1 {
        return Ok(());
    }
    // SAFETY: the handle is opened for this recorded PID only, with no
    // tree-walking or inherited-handle behavior.
    let process = unsafe { OpenProcess(PROCESS_TERMINATE, FALSE, pid) };
    if process == 0 {
        return Ok(());
    }
    // SAFETY: `process` is the valid handle opened immediately above.
    let result = unsafe { TerminateProcess(process, 1) };
    // SAFETY: `process` is owned by this function.
    unsafe { CloseHandle(process) };
    if result == FALSE {
        return Err(BatonError::Io(format!(
            "could not terminate recorded Windows pid {pid} for {sig}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn terminate_record_job(job_name: Option<&str>, pid: u32, sig: &str) -> Result<bool> {
    if let Some(name) = job_name
        && let Some(job) = open_job(name)?
    {
        terminate_job(&job)?;
        return Ok(true);
    }
    signal_group(pid, sig)?;
    Ok(false)
}

fn terminate_running_task(running: &RunningTask, sig: &str) -> Result<()> {
    if let Some(job) = running.job.as_ref() {
        terminate_job(job)
    } else {
        signal_group(running.record.pid, sig)
    }
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
    let mut record = record.clone();
    upgrade_legacy_session_record(control, &mut record)?;
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
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
        }
        liveness = Liveness::Dead;
    } else {
        wait_while_alive(&record, STOP_GRACE_MS);
        liveness = is_session_alive(&record);
        if liveness == Liveness::Live {
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            wait_while_alive(&record, KILL_GRACE_MS);
            liveness = is_session_alive(&record);
            if liveness == Liveness::Live {
                let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
                wait_while_alive(&record, KILL_GRACE_MS);
                liveness = is_session_alive(&record);
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
            argv: session_recorded_argv(&record),
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
        Ok(data) => serde_json::from_str(&data)
            .map(Some)
            .map_err(|err| BatonError::Decode(format!("malformed session record {path:?}: {err}"))),
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
        let _ = terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
        wait_while_task_alive(record, KILL_GRACE_MS);
        if is_task_alive(record) == Liveness::Live {
            let _ = terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
