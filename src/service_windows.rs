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
    TaskState, first_non_ascending_milestone, max_duration_exceeded, milestones_due, task_event_id,
};
use windows_sys::Win32::Foundation::{
    CloseHandle, FALSE, FILETIME, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
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
    CREATE_SUSPENDED, GetProcessTimes, OpenProcess, OpenThread, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, ResumeThread, THREAD_SUSPEND_RESUME, TerminateProcess,
    WaitForSingleObject,
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
        r"Local\baton-{kind}-{}-{}-{}",
        std::process::id(),
        crate::events::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn create_job(name: &str, inheritable: bool) -> Result<JobHandle> {
    let name = wide_null(name);
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: if inheritable { 1 } else { 0 },
    };
    let attributes_ptr = if inheritable {
        &mut attributes
    } else {
        std::ptr::null_mut()
    };
    // SAFETY: `name` is a valid, NUL-terminated UTF-16 string and a null
    // security descriptor requests the default security. When requested,
    // the handle is inheritable so a task process can keep the named job
    // alive across a supervisor restart and descendant drain.
    let handle = unsafe { CreateJobObjectW(attributes_ptr, name.as_ptr()) };
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

/// `baton serve` calls this before constructing its participant. `Run` has
/// already assigned the child process to the Job Object; this handle is
/// retained so the named job remains available while descendants are active.
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
/// Initial delay before retrying a failed task-event callback delivery.
/// Governs both milestone and terminal delivery — the same bounded
/// exponential backoff policy applies to every task event.
const EVENT_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
/// Longest delay between task-event callback delivery attempts.
const EVENT_RETRY_MAX_DELAY_MS: u64 = 60_000;
/// Total callback delivery attempts for a single task event before it is
/// dropped (a terminal event drops the tracker entry; a milestone is
/// skipped so supervision continues).
const MAX_EVENT_DELIVERY_ATTEMPTS: u32 = 10;
/// Bound on how long `Start` waits for a live `Run` to answer.
const START_AWAIT_MS: u64 = 10_000;
/// Bound on the cooperative `serve --stop` grace before escalating to
/// `TerminateJobObject`.
const STOP_GRACE_MS: u64 = 5_000;
/// Bound on the second `TerminateJobObject` attempt's grace.
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
    /// Windows process creation-time key from `GetProcessTimes`,
    /// corroborating `pid` against reuse after a `Run` restart; `None`
    /// where the platform probe could not be read or for a legacy record.
    started_at: Option<String>,
    /// Legacy Unix epoch field retained for cross-platform record decoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_epoch_secs: Option<i64>,
    /// Named Job Object owned by the session's supervisor and serve child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job: Option<String>,
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
                let Some((record, running)) = outcome? else {
                    return Ok(None);
                };
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
fn write_start_response(control: &Path, request_id: &str, response: &StartResponse) -> Result<()> {
    let json = serde_json::to_string(response)
        .map_err(|err| BatonError::Io(format!("could not serialize start response: {err}")))?;
    let responses = responses_dir(control);
    fs::create_dir_all(&responses)
        .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
    mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)
}

/// Answers a claimed start request with an admission failure the supervisor
/// can name, so the client fails immediately with the real reason instead of
/// waiting out [`START_AWAIT_MS`]. Only the response write itself can still
/// fail the request loop.
fn reject_start_request(
    control: &Path,
    request_id: &str,
    error: String,
) -> Result<Option<(SessionRecord, RunningSession)>> {
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
/// An admission failure after the request is claimed — a job/spawn failure, a
/// post-spawn corroboration failure, a record-write failure — is answered as
/// an error response and reported as `Ok(None)`; only a failure to deliver a
/// response at all is propagated as `Err`.
fn handle_start_request(
    control: &Path,
    request_id: &str,
    spec_path: &Path,
) -> Result<Option<(SessionRecord, RunningSession)>> {
    let data = fs::read_to_string(spec_path)
        .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
    let spec: SessionSpec = serde_json::from_str(&data).map_err(|err| {
        BatonError::Decode(format!("malformed session spec {spec_path:?}: {err}"))
    })?;
    let job_name = fresh_job_name("session");
    let job = match create_job(&job_name, false) {
        Ok(job) => job,
        Err(err) => {
            return reject_start_request(control, request_id, admission_error_text(&err));
        }
    };
    let mut child = match spawn_serve_child(&spec, &job_name) {
        Ok(child) => child,
        Err(err) => {
            drop(job);
            return reject_start_request(control, request_id, admission_error_text(&err));
        }
    };
    if let Err(err) =
        assign_job_to_child(&job, &child).and_then(|_| resume_initial_thread(child.id()))
    {
        let _ = terminate_job(&job);
        let _ = child.wait();
        return reject_start_request(control, request_id, admission_error_text(&err));
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
    command.creation_flags(CREATE_SUSPENDED);
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
        let mut liveness = is_task_alive(&record);
        if liveness == Liveness::Unresolved {
            return Ok(false);
        }
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            wait_while_task_alive(&record, KILL_GRACE_MS);
            liveness = cleanup_liveness_after_pid_signal(is_task_alive(&record), record.pid);
            if liveness == Liveness::Live {
                let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
                wait_while_task_alive(&record, KILL_GRACE_MS);
                liveness = cleanup_liveness_after_pid_signal(is_task_alive(&record), record.pid);
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

/// Exit outcome of a task's direct child, retained across the descendant
/// drain so the eventual terminal state reflects the command's own result
/// instead of defaulting to a code-less failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildExit {
    /// `ExitStatus::success()` of the direct child.
    succeeded: bool,
    /// `ExitStatus::code()` of the direct child.
    code: Option<i32>,
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
    /// Best-effort Job Object re-adoption. A missing handle does not change
    /// a corroborated PID's identity; signaling falls back to that PID and
    /// makes no descendant-reachability claim.
    job: Option<JobHandle>,
    started_ms: u64,
    /// Set once this task's max duration has been exceeded and a first Job
    /// Object termination was sent, so a later tick knows to escalate after
    /// `KILL_GRACE_MS`, and a successful reap after this is set is
    /// attributed to `timeout`, not `completed`/`failed`.
    term_sent_at_ms: Option<u64>,
    /// Set once the second Job Object termination has been sent, so it is
    /// only ever sent once.
    kill_sent: bool,
    /// Number of failed terminal callback delivery attempts. These are
    /// deliberately in-memory: a terminal record is replayed once after
    /// restart, then follows the same bounded retry policy.
    terminal_delivery_attempts: u32,
    /// Clock deadline at which the next terminal callback delivery may be
    /// attempted.
    next_terminal_retry_ms: Option<u64>,
    /// Delay used for the most recent failed terminal delivery, so the next
    /// failure can double it without a wall-clock dependency.
    terminal_retry_delay_ms: u64,
    /// Number of failed delivery attempts for the current lowest undelivered
    /// milestone. Reset once that milestone is delivered or dropped. In-memory
    /// only, like the terminal counters: a rehydrated task re-attempts a due
    /// milestone with a fresh backoff.
    milestone_delivery_attempts: u32,
    /// Clock deadline at which the next milestone callback delivery may be
    /// attempted; `None` when no milestone delivery is currently backed off.
    next_milestone_retry_ms: Option<u64>,
    /// Delay used for the most recent failed milestone delivery, so the next
    /// failure can double it without a wall-clock dependency.
    milestone_retry_delay_ms: u64,
    /// Exit outcome of the direct child, stashed when the task parks to
    /// drain surviving Job Object descendants (which clears `child`).
    /// `None` for a task that never owned a child handle (rehydrated after
    /// a supervisor restart) or whose child has not exited yet.
    child_exit: Option<ChildExit>,
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
                terminal_delivery_attempts: 0,
                next_terminal_retry_ms: None,
                terminal_retry_delay_ms: 0,
                milestone_delivery_attempts: 0,
                next_milestone_retry_ms: None,
                milestone_retry_delay_ms: 0,
                child_exit: None,
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
                    terminal_delivery_attempts: 0,
                    next_terminal_retry_ms: None,
                    terminal_retry_delay_ms: 0,
                    milestone_delivery_attempts: 0,
                    next_milestone_retry_ms: None,
                    milestone_retry_delay_ms: 0,
                    child_exit: None,
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
) -> Result<Option<(TaskRecord, Child, JobHandle, u64)>> {
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
/// creation, spawn, post-spawn corroboration, record write — is answered as an
/// error response and reported as `Ok(None)`, so the client fails immediately
/// with the real reason instead of waiting out [`START_AWAIT_MS`]. Only a
/// failure to deliver a response at all is propagated as `Err`.
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
    if let Some((previous, current)) = first_non_ascending_milestone(&spec.milestones_ms) {
        return reject_task_start_request(
            control,
            request_id,
            format!(
                "task start rejected: --milestone-ms values must be strictly ascending: got {previous} followed by {current}"
            ),
        );
    }
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
    let (mut child, job, job_name) = match spawn_task_child(&spec, &stdout_path, &stderr_path) {
        Ok(spawned) => spawned,
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
        let _ = terminate_job(&job);
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
        return reject_task_start_request(control, request_id, admission_error_text(&err));
    }
    wait_for_test_task_admission_barrier();
    record.admission = TaskAdmissionPhase::Committed;
    if let Err(err) = write_task_record(control, &record) {
        let _ = terminate_job(&job);
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
/// production callers never set it. This helper is compiled only with debug
/// assertions; release builds use the no-op fallback below so the test seam
/// cannot affect a shipped service.
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
/// is compiled only with debug assertions; the release fallback is a no-op.
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
/// production callers never set it. This helper is compiled only with debug
/// assertions; the release fallback is a no-op.
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

/// Spawns `spec` suspended, assigns it to a private inheritable Job Object
/// before any user code can run, then resumes its one initial thread. The
/// inherited handle keeps the named Job Object available to a restarted
/// supervisor while the task process tree is still draining.
fn spawn_task_child(
    spec: &TaskSpec,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<(Child, JobHandle, String)> {
    let job_name = fresh_job_name("task");
    let job = create_job(&job_name, true)?;
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
/// newly-due milestone events, escalates Job Object termination past
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
    if !task_record_exists(control, id)? {
        return Ok(TaskTick::Finished);
    }
    if running.record.state != TaskState::Running {
        return deliver_terminal_event(running, clock);
    }

    let elapsed_ms = clock.now_ms().saturating_sub(running.started_ms);

    // Deliver due milestones best-effort: a failing or backed-off callback
    // inbox must not abort the tick before the liveness/timeout/reap handling
    // below, or an unreachable inbox would keep a task un-reapable forever and
    // re-fire the same milestone at loop rate.
    deliver_due_milestones(control, id, running, elapsed_ms, clock)?;

    // A rehydrated task has no Child handle. Check its identity before
    // any timeout signal so a gone or PID-reused process is never
    // accidentally signalled as this task. An unresolved identity is
    // retained and retried on a later tick.
    if running.child.is_none() {
        match is_task_alive(&running.record) {
            Liveness::Dead => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
            }
            Liveness::Live => {}
            Liveness::Unresolved if controlled_task_pid_is_gone(control, id, running)? => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
            }
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
                    let (state, exit_code) = parked_terminal(running, cancelled);
                    return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
                }
                Liveness::Live => {}
                Liveness::Unresolved if controlled_task_pid_is_gone(control, id, running)? => {
                    let cancelled = consume_task_cancel_sentinel(control, id)?;
                    let (state, exit_code) = parked_terminal(running, cancelled);
                    return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
                }
                Liveness::Unresolved => return Ok(TaskTick::StillRunning),
            }
        }
        let _ = terminate_running_task(running, "-KILL");
        running.kill_sent = true;
    }

    match running.child.as_mut() {
        None => match is_task_alive(&running.record) {
            Liveness::Live => Ok(TaskTick::StillRunning),
            Liveness::Unresolved if controlled_task_pid_is_gone(control, id, running)? => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                finalize_task(control, running, state, exit_code, elapsed_ms, clock)
            }
            Liveness::Unresolved => Ok(TaskTick::StillRunning),
            Liveness::Dead => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                finalize_task(control, running, state, exit_code, elapsed_ms, clock)
            }
        },
        Some(child) => match child.try_wait() {
            Ok(Some(status)) => {
                if let Some(job) = running.job.as_ref() {
                    match active_job_processes(job) {
                        Ok(0) => {}
                        Ok(_) | Err(_) => {
                            // The direct command may have exited while a
                            // descendant remains. Keep the Job Object handle
                            // and continue tracking the tree until it drains,
                            // stashing the direct child's outcome so the
                            // eventual terminal state reflects the command's
                            // own result instead of a code-less failure.
                            running.child_exit = Some(ChildExit {
                                succeeded: status.success(),
                                code: status.code(),
                            });
                            running.child = None;
                            return Ok(TaskTick::StillRunning);
                        }
                    }
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

/// Resolves the terminal state and exit code for a task whose `Child`
/// handle is gone. Cancel and timeout sentinels win over the direct
/// child's stashed exit outcome (which is `None` for a rehydrated task
/// that never owned a child handle); otherwise the stashed outcome
/// decides `completed`/`failed` and supplies the real exit code.
fn parked_terminal(running: &RunningTask, cancelled: bool) -> (TaskState, Option<i32>) {
    if cancelled {
        (TaskState::Cancelled, None)
    } else if running.term_sent_at_ms.is_some() {
        (TaskState::Timeout, None)
    } else if running.child_exit.is_some_and(|exit| exit.succeeded) {
        (
            TaskState::Completed,
            running.child_exit.and_then(|exit| exit.code),
        )
    } else {
        (
            TaskState::Failed,
            running.child_exit.and_then(|exit| exit.code),
        )
    }
}

/// Delivers every milestone newly due at `elapsed_ms`, best-effort.
///
/// A callback inbox that is unavailable is handled by the same bounded
/// exponential backoff [`deliver_terminal_event`] applies: the lowest
/// undelivered milestone is retried on a doubling delay (from
/// [`EVENT_RETRY_INITIAL_DELAY_MS`], capped at [`EVENT_RETRY_MAX_DELAY_MS`]) and
/// dropped after [`MAX_EVENT_DELIVERY_ATTEMPTS`], so a stuck inbox never
/// re-fires a milestone at loop rate. Unlike the terminal outcome, milestone
/// retry/drop warnings are emitted here rather than routed up through
/// [`TaskTick`], because the caller's tick must continue past them to reap,
/// time out, and cancel the task regardless.
///
/// Only a control-dir *write* failure propagates (mirroring [`finalize_task`]);
/// a callback-delivery failure is absorbed into the backoff so supervision is
/// never blocked by an unreachable inbox.
fn deliver_due_milestones(
    control: &Path,
    id: &str,
    running: &mut RunningTask,
    elapsed_ms: u64,
    clock: &dyn Clock,
) -> Result<()> {
    let now_ms = clock.now_ms();
    if let Some(next_retry_ms) = running.next_milestone_retry_ms
        && now_ms < next_retry_ms
    {
        return Ok(());
    }

    for index in milestones_due(
        elapsed_ms,
        &running.record.spec.milestones_ms,
        running.record.delivered_milestones,
    ) {
        match deliver_task_event(&running.record, TaskEventKind::Milestone { index }) {
            Ok(()) => {
                running.record.delivered_milestones = index + 1;
                if let Err(err) = write_task_record(control, &running.record) {
                    running.record.delivered_milestones = index;
                    return Err(err);
                }
                // A milestone delivered clears any backoff left by an earlier
                // failure, so the next milestone starts fresh.
                running.milestone_delivery_attempts = 0;
                running.next_milestone_retry_ms = None;
                running.milestone_retry_delay_ms = 0;
            }
            Err(err) => {
                let attempt = running.milestone_delivery_attempts.saturating_add(1);
                running.milestone_delivery_attempts = attempt;
                if attempt >= MAX_EVENT_DELIVERY_ATTEMPTS {
                    eprintln!(
                        "warning: baton service dropped milestone {index} for task {id} after {attempt} failed deliveries to callback inbox {:?}: {err}",
                        running.record.spec.callback.inbox
                    );
                    // Advance past the dropped milestone and persist it, so a
                    // supervisor restart does not re-enter the same stuck
                    // milestone via `rehydrate_tasks`.
                    running.record.delivered_milestones = index + 1;
                    if let Err(write_err) = write_task_record(control, &running.record) {
                        running.record.delivered_milestones = index;
                        return Err(write_err);
                    }
                    running.milestone_delivery_attempts = 0;
                    running.next_milestone_retry_ms = None;
                    running.milestone_retry_delay_ms = 0;
                    // Let the next tick handle any further due milestones, each
                    // with its own fresh backoff, rather than hammering the same
                    // unavailable inbox for the whole batch now.
                    break;
                }

                let delay_ms = if attempt == 1 {
                    EVENT_RETRY_INITIAL_DELAY_MS
                } else {
                    running
                        .milestone_retry_delay_ms
                        .saturating_mul(2)
                        .min(EVENT_RETRY_MAX_DELAY_MS)
                };
                running.milestone_retry_delay_ms = delay_ms;
                running.next_milestone_retry_ms = Some(now_ms.saturating_add(delay_ms));
                eprintln!(
                    "warning: baton service failed to deliver milestone {index} for task {id} to callback inbox {:?} (attempt {attempt}/{MAX_EVENT_DELIVERY_ATTEMPTS}; retrying in {delay_ms} ms): {err}",
                    running.record.spec.callback.inbox
                );
                // Milestones are delivered in order; stop the batch until this
                // one is delivered or dropped.
                break;
            }
        }
    }
    Ok(())
}

/// Delivers a terminal event, applying bounded exponential backoff when the
/// callback inbox is unavailable.
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
            if attempt >= MAX_EVENT_DELIVERY_ATTEMPTS {
                return Ok(TaskTick::TerminalDeliveryDropped {
                    error,
                    attempts: attempt,
                });
            }

            let delay_ms = if attempt == 1 {
                EVENT_RETRY_INITIAL_DELAY_MS
            } else {
                running
                    .terminal_retry_delay_ms
                    .saturating_mul(2)
                    .min(EVENT_RETRY_MAX_DELAY_MS)
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
    running.record.state = state;
    running.record.exit_code = exit_code;
    running.record.elapsed_ms = Some(elapsed_ms);
    if let Err(err) = write_task_record(control, &running.record) {
        running.record = previous;
        return Err(err);
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
                    "warning: baton service failed to deliver terminal event for task {id} to callback inbox {:?} (attempt {attempt}/{MAX_EVENT_DELIVERY_ATTEMPTS}; retrying in {delay_ms} ms): {error}",
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
        Ok(data) => serde_json::from_str(&data)
            .map(Some)
            .map_err(|err| BatonError::Decode(format!("malformed task record {path:?}: {err}"))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
    }
}

fn task_record_exists(control: &Path, id: &str) -> Result<bool> {
    let path = task_record_path(control, id)?;
    path.try_exists()
        .map_err(|err| BatonError::Io(format!("could not probe {path:?}: {err}")))
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
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
                let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
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
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            term_sent = true;
        }
        if liveness != Liveness::Dead {
            wait(&record, KILL_GRACE_MS);
            liveness = cleanup_liveness_after_pid_signal(is_task_alive(&record), record.pid);
        }
        if liveness == Liveness::Live && !term_sent {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            wait(&record, KILL_GRACE_MS);
            liveness = cleanup_liveness_after_pid_signal(is_task_alive(&record), record.pid);
        }
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
            wait(&record, KILL_GRACE_MS);
            liveness = cleanup_liveness_after_pid_signal(is_task_alive(&record), record.pid);
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
// `cancelled` rather than misreading a forced exit as `failed`.

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
#[cfg(windows)]
fn job_name_resolves(name: Option<&str>) -> bool {
    match name {
        None => true,
        Some(name) => matches!(open_job(name), Ok(Some(_))),
    }
}

#[cfg(windows)]
fn record_job_available(name: Option<&str>) -> bool {
    name.is_some() && job_name_resolves(name)
}

#[cfg(windows)]
fn job_tree_liveness(name: Option<&str>) -> Option<Liveness> {
    let name = name?;
    match open_job(name) {
        Ok(Some(job)) => match active_job_processes(&job) {
            Ok(0) => Some(Liveness::Dead),
            Ok(_) => Some(Liveness::Live),
            Err(_) => Some(Liveness::Unresolved),
        },
        Ok(None) | Err(_) => Some(Liveness::Unresolved),
    }
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
        ProbeResult::Gone => (
            job_tree_liveness(record.job.as_deref()).unwrap_or(Liveness::Dead),
            None,
        ),
        ProbeResult::Unreadable => (Liveness::Unresolved, None),
        ProbeResult::Present(current) => match &record.started_at {
            Some(expected) if expected == &current => (Liveness::Live, None),
            Some(_) => (Liveness::Unresolved, None),
            None => (Liveness::Unresolved, None),
        },
    }
}

fn is_session_alive(record: &SessionRecord) -> Liveness {
    session_liveness(record).0
}

fn task_liveness(record: &TaskRecord) -> (Liveness, Option<i64>) {
    match process_probe(record.pid) {
        ProbeResult::Gone => (
            job_tree_liveness(record.job.as_deref()).unwrap_or(Liveness::Dead),
            None,
        ),
        ProbeResult::Unreadable => (Liveness::Unresolved, None),
        ProbeResult::Present(current) => match &record.started_at {
            Some(expected) if expected == &current => (Liveness::Live, None),
            Some(_) => (Liveness::Unresolved, None),
            None => (Liveness::Unresolved, None),
        },
    }
}

fn is_task_alive(record: &TaskRecord) -> Liveness {
    task_liveness(record).0
}

fn upgrade_legacy_session_record(_control: &Path, _record: &mut SessionRecord) -> Result<()> {
    Ok(())
}

fn upgrade_legacy_task_record(_control: &Path, _record: &mut TaskRecord) -> Result<()> {
    Ok(())
}

/// Terminates only the recorded PID after its identity has been corroborated.
/// This is the fallback when a named Job Object cannot be resolved; it makes
/// no claim about descendants.
fn terminate_record_pid(pid: u32, phase: &str) -> Result<()> {
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
            "could not terminate recorded Windows pid {pid} during {phase}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn terminate_record_job(job_name: Option<&str>, _pid: u32, _phase: &str) -> Result<bool> {
    if let Some(name) = job_name {
        if let Some(job) = open_job(name)? {
            terminate_job(&job)?;
            return Ok(true);
        }
        return Ok(false);
    }
    Ok(false)
}

/// Terminates a corroborated record through its Job Object when possible,
/// otherwise falls back to the recorded PID. The fallback is safe for the
/// identity-confirmed process but cannot promise descendant termination.
fn terminate_record_job_or_pid(job_name: Option<&str>, pid: u32, phase: &str) -> Result<()> {
    match terminate_record_job(job_name, pid, phase) {
        Ok(true) => Ok(()),
        Ok(false) => {
            eprintln!(
                "warning: Windows Job Object unavailable during {phase}; terminating only recorded pid {pid}; descendants may survive"
            );
            terminate_record_pid(pid, phase)
        }
        Err(err) => {
            eprintln!(
                "warning: Windows Job Object termination during {phase} failed ({err}); terminating only recorded pid {pid}; descendants may survive"
            );
            terminate_record_pid(pid, phase)
        }
    }
}

fn force_terminate_record_job(job_name: Option<&str>, pid: u32, phase: &str) -> Result<()> {
    match terminate_record_job(job_name, pid, phase) {
        Ok(true) => Ok(()),
        Ok(false) => terminate_record_pid(pid, phase),
        Err(_) => terminate_record_pid(pid, phase),
    }
}

fn terminate_running_task(running: &RunningTask, phase: &str) -> Result<()> {
    if let Some(job) = running.job.as_ref() {
        terminate_job(job)
    } else {
        eprintln!(
            "warning: Windows Job Object unavailable for task {}; terminating only recorded pid {}; descendants may survive",
            running.record.id, running.record.pid
        );
        terminate_record_pid(running.record.pid, phase)
    }
}

/// A PID-only cancel/timeout may leave an unknown descendant after the
/// supervisor's Job Object handle was lost. Once the corroborated recorded
/// PID is gone, a controlled outcome can still be finalized without claiming
/// that an unresolvable descendant tree is gone.
fn controlled_task_pid_is_gone(control: &Path, id: &str, running: &RunningTask) -> Result<bool> {
    let controlled =
        running.term_sent_at_ms.is_some() || task_cancel_sentinel_path(control, id).is_file();
    Ok(controlled && recorded_pid_is_gone(running.record.pid))
}

fn recorded_pid_is_gone(pid: u32) -> bool {
    matches!(process_probe(pid), ProbeResult::Gone)
}

/// A PID-only cleanup has no tree evidence after the Job Object name is
/// lost. Once the corroborated recorded PID is gone, cleanup may remove its
/// record while the signal path has already warned that descendants may
/// survive.
fn cleanup_liveness_after_pid_signal(liveness: Liveness, pid: u32) -> Liveness {
    if liveness == Liveness::Unresolved && recorded_pid_is_gone(pid) {
        Liveness::Dead
    } else {
        liveness
    }
}

fn wait_while_alive(record: &SessionRecord, grace_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(grace_ms);
    while {
        let liveness = is_session_alive(record);
        liveness != Liveness::Dead
            && !(liveness == Liveness::Unresolved && recorded_pid_is_gone(record.pid))
            && Instant::now() < deadline
    } {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

fn wait_while_task_alive(record: &TaskRecord, grace_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(grace_ms);
    while {
        let liveness = is_task_alive(record);
        liveness != Liveness::Dead
            && !(liveness == Liveness::Unresolved && recorded_pid_is_gone(record.pid))
            && Instant::now() < deadline
    } {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

/// Stops one session. The caller must hold the admission lock:
/// cooperative `serve --stop` on its inbox first,
/// bounded wait, then Job Object termination if still alive, then reaps
/// every task this session owns
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
            let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-TERM");
            let _ = force_terminate_record_job(record.job.as_deref(), record.pid, "-KILL");
        }
        liveness = Liveness::Dead;
    } else {
        wait_while_alive(&record, STOP_GRACE_MS);
        liveness = is_session_alive(&record);
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            wait_while_alive(&record, KILL_GRACE_MS);
            liveness = cleanup_liveness_after_pid_signal(is_session_alive(&record), record.pid);
            if liveness == Liveness::Live {
                let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
                wait_while_alive(&record, KILL_GRACE_MS);
                liveness = cleanup_liveness_after_pid_signal(is_session_alive(&record), record.pid);
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
        let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
        wait_while_task_alive(record, KILL_GRACE_MS);
        if is_task_alive(record) == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
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
    use crate::task::{FakeClock, TASK_SPEC_SCHEMA, TaskCallback};
    use std::path::PathBuf;

    fn session_spec() -> SessionSpec {
        SessionSpec {
            schema: SESSION_SPEC_SCHEMA.to_string(),
            inbox: "test-inbox".to_string(),
            outbox: "test-outbox".to_string(),
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

    fn task_spec(session: &str) -> TaskSpec {
        TaskSpec {
            schema: TASK_SPEC_SCHEMA.to_string(),
            session: session.to_string(),
            command: "cmd.exe".to_string(),
            args: vec!["/D".to_string(), "/C".to_string(), "exit 0".to_string()],
            cwd: None,
            env: Vec::new(),
            milestones_ms: Vec::new(),
            max_duration_ms: 60_000,
            callback: TaskCallback {
                inbox: "test-callback".to_string(),
                role: None,
            },
        }
    }

    fn session_record(pid: u32, started_at: Option<String>, job: Option<String>) -> SessionRecord {
        SessionRecord {
            id: "svc-test".to_string(),
            spec: session_spec(),
            pid,
            started_at,
            start_epoch_secs: None,
            job,
        }
    }

    fn task_record(
        session: &str,
        pid: u32,
        started_at: Option<String>,
        job: Option<String>,
    ) -> TaskRecord {
        TaskRecord {
            id: "task-test".to_string(),
            request_id: None,
            admission: TaskAdmissionPhase::Responded,
            spec: TaskSpec {
                session: session.to_string(),
                ..task_spec(session)
            },
            pid,
            started_at,
            start_epoch_secs: None,
            job,
            started_ms: Some(0),
            state: TaskState::Running,
            exit_code: None,
            elapsed_ms: None,
            stdout_path: "stdout.log".to_string(),
            stderr_path: "stderr.log".to_string(),
            delivered_milestones: 0,
        }
    }

    fn temp_control(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "baton-windows-service-unit-{}-{}-{tag}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create test control directory");
        path
    }

    fn terminal_running_task(
        control: &Path,
        id: &str,
        callback_inbox: &Path,
        clock: &FakeClock,
    ) -> RunningTask {
        let mut record = task_record(
            "svc-test",
            std::process::id(),
            recorded_start_identity(std::process::id()).0,
            None,
        );
        record.id = id.to_string();
        record.spec.callback.inbox = callback_inbox.display().to_string();
        record.started_ms = Some(clock.now_ms());
        record.state = TaskState::Completed;
        record.exit_code = Some(0);
        record.elapsed_ms = Some(10);
        write_task_record(control, &record).expect("write terminal task record");
        RunningTask {
            record,
            child: None,
            job: None,
            started_ms: clock.now_ms(),
            term_sent_at_ms: None,
            kill_sent: false,
            terminal_delivery_attempts: 0,
            next_terminal_retry_ms: None,
            terminal_retry_delay_ms: 0,
            milestone_delivery_attempts: 0,
            next_milestone_retry_ms: None,
            milestone_retry_delay_ms: 0,
            child_exit: None,
        }
    }

    #[test]
    fn tick_one_task_drops_when_durable_record_is_removed() {
        let control = temp_control("tick-record-removed");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut running =
            terminal_running_task(&control, "task-record-removed", &callback_inbox, &clock);
        remove_task_record(&control, "task-record-removed").expect("remove task record");

        assert!(matches!(
            tick_one_task(&control, "task-record-removed", &mut running, &clock)
                .expect("tick removed task"),
            TaskTick::Finished
        ));
    }

    #[test]
    fn tick_one_task_ignores_malformed_durable_record_at_start() {
        let control = temp_control("tick-record-malformed");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut running =
            terminal_running_task(&control, "task-record-malformed", &callback_inbox, &clock);
        let path = task_record_path(&control, "task-record-malformed").expect("task record path");
        fs::write(path, "not json").expect("write malformed task record");

        assert!(matches!(
            tick_one_task(&control, "task-record-malformed", &mut running, &clock)
                .expect("tick malformed task"),
            TaskTick::Finished
        ));
    }

    #[test]
    fn tick_one_task_reports_record_probe_io_error() {
        let control = temp_control("tick-record-probe-error");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut running =
            terminal_running_task(&control, "task-record-probe-error", &callback_inbox, &clock);
        fs::remove_dir_all(control.join("tasks")).expect("remove task record directory");
        fs::write(control.join("tasks"), "not a directory")
            .expect("replace tasks directory with a file");

        match tick_one_task(&control, "task-record-probe-error", &mut running, &clock) {
            Err(BatonError::Io(message)) => assert!(message.contains("could not probe")),
            other => panic!("expected record probe I/O error, got {other:?}"),
        }
    }

    #[test]
    fn terminal_delivery_uses_fake_clock_backoff_and_recovers() {
        let control = temp_control("terminal-delivery-retry");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
        let mut running =
            terminal_running_task(&control, "task-terminal-retry", &callback_inbox, &clock);

        match tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
            .expect("first terminal delivery attempt")
        {
            TaskTick::TerminalDeliveryRetry {
                attempt, delay_ms, ..
            } => {
                assert_eq!(attempt, 1);
                assert_eq!(delay_ms, EVENT_RETRY_INITIAL_DELAY_MS);
            }
            other => panic!("expected a scheduled retry, got {other:?}"),
        }
        assert_eq!(running.terminal_delivery_attempts, 1);
        assert_eq!(running.next_terminal_retry_ms, Some(1_000));

        clock.advance(EVENT_RETRY_INITIAL_DELAY_MS - 1);
        assert!(matches!(
            tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
                .expect("early terminal retry tick"),
            TaskTick::StillRunning
        ));
        fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");

        clock.advance(1);
        assert!(matches!(
            tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
                .expect("recovered terminal delivery"),
            TaskTick::Finished
        ));
        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open callback mailbox");
        assert_eq!(
            mailbox
                .claim_next()
                .expect("claim terminal event")
                .expect("terminal event present")
                .key,
            "task-terminal-retry-terminal"
        );
        drop(mailbox);
        let _ = fs::remove_dir_all(control);
    }

    #[test]
    fn terminal_delivery_drops_after_bounded_backoff_attempts() {
        let control = temp_control("terminal-delivery-drop");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
        let mut running =
            terminal_running_task(&control, "task-terminal-drop", &callback_inbox, &clock);
        let mut expected_delay = EVENT_RETRY_INITIAL_DELAY_MS;

        for attempt in 1..=MAX_EVENT_DELIVERY_ATTEMPTS {
            if attempt > 1 {
                clock.advance(running.terminal_retry_delay_ms);
            }
            let tick = tick_one_task(&control, "task-terminal-drop", &mut running, &clock)
                .expect("terminal delivery attempt");
            if attempt < MAX_EVENT_DELIVERY_ATTEMPTS {
                match tick {
                    TaskTick::TerminalDeliveryRetry {
                        attempt: reported_attempt,
                        delay_ms,
                        ..
                    } => {
                        assert_eq!(reported_attempt, attempt);
                        assert_eq!(delay_ms, expected_delay);
                        assert!(delay_ms <= EVENT_RETRY_MAX_DELAY_MS);
                        expected_delay = expected_delay
                            .saturating_mul(2)
                            .min(EVENT_RETRY_MAX_DELAY_MS);
                    }
                    other => panic!("expected another retry, got {other:?}"),
                }
            } else {
                assert!(matches!(
                    tick,
                    TaskTick::TerminalDeliveryDropped {
                        attempts: MAX_EVENT_DELIVERY_ATTEMPTS,
                        ..
                    }
                ));
            }
        }
        assert_eq!(
            running.terminal_delivery_attempts,
            MAX_EVENT_DELIVERY_ATTEMPTS
        );
        assert!(
            callback_inbox.is_file(),
            "failed callback remains unavailable"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A still-running task whose milestone delivery can be driven by the
    /// [`FakeClock`]. Its recorded identity is the live test process, so the
    /// tick's liveness check keeps it `Running` (never reaped) across ticks
    /// while milestone backoff is exercised — no child spawn needed.
    fn milestone_running_task(
        control: &Path,
        id: &str,
        callback_inbox: &Path,
        milestones_ms: Vec<u64>,
        clock: &FakeClock,
    ) -> RunningTask {
        let mut record = task_record(
            "svc-test",
            std::process::id(),
            recorded_start_identity(std::process::id()).0,
            None,
        );
        record.id = id.to_string();
        record.spec.callback.inbox = callback_inbox.display().to_string();
        record.spec.milestones_ms = milestones_ms;
        // Large enough that these tests never cross it: only milestone backoff
        // is under test, not max-duration termination (which would signal the
        // live test process's own group).
        record.spec.max_duration_ms = 3_600_000;
        record.started_ms = Some(clock.now_ms());
        record.state = TaskState::Running;
        write_task_record(control, &record).expect("write running task record");
        RunningTask {
            record,
            child: None,
            job: None,
            started_ms: clock.now_ms(),
            term_sent_at_ms: None,
            kill_sent: false,
            terminal_delivery_attempts: 0,
            next_terminal_retry_ms: None,
            terminal_retry_delay_ms: 0,
            milestone_delivery_attempts: 0,
            next_milestone_retry_ms: None,
            milestone_retry_delay_ms: 0,
            child_exit: None,
        }
    }

    /// Spawns `spec` under `control` inside a private Job Object and wraps it
    /// as a durably-recorded [`RunningTask`], mirroring what
    /// `handle_task_start_request` does — but callable directly, so a test can
    /// drive [`tick_one_task`] through a real reap without the request-file
    /// dance. Used where a live child is required (max-duration termination and
    /// reap), unlike [`milestone_running_task`] which never spawns.
    fn spawn_running_task(
        control: &Path,
        id: &str,
        spec: TaskSpec,
        clock: &FakeClock,
    ) -> RunningTask {
        let log_dir = task_logs_dir(control, id);
        fs::create_dir_all(&log_dir).expect("create log dir");
        let stdout_path = log_dir.join("stdout.log");
        let stderr_path = log_dir.join("stderr.log");
        let (child, job, job_name) =
            spawn_task_child(&spec, &stdout_path, &stderr_path).expect("spawn task");
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
            job: Some(job_name),
            started_ms: Some(started_ms),
            state: TaskState::Running,
            exit_code: None,
            elapsed_ms: None,
            stdout_path: stdout_path.display().to_string(),
            stderr_path: stderr_path.display().to_string(),
            delivered_milestones: 0,
        };
        write_task_record(control, &record).expect("write task record");
        RunningTask {
            record,
            child: Some(child),
            job: Some(job),
            started_ms,
            term_sent_at_ms: None,
            kill_sent: false,
            terminal_delivery_attempts: 0,
            next_terminal_retry_ms: None,
            terminal_retry_delay_ms: 0,
            milestone_delivery_attempts: 0,
            next_milestone_retry_ms: None,
            milestone_retry_delay_ms: 0,
            child_exit: None,
        }
    }

    /// Failed milestone callback delivery backs off from one second on the
    /// same schedule as terminal delivery, keeps the tick `StillRunning`
    /// (supervision never blocked by the stuck milestone), does not advance
    /// `delivered_milestones` while it fails, and delivers exactly once when
    /// the inbox recovers.
    #[test]
    fn milestone_delivery_uses_fake_clock_backoff_and_recovers() {
        let control = temp_control("milestone-delivery-retry");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
        let mut running =
            milestone_running_task(&control, "task-m-retry", &callback_inbox, vec![50], &clock);

        clock.advance(60);
        assert!(matches!(
            tick_one_task(&control, "task-m-retry", &mut running, &clock)
                .expect("first milestone delivery attempt"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.milestone_delivery_attempts, 1);
        assert_eq!(
            running.next_milestone_retry_ms,
            Some(60 + EVENT_RETRY_INITIAL_DELAY_MS)
        );
        assert_eq!(running.record.delivered_milestones, 0);

        clock.advance(EVENT_RETRY_INITIAL_DELAY_MS - 1);
        assert!(matches!(
            tick_one_task(&control, "task-m-retry", &mut running, &clock)
                .expect("early milestone retry tick"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.milestone_delivery_attempts, 1);
        assert_eq!(running.record.delivered_milestones, 0);

        fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");
        clock.advance(1);
        assert!(matches!(
            tick_one_task(&control, "task-m-retry", &mut running, &clock)
                .expect("recovered milestone delivery"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.record.delivered_milestones, 1);
        assert_eq!(running.milestone_delivery_attempts, 0);
        assert_eq!(running.next_milestone_retry_ms, None);

        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
        assert_eq!(
            mailbox
                .claim_next()
                .expect("claim")
                .expect("milestone event present")
                .key,
            "task-m-retry-milestone-0"
        );
        assert!(
            mailbox.claim_next().expect("claim").is_none(),
            "milestone delivered exactly once"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A milestone already delivered before a later milestone's delivery fails
    /// is never redelivered when the inbox recovers: the failure backs off the
    /// stuck index only, and `delivered_milestones` never regresses.
    #[test]
    fn milestone_batch_failure_does_not_redeliver_earlier_index() {
        let control = temp_control("milestone-batch-retry");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut running = milestone_running_task(
            &control,
            "task-m-batch",
            &callback_inbox,
            vec![10, 20],
            &clock,
        );

        clock.advance(15);
        assert!(matches!(
            tick_one_task(&control, "task-m-batch", &mut running, &clock)
                .expect("milestone 0 delivery"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.record.delivered_milestones, 1);
        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
        assert_eq!(
            mailbox
                .claim_next()
                .expect("claim")
                .expect("milestone 0 present")
                .key,
            "task-m-batch-milestone-0"
        );

        fs::remove_dir_all(&callback_inbox).expect("drop callback inbox");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");

        clock.advance(10);
        assert!(matches!(
            tick_one_task(&control, "task-m-batch", &mut running, &clock)
                .expect("milestone 1 delivery attempt"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.milestone_delivery_attempts, 1);
        assert_eq!(running.record.delivered_milestones, 1);

        fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");
        clock.advance(EVENT_RETRY_INITIAL_DELAY_MS);
        assert!(matches!(
            tick_one_task(&control, "task-m-batch", &mut running, &clock)
                .expect("recovered milestone 1 delivery"),
            TaskTick::StillRunning
        ));
        assert_eq!(running.record.delivered_milestones, 2);

        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
        assert_eq!(
            mailbox
                .claim_next()
                .expect("claim")
                .expect("milestone 1 present")
                .key,
            "task-m-batch-milestone-1",
            "recovery delivers the stuck index",
        );
        assert!(
            mailbox.claim_next().expect("claim").is_none(),
            "milestone 0 is not redelivered after recovery"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// Persistent milestone callback failure is retried at most the configured
    /// bound, then the milestone is dropped and the advance past it is
    /// persisted — so a supervisor restart's `rehydrate_tasks` does not
    /// re-enter the same stuck milestone.
    #[test]
    fn milestone_delivery_drops_after_bounded_backoff_and_persists_advance() {
        let control = temp_control("milestone-delivery-drop");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
        let mut running =
            milestone_running_task(&control, "task-m-drop", &callback_inbox, vec![50], &clock);

        clock.advance(60);
        for attempt in 1..=MAX_EVENT_DELIVERY_ATTEMPTS {
            if attempt > 1 {
                clock.advance(running.milestone_retry_delay_ms);
            }
            assert!(matches!(
                tick_one_task(&control, "task-m-drop", &mut running, &clock)
                    .expect("milestone delivery attempt"),
                TaskTick::StillRunning
            ));
            if attempt < MAX_EVENT_DELIVERY_ATTEMPTS {
                assert_eq!(running.milestone_delivery_attempts, attempt);
                assert_eq!(running.record.delivered_milestones, 0);
            }
        }

        assert_eq!(running.record.delivered_milestones, 1);
        assert_eq!(running.milestone_delivery_attempts, 0);
        assert_eq!(running.next_milestone_retry_ms, None);
        let durable = read_task_record(&control, "task-m-drop")
            .expect("read durable record")
            .expect("durable record present");
        assert_eq!(
            durable.delivered_milestones, 1,
            "dropped milestone advance is persisted so a restart does not re-enter it"
        );
        assert!(
            callback_inbox.is_file(),
            "failed callback remains unavailable"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A stuck milestone delivery must not block the tick's supervision: the
    /// max-duration escalation still terminates the Job Object and the child is
    /// still reaped to a terminal state while the milestone stays undelivered
    /// and backed off.
    #[test]
    fn supervision_continues_while_milestone_delivery_is_stuck() {
        let control = temp_control("milestone-stuck-supervision");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
        // A child that stays alive until the Job Object is terminated, so the
        // max-duration breach has something live to escalate against.
        let spec = TaskSpec {
            schema: TASK_SPEC_SCHEMA.to_string(),
            session: "svc-test".to_string(),
            command: "cmd.exe".to_string(),
            args: vec![
                "/D".to_string(),
                "/C".to_string(),
                "ping".to_string(),
                "-n".to_string(),
                "31".to_string(),
                "127.0.0.1".to_string(),
            ],
            cwd: None,
            env: Vec::new(),
            milestones_ms: vec![1],
            max_duration_ms: 100,
            callback: TaskCallback {
                inbox: callback_inbox.display().to_string(),
                role: None,
            },
        };
        let mut running = spawn_running_task(&control, "task-m-stuck", spec, &clock);

        // Past both the milestone threshold and the max duration: the milestone
        // delivery fails, but the tick still escalates Job Object termination.
        clock.advance(150);
        let tick =
            tick_one_task(&control, "task-m-stuck", &mut running, &clock).expect("stuck tick");
        assert!(
            running.term_sent_at_ms.is_some(),
            "max-duration breach must terminate even while a milestone is stuck"
        );
        assert_eq!(running.milestone_delivery_attempts, 1);
        assert_eq!(running.record.delivered_milestones, 0);

        // The terminated child is reaped to a terminal state even though the
        // callback inbox is still unavailable.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut tick = tick;
        while running.record.state == TaskState::Running {
            assert!(
                Instant::now() < deadline,
                "task was not reaped while its milestone delivery stayed stuck"
            );
            std::thread::sleep(Duration::from_millis(20));
            tick =
                tick_one_task(&control, "task-m-stuck", &mut running, &clock).expect("reap tick");
        }
        assert_eq!(running.record.state, TaskState::Timeout);
        assert!(
            matches!(tick, TaskTick::TerminalDeliveryRetry { .. }),
            "terminal delivery to the unavailable inbox follows the backoff policy"
        );
        assert_eq!(
            running.record.delivered_milestones, 0,
            "the milestone stayed undelivered throughout"
        );
        let _ = fs::remove_dir_all(control);
    }

    #[test]
    fn windows_liveness_ladder_is_fail_closed() {
        let start_key = recorded_start_identity(std::process::id())
            .0
            .expect("current test process has a Windows creation key");
        assert!(spawn_start_key_ok(&Some(start_key.clone()), &None));
        assert!(!spawn_start_key_ok(&None, &None));

        let job_name = fresh_job_name("liveness");
        let job = create_job(&job_name, false).expect("create liveness job");
        let mut session = session_record(
            std::process::id(),
            Some(start_key.clone()),
            Some(job_name.clone()),
        );
        assert_eq!(session_liveness(&session).0, Liveness::Live);

        session.started_at = Some("different-process".to_string());
        assert_eq!(session_liveness(&session).0, Liveness::Unresolved);

        session.started_at = Some(start_key.clone());
        session.job = Some(fresh_job_name("missing"));
        assert_eq!(session_liveness(&session).0, Liveness::Live);

        let mut task = task_record(
            "svc-test",
            std::process::id(),
            Some(start_key.clone()),
            Some(job_name.clone()),
        );
        assert_eq!(task_liveness(&task).0, Liveness::Live);
        task.started_at = Some("different-process".to_string());
        assert_eq!(task_liveness(&task).0, Liveness::Unresolved);
        task.started_at = Some(start_key);
        task.job = Some(fresh_job_name("missing-task"));
        assert_eq!(task_liveness(&task).0, Liveness::Live);

        assert_eq!(job_tree_liveness(Some(&job_name)), Some(Liveness::Dead));
        assert_eq!(
            job_tree_liveness(Some(&fresh_job_name("missing-tree"))),
            Some(Liveness::Unresolved)
        );

        let gone = session_record(
            1,
            Some("stale-process".to_string()),
            Some(fresh_job_name("missing-gone")),
        );
        assert_eq!(session_liveness(&gone).0, Liveness::Unresolved);
        drop(job);
    }

    #[test]
    fn corroborated_pid_fallback_terminates_when_job_name_is_gone() {
        let job_name = fresh_job_name("pid-fallback");
        let job = create_job(&job_name, false).expect("create non-inheritable job");
        let mut command = Command::new("cmd.exe");
        command.args(["/D", "/C", "ping -n 60 127.0.0.1 > NUL"]);
        command.stdin(Stdio::null());
        command.stdout(Stdio::null());
        command.stderr(Stdio::null());
        command.creation_flags(CREATE_SUSPENDED);
        let mut child = command.spawn().expect("spawn fallback task");
        assign_job_to_child(&job, &child).expect("assign fallback task");
        resume_initial_thread(child.id()).expect("resume fallback task");
        let pid = child.id();
        let started_at = recorded_start_identity(pid)
            .0
            .expect("fallback task has a creation key");
        drop(job);

        let record = task_record("svc-test", pid, Some(started_at), Some(job_name.clone()));
        assert_eq!(task_liveness(&record).0, Liveness::Live);
        terminate_record_job_or_pid(Some(&job_name), pid, "-TERM")
            .expect("terminate corroborated pid fallback");

        let deadline = Instant::now() + Duration::from_secs(5);
        while !recorded_pid_is_gone(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            recorded_pid_is_gone(pid),
            "PID fallback did not terminate task"
        );
        let _ = child.wait();
    }

    #[test]
    fn rehydrate_reopens_recorded_job_handles() {
        let control = temp_control("rehydrate");
        let session_job_name = fresh_job_name("rehydrate-session");
        let task_job_name = fresh_job_name("rehydrate-task");
        let session_job = create_job(&session_job_name, false).expect("create session job");
        let task_job = create_job(&task_job_name, true).expect("create task job");
        let start_key = recorded_start_identity(std::process::id())
            .0
            .expect("current test process has a Windows creation key");

        write_session_record(
            &control,
            &session_record(
                std::process::id(),
                Some(start_key.clone()),
                Some(session_job_name),
            ),
        )
        .expect("write session record");
        let sessions = rehydrate_sessions(&control).expect("rehydrate sessions");
        assert!(sessions["svc-test"].job.is_some());

        write_task_record(
            &control,
            &task_record(
                "svc-test",
                std::process::id(),
                Some(start_key),
                Some(task_job_name),
            ),
        )
        .expect("write task record");
        let clock = SystemClock;
        let tasks = rehydrate_tasks(&control, &clock).expect("rehydrate tasks");
        assert!(tasks["task-test"].job.is_some());

        drop(tasks);
        drop(sessions);
        drop(task_job);
        drop(session_job);
        let _ = fs::remove_dir_all(control);
    }

    #[test]
    fn unresolved_task_cleanup_retains_then_force_removes_record() {
        let control = temp_control("unresolved-cleanup");
        let record = task_record(
            "svc-test",
            1,
            Some("stale-process".to_string()),
            Some(fresh_job_name("missing-cleanup")),
        );
        write_task_record(&control, &record).expect("write unresolved task");

        let residue = reap_session_tasks_with_wait(&control, "svc-test", false, |_, _| {})
            .expect("retain unresolved task");
        assert_eq!(residue.len(), 1);
        assert_eq!(residue[0].liveness, Liveness::Unresolved);
        assert!(
            read_task_record(&control, &record.id)
                .expect("read retained task")
                .is_some()
        );

        let residue = reap_session_tasks_with_wait(&control, "svc-test", true, |_, _| {})
            .expect("force unresolved task cleanup");
        assert!(residue.is_empty());
        assert!(
            read_task_record(&control, &record.id)
                .expect("read removed task")
                .is_none()
        );
        let _ = fs::remove_dir_all(control);
    }
}
