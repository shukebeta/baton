#[cfg(test)]
use super::records::task_record_path;
use super::records::{
    AwaitConfig, SessionRecord, StartResponse, TEST_TASK_ROLLBACK_RECONCILE_BARRIER,
    TEST_TASK_ROLLBACK_REQUEST_BARRIER, TaskStartResponse, discard_pending_task_start_request,
    fresh_request_id, fresh_session_id, fresh_task_id, list_session_records, list_task_records,
    list_task_start_acks, list_task_start_response_claims, list_task_start_rollbacks,
    mark_task_start_rollback, read_session_record, read_task_record, reclaim_stale_requests,
    remove_session_record, remove_task_record, remove_task_start_ack,
    remove_task_start_response_files, remove_task_start_rollback, remove_task_start_transaction,
    responses_dir, restore_task_start_response_claim, start_channel,
    take_task_start_response_locked, task_cancel_dir, task_channel, task_logs_dir,
    task_start_ack_exists, task_start_response_boundary_exists, task_start_response_claim_path,
    task_start_response_id, task_start_response_path, task_start_rollback_exists,
    wait_for_test_task_admission_barrier, wait_for_test_task_response_phase_barrier,
    wait_for_test_task_rollback_cleanup_barrier, write_session_record, write_start_response,
    write_task_record, write_task_start_response,
};
#[cfg(test)]
use super::task_tick::tick_one_task;
use super::task_tick::{
    self, Liveness, RunningTask as SharedRunningTask, ServicePlatform, TaskLivenessMode,
    TaskLivenessRefresh, TerminationSignal, task_cancel_sentinel_path,
};
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
use crate::task::{
    Clock, SystemClock, TaskAdmissionPhase, TaskRecord, TaskSpec, TaskState,
    first_non_ascending_milestone,
};

type RunningTask = SharedRunningTask<WindowsServicePlatform>;
#[cfg(test)]
type TaskTick = task_tick::TaskTick;
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
#[cfg(test)]
const EVENT_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
/// Longest delay between task-event callback delivery attempts.
#[cfg(test)]
const EVENT_RETRY_MAX_DELAY_MS: u64 = 60_000;
/// Total callback delivery attempts for a single task event before it is
/// dropped (a terminal event drops the tracker entry; a milestone is
/// skipped so supervision continues).
#[cfg(test)]
const MAX_EVENT_DELIVERY_ATTEMPTS: u32 = 10;
/// Bound on how long `Start` waits for a live `Run` to answer.
const START_AWAIT_MS: u64 = 10_000;
/// Bound on the cooperative `serve --stop` grace before escalating to
/// `TerminateJobObject`.
const STOP_GRACE_MS: u64 = 5_000;
/// Bound on the second `TerminateJobObject` attempt's grace.
const KILL_GRACE_MS: u64 = 2_000;
/// Bound on how long teardown waits for `Run` to release the control
/// lock before continuing with record cleanup.
const CONTROL_RELEASE_TIMEOUT_MS: u64 = 10_000;

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

/// Whether a live `Run` holds the control lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlLiveness {
    Live,
    NotRunning,
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

/// Job Object ownership returned by the future platform spawn seam. The
/// durable name remains paired with the handle because a restarted service
/// reopens the name while an in-process tracker uses the handle directly.
#[allow(dead_code)]
struct WindowsProcessOwner {
    job: JobHandle,
    name: String,
}

#[allow(dead_code)]
struct WindowsServicePlatform;

#[allow(dead_code)]
impl ServicePlatform for WindowsServicePlatform {
    type SessionHandle = WindowsProcessOwner;
    type TaskHandle = JobHandle;
    // Windows currently recomputes its Job Object/PID observation each tick;
    // the shared API still carries cache state so a later migration can make
    // that policy explicit without changing the consumer shape.
    type TaskLivenessCache = ();

    fn spawn_session(
        spec: &SessionSpec,
        stderr_path: &Path,
    ) -> Result<(Child, Self::SessionHandle)> {
        let job_name = fresh_job_name("session");
        let job = create_job(&job_name, false)?;
        let mut child = match spawn_serve_child(spec, &job_name) {
            Ok(child) => child,
            Err(err) => {
                drop(job);
                return Err(err);
            }
        };
        let assign =
            assign_job_to_child(&job, &child).and_then(|_| resume_initial_thread(child.id()));
        if let Err(err) = assign {
            let _ = terminate_job(&job);
            let _ = child.wait();
            return Err(err);
        }
        let _ = stderr_path;
        Ok((
            child,
            WindowsProcessOwner {
                job,
                name: job_name,
            },
        ))
    }

    fn spawn_task(
        spec: &TaskSpec,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(Child, Self::TaskHandle)> {
        let (child, job, _name) = spawn_task_child(spec, stdout_path, stderr_path)?;
        Ok((child, job))
    }

    fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
        recorded_start_identity(pid)
    }

    fn start_identity_is_valid(
        started_at: &Option<String>,
        start_epoch_secs: &Option<i64>,
    ) -> bool {
        spawn_start_key_ok(started_at, start_epoch_secs)
    }

    fn session_liveness(record: &SessionRecord) -> Liveness {
        is_session_alive(record)
    }

    fn task_liveness(record: &TaskRecord) -> Liveness {
        is_task_alive(record)
    }

    fn task_liveness_for_tick(
        record: &TaskRecord,
        owner: Option<&Self::TaskHandle>,
        mode: TaskLivenessMode,
        cache: &mut Self::TaskLivenessCache,
        now_ms: u64,
        refresh: TaskLivenessRefresh,
    ) -> Liveness {
        // Windows has no Unix process-group scan cache today. Keep the
        // ownership, clock, and refresh inputs in the seam even though this
        // implementation intentionally follows its current recomputed path.
        let _ = (mode, cache, now_ms, refresh);
        match process_probe(record.pid) {
            ProbeResult::Gone => match owner {
                Some(job) => match active_job_processes(job) {
                    Ok(0) => Liveness::Dead,
                    Ok(_) => Liveness::Live,
                    Err(_) => Liveness::Unresolved,
                },
                None => job_tree_liveness(record.job.as_deref()).unwrap_or(Liveness::Dead),
            },
            ProbeResult::Unreadable => Liveness::Unresolved,
            ProbeResult::Present(current) => match &record.started_at {
                Some(expected) if expected == &current => Liveness::Live,
                Some(_) => Liveness::Unresolved,
                None => Liveness::Unresolved,
            },
        }
    }

    fn terminate_session(
        record: &SessionRecord,
        signal: TerminationSignal,
        force: bool,
    ) -> Result<()> {
        if force {
            force_terminate_record_job(record.job.as_deref(), record.pid, signal.phase())
        } else {
            terminate_record_job_or_pid(record.job.as_deref(), record.pid, signal.phase())
        }
    }

    fn terminate_task(record: &TaskRecord, signal: TerminationSignal, force: bool) -> Result<()> {
        if force {
            force_terminate_record_job(record.job.as_deref(), record.pid, signal.phase())
        } else {
            terminate_record_job_or_pid(record.job.as_deref(), record.pid, signal.phase())
        }
    }

    fn terminate_owned_task(
        owner: Option<&Self::TaskHandle>,
        record: &TaskRecord,
        signal: TerminationSignal,
        force: bool,
    ) -> Result<()> {
        match owner {
            Some(owner) => terminate_job(owner),
            None if force => {
                force_terminate_record_job(record.job.as_deref(), record.pid, signal.phase())
            }
            None => terminate_record_job_or_pid(record.job.as_deref(), record.pid, signal.phase()),
        }
    }

    fn pid_is_gone(pid: u32) -> bool {
        recorded_pid_is_gone(pid)
    }

    fn unresolved_task_is_gone(
        control: &Path,
        id: &str,
        record: &TaskRecord,
        term_sent_at_ms: Option<u64>,
    ) -> Result<bool> {
        let controlled =
            term_sent_at_ms.is_some() || task_cancel_sentinel_path(control, id).is_file();
        Ok(controlled && Self::pid_is_gone(record.pid))
    }

    fn rehydrate_task(record: &TaskRecord) -> Result<Option<Self::TaskHandle>> {
        record
            .job
            .as_deref()
            .map(open_job)
            .transpose()
            .map(|job| job.flatten())
    }

    fn persist_terminal_task(
        control: &Path,
        record: &mut TaskRecord,
        state: TaskState,
        exit_code: Option<i32>,
        elapsed_ms: u64,
    ) -> Result<bool> {
        record.state = state;
        record.exit_code = exit_code;
        record.elapsed_ms = Some(elapsed_ms);
        write_task_record(control, record)?;
        Ok(true)
    }

    fn keep_child_handle_while_draining() -> bool {
        false
    }
}

/// Dispatches one parsed [`ServiceCommand`].
pub(super) fn dispatch(cmd: ServiceCommand, mut out: impl Write) -> Result<()> {
    match cmd {
        ServiceCommand::Run { control } => {
            let control = crate::roles::resolve_control_dir(control)?;
            run_service(&control, out)
        }
        ServiceCommand::Start { control, spec } => {
            let control = crate::roles::resolve_control_dir(control)?;
            let session_id = submit_start_request(&control, &spec)?;
            writeln!(out, "{session_id}").map_err(io_err)
        }
        ServiceCommand::Status { control, session } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_status(&control, session.as_deref(), out)
        }
        ServiceCommand::Stop {
            control,
            session,
            force,
        } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_stop(&control, &session, force, out)
        }
        ServiceCommand::Teardown { control, force } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_teardown(&control, force, out)
        }
    }
}

/// Dispatches one parsed [`TaskCommand`].
pub(super) fn dispatch_task(cmd: TaskCommand, mut out: impl Write) -> Result<()> {
    match cmd {
        TaskCommand::Start { control, spec } => {
            let control = crate::roles::resolve_control_dir(control)?;
            let task_id = submit_task_start_request(&control, &spec)?;
            writeln!(out, "{task_id}").map_err(io_err)
        }
        TaskCommand::Status { control, task } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_task_status(&control, task.as_deref(), out)
        }
        TaskCommand::Cancel { control, task } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_task_cancel(&control, &task, out)
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
    let mut tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(control, &clock)?;
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
        task_tick::tick_tasks::<WindowsServicePlatform>(control, &mut tasks, &clock);

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

/// Submits `spec` to a live `Run` and awaits its session id.
///
/// Fails fast (before writing anything) when no `Run` holds the control
/// lock, rather than waiting out the full await bound against a service
/// that was never started.
fn submit_start_request(control: &Path, spec: &SessionSpec) -> Result<String> {
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

fn await_start_response(control: &Path, request_id: &str) -> Result<String> {
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
fn process_one_request(control: &Path) -> Result<Option<(String, RunningSession)>> {
    start_channel(control).process_one(|request_id, claimed_path| {
        let outcome = handle_start_request(control, request_id, claimed_path)?;
        let Some((record, running)) = outcome else {
            return Ok(None);
        };
        Ok(Some((record.id, running)))
    })
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

/// Answers a claimed start request with an admission failure the supervisor
/// can name, so the client fails immediately with the real reason instead of
/// waiting out [`START_AWAIT_MS`]. Only the response write itself can still
/// fail the request loop.
fn reject_start_request(
    control: &Path,
    request_id: &str,
    error: String,
) -> Result<Option<(SessionRecord, RunningSession)>> {
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

/// Submits `spec` through the shared request channel and awaits its task id.
/// The task response claim and rollback transaction remain specific to this
/// caller.
fn submit_task_start_request(control: &Path, spec: &TaskSpec) -> Result<String> {
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
fn take_task_start_response(control: &Path, request_id: &str) -> Result<Option<TaskStartResponse>> {
    if !task_start_response_boundary_exists(control, request_id)? {
        return Ok(None);
    }
    let _admission = acquire_admission_lock(control)?;
    take_task_start_response_locked(control, request_id)
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
/// `task-requests/` through the shared request channel.
fn reclaim_stale_task_requests(control: &Path) -> Result<()> {
    task_channel(control).reclaim_stale()
}

/// Claims the next task-start request through the shared request channel, then
/// applies task-specific admission and lifecycle handling.
fn process_one_task_request(
    control: &Path,
    clock: &dyn Clock,
) -> Result<Option<(String, RunningTask)>> {
    task_channel(control).process_one(|request_id, claimed_path| {
        // The lock is intentionally acquired after the request is claimed but
        // before owner validation and spawn. If session cleanup wins the race,
        // validation observes the removed/dead owner; if admission wins,
        // cleanup waits and reaps the newly recorded task.
        let outcome = acquire_admission_lock(control).and_then(|_admission| {
            if task_start_rollback_exists(control, request_id)? {
                discard_pending_task_start_request(control, request_id)?;
                wait_for_test_task_rollback_cleanup_barrier(TEST_TASK_ROLLBACK_REQUEST_BARRIER);
                remove_task_start_rollback(control, request_id)?;
                return Ok(None);
            }
            handle_task_start_request(control, request_id, claimed_path, clock)
        });
        let Some((record, child, job, started_ms)) = outcome? else {
            return Ok(None);
        };
        let id = record.id.clone();
        let running = RunningTask::new(record, Some(child), Some(job), started_ms);
        Ok(Some((id, running)))
    })
}

/// Answers a claimed task-start request with an admission failure the
/// supervisor can name, mirroring [`reject_start_request`].
fn reject_task_start_request(
    control: &Path,
    request_id: &str,
    error: String,
) -> Result<Option<(TaskRecord, Child, JobHandle, u64)>> {
    task_channel(control).reject(
        request_id,
        &TaskStartResponse {
            task_id: None,
            error: Some(error),
        },
        "task start response",
    )
}

/// Validates the requested owner, then spawns the task, persists its
/// [`TaskRecord`], and answers the request with its task id. It shares the
/// session handler's admission failure discipline while retaining the
/// task-only transaction phases.
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

// -- Task records -------------------------------------------------------

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

fn request_task_cancel_sentinel(control: &Path, task_id: &str) -> Result<()> {
    let dir = task_cancel_dir(control);
    fs::create_dir_all(&dir)
        .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
    mailbox::atomic_write(&dir, &mailbox::file_name(task_id), "")
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

fn execute_teardown(control: &Path, force: bool, out: impl Write) -> Result<()> {
    let mut stderr = std::io::stderr();
    execute_teardown_with_timeout(
        control,
        force,
        out,
        Duration::from_millis(CONTROL_RELEASE_TIMEOUT_MS),
        &mut stderr,
    )
}

fn execute_teardown_with_timeout(
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

/// Waits for the supervisor's control lock with a bounded deadline. A
/// timeout is deliberately non-fatal: the admission lock is independent
/// from `service.lock`, so teardown can still drain the durable records.
fn wait_for_control_release_with_timeout(
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

    #[test]
    fn control_release_wait_timeout_warns_and_returns() {
        let control = temp_control("control-release-timeout");
        let held = acquire_control_lock(&control).expect("lock");
        let mut warning = Vec::new();

        wait_for_control_release_with_timeout(&control, Duration::from_millis(20), &mut warning)
            .expect("bounded control-release wait");

        let warning = String::from_utf8(warning).expect("warning text");
        assert!(warning.contains(&control.display().to_string()));
        assert!(warning.contains("service.lock"));
        assert_eq!(
            probe_control(&control).expect("probe after timeout"),
            ControlLiveness::Live,
            "timeout leaves the held control lock untouched"
        );
        drop(held);
        let _ = fs::remove_dir_all(control);
    }

    #[test]
    fn teardown_continues_after_control_release_timeout() {
        let control = temp_control("teardown-control-release-timeout");
        let held = acquire_control_lock(&control).expect("lock");
        let record = SessionRecord {
            id: "svc-timeout".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &record).expect("write stale session");
        let mut out = Vec::new();
        let mut warning = Vec::new();

        execute_teardown_with_timeout(
            &control,
            false,
            &mut out,
            Duration::from_millis(20),
            &mut warning,
        )
        .expect("teardown after control-release timeout");

        let warning = String::from_utf8(warning).expect("warning text");
        assert!(warning.contains(&control.display().to_string()));
        assert!(warning.contains("service.lock"));
        assert!(
            String::from_utf8(out)
                .expect("teardown output")
                .contains("requested teardown of baton service")
        );
        assert!(
            read_session_record(&control, "svc-timeout")
                .expect("read session after teardown")
                .is_none(),
            "teardown reached durable-record cleanup after the warning"
        );
        drop(held);
        let _ = fs::remove_dir_all(control);
    }

    #[test]
    fn control_release_wait_returns_without_warning_when_released() {
        let control = temp_control("control-release-responsive");
        let held = acquire_control_lock(&control).expect("lock");
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(held);
        });
        let mut warning = Vec::new();

        wait_for_control_release_with_timeout(&control, Duration::from_millis(200), &mut warning)
            .expect("responsive control-release wait");
        releaser.join().expect("release control lock");

        assert!(warning.is_empty(), "normal release emitted a warning");
        let _ = fs::remove_dir_all(control);
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
        RunningTask::new(record, None, None, clock.now_ms())
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
        RunningTask::new(record, None, None, clock.now_ms())
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
        RunningTask::new(record, Some(child), Some(job), started_ms)
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
        let tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(&control, &clock)
            .expect("rehydrate tasks");
        assert!(tasks["task-test"].task_handle.is_some());

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
