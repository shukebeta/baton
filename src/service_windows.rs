#[cfg(test)]
use super::control::{
    ADMISSION_LOCK_FILE, AdmissionGuard, ControlLiveness, KILL_GRACE_MS, STOP_GRACE_MS,
    SessionStopGuard, SessionStopMarker, TASK_CONFIRM_READS, TASK_FULL_LISTINGS,
    TASK_NEW_ID_PARSES, acquire_control_lock, execute_teardown_with_timeout, probe_control,
    reap_session_tasks_with_wait, rehydrate_sessions, request_task_cancel_sentinel,
    rescan_owned_tasks, stop_session_record_with_wait, wait_for_control_release_with_timeout,
};
use super::control::{
    POLL_INTERVAL_MS, acquire_admission_lock, current_baton_exe, execute_status, execute_stop,
    execute_task_cancel, execute_task_status, execute_teardown, io_err, is_session_alive,
    run_service, serve_argv, submit_start_request, submit_task_start_request,
};
use super::records::{SessionRecord, read_task_record, write_task_record};
#[cfg(test)]
use super::records::{
    TaskStartResponse, list_session_records, list_task_records, mark_task_start_ack,
    read_session_record, remove_task_record, session_record_path, task_logs_dir, task_record_path,
    task_start_ack_exists, task_start_response_path, write_session_record,
};
use super::task_tick::{
    self, Liveness, ServicePlatform, TaskLivenessMode, TaskLivenessRefresh, TerminationSignal,
    liveness_sample_is_fresh, task_cancel_sentinel_path,
};
#[cfg(test)]
use super::task_tick::{
    DEFAULT_TASK_RETENTION_MS, REHYDRATED_LIVENESS_CACHE_MS, RunningTask as SharedRunningTask,
    finalize_task, tick_one_task,
};
use super::*;
#[cfg(test)]
use std::fs;
use std::fs::File;
use std::os::windows::io::AsRawHandle;
use std::os::windows::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
use crate::mailbox;
#[cfg(test)]
use crate::task::{Clock, SystemClock, TaskAdmissionPhase};
use crate::task::{TaskRecord, TaskSpec, TaskState};

#[cfg(test)]
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
/// closes only after `ActiveProcesses == 0`. `name` is the durable job name
/// a [`TaskRecord`] persists so a later `baton service run` can rehydrate
/// the handle via [`open_job`].
pub(super) struct JobHandle {
    handle: HANDLE,
    name: String,
}

impl JobHandle {
    fn raw(&self) -> HANDLE {
        self.handle
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        if self.handle != 0 {
            // SAFETY: This wrapper owns the handle and drops it exactly once.
            unsafe { CloseHandle(self.handle) };
        }
    }
}

static SERVICE_JOB: OnceLock<JobHandle> = OnceLock::new();

fn wide_null(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

pub(super) fn fresh_job_name(kind: &str) -> String {
    format!(
        r"Local\baton-{kind}-{}-{}-{}",
        std::process::id(),
        crate::events::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn create_job(name: &str, inheritable: bool) -> Result<JobHandle> {
    let name_owned = name.to_string();
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
    Ok(JobHandle {
        handle,
        name: name_owned,
    })
}

pub(super) fn open_job(name: &str) -> Result<Option<JobHandle>> {
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
        return Ok(Some(JobHandle {
            handle,
            name: name.to_string(),
        }));
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

pub(super) fn assign_job_to_child(job: &JobHandle, child: &Child) -> Result<()> {
    assign_job_to_process(job, child.as_raw_handle() as HANDLE)
}

pub(super) fn active_job_processes(job: &JobHandle) -> Result<u32> {
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

pub(super) fn terminate_job(job: &JobHandle) -> Result<()> {
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

/// Process-local sequence, making request/session ids unique even across
/// several calls within the same millisecond.
static SEQ: AtomicU64 = AtomicU64::new(0);

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

/// Bounds how often [`WindowsServicePlatform::task_liveness_for_tick`]
/// re-runs the Job Object/PID probe: one sample is reused for the rest of
/// the [`REHYDRATED_LIVENESS_CACHE_MS`] window unless a force refresh is
/// requested, mirroring Unix's rehydrated liveness cache.
#[derive(Default)]
struct WindowsTaskLivenessCache {
    sample: Option<(u64, Liveness, Instant)>,
}

#[cfg(test)]
thread_local! {
    static TASK_LIVENESS_PROBE_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_task_liveness_probe() {
    TASK_LIVENESS_PROBE_COUNT.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_task_liveness_probe_count() {
    TASK_LIVENESS_PROBE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn task_liveness_probe_count() -> u64 {
    TASK_LIVENESS_PROBE_COUNT.with(std::cell::Cell::get)
}

#[allow(dead_code)]
struct WindowsServicePlatform;

#[allow(dead_code)]
impl ServicePlatform for WindowsServicePlatform {
    type SessionHandle = WindowsProcessOwner;
    type TaskHandle = JobHandle;
    type TaskLivenessCache = WindowsTaskLivenessCache;

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

    fn task_handle_identity(handle: &Self::TaskHandle) -> Option<String> {
        Some(handle.name.clone())
    }

    fn abort_uncommitted_spawn(_pid: u32, handle: &Self::TaskHandle) -> Result<()> {
        terminate_job(handle)
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
        let _ = mode;
        let force_refresh = matches!(refresh, TaskLivenessRefresh::Forced);
        let refresh_sample = force_refresh
            || !cache
                .sample
                .map(|(checked_ms, _, checked_at)| {
                    liveness_sample_is_fresh(checked_ms, checked_at, now_ms)
                })
                .unwrap_or(false);
        if refresh_sample {
            #[cfg(test)]
            note_task_liveness_probe();
            let liveness = match process_probe(record.pid) {
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
            };
            cache.sample = Some((now_ms, liveness, Instant::now()));
        }
        cache.sample.expect("task liveness cache populated").1
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

    fn upgrade_legacy_task_record(control: &Path, record: &mut TaskRecord) -> Result<()> {
        upgrade_legacy_task_record(control, record)
    }

    fn escalate_task_to_death(record: &TaskRecord, grace_ms: u64) -> Liveness {
        let mut liveness = is_task_alive(record);
        if liveness == Liveness::Live {
            let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-TERM");
            wait_while_task_alive(record, grace_ms);
            liveness = cleanup_liveness_after_pid_signal(is_task_alive(record), record.pid);
            if liveness == Liveness::Live {
                let _ = terminate_record_job_or_pid(record.job.as_deref(), record.pid, "-KILL");
                wait_while_task_alive(record, grace_ms);
                liveness = cleanup_liveness_after_pid_signal(is_task_alive(record), record.pid);
            }
        }
        liveness
    }

    fn acquire_admission_lock(control: &Path) -> Result<File> {
        acquire_admission_lock(control)
    }

    fn persist_terminal_task(
        control: &Path,
        record: &mut TaskRecord,
        state: TaskState,
        exit_code: Option<i32>,
        elapsed_ms: u64,
    ) -> Result<bool> {
        let _admission = acquire_admission_lock(control)?;
        if read_task_record(control, &record.id)?.is_none() {
            return Ok(false);
        }
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
        ServiceCommand::Run {
            control,
            task_retention_ms,
        } => {
            let control = crate::roles::resolve_control_dir(control)?;
            let task_retention_ms =
                task_retention_ms.unwrap_or(task_tick::DEFAULT_TASK_RETENTION_MS);
            run_service::<WindowsServicePlatform>(&control, task_retention_ms, out)
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
            execute_task_status::<WindowsServicePlatform>(&control, task.as_deref(), out)
        }
        TaskCommand::Cancel { control, task } => {
            let control = crate::roles::resolve_control_dir(control)?;
            execute_task_cancel(&control, &task, out)
        }
    }
}

/// Spawns `baton serve` detached from this process's stdio. The child
/// adopts the named Job Object before it can create its participant.
pub(super) fn spawn_serve_child(spec: &SessionSpec, job_name: &str) -> Result<Child> {
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

pub(super) fn resume_initial_thread(pid: u32) -> Result<()> {
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

// -- Liveness ---------------------------------------------------------
#[cfg(windows)]
fn job_name_resolves(name: Option<&str>) -> bool {
    match name {
        None => true,
        Some(name) => matches!(open_job(name), Ok(Some(_))),
    }
}

#[cfg(windows)]
pub(super) fn record_job_available(name: Option<&str>) -> bool {
    name.is_some() && job_name_resolves(name)
}

/// Every call site invokes this only after the recorded PID has already
/// been corroborated gone (`ProbeResult::Gone`). `open_job`'s `Ok(None)`
/// means the name was confirmed not to resolve (ERROR_FILE_NOT_FOUND /
/// ERROR_PATH_NOT_FOUND): the whole tracked tree, including the last handle
/// that kept the object alive, has exited, so the object was destroyed —
/// that case is unambiguously `Dead`. `Err(_)` means the probe itself
/// failed (access denied, handle-quota exhaustion, ...); the object may
/// still exist with live descendants, so it stays fail-closed
/// `Unresolved` and retries, matching the rest of the liveness ladder.
#[cfg(windows)]
fn classify_job_tree(probe: Result<Option<JobHandle>>) -> Liveness {
    match probe {
        Ok(Some(job)) => match active_job_processes(&job) {
            Ok(0) => Liveness::Dead,
            Ok(_) => Liveness::Live,
            Err(_) => Liveness::Unresolved,
        },
        Ok(None) => Liveness::Dead,
        Err(_) => Liveness::Unresolved,
    }
}

#[cfg(windows)]
fn job_tree_liveness(name: Option<&str>) -> Option<Liveness> {
    let name = name?;
    Some(classify_job_tree(open_job(name)))
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

pub(super) fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
    match process_probe(pid) {
        ProbeResult::Present(start_key) => (Some(start_key), None),
        _ => (None, None),
    }
}

pub(super) fn spawn_start_key_ok(
    started_at: &Option<String>,
    _start_epoch_secs: &Option<i64>,
) -> bool {
    started_at.is_some()
}

pub(super) fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
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

pub(super) fn is_task_alive(record: &TaskRecord) -> Liveness {
    task_liveness(record).0
}

pub(super) fn upgrade_legacy_session_record(
    _control: &Path,
    _record: &mut SessionRecord,
) -> Result<()> {
    Ok(())
}

pub(super) fn upgrade_legacy_task_record(_control: &Path, _record: &mut TaskRecord) -> Result<()> {
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
pub(super) fn terminate_record_job_or_pid(
    job_name: Option<&str>,
    pid: u32,
    phase: &str,
) -> Result<()> {
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

pub(super) fn force_terminate_record_job(
    job_name: Option<&str>,
    pid: u32,
    phase: &str,
) -> Result<()> {
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
pub(super) fn cleanup_liveness_after_pid_signal(liveness: Liveness, pid: u32) -> Liveness {
    if liveness == Liveness::Unresolved && recorded_pid_is_gone(pid) {
        Liveness::Dead
    } else {
        liveness
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
pub(super) fn wait_while_alive(record: &SessionRecord, grace_ms: u64) {
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

pub(super) fn wait_while_task_alive(record: &TaskRecord, grace_ms: u64) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{FakeClock, TASK_SPEC_SCHEMA, TaskCallback};
    use crate::test_support::serialize_forks_and_locks;
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
            terminal_delivered_at_ms: None,
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

    /// A single corrupt session record is skipped with a warning; the
    /// remaining healthy records are still returned. Mirrors the Unix-side
    /// `list_session_records_skips_malformed_record_and_warns`.
    #[test]
    fn list_session_records_skips_malformed_record_and_warns() {
        let control = temp_control("list-session-malformed");
        for i in 0..2 {
            let mut record = session_record(1000 + i, None, None);
            record.id = format!("svc-{i}");
            write_session_record(&control, &record).expect("write");
        }
        let path = session_record_path(&control, "svc-bad").expect("session record path");
        fs::write(path, "not json").expect("write malformed session record");
        let path = session_record_path(&control, "svc-non-utf8").expect("session record path");
        fs::write(path, b"\xff\xfe not utf8").expect("write non-UTF-8 session record");

        let mut ids: Vec<String> = list_session_records(&control)
            .expect("list")
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["svc-0", "svc-1"]);
    }

    /// A single corrupt task record is skipped with a warning; the
    /// remaining healthy records are still returned. Mirrors the Unix-side
    /// `list_task_records_skips_malformed_record_and_warns`.
    #[test]
    fn list_task_records_skips_malformed_record_and_warns() {
        let control = temp_control("list-task-malformed");
        for i in 0..2 {
            let mut record = task_record("svc-1", 1000 + i, None, None);
            record.id = format!("task-{i}");
            write_task_record(&control, &record).expect("write");
        }
        let path = task_record_path(&control, "task-bad").expect("task record path");
        fs::write(path, "not json").expect("write malformed task record");
        let path = task_record_path(&control, "task-non-utf8").expect("task record path");
        fs::write(path, b"\xff\xfe not utf8").expect("write non-UTF-8 task record");

        let mut ids: Vec<String> = list_task_records(&control)
            .expect("list")
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["task-0", "task-1"]);
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
        let _guard = serialize_forks_and_locks();
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

    /// A non-force `service teardown` across several sessions with only
    /// terminal task history performs exactly one full `tasks/` listing
    /// for the whole run — not one per session — and confirms nothing
    /// directly.
    #[test]
    fn execute_teardown_with_timeout_with_terminal_only_history_performs_one_full_listing_and_no_confirm_reads()
     {
        let _guard = serialize_forks_and_locks();
        TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
        TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        let control = temp_control("teardown-terminal-only-history");
        for id in ["svc-1", "svc-2", "svc-3"] {
            write_session_record(
                &control,
                &SessionRecord {
                    id: id.to_string(),
                    spec: session_spec(),
                    pid: u32::MAX - 1,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                },
            )
            .expect("write session");
        }
        let terminal = |id: &str, session: &str| TaskRecord {
            id: id.to_string(),
            request_id: None,
            admission: TaskAdmissionPhase::Committed,
            spec: task_spec(session),
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
            terminal_delivered_at_ms: None,
        };
        write_task_record(&control, &terminal("task-1a", "svc-1")).expect("write task-1a");
        write_task_record(&control, &terminal("task-1b", "svc-1")).expect("write task-1b");
        write_task_record(&control, &terminal("task-2a", "svc-2")).expect("write task-2a");
        write_task_record(&control, &terminal("task-3a", "svc-3")).expect("write task-3a");

        let mut out = Vec::new();
        let mut warning = Vec::new();
        execute_teardown_with_timeout(
            &control,
            false,
            &mut out,
            Duration::from_millis(20),
            &mut warning,
        )
        .expect("teardown");

        assert_eq!(
            TASK_FULL_LISTINGS.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one full tasks/ listing for the entire multi-session teardown"
        );
        assert_eq!(
            TASK_CONFIRM_READS.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "every entry is trusted directly as cached-terminal"
        );
        for id in ["svc-1", "svc-2", "svc-3"] {
            assert!(
                read_session_record(&control, id).expect("read").is_none(),
                "{id}'s session record is removed"
            );
        }
        for id in ["task-1a", "task-1b", "task-2a", "task-3a"] {
            assert!(
                read_task_record(&control, id).expect("read").is_none(),
                "{id}'s task record is removed"
            );
        }
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
    fn finalize_task_does_not_resurrect_removed_record() {
        let control = temp_control("finalize-removed-task");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut running =
            terminal_running_task(&control, "task-finalize-removed", &callback_inbox, &clock);
        remove_task_record(&control, "task-finalize-removed").expect("remove task record");

        assert!(matches!(
            finalize_task(&control, &mut running, TaskState::Failed, None, 10, &clock)
                .expect("finalize removed task"),
            TaskTick::Finished
        ));
        assert!(
            read_task_record(&control, "task-finalize-removed")
                .expect("read removed task")
                .is_none(),
            "finalizing an externally removed task does not resurrect its record"
        );
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
            TaskTick::StillRunning
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
        assert!(
            matches!(
                tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
                    .expect("recovered terminal delivery"),
                TaskTick::StillRunning
            ),
            "delivery succeeds but the record is retained until it ages out"
        );
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

        // Re-ticking before retention elapses neither redelivers nor reaps.
        clock.advance(running.retention_ms - 1);
        assert!(matches!(
            tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
                .expect("terminal tick within retention window"),
            TaskTick::StillRunning
        ));
        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("reopen callback mailbox");
        assert!(
            mailbox
                .claim_next()
                .expect("check for redelivered terminal event")
                .is_none(),
            "a retained terminal record is not redelivered"
        );
        drop(mailbox);

        // Once retention elapses the record is reaped.
        clock.advance(1);
        assert!(matches!(
            tick_one_task(&control, "task-terminal-retry", &mut running, &clock)
                .expect("terminal tick at retention boundary"),
            TaskTick::Finished
        ));
        assert!(
            read_task_record(&control, "task-terminal-retry")
                .expect("read reaped task")
                .is_none(),
            "the retained record is removed once retention elapses"
        );
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
            terminal_delivered_at_ms: None,
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
            Some(Liveness::Dead)
        );

        // A transient probe failure (access denied, handle-quota exhaustion,
        // ...) is not "object destroyed": it must stay fail-closed
        // `Unresolved` and retry, not finalize the tree as `Dead`.
        assert_eq!(
            classify_job_tree(Err(BatonError::Io("probe failed".to_string()))),
            Liveness::Unresolved
        );

        // A gone PID whose Job Object name no longer resolves finalizes as
        // `Dead`: every call site already corroborated the PID gone, so a
        // destroyed/renamed Job Object cannot mean the process is still
        // running. Only a still-live PID with mismatched identity (PID
        // reuse) or an unreadable probe stays fail-closed `Unresolved`.
        let gone = session_record(
            1,
            Some("stale-process".to_string()),
            Some(fresh_job_name("missing-gone")),
        );
        assert_eq!(session_liveness(&gone).0, Liveness::Dead);
        drop(job);
    }

    /// A rehydrated/draining task's liveness now stays inside the same
    /// 500ms TTL cache the Unix rehydrated path already had, instead of
    /// recomputing the Job Object/PID probe on every 100ms tick.
    #[test]
    fn rehydrated_task_liveness_is_rate_limited() {
        let control = temp_control("rehydrated-liveness-cache");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
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
            milestones_ms: Vec::new(),
            max_duration_ms: 3_600_000,
            callback: TaskCallback {
                inbox: callback_inbox.display().to_string(),
                role: None,
            },
        };
        let mut running = spawn_running_task(&control, "task-rehydrated-cache", spec, &clock);
        let mut child = running.child.take().expect("rehydrated task child");

        reset_task_liveness_probe_count();
        assert!(matches!(
            tick_one_task(&control, "task-rehydrated-cache", &mut running, &clock),
            Ok(TaskTick::StillRunning)
        ));
        assert_eq!(
            task_liveness_probe_count(),
            1,
            "the first rehydrated tick samples liveness once"
        );

        clock.advance(100);
        tick_one_task(&control, "task-rehydrated-cache", &mut running, &clock)
            .expect("cached liveness tick");
        assert_eq!(
            task_liveness_probe_count(),
            1,
            "rehydrated liveness remains cached inside the 500ms window"
        );

        clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 101);
        tick_one_task(&control, "task-rehydrated-cache", &mut running, &clock)
            .expect("cached liveness tick before refresh");
        assert_eq!(
            task_liveness_probe_count(),
            1,
            "rehydrated liveness remains cached up to the exact boundary"
        );

        clock.advance(1);
        tick_one_task(&control, "task-rehydrated-cache", &mut running, &clock)
            .expect("refresh liveness tick");
        assert_eq!(
            task_liveness_probe_count(),
            2,
            "the cache refreshes once the 500ms TTL elapses"
        );

        if let Some(job) = running.task_handle.take() {
            let _ = terminate_job(&job);
        }
        let _ = child.kill();
        let _ = child.wait();
        let _ = fs::remove_dir_all(control);
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
        let tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            DEFAULT_TASK_RETENTION_MS,
            None,
        )
        .expect("rehydrate tasks");
        assert!(tasks["task-test"].task_handle.is_some());

        drop(tasks);
        drop(sessions);
        drop(task_job);
        drop(session_job);
        let _ = fs::remove_dir_all(control);
    }

    /// A terminal record already delivered before a restart is rehydrated
    /// with its `terminal_delivered_at_ms` intact, is never redelivered, and
    /// is reaped exactly once retention elapses.
    #[test]
    fn rehydrated_delivered_task_reaps_at_retention_boundary_without_redelivery() {
        let control = temp_control("rehydrate-retention");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut record = task_record("svc-test", std::process::id(), None, None);
        record.id = "task-delivered".to_string();
        record.spec.callback.inbox = callback_inbox.display().to_string();
        record.state = TaskState::Completed;
        record.exit_code = Some(0);
        record.elapsed_ms = Some(10);
        record.terminal_delivered_at_ms = Some(clock.now_ms());
        write_task_record(&control, &record).expect("write delivered terminal record");

        let retention_ms = 1_000;
        let mut tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            retention_ms,
            None,
        )
        .expect("rehydrate tasks");
        let mut running = tasks.remove("task-delivered").expect("rehydrated task");

        clock.advance(retention_ms - 1);
        assert!(matches!(
            tick_one_task(&control, "task-delivered", &mut running, &clock)
                .expect("tick within retention window"),
            TaskTick::StillRunning
        ));
        assert!(
            !callback_inbox.exists(),
            "an already-delivered record is never redelivered after a restart"
        );
        assert!(
            read_task_record(&control, "task-delivered")
                .expect("read retained task")
                .is_some(),
            "the record survives while retention has not yet elapsed"
        );

        clock.advance(1);
        assert!(matches!(
            tick_one_task(&control, "task-delivered", &mut running, &clock)
                .expect("tick at retention boundary"),
            TaskTick::Finished
        ));
        assert!(
            read_task_record(&control, "task-delivered")
                .expect("read reaped task")
                .is_none(),
            "the record is reaped exactly once retention elapses"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A terminal record whose delivery had not yet succeeded before a
    /// restart still redelivers on the next tick, and persists
    /// `terminal_delivered_at_ms` so a later restart does not redeliver
    /// again.
    #[test]
    fn rehydrated_undelivered_terminal_task_still_redelivers() {
        let control = temp_control("rehydrate-undelivered");
        let clock = FakeClock::new();
        let callback_inbox = control.join("callback");
        let mut record = task_record("svc-test", std::process::id(), None, None);
        record.id = "task-undelivered".to_string();
        record.spec.callback.inbox = callback_inbox.display().to_string();
        record.state = TaskState::Completed;
        record.exit_code = Some(0);
        record.elapsed_ms = Some(10);
        write_task_record(&control, &record).expect("write undelivered terminal record");

        let mut tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            DEFAULT_TASK_RETENTION_MS,
            None,
        )
        .expect("rehydrate tasks");
        let mut running = tasks.remove("task-undelivered").expect("rehydrated task");

        assert!(matches!(
            tick_one_task(&control, "task-undelivered", &mut running, &clock)
                .expect("redeliver after restart"),
            TaskTick::StillRunning
        ));
        let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open callback mailbox");
        assert_eq!(
            mailbox
                .claim_next()
                .expect("claim terminal event")
                .expect("terminal event present")
                .key,
            "task-undelivered-terminal"
        );
        drop(mailbox);
        let record = read_task_record(&control, "task-undelivered")
            .expect("read redelivered task")
            .expect("record still present");
        assert!(
            record.terminal_delivered_at_ms.is_some(),
            "delivery is persisted so a later restart does not redeliver again"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A missing (already reaped, or never written) task record answers
    /// `task status` with an empty task list, exactly like any other
    /// unknown id.
    #[test]
    fn task_status_reports_nothing_for_a_reaped_task() {
        let control = temp_control("status-reaped");
        let mut out = Vec::new();
        execute_task_status::<WindowsServicePlatform>(&control, Some("task-gone"), &mut out)
            .expect("status for missing task");
        let json: serde_json::Value = serde_json::from_slice(&out).expect("json");
        assert_eq!(json["tasks"].as_array().unwrap().len(), 0);
        let _ = fs::remove_dir_all(control);
    }

    /// When `reconcile_task_admissions` reports no mutation, boot reuses its
    /// returned snapshot instead of walking `tasks/` a second time: proven by
    /// replacing `tasks/` with a plain file afterward and showing
    /// `rehydrate_tasks` still succeeds when given the reused snapshot, but
    /// fails (as a control) when forced to re-list.
    #[test]
    fn rehydrate_reuses_reconciled_records_without_a_second_directory_walk() {
        let control = temp_control("boot-reuse-records");
        let record = task_record("svc-test", std::process::id(), None, None);
        write_task_record(&control, &record).expect("write task record");

        let (records, mutated) =
            admission::reconcile_task_admissions::<WindowsServicePlatform>(&control)
                .expect("reconcile clean boot");
        assert!(!mutated, "a clean tasks/ directory reports no mutation");

        fs::remove_dir_all(control.join("tasks")).expect("remove tasks directory");
        fs::write(control.join("tasks"), "not a directory")
            .expect("replace tasks directory with a file");

        let clock = SystemClock;
        let reused = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            DEFAULT_TASK_RETENTION_MS,
            Some(records),
        )
        .expect("rehydrate reuses the reconciled snapshot without re-listing tasks/");
        assert_eq!(reused.len(), 1);

        let relisted = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            DEFAULT_TASK_RETENTION_MS,
            None,
        );
        assert!(
            relisted.is_err(),
            "control: without the reused snapshot, rehydrate_tasks re-lists tasks/ and fails \
             against the broken directory"
        );

        let _ = fs::remove_file(control.join("tasks"));
        let _ = fs::remove_dir_all(control);
    }

    /// A mutating reconciliation pass (here, aborting a `Prepared`
    /// admission) is never reused: boot re-lists `tasks/` and observes the
    /// removal, rather than trusting the stale pre-reconciliation snapshot.
    #[test]
    fn rehydrate_relists_when_reconciliation_mutated_tasks() {
        let control = temp_control("boot-reuse-mutated");
        let mut prepared = task_record("svc-test", std::process::id(), None, None);
        prepared.id = "task-prepared".to_string();
        prepared.admission = TaskAdmissionPhase::Prepared;
        prepared.state = TaskState::Completed;
        write_task_record(&control, &prepared).expect("write prepared task record");

        let mut settled = task_record("svc-test", std::process::id(), None, None);
        settled.id = "task-settled".to_string();
        write_task_record(&control, &settled).expect("write settled task record");

        let (records, mutated) =
            admission::reconcile_task_admissions::<WindowsServicePlatform>(&control)
                .expect("reconcile mutated boot");
        assert!(mutated, "aborting the prepared admission is a mutation");
        assert_eq!(
            records.len(),
            2,
            "the pre-reconciliation snapshot still lists both records"
        );

        let clock = SystemClock;
        let tasks = task_tick::rehydrate_tasks::<WindowsServicePlatform>(
            &control,
            &clock,
            DEFAULT_TASK_RETENTION_MS,
            if mutated { None } else { Some(records) },
        )
        .expect("rehydrate re-lists after a mutation");
        assert_eq!(
            tasks.len(),
            1,
            "re-listing reflects the prepared record's removal; reusing the stale snapshot would not"
        );
        assert!(tasks.contains_key("task-settled"));

        let _ = fs::remove_dir_all(control);
    }

    /// A non-force `service stop` on a session with only terminal task
    /// history performs exactly one full `tasks/` listing and confirms
    /// nothing directly.
    #[test]
    fn execute_stop_with_terminal_only_history_performs_one_full_listing_and_no_confirm_reads() {
        let _guard = serialize_forks_and_locks();
        TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
        TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        let control = temp_control("stop-terminal-only-history");
        let session_record = SessionRecord {
            id: "svc-1".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_record).expect("write session");

        let terminal = |id: &str, session: &str| TaskRecord {
            id: id.to_string(),
            request_id: None,
            admission: TaskAdmissionPhase::Committed,
            spec: task_spec(session),
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
            terminal_delivered_at_ms: None,
        };
        write_task_record(&control, &terminal("task-owned-1", "svc-1")).expect("write owned-1");
        write_task_record(&control, &terminal("task-owned-2", "svc-1")).expect("write owned-2");
        write_task_record(&control, &terminal("task-other", "svc-2")).expect("write other");

        let mut out = Vec::new();
        execute_stop(&control, "svc-1", false, &mut out).expect("stop svc-1");

        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("stopped session svc-1")
        );
        assert_eq!(
            TASK_FULL_LISTINGS.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "exactly one full tasks/ listing"
        );
        assert_eq!(
            TASK_CONFIRM_READS.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a terminal cache entry is trusted directly, with no confirm read"
        );
        assert!(
            read_session_record(&control, "svc-1")
                .expect("read")
                .is_none(),
            "svc-1's session record is removed"
        );
        assert!(
            read_task_record(&control, "task-owned-1")
                .expect("read")
                .is_none(),
            "svc-1's first owned task record is removed"
        );
        assert!(
            read_task_record(&control, "task-owned-2")
                .expect("read")
                .is_none(),
            "svc-1's second owned task record is removed"
        );
        assert!(
            read_task_record(&control, "task-other")
                .expect("read")
                .is_some(),
            "an unrelated session's task record is untouched"
        );
        let _ = fs::remove_dir_all(control);
    }

    /// A gone-PID record with an unresolvable Job Object name is no longer
    /// this ladder's `Unresolved` case (see `job_tree_liveness`) — it now
    /// finalizes as `Dead` on the first non-force pass. The genuinely
    /// fail-closed case that must still retain-then-force is a live,
    /// identity-mismatched PID (standing in for PID reuse), so this test
    /// spawns a real child and records a mismatched `started_at` for it.
    #[test]
    fn unresolved_task_cleanup_retains_then_force_removes_record() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("unresolved-cleanup");
        let (mut child, job, job_name) = spawn_live_task_child(&control, "task-test");
        let mut record = live_task_record("svc-test", "task-test", child.id(), Some(job_name));
        record.started_at = Some("mismatched-identity".to_string());
        write_task_record(&control, &record).expect("write unresolved task");

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let (residue, _handled) =
            reap_session_tasks_with_wait(&mut admission, "svc-test", false, |_, _| {})
                .expect("retain unresolved task");
        assert_eq!(residue.len(), 1);
        assert_eq!(residue[0].liveness, Liveness::Unresolved);
        assert!(
            read_task_record(&control, &record.id)
                .expect("read retained task")
                .is_some()
        );

        let (residue, _handled) =
            reap_session_tasks_with_wait(&mut admission, "svc-test", true, |_, _| {})
                .expect("force unresolved task cleanup");
        assert!(residue.is_empty());
        assert!(
            read_task_record(&control, &record.id)
                .expect("read removed task")
                .is_none()
        );
        let _ = child.wait();
        drop(job);
        let _ = fs::remove_dir_all(control);
    }

    fn live_task_spec(session: &str) -> TaskSpec {
        let mut spec = task_spec(session);
        spec.command = "cmd.exe".to_string();
        spec.args = vec![
            "/D".to_string(),
            "/C".to_string(),
            "ping -n 60 127.0.0.1 > NUL".to_string(),
        ];
        spec
    }

    /// Spawns a real, long-lived Windows process standing in for a live
    /// task/session record, so `is_task_alive`/`is_session_alive` resolve
    /// it `Live` via a corroborated start identity and Job Object.
    fn spawn_live_task_child(control: &Path, task_id: &str) -> (Child, JobHandle, String) {
        let log_dir = task_logs_dir(control, task_id);
        fs::create_dir_all(&log_dir).expect("create task log dir");
        spawn_task_child(
            &live_task_spec("svc-1"),
            &log_dir.join("stdout.log"),
            &log_dir.join("stderr.log"),
        )
        .expect("spawn live task child")
    }

    /// A `Running` record pinned to `pid`'s corroborated start identity and
    /// `job`, so `is_task_alive` reports it `Live`.
    fn live_task_record(session: &str, task_id: &str, pid: u32, job: Option<String>) -> TaskRecord {
        let started_at = recorded_start_identity(pid).0;
        TaskRecord {
            id: task_id.to_string(),
            request_id: None,
            admission: TaskAdmissionPhase::Committed,
            spec: live_task_spec(session),
            pid,
            started_at,
            start_epoch_secs: None,
            job,
            started_ms: None,
            state: TaskState::Running,
            exit_code: None,
            elapsed_ms: None,
            stdout_path: String::new(),
            stderr_path: String::new(),
            delivered_milestones: 0,
            terminal_delivered_at_ms: None,
        }
    }

    /// Before #236, a non-force stop held the admission lock for its whole
    /// `STOP_GRACE_MS` wait, freezing the supervisor's task admission and
    /// every task-start client for up to the sum of the grace windows.
    #[test]
    fn stop_grace_does_not_hold_the_admission_lock() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("stop-grace-unlocked");
        let inbox = control.join("inbox");
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

        let log_dir = control.join("grace-session-logs");
        fs::create_dir_all(&log_dir).expect("create log dir");
        let (mut child, job, job_name) = spawn_task_child(
            &live_task_spec("svc-grace"),
            &log_dir.join("stdout.log"),
            &log_dir.join("stderr.log"),
        )
        .expect("spawn session stand-in");
        let started_at = recorded_start_identity(child.id()).0;
        let session_record = SessionRecord {
            id: "svc-grace".to_string(),
            spec: SessionSpec {
                inbox: inbox.display().to_string(),
                outbox: control.join("outbox").display().to_string(),
                ..session_spec()
            },
            pid: child.id(),
            started_at,
            start_epoch_secs: None,
            job: Some(job_name),
        };
        assert_eq!(
            is_session_alive(&session_record),
            Liveness::Live,
            "fixture session is live, so the stop enters its grace window"
        );
        write_session_record(&control, &session_record).expect("write session record");

        let stop_control = control.clone();
        let stopper = std::thread::spawn(move || {
            let mut out = Vec::new();
            execute_stop(&stop_control, "svc-grace", false, &mut out)
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

        // Non-blocking on purpose: a blocking acquire would simply wait the
        // grace out and pass against the old implementation too.
        let probe = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(control.join(ADMISSION_LOCK_FILE))
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
            read_session_record(&control, "svc-grace")
                .expect("read")
                .is_none(),
            "the session is still stopped and its record removed"
        );
        let _ = child.wait();
        drop(job);
        drop(mailbox_lock);
        let _ = fs::remove_dir_all(control);
    }

    /// A task start admitted while a grace wait had the admission lock
    /// released lands outside the reaper's snapshot. The final locked
    /// rescan reports a still-live racer as residue instead of dropping it,
    /// and the session record is retained.
    #[test]
    fn rescan_reports_a_task_admitted_during_a_released_grace_wait() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("rescan-live-racer");
        let session_record = SessionRecord {
            id: "svc-1".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_record).expect("write session");

        // A live record the reaper does see, so it reaches its grace wait —
        // the point at which the racing admission lands. Terminating its job
        // ends this one, so it leaves no residue of its own.
        let (mut reaped_child, reaped_job, reaped_job_name) =
            spawn_live_task_child(&control, "task-reaped");
        let reaped = live_task_record(
            "svc-1",
            "task-reaped",
            reaped_child.id(),
            Some(reaped_job_name),
        );
        write_task_record(&control, &reaped).expect("write reaped task");

        let (mut child, racer_job, racer_job_name) = spawn_live_task_child(&control, "task-racer");
        let racer = live_task_record(
            "svc-1",
            "task-racer",
            child.id(),
            Some(racer_job_name.clone()),
        );

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let residue = stop_session_record_with_wait(
            &mut admission,
            &session_record,
            false,
            |_, _| {},
            // Stands in for a task start admitted while this wait had the
            // admission lock released. `TerminateJobObject` is asynchronous,
            // so poll the terminated fixture task rather than trusting a
            // single reprobe right after the write.
            |record, grace_ms| {
                write_task_record(&control, &racer).expect("admit racing task");
                let deadline = Instant::now() + Duration::from_millis(grace_ms);
                while is_task_alive(record) != Liveness::Dead && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            },
        )
        .expect("stop session");
        drop(admission);

        assert!(
            residue.iter().any(|entry| entry.id == "task-racer"),
            "the racing task is reported, not silently dropped: {residue:?}"
        );
        assert!(
            read_task_record(&control, "task-racer")
                .expect("read")
                .is_some(),
            "the racing task record is retained for a later cleanup attempt"
        );
        assert!(
            read_session_record(&control, "svc-1")
                .expect("read")
                .is_some(),
            "the session record is not removed while an owned task is outstanding"
        );

        let _ = force_terminate_record_job(Some(&racer_job_name), child.id(), "-KILL");
        let _ = child.wait();
        drop(racer_job);
        let _ = reaped_child.wait();
        drop(reaped_job);
        let _ = fs::remove_dir_all(control);
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
    /// rather than simulated. Mirrors the Unix-side
    /// `task_start_is_rejected_while_a_stop_owns_the_session`.
    #[test]
    fn task_start_is_rejected_while_a_stop_owns_the_session() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("admit-stopping");
        let inbox = control.join("inbox");
        fs::create_dir_all(&inbox).expect("create inbox");

        // A real child stands in for the session, so admission's own
        // liveness check keeps saying `Live` right up until the stop's
        // escalation ladder ends it.
        let (mut child, job, job_name) = spawn_live_task_child(&control, "svc-stopping");
        let started_at = recorded_start_identity(child.id()).0;
        let session_record = SessionRecord {
            id: "svc-stopping".to_string(),
            spec: SessionSpec {
                inbox: inbox.display().to_string(),
                outbox: control.join("outbox").display().to_string(),
                ..session_spec()
            },
            pid: child.id(),
            started_at,
            start_epoch_secs: None,
            job: Some(job_name),
        };
        assert_eq!(
            is_session_alive(&session_record),
            Liveness::Live,
            "fixture session is live before the stop begins"
        );
        write_session_record(&control, &session_record).expect("write session");

        let spec_path = control.join("racing-spec.json");
        fs::write(
            &spec_path,
            serde_json::to_string(&task_spec("svc-stopping")).expect("serialize spec"),
        )
        .expect("write spec");

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let racing_outcome = std::cell::RefCell::new(None);
        let _ = stop_session_record_with_wait(
            &mut admission,
            &session_record,
            false,
            |_, _| {
                // Only the first grace window: later escalation rounds
                // run after termination, when the owner is no longer live
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
                    admission::handle_task_start_request::<WindowsServicePlatform>(
                        &control,
                        "racing-request",
                        &spec_path,
                        &SystemClock,
                    )
                    .expect("handle racing start"),
                );
            },
            |_, _| {},
        )
        .expect("stop session");
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
                task_start_response_path(&control, "racing-request").expect("response path"),
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
            !admission::session_stop_in_progress::<WindowsServicePlatform>(
                &control,
                "svc-stopping"
            )
            .expect("probe"),
            "the finished stop released its marker"
        );
        let _ = child.wait();
        drop(job);
        let _ = fs::remove_dir_all(control);
    }

    /// The stop marker is released when the stop finishes, and a marker
    /// orphaned by a killed `service stop` cannot wedge admission: a
    /// reader whose recorded identity no longer resolves discards it.
    /// Mirrors the Unix-side `a_stale_session_stop_marker_does_not_wedge_admission`.
    #[test]
    fn a_stale_session_stop_marker_does_not_wedge_admission() {
        let control = temp_control("stale-stop-marker");

        {
            let _claim = SessionStopGuard::claim(&control, "svc-1").expect("claim");
            assert!(
                admission::session_stop_in_progress::<WindowsServicePlatform>(&control, "svc-1")
                    .expect("probe"),
                "a live stop owns the session"
            );
        }
        assert!(
            !admission::session_stop_in_progress::<WindowsServicePlatform>(&control, "svc-1")
                .expect("probe"),
            "finishing the stop releases the marker"
        );

        let dir_path = admission::session_stop_markers_dir(&control);
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
            !admission::session_stop_in_progress::<WindowsServicePlatform>(&control, "svc-1")
                .expect("probe"),
            "a marker whose owner no longer resolves is stale"
        );
        assert!(
            !admission::session_stop_marker_path(&control, "svc-1")
                .expect("marker path")
                .exists(),
            "and is cleared, so it costs at most one rejected start"
        );

        mailbox::atomic_write(&dir_path, &mailbox::file_name("svc-1"), "not json")
            .expect("write malformed marker");
        assert!(
            !admission::session_stop_in_progress::<WindowsServicePlatform>(&control, "svc-1")
                .expect("probe"),
            "a malformed marker is not evidence of a live stop"
        );
    }

    /// The supervisor can tick a racing task to a terminal state before the
    /// rescan looks. Such a record gets the same cleanup the reaper applies
    /// to any terminal record — and, with nothing else outstanding, the
    /// session record is still removed, so the rescan does not block the
    /// success path.
    #[test]
    fn rescan_cleans_up_a_terminal_task_admitted_during_a_released_grace_wait() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("rescan-terminal-racer");
        let session_record = SessionRecord {
            id: "svc-1".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_record).expect("write session");

        // A live record the reaper does see, so it reaches its grace wait —
        // the point at which the racing admission lands. Terminating its job
        // ends this one, so it leaves no residue of its own.
        let (mut reaped_child, reaped_job, reaped_job_name) =
            spawn_live_task_child(&control, "task-reaped");
        let reaped = live_task_record(
            "svc-1",
            "task-reaped",
            reaped_child.id(),
            Some(reaped_job_name),
        );
        write_task_record(&control, &reaped).expect("write reaped task");

        let request_id = "racer-request";
        let mut racer = live_task_record("svc-1", "task-racer", u32::MAX - 1, None);
        racer.request_id = Some(request_id.to_string());
        racer.started_at = None;
        racer.state = TaskState::Completed;
        racer.exit_code = Some(0);
        racer.elapsed_ms = Some(1);

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let residue = stop_session_record_with_wait(
            &mut admission,
            &session_record,
            false,
            |_, _| {},
            // `TerminateJobObject` is asynchronous, so poll the terminated
            // fixture task rather than trusting a single reprobe right
            // after the writes.
            |record, grace_ms| {
                write_task_record(&control, &racer).expect("admit racing task");
                mark_task_start_ack(&control, request_id).expect("write acknowledgement");
                request_task_cancel_sentinel(&control, &racer.id).expect("write sentinel");
                let deadline = Instant::now() + Duration::from_millis(grace_ms);
                while is_task_alive(record) != Liveness::Dead && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            },
        )
        .expect("stop session");
        drop(admission);

        assert!(residue.is_empty(), "nothing is outstanding: {residue:?}");
        assert!(
            read_task_record(&control, "task-racer")
                .expect("read")
                .is_none(),
            "the terminal racing record is removed"
        );
        assert!(
            !task_start_ack_exists(&control, request_id).expect("probe acknowledgement"),
            "its start transaction is removed too"
        );
        assert!(
            !task_cancel_sentinel_path(&control, "task-racer").exists(),
            "its cancel sentinel is removed too"
        );
        assert!(
            read_session_record(&control, "svc-1")
                .expect("read")
                .is_none(),
            "the rescan does not block the success path"
        );
        let _ = reaped_child.wait();
        drop(reaped_job);
        let _ = fs::remove_dir_all(control);
    }

    /// A task admitted for a session not yet reached in a teardown loop —
    /// or one already finished — is discovered the moment `unlocked_wait`
    /// releases the lock, via its embedded id-only refresh, not by a
    /// second full listing. One shared `AdmissionGuard`, mirroring how
    /// `execute_teardown_with_timeout` shares one guard across its session
    /// loop, proves the delta lands during svc-j's own wait.
    #[test]
    fn unlocked_wait_discovers_a_task_admitted_for_a_later_session() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("unlocked-wait-later-session");
        TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
        TASK_NEW_ID_PARSES.store(0, std::sync::atomic::Ordering::Relaxed);

        let session_j = SessionRecord {
            id: "svc-j".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_j).expect("write svc-j");
        let session_k = SessionRecord {
            id: "svc-k".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_k).expect("write svc-k");

        // A live task the reaper does see, so svc-j's reap reaches its
        // task-level grace wait — the point at which the racing admission
        // for svc-k lands. Terminating its job ends this one, so it leaves
        // no residue of its own.
        let (mut task_j_child, task_j_job, task_j_job_name) =
            spawn_live_task_child(&control, "task-j");
        let task_j = live_task_record("svc-j", "task-j", task_j_child.id(), Some(task_j_job_name));
        write_task_record(&control, &task_j).expect("write task-j");

        let mut racer = live_task_record("svc-k", "task-k-racer", u32::MAX - 1, None);
        racer.started_at = None;
        racer.state = TaskState::Completed;
        racer.exit_code = Some(0);
        racer.elapsed_ms = Some(1);

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let residue_j = stop_session_record_with_wait(
            &mut admission,
            &session_j,
            false,
            |_, _| {},
            // Stands in for a task admitted for a session this teardown
            // loop has not reached yet. `TerminateJobObject` is
            // asynchronous, so poll the terminated fixture task rather
            // than trusting a single reprobe right after the write.
            |record, grace_ms| {
                write_task_record(&control, &racer).expect("admit racing task for svc-k");
                let deadline = Instant::now() + Duration::from_millis(grace_ms);
                while is_task_alive(record) != Liveness::Dead && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            },
        )
        .expect("stop svc-j");

        assert!(
            residue_j.is_empty(),
            "svc-j has nothing outstanding: {residue_j:?}"
        );
        assert_eq!(
            TASK_FULL_LISTINGS.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one full listing seeds the whole guard's cache"
        );
        assert_eq!(
            TASK_NEW_ID_PARSES.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the racer is discovered by unlocked_wait's embedded refresh during svc-j's own wait"
        );

        let residue_k =
            stop_session_record_with_wait(&mut admission, &session_k, false, |_, _| {}, |_, _| {})
                .expect("stop svc-k");
        drop(admission);

        assert!(
            residue_k.is_empty(),
            "svc-k has nothing outstanding: {residue_k:?}"
        );
        assert!(
            read_task_record(&control, "task-k-racer")
                .expect("read")
                .is_none(),
            "the cached-terminal racer is removed when svc-k is stopped"
        );
        assert!(
            read_session_record(&control, "svc-j")
                .expect("read")
                .is_none(),
            "svc-j's session record is removed"
        );
        assert!(
            read_session_record(&control, "svc-k")
                .expect("read")
                .is_none(),
            "svc-k's session record is removed"
        );
        let _ = task_j_child.wait();
        drop(task_j_job);
        let _ = fs::remove_dir_all(control);
    }

    /// The daemon's own supervisor tick can persist a terminal state for a
    /// task while this guard's cached copy still says `Running` and a
    /// grace wait is in flight. The post-wait recheck must see that write
    /// and stop the escalation ladder before ever reaching a second
    /// (`-KILL`) round — continuing to act on the stale `Running` copy
    /// would risk targeting a pid/job Windows has already begun tearing
    /// down on its own asynchronous schedule. Unlike the Unix side,
    /// Windows has no signal-ignoring disposition to fake a stubborn
    /// process with, so `TASK_CONFIRM_READS == 2` (entry confirmation plus
    /// exactly one post-wait recheck) is the decisive proof here: a second
    /// round would add a third.
    #[test]
    fn wait_then_recheck_terminal_stops_the_ladder_before_the_kill_round_when_the_supervisor_wins_the_race()
     {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("wait-then-recheck-terminal");
        TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        let session_record = SessionRecord {
            id: "svc-1".to_string(),
            spec: session_spec(),
            pid: u32::MAX - 1,
            started_at: None,
            start_epoch_secs: None,
            job: None,
        };
        write_session_record(&control, &session_record).expect("write session");

        let (mut child, job, job_name) = spawn_live_task_child(&control, "task-stubborn");
        let record = live_task_record("svc-1", "task-stubborn", child.id(), Some(job_name.clone()));
        write_task_record(&control, &record).expect("write stubborn task");

        let invocations = std::sync::atomic::AtomicUsize::new(0);
        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let residue = stop_session_record_with_wait(
            &mut admission,
            &session_record,
            false,
            |_, _| {},
            |wait_record, _| {
                if invocations.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    // Test seam simulating `task_tick::finalize_task`
                    // winning the race: not a faithful reproduction of the
                    // real supervisor's own exit-detection logic, which
                    // only persists a terminal state after observing the
                    // process's owned boundary dead. It exists solely to
                    // prove the confirm-then-decide discipline governs
                    // regardless of how the terminal record came to be,
                    // and regardless of whether `TerminateJobObject`'s own
                    // asynchronous kill has finished yet.
                    let mut terminal = wait_record.clone();
                    terminal.state = TaskState::Completed;
                    terminal.exit_code = Some(0);
                    terminal.elapsed_ms = Some(1);
                    write_task_record(&control, &terminal).expect("supervisor wins the race");
                }
            },
        )
        .expect("stop session");
        drop(admission);

        assert!(
            invocations.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the wait callback ran"
        );
        assert!(
            residue.is_empty(),
            "the racing termination leaves nothing outstanding: {residue:?}"
        );
        assert!(
            read_task_record(&control, "task-stubborn")
                .expect("read")
                .is_none(),
            "the terminal record is removed"
        );
        assert_eq!(
            TASK_CONFIRM_READS.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "one entry confirmation plus one post-wait recheck — the ladder never reaches a second (-KILL) round"
        );

        let _ = force_terminate_record_job(Some(&job_name), child.id(), "-KILL");
        let _ = child.wait();
        drop(job);
        let _ = fs::remove_dir_all(control);
    }

    /// `force=true` skips every wait, so a racing admission can never land
    /// through the `task_wait`/`session_wait` callbacks the non-force rescan
    /// tests above use. `rescan_owned_tasks` is exercised directly instead,
    /// mirroring `unresolved_task_cleanup_retains_then_force_removes_record`'s
    /// direct-call style: a live task record present when the rescan runs
    /// must be force-terminated and its record removed, not merely reported
    /// as residue.
    #[test]
    fn rescan_force_terminates_and_removes_a_racing_task_record() {
        let _guard = serialize_forks_and_locks();
        let control = temp_control("rescan-force-racer");
        let (mut child, job, job_name) = spawn_live_task_child(&control, "task-racer");
        let racer = live_task_record("svc-1", "task-racer", child.id(), Some(job_name));
        write_task_record(&control, &racer).expect("write racing task");

        let mut admission = AdmissionGuard::acquire(&control).expect("admission lock");
        let mut residue = Vec::new();
        let handled = std::collections::HashSet::new();
        rescan_owned_tasks(&mut admission, "svc-1", true, &handled, &mut residue)
            .expect("force rescan_owned_tasks");
        drop(admission);

        assert!(
            residue.is_empty(),
            "rescan_owned_tasks' force branch removes the racer instead of reporting it: {residue:?}"
        );
        assert!(
            read_task_record(&control, "task-racer")
                .expect("read")
                .is_none(),
            "rescan_owned_tasks' force branch removes the racing task's record"
        );

        // `TerminateJobObject` is asynchronous, so poll rather than trusting
        // a single reprobe right after the call returns.
        let deadline = Instant::now() + Duration::from_millis(KILL_GRACE_MS);
        let mut liveness = is_task_alive(&racer);
        while liveness != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
            liveness = is_task_alive(&racer);
        }
        assert_eq!(
            liveness,
            Liveness::Dead,
            "rescan_owned_tasks' force branch terminates the racing task's process"
        );

        let _ = child.wait();
        drop(job);
        let _ = fs::remove_dir_all(control);
    }
}
