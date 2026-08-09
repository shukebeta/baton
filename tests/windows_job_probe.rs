#![cfg(windows)]

use std::ffi::c_void;
use std::io::Read;
use std::mem::size_of;
use std::os::windows::io::AsRawHandle;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{BOOL, CloseHandle, FALSE, HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JobObjectBasicAccountingInformation, OpenJobObjectW, QueryInformationJobObject,
    TerminateJobObject,
};
use windows_sys::Win32::System::SystemServices::{JOB_OBJECT_QUERY, JOB_OBJECT_TERMINATE};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

const PROBE_TIMEOUT_MS: u32 = 5_000;

fn helper() -> &'static str {
    env!("CARGO_BIN_EXE_windows_job_probe_child")
}

fn unique_name(label: &str) -> Vec<u16> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    format!(
        "Local\\baton-job-probe-{label}-{}-{nanos}",
        std::process::id()
    )
    .encode_utf16()
    .chain(std::iter::once(0))
    .collect()
}

fn spawn_child(args: &[&str], capture_stdout: bool) -> Child {
    let mut command = Command::new(helper());
    command.args(args);
    command.stdin(Stdio::null());
    if capture_stdout {
        command.stdout(Stdio::piped());
    } else {
        command.stdout(Stdio::null());
    }
    command.stderr(Stdio::null());
    command.spawn().expect("spawn probe child")
}

fn process_handle(child: &Child) -> HANDLE {
    child.as_raw_handle() as HANDLE
}

fn create_job(name: Option<&[u16]>, inheritable: bool) -> HANDLE {
    let mut attrs = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: if inheritable { 1 } else { 0 } as BOOL,
    };
    let attrs_ptr = if inheritable {
        &mut attrs
    } else {
        std::ptr::null_mut()
    };
    let name_ptr = name.map_or(std::ptr::null(), |value| value.as_ptr());
    // SAFETY: The optional name is a null-terminated UTF-16 buffer alive for
    // this call, and the security attributes point to a fully initialized
    // structure when inheritance is requested.
    let handle = unsafe { CreateJobObjectW(attrs_ptr, name_ptr) };
    assert_ne!(handle, 0, "CreateJobObjectW failed");
    handle
}

fn assign(job: HANDLE, child: &Child) {
    // SAFETY: `child` owns a live process handle for the duration of this call;
    // the job handle was returned by CreateJobObjectW and remains open.
    let ok = unsafe { AssignProcessToJobObject(job, process_handle(child)) };
    assert_ne!(ok, 0, "AssignProcessToJobObject failed");
}

fn close(handle: HANDLE) {
    if handle != 0 {
        // SAFETY: Every handle passed here was returned by a Win32 create/open
        // call and is closed at most once by this probe.
        unsafe { CloseHandle(handle) };
    }
}

fn child_reached_by_termination(pid: u32) -> bool {
    // SAFETY: The requested rights are sufficient for waiting and querying the
    // process identified by the PID emitted by the probe child.
    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            FALSE,
            pid,
        )
    };
    if handle == 0 {
        return true;
    }
    // SAFETY: `handle` is a live process handle returned by OpenProcess.
    let wait = unsafe { WaitForSingleObject(handle, PROBE_TIMEOUT_MS) };
    close(handle);
    wait == WAIT_OBJECT_0
}

fn probe_named_job_reopen() -> bool {
    let name = unique_name("reopen");
    let job = create_job(Some(&name), false);
    let mut child = spawn_child(&["--sleep"], false);
    assign(job, &child);
    close(job);

    // SAFETY: `name` is a live null-terminated UTF-16 name. The query and
    // terminate rights are the only rights needed for this measurement.
    let reopened = unsafe {
        OpenJobObjectW(
            JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE,
            FALSE,
            name.as_ptr(),
        )
    };
    let resolved = reopened != 0;
    if resolved {
        // SAFETY: The reopened handle names the job that owns `child`.
        unsafe { TerminateJobObject(reopened, 1) };
        close(reopened);
    } else {
        let _ = child.kill();
    }
    let _ = child.wait();
    resolved
}

fn probe_grandchild_termination() -> bool {
    let job = create_job(None, false);
    let mut parent = spawn_child(&["--spawn-grandchild"], true);
    assign(job, &parent);
    let mut stdout = String::new();
    parent
        .stdout
        .take()
        .expect("probe stdout")
        .read_to_string(&mut stdout)
        .expect("read grandchild pid");
    let _ = parent.wait();
    let grandchild_pid = stdout.trim().parse::<u32>().expect("grandchild pid");

    // SAFETY: The job handle is still open and owns both the exited parent and
    // its surviving grandchild.
    let terminated = unsafe { TerminateJobObject(job, 1) } != 0;
    let reached = terminated && child_reached_by_termination(grandchild_pid);
    close(job);
    reached
}

fn probe_std_inherits_job_handle() -> bool {
    let job = create_job(None, true);
    let handle_value = job.to_string();
    let mut child = spawn_child(&["--check-job-handle", &handle_value], true);
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .expect("probe stdout")
        .read_to_string(&mut stdout)
        .expect("read handle result");
    let _ = child.wait();
    close(job);
    stdout.trim() == "job_handle_inherited=true"
}

#[test]
fn windows_job_object_probe_reports_platform_semantics() {
    let named_job_reopens_after_handles_close = probe_named_job_reopen();
    let terminate_reaches_orphaned_grandchild = probe_grandchild_termination();
    let std_inherits_inheritable_job_handle = probe_std_inherits_job_handle();
    println!(
        "WINDOWS_JOB_PROBE {{\"named_job_reopens_after_handles_close\":{named_job_reopens_after_handles_close},\"terminate_reaches_orphaned_grandchild\":{terminate_reaches_orphaned_grandchild},\"std_inherits_inheritable_job_handle\":{std_inherits_inheritable_job_handle}}}"
    );
    assert!(
        terminate_reaches_orphaned_grandchild,
        "TerminateJobObject did not reach the surviving grandchild"
    );
}

#[test]
fn windows_job_probe_has_expected_accounting_query_shape() {
    let job = create_job(None, false);
    let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
        TotalUserTime: 0,
        TotalKernelTime: 0,
        ThisPeriodTotalUserTime: 0,
        ThisPeriodTotalKernelTime: 0,
        TotalPageFaultCount: 0,
        TotalProcesses: 0,
        ActiveProcesses: 0,
        TotalTerminatedProcesses: 0,
    };
    // SAFETY: The job handle is live and the destination buffer has exactly
    // the size required by JobObjectBasicAccountingInformation.
    let ok = unsafe {
        QueryInformationJobObject(
            job,
            JobObjectBasicAccountingInformation,
            &mut info as *mut _ as *mut c_void,
            size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    close(job);
    assert_ne!(ok, 0);
}
