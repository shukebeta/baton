#![cfg(windows)]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{CloseHandle, FALSE, WAIT_OBJECT_0};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

fn baton(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(args)
        .output()
        .expect("run baton")
}

fn wait_for_service(control: &str) {
    for _ in 0..100 {
        let output = baton(&["service", "status", "--control", control]);
        if output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains("\"service_running\":true")
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("Windows baton service did not acquire its control lock");
}

struct ServiceGuard {
    root: PathBuf,
    control: PathBuf,
    run: Option<Child>,
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        let control = self.control.to_string_lossy().into_owned();
        let _ = baton(&["service", "teardown", "--control", &control, "--force"]);
        if let Some(mut run) = self.run.take() {
            let _ = run.kill();
            let _ = run.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn start_service() -> ServiceGuard {
    start_service_with_env(None)
}

fn start_service_with_env(environment: Option<(&str, &PathBuf)>) -> ServiceGuard {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "baton-windows-service-{}-{nanos}",
        std::process::id()
    ));
    let control = root.join("control");
    fs::create_dir_all(&root).expect("create Windows service test root");
    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    if let Some((name, value)) = environment {
        run.env(name, value);
    }
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let run = run.spawn().expect("spawn Windows baton service");
    let guard = ServiceGuard {
        root,
        control,
        run: Some(run),
    };
    wait_for_service(guard.control.to_str().unwrap());
    guard
}

fn wait_for_task_record<F>(control: &Path, matches: F) -> (PathBuf, serde_json::Value)
where
    F: Fn(&serde_json::Value) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(entries) = fs::read_dir(control.join("tasks")) {
            for path in entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            {
                let Ok(data) = fs::read(&path) else { continue };
                let Ok(record) = serde_json::from_slice::<serde_json::Value>(&data) else {
                    continue;
                };
                if matches(&record) {
                    return (path, record);
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "matching Windows task record was not persisted"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn task_status(control: &str) -> serde_json::Value {
    let output = baton(&["task", "status", "--control", control]);
    assert!(
        output.status.success(),
        "task status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("task status is JSON")
}

fn wait_for_process_gone(pid: u32) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        // SAFETY: the access mask only queries and waits on the process named
        // by the durable PID. A zero handle means Windows no longer exposes
        // the test-owned process, which is the expected path after Job Object
        // termination.
        let process = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                FALSE,
                pid,
            )
        };
        if process == 0 {
            return;
        }
        // SAFETY: `process` is the live handle returned by OpenProcess.
        let exited = unsafe { WaitForSingleObject(process, 0) } == WAIT_OBJECT_0;
        // SAFETY: close the handle opened in this iteration exactly once.
        unsafe { CloseHandle(process) };
        if exited {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Windows task process {pid} remained live after admission rollback"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_task_request(control: &Path) -> PathBuf {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        for directory in ["task-requests", "task-processing"] {
            if let Ok(entries) = fs::read_dir(control.join(directory))
                && let Some(path) = entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            {
                return path;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Windows task-start request was not written"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

/// Starts a live managed session the task tests can own tasks under. The
/// session's serve child stays alive polling its (empty) inbox.
fn start_session(guard: &ServiceGuard) -> String {
    let control = guard.control.to_string_lossy().into_owned();
    let inbox = guard.root.join("session-inbox");
    let outbox = guard.root.join("session-outbox");
    let start = baton(&[
        "service",
        "start",
        "--control",
        &control,
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--agent-cmd",
        "cmd.exe",
        "--agent-arg",
        "/D",
        "--agent-arg",
        "/C",
        "--agent-arg",
        "exit 0",
    ]);
    assert!(
        start.status.success(),
        "service start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let session = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!session.is_empty(), "service start returned a session id");
    session
}

/// Polls `task status` until the task leaves `running`, returning its
/// terminal state, exit code, and elapsed milliseconds.
fn wait_for_terminal_task(control: &str, task_id: &str) -> (String, Option<i64>, Option<u64>) {
    for _ in 0..200 {
        let status = baton(&["task", "status", "--control", control, "--task", task_id]);
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout) {
            let state = json["tasks"][0]["state"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if state != "running" {
                return (
                    state,
                    json["tasks"][0]["exit_code"].as_i64(),
                    json["tasks"][0]["elapsed_ms"].as_u64(),
                );
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("task {task_id} did not reach a terminal state in time");
}

#[test]
fn windows_task_job_owns_and_terminates_command_tree() {
    let guard = start_service();
    let control = guard.control.to_string_lossy().into_owned();
    let inbox = guard.root.join("session-inbox");
    let outbox = guard.root.join("session-outbox");
    let callback = guard.root.join("callback");
    let start = baton(&[
        "service",
        "start",
        "--control",
        &control,
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--agent-cmd",
        "cmd.exe",
        "--agent-arg",
        "/D",
        "--agent-arg",
        "/C",
        "--agent-arg",
        "exit 0",
    ]);
    assert!(
        start.status.success(),
        "service start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let session = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!session.is_empty(), "service start returned a session id");

    let task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        "cmd.exe",
        "--arg",
        "/D",
        "--arg",
        "/C",
        "--arg",
        "ping -n 60 127.0.0.1 > NUL",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        task.status.success(),
        "task start failed: {}",
        String::from_utf8_lossy(&task.stderr)
    );
    let task_id = String::from_utf8_lossy(&task.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "task start returned a task id");

    let mut running = false;
    for _ in 0..100 {
        let status = baton(&["task", "status", "--control", &control, "--task", &task_id]);
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout)
            && json["tasks"][0]["state"] == "running"
        {
            running = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(running, "suspended task was not admitted as running");

    let cancel = baton(&["task", "cancel", "--control", &control, "--task", &task_id]);
    assert!(
        cancel.status.success(),
        "task cancel failed: {}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    for _ in 0..100 {
        let status = baton(&["task", "status", "--control", &control, "--task", &task_id]);
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout)
            && json["tasks"][0]["state"] != "running"
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("Windows Job Object did not terminate the task command tree");
}

/// A restarted supervisor must retain control of a task whose process
/// creation key still matches even though the original supervisor no longer
/// owns its Job Object handle. Cancellation uses the re-adopted Job Object
/// when the inherited task handle kept it named; timeout remains effective
/// through the same rehydrated PID tracker.
#[test]
fn windows_rehydrated_tasks_remain_live_and_controllable() {
    let mut guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let command = "cmd.exe";

    let cancel_task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        command,
        "--arg",
        "/D",
        "--arg",
        "/C",
        "--arg",
        "ping -n 60 127.0.0.1 > NUL",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        cancel_task.status.success(),
        "cancel task start failed: {}",
        String::from_utf8_lossy(&cancel_task.stderr)
    );
    let cancel_task_id = String::from_utf8_lossy(&cancel_task.stdout)
        .trim()
        .to_string();
    assert!(!cancel_task_id.is_empty(), "cancel task returned an id");

    let timeout_task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        command,
        "--arg",
        "/D",
        "--arg",
        "/C",
        "--arg",
        "ping -n 60 127.0.0.1 > NUL",
        "--max-duration-ms",
        "1000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        timeout_task.status.success(),
        "timeout task start failed: {}",
        String::from_utf8_lossy(&timeout_task.stderr)
    );
    let timeout_task_id = String::from_utf8_lossy(&timeout_task.stdout)
        .trim()
        .to_string();
    assert!(!timeout_task_id.is_empty(), "timeout task returned an id");

    for task_id in [&cancel_task_id, &timeout_task_id] {
        let mut running = false;
        for _ in 0..100 {
            let status = baton(&["task", "status", "--control", &control, "--task", task_id]);
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout)
                && json["tasks"][0]["state"] == "running"
            {
                running = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(running, "task {task_id} was not running before restart");
    }

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run.kill().expect("kill initial service supervisor");
    let old_status = old_run.wait().expect("initial service supervisor exits");
    assert!(!old_status.success(), "initial supervisor was interrupted");

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(
        restarted
            .spawn()
            .expect("spawn restarted service supervisor"),
    );
    wait_for_service(&control);

    let status = baton(&[
        "task",
        "status",
        "--control",
        &control,
        "--task",
        &cancel_task_id,
    ]);
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("rehydrated task status is JSON");
    assert_eq!(status_json["tasks"][0]["state"], "running");
    assert_eq!(status_json["tasks"][0]["live"], true);
    assert_eq!(status_json["tasks"][0]["liveness"], "live");

    let cancel = baton(&[
        "task",
        "cancel",
        "--control",
        &control,
        "--task",
        &cancel_task_id,
    ]);
    assert!(
        cancel.status.success(),
        "rehydrated task cancel failed: {}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    let (state, _, _) = wait_for_terminal_task(&control, &cancel_task_id);
    assert_eq!(state, "cancelled");

    let (state, _, _) = wait_for_terminal_task(&control, &timeout_task_id);
    assert_eq!(state, "timeout");
}

/// A direct task child may have exited and left a descendant in its Job
/// Object when the supervisor stops. The inherited Job Object handle lets a
/// restarted supervisor continue the descendant drain and reach a terminal
/// result instead of leaving the task permanently unresolved. The direct
/// child's exit code is intentionally unavailable after restart.
#[test]
fn windows_rehydrated_descendant_drain_reaches_terminal_state() {
    let mut guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "--arg",
        "--spawn-descendant",
        "--arg",
        "3000",
        "--arg",
        "0",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        task.status.success(),
        "descendant task start failed: {}",
        String::from_utf8_lossy(&task.stderr)
    );
    let task_id = String::from_utf8_lossy(&task.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "descendant task returned an id");

    let mut descendant_started = false;
    for _ in 0..100 {
        let status = baton(&["task", "status", "--control", &control, "--task", &task_id]);
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout)
            && let Some(stdout_path) = json["tasks"][0]["stdout_path"].as_str()
            && std::fs::read_to_string(stdout_path)
                .map(|stdout| !stdout.trim().is_empty())
                .unwrap_or(false)
        {
            descendant_started = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(descendant_started, "task descendant did not start in time");
    // Allow the service tick to reap the direct child and park on the
    // descendant before the supervisor is interrupted.
    thread::sleep(Duration::from_millis(250));

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run.kill().expect("kill initial service supervisor");
    let old_status = old_run.wait().expect("initial service supervisor exits");
    assert!(!old_status.success(), "initial supervisor was interrupted");

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(
        restarted
            .spawn()
            .expect("spawn restarted service supervisor"),
    );
    wait_for_service(&control);

    let (state, exit_code, _) = wait_for_terminal_task(&control, &task_id);
    assert_eq!(state, "failed");
    assert_eq!(exit_code, None);
}

/// A task whose entire process tree exits while the supervisor is down
/// must still finalize after restart. Unlike the surviving-descendant case
/// above, nothing is left running: the Job Object is fully destroyed once
/// every member process (and the last handle keeping it alive) is gone, so
/// its name no longer resolves on restart. Before the fix underlying this
/// test, that made the task permanently `running`/`unresolved`.
#[test]
fn windows_task_finished_during_downtime_finalizes_after_restart() {
    let mut guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        "cmd.exe",
        "--arg",
        "/D",
        "--arg",
        "/C",
        "--arg",
        "ping -n 2 127.0.0.1 > NUL",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        task.status.success(),
        "short-lived task start failed: {}",
        String::from_utf8_lossy(&task.stderr)
    );
    let task_id = String::from_utf8_lossy(&task.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "short-lived task returned an id");

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run.kill().expect("kill initial service supervisor");
    let old_status = old_run.wait().expect("initial service supervisor exits");
    assert!(!old_status.success(), "initial supervisor was interrupted");

    // Let the whole tree (cmd.exe and the ping it waits on) exit while the
    // supervisor is down, so the Job Object is fully destroyed before
    // restart.
    thread::sleep(Duration::from_millis(3000));

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(
        restarted
            .spawn()
            .expect("spawn restarted service supervisor"),
    );
    wait_for_service(&control);

    let (state, exit_code, _) = wait_for_terminal_task(&control, &task_id);
    assert_eq!(state, "failed");
    assert_eq!(exit_code, None);
}

/// Starts a task whose direct command spawns a short-lived descendant and
/// then exits `0`. The descendant outlives the parent inside the shared Job
/// Object, so the service must park the reap until the tree drains — and must
/// then record `completed` with the real exit code, not a code-less failure.
#[test]
fn windows_task_successful_direct_child_with_surviving_descendant_completes() {
    let guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");

    let task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "--arg",
        "--spawn-descendant",
        "--arg",
        "2000",
        "--arg",
        "0",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        task.status.success(),
        "task start failed: {}",
        String::from_utf8_lossy(&task.stderr)
    );
    let task_id = String::from_utf8_lossy(&task.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "task start returned a task id");

    let (state, exit_code, elapsed_ms) = wait_for_terminal_task(&control, &task_id);
    assert_eq!(state, "completed", "successful direct child must complete");
    assert_eq!(exit_code, Some(0), "real exit code must be recorded");
    assert!(
        elapsed_ms.is_some_and(|ms| ms >= 1000),
        "elapsed must span the descendant drain, got {elapsed_ms:?}"
    );
}

/// The failing mirror: the direct command exits `7` while its descendant
/// still holds the Job Object. The task must record `failed` *with* exit
/// code 7 — the fix for the code-less `failed` regression.
#[test]
fn windows_task_failed_direct_child_with_surviving_descendant_keeps_exit_code() {
    let guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");

    let task = baton(&[
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "--arg",
        "--spawn-descendant",
        "--arg",
        "2000",
        "--arg",
        "7",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    assert!(
        task.status.success(),
        "task start failed: {}",
        String::from_utf8_lossy(&task.stderr)
    );
    let task_id = String::from_utf8_lossy(&task.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "task start returned a task id");

    let (state, exit_code, _elapsed_ms) = wait_for_terminal_task(&control, &task_id);
    assert_eq!(state, "failed", "non-zero direct child must fail");
    assert_eq!(exit_code, Some(7), "real exit code must be recorded");
}

/// If the supervisor dies after persisting a prepared task record but before
/// publishing the task-start response, the client records rollback and the
/// next Windows supervisor must terminate the inherited Job Object and remove
/// the unadmitted record instead of rehydrating it.
#[test]
fn windows_task_start_rolls_back_prepared_record_after_supervisor_loss() {
    let admission_barrier = std::env::temp_dir().join(format!(
        "baton-windows-task-admission-{}-{}.barrier",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos()
    ));
    fs::write(&admission_barrier, "hold").expect("create admission barrier");
    let mut guard = start_service_with_env(Some((
        "BATON_TEST_TASK_ADMISSION_BARRIER",
        &admission_barrier,
    )));
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");

    let mut client = Command::new(env!("CARGO_BIN_EXE_baton"));
    client.args([
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "--arg",
        "--sleep-ms",
        "--arg",
        "30000",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    client.stdout(Stdio::piped());
    client.stderr(Stdio::piped());
    let client = client
        .spawn()
        .expect("spawn task start client at prepared boundary");

    let (task_record_path, task_record) =
        wait_for_task_record(&guard.control, |record| record["admission"] == "prepared");
    let task_pid = task_record["pid"]
        .as_u64()
        .expect("prepared task record has a PID") as u32;
    let request_id = task_record["request_id"]
        .as_str()
        .expect("prepared task record has a request id")
        .to_string();
    assert!(
        task_record["job"].as_str().is_some(),
        "prepared Windows task records its Job Object name"
    );

    let mut run = guard.run.take().expect("initial service supervisor");
    run.kill().expect("kill initial service supervisor");
    assert!(
        !run.wait()
            .expect("initial service supervisor exits")
            .success(),
        "initial supervisor was interrupted"
    );

    let output = client
        .wait_with_output()
        .expect("wait for task start after supervisor loss");
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "task start must fail after supervisor loss"
    );
    assert!(
        message.contains("task start request was not admitted"),
        "failure explains the lost admission: {message}"
    );
    let rollback_path = guard
        .control
        .join("task-start-rollback")
        .join(format!("{request_id}.json"));
    assert!(
        rollback_path.is_file(),
        "client persists a rollback marker before restart"
    );
    assert!(
        !guard
            .control
            .join("task-requests")
            .join(format!("{request_id}.json"))
            .exists()
            && !guard
                .control
                .join("task-processing")
                .join(format!("{request_id}.json"))
                .exists(),
        "lost task-start request is removed from both pending states"
    );

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(
        restarted
            .spawn()
            .expect("spawn restarted service supervisor"),
    );
    wait_for_service(&control);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = task_status(&control);
        if status["tasks"]
            .as_array()
            .is_some_and(|tasks| tasks.is_empty())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "prepared task was rehydrated after Windows rollback"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !task_record_path.exists(),
        "rollback removes the prepared Windows task record"
    );
    assert!(
        !rollback_path.exists(),
        "rollback cleanup removes the marker after the record is gone"
    );
    wait_for_process_gone(task_pid);

    let teardown = baton(&["service", "teardown", "--control", &control, "--force"]);
    assert!(
        teardown.status.success(),
        "teardown succeeds: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    assert!(
        guard
            .run
            .take()
            .expect("restarted service supervisor")
            .wait()
            .expect("restarted supervisor exits")
            .success(),
        "restarted supervisor exits cleanly"
    );
    let _ = fs::remove_file(admission_barrier);
}

/// Startup reconciliation must keep a rollback marker until the task record
/// and both pending request locations are gone. Killing the Windows
/// supervisor at that boundary proves a later startup can finish cleanup and
/// cannot replay the request.
#[test]
fn windows_task_start_reconcile_keeps_rollback_marker_until_cleanup() {
    let mut guard = start_service();
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let barrier = guard.root.join("rollback-reconcile.barrier");
    let request_id = "windows-rollback-reconcile-request";
    let task_id = "windows-rollback-reconcile-task";
    let request_file = guard
        .control
        .join("task-requests")
        .join(format!("{request_id}.json"));
    let processing_file = guard
        .control
        .join("task-processing")
        .join(format!("{request_id}.json"));
    let rollback_file = guard
        .control
        .join("task-start-rollback")
        .join(format!("{request_id}.json"));
    let task_record_file = guard.control.join("tasks").join(format!("{task_id}.json"));
    let task_spec = serde_json::json!({
        "schema": "baton.task-spec/v1",
        "session": "session-not-needed-for-reconciliation",
        "command": "cmd.exe",
        "args": ["/D", "/C", "exit 0"],
        "cwd": null,
        "env": [],
        "milestones_ms": [],
        "max_duration_ms": 60000,
        "callback": {"inbox": callback, "role": null}
    });
    let task_request = serde_json::json!({
        "schema": "baton.task-spec/v1",
        "session": "session-not-needed-for-reconciliation",
        "command": "cmd.exe",
        "args": ["/D", "/C", "exit 0"],
        "cwd": null,
        "env": [],
        "milestones_ms": [],
        "max_duration_ms": 60000,
        "callback": {"inbox": callback, "role": null}
    });
    let task_record = serde_json::json!({
        "id": task_id,
        "request_id": request_id,
        "admission": "committed",
        "spec": task_spec,
        "pid": 0,
        "started_at": null,
        "started_ms": null,
        "state": "completed",
        "exit_code": 0,
        "elapsed_ms": 1,
        "stdout_path": "",
        "stderr_path": "",
        "delivered_milestones": 0
    });

    fs::create_dir_all(guard.control.join("task-requests")).expect("create task requests");
    fs::create_dir_all(guard.control.join("task-processing")).expect("create task processing");
    fs::create_dir_all(guard.control.join("task-start-rollback")).expect("create rollback markers");
    fs::create_dir_all(guard.control.join("tasks")).expect("create task records");
    fs::write(&request_file, serde_json::to_vec(&task_request).unwrap())
        .expect("write pending task request");
    fs::write(&processing_file, b"{}").expect("write claimed task request");
    fs::write(&rollback_file, b"").expect("write rollback marker");
    fs::write(&task_record_file, serde_json::to_vec(&task_record).unwrap())
        .expect("write task record");
    fs::write(&barrier, b"hold").expect("create rollback barrier");

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run
        .kill()
        .expect("stop initial service before barrier run");
    let _ = old_run.wait();

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", &control]);
    run.env("BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER", &barrier);
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn reconciliation service");
    wait_for_service(&control);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "Windows reconciliation did not reach the rollback cleanup boundary"
        );
        if rollback_file.exists()
            && !task_record_file.exists()
            && !request_file.exists()
            && !processing_file.exists()
        {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    run_child.kill().expect("kill service at rollback boundary");
    assert!(
        !run_child
            .wait()
            .expect("service at barrier exits")
            .success(),
        "service was interrupted at the rollback boundary"
    );

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(restarted.spawn().expect("spawn restarted service"));
    wait_for_service(&control);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while rollback_file.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "restart did not clear the completed Windows rollback marker"
        );
        thread::sleep(Duration::from_millis(25));
    }
    let status = task_status(&control);
    assert!(
        status["tasks"]
            .as_array()
            .is_some_and(|tasks| tasks.is_empty()),
        "a rollback request is not replayed after Windows startup cleanup"
    );

    let teardown = baton(&["service", "teardown", "--control", &control, "--force"]);
    assert!(teardown.status.success(), "teardown succeeds");
    assert!(
        guard
            .run
            .take()
            .expect("restarted service")
            .wait()
            .expect("restarted service exits")
            .success(),
        "restarted service exits cleanly"
    );
}

/// A failed response publication leaves a committed Windows task durable. A
/// new supervisor must restore the response and finalize the admission phase
/// without spawning a second task.
#[test]
fn windows_task_start_response_write_failure_retries_committed_record() {
    let failure_marker = std::env::temp_dir().join(format!(
        "baton-windows-response-failure-{}-{}.marker",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos()
    ));
    fs::write(&failure_marker, "fail once").expect("create response failure marker");
    let mut guard = start_service_with_env(Some((
        "BATON_TEST_TASK_START_RESPONSE_WRITE_FAILURE",
        &failure_marker,
    )));
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let request_id = "windows-response-write-failure-request";
    let task_request = serde_json::json!({
        "schema": "baton.task-spec/v1",
        "session": session,
        "command": env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "args": ["--sleep-ms", "30000"],
        "cwd": null,
        "env": [],
        "milestones_ms": [],
        "max_duration_ms": 60000,
        "callback": {"inbox": callback, "role": null}
    });
    let task_requests = guard.control.join("task-requests");
    fs::create_dir_all(&task_requests).expect("create task requests");
    fs::write(
        task_requests.join(format!("{request_id}.json")),
        serde_json::to_vec(&task_request).expect("serialize task request"),
    )
    .expect("write task request");

    let (task_record_path, committed) = wait_for_task_record(&guard.control, |record| {
        record["request_id"] == request_id && record["admission"] == "committed"
    });
    let response_path = guard
        .control
        .join("task-responses")
        .join(format!("{request_id}.json"));
    assert!(
        !failure_marker.exists(),
        "response failure marker is consumed"
    );
    assert!(!response_path.exists(), "failed response is not published");

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run.kill().expect("kill service after response failure");
    assert!(
        !old_run
            .wait()
            .expect("service after response failure exits")
            .success(),
        "service after response failure was interrupted"
    );

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(restarted.spawn().expect("spawn response retry service"));
    wait_for_service(&control);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let record: serde_json::Value = serde_json::from_slice(
            &fs::read(&task_record_path).expect("read restored task record"),
        )
        .expect("restored task record is JSON");
        if response_path.is_file() && record["admission"] == "responded" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Windows restart did not restore and finalize the response"
        );
        thread::sleep(Duration::from_millis(25));
    }
    let status = task_status(&control);
    let tasks = status["tasks"].as_array().expect("tasks array");
    assert_eq!(tasks.len(), 1, "response retry retains one Windows task");
    assert_eq!(tasks[0]["id"], committed["id"]);

    let teardown = baton(&["service", "teardown", "--control", &control, "--force"]);
    assert!(teardown.status.success(), "teardown succeeds");
    assert!(
        guard
            .run
            .take()
            .expect("response retry service")
            .wait()
            .expect("response retry service exits")
            .success(),
        "response retry service exits cleanly"
    );
    let _ = fs::remove_file(failure_marker);
}

/// A task-start client that dies after durable acknowledgement leaves both a
/// private response claim and the acknowledgement marker. Windows startup
/// reconciliation must remove both without replaying or duplicating the task.
#[test]
fn windows_task_start_claim_ack_cleanup_survives_client_loss() {
    let mut guard = start_service();
    let session = start_session(&guard);
    let control = guard.control.to_string_lossy().into_owned();
    let callback = guard.root.join("callback");
    let ack_barrier = guard.root.join("task-start-ack.barrier");
    fs::write(&ack_barrier, "hold").expect("create task-start ack barrier");

    let admission_lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(guard.control.join("service.admission.lock"))
        .expect("open service admission lock");
    admission_lock.lock().expect("hold service admission lock");

    let mut client = Command::new(env!("CARGO_BIN_EXE_baton"));
    client.args([
        "task",
        "start",
        "--control",
        &control,
        "--session",
        &session,
        "--command",
        env!("CARGO_BIN_EXE_windows_job_probe_child"),
        "--arg",
        "--sleep-ms",
        "--arg",
        "30000",
        "--max-duration-ms",
        "60000",
        "--callback-inbox",
        callback.to_str().unwrap(),
    ]);
    client.env("BATON_TEST_TASK_START_ACK_BARRIER", &ack_barrier);
    client.stdout(Stdio::null());
    client.stderr(Stdio::null());
    let mut client = client.spawn().expect("spawn task-start client");

    let request_path = wait_for_task_request(&guard.control);
    let request_name = request_path
        .file_name()
        .expect("task request filename")
        .to_string_lossy()
        .into_owned();
    let request_id = request_name
        .strip_suffix(".json")
        .expect("task request has a JSON suffix")
        .to_string();
    drop(admission_lock);

    let response_path = guard.control.join("task-responses").join(&request_name);
    let ack_path = guard.control.join("task-start-ack").join(&request_name);
    let claim_path = guard
        .control
        .join("task-responses")
        .join(format!(".{request_name}.claimed"));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if ack_path.is_file() && claim_path.is_file() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Windows task-start client did not reach the durable acknowledgement boundary"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !response_path.exists(),
        "claimed response is no longer public"
    );
    client
        .kill()
        .expect("kill task-start client at claim boundary");
    assert!(
        !client.wait().expect("task-start client exits").success(),
        "task-start client was interrupted at the claim boundary"
    );
    assert!(ack_path.is_file(), "acknowledgement survives client loss");
    assert!(claim_path.is_file(), "private claim survives client loss");
    let (_, task_record) =
        wait_for_task_record(&guard.control, |record| record["request_id"] == request_id);
    let task_id = task_record["id"].clone();

    let mut old_run = guard.run.take().expect("initial service supervisor");
    old_run.kill().expect("kill service after client loss");
    assert!(
        !old_run
            .wait()
            .expect("service after client loss exits")
            .success(),
        "service after client loss was interrupted"
    );

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_baton"));
    restarted.args(["service", "run", "--control", &control]);
    restarted.stdout(Stdio::null());
    restarted.stderr(Stdio::null());
    guard.run = Some(restarted.spawn().expect("spawn restarted service"));
    wait_for_service(&control);
    assert!(
        !ack_path.exists(),
        "restart cleans the durable acknowledgement"
    );
    assert!(!claim_path.exists(), "restart cleans the private claim");
    assert!(
        !response_path.exists(),
        "restart does not recreate the consumed response"
    );

    let status = task_status(&control);
    let tasks = status["tasks"].as_array().expect("tasks array");
    assert_eq!(tasks.len(), 1, "client-loss cleanup retains one task");
    assert_eq!(
        tasks[0]["id"], task_id,
        "client-loss cleanup keeps the original task"
    );

    let teardown = baton(&["service", "teardown", "--control", &control, "--force"]);
    assert!(teardown.status.success(), "teardown succeeds");
    assert!(
        guard
            .run
            .take()
            .expect("restarted service")
            .wait()
            .expect("restarted service exits")
            .success(),
        "restarted service exits cleanly"
    );
}
