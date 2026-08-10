#![cfg(windows)]

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout) {
            if json["tasks"][0]["state"] == "running" {
                running = true;
                break;
            }
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
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&status.stdout) {
            if json["tasks"][0]["state"] != "running" {
                return;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("Windows Job Object did not terminate the task command tree");
}
