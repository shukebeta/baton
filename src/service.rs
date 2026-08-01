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
//! `kill -<pid>` reaches both the `serve` process and its in-flight
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
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BatonError, Result};

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

    /// Name of the control-plane lockfile at the control root, mirroring
    /// [`mailbox`]'s `serve.lock`.
    const CONTROL_LOCK_FILE: &str = "service.lock";
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
    /// Bound on `Run`'s final reap sweep after observing its own stop.
    const FINAL_REAP_GRACE_MS: u64 = 2_000;

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
        // session id on this restart.
        reclaim_stale_requests(control)?;
        writeln!(out, "baton service running on {}", control.display()).map_err(io_err)?;

        let mut children: HashMap<String, Child> = HashMap::new();
        loop {
            if consume_stop_sentinel(control)? {
                break;
            }
            reap_exited(&mut children);
            // One request's failure (a malformed spec, a transient spawn
            // error) must not crash the loop out from under every other
            // session this instance already owns — warn and keep polling,
            // the same "one bad message can't wedge the daemon" posture
            // `Mailbox::claim_next` takes for a malformed mailbox entry.
            match process_one_request(control) {
                Ok(Some((session_id, child))) => {
                    children.insert(session_id, child);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS)),
                Err(err) => {
                    eprintln!(
                        "warning: baton service failed to process a session-start request: {err}"
                    );
                    std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
                }
            }
        }

        // Best-effort final reap: `Teardown` already killed every known
        // session before dropping the sentinel, so this is normally instant.
        let deadline = Instant::now() + Duration::from_millis(FINAL_REAP_GRACE_MS);
        while !children.is_empty() && Instant::now() < deadline {
            reap_exited(&mut children);
            if !children.is_empty() {
                std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
            }
        }
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
        let child = spawn_serve_child(&spec)?;
        let pid = child.id();
        let started_at = recorded_start_key(pid);
        let record = SessionRecord {
            id: fresh_session_id(),
            spec,
            pid,
            started_at,
        };
        write_session_record(control, &record)?;
        let response = StartResponse {
            session_id: record.id.clone(),
        };
        let json = serde_json::to_string(&response)
            .map_err(|err| BatonError::Io(format!("could not serialize start response: {err}")))?;
        let responses = responses_dir(control);
        fs::create_dir_all(&responses)
            .map_err(|err| BatonError::Io(format!("could not create {responses:?}: {err}")))?;
        mailbox::atomic_write(&responses, &mailbox::file_name(request_id), &json)?;
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
        // `kill -<pid>` escalation reaches exactly this session's `serve`
        // process and its `agent-cmd` grandchild, nothing else this service
        // manages. Safe and stable — deliberately not `pre_exec(setsid)`,
        // which would require `unsafe`.
        command.process_group(0);
        command
            .spawn()
            .map_err(|err| BatonError::Io(format!("could not spawn baton serve: {err}")))
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

    #[cfg(target_os = "linux")]
    fn is_session_alive(record: &SessionRecord) -> bool {
        match (&record.started_at, process_start_key(record.pid)) {
            (Some(recorded), Some(current)) => *recorded == current,
            (None, Some(_)) => true,
            (_, None) => false,
        }
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

    #[cfg(not(target_os = "linux"))]
    fn is_session_alive(record: &SessionRecord) -> bool {
        Command::new("kill")
            .args(["-0", &record.pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// Sends `sig` (e.g. `"-TERM"`) to the process **group** led by `pid`
    /// (`kill <sig> -<pid>`). A failure (the group is already gone) is not
    /// surfaced — only a failure to run `kill` itself is.
    fn signal_group(pid: u32, sig: &str) -> Result<()> {
        Command::new("kill")
            .arg(sig)
            .arg(format!("-{pid}"))
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

    /// Stops one session: cooperative `serve --stop` on its inbox first,
    /// bounded wait, then `SIGTERM`/`SIGKILL` process-group escalation if
    /// still alive, then removes its durable record. Idempotent — a session
    /// already gone just gets its (possibly already-absent) record cleaned
    /// up.
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

    fn execute_teardown(control: &Path, mut out: impl Write) -> Result<()> {
        for record in list_session_records(control)? {
            stop_session_record(control, &record)?;
        }
        match request_control_stop(control)? {
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

    #[cfg(test)]
    mod tests {
        use super::*;

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
            let dir = TempDir::new("lock");
            let _held = acquire_control_lock(&dir.path).expect("first lock");
            assert!(acquire_control_lock(&dir.path).is_err());
        }

        /// `probe_control` reports `Live` while a lock is held and
        /// `NotRunning` once released, without ever writing the stop
        /// sentinel (a pure read).
        #[test]
        fn probe_control_reflects_lock_without_signalling() {
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
    }
}
