//! `baton service`: a host-owned supervisor for `baton serve` sessions.
//!
//! `baton serve --agent-cmd` is already a resident, single-instance-locked
//! mailbox daemon (see [`crate::mailbox`]), but nothing durable *owns* it: an
//! integration that launches it directly inherits the daemon as a child of its
//! own process tree, and `setsid`/`disown` only detach a process group — an
//! external agent/tool runner that reaps that tree takes the daemon with it.
//! `baton service run [--control <dir>]` is the missing owner: a long-lived
//! foreground process (meant to be kept alive by an OS service manager, e.g.
//! the systemd user-service unit under `packaging/systemd/`) that spawns each
//! `baton serve` session as its own direct child, detached into its own
//! process group, and tracks it durably so a short-lived client can start,
//! inspect, stop, or tear one down without ever sharing a process tree with it.
//!
//! ## Control surface
//!
//! The selected control directory holds (the default is the per-user
//! `<BATON_HOME>/service` or `$HOME/.baton/service` path):
//! - `service.lock` — the exclusive single-instance advisory lock, taken by
//!   [`ServiceCommand::Run`] for as long as it runs. Mirrors
//!   [`mailbox::Mailbox`]'s `serve.lock`.
//! - `service.admission.lock` — a short-lived advisory lock shared by task
//!   admission and session cleanup. It is separate from `service.lock`,
//!   which the long-lived `Run` process holds for its entire lifetime.
//! - `service.probe.lock` — a short-lived advisory lock serializing
//!   `service.lock`'s acquisition against the liveness probes behind
//!   `Status`/`Stop`/`Teardown`/`Start`. A probe answers "not running" by
//!   *taking* `service.lock`, so without this a probe landing on a starting
//!   `Run` would refuse it as a duplicate instance.
//! - `service.stop` — the cooperative-stop sentinel `Teardown` drops for a live
//!   `Run` to observe between polls. Mirrors `serve.stop`.
//! - `requests/` / `processing/` / `responses/` — the atomic-rename request
//!   protocol `Start` uses to reach the live `Run` loop (the only operation
//!   that must run *in* the long-lived process, since spawning there is the
//!   entire point). A session-spec request is delivered into `requests/`,
//!   claimed into `processing/` by `Run`, and answered into `responses/` keyed
//!   by the request id — the same temp-file-then-`rename` idiom as
//!   [`mailbox::deliver_to`]/[`mailbox::atomic_write`], reused directly.
//! - `task-start-ack/` — durable markers written by a task-start client after
//!   it claims a response, allowing restart reconciliation to distinguish a
//!   consumed response from one that was never published.
//! - `sessions/<id>.json` — one durable [`SessionRecord`] per session, holding
//!   its effective spec, real PID, and (on Unix) its stderr log path. The
//!   corresponding `sessions/<id>/stderr.log` keeps daemon warnings durable.
//!   `Status`/`Stop`/`Teardown` read this directly and act on the OS process by
//!   PID; none of them need the `Run` loop to be alive, so a session started by
//!   a since-crashed `Run` can still be inspected, stopped, or torn down.
//!
//! ## Ownership boundary
//!
//! A spawned `baton serve` child is never `wait()`-ed by its short-lived
//! submitter — only `Run` holds the [`std::process::Child`] handle, reaping it
//! (via non-blocking [`std::process::Child::try_wait`]) as its loop ticks. On
//! Unix, cleanup targets the child's process group. On Windows, each session
//! and task has a Job Object retained until its active-process count reaches
//! zero, so an exited `serve` or task parent does not release ownership of a
//! surviving descendant.
//!
//! ## Host support
//!
//! Linux and macOS use process groups, `/proc`/`ps` corroboration, and Unix
//! signal escalation. Windows uses `windows-sys` Job Objects and
//! `GetProcessTimes` in the separate `cfg(windows)` implementation. The
//! systemd user-service integration (`packaging/systemd/`) and macOS
//! LaunchAgent integration (`packaging/launchd/`) are external to this binary;
//! choosing a Windows host-service manager remains a packaging concern.

use std::io::Write;
#[cfg(any(unix, windows))]
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
        /// The optional `--control <dir>` root; `None` uses the per-user
        /// default `BATON_HOME/service` or `home/.baton/service`.
        control: Option<String>,
        /// The optional `--task-retention <duration>` milliseconds a
        /// delivered terminal task record is kept before automatic runtime
        /// reaping; `None` uses [`task_tick::DEFAULT_TASK_RETENTION_MS`].
        task_retention_ms: Option<u64>,
    },
    /// Submit a session spec to a live `Run` and return its session id.
    Start {
        /// The optional `--control <dir>` root; `None` uses the per-user
        /// default `BATON_HOME/service` or `home/.baton/service`.
        control: Option<String>,
        /// The session to start. Boxed: `SessionSpec` is by far the largest
        /// field of any `ServiceCommand` variant, and boxing it keeps the
        /// enum itself small regardless of how many optional agent flags it
        /// carries.
        spec: Box<SessionSpec>,
    },
    /// Report the service's own liveness plus every managed session's (or
    /// just `session`'s, when given).
    Status {
        /// The optional `--control <dir>` root; `None` uses the per-user
        /// default `BATON_HOME/service` or `home/.baton/service`.
        control: Option<String>,
        /// `--session <id>`; `None` reports every known session.
        session: Option<String>,
    },
    /// Stop one session: cooperative `serve --stop` first, then a bounded
    /// process-group escalation. Idempotent. `force` permits cleanup when
    /// process identity cannot be corroborated.
    Stop {
        /// The optional `--control <dir>` root; `None` uses the per-user
        /// default `BATON_HOME/service` or `home/.baton/service`.
        control: Option<String>,
        /// The session id to stop.
        session: String,
        /// Signal and remove a record whose identity is unresolved.
        force: bool,
    },
    /// Stop every managed session, then request `Run`'s own cooperative stop.
    /// Idempotent, and independent of whether `Run` is currently alive.
    Teardown {
        /// The optional `--control <dir>` root; `None` uses the per-user
        /// default `BATON_HOME/service` or `home/.baton/service`.
        control: Option<String>,
        /// Signal and remove records whose identities are unresolved.
        force: bool,
    },
}

/// Runs `cmd` to completion, writing any human-readable output to `out`.
#[cfg(any(unix, windows))]
pub fn execute_service(cmd: ServiceCommand, out: impl Write) -> Result<()> {
    imp::dispatch(cmd, out)
}

/// `baton service` has no supported implementation on this host: process
/// ownership and the platform-specific escalation this module relies on have
/// no implementation on this host. Fails clearly rather than silently
/// degrading the ownership guarantee.
#[cfg(not(any(unix, windows)))]
pub fn execute_service(cmd: ServiceCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton service requires a supported host (Linux, macOS, or Windows)".to_string(),
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
#[cfg(any(unix, windows))]
pub fn execute_task(cmd: TaskCommand, out: impl Write) -> Result<()> {
    imp::dispatch_task(cmd, out)
}

/// `baton task` has no supported implementation on this host; see
/// [`execute_service`]'s non-Unix stub for why.
#[cfg(not(any(unix, windows)))]
pub fn execute_task(cmd: TaskCommand, _out: impl Write) -> Result<()> {
    let _ = cmd;
    Err(BatonError::Io(
        "baton task requires a supported host (Linux, macOS, or Windows)".to_string(),
    ))
}

#[cfg(any(unix, windows))]
mod admission;
#[cfg(any(unix, windows))]
mod control;
#[cfg(any(unix, windows))]
mod records;
#[cfg(any(unix, windows))]
mod task_tick;

#[cfg(unix)]
mod imp {
    #[cfg(test)]
    use super::control::{
        ADMISSION_LOCK_FILE, AdmissionGuard, CONTROL_STOP_FILE, ControlLiveness, KILL_GRACE_MS,
        START_AWAIT_MS, STOP_GRACE_MS, SessionStopGuard, SessionStopMarker, TASK_CONFIRM_READS,
        TASK_FULL_LISTINGS, TASK_NEW_ID_PARSES, acquire_control_lock, await_start_response,
        execute_teardown_with_timeout, handle_start_request, probe_control,
        probe_or_signal_control, reap_session_tasks_with_wait, request_control_stop,
        request_task_cancel_sentinel, rescan_owned_tasks, session_logs_dir,
        stop_session_record_with_wait, take_task_start_response,
        wait_for_control_release_with_timeout,
    };
    use super::control::{
        POLL_INTERVAL_MS, acquire_admission_lock, current_baton_exe, execute_status, execute_stop,
        execute_task_cancel, execute_task_status, execute_teardown, io_err, is_session_alive,
        run_service, serve_argv, submit_start_request, submit_task_start_request,
    };
    #[cfg(all(test, target_os = "linux"))]
    use super::records::mark_task_start_ack;
    #[cfg(all(test, target_os = "linux"))]
    use super::records::task_start_rollback_exists;
    use super::records::{
        SessionRecord, read_task_record, write_session_record, write_task_record,
    };
    #[cfg(test)]
    use super::records::{
        StartResponse, TaskStartResponse, list_session_records, list_task_records,
        mark_task_start_rollback, read_session_record, remove_session_record, responses_dir,
        sessions_dir, task_start_ack_exists, task_start_response_boundary_exists,
    };
    #[cfg(test)]
    use super::records::{
        remove_task_record, session_record_path, task_logs_dir, task_processing_dir,
        task_record_path, task_requests_dir, task_responses_dir, task_start_ack_path,
        task_start_response_claim_path, task_start_response_path, task_start_rollback_dir,
        write_task_start_response,
    };
    #[cfg(all(test, target_os = "linux"))]
    use super::task_tick::task_cancel_sentinel_path;
    use super::task_tick::{
        self, Liveness, REHYDRATED_LIVENESS_CACHE_MS, ServicePlatform, TaskLivenessMode,
        TaskLivenessRefresh, TerminationSignal, liveness_sample_is_fresh,
    };
    #[cfg(test)]
    use super::task_tick::{
        DEFAULT_TASK_RETENTION_MS, RunningTask as SharedRunningTask, deliver_task_event,
        finalize_task, tick_one_task,
    };
    use super::*;
    #[cfg(test)]
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use crate::mailbox;
    #[cfg(test)]
    use crate::task::{Clock, FakeClock, TaskAdmissionPhase, TaskCallback, TaskEventKind};
    use crate::task::{SystemClock, TaskRecord, TaskSpec, TaskState};

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

    #[cfg(test)]
    thread_local! {
        static PROCESS_PROBE_COUNT: Cell<u64> = const { Cell::new(0) };
    }

    #[cfg(test)]
    fn note_process_probe() {
        PROCESS_PROBE_COUNT.with(|count| count.set(count.get() + 1));
    }

    #[cfg(test)]
    fn reset_process_probe_count() {
        PROCESS_PROBE_COUNT.with(|count| count.set(0));
    }

    #[cfg(test)]
    fn process_probe_count() -> u64 {
        PROCESS_PROBE_COUNT.with(Cell::get)
    }

    #[cfg(test)]
    thread_local! {
        static GROUP_SCAN_COUNT: Cell<u64> = const { Cell::new(0) };
    }

    #[cfg(test)]
    fn note_group_scan() {
        GROUP_SCAN_COUNT.with(|count| count.set(count.get() + 1));
    }

    #[cfg(test)]
    fn reset_group_scan_count() {
        GROUP_SCAN_COUNT.with(|count| count.set(0));
    }

    #[cfg(test)]
    fn group_scan_count() -> u64 {
        GROUP_SCAN_COUNT.with(Cell::get)
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

    /// Platform-owned liveness state. Linux caches only its expensive group
    /// scan; other Unix hosts cache the complete rehydrated execution sample.
    #[allow(dead_code)]
    #[derive(Default)]
    struct UnixTaskLivenessCache {
        #[cfg(target_os = "linux")]
        group: Option<(u64, Liveness, Instant)>,
        #[cfg(not(target_os = "linux"))]
        rehydrated: Option<(u64, Liveness, Instant)>,
    }

    #[allow(dead_code)]
    struct UnixServicePlatform;

    #[allow(dead_code)]
    impl ServicePlatform for UnixServicePlatform {
        type SessionHandle = ();
        type TaskHandle = ();
        type TaskLivenessCache = UnixTaskLivenessCache;

        fn spawn_session(
            spec: &SessionSpec,
            stderr_path: &Path,
        ) -> Result<(Child, Self::SessionHandle)> {
            spawn_serve_child(spec, stderr_path).map(|child| (child, ()))
        }

        fn spawn_task(
            spec: &TaskSpec,
            stdout_path: &Path,
            stderr_path: &Path,
        ) -> Result<(Child, Self::TaskHandle)> {
            spawn_task_child(spec, stdout_path, stderr_path).map(|child| (child, ()))
        }

        fn task_handle_identity(_handle: &Self::TaskHandle) -> Option<String> {
            None
        }

        fn abort_uncommitted_spawn(pid: u32, _handle: &Self::TaskHandle) -> Result<()> {
            signal_group(pid, libc::SIGKILL)
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
            task_execution_liveness(record)
        }

        fn task_liveness_for_tick(
            record: &TaskRecord,
            _owner: Option<&Self::TaskHandle>,
            mode: TaskLivenessMode,
            cache: &mut Self::TaskLivenessCache,
            now_ms: u64,
            refresh: TaskLivenessRefresh,
        ) -> Liveness {
            #[cfg(target_os = "linux")]
            {
                let force_refresh = matches!(refresh, TaskLivenessRefresh::Forced);
                let (cached_group, refresh_group) = match cache.group {
                    Some((checked_ms, _, checked_at))
                        if !force_refresh
                            && liveness_sample_is_fresh(checked_ms, checked_at, now_ms) =>
                    {
                        (
                            Some(cache.group.expect("group liveness cache populated").1),
                            false,
                        )
                    }
                    _ => (None, true),
                };
                let (liveness, used_group_liveness) = match mode {
                    TaskLivenessMode::Owned {
                        leader_exited: true,
                    } => task_group_liveness_with_cached_sample(record.pid, cached_group),
                    TaskLivenessMode::Owned {
                        leader_exited: false,
                    }
                    | TaskLivenessMode::Rehydrated => {
                        let direct_probe = process_probe(record.pid);
                        task_execution_liveness_from_probe(record, &direct_probe, cached_group)
                    }
                };
                if used_group_liveness && refresh_group {
                    cache.group = Some((now_ms, liveness, Instant::now()));
                }
                liveness
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = mode;
                let force_refresh = matches!(refresh, TaskLivenessRefresh::Forced);
                let refresh_sample = force_refresh
                    || !cache
                        .rehydrated
                        .map(|(checked_ms, _, checked_at)| {
                            liveness_sample_is_fresh(checked_ms, checked_at, now_ms)
                        })
                        .unwrap_or(false);
                if refresh_sample {
                    cache.rehydrated =
                        Some((now_ms, task_execution_liveness(record), Instant::now()));
                }
                cache
                    .rehydrated
                    .expect("rehydrated liveness cache populated")
                    .1
            }
        }

        fn terminate_session(
            record: &SessionRecord,
            signal: TerminationSignal,
            force: bool,
        ) -> Result<()> {
            let _ = force;
            signal_group(record.pid, unix_signal(signal))
        }

        fn terminate_task(
            record: &TaskRecord,
            signal: TerminationSignal,
            force: bool,
        ) -> Result<()> {
            let _ = force;
            signal_group(record.pid, unix_signal(signal))
        }

        fn terminate_owned_task(
            owner: Option<&Self::TaskHandle>,
            record: &TaskRecord,
            signal: TerminationSignal,
            force: bool,
        ) -> Result<()> {
            let _ = (owner, force);
            signal_group(record.pid, unix_signal(signal))
        }

        fn pid_is_gone(pid: u32) -> bool {
            matches!(process_probe(pid), ProbeResult::Gone)
        }

        fn unresolved_task_is_gone(
            _control: &Path,
            _id: &str,
            _record: &TaskRecord,
            _term_sent_at_ms: Option<u64>,
        ) -> Result<bool> {
            Ok(false)
        }

        fn rehydrate_task(_record: &TaskRecord) -> Result<Option<Self::TaskHandle>> {
            Ok(None)
        }

        fn upgrade_legacy_task_record(control: &Path, record: &mut TaskRecord) -> Result<()> {
            upgrade_legacy_task_record(control, record)
        }

        fn escalate_task_to_death(record: &TaskRecord, grace_ms: u64) -> Liveness {
            let mut liveness = task_execution_liveness_after_retry(record, grace_ms);
            if liveness == Liveness::Live {
                let _ = signal_group(record.pid, libc::SIGTERM);
                wait_while_task_alive(record, grace_ms);
                liveness = task_execution_liveness_after_retry(record, grace_ms);
                if liveness == Liveness::Live {
                    let _ = signal_group(record.pid, libc::SIGKILL);
                    wait_while_task_alive(record, grace_ms);
                    liveness = task_execution_liveness_after_retry(record, grace_ms);
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
            true
        }
    }

    fn unix_signal(signal: TerminationSignal) -> libc::c_int {
        match signal {
            TerminationSignal::Term => libc::SIGTERM,
            TerminationSignal::Kill => libc::SIGKILL,
        }
    }

    #[cfg(test)]
    type RunningTask = SharedRunningTask<UnixServicePlatform>;
    #[cfg(test)]
    type TaskTick = task_tick::TaskTick;

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
                run_service::<UnixServicePlatform>(&control, task_retention_ms, out)
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
                execute_task_status::<UnixServicePlatform>(&control, task.as_deref(), out)
            }
            TaskCommand::Cancel { control, task } => {
                let control = crate::roles::resolve_control_dir(control)?;
                execute_task_cancel(&control, &task, out)
            }
        }
    }

    /// Spawns `baton serve` for `spec` as its own process-group leader
    /// (`pgid == pid`), detached from this process's stdio except for durable
    /// stderr capture, and returns the live [`Child`] without waiting on it —
    /// `Run`'s loop reaps it later.
    pub(super) fn spawn_serve_child(spec: &SessionSpec, stderr_path: &Path) -> Result<Child> {
        let exe = current_baton_exe()?;
        let mut command = Command::new(&exe);
        command.args(serve_argv(spec));
        command.stdin(Stdio::null());
        command.stdout(Stdio::null());
        let stderr_file = File::create(stderr_path)
            .map_err(|err| BatonError::Io(format!("could not create {stderr_path:?}: {err}")))?;
        command.stderr(Stdio::from(stderr_file));
        // A fresh process group (not this service's own) so a later
        // `libc::kill(-pid, sig)` escalation reaches exactly this session's
        // `serve` process and its `agent-cmd` grandchild, nothing else this
        // service manages. Safe and stable — deliberately not
        // `pre_exec(setsid)`, which would require `unsafe`.
        command.process_group(0);
        command
            .spawn()
            .map_err(|err| BatonError::Io(format!("could not spawn baton serve: {err}")))
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
        // `libc::kill(-pid, sig)` (max-duration enforcement or `baton task
        // cancel`) reaches the task's whole subtree, not just this direct
        // child.
        command.process_group(0);
        command.spawn().map_err(|err| {
            BatonError::Io(format!(
                "could not spawn task command {:?}: {err}",
                spec.command
            ))
        })
    }

    // -- Liveness ---------------------------------------------------------

    /// Converts a canonical UTC `ps lstart` value into Unix epoch seconds.
    /// The weekday is intentionally ignored: `ps` supplies it for humans, but
    /// the calendar date and time are the identity-bearing fields.
    #[cfg(any(not(target_os = "linux"), test))]
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
    #[cfg(any(not(target_os = "linux"), test))]
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

    /// Parses the platform process-state token used by both the Linux
    /// `/proc` and macOS `ps` probes. Unknown states are incomplete evidence,
    /// not proof that a process is live or drained.
    fn parse_process_state(state: &str) -> Option<bool> {
        let bytes = state.as_bytes();
        let first = *bytes.first()?;
        #[cfg(target_os = "linux")]
        {
            if !matches!(
                first,
                b'R' | b'S'
                    | b'D'
                    | b'T'
                    | b't'
                    | b'Z'
                    | b'X'
                    | b'x'
                    | b'K'
                    | b'W'
                    | b'P'
                    | b'I'
                    | b'U'
            ) || bytes.len() != 1
            {
                return None;
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            if !matches!(first, b'R' | b'S' | b'I' | b'U' | b'T' | b'W' | b'Z')
                || bytes[1..]
                    .iter()
                    .any(|byte| !matches!(byte, b'<' | b'>' | b'N' | b'L' | b's' | b'l' | b'+'))
            {
                return None;
            }
        }
        Some(first == b'Z')
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, PartialEq, Eq)]
    struct ProcessProbe {
        state: String,
        start_key: String,
    }

    #[cfg(target_os = "linux")]
    impl ProcessProbe {
        fn is_zombie(&self) -> bool {
            self.state.starts_with('Z')
        }
    }

    /// Parses `/proc/<pid>/stat`; the executable name is `(comm)` and may
    /// itself contain `)` or whitespace, so fields are counted from the last
    /// `)` rather than split naively.
    #[cfg(target_os = "linux")]
    fn parse_linux_process_probe(stat: &str) -> Option<ProcessProbe> {
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        // `fields[0]` is field 3 (state) overall; starttime is field 22
        // overall, i.e. `fields[19]`.
        let state = fields.first()?;
        parse_process_state(state)?;
        Some(ProcessProbe {
            state: state.to_string(),
            start_key: fields.get(19)?.to_string(),
        })
    }

    #[cfg(target_os = "linux")]
    fn process_probe(pid: u32) -> ProbeResult<ProcessProbe> {
        if pid <= 1 {
            return ProbeResult::Gone;
        }
        #[cfg(test)]
        note_process_probe();
        match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => parse_linux_process_probe(&stat)
                .map(ProbeResult::Present)
                .unwrap_or(ProbeResult::Unreadable),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => ProbeResult::Gone,
            Err(_) => ProbeResult::Unreadable,
        }
    }

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
    pub(super) fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
        match process_probe(pid) {
            ProbeResult::Present(probe) if !probe.is_zombie() => (Some(probe.start_key), None),
            _ => (None, None),
        }
    }

    /// Whether a freshly-spawned child's start key is trustworthy enough to
    /// persist. A missing key means the child was already gone or a zombie
    /// microseconds after `spawn()` — fail closed as a spawn failure.
    #[cfg(target_os = "linux")]
    pub(super) fn spawn_start_key_ok(
        started_at: &Option<String>,
        _start_epoch_secs: &Option<i64>,
    ) -> bool {
        started_at.is_some()
    }

    #[cfg(target_os = "linux")]
    fn linux_session_argv_matches(actual: &[String], spec: &SessionSpec) -> bool {
        let expected = serve_argv(spec);
        actual.len() >= expected.len() && actual.ends_with(&expected)
    }

    #[cfg(target_os = "linux")]
    fn linux_task_argv_matches(actual: &[String], record: &TaskRecord) -> bool {
        let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
        expected.push(record.spec.command.clone());
        expected.extend(record.spec.args.iter().cloned());
        actual == expected
    }

    #[cfg(target_os = "linux")]
    pub(super) fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
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

    #[cfg(target_os = "linux")]
    fn task_liveness_from_probe(
        record: &TaskRecord,
        probe: &ProbeResult<ProcessProbe>,
    ) -> (Liveness, Option<i64>) {
        match probe {
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

    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    fn task_liveness(record: &TaskRecord) -> (Liveness, Option<i64>) {
        task_liveness_from_probe(record, &process_probe(record.pid))
    }

    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        task_liveness(record).0
    }

    /// A non-Linux Unix `ps` sample. macOS has no `/proc`, so `state`,
    /// `lstart`, and the untruncated command line are the available process
    /// corroborators. Every probe pins the locale and time zone so the
    /// canonical key is independent of the supervisor/client environment.
    #[cfg(not(target_os = "linux"))]
    #[derive(Debug, PartialEq, Eq)]
    struct ProcessProbe {
        state: String,
        start_key: String,
        start_epoch_secs: Option<i64>,
        command: String,
    }

    #[cfg(not(target_os = "linux"))]
    impl ProcessProbe {
        fn is_zombie(&self) -> bool {
            self.state.starts_with('Z')
        }
    }

    /// Parses one `ps -p <pid> -o state=,lstart=,command=` row. The five
    /// lstart fields are fixed by the C locale; the remaining text is the
    /// command line and is whitespace-normalized for comparison.
    #[cfg(not(target_os = "linux"))]
    fn parse_process_probe(output: &str) -> Option<ProcessProbe> {
        let fields: Vec<&str> = output.split_whitespace().collect();
        if fields.len() < 6 {
            return None;
        }
        let state = fields[0].to_string();
        parse_process_state(&state)?;
        let start_key = fields[1..6].join(" ");
        Some(ProcessProbe {
            state,
            start_epoch_secs: parse_lstart_epoch_secs(&start_key),
            start_key,
            command: fields[6..].join(" "),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn process_probe(pid: u32) -> ProbeResult<ProcessProbe> {
        if pid <= 1 {
            return ProbeResult::Gone;
        }
        #[cfg(test)]
        note_process_probe();
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

    #[cfg(not(target_os = "linux"))]
    fn start_identity_from_probe(probe: &ProcessProbe) -> (Option<String>, Option<i64>) {
        if probe.is_zombie() {
            (None, None)
        } else {
            (Some(probe.start_key.clone()), probe.start_epoch_secs)
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>) {
        match process_probe(pid) {
            ProbeResult::Present(probe) => start_identity_from_probe(&probe),
            _ => (None, None),
        }
    }

    /// A missing start key after spawn means the process was already gone or
    /// a zombie, so fail closed rather than persisting an uncorroborated PID.
    #[cfg(not(target_os = "linux"))]
    pub(super) fn spawn_start_key_ok(
        started_at: &Option<String>,
        start_epoch_secs: &Option<i64>,
    ) -> bool {
        started_at.is_some() && start_epoch_secs.is_some()
    }

    #[cfg(not(target_os = "linux"))]
    fn normalize_process_text(value: &str) -> String {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[cfg(not(target_os = "linux"))]
    fn session_argv_matches(command: &str, spec: &SessionSpec) -> bool {
        let command = normalize_process_text(command);
        let expected = serve_argv(spec).join(" ");
        if expected.is_empty() {
            return false;
        }
        command == expected || command.ends_with(&format!(" {expected}"))
    }

    #[cfg(not(target_os = "linux"))]
    fn task_argv_matches(command: &str, record: &TaskRecord) -> bool {
        let mut expected = Vec::with_capacity(record.spec.args.len() + 1);
        expected.push(record.spec.command.as_str());
        expected.extend(record.spec.args.iter().map(String::as_str));
        normalize_process_text(command) == normalize_process_text(&expected.join(" "))
    }

    #[cfg(not(target_os = "linux"))]
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

    #[cfg(not(target_os = "linux"))]
    pub(super) fn session_liveness(record: &SessionRecord) -> (Liveness, Option<i64>) {
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

    #[cfg(not(target_os = "linux"))]
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

    #[cfg(not(target_os = "linux"))]
    fn is_task_alive(record: &TaskRecord) -> Liveness {
        task_liveness(record).0
    }

    /// Probes whether the task's process group still has any members. The
    /// group is the Unix ownership boundary, so a reaped leader does not mean
    /// the task is gone while a descendant remains in that group. `EPERM`
    /// still proves that a member exists; every other unexpected probe error
    /// is unresolved and therefore fails closed.
    fn task_group_liveness(pid: u32) -> Liveness {
        if pid <= 1 || pid > i32::MAX as u32 {
            return Liveness::Dead;
        }
        let result = unsafe { libc::kill(-(pid as libc::pid_t), 0) };
        match result {
            0 => group_scan_with_absence_recheck(pid),
            _ => match std::io::Error::last_os_error().raw_os_error() {
                Some(libc::ESRCH) => Liveness::Dead,
                Some(libc::EPERM) => group_scan_with_absence_recheck(pid),
                _ => Liveness::Unresolved,
            },
        }
    }

    /// A process can disappear while a strict group scan is reading the
    /// system process table. If a second kernel-level probe now proves that
    /// the group itself is gone, that is complete absence evidence; otherwise
    /// preserve `Unresolved` rather than turning an incomplete scan into
    /// `Dead`.
    fn group_scan_with_absence_recheck(pgid: u32) -> Liveness {
        let liveness = process_group_member_liveness(pgid);
        if liveness == Liveness::Unresolved {
            let result = unsafe { libc::kill(-(pgid as libc::pid_t), 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return Liveness::Dead;
            }
        }
        liveness
    }

    /// Confirms whether a process-group probe that succeeded with `kill(0)`
    /// has a non-zombie member. `kill(0)` also succeeds for zombie-only groups,
    /// which otherwise leaves an already-reaped direct child looking live
    /// forever during cleanup.
    #[cfg(target_os = "linux")]
    fn process_group_member_liveness(pgid: u32) -> Liveness {
        #[cfg(test)]
        note_group_scan();
        let entries = match fs::read_dir("/proc") {
            Ok(entries) => entries,
            Err(_) => return Liveness::Unresolved,
        };
        let mut found_member = false;
        let mut found_live_member = false;
        for entry in entries {
            let Ok(entry) = entry else {
                return Liveness::Unresolved;
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Liveness::Unresolved;
            };
            if !name.bytes().all(|byte| byte.is_ascii_digit()) {
                continue;
            }
            let Ok(pid) = name.parse::<u32>() else {
                return Liveness::Unresolved;
            };
            let stat = match fs::read_to_string(entry.path().join("stat")) {
                Ok(stat) => stat,
                // The pid vanished between the directory yield and this read.
                // A process that no longer exists cannot be a live member, so
                // skipping it keeps the scan complete instead of letting
                // unrelated host churn make every scan unresolved. It can only
                // leave `found_member` unset, and the no-member path still
                // needs the kernel `ESRCH` recheck before reporting `Dead`.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                // Present but genuinely unreadable: fail closed.
                Err(_) => return Liveness::Unresolved,
            };
            let Some((stat_pid, is_zombie, current_pgid)) = parse_linux_process_group_member(&stat)
            else {
                return Liveness::Unresolved;
            };
            if stat_pid != pid {
                return Liveness::Unresolved;
            }
            if current_pgid != pgid {
                continue;
            }
            found_member = true;
            if !is_zombie {
                found_live_member = true;
            }
        }
        if found_live_member {
            Liveness::Live
        } else if found_member {
            Liveness::Dead
        } else {
            // The group existed for the kill(0) sample but no member was
            // observable in the scan; retry rather than finalizing on a race.
            Liveness::Unresolved
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn process_group_member_liveness(pgid: u32) -> Liveness {
        #[cfg(test)]
        note_group_scan();
        let output = match Command::new("ps")
            .args(["-ww", "-axo", "pid=,pgid=,state="])
            .env("LC_ALL", "C")
            .env("LC_TIME", "C")
            .env("TZ", "UTC")
            .output()
        {
            Ok(output) if output.status.success() => output,
            _ => return Liveness::Unresolved,
        };
        if !output.stderr.is_empty() {
            return Liveness::Unresolved;
        }
        let Ok(output) = std::str::from_utf8(&output.stdout) else {
            return Liveness::Unresolved;
        };
        let mut found_member = false;
        let mut found_live_member = false;
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() != 3 {
                return Liveness::Unresolved;
            }
            let Some(pid) = fields[0].parse::<u32>().ok() else {
                return Liveness::Unresolved;
            };
            let Some(current_pgid) = fields[1].parse::<u32>().ok() else {
                return Liveness::Unresolved;
            };
            if pid == 0 || current_pgid == 0 || fields[2].is_empty() {
                return Liveness::Unresolved;
            }
            let Some(is_zombie) = parse_process_state(fields[2]) else {
                return Liveness::Unresolved;
            };
            if current_pgid != pgid {
                continue;
            }
            found_member = true;
            if !is_zombie {
                found_live_member = true;
            }
        }
        if found_live_member {
            Liveness::Live
        } else if found_member {
            Liveness::Dead
        } else {
            Liveness::Unresolved
        }
    }

    #[cfg(target_os = "linux")]
    fn task_group_liveness_with_cached_sample(
        pid: u32,
        cached_liveness: Option<Liveness>,
    ) -> (Liveness, bool) {
        if pid <= 1 || pid > i32::MAX as u32 {
            return (Liveness::Dead, false);
        }
        let result = unsafe { libc::kill(-(pid as libc::pid_t), 0) };
        match result {
            0 => (
                cached_liveness.unwrap_or_else(|| task_group_liveness(pid)),
                true,
            ),
            _ => match std::io::Error::last_os_error().raw_os_error() {
                Some(libc::ESRCH) => (Liveness::Dead, false),
                Some(libc::EPERM) => (
                    cached_liveness.unwrap_or_else(|| task_group_liveness(pid)),
                    true,
                ),
                _ => (Liveness::Unresolved, false),
            },
        }
    }

    #[cfg(target_os = "linux")]
    fn parse_linux_process_group_member(stat: &str) -> Option<(u32, bool, u32)> {
        let pid = stat.split_once(' ')?.0.parse::<u32>().ok()?;
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        let state = fields.first()?;
        let is_zombie = parse_process_state(state)?;
        let pgid = fields.get(2)?.parse::<u32>().ok()?;
        Some((pid, is_zombie, pgid))
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TaskLeaderExit {
        Gone,
        MatchingZombie,
        Mismatched,
        NotExited,
        Unresolved,
    }

    #[cfg(target_os = "linux")]
    fn zombie_identity_matches(record: &TaskRecord, probe: &ProcessProbe) -> Option<bool> {
        record
            .started_at
            .as_deref()
            .map(|recorded| recorded == probe.start_key)
    }

    #[cfg(not(target_os = "linux"))]
    fn zombie_identity_matches(record: &TaskRecord, probe: &ProcessProbe) -> Option<bool> {
        if let Some(recorded) = record.start_epoch_secs {
            probe.start_epoch_secs.map(|current| current == recorded)
        } else {
            record
                .started_at
                .as_deref()
                .map(|recorded| recorded == probe.start_key)
        }
    }

    /// Returns whether the recorded direct PID is definitely gone (or is a
    /// zombie that cannot be the live task). A zombie may fall through to a
    /// process-group probe only after its start identity matches the durable
    /// record. A mismatched or legacy identity is never allowed to use the
    /// numeric PID as a group id.
    #[cfg(target_os = "linux")]
    fn task_leader_exited_from_probe(
        record: &TaskRecord,
        probe: &ProbeResult<ProcessProbe>,
    ) -> TaskLeaderExit {
        match probe {
            ProbeResult::Gone => TaskLeaderExit::Gone,
            ProbeResult::Present(probe) if probe.is_zombie() => {
                match zombie_identity_matches(record, probe) {
                    Some(true) => TaskLeaderExit::MatchingZombie,
                    Some(false) => TaskLeaderExit::Mismatched,
                    None => TaskLeaderExit::Unresolved,
                }
            }
            ProbeResult::Present(_) => TaskLeaderExit::NotExited,
            ProbeResult::Unreadable => TaskLeaderExit::Unresolved,
        }
    }

    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    fn task_leader_exited(record: &TaskRecord) -> TaskLeaderExit {
        task_leader_exited_from_probe(record, &process_probe(record.pid))
    }

    #[cfg(not(target_os = "linux"))]
    fn task_leader_exited(record: &TaskRecord) -> TaskLeaderExit {
        match process_probe(record.pid) {
            ProbeResult::Gone => TaskLeaderExit::Gone,
            ProbeResult::Present(probe) if probe.is_zombie() => {
                match zombie_identity_matches(record, &probe) {
                    Some(true) => TaskLeaderExit::MatchingZombie,
                    Some(false) => TaskLeaderExit::Mismatched,
                    None => TaskLeaderExit::Unresolved,
                }
            }
            ProbeResult::Present(_) => TaskLeaderExit::NotExited,
            ProbeResult::Unreadable => TaskLeaderExit::Unresolved,
        }
    }

    /// Combines the direct-PID identity with the Unix process-group boundary.
    /// A rehydrated task has no `Child` handle and its direct leader may have
    /// exited while descendants remain; that group is still live work. An
    /// unresolved group probe is retained rather than finalized or signalled.
    #[cfg(target_os = "linux")]
    fn task_execution_liveness_from_probe(
        record: &TaskRecord,
        direct_probe: &ProbeResult<ProcessProbe>,
        group_liveness: Option<Liveness>,
    ) -> (Liveness, bool) {
        match task_liveness_from_probe(record, direct_probe).0 {
            Liveness::Dead => match task_leader_exited_from_probe(record, direct_probe) {
                TaskLeaderExit::Gone | TaskLeaderExit::MatchingZombie => {
                    task_group_liveness_with_cached_sample(record.pid, group_liveness)
                }
                TaskLeaderExit::Mismatched | TaskLeaderExit::Unresolved => {
                    (Liveness::Unresolved, false)
                }
                TaskLeaderExit::NotExited => (Liveness::Dead, false),
            },
            liveness => (liveness, false),
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn task_execution_liveness(record: &TaskRecord) -> Liveness {
        task_execution_liveness_from_probe(record, &process_probe(record.pid), None).0
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn task_execution_liveness(record: &TaskRecord) -> Liveness {
        match is_task_alive(record) {
            Liveness::Dead => match task_leader_exited(record) {
                TaskLeaderExit::Gone | TaskLeaderExit::MatchingZombie => {
                    task_group_liveness(record.pid)
                }
                TaskLeaderExit::Mismatched | TaskLeaderExit::Unresolved => Liveness::Unresolved,
                TaskLeaderExit::NotExited => Liveness::Dead,
            },
            liveness => liveness,
        }
    }

    /// Persists the canonical epoch after a legacy macOS record is rescued by
    /// the fallback ladder. Callers must hold the admission lock; status and
    /// supervisor tick paths intentionally remain read-only.
    #[cfg(target_os = "macos")]
    pub(super) fn upgrade_legacy_session_record(
        control: &Path,
        record: &mut SessionRecord,
    ) -> Result<()> {
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
    pub(super) fn upgrade_legacy_session_record(
        _control: &Path,
        _record: &mut SessionRecord,
    ) -> Result<()> {
        Ok(())
    }

    /// Persists the canonical epoch after a legacy macOS task record is
    /// rescued by the fallback ladder. Callers must hold the admission lock;
    /// the supervisor's rehydration/tick paths deliberately do not rewrite.
    #[cfg(target_os = "macos")]
    pub(super) fn upgrade_legacy_task_record(
        control: &Path,
        record: &mut TaskRecord,
    ) -> Result<()> {
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
    pub(super) fn upgrade_legacy_task_record(
        _control: &Path,
        _record: &mut TaskRecord,
    ) -> Result<()> {
        Ok(())
    }

    /// Sends `sig` to the process **group** led by `pid`.
    ///
    /// A process-group id is represented by a negative PID for `kill(2)`.
    /// Invalid low or out-of-range PIDs are ignored because they cannot
    /// identify one of the process groups owned by this service. A group that
    /// has already exited is also treated as success, matching the old
    /// command's ignored exit status at every call site.
    pub(super) fn signal_group(pid: u32, sig: libc::c_int) -> Result<()> {
        if pid <= 1 || pid > i32::MAX as u32 {
            return Ok(());
        }
        let result = unsafe { libc::kill(-(pid as libc::pid_t), sig) };
        if result == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(BatonError::Io(format!(
                "could not signal process group -{pid}: {err}"
            )))
        }
    }

    /// Caches only the probes whose cost is amplified by a grace wait. The
    /// cache is deliberately created by each wait invocation: a sample from
    /// one cleanup ladder must not authorize a later signal in another one.
    struct GraceWaitLivenessCache {
        #[cfg(target_os = "linux")]
        group_liveness: Option<(Instant, Liveness)>,
        #[cfg(not(target_os = "linux"))]
        liveness: Option<(Instant, Liveness)>,
    }

    impl GraceWaitLivenessCache {
        fn new() -> Self {
            Self {
                #[cfg(target_os = "linux")]
                group_liveness: None,
                #[cfg(not(target_os = "linux"))]
                liveness: None,
            }
        }

        #[cfg(target_os = "linux")]
        fn task_liveness(&mut self, record: &TaskRecord) -> Liveness {
            let direct_probe = process_probe(record.pid);
            let cached_group_liveness = self
                .group_liveness
                .filter(|(checked_at, _)| {
                    checked_at.elapsed() < Duration::from_millis(REHYDRATED_LIVENESS_CACHE_MS)
                })
                .map(|(_, liveness)| liveness);
            let refresh_group_liveness = cached_group_liveness.is_none();
            let (liveness, used_group_liveness) =
                task_execution_liveness_from_probe(record, &direct_probe, cached_group_liveness);
            if used_group_liveness && refresh_group_liveness {
                self.group_liveness = Some((Instant::now(), liveness));
            }
            liveness
        }

        #[cfg(not(target_os = "linux"))]
        fn task_liveness(&mut self, record: &TaskRecord) -> Liveness {
            self.cached(|| task_execution_liveness(record))
        }

        #[cfg(target_os = "linux")]
        fn session_liveness(&mut self, record: &SessionRecord) -> Liveness {
            // Linux's session probe is a cheap direct /proc read (plus a
            // legacy argv read), so retain its per-poll death detection.
            is_session_alive(record)
        }

        #[cfg(not(target_os = "linux"))]
        fn session_liveness(&mut self, record: &SessionRecord) -> Liveness {
            self.cached(|| is_session_alive(record))
        }

        #[cfg(not(target_os = "linux"))]
        fn cached(&mut self, probe: impl FnOnce() -> Liveness) -> Liveness {
            if let Some((checked_at, liveness)) = self.liveness
                && checked_at.elapsed() < Duration::from_millis(REHYDRATED_LIVENESS_CACHE_MS)
            {
                return liveness;
            }
            let liveness = probe();
            self.liveness = Some((Instant::now(), liveness));
            liveness
        }
    }

    pub(super) fn wait_while_alive(record: &SessionRecord, grace_ms: u64) {
        let mut cache = GraceWaitLivenessCache::new();
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        while cache.session_liveness(record) != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    pub(super) fn wait_while_task_alive(record: &TaskRecord, grace_ms: u64) {
        let mut cache = GraceWaitLivenessCache::new();
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        while cache.task_liveness(record) != Liveness::Dead && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    /// Retries an incomplete Unix process-group scan for one bounded grace
    /// period. An unresolved result is never treated as permission to signal;
    /// this only gives a transient `/proc` or `ps` snapshot a chance to become
    /// complete before a cancellation or escalation decision is made.
    pub(super) fn task_execution_liveness_after_retry(
        record: &TaskRecord,
        grace_ms: u64,
    ) -> Liveness {
        // Start empty so a Live/Dead result returned to the caller is never
        // authorized by an earlier grace wait's cached sample.
        let mut cache = GraceWaitLivenessCache::new();
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        loop {
            let liveness = cache.task_liveness(record);
            if liveness != Liveness::Unresolved || Instant::now() >= deadline {
                return liveness;
            }
            std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::atomic::{AtomicU64, Ordering};

        use super::*;

        static SEQ: AtomicU64 = AtomicU64::new(0);

        // Serializes every test in this module that either holds the
        // control-plane flock directly or forks a real child process
        // (`spawn_task_child`/`Mailbox::open`'s own lock) against the rest of
        // the crate's forks. The guard is crate-wide rather than module-local
        // because the fd table it protects is process-wide: a spawn from any
        // other module's tests is just as capable of pinning this module's
        // locks open. See `crate::test_support`.
        //
        // "Forks a real child" includes the *indirect* forks a liveness check
        // performs off Linux: `process_probe` shells out to `ps`, so
        // `execute_status`/`execute_stop`/
        // `execute_teardown`/`reconcile_task_admissions` and friends fork on
        // macOS even when the test itself never spawns anything. Every test
        // that can reach one of those takes the guard.
        use crate::test_support::serialize_forks_and_locks;

        #[cfg(not(target_os = "linux"))]
        struct ChildCleanup {
            child: Option<Child>,
        }

        #[cfg(not(target_os = "linux"))]
        impl ChildCleanup {
            fn new(child: Child) -> Self {
                Self { child: Some(child) }
            }

            fn child(&self) -> &Child {
                self.child.as_ref().expect("child fixture exists")
            }

            fn id(&self) -> u32 {
                self.child().id()
            }

            fn reap(&mut self) {
                if let Some(mut child) = self.child.take() {
                    let _ = signal_group(child.id(), libc::SIGKILL);
                    let _ = child.wait();
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        impl Drop for ChildCleanup {
            fn drop(&mut self) {
                self.reap();
            }
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
        /// request-file dance or the infinite `run_service` loop. Retention
        /// is zeroed so callers exercising `reap_task_until_finished`'s
        /// bounded real-time loop see an immediate reap on terminal
        /// delivery, as tests unrelated to retention expect; tests that
        /// exercise retention itself build their own `RunningTask` with an
        /// explicit `.with_retention_ms(...)`.
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
                job: None,
                started_ms: Some(started_ms),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(dir, &record).expect("write task record");
            RunningTask::new(record, Some(child), None, started_ms).with_retention_ms(0)
        }

        /// Builds a terminal task without a live child so callback delivery
        /// retry behavior can be driven entirely by [`FakeClock`].
        fn terminal_running_task(
            dir: &Path,
            id: &str,
            callback_inbox: &Path,
            clock: &dyn Clock,
        ) -> RunningTask {
            let spec = task_spec(
                "svc-1",
                "true",
                vec![],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let record = TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid: 0,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: Some(clock.now_ms()),
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(10),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(dir, &record).expect("write terminal task record");
            RunningTask::new(record, None, None, clock.now_ms())
        }

        #[test]
        fn tick_one_task_drops_when_durable_record_is_removed() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-record-removed");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running =
                terminal_running_task(&dir.path, "task-record-removed", &callback_inbox, &clock);
            remove_task_record(&dir.path, "task-record-removed").expect("remove task record");

            assert!(matches!(
                tick_one_task(&dir.path, "task-record-removed", &mut running, &clock)
                    .expect("tick removed task"),
                TaskTick::Finished
            ));
        }

        #[test]
        fn tick_one_task_ignores_malformed_durable_record_at_start() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-record-malformed");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running =
                terminal_running_task(&dir.path, "task-record-malformed", &callback_inbox, &clock);
            let path =
                task_record_path(&dir.path, "task-record-malformed").expect("task record path");
            fs::write(path, "not json").expect("write malformed task record");

            assert!(matches!(
                tick_one_task(&dir.path, "task-record-malformed", &mut running, &clock)
                    .expect("tick malformed task"),
                TaskTick::StillRunning
            ));
        }

        #[test]
        fn tick_one_task_reports_record_probe_io_error() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-record-probe-error");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running = terminal_running_task(
                &dir.path,
                "task-record-probe-error",
                &callback_inbox,
                &clock,
            );
            fs::remove_dir_all(dir.path.join("tasks")).expect("remove task record directory");
            fs::write(dir.path.join("tasks"), "not a directory")
                .expect("replace tasks directory with a file");

            match tick_one_task(&dir.path, "task-record-probe-error", &mut running, &clock) {
                Err(BatonError::Io(message)) => assert!(message.contains("could not probe")),
                other => panic!("expected record probe I/O error, got {other:?}"),
            }
        }

        /// A child spawned while `Run` owns the control lock must not retain
        /// that lock after the supervisor exits. Otherwise a killed
        /// supervisor cannot be restarted until every descendant happens to
        /// exit, which breaks crash recovery on macOS.
        #[test]
        fn spawned_task_does_not_retain_control_lock_after_owner_exits() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("lock-inheritance");
            let owner_lock = acquire_control_lock(&dir.path).expect("owner lock");

            let log_dir = task_logs_dir(&dir.path, "lock-child");
            fs::create_dir_all(&log_dir).expect("create child log dir");
            let mut child = spawn_task_child(
                &task_spec(
                    "session",
                    "sh",
                    vec!["-c".to_string(), "sleep 30".to_string()],
                    Vec::new(),
                    60_000,
                    "/tmp/callback",
                ),
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn lock inheritance probe child");

            drop(owner_lock);
            let deadline = Instant::now() + Duration::from_secs(10);
            let replacement_lock = loop {
                match acquire_control_lock(&dir.path) {
                    Ok(lock) => break lock,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(err) => {
                        panic!("descendant must not retain the owner control lock: {err:?}")
                    }
                }
            };
            drop(replacement_lock);

            let _ = child.kill();
            let _ = child.wait();
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
                start_epoch_secs: Some(123456),
                stderr_path: "/tmp/stderr.log".to_string(),
            };
            write_session_record(&dir.path, &record).expect("write");
            let read = read_session_record(&dir.path, "svc-1")
                .expect("read")
                .expect("present");
            assert_eq!(read, record);
        }

        /// A record written before session stderr capture was added remains
        /// readable and reports an empty path rather than failing status or
        /// cleanup.
        #[test]
        fn legacy_session_record_defaults_stderr_path() {
            let dir = TempDir::new("legacy-record");
            let record = SessionRecord {
                id: "svc-legacy".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: 4242,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            let mut json = serde_json::to_value(record).expect("serialize record");
            json.as_object_mut()
                .expect("record object")
                .remove("stderr_path");
            fs::create_dir_all(sessions_dir(&dir.path)).expect("create sessions dir");
            fs::write(
                session_record_path(&dir.path, "svc-legacy").expect("record path"),
                serde_json::to_vec(&json).expect("serialize legacy record"),
            )
            .expect("write legacy record");

            let read = read_session_record(&dir.path, "svc-legacy")
                .expect("read legacy record")
                .expect("legacy record exists");
            assert!(read.stderr_path.is_empty());
        }

        /// A legacy macOS task rescued by its spawn instant is upgraded once
        /// on a lock-holding cleanup path. Session argv rescue is covered by
        /// the real-binary integration fixture.
        #[cfg(target_os = "macos")]
        #[test]
        fn legacy_macos_task_record_upgrades_after_instant_rescue() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("legacy-macos-upgrade");

            let task_specification = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                Vec::new(),
                60_000,
                &dir.path.join("callback").display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-legacy");
            fs::create_dir_all(&log_dir).expect("create task logs");
            let mut task_child = spawn_task_child(
                &task_specification,
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn task child");
            let task_record = TaskRecord {
                id: "task-legacy".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Responded,
                spec: task_specification,
                pid: task_child.id(),
                started_at: Some("legacy-lstart".to_string()),
                start_epoch_secs: None,
                job: None,
                started_ms: Some(SystemClock.now_ms()),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: log_dir.join("stdout.log").display().to_string(),
                stderr_path: log_dir.join("stderr.log").display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &task_record).expect("write legacy task");
            let mut upgraded_task = task_record.clone();
            upgrade_legacy_task_record(&dir.path, &mut upgraded_task).expect("upgrade legacy task");
            assert!(
                upgraded_task.start_epoch_secs.is_some(),
                "instant rescue populates the canonical task epoch"
            );
            assert_eq!(
                read_task_record(&dir.path, "task-legacy")
                    .expect("read upgraded task")
                    .expect("upgraded task exists")
                    .start_epoch_secs,
                upgraded_task.start_epoch_secs
            );

            signal_group(task_child.id(), libc::SIGKILL).expect("kill task child");
            task_child.wait().expect("wait for task child");
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
            let _guard = serialize_forks_and_locks();
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
                        start_epoch_secs: None,
                        stderr_path: String::new(),
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

                let outcome = admission::handle_task_start_request::<UnixServicePlatform>(
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

        /// Non-ascending milestone schedules are rejected from the actual
        /// task-requests/ admission path before owner validation or spawn.
        #[test]
        fn task_start_rejects_non_ascending_milestones_from_request_file() {
            let _guard = serialize_forks_and_locks();
            for (tag, milestones_ms, expected_pair) in [
                (
                    "milestones-descending",
                    vec![5_000, 1_000],
                    "5000 followed by 1000",
                ),
                (
                    "milestones-duplicate",
                    vec![1_000, 1_000],
                    "1000 followed by 1000",
                ),
            ] {
                let dir = TempDir::new(tag);
                let request_id = format!("{tag}-request");
                let spawned_marker = dir.path.join("spawned");
                let request = task_spec(
                    "missing-owner",
                    "sh",
                    vec![
                        "-c".to_string(),
                        format!("touch {}", spawned_marker.display()),
                    ],
                    milestones_ms,
                    10_000,
                    "/tmp/callback",
                );
                let requests = task_requests_dir(&dir.path);
                fs::create_dir_all(&requests).expect("create task requests");
                fs::write(
                    requests.join(mailbox::file_name(&request_id)),
                    serde_json::to_string(&request).expect("serialize task request"),
                )
                .expect("write task request");

                let outcome = admission::process_one_task_request::<UnixServicePlatform>(
                    &dir.path,
                    &FakeClock::new(),
                )
                .expect("ordering rejection is a handled response");
                assert!(outcome.is_none(), "rejected request must not return a task");
                assert!(!spawned_marker.exists(), "rejected request must not spawn");
                assert!(
                    !dir.path.join("tasks").exists(),
                    "rejected request must not create a task record directory"
                );
                assert!(
                    !dir.path.join("task-logs").exists(),
                    "rejected request must not create task logs"
                );
                assert!(
                    !requests.join(mailbox::file_name(&request_id)).exists()
                        && !task_processing_dir(&dir.path)
                            .join(mailbox::file_name(&request_id))
                            .exists(),
                    "rejected request must be removed from both request states"
                );

                let response: TaskStartResponse = serde_json::from_str(
                    &fs::read_to_string(
                        task_responses_dir(&dir.path).join(mailbox::file_name(&request_id)),
                    )
                    .expect("ordering rejection response"),
                )
                .expect("decode ordering rejection response");
                assert!(response.task_id.is_none());
                let expected_error = format!(
                    "task start rejected: --milestone-ms values must be strictly ascending: got {expected_pair}"
                );
                assert_eq!(response.error.as_deref(), Some(expected_error.as_str()));
            }
        }

        /// A task command that cannot be spawned is answered through the
        /// task-start response, so the submitting client fails with the
        /// spawn reason instead of waiting out `START_AWAIT_MS`.
        #[test]
        fn task_start_spawn_failure_answers_with_error_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-spawn-failure");
            let pid = std::process::id();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            write_session_record(
                &dir.path,
                &SessionRecord {
                    id: "svc-live".to_string(),
                    spec: spec("/tmp/in", "/tmp/out"),
                    pid,
                    started_at,
                    start_epoch_secs,
                    stderr_path: String::new(),
                },
            )
            .expect("write live session record");

            let unspawnable = dir.path.join("no-such-binary").display().to_string();
            let spec_path = dir.path.join("task-request.json");
            let task_spec = task_spec(
                "svc-live",
                &unspawnable,
                vec![],
                vec![],
                1_000,
                "/tmp/callback",
            );
            fs::write(
                &spec_path,
                serde_json::to_string(&task_spec).expect("serialize task spec"),
            )
            .expect("write task spec");

            let outcome = admission::handle_task_start_request::<UnixServicePlatform>(
                &dir.path,
                "spawn-fail",
                &spec_path,
                &FakeClock::new(),
            )
            .expect("spawn failure is a handled response, not a request-loop error");
            assert!(outcome.is_none(), "a failed spawn returns no task");
            assert!(
                !dir.path.join("tasks").exists(),
                "a failed spawn leaves no task record"
            );
            assert!(
                fs::read_dir(dir.path.join("task-logs"))
                    .map(|entries| entries.count() == 0)
                    .unwrap_or(true),
                "a failed spawn leaves no orphan task log directory"
            );

            let response_path =
                task_responses_dir(&dir.path).join(mailbox::file_name("spawn-fail"));
            let response: TaskStartResponse =
                serde_json::from_str(&fs::read_to_string(response_path).expect("spawn response"))
                    .expect("decode spawn response");
            assert!(response.task_id.is_none());
            let error = response.error.expect("spawn failure carries an error");
            assert!(
                error.contains("could not spawn task command") && error.contains(&unspawnable),
                "the response names the spawn failure: {error}"
            );
        }

        /// A session admission failure after the request is claimed is
        /// answered through the start response rather than propagated to the
        /// request loop, where it would strand the client on the await bound.
        #[test]
        fn start_request_admission_failure_answers_with_error_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("session-admission-failure");
            // A file where `sessions/` must be a directory, so the record
            // write fails — the last admission step before the success
            // response. Which named failure the handler reaches first is
            // platform-dependent: where the post-spawn corroborator cannot
            // read a just-spawned child's start key (observed on macOS), the
            // earlier corroboration step rejects instead. Both are named
            // post-claim admission failures, and the contract under test is
            // the same for either — an error response, not a propagated
            // `Err` the client would only see as a timeout.
            fs::write(sessions_dir(&dir.path), b"not a directory").expect("block sessions dir");

            let spec_path = dir.path.join("session-request.json");
            fs::write(
                &spec_path,
                serde_json::to_string(&spec(
                    &dir.path.join("in").display().to_string(),
                    &dir.path.join("out").display().to_string(),
                ))
                .expect("serialize session spec"),
            )
            .expect("write session spec");

            let outcome = handle_start_request(&dir.path, "session-fail", &spec_path)
                .expect("admission failure is a handled response, not a request-loop error");
            assert!(outcome.is_none(), "a failed admission returns no session");

            let response_path = responses_dir(&dir.path).join(mailbox::file_name("session-fail"));
            let response: StartResponse =
                serde_json::from_str(&fs::read_to_string(response_path).expect("start response"))
                    .expect("decode start response");
            assert!(response.session_id.is_none());
            let error = response.error.expect("the response carries an error");
            assert!(
                error.contains("sessions") || error.contains("could not be corroborated"),
                "the response names a post-claim admission failure: {error}"
            );
            assert!(
                list_session_records(&dir.path)
                    .unwrap_or_default()
                    .is_empty(),
                "a failed admission persists no session record"
            );
        }

        /// The start-response client surfaces an error response immediately,
        /// and still reads a response persisted before `error` existed.
        #[test]
        fn await_start_response_surfaces_error_and_legacy_bodies() {
            let dir = TempDir::new("start-response-shapes");
            let responses = responses_dir(&dir.path);
            fs::create_dir_all(&responses).expect("create responses dir");

            fs::write(
                responses.join(mailbox::file_name("failed")),
                serde_json::to_string(&StartResponse {
                    session_id: None,
                    error: Some("could not spawn baton serve: nope".to_string()),
                })
                .expect("serialize error response"),
            )
            .expect("write error response");
            let err = await_start_response(&dir.path, "failed").expect_err("error response fails");
            assert!(
                err.to_string()
                    .contains("could not spawn baton serve: nope"),
                "the client error names the admission failure: {err}"
            );

            fs::write(
                responses.join(mailbox::file_name("legacy")),
                r#"{"session_id":"svc-legacy"}"#,
            )
            .expect("write legacy response");
            assert_eq!(
                await_start_response(&dir.path, "legacy").expect("legacy response parses"),
                "svc-legacy"
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
                    start_epoch_secs: None,
                    stderr_path: String::new(),
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

        /// A single corrupt session record is skipped with a warning; the
        /// remaining healthy records are still returned.
        #[test]
        fn list_session_records_skips_malformed_record_and_warns() {
            let dir = TempDir::new("list-session-malformed");
            for i in 0..2 {
                let record = SessionRecord {
                    id: format!("svc-{i}"),
                    spec: spec("/tmp/in", "/tmp/out"),
                    pid: 1000 + i,
                    started_at: None,
                    start_epoch_secs: None,
                    stderr_path: String::new(),
                };
                write_session_record(&dir.path, &record).expect("write");
            }
            let path = session_record_path(&dir.path, "svc-bad").expect("session record path");
            fs::write(path, "not json").expect("write malformed session record");
            let path = session_record_path(&dir.path, "svc-non-utf8").expect("session record path");
            fs::write(path, b"\xff\xfe not utf8").expect("write non-UTF-8 session record");

            let mut ids: Vec<String> = list_session_records(&dir.path)
                .expect("list")
                .into_iter()
                .map(|r| r.id)
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["svc-0", "svc-1"]);
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
                start_epoch_secs: None,
                stderr_path: String::new(),
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

        #[test]
        fn control_release_wait_timeout_warns_and_returns() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("control-release-timeout");
            let _held = acquire_control_lock(&dir.path).expect("lock");
            let mut warning = Vec::new();

            wait_for_control_release_with_timeout(
                &dir.path,
                Duration::from_millis(20),
                &mut warning,
            )
            .expect("bounded control-release wait");

            let warning = String::from_utf8(warning).expect("warning text");
            assert!(warning.contains(&dir.path.display().to_string()));
            assert!(warning.contains("service.lock"));
            assert_eq!(
                probe_control(&dir.path).expect("probe after timeout"),
                ControlLiveness::Live,
                "timeout leaves the held control lock untouched"
            );
        }

        #[test]
        fn teardown_continues_after_control_release_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-control-release-timeout");
            let _held = acquire_control_lock(&dir.path).expect("lock");
            let record = SessionRecord {
                id: "svc-timeout".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &record).expect("write stale session");
            let mut out = Vec::new();
            let mut warning = Vec::new();

            execute_teardown_with_timeout(
                &dir.path,
                false,
                &mut out,
                Duration::from_millis(20),
                &mut warning,
            )
            .expect("teardown after control-release timeout");

            let warning = String::from_utf8(warning).expect("warning text");
            assert!(warning.contains(&dir.path.display().to_string()));
            assert!(warning.contains("service.lock"));
            assert!(
                String::from_utf8(out)
                    .expect("teardown output")
                    .contains("requested teardown of baton service")
            );
            assert!(
                read_session_record(&dir.path, "svc-timeout")
                    .expect("read session after teardown")
                    .is_none(),
                "teardown reached durable-record cleanup after the warning"
            );
        }

        /// A non-force `service teardown` across several sessions with only
        /// terminal task history performs exactly one full `tasks/`
        /// listing for the whole run — not one per session — and confirms
        /// nothing directly.
        #[test]
        fn execute_teardown_with_timeout_with_terminal_only_history_performs_one_full_listing_and_no_confirm_reads()
         {
            let _guard = serialize_forks_and_locks();
            TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
            TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
            let dir = TempDir::new("teardown-terminal-only-history");
            for id in ["svc-1", "svc-2", "svc-3"] {
                write_session_record(
                    &dir.path,
                    &SessionRecord {
                        id: id.to_string(),
                        spec: spec("/tmp/in", "/tmp/out"),
                        pid: u32::MAX - 1,
                        started_at: None,
                        start_epoch_secs: None,
                        stderr_path: String::new(),
                    },
                )
                .expect("write session");
            }
            let terminal = |id: &str, session: &str| TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec(session, "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
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
            write_task_record(&dir.path, &terminal("task-1a", "svc-1")).expect("write task-1a");
            write_task_record(&dir.path, &terminal("task-1b", "svc-1")).expect("write task-1b");
            write_task_record(&dir.path, &terminal("task-2a", "svc-2")).expect("write task-2a");
            write_task_record(&dir.path, &terminal("task-3a", "svc-3")).expect("write task-3a");

            let mut out = Vec::new();
            let mut warning = Vec::new();
            execute_teardown_with_timeout(
                &dir.path,
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
                    read_session_record(&dir.path, id).expect("read").is_none(),
                    "{id}'s session record is removed"
                );
            }
            for id in ["task-1a", "task-1b", "task-2a", "task-3a"] {
                assert!(
                    read_task_record(&dir.path, id).expect("read").is_none(),
                    "{id}'s task record is removed"
                );
            }
        }

        #[test]
        fn control_release_wait_returns_without_warning_when_released() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("control-release-responsive");
            let held = acquire_control_lock(&dir.path).expect("lock");
            let releaser = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                drop(held);
            });
            let mut warning = Vec::new();

            wait_for_control_release_with_timeout(
                &dir.path,
                Duration::from_millis(200),
                &mut warning,
            )
            .expect("responsive control-release wait");
            releaser.join().expect("release control lock");

            assert!(warning.is_empty(), "normal release emitted a warning");
        }

        /// A concurrent liveness probe must never defeat a starting
        /// supervisor.
        ///
        /// `probe_or_signal_control` decides "not running" by *taking* the
        /// exclusive control lock, so its hold — however brief — is
        /// indistinguishable from a live `Run` to
        /// [`acquire_control_lock`]'s single non-blocking attempt. Every
        /// client that polls `service status` while a supervisor starts
        /// (exactly what the restart integration test does) can therefore
        /// kill it with a spurious "another baton service already holds the
        /// control lock". The two must be serialized against each other.
        #[test]
        fn probe_never_defeats_a_concurrent_control_lock_acquisition() {
            use std::sync::Arc;
            use std::sync::atomic::AtomicBool;

            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("probe-vs-acquire");
            // Create the lock file up front so the probers spend their time
            // contending for it rather than creating it.
            drop(acquire_control_lock(&dir.path).expect("seed the lock file"));

            let stop = Arc::new(AtomicBool::new(false));
            let probers: Vec<_> = (0..2)
                .map(|_| {
                    let path = dir.path.clone();
                    let stop = Arc::clone(&stop);
                    std::thread::spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            probe_control(&path).expect("probe");
                        }
                    })
                })
                .collect();

            let mut lost = None;
            for attempt in 0..500 {
                match acquire_control_lock(&dir.path) {
                    Ok(lock) => drop(lock),
                    Err(err) => {
                        lost = Some((attempt, err));
                        break;
                    }
                }
            }

            stop.store(true, Ordering::Relaxed);
            for prober in probers {
                prober.join().expect("prober thread");
            }
            assert!(
                lost.is_none(),
                "a concurrent probe defeated the control-lock acquisition: {lost:?}"
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

        /// A low PID is ignored rather than becoming a process-group signal.
        #[test]
        fn signal_group_ignores_low_pid() {
            signal_group(1, libc::SIGTERM).expect("low PID is a safe no-op");
        }

        #[test]
        fn signal_group_ignores_pid_outside_pid_t() {
            signal_group(i32::MAX as u32 + 1, libc::SIGTERM)
                .expect("out-of-range PID is a safe no-op");
        }

        #[test]
        fn signal_group_treats_gone_group_as_success() {
            let _guard = serialize_forks_and_locks();
            let mut child = Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .process_group(0)
                .spawn()
                .expect("spawn process-group fixture");
            let pid = child.id();
            child.wait().expect("reap process-group fixture");

            signal_group(pid, libc::SIGKILL).expect("gone process group is already stopped");
        }

        #[test]
        fn signal_group_surfaces_invalid_signal() {
            let _guard = serialize_forks_and_locks();
            let mut child = Command::new("/bin/sleep")
                .arg("30")
                .process_group(0)
                .spawn()
                .expect("spawn process-group fixture");
            let pid = child.id();
            let result = signal_group(pid, -1);

            signal_group(pid, libc::SIGKILL).expect("clean up process-group fixture");
            child.wait().expect("reap process-group fixture");

            let error = result.expect_err("invalid signal must be returned");
            assert!(matches!(
                error,
                BatonError::Io(message) if message.contains("could not signal process group")
            ));
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
            let _guard = serialize_forks_and_locks();
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
                start_epoch_secs: None,
                stderr_path: String::new(),
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
            assert_eq!(sessions[0]["liveness"], "dead");
        }

        /// A non-force `service stop` with only terminal task history
        /// performs exactly one full `tasks/` listing and confirms nothing
        /// directly — a terminal cache entry is trusted outright, so no
        /// per-record read beyond the listing's own parse is needed.
        #[test]
        fn execute_stop_with_terminal_only_history_performs_one_full_listing_and_no_confirm_reads()
        {
            let _guard = serialize_forks_and_locks();
            TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
            TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
            let dir = TempDir::new("stop-terminal-only-history");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            let terminal = |id: &str, session: &str| TaskRecord {
                id: id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec(session, "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
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
            write_task_record(&dir.path, &terminal("task-owned-1", "svc-1"))
                .expect("write owned-1");
            write_task_record(&dir.path, &terminal("task-owned-2", "svc-1"))
                .expect("write owned-2");
            write_task_record(&dir.path, &terminal("task-other", "svc-2")).expect("write other");

            let mut out = Vec::new();
            execute_stop(&dir.path, "svc-1", false, &mut out).expect("stop svc-1");

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
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none(),
                "svc-1's session record is removed"
            );
            assert!(
                read_task_record(&dir.path, "task-owned-1")
                    .expect("read")
                    .is_none(),
                "svc-1's first owned task record is removed"
            );
            assert!(
                read_task_record(&dir.path, "task-owned-2")
                    .expect("read")
                    .is_none(),
                "svc-1's second owned task record is removed"
            );
            assert!(
                read_task_record(&dir.path, "task-other")
                    .expect("read")
                    .is_some(),
                "an unrelated session's task record is untouched"
            );
        }

        /// `execute_stop` on an unknown session id is a no-op success
        /// (idempotent), leaving nothing behind.
        #[test]
        fn execute_stop_unknown_session_is_idempotent_success() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-unknown");
            let mut out = Vec::new();
            execute_stop(&dir.path, "nope", false, &mut out).expect("stop");
            assert!(String::from_utf8(out).unwrap().contains("nothing to stop"));
        }

        /// `execute_teardown` reaps a dead session's stale record and reports
        /// no running service when none holds the lock — idempotent even
        /// when `Run` was never (or is no longer) alive.
        #[test]
        fn execute_teardown_reaps_stale_record_without_live_service() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-stale");
            let record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &record).expect("write");

            let mut out = Vec::new();
            execute_teardown(&dir.path, false, &mut out).expect("teardown");
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

        /// Teardown removes a terminal legacy task record without probing or
        /// signalling a live process whose PID and argv happen to match it.
        #[cfg(target_os = "linux")]
        #[test]
        fn execute_teardown_does_not_signal_terminal_legacy_task() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-terminal-task");
            let task_id = "task-terminal-legacy";
            let callback_inbox = dir.path.join("callback");
            let task_specification = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                Vec::new(),
                60_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, task_id);
            fs::create_dir_all(&log_dir).expect("create task log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                .expect("spawn unrelated process");
            let task_record = TaskRecord {
                id: task_id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Responded,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(1),
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &task_record).expect("write terminal task record");
            request_task_cancel_sentinel(&dir.path, task_id).expect("write cancel sentinel");
            let deadline = Instant::now() + Duration::from_secs(10);
            let liveness = loop {
                let liveness = is_task_alive(&task_record);
                if liveness == Liveness::Live || Instant::now() >= deadline {
                    break liveness;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            assert_eq!(
                liveness,
                Liveness::Live,
                "fixture must match the live process by PID and argv within 10 seconds"
            );

            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session record");

            execute_teardown(&dir.path, false, &mut Vec::new()).expect("teardown");

            assert!(
                child.try_wait().expect("poll unrelated process").is_none(),
                "teardown leaves the matching unrelated process alive"
            );
            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read removed terminal task")
                    .is_none(),
                "teardown removes the terminal task record"
            );
            assert!(
                !task_cancel_sentinel_path(&dir.path, task_id).is_file(),
                "teardown removes the terminal task cancel sentinel"
            );

            child.kill().expect("clean up unrelated process");
            child.wait().expect("wait for unrelated process");
        }

        // -- Task records -----------------------------------------------

        /// A task record round-trips through the atomic-write file protocol
        /// byte-for-byte.
        #[test]
        fn task_record_round_trips() {
            let dir = TempDir::new("task-record");
            let record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: 4242,
                started_at: Some("123456".to_string()),
                start_epoch_secs: Some(123456),
                job: None,
                started_ms: Some(42),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: "/tmp/out.log".to_string(),
                stderr_path: "/tmp/err.log".to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
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
                    request_id: None,
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                    pid: 1000 + i,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Running,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout_path: String::new(),
                    stderr_path: String::new(),
                    delivered_milestones: 0,
                    terminal_delivered_at_ms: None,
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

        /// A single corrupt task record is skipped with a warning; the
        /// remaining healthy records are still returned.
        #[test]
        fn list_task_records_skips_malformed_record_and_warns() {
            let dir = TempDir::new("task-list-malformed");
            for i in 0..2 {
                let record = TaskRecord {
                    id: format!("task-{i}"),
                    request_id: None,
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                    pid: 1000 + i,
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Running,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout_path: String::new(),
                    stderr_path: String::new(),
                    delivered_milestones: 0,
                    terminal_delivered_at_ms: None,
                };
                write_task_record(&dir.path, &record).expect("write");
            }
            let path = task_record_path(&dir.path, "task-bad").expect("task record path");
            fs::write(path, "not json").expect("write malformed task record");
            let path = task_record_path(&dir.path, "task-non-utf8").expect("task record path");
            fs::write(path, b"\xff\xfe not utf8").expect("write non-UTF-8 task record");

            let mut ids: Vec<String> = list_task_records(&dir.path)
                .expect("list")
                .into_iter()
                .map(|r| r.id)
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["task-0", "task-1"]);
        }

        /// An absent response returns without waiting on the admission lock,
        /// so a polling client cannot hold that lock across the wait interval.
        #[test]
        fn absent_task_start_response_poll_is_lock_free() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-response-poll");
            let _admission = acquire_admission_lock(&dir.path).expect("hold admission lock");
            assert!(
                take_task_start_response(&dir.path, "poll-request")
                    .expect("poll response")
                    .is_none()
            );
        }

        /// A response claim writes its acknowledgement before cleanup, and a
        /// repeated consumer cannot read the response again.
        #[test]
        fn task_start_response_claim_records_ack_idempotently() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-response-ack");
            let request_id = "response-ack-request";
            let response = TaskStartResponse {
                task_id: Some("task-1".to_string()),
                error: None,
            };
            write_task_start_response(&dir.path, request_id, &response).expect("write response");

            let consumed = take_task_start_response(&dir.path, request_id)
                .expect("take response")
                .expect("response is present");
            assert_eq!(consumed.task_id, response.task_id);
            assert!(task_start_ack_exists(&dir.path, request_id).expect("ack exists"));
            assert!(
                !task_start_response_path(&dir.path, request_id)
                    .expect("response path")
                    .exists()
            );
            assert!(
                !task_start_response_claim_path(&dir.path, request_id)
                    .expect("claim path")
                    .exists()
            );
            assert!(
                take_task_start_response(&dir.path, request_id)
                    .expect("repeat take")
                    .is_none()
            );
            admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                .expect("reconcile orphan acknowledgement");
            assert!(!task_start_ack_exists(&dir.path, request_id).expect("ack cleanup"));
        }

        /// A private claim left by a client before acknowledgement is
        /// restored even when its response has no task record, such as an
        /// owner-rejection response.
        #[test]
        fn reconcile_orphan_task_response_claim_restores_response() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("orphan-task-response-claim");
            let request_id = "orphan-response-claim";
            let response = TaskStartResponse {
                task_id: None,
                error: Some("owner rejected".to_string()),
            };
            write_task_start_response(&dir.path, request_id, &response)
                .expect("write owner rejection response");
            fs::rename(
                task_start_response_path(&dir.path, request_id).expect("response path"),
                task_start_response_claim_path(&dir.path, request_id).expect("claim path"),
            )
            .expect("claim owner rejection response");

            admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                .expect("reconcile orphan claim");

            assert!(
                task_start_response_path(&dir.path, request_id)
                    .expect("response path")
                    .is_file()
            );
            assert!(
                !task_start_response_claim_path(&dir.path, request_id)
                    .expect("claim path")
                    .exists()
            );
        }

        /// Startup reconciliation finalizes a committed record for an
        /// acknowledgement, a response, or a recoverable private claim, and
        /// remains safe when repeated.
        #[test]
        fn reconcile_task_admission_finalizes_response_boundaries() {
            let _guard = serialize_forks_and_locks();
            for (index, boundary) in ["ack", "response", "claim", "missing"]
                .into_iter()
                .enumerate()
            {
                let dir = TempDir::new(&format!("task-response-boundary-{boundary}"));
                let request_id = format!("response-boundary-request-{index}");
                let task_id = format!("response-boundary-task-{index}");
                let record = TaskRecord {
                    id: task_id.clone(),
                    request_id: Some(request_id.clone()),
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/callback"),
                    pid: 0,
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
                write_task_record(&dir.path, &record).expect("write task record");
                let response = TaskStartResponse {
                    task_id: Some(task_id.clone()),
                    error: None,
                };
                if boundary != "missing" {
                    write_task_start_response(&dir.path, &request_id, &response)
                        .expect("write task response");
                }
                if boundary == "ack" {
                    take_task_start_response(&dir.path, &request_id)
                        .expect("take response")
                        .expect("response is present");
                } else if boundary == "claim" {
                    fs::rename(
                        task_start_response_path(&dir.path, &request_id).expect("response path"),
                        task_start_response_claim_path(&dir.path, &request_id).expect("claim path"),
                    )
                    .expect("claim response");
                }

                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("reconcile response boundary");
                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("repeat reconciliation");

                let reconciled = read_task_record(&dir.path, &task_id)
                    .expect("read reconciled task")
                    .expect("reconciled task exists");
                assert_eq!(reconciled.admission, TaskAdmissionPhase::Responded);
                assert_eq!(
                    task_start_response_path(&dir.path, &request_id)
                        .expect("response path")
                        .is_file(),
                    boundary != "ack"
                );
                assert!(
                    !task_start_response_claim_path(&dir.path, &request_id)
                        .expect("claim path")
                        .exists()
                );
                assert!(
                    !task_start_ack_path(&dir.path, &request_id)
                        .expect("ack path")
                        .exists()
                );
            }
        }

        /// A rollback marker removes every admission phase's task record and
        /// both request locations before the marker is cleared. Repeating
        /// reconciliation is harmless once the first pass has completed.
        #[test]
        fn reconcile_task_admission_rollback_is_idempotent_across_phases() {
            let _guard = serialize_forks_and_locks();
            for (index, admission) in [
                TaskAdmissionPhase::Prepared,
                TaskAdmissionPhase::Committed,
                TaskAdmissionPhase::Responded,
            ]
            .into_iter()
            .enumerate()
            {
                let dir = TempDir::new(&format!("task-rollback-phase-{index}"));
                let request_id = format!("request-{index}");
                let task_id = format!("task-{index}");
                let record = TaskRecord {
                    id: task_id.clone(),
                    request_id: Some(request_id.clone()),
                    admission,
                    spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/callback"),
                    pid: 0,
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
                write_task_record(&dir.path, &record).expect("write task record");

                for path in [
                    task_requests_dir(&dir.path),
                    task_processing_dir(&dir.path),
                    task_start_rollback_dir(&dir.path),
                ] {
                    fs::create_dir_all(&path).expect("create task admission directory");
                }
                let file_name = mailbox::file_name(&request_id);
                fs::write(task_requests_dir(&dir.path).join(&file_name), "{}")
                    .expect("write request");
                fs::write(task_processing_dir(&dir.path).join(&file_name), "{}")
                    .expect("write claimed request");
                fs::write(task_start_rollback_dir(&dir.path).join(&file_name), "")
                    .expect("write rollback marker");

                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("reconcile rollback");
                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("repeat reconciliation");

                assert!(
                    read_task_record(&dir.path, &task_id)
                        .expect("read task record")
                        .is_none(),
                    "{admission:?} task record is removed"
                );
                assert!(
                    !task_requests_dir(&dir.path).join(&file_name).exists(),
                    "pending request is removed"
                );
                assert!(
                    !task_processing_dir(&dir.path).join(&file_name).exists(),
                    "claimed request is removed"
                );
                assert!(
                    !task_start_rollback_dir(&dir.path).join(&file_name).exists(),
                    "rollback marker is removed last"
                );
            }
        }

        /// An unresolved prepared admission is retained as cleanup residue,
        /// not rehydrated as active work. Once its PID is positively dead,
        /// reconciliation removes the record and every admission artifact.
        #[cfg(target_os = "linux")]
        #[test]
        fn unresolved_prepared_admission_retains_marker_until_dead_cleanup() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("prepared-unresolved");
            let request_id = "prepared-unresolved-request";
            let task_id = "prepared-unresolved-task";
            let log_dir = task_logs_dir(&dir.path, task_id);
            fs::create_dir_all(&log_dir).expect("create task logs");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let (mut child, spec) = spawn_unresolved_identity_child(&stdout_path, &stderr_path);
            let record = TaskRecord {
                id: task_id.to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Prepared,
                spec,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: Some(1),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &record).expect("write prepared task");
            write_task_start_response(
                &dir.path,
                request_id,
                &TaskStartResponse {
                    task_id: Some(task_id.to_string()),
                    error: None,
                },
            )
            .expect("write task response");
            mark_task_start_ack(&dir.path, request_id).expect("write task acknowledgement");
            mark_task_start_rollback(&dir.path, request_id).expect("write rollback marker");

            assert_eq!(
                is_task_alive(&record),
                Liveness::Unresolved,
                "fixture is in the unresolved identity state"
            );

            admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                .expect("retain unresolved admission");
            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read retained task")
                    .is_some(),
                "unresolved prepared record remains durable"
            );
            assert!(
                task_start_rollback_exists(&dir.path, request_id).expect("rollback marker exists"),
                "rollback marker remains until cleanup succeeds"
            );
            assert!(
                task_tick::rehydrate_tasks::<UnixServicePlatform>(
                    &dir.path,
                    &FakeClock::new(),
                    DEFAULT_TASK_RETENTION_MS,
                    None,
                )
                .expect("rehydrate tasks")
                .is_empty(),
                "unresolved prepared record is not active work"
            );

            signal_group(record.pid, libc::SIGKILL).expect("kill unresolved task");
            child.wait().expect("wait for unresolved task");
            admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                .expect("remove dead admission");

            assert!(
                read_task_record(&dir.path, task_id)
                    .expect("read removed task")
                    .is_none(),
                "dead prepared record is removed"
            );
            assert!(
                !task_start_rollback_exists(&dir.path, request_id)
                    .expect("rollback marker cleanup"),
                "dead prepared rollback marker is removed"
            );
            assert!(
                !task_start_response_boundary_exists(&dir.path, request_id)
                    .expect("response boundary cleanup"),
                "dead prepared response boundary is removed"
            );
        }

        /// A PID whose current start key does not match the durable record is
        /// finalized as gone and is never signalled as the task.
        #[test]
        fn rehydrate_rejects_pid_reuse_without_signalling() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-pid-reuse");
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
            let log_dir = task_logs_dir(&dir.path, "task-reused");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child =
                spawn_task_child(&spec, &stdout_path, &stderr_path).expect("spawn task");
            let record = TaskRecord {
                id: "task-reused".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid: child.id(),
                started_at: Some("not-the-current-start-key".to_string()),
                start_epoch_secs: None,
                job: None,
                started_ms: Some(clock.now_ms()),
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &record).expect("write task record");

            let mut tasks =
                task_tick::rehydrate_tasks::<UnixServicePlatform>(&dir.path, &clock, 0, None)
                    .expect("rehydrate task");
            let mut running = tasks.remove("task-reused").expect("rehydrated task");
            assert!(
                running.child.is_none(),
                "rehydrated task has no Child handle"
            );
            assert!(
                matches!(
                    tick_one_task(&dir.path, "task-reused", &mut running, &clock),
                    Ok(TaskTick::Finished)
                ),
                "PID reuse is finalized without adoption"
            );
            assert_eq!(running.record.state, TaskState::Failed);
            assert_eq!(running.record.exit_code, None);
            assert!(
                child.try_wait().expect("poll unrelated process").is_none(),
                "a mismatched PID is not signalled"
            );

            let _ = signal_group(child.id(), libc::SIGKILL);
            let _ = child.wait();
        }

        /// A terminal record already delivered before a restart is rehydrated
        /// with its `terminal_delivered_at_ms` intact, is never redelivered,
        /// and is reaped exactly once retention elapses.
        #[test]
        fn rehydrated_delivered_task_reaps_at_retention_boundary_without_redelivery() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-retention");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running =
                terminal_running_task(&dir.path, "task-delivered", &callback_inbox, &clock);
            running.record.terminal_delivered_at_ms = Some(clock.now_ms());
            write_task_record(&dir.path, &running.record).expect("write delivered task record");

            let log_dir = task_logs_dir(&dir.path, "task-delivered");
            fs::create_dir_all(&log_dir).expect("create task logs");
            fs::write(log_dir.join("stdout.log"), b"out").expect("write captured stdout");

            let retention_ms = 1_000;
            let mut tasks = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &clock,
                retention_ms,
                None,
            )
            .expect("rehydrate tasks");
            let mut running = tasks.remove("task-delivered").expect("rehydrated task");

            clock.advance(retention_ms - 1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-delivered", &mut running, &clock)
                    .expect("tick within retention window"),
                TaskTick::StillRunning
            ));
            assert!(
                !callback_inbox.exists(),
                "an already-delivered record is never redelivered after a restart"
            );
            assert!(
                read_task_record(&dir.path, "task-delivered")
                    .expect("read retained task")
                    .is_some(),
                "the record survives while retention has not yet elapsed"
            );
            assert!(
                log_dir.join("stdout.log").is_file(),
                "the captured output stays readable while the record is retained"
            );

            clock.advance(1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-delivered", &mut running, &clock)
                    .expect("tick at retention boundary"),
                TaskTick::Finished
            ));
            assert!(
                read_task_record(&dir.path, "task-delivered")
                    .expect("read reaped task")
                    .is_none(),
                "the record is reaped exactly once retention elapses"
            );
            assert!(
                !log_dir.exists(),
                "the captured output is reclaimed with the record it belonged to"
            );
        }

        /// A terminal record whose delivery had not yet succeeded before a
        /// restart still redelivers on the next tick, and persists
        /// `terminal_delivered_at_ms` so a later restart does not redeliver
        /// again.
        #[test]
        fn rehydrated_undelivered_terminal_task_still_redelivers() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-undelivered");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let running =
                terminal_running_task(&dir.path, "task-undelivered", &callback_inbox, &clock);
            write_task_record(&dir.path, &running.record).expect("write undelivered task record");

            let mut tasks = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &clock,
                DEFAULT_TASK_RETENTION_MS,
                None,
            )
            .expect("rehydrate tasks");
            let mut running = tasks.remove("task-undelivered").expect("rehydrated task");

            assert!(matches!(
                tick_one_task(&dir.path, "task-undelivered", &mut running, &clock)
                    .expect("redeliver after restart"),
                TaskTick::StillRunning
            ));
            assert_terminal_task_event(&callback_inbox, "task-undelivered");
            let record = read_task_record(&dir.path, "task-undelivered")
                .expect("read redelivered task")
                .expect("record still present");
            assert!(
                record.terminal_delivered_at_ms.is_some(),
                "delivery is persisted so a later restart does not redeliver again"
            );
        }

        /// A missing (already reaped, or never written) task record answers
        /// `task status` with an empty task list, exactly like any other
        /// unknown id.
        #[test]
        fn task_status_reports_nothing_for_a_reaped_task() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("status-reaped");
            let mut out = Vec::new();
            execute_task_status(&dir.path, Some("task-gone"), &mut out)
                .expect("status for missing task");
            let json: serde_json::Value = serde_json::from_slice(&out).expect("json");
            assert_eq!(json["tasks"].as_array().unwrap().len(), 0);
        }

        /// When `reconcile_task_admissions` reports no mutation, boot reuses
        /// its returned snapshot instead of walking `tasks/` a second time:
        /// proven by replacing `tasks/` with a plain file afterward and
        /// showing `rehydrate_tasks` still succeeds when given the reused
        /// snapshot, but fails (as a control) when forced to re-list.
        #[test]
        fn rehydrate_reuses_reconciled_records_without_a_second_directory_walk() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("boot-reuse-records");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let running = terminal_running_task(&dir.path, "task-settled", &callback_inbox, &clock);
            write_task_record(&dir.path, &running.record).expect("write task record");

            let (records, mutated) =
                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("reconcile clean boot");
            assert!(!mutated, "a clean tasks/ directory reports no mutation");

            fs::remove_dir_all(dir.path.join("tasks")).expect("remove tasks directory");
            fs::write(dir.path.join("tasks"), "not a directory")
                .expect("replace tasks directory with a file");

            let system_clock = SystemClock;
            let reused = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &system_clock,
                DEFAULT_TASK_RETENTION_MS,
                Some(records),
            )
            .expect("rehydrate reuses the reconciled snapshot without re-listing tasks/");
            assert_eq!(reused.len(), 1);

            let relisted = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &system_clock,
                DEFAULT_TASK_RETENTION_MS,
                None,
            );
            assert!(
                relisted.is_err(),
                "control: without the reused snapshot, rehydrate_tasks re-lists tasks/ and fails \
                 against the broken directory"
            );
        }

        /// A mutating reconciliation pass (here, aborting a `Prepared`
        /// admission) is never reused: boot re-lists `tasks/` and observes
        /// the removal, rather than trusting the stale pre-reconciliation
        /// snapshot.
        #[test]
        fn rehydrate_relists_when_reconciliation_mutated_tasks() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("boot-reuse-mutated");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");

            let mut prepared =
                terminal_running_task(&dir.path, "task-prepared", &callback_inbox, &clock).record;
            prepared.admission = TaskAdmissionPhase::Prepared;
            write_task_record(&dir.path, &prepared).expect("write prepared task record");

            let settled =
                terminal_running_task(&dir.path, "task-settled", &callback_inbox, &clock).record;
            write_task_record(&dir.path, &settled).expect("write settled task record");

            let (records, mutated) =
                admission::reconcile_task_admissions::<UnixServicePlatform>(&dir.path)
                    .expect("reconcile mutated boot");
            assert!(mutated, "aborting the prepared admission is a mutation");
            assert_eq!(
                records.len(),
                2,
                "the pre-reconciliation snapshot still lists both records"
            );

            let system_clock = SystemClock;
            let tasks = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &system_clock,
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
        }

        /// A reused PID can still be a zombie while the original task's
        /// descendant keeps the old process group alive. The zombie's
        /// mismatched identity must remain unresolved rather than making that
        /// unrelated group look like this task.
        #[cfg(target_os = "linux")]
        #[test]
        fn task_rejects_reused_zombie_pid_with_live_group() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-zombie-pid-reuse");
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-zombie-pid-reuse");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&spec, &stdout_path, &stderr_path)
                .expect("spawn task with descendant");
            let pid = child.id();
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            assert!(started_at.is_some(), "fixture has a start identity");
            let record = TaskRecord {
                id: "task-zombie-pid-reuse".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec,
                pid,
                started_at: Some("not-the-zombie-start-key".to_string()),
                start_epoch_secs,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };

            wait_for_zombie_group_descendant(&child);
            write_task_record(&dir.path, &record).expect("write zombie task record");
            assert_eq!(
                task_execution_liveness(&record),
                Liveness::Unresolved,
                "a mismatched zombie remains unresolved"
            );
            let clock = FakeClock::new();
            let mut tasks = task_tick::rehydrate_tasks::<UnixServicePlatform>(
                &dir.path,
                &clock,
                DEFAULT_TASK_RETENTION_MS,
                None,
            )
            .expect("rehydrate unresolved task");
            let mut running = tasks
                .remove(&record.id)
                .expect("rehydrated unresolved task");
            assert!(
                running.child.is_none(),
                "fixture follows the rehydrated tick path"
            );
            running.term_sent_at_ms = Some(clock.now_ms().saturating_sub(KILL_GRACE_MS));
            assert!(matches!(
                tick_one_task(&dir.path, &record.id, &mut running, &clock),
                Ok(TaskTick::StillRunning)
            ));
            assert_eq!(running.record.state, TaskState::Running);
            assert_eq!(
                read_task_record(&dir.path, &record.id)
                    .expect("read unresolved task after tick")
                    .expect("unresolved task remains durable")
                    .state,
                TaskState::Running,
                "an unresolved Unix group is not finalized by the shared tick"
            );
            let retry_grace_ms = POLL_INTERVAL_MS * 2;
            let started = Instant::now();
            assert_eq!(
                task_execution_liveness_after_retry(&record, retry_grace_ms),
                Liveness::Unresolved,
                "a mismatched zombie remains unresolved after retry"
            );
            assert!(
                started.elapsed() >= Duration::from_millis(retry_grace_ms),
                "unresolved retry returned before its deadline"
            );
            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let (residue, _handled) =
                reap_session_tasks_with_wait(&mut admission, "svc-1", false, wait_while_task_alive)
                    .expect("retain task");
            drop(admission);
            assert_eq!(residue.len(), 1);
            assert_eq!(residue[0].liveness, Liveness::Unresolved);
            assert!(
                read_task_record(&dir.path, &record.id)
                    .expect("read retained zombie task")
                    .is_some(),
                "unresolved zombie task record is retained"
            );
            let mut legacy = record.clone();
            legacy.started_at = None;
            assert_eq!(
                task_execution_liveness(&legacy),
                Liveness::Unresolved,
                "a zombie without durable identity remains unresolved"
            );
            assert_eq!(
                unsafe { libc::kill(-(pid as libc::pid_t), 0) },
                0,
                "the descendant's process group remains alive after the rejected probes"
            );

            signal_group(pid, libc::SIGKILL).expect("kill fixture group");
            let _ = child.wait();
        }

        /// The lstart parser accepts a leap-day in a divisible-by-four year.
        #[test]
        fn lstart_parser_leap_year_case() {
            assert_eq!(
                parse_lstart_epoch_secs("Thu Feb 29 00:00:00 2024"),
                Some(1_709_164_800)
            );
        }

        /// The lstart parser applies Gregorian century rules: 1900 is not a
        /// leap year, while 2000 is.
        #[test]
        fn lstart_parser_century_boundary_case() {
            assert_eq!(parse_lstart_epoch_secs("Thu Feb 29 00:00:00 1900"), None);
            assert_eq!(
                parse_lstart_epoch_secs("Tue Feb 29 00:00:00 2000"),
                Some(951_782_400)
            );
        }

        /// The lstart parser carries a month boundary by exactly one day.
        #[test]
        fn lstart_parser_month_boundary_case() {
            let january =
                parse_lstart_epoch_secs("Wed Jan 31 23:59:59 2024").expect("January 31 is valid");
            let february =
                parse_lstart_epoch_secs("Thu Feb 1 00:00:00 2024").expect("February 1 is valid");
            assert_eq!(february - january, 1);
        }

        /// The lstart parser maps the Unix epoch itself to zero.
        #[test]
        fn lstart_parser_unix_epoch_case() {
            assert_eq!(parse_lstart_epoch_secs("Thu Jan 1 00:00:00 1970"), Some(0));
        }

        /// Linux must not condemn a task whose durable start key is absent
        /// merely because bash has exec-replaced its command line. The
        /// command-line corroborator may confirm a task, but a mismatch is
        /// unresolved because the same PID can legitimately change argv.
        #[cfg(target_os = "linux")]
        #[test]
        fn linux_exec_replaced_task_without_start_key_is_unresolved() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("linux-exec-unresolved");
            let callback_inbox = dir.path.join("callback");
            let task_specification = task_spec(
                "svc-1",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                &callback_inbox.display().to_string(),
            );
            let log_dir = task_logs_dir(&dir.path, "task-exec-unresolved");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let mut child = spawn_task_child(&task_specification, &stdout_path, &stderr_path)
                .expect("spawn task");
            let record = TaskRecord {
                id: "task-exec-unresolved".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            let deadline = Instant::now() + Duration::from_secs(10);
            let liveness = loop {
                let liveness = is_task_alive(&record);
                if liveness == Liveness::Unresolved || Instant::now() >= deadline {
                    break liveness;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            assert_eq!(liveness, Liveness::Unresolved);
            assert!(
                child.try_wait().expect("poll exec-replaced task").is_none(),
                "an unresolved task is not signalled"
            );
            let _ = signal_group(child.id(), libc::SIGKILL);
            let _ = child.wait();
        }

        /// A zombie sample is never accepted as a live, corroborated task
        /// even when its reported start time matches the durable record.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn process_probe_rejects_zombie_state() {
            let probe = parse_process_probe("Z Mon Aug 3 12:34:56 2026\n")
                .expect("parse zombie process sample");
            assert!(probe.is_zombie());
            let (started_at, start_epoch_secs) = start_identity_from_probe(&probe);
            assert!(started_at.is_none());
            assert!(start_epoch_secs.is_none());
        }

        /// macOS/BSD `ps` appends metadata flags to the leading process
        /// state; those flags do not change whether the process is a zombie.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn process_state_accepts_bsd_ps_flags() {
            assert_eq!(parse_process_state("Ssl+"), Some(false));
            assert_eq!(parse_process_state("Zs+"), Some(true));
            assert_eq!(parse_process_state("Q"), None);
            assert_eq!(parse_process_state("S?"), None);
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
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
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
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(10),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
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

            let _ = signal_group(running.record.pid, libc::SIGKILL);
            let _ = running.child.as_mut().expect("owned child").wait();
        }

        /// Exceeding `max_duration_ms` (per the injected clock) escalates
        /// `SIGTERM`→`SIGKILL` when the child remains alive, while an exit
        /// observed immediately after `SIGTERM` is also attributed to
        /// `timeout`. Both valid Unix reap outcomes deliver the same terminal
        /// event.
        #[test]
        fn tick_one_task_enforces_max_duration_and_marks_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-timeout");
            let clock = FakeClock::new();
            let helper = task_timeout_helper_path();

            let immediate_callback = dir.path.join("callback-immediate");
            let immediate_ready = dir.path.join("immediate.ready");
            let immediate_spec = task_spec(
                "svc-1",
                &helper.display().to_string(),
                vec![
                    "--mode".to_string(),
                    "exit-on-term".to_string(),
                    "--ready-file".to_string(),
                    immediate_ready.display().to_string(),
                ],
                vec![],
                100,
                &immediate_callback.display().to_string(),
            );
            let mut immediate =
                spawn_running_task(&dir.path, "task-t-immediate", immediate_spec, &clock);
            wait_for_task_helper(&mut immediate, &immediate_ready);

            clock.advance(150);
            let tick =
                tick_one_task(&dir.path, "task-t-immediate", &mut immediate, &clock).expect("tick");
            assert!(
                immediate.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM"
            );
            reap_task_until_finished(&dir.path, "task-t-immediate", &mut immediate, &clock, tick);
            assert!(
                !immediate.kill_sent,
                "immediate TERM exit must not need SIGKILL"
            );
            assert_eq!(immediate.record.state, TaskState::Timeout);
            assert_terminal_task_event(&immediate_callback, "task-t-immediate");

            let kill_callback = dir.path.join("callback-kill");
            let kill_ready = dir.path.join("kill.ready");
            let kill_spec = task_spec(
                "svc-1",
                &helper.display().to_string(),
                vec![
                    "--mode".to_string(),
                    "ignore-term".to_string(),
                    "--ready-file".to_string(),
                    kill_ready.display().to_string(),
                ],
                vec![],
                100,
                &kill_callback.display().to_string(),
            );
            let mut kill_task = spawn_running_task(&dir.path, "task-t-kill", kill_spec, &clock);
            wait_for_task_helper(&mut kill_task, &kill_ready);

            clock.advance(150);
            let tick =
                tick_one_task(&dir.path, "task-t-kill", &mut kill_task, &clock).expect("tick");
            assert!(
                matches!(tick, TaskTick::StillRunning),
                "TERM-ignoring helper must remain alive for KILL escalation"
            );
            assert!(
                kill_task.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM"
            );

            clock.advance(KILL_GRACE_MS);
            let tick =
                tick_one_task(&dir.path, "task-t-kill", &mut kill_task, &clock).expect("tick");
            assert!(
                kill_task.kill_sent,
                "SIGTERM grace expiry must send SIGKILL"
            );
            reap_task_until_finished(&dir.path, "task-t-kill", &mut kill_task, &clock, tick);
            assert_eq!(kill_task.record.state, TaskState::Timeout);

            assert_terminal_task_event(&kill_callback, "task-t-kill");
        }

        /// Builds the explicit-signal child used by the timeout test. The
        /// helper is an example that runs as a real process-group leader.
        fn task_timeout_helper_path() -> std::path::PathBuf {
            let cargo = option_env!("CARGO").unwrap_or("cargo");
            let built = Command::new(cargo)
                .args(["build", "--quiet", "--example", "task_timeout_helper"])
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .status()
                .expect("build task_timeout_helper");
            assert!(built.success(), "task_timeout_helper builds");

            let test_bin = std::env::current_exe().expect("resolve service test executable");
            let profile_dir = test_bin
                .parent()
                .and_then(std::path::Path::parent)
                .expect("service test executable has a profile directory");
            let helper = profile_dir.join("examples").join("task_timeout_helper");
            assert!(
                helper.is_file(),
                "task_timeout_helper at {}",
                helper.display()
            );
            helper
        }

        /// Waits for the helper's signal handler to be installed before the
        /// fake clock drives the timeout. A readiness file avoids a startup
        /// race where TERM could arrive before the helper has configured its
        /// explicit handler.
        fn wait_for_task_helper(running: &mut RunningTask, ready: &Path) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if ready.is_file() {
                    return;
                }
                if let Some(child) = running.child.as_mut()
                    && child.try_wait().expect("poll task helper").is_some()
                {
                    panic!("task timeout helper exited before becoming ready");
                }
                if Instant::now() >= deadline {
                    let _ = signal_group(running.record.pid, libc::SIGKILL);
                    let _ = running.child.as_mut().expect("owned helper").wait();
                    panic!("task timeout helper did not become ready within the test bound");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// Reaps a task after the fake-clock decision has been made. Process
        /// exit and `Child::try_wait` are real-time OS observations, so keep
        /// polling at the same fake time under a finite wall-clock bound.
        fn reap_task_until_finished(
            control: &Path,
            id: &str,
            running: &mut RunningTask,
            clock: &dyn Clock,
            first_tick: TaskTick,
        ) {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut tick = first_tick;
            loop {
                if matches!(tick, TaskTick::Finished) {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task {id} did not exit within the bounded real-time reap loop"
                );
                std::thread::sleep(Duration::from_millis(20));
                tick = tick_one_task(control, id, running, clock).expect("tick");
            }
        }

        fn assert_terminal_task_event(callback_inbox: &Path, task_id: &str) {
            let mailbox = mailbox::Mailbox::open(callback_inbox).expect("open");
            let claimed = mailbox
                .claim_next()
                .expect("claim")
                .expect("terminal event present");
            assert_eq!(claimed.key, format!("{task_id}-terminal"));
        }

        fn wait_for_zombie_group_descendant(child: &Child) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let zombie = matches!(
                    process_probe(child.id()),
                    ProbeResult::Present(probe) if probe.is_zombie()
                );
                if zombie && task_group_liveness(child.id()) == Liveness::Live {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task leader did not become a zombie with a live group descendant"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// Waits until the direct shell exits while its background child
        /// keeps the task's process group alive. The bounded wait avoids
        /// making the process-group regressions depend on scheduler timing.
        fn wait_for_group_descendant(running: &mut RunningTask) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let direct_exited = running
                    .child
                    .as_mut()
                    .expect("owned task child")
                    .try_wait()
                    .expect("poll task child")
                    .is_some();
                if direct_exited && task_group_liveness(running.record.pid) == Liveness::Live {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "task direct child did not exit with a live group descendant"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        fn assert_durable_task_state(control: &Path, id: &str, state: TaskState) -> TaskRecord {
            let record = read_task_record(control, id)
                .expect("read durable task")
                .expect("durable task record");
            assert_eq!(record.state, state);
            record
        }

        /// Failed terminal callback delivery backs off from one second and
        /// still redelivers the deterministic event when the inbox recovers.
        #[test]
        fn terminal_delivery_uses_fake_clock_backoff_and_recovers() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("terminal-delivery-retry");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let mut running =
                terminal_running_task(&dir.path, "task-terminal-retry", &callback_inbox, &clock);

            match tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
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
                tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                    .expect("early terminal retry tick"),
                TaskTick::StillRunning
            ));
            fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");

            clock.advance(1);
            assert!(
                matches!(
                    tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                        .expect("recovered terminal delivery"),
                    TaskTick::StillRunning
                ),
                "delivery succeeds but the record is retained until it ages out"
            );
            assert_terminal_task_event(&callback_inbox, "task-terminal-retry");

            // Re-ticking before retention elapses neither redelivers nor reaps.
            clock.advance(running.retention_ms - 1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
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
            assert_durable_task_state(&dir.path, "task-terminal-retry", TaskState::Completed);

            // Once retention elapses the record is reaped.
            clock.advance(1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-terminal-retry", &mut running, &clock)
                    .expect("terminal tick at retention boundary"),
                TaskTick::Finished
            ));
            assert!(
                read_task_record(&dir.path, "task-terminal-retry")
                    .expect("read reaped task")
                    .is_none(),
                "the retained record is removed once retention elapses"
            );
        }

        /// Persistent terminal callback failure is retried at most the
        /// configured bound, with no delay growing beyond one minute, then
        /// returns `Finished` so `tick_tasks` drops the tracker entry.
        #[test]
        fn terminal_delivery_drops_after_bounded_backoff_attempts() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("terminal-delivery-drop");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let mut running =
                terminal_running_task(&dir.path, "task-terminal-drop", &callback_inbox, &clock);
            let mut expected_delay = EVENT_RETRY_INITIAL_DELAY_MS;

            for attempt in 1..=MAX_EVENT_DELIVERY_ATTEMPTS {
                if attempt > 1 {
                    clock.advance(running.terminal_retry_delay_ms);
                }
                let tick = tick_one_task(&dir.path, "task-terminal-drop", &mut running, &clock)
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
        }

        /// Failed milestone callback delivery backs off from one second on the
        /// same schedule as terminal delivery, does not advance
        /// `delivered_milestones` while it fails, and delivers the milestone
        /// exactly once when the inbox recovers.
        #[test]
        fn milestone_delivery_uses_fake_clock_backoff_and_recovers() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("milestone-delivery-retry");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![50],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-m-retry", spec, &clock);

            // Milestone becomes due; delivery fails and schedules a retry a
            // second out. The live child keeps the tick `StillRunning`.
            clock.advance(60);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-retry", &mut running, &clock)
                    .expect("first milestone delivery attempt"),
                TaskTick::StillRunning
            ));
            assert_eq!(running.milestone_delivery_attempts, 1);
            assert_eq!(
                running.next_milestone_retry_ms,
                Some(60 + EVENT_RETRY_INITIAL_DELAY_MS)
            );
            assert_eq!(running.record.delivered_milestones, 0);

            // Before the backoff elapses the milestone is not re-attempted.
            clock.advance(EVENT_RETRY_INITIAL_DELAY_MS - 1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-retry", &mut running, &clock)
                    .expect("early milestone retry tick"),
                TaskTick::StillRunning
            ));
            assert_eq!(running.milestone_delivery_attempts, 1);
            assert_eq!(running.record.delivered_milestones, 0);

            // Inbox recovers; the milestone is delivered once and the backoff
            // state is cleared.
            fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");
            clock.advance(1);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-retry", &mut running, &clock)
                    .expect("recovered milestone delivery"),
                TaskTick::StillRunning
            ));
            assert_eq!(running.record.delivered_milestones, 1);
            assert_eq!(running.milestone_delivery_attempts, 0);
            assert_eq!(running.next_milestone_retry_ms, None);

            let mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open");
            let claimed = mailbox
                .claim_next()
                .expect("claim")
                .expect("milestone event present");
            assert_eq!(claimed.key, "task-m-retry-milestone-0");
            assert!(
                mailbox.claim_next().expect("claim").is_none(),
                "milestone delivered exactly once"
            );

            let _ = signal_group(running.record.pid, libc::SIGKILL);
            let _ = running.child.as_mut().expect("owned child").wait();
        }

        /// A milestone already delivered before a later milestone's delivery
        /// fails is never redelivered when the inbox recovers: the failure
        /// backs off the stuck index only, and `delivered_milestones` never
        /// regresses.
        #[test]
        fn milestone_batch_failure_does_not_redeliver_earlier_index() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("milestone-batch-retry");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![10, 20],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-m-batch", spec, &clock);

            // Milestone 0 delivers while the inbox is available.
            clock.advance(15);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-batch", &mut running, &clock)
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

            // The inbox goes away — including its dedup ledger — so a
            // spurious redelivery of milestone 0 would reappear on recovery.
            fs::remove_dir_all(&callback_inbox).expect("drop callback inbox");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");

            // Milestone 1 becomes due and fails; milestone 0 stays delivered.
            clock.advance(10);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-batch", &mut running, &clock)
                    .expect("milestone 1 delivery attempt"),
                TaskTick::StillRunning
            ));
            assert_eq!(running.milestone_delivery_attempts, 1);
            assert_eq!(running.record.delivered_milestones, 1);

            // Inbox recovers; only milestone 1 is delivered.
            fs::remove_file(&callback_inbox).expect("remove unavailable callback marker");
            clock.advance(EVENT_RETRY_INITIAL_DELAY_MS);
            assert!(matches!(
                tick_one_task(&dir.path, "task-m-batch", &mut running, &clock)
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

            let _ = signal_group(running.record.pid, libc::SIGKILL);
            let _ = running.child.as_mut().expect("owned child").wait();
        }

        /// Persistent milestone callback failure is retried at most the
        /// configured bound, then the milestone is dropped and the advance past
        /// it is persisted — so a supervisor restart's `rehydrate_tasks` does
        /// not re-enter the same stuck milestone.
        #[test]
        fn milestone_delivery_drops_after_bounded_backoff_and_persists_advance() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("milestone-delivery-drop");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            // Max duration is far past the cumulative backoff so this test
            // exercises only the milestone drop, not timeout escalation.
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![50],
                3_600_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-m-drop", spec, &clock);

            clock.advance(60);
            for attempt in 1..=MAX_EVENT_DELIVERY_ATTEMPTS {
                if attempt > 1 {
                    clock.advance(running.milestone_retry_delay_ms);
                }
                assert!(matches!(
                    tick_one_task(&dir.path, "task-m-drop", &mut running, &clock)
                        .expect("milestone delivery attempt"),
                    TaskTick::StillRunning
                ));
                if attempt < MAX_EVENT_DELIVERY_ATTEMPTS {
                    assert_eq!(running.milestone_delivery_attempts, attempt);
                    assert_eq!(running.record.delivered_milestones, 0);
                }
            }

            // The milestone is dropped: the in-memory advance and its durable
            // record both move past it, and the backoff state is cleared.
            assert_eq!(running.record.delivered_milestones, 1);
            assert_eq!(running.milestone_delivery_attempts, 0);
            assert_eq!(running.next_milestone_retry_ms, None);
            let durable = read_task_record(&dir.path, "task-m-drop")
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

            let _ = signal_group(running.record.pid, libc::SIGKILL);
            let _ = running.child.as_mut().expect("owned child").wait();
        }

        /// A stuck milestone delivery must not block the tick's supervision:
        /// the max-duration escalation still sends SIGTERM and the child is
        /// still reaped to a terminal state while the milestone stays undel-
        /// ivered and backed off.
        #[test]
        fn supervision_continues_while_milestone_delivery_is_stuck() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("milestone-stuck-supervision");
            let clock = FakeClock::new();
            let helper = task_timeout_helper_path();
            let callback_inbox = dir.path.join("callback");
            fs::write(&callback_inbox, "callback unavailable").expect("make callback a file");
            let ready = dir.path.join("stuck.ready");
            let spec = task_spec(
                "svc-1",
                &helper.display().to_string(),
                vec![
                    "--mode".to_string(),
                    "exit-on-term".to_string(),
                    "--ready-file".to_string(),
                    ready.display().to_string(),
                ],
                vec![1],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-m-stuck", spec, &clock);
            wait_for_task_helper(&mut running, &ready);

            // Past both the milestone threshold and the max duration: the
            // milestone delivery fails, but the tick still escalates SIGTERM.
            clock.advance(150);
            let tick =
                tick_one_task(&dir.path, "task-m-stuck", &mut running, &clock).expect("stuck tick");
            assert!(
                running.term_sent_at_ms.is_some(),
                "max-duration breach must send SIGTERM even while a milestone is stuck"
            );
            assert_eq!(running.milestone_delivery_attempts, 1);
            assert_eq!(running.record.delivered_milestones, 0);

            // The child exits on SIGTERM and is reaped to a terminal state even
            // though the callback inbox is still unavailable.
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut tick = tick;
            while running.record.state == TaskState::Running {
                assert!(
                    Instant::now() < deadline,
                    "task was not reaped while its milestone delivery stayed stuck"
                );
                std::thread::sleep(Duration::from_millis(20));
                tick = tick_one_task(&dir.path, "task-m-stuck", &mut running, &clock)
                    .expect("reap tick");
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
        }

        /// A supervisor tick that races external cleanup must not recreate a
        /// durable task record after the cleanup has removed it.
        #[test]
        fn finalize_task_does_not_resurrect_removed_record() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("finalize-removed-task");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let mut running =
                terminal_running_task(&dir.path, "task-finalize-removed", &callback_inbox, &clock);
            remove_task_record(&dir.path, "task-finalize-removed").expect("remove task record");

            assert!(matches!(
                finalize_task(&dir.path, &mut running, TaskState::Failed, None, 10, &clock,)
                    .expect("finalize removed task"),
                TaskTick::Finished
            ));
            assert!(
                read_task_record(&dir.path, "task-finalize-removed")
                    .expect("read removed task")
                    .is_none(),
                "finalizing an externally removed task does not resurrect its record"
            );
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
                    other => panic!("unexpected terminal delivery outcome: {other:?}"),
                }
            }
            assert_eq!(running.record.state, TaskState::Completed);
            assert_eq!(running.record.exit_code, Some(0));
        }

        /// A direct shell exit does not finalize a task while its same-group
        /// background descendant remains alive. The retained Child handle
        /// keeps the direct exit status available for completion after drain.
        #[test]
        fn tick_one_task_waits_for_same_group_descendant_before_completion() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("tick-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-group-drain", spec, &clock);

            wait_for_group_descendant(&mut running);
            let tick = tick_one_task(&dir.path, "task-group-drain", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-group-drain", TaskState::Running);

            let mut status = Vec::new();
            execute_task_status(&dir.path, Some("task-group-drain"), &mut status)
                .expect("status while group remains");
            let status: serde_json::Value = serde_json::from_slice(&status).expect("status JSON");
            assert_eq!(status["tasks"][0]["state"], "running");
            assert!(
                matches!(
                    status["tasks"][0]["liveness"].as_str(),
                    Some("live") | Some("unresolved")
                ),
                "an incomplete group scan remains safe: {status}"
            );

            signal_group(running.record.pid, libc::SIGKILL).expect("kill group descendant");
            reap_task_until_finished(&dir.path, "task-group-drain", &mut running, &clock, tick);
            // Zero retention reaps the durable record on this same tick, so
            // the terminal state can only be checked against the in-memory
            // record `finalize_task` updates before reaping, not by re-reading it.
            assert_eq!(running.record.state, TaskState::Completed);
            assert_eq!(running.record.exit_code, Some(0));
            assert_terminal_task_event(&callback_inbox, "task-group-drain");
        }

        /// Cancellation after the direct leader exits still reaches a live
        /// same-group descendant and records `cancelled` only after drain.
        #[test]
        fn task_cancel_reaches_same_group_descendant_after_leader_exit() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("cancel-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-cancel-group", spec, &clock);

            wait_for_group_descendant(&mut running);
            let tick = tick_one_task(&dir.path, "task-cancel-group", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-cancel-group", TaskState::Running);

            execute_task_cancel(&dir.path, "task-cancel-group", &mut Vec::new())
                .expect("cancel task");
            reap_task_until_finished(&dir.path, "task-cancel-group", &mut running, &clock, tick);
            // Zero retention already reaped the durable record by this
            // point; the in-memory record is the only place left to check it.
            assert_eq!(running.record.state, TaskState::Cancelled);
            assert_eq!(task_group_liveness(running.record.pid), Liveness::Dead);
            assert_terminal_task_event(&callback_inbox, "task-cancel-group");
        }

        /// Timeout escalation never signals an owned task while its process
        /// identity is unresolved, even when the duration has elapsed.
        #[test]
        fn task_timeout_does_not_signal_unresolved_owned_task() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("timeout-unresolved-owned");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running =
                spawn_running_task(&dir.path, "task-timeout-unresolved", spec, &clock);
            running.record.started_at = None;
            running.record.start_epoch_secs = None;
            running.record.started_ms = Some(SystemClock.now_ms().saturating_add(10_000));
            running.record.spec.command = "not-the-running-command".to_string();

            clock.advance(100);
            let first_tick =
                tick_one_task(&dir.path, "task-timeout-unresolved", &mut running, &clock)
                    .expect("unresolved timeout tick");
            assert!(matches!(first_tick, TaskTick::StillRunning));
            assert_eq!(running.term_sent_at_ms, None);
            assert!(!running.kill_sent);

            running.term_sent_at_ms = Some(clock.now_ms().saturating_sub(KILL_GRACE_MS));
            let second_tick =
                tick_one_task(&dir.path, "task-timeout-unresolved", &mut running, &clock)
                    .expect("unresolved timeout escalation tick");
            assert!(matches!(second_tick, TaskTick::StillRunning));
            assert!(!running.kill_sent);

            let mut child = running.child.take().expect("unresolved task child");
            assert!(child.try_wait().expect("poll unresolved task").is_none());
            signal_group(running.record.pid, libc::SIGKILL).expect("clean up unresolved task");
            child.wait().expect("wait for unresolved task");
        }

        /// A `/proc` entry that disappears mid-scan is absence of evidence,
        /// not evidence of absence: the group probe must keep reporting
        /// `Live` for a leader-exited group whose descendant is alive, even
        /// while unrelated processes exit continuously underneath the scan.
        #[cfg(target_os = "linux")]
        #[test]
        fn group_liveness_survives_unrelated_process_churn_after_leader_exit() {
            /// Terminates and reaps the churn helper on every exit path,
            /// including a panicking assertion.
            struct ChurnGuard(Child);
            impl Drop for ChurnGuard {
                fn drop(&mut self) {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }

            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("group-scan-churn");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-group-churn", spec, &clock);

            wait_for_group_descendant(&mut running);
            assert!(
                running
                    .child
                    .as_mut()
                    .expect("owned task child")
                    .try_wait()
                    .expect("poll task child")
                    .is_some(),
                "the direct leader must have exited before the group is probed"
            );
            assert_eq!(
                task_group_liveness(running.record.pid),
                Liveness::Live,
                "a non-zombie same-group descendant must hold the group open"
            );

            let churn = ChurnGuard(
                Command::new("sh")
                    .args(["-c", "while :; do /bin/true; done"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("spawn churn helper"),
            );
            const PROBES: usize = 30;
            for probe in 0..PROBES {
                assert_eq!(
                    task_group_liveness(running.record.pid),
                    Liveness::Live,
                    "probe {probe} of {PROBES}: a vanished unrelated process must not \
                     make the group scan unresolved"
                );
            }

            drop(churn);
            signal_group(running.record.pid, libc::SIGKILL).expect("kill group descendant");
        }

        /// Max-duration escalation after the direct leader exits still
        /// reaches a TERM-ignoring same-group descendant, then records
        /// `timeout` after SIGKILL drains the group.
        #[test]
        fn task_timeout_reaches_same_group_descendant_after_leader_exit() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("timeout-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-timeout-group", spec, &clock);

            wait_for_group_descendant(&mut running);
            let first_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("tick while group remains");
            assert!(matches!(first_tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-timeout-group", TaskState::Running);

            clock.advance(100);
            let term_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("timeout TERM tick");
            assert!(matches!(term_tick, TaskTick::StillRunning));
            assert!(running.term_sent_at_ms.is_some());
            assert_durable_task_state(&dir.path, "task-timeout-group", TaskState::Running);
            assert_eq!(
                unsafe { libc::kill(-(running.record.pid as libc::pid_t), 0) },
                0,
                "the TERM-ignoring descendant keeps the process group alive"
            );

            clock.advance(KILL_GRACE_MS);
            let kill_tick = tick_one_task(&dir.path, "task-timeout-group", &mut running, &clock)
                .expect("timeout KILL tick");
            assert!(running.kill_sent);
            reap_task_until_finished(
                &dir.path,
                "task-timeout-group",
                &mut running,
                &clock,
                kill_tick,
            );
            // Zero retention already reaped the durable record by this
            // point; the in-memory record is the only place left to check it.
            assert_eq!(running.record.state, TaskState::Timeout);
            assert_eq!(task_group_liveness(running.record.pid), Liveness::Dead);
            assert_terminal_task_event(&callback_inbox, "task-timeout-group");
        }

        /// An owned task reuses its cached group sample after the direct
        /// leader exits. Timeout TERM and KILL decisions still force fresh
        /// samples inside the cadence window.
        #[cfg(target_os = "linux")]
        #[test]
        fn owned_linux_group_scan_is_rate_limited_and_forced_for_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("owned-linux-group-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-owned-linux-cache", spec, &clock);

            wait_for_group_descendant(&mut running);
            reset_group_scan_count();
            assert!(matches!(
                tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock),
                Ok(TaskTick::StillRunning)
            ));
            assert_eq!(
                group_scan_count(),
                1,
                "the first owned reaped-leader tick scans /proc once"
            );

            // The timeout is due at 100ms. TERM forces a fresh sample even
            // though the first owned group sample is still inside the cache.
            clock.advance(100);
            assert!(matches!(
                tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock),
                Ok(TaskTick::StillRunning)
            ));
            assert!(running.term_sent_at_ms.is_some());
            assert_eq!(group_scan_count(), 2, "TERM uses a fresh group sample");

            clock.advance(100);
            tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock)
                .expect("cached owned group liveness tick");
            assert_eq!(
                group_scan_count(),
                2,
                "owned group scan remains cached inside the window"
            );

            // The sample was refreshed at 100ms, so 600ms is the exact
            // inclusive cache boundary.
            clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 100);
            tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock)
                .expect("owned boundary group liveness tick");
            assert_eq!(
                group_scan_count(),
                3,
                "the exact boundary refreshes the owned group scan"
            );

            // Refresh shortly before KILL, then verify that the due decision
            // forces another sample inside the cache window.
            let term_sent_at = running.term_sent_at_ms.expect("TERM timestamp");
            let pre_kill_at = term_sent_at + KILL_GRACE_MS - 100;
            clock.advance(pre_kill_at - clock.now_ms());
            tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock)
                .expect("owned pre-KILL group liveness tick");
            assert_eq!(
                group_scan_count(),
                4,
                "pre-KILL sample refreshes after expiry"
            );

            clock.advance(100);
            let kill_tick =
                tick_one_task(&dir.path, "task-owned-linux-cache", &mut running, &clock)
                    .expect("forced owned KILL liveness tick");
            assert!(matches!(kill_tick, TaskTick::StillRunning));
            assert!(running.kill_sent);
            assert_eq!(group_scan_count(), 5, "KILL uses a fresh group sample");

            clock.advance(REHYDRATED_LIVENESS_CACHE_MS);
            reap_task_until_finished(
                &dir.path,
                "task-owned-linux-cache",
                &mut running,
                &clock,
                kill_tick,
            );
            // Zero retention already reaped the durable record by this
            // point; the in-memory record is the only place left to check it.
            assert_eq!(running.record.state, TaskState::Timeout);
            assert_terminal_task_event(&callback_inbox, "task-owned-linux-cache");
        }

        /// Linux rehydrated liveness probes the direct PID on every tick but
        /// rate-limits the table-wide group scan. Timeout TERM and KILL
        /// decisions force a fresh group sample even inside the cache window.
        #[cfg(target_os = "linux")]
        #[test]
        fn rehydrated_linux_group_scan_is_rate_limited_and_forced_for_timeout() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-linux-group-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                100,
                &callback_inbox.display().to_string(),
            );
            let mut owned =
                spawn_running_task(&dir.path, "task-rehydrated-linux-cache", spec, &clock);

            wait_for_group_descendant(&mut owned);
            owned
                .child
                .take()
                .expect("owned task child")
                .wait()
                .expect("wait for direct leader");
            let mut rehydrated =
                RunningTask::new(owned.record.clone(), None, None, owned.started_ms)
                    .with_retention_ms(0);

            reset_process_probe_count();
            reset_group_scan_count();
            assert!(matches!(
                tick_one_task(
                    &dir.path,
                    "task-rehydrated-linux-cache",
                    &mut rehydrated,
                    &clock,
                ),
                Ok(TaskTick::StillRunning)
            ));
            assert_eq!(
                process_probe_count(),
                1,
                "direct PID is checked on the first tick"
            );
            assert_eq!(
                group_scan_count(),
                1,
                "the first group sample scans /proc once"
            );

            // The timeout is due at 100ms, while the initial group sample is
            // still fresh. The signal decision nevertheless forces a scan.
            clock.advance(100);
            assert!(matches!(
                tick_one_task(
                    &dir.path,
                    "task-rehydrated-linux-cache",
                    &mut rehydrated,
                    &clock,
                ),
                Ok(TaskTick::StillRunning)
            ));
            assert!(rehydrated.term_sent_at_ms.is_some());
            assert_eq!(
                process_probe_count(),
                2,
                "direct PID is checked before TERM"
            );
            assert_eq!(group_scan_count(), 2, "TERM uses a fresh group sample");

            clock.advance(100);
            tick_one_task(
                &dir.path,
                "task-rehydrated-linux-cache",
                &mut rehydrated,
                &clock,
            )
            .expect("cached group liveness tick");
            assert_eq!(process_probe_count(), 3, "direct PID remains per-tick");
            assert_eq!(
                group_scan_count(),
                2,
                "group scan remains cached inside the window"
            );

            // The sample was refreshed at 100ms, so 600ms is the exact
            // inclusive cache boundary.
            clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 100);
            tick_one_task(
                &dir.path,
                "task-rehydrated-linux-cache",
                &mut rehydrated,
                &clock,
            )
            .expect("boundary group liveness tick");
            assert_eq!(
                process_probe_count(),
                4,
                "direct PID remains per-tick at the boundary"
            );
            assert_eq!(
                group_scan_count(),
                3,
                "the exact boundary refreshes the group scan"
            );

            // Refresh the group sample shortly before the real KILL deadline,
            // then verify the due decision forces another sample inside the
            // cache window without waiting in wall-clock time.
            let term_sent_at = rehydrated.term_sent_at_ms.expect("TERM timestamp");
            let pre_kill_at = term_sent_at + KILL_GRACE_MS - 100;
            clock.advance(pre_kill_at - clock.now_ms());
            tick_one_task(
                &dir.path,
                "task-rehydrated-linux-cache",
                &mut rehydrated,
                &clock,
            )
            .expect("pre-KILL group liveness tick");
            assert_eq!(
                process_probe_count(),
                5,
                "direct PID remains per-tick before KILL"
            );
            assert_eq!(
                group_scan_count(),
                4,
                "pre-KILL sample refreshes after expiry"
            );

            clock.advance(100);
            let kill_tick = tick_one_task(
                &dir.path,
                "task-rehydrated-linux-cache",
                &mut rehydrated,
                &clock,
            )
            .expect("forced KILL liveness tick");
            assert!(matches!(kill_tick, TaskTick::StillRunning));
            assert!(rehydrated.kill_sent);
            assert_eq!(
                process_probe_count(),
                6,
                "direct PID is checked before KILL"
            );
            assert_eq!(group_scan_count(), 5, "KILL uses a fresh group sample");

            clock.advance(REHYDRATED_LIVENESS_CACHE_MS);
            reap_task_until_finished(
                &dir.path,
                "task-rehydrated-linux-cache",
                &mut rehydrated,
                &clock,
                kill_tick,
            );
            assert_eq!(rehydrated.record.state, TaskState::Timeout);
            assert_terminal_task_event(&callback_inbox, "task-rehydrated-linux-cache");
        }

        /// Rehydrated liveness is sampled once per tick and cached between
        /// ticks. The fake clock makes the 500 ms cadence deterministic while
        /// the process-probe seam proves that the supervisor does not repeat
        /// the same sample at each decision point.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn rehydrated_task_liveness_is_deduplicated_and_rate_limited() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-liveness-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                60_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-rehydrated-cache", spec, &clock);
            let mut child = running.child.take().expect("rehydrated task child");

            reset_process_probe_count();
            assert!(matches!(
                tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock,),
                Ok(TaskTick::StillRunning)
            ));
            assert_eq!(process_probe_count(), 1);

            clock.advance(100);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("cached liveness tick");
            assert_eq!(process_probe_count(), 1);

            clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 101);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("cached liveness tick before refresh");
            assert_eq!(process_probe_count(), 1);

            clock.advance(1);
            tick_one_task(&dir.path, "task-rehydrated-cache", &mut running, &clock)
                .expect("refresh liveness tick");
            assert_eq!(process_probe_count(), 2);

            signal_group(running.record.pid, libc::SIGKILL).expect("clean up rehydrated task");
            child.wait().expect("wait for rehydrated task");
        }

        /// The owned-drain arm (leader reaped, group descendant still alive)
        /// now shares the same rehydrated liveness cache instead of
        /// re-probing on every 100ms tick. `max_duration_ms` is set far in
        /// the future so only the cache is under test, not TERM/KILL
        /// escalation (the force-refresh-before-signalling rule is preserved
        /// by construction, since escalation always passes
        /// `TaskLivenessRefresh::Forced`, and is exercised end-to-end by
        /// `owned_linux_group_scan_is_rate_limited_and_forced_for_timeout`'s
        /// shared `task_tick.rs` logic on Linux).
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn owned_drain_liveness_is_rate_limited() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("owned-drain-liveness-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                3_600_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-owned-drain-cache", spec, &clock);
            wait_for_group_descendant(&mut running);

            reset_process_probe_count();
            assert!(matches!(
                tick_one_task(&dir.path, "task-owned-drain-cache", &mut running, &clock),
                Ok(TaskTick::StillRunning)
            ));
            let first_sample = process_probe_count();
            assert!(
                first_sample >= 1,
                "the first owned reaped-leader tick samples liveness"
            );

            clock.advance(100);
            tick_one_task(&dir.path, "task-owned-drain-cache", &mut running, &clock)
                .expect("cached owned liveness tick");
            assert_eq!(
                process_probe_count(),
                first_sample,
                "owned drain liveness remains cached inside the 500ms window"
            );

            clock.advance(REHYDRATED_LIVENESS_CACHE_MS - 100);
            tick_one_task(&dir.path, "task-owned-drain-cache", &mut running, &clock)
                .expect("owned boundary liveness tick");
            let refreshed_sample = process_probe_count();
            assert!(
                refreshed_sample > first_sample,
                "the exact boundary refreshes the owned drain liveness"
            );

            clock.advance(100);
            tick_one_task(&dir.path, "task-owned-drain-cache", &mut running, &clock)
                .expect("owned re-cached liveness tick");
            assert_eq!(
                process_probe_count(),
                refreshed_sample,
                "owned drain liveness remains cached again after the refresh"
            );

            signal_group(running.record.pid, libc::SIGKILL).expect("clean up owned-drain task");
        }

        /// Record-based task waits keep the direct PID probe live on every
        /// Linux poll while limiting the expensive process-group scan to the
        /// shared 500ms budget. Session waits retain their per-poll direct
        /// liveness check. Both fixtures are real children so cleanup covers
        /// the process groups used by the production paths.
        #[cfg(target_os = "linux")]
        #[test]
        fn record_grace_waits_rate_limit_linux_group_scans() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("record-grace-linux-cache");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let task = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    "trap '' TERM; sleep 30 & exit 0".to_string(),
                ],
                vec![],
                60_000,
                &callback_inbox.display().to_string(),
            );
            let mut running = spawn_running_task(&dir.path, "task-grace-linux", task, &clock);
            wait_for_group_descendant(&mut running);

            let grace_ms = REHYDRATED_LIVENESS_CACHE_MS * 4 + POLL_INTERVAL_MS;
            reset_group_scan_count();
            reset_process_probe_count();
            assert_eq!(
                task_execution_liveness_after_retry(&running.record, POLL_INTERVAL_MS),
                Liveness::Live,
                "the retry helper recognizes the live descendant group"
            );
            assert_eq!(
                group_scan_count(),
                1,
                "the retry helper performs one fresh group scan"
            );
            reset_group_scan_count();
            let started = Instant::now();
            wait_while_task_alive(&running.record, grace_ms);
            assert!(
                started.elapsed() >= Duration::from_millis(grace_ms),
                "task wait returned before its deadline"
            );
            assert!(
                group_scan_count() <= grace_ms / REHYDRATED_LIVENESS_CACHE_MS + 1,
                "task group scans exceeded the 500ms budget: {}",
                group_scan_count()
            );
            assert!(
                process_probe_count() > group_scan_count(),
                "Linux task wait no longer performs the direct probe per poll"
            );
            signal_group(running.record.pid, libc::SIGKILL).expect("kill task fixture group");
            running
                .child
                .take()
                .expect("task fixture child")
                .wait()
                .expect("reap task fixture");

            let session_spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let session_logs = task_logs_dir(&dir.path, "session-grace-linux");
            fs::create_dir_all(&session_logs).expect("create session fixture logs");
            let mut session_child = spawn_task_child(
                &session_spec,
                &session_logs.join("stdout.log"),
                &session_logs.join("stderr.log"),
            )
            .expect("spawn session fixture");
            let (started_at, start_epoch_secs) = recorded_start_identity(session_child.id());
            let session = SessionRecord {
                id: "svc-grace-linux".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: session_child.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };
            reset_process_probe_count();
            let started = Instant::now();
            wait_while_alive(&session, grace_ms);
            assert!(
                started.elapsed() >= Duration::from_millis(grace_ms),
                "session wait returned before its deadline"
            );
            assert!(
                process_probe_count() > 1,
                "Linux session wait no longer performs its per-poll direct probe"
            );
            signal_group(session.pid, libc::SIGKILL).expect("kill session fixture group");
            session_child.wait().expect("reap session fixture");
        }

        /// Non-Linux record-based waits cache their complete `ps` liveness
        /// chain, including direct-leader and process-group probes. Task and
        /// session fixtures both exercise the cache and are explicitly
        /// reaped after the wall-clock cadence check.
        #[cfg(not(target_os = "linux"))]
        #[test]
        fn record_grace_waits_rate_limit_non_linux_probe_chain() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("record-grace-non-linux-cache");
            let clock = FakeClock::new();
            let grace_ms = REHYDRATED_LIVENESS_CACHE_MS * 4 + POLL_INTERVAL_MS;
            // The leader holds its exit on this gate. `spawn_running_task`
            // records the start identity by probing `ps` *after* spawn, and a
            // zombie yields no identity at all: a leader that exits first
            // leaves a record whose zombie identity can never match, which
            // resolves as `Unresolved` instead of reaching the group probe.
            let gate = dir.path.join("leader-gate");
            let gate_arg = gate.display().to_string();
            assert!(
                !gate_arg.contains('\''),
                "fixture gate path is not shell-safe: {gate_arg}"
            );
            let task = task_spec(
                "svc-1",
                "sh",
                vec![
                    "-c".to_string(),
                    format!(
                        "trap '' TERM; sleep 30 & while [ ! -f '{gate_arg}' ]; do sleep 0.05; done; exit 0"
                    ),
                ],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let mut running = spawn_running_task(&dir.path, "task-grace-non-linux", task, &clock);
            let task_child = running.child.take().expect("task fixture child");
            let mut task_cleanup = ChildCleanup::new(task_child);
            assert!(
                running.record.started_at.is_some() && running.record.start_epoch_secs.is_some(),
                "task fixture recorded no start identity: the leader exited before its live probe"
            );
            fs::write(&gate, b"go").expect("release task fixture leader");
            wait_for_zombie_group_descendant(task_cleanup.child());

            reset_group_scan_count();
            assert_eq!(
                task_execution_liveness_after_retry(&running.record, POLL_INTERVAL_MS),
                Liveness::Live,
                "the retry helper recognizes the live descendant group"
            );
            assert_eq!(
                group_scan_count(),
                1,
                "the retry helper performs one fresh process-table scan"
            );

            let session_spec = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let session_logs = task_logs_dir(&dir.path, "session-grace-non-linux");
            fs::create_dir_all(&session_logs).expect("create session fixture logs");
            let session_child = spawn_task_child(
                &session_spec,
                &session_logs.join("stdout.log"),
                &session_logs.join("stderr.log"),
            )
            .expect("spawn session fixture");
            let mut session_cleanup = ChildCleanup::new(session_child);
            let (started_at, start_epoch_secs) = recorded_start_identity(session_cleanup.id());
            let session = SessionRecord {
                id: "svc-grace-non-linux".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: session_cleanup.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };

            // A live leader with no durable identity and an impossible
            // historical start time is unresolved without entering the group
            // path. Keep this separate from the zombie fixture: macOS may
            // reap that leader between probes, which would correctly take a
            // Gone -> group-Live path instead of exercising retry/deadline.
            let mut unresolved = running.record.clone();
            unresolved.pid = session.pid;
            unresolved.spec = session_spec;
            unresolved.started_at = None;
            unresolved.start_epoch_secs = None;
            unresolved.started_ms = Some(SystemClock.now_ms().saturating_add(10_000));
            let retry_grace_ms = POLL_INTERVAL_MS * 2;
            reset_process_probe_count();
            reset_group_scan_count();
            let started = Instant::now();
            assert_eq!(
                task_execution_liveness_after_retry(&unresolved, retry_grace_ms),
                Liveness::Unresolved,
                "a mismatched zombie remains unresolved after retry"
            );
            assert!(
                started.elapsed() >= Duration::from_millis(retry_grace_ms),
                "unresolved retry returned before its deadline"
            );
            assert_eq!(
                group_scan_count(),
                0,
                "mismatched identity never reaches the process-table scan"
            );
            reset_process_probe_count();
            reset_group_scan_count();
            let started = Instant::now();
            wait_while_task_alive(&running.record, grace_ms);
            assert!(
                started.elapsed() >= Duration::from_millis(grace_ms),
                "task wait returned before its deadline"
            );
            assert!(
                process_probe_count() <= 2 * (grace_ms / REHYDRATED_LIVENESS_CACHE_MS + 1),
                "task probe chain exceeded the 500ms budget: {}",
                process_probe_count()
            );
            assert!(
                group_scan_count() <= grace_ms / REHYDRATED_LIVENESS_CACHE_MS + 1,
                "task process-table scans exceeded the 500ms budget: {}",
                group_scan_count()
            );
            assert!(
                group_scan_count() > 1,
                "task wait did not exercise repeated process-table scans"
            );
            task_cleanup.reap();
            reset_process_probe_count();
            let started = Instant::now();
            wait_while_alive(&session, grace_ms);
            assert!(
                started.elapsed() >= Duration::from_millis(grace_ms),
                "session wait returned before its deadline"
            );
            assert!(
                process_probe_count() <= grace_ms / REHYDRATED_LIVENESS_CACHE_MS + 1,
                "session probe chain exceeded the 500ms budget: {}",
                process_probe_count()
            );
            session_cleanup.reap();
        }

        /// A restarted supervisor has no Child handle, but still waits for a
        /// same-group descendant after the recorded direct PID disappears.
        /// Once the group drains, the rehydrated path records failed/null
        /// because no direct exit status can be recovered.
        #[test]
        fn rehydrated_task_waits_for_same_group_descendant_before_failure() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rehydrate-group-drain");
            let clock = FakeClock::new();
            let callback_inbox = dir.path.join("callback");
            let spec = task_spec(
                "svc-1",
                "sh",
                vec!["-c".to_string(), "sleep 30 & exit 0".to_string()],
                vec![],
                10_000,
                &callback_inbox.display().to_string(),
            );
            let mut owned = spawn_running_task(&dir.path, "task-rehydrated-group", spec, &clock);

            wait_for_group_descendant(&mut owned);
            owned
                .child
                .take()
                .expect("owned task child")
                .wait()
                .expect("wait for direct leader");
            let mut rehydrated =
                RunningTask::new(owned.record.clone(), None, None, owned.started_ms)
                    .with_retention_ms(0);

            let tick = tick_one_task(&dir.path, "task-rehydrated-group", &mut rehydrated, &clock)
                .expect("rehydrated tick while group remains");
            assert!(matches!(tick, TaskTick::StillRunning));
            assert_durable_task_state(&dir.path, "task-rehydrated-group", TaskState::Running);

            signal_group(rehydrated.record.pid, libc::SIGKILL).expect("kill group descendant");
            reap_task_until_finished(
                &dir.path,
                "task-rehydrated-group",
                &mut rehydrated,
                &clock,
                tick,
            );
            // Zero retention reaps the durable record on this same tick, so
            // the terminal state can only be checked against the in-memory
            // record `finalize_task` updates before reaping, not by re-reading it.
            assert_eq!(rehydrated.record.state, TaskState::Failed);
            assert_eq!(rehydrated.record.exit_code, None);
            assert_terminal_task_event(&callback_inbox, "task-rehydrated-group");
        }

        // -- Session-scoped task reaping ----------------------------------

        /// `reap_session_tasks` reaps only the records owned by the given
        /// session, leaving another session's task record untouched.
        #[test]
        fn reap_session_tasks_is_scoped_to_the_owning_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("reap-scoped");
            let owned = TaskRecord {
                id: "task-owned".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb-a"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            let other = TaskRecord {
                id: "task-other".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-2", "true", vec![], vec![], 1_000, "/tmp/cb-b"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &owned).expect("write owned");
            write_task_record(&dir.path, &other).expect("write other");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            reap_session_tasks_with_wait(&mut admission, "svc-1", false, wait_while_task_alive)
                .expect("reap");

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
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-tasks");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            let task_record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
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
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &task_record).expect("write task");

            let mut out = Vec::new();
            execute_teardown(&dir.path, false, &mut out).expect("teardown");

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

        /// Spawns a real long-lived child to stand in for a live task, and
        /// returns it. `SIGTERM` ends it, so a reap ladder run against its
        /// record terminates within the first escalation round.
        #[cfg(target_os = "linux")]
        fn spawn_live_task_child(control: &Path, task_id: &str) -> Child {
            let log_dir = task_logs_dir(control, task_id);
            fs::create_dir_all(&log_dir).expect("create task log dir");
            spawn_task_child(
                &live_task_spec("svc-1"),
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn live task child")
        }

        /// A live child whose recorded argv can never corroborate its pid:
        /// the process runs the already-exec'd `sleep 30` while its record
        /// claims `bash -c 'exec sleep 30'`. That is the same unresolved
        /// identity state the fixtures previously reached by spawning `bash`
        /// and polling until it exec-replaced its own argv, except it holds
        /// from the first probe instead of depending on how promptly the
        /// runner schedules a shell startup. Returns the child and the spec
        /// to record for it.
        #[cfg(target_os = "linux")]
        fn spawn_unresolved_identity_child(
            stdout_path: &Path,
            stderr_path: &Path,
        ) -> (Child, TaskSpec) {
            let recorded = task_spec(
                "svc-1",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let spawned = task_spec(
                "svc-1",
                "sleep",
                vec!["30".to_string()],
                vec![],
                60_000,
                "/tmp/callback",
            );
            let child = spawn_task_child(&spawned, stdout_path, stderr_path)
                .expect("spawn unresolved-identity task");
            (child, recorded)
        }

        #[cfg(target_os = "linux")]
        fn live_task_spec(session: &str) -> TaskSpec {
            task_spec(
                session,
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                Vec::new(),
                60_000,
                "/tmp/cb",
            )
        }

        /// A committed `Running` record pinned to `pid`'s corroborated start
        /// identity, so `is_task_alive` reports it `Live`.
        #[cfg(target_os = "linux")]
        fn live_task_record(session: &str, task_id: &str, pid: u32) -> TaskRecord {
            let (started_at, start_epoch_secs) = recorded_start_identity(pid);
            TaskRecord {
                id: task_id.to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: live_task_spec(session),
                pid,
                started_at,
                start_epoch_secs,
                job: None,
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
        /// rather than simulated.
        #[cfg(target_os = "linux")]
        #[test]
        fn task_start_is_rejected_while_a_stop_owns_the_session() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("admit-stopping");
            let inbox = dir.path.join("inbox");
            fs::create_dir_all(&inbox).expect("create inbox");

            // A real child stands in for the session, so admission's own
            // liveness check keeps saying `Live` right up until the stop's
            // escalation ladder ends it.
            let mut child = spawn_live_task_child(&dir.path, "svc-stopping");
            let (started_at, start_epoch_secs) = recorded_start_identity(child.id());
            let session_record = SessionRecord {
                id: "svc-stopping".to_string(),
                spec: spec(
                    &inbox.display().to_string(),
                    &dir.path.join("outbox").display().to_string(),
                ),
                pid: child.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };
            assert_eq!(
                is_session_alive(&session_record),
                Liveness::Live,
                "fixture session is live before the stop begins"
            );
            write_session_record(&dir.path, &session_record).expect("write session");

            let spec_path = dir.path.join("racing-spec.json");
            fs::write(
                &spec_path,
                serde_json::to_string(&task_spec(
                    "svc-stopping",
                    "true",
                    Vec::new(),
                    Vec::new(),
                    1_000,
                    &dir.path.join("callback").display().to_string(),
                ))
                .expect("serialize spec"),
            )
            .expect("write spec");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let racing_outcome = std::cell::RefCell::new(None);
            let _ = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {
                    // Only the first grace window: later escalation rounds
                    // run after `SIGTERM`, when the owner is no longer live
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
                        admission::handle_task_start_request::<UnixServicePlatform>(
                            &dir.path,
                            "racing-request",
                            &spec_path,
                            &SystemClock,
                        )
                        .expect("handle racing start"),
                    );
                },
                |_, _| {},
            );
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
                    task_start_response_path(&dir.path, "racing-request").expect("response path"),
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
                !admission::session_stop_in_progress::<UnixServicePlatform>(
                    &dir.path,
                    "svc-stopping"
                )
                .expect("probe"),
                "the finished stop released its marker"
            );
            child.wait().expect("reap session stand-in");
        }

        /// The stop marker is released when the stop finishes, and a marker
        /// orphaned by a killed `service stop` cannot wedge admission: a
        /// reader whose recorded identity no longer resolves discards it.
        #[test]
        fn a_stale_session_stop_marker_does_not_wedge_admission() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stale-stop-marker");

            {
                let _claim = SessionStopGuard::claim(&dir.path, "svc-1").expect("claim");
                assert!(
                    admission::session_stop_in_progress::<UnixServicePlatform>(&dir.path, "svc-1")
                        .expect("probe"),
                    "a live stop owns the session"
                );
            }
            assert!(
                !admission::session_stop_in_progress::<UnixServicePlatform>(&dir.path, "svc-1")
                    .expect("probe"),
                "finishing the stop releases the marker"
            );

            let dir_path = admission::session_stop_markers_dir(&dir.path);
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
                !admission::session_stop_in_progress::<UnixServicePlatform>(&dir.path, "svc-1")
                    .expect("probe"),
                "a marker whose owner no longer resolves is stale"
            );
            assert!(
                !admission::session_stop_marker_path(&dir.path, "svc-1")
                    .expect("marker path")
                    .exists(),
                "and is cleared, so it costs at most one rejected start"
            );

            mailbox::atomic_write(&dir_path, &mailbox::file_name("svc-1"), "not json")
                .expect("write malformed marker");
            assert!(
                !admission::session_stop_in_progress::<UnixServicePlatform>(&dir.path, "svc-1")
                    .expect("probe"),
                "a malformed marker is not evidence of a live stop"
            );
        }

        /// The session grace window must not hold the admission lock: while a
        /// non-force stop is waiting out `STOP_GRACE_MS`, another party can
        /// still take `service.admission.lock`. Before #195 the lock was held
        /// for the whole stop, freezing the supervisor's request admission and
        /// every task-start client for up to the sum of the grace windows.
        #[cfg(target_os = "linux")]
        #[test]
        fn stop_grace_does_not_hold_the_admission_lock() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-grace-unlocked");
            let inbox = dir.path.join("inbox");
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

            let task_specification = task_spec(
                "svc-grace",
                "bash",
                vec!["-c".to_string(), "exec sleep 30".to_string()],
                Vec::new(),
                60_000,
                "/tmp/callback",
            );
            let log_dir = task_logs_dir(&dir.path, "grace-session");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let mut child = spawn_task_child(
                &task_specification,
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn session stand-in");
            let (started_at, start_epoch_secs) = recorded_start_identity(child.id());
            let session_record = SessionRecord {
                id: "svc-grace".to_string(),
                spec: spec(
                    &inbox.display().to_string(),
                    &dir.path.join("outbox").display().to_string(),
                ),
                pid: child.id(),
                started_at,
                start_epoch_secs,
                stderr_path: String::new(),
            };
            assert_eq!(
                is_session_alive(&session_record),
                Liveness::Live,
                "fixture session is live, so the stop enters its grace window"
            );
            write_session_record(&dir.path, &session_record).expect("write session record");

            let control = dir.path.clone();
            let stopper = std::thread::spawn(move || {
                let mut out = Vec::new();
                execute_stop(&control, "svc-grace", false, &mut out)
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

            // Non-blocking on purpose: a blocking acquire would simply wait
            // the grace out and pass against the old implementation too.
            let probe = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.path.join(ADMISSION_LOCK_FILE))
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
                read_session_record(&dir.path, "svc-grace")
                    .expect("read")
                    .is_none(),
                "the session is still stopped and its record removed"
            );
            child.wait().expect("reap session stand-in");
            drop(mailbox_lock);
        }

        /// A task start admitted while a grace wait had the admission lock
        /// released lands outside the reaper's snapshot. The final locked
        /// rescan reports a still-live racer as residue instead of dropping
        /// it, and the session record is retained.
        #[cfg(target_os = "linux")]
        #[test]
        fn rescan_reports_a_task_admitted_during_a_released_grace_wait() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rescan-live-racer");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            // A live record the reaper does see, so it reaches its grace
            // wait — the point at which the racing admission lands. `SIGTERM`
            // ends this one, so it leaves no residue of its own.
            let mut reaped_child = spawn_live_task_child(&dir.path, "task-reaped");
            let reaped = live_task_record("svc-1", "task-reaped", reaped_child.id());
            write_task_record(&dir.path, &reaped).expect("write reaped task");

            let mut child = spawn_live_task_child(&dir.path, "task-racer");
            let racer = live_task_record("svc-1", "task-racer", child.id());

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                // Stands in for a task start admitted while this wait had the
                // admission lock released.
                |_, _| {
                    write_task_record(&dir.path, &racer).expect("admit racing task");
                },
            )
            .expect("stop session");
            drop(admission);

            assert!(
                residue.iter().any(|entry| entry.id == "task-racer"),
                "the racing task is reported, not silently dropped: {residue:?}"
            );
            assert!(
                read_task_record(&dir.path, "task-racer")
                    .expect("read")
                    .is_some(),
                "the racing task record is retained for a later cleanup attempt"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_some(),
                "the session record is not removed while an owned task is outstanding"
            );

            signal_group(child.id(), libc::SIGKILL).expect("kill racer");
            child.wait().expect("reap racer");
            reaped_child
                .wait()
                .expect("reap the terminated fixture task");
        }

        /// The supervisor can tick a racing task to a terminal state before
        /// the rescan looks. Such a record gets the same cleanup the reaper
        /// applies to any terminal record — and, with nothing else
        /// outstanding, the session record is still removed, so the rescan
        /// does not block the success path.
        #[cfg(target_os = "linux")]
        #[test]
        fn rescan_cleans_up_a_terminal_task_admitted_during_a_released_grace_wait() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rescan-terminal-racer");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            // A live record the reaper does see, so it reaches its grace
            // wait — the point at which the racing admission lands. `SIGTERM`
            // ends this one, so it leaves no residue of its own.
            let mut reaped_child = spawn_live_task_child(&dir.path, "task-reaped");
            let reaped = live_task_record("svc-1", "task-reaped", reaped_child.id());
            write_task_record(&dir.path, &reaped).expect("write reaped task");

            let request_id = "racer-request";
            let racer = TaskRecord {
                id: "task-racer".to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
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

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                |_, _| {
                    write_task_record(&dir.path, &racer).expect("admit racing task");
                    mark_task_start_ack(&dir.path, request_id).expect("write acknowledgement");
                    request_task_cancel_sentinel(&dir.path, &racer.id).expect("write sentinel");
                },
            )
            .expect("stop session");
            drop(admission);

            assert!(residue.is_empty(), "nothing is outstanding: {residue:?}");
            assert!(
                read_task_record(&dir.path, "task-racer")
                    .expect("read")
                    .is_none(),
                "the terminal racing record is removed"
            );
            assert!(
                !task_start_ack_exists(&dir.path, request_id).expect("probe acknowledgement"),
                "its start transaction is removed too"
            );
            assert!(
                !task_cancel_sentinel_path(&dir.path, "task-racer").exists(),
                "its cancel sentinel is removed too"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none(),
                "the rescan does not block the success path"
            );
            reaped_child
                .wait()
                .expect("reap the terminated fixture task");
        }

        /// A task admitted for a session not yet reached in a teardown
        /// loop — or one already finished — is discovered the moment
        /// `unlocked_wait` releases the lock, via its embedded id-only
        /// refresh, not by a second full listing. One shared
        /// `AdmissionGuard`, mirroring how `execute_teardown_with_timeout`
        /// shares one guard across its session loop, proves the delta
        /// lands during svc-j's own wait.
        #[cfg(target_os = "linux")]
        #[test]
        fn unlocked_wait_discovers_a_task_admitted_for_a_later_session() {
            let _guard = serialize_forks_and_locks();
            TASK_FULL_LISTINGS.store(0, std::sync::atomic::Ordering::Relaxed);
            TASK_NEW_ID_PARSES.store(0, std::sync::atomic::Ordering::Relaxed);
            let dir = TempDir::new("unlocked-wait-later-session");

            let session_j = SessionRecord {
                id: "svc-j".to_string(),
                spec: spec("/tmp/in-j", "/tmp/out-j"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_j).expect("write svc-j");
            let session_k = SessionRecord {
                id: "svc-k".to_string(),
                spec: spec("/tmp/in-k", "/tmp/out-k"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_k).expect("write svc-k");

            // A live task the reaper does see, so svc-j's reap reaches its
            // task-level grace wait — the point at which the racing
            // admission for svc-k lands. `SIGTERM` ends this one.
            let mut task_j_child = spawn_live_task_child(&dir.path, "task-j");
            let task_j = live_task_record("svc-j", "task-j", task_j_child.id());
            write_task_record(&dir.path, &task_j).expect("write task-j");

            let racer = TaskRecord {
                id: "task-k-racer".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-k", "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
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

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue_j = stop_session_record_with_wait(
                &mut admission,
                &session_j,
                false,
                |_, _| {},
                // Stands in for a task admitted for a session this
                // teardown loop has not reached yet, then performs the
                // production grace wait: the ladder re-probes liveness as
                // soon as this returns, and signal delivery plus child
                // reaping are asynchronous, so a wait that returns
                // immediately would report the still-living task as residue.
                |record, grace_ms| {
                    write_task_record(&dir.path, &racer).expect("admit racing task for svc-k");
                    wait_while_task_alive(record, grace_ms);
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

            let residue_k = stop_session_record_with_wait(
                &mut admission,
                &session_k,
                false,
                |_, _| {},
                |_, _| {},
            )
            .expect("stop svc-k");
            drop(admission);

            assert!(
                residue_k.is_empty(),
                "svc-k has nothing outstanding: {residue_k:?}"
            );
            assert!(
                read_task_record(&dir.path, "task-k-racer")
                    .expect("read")
                    .is_none(),
                "the cached-terminal racer is removed when svc-k is stopped"
            );
            assert!(
                read_session_record(&dir.path, "svc-j")
                    .expect("read")
                    .is_none(),
                "svc-j's session record is removed"
            );
            assert!(
                read_session_record(&dir.path, "svc-k")
                    .expect("read")
                    .is_none(),
                "svc-k's session record is removed"
            );
            task_j_child.wait().expect("reap svc-j's task process");
        }

        /// The daemon's own supervisor tick can persist a terminal state for
        /// a task while this guard's cached copy still says `Running` and a
        /// grace wait is in flight. The post-wait recheck must see that
        /// write and stop the escalation ladder before ever reaching
        /// `SIGKILL` — continuing to act on the stale `Running` copy would
        /// risk signalling a PID the supervisor's own boundary has already
        /// released.
        #[cfg(target_os = "linux")]
        #[test]
        fn wait_then_recheck_terminal_stops_the_ladder_before_sigkill_when_the_supervisor_wins_the_race()
         {
            let _guard = serialize_forks_and_locks();
            TASK_CONFIRM_READS.store(0, std::sync::atomic::Ordering::Relaxed);
            let dir = TempDir::new("wait-then-recheck-terminal");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");

            // The ready file is only touched after `trap` installs, so
            // waiting for it rules out the race where `SIGTERM` arrives
            // before the ignore disposition is active.
            let ready = dir.path.join("stubborn-ready");
            let stubborn_spec = task_spec(
                "svc-1",
                "bash",
                vec![
                    "-c".to_string(),
                    format!("trap '' TERM; touch '{}'; exec sleep 30", ready.display()),
                ],
                Vec::new(),
                60_000,
                "/tmp/cb",
            );
            let log_dir = task_logs_dir(&dir.path, "task-stubborn");
            fs::create_dir_all(&log_dir).expect("create task log dir");
            let mut child = spawn_task_child(
                &stubborn_spec,
                &log_dir.join("stdout.log"),
                &log_dir.join("stderr.log"),
            )
            .expect("spawn SIGTERM-ignoring task");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ready.exists() {
                assert!(
                    Instant::now() < deadline,
                    "the SIGTERM-ignoring task did not become ready"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            let (started_at, start_epoch_secs) = recorded_start_identity(child.id());
            let record = TaskRecord {
                id: "task-stubborn".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: stubborn_spec,
                pid: child.id(),
                started_at,
                start_epoch_secs,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_task_record(&dir.path, &record).expect("write stubborn task");

            let invocations = std::sync::atomic::AtomicUsize::new(0);
            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                |wait_record, _| {
                    if invocations.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                        // Test seam simulating `task_tick::finalize_task`
                        // winning the race: not a faithful reproduction of
                        // the real supervisor's own exit-detection logic,
                        // which only persists a terminal state after
                        // observing the process's owned boundary dead. It
                        // exists solely to prove the confirm-then-decide
                        // discipline governs regardless of how the terminal
                        // record came to be.
                        let mut terminal = wait_record.clone();
                        terminal.state = TaskState::Completed;
                        terminal.exit_code = Some(0);
                        terminal.elapsed_ms = Some(1);
                        write_task_record(&dir.path, &terminal).expect("supervisor wins the race");
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
                read_task_record(&dir.path, "task-stubborn")
                    .expect("read")
                    .is_none(),
                "the terminal record is removed"
            );
            assert_eq!(
                TASK_CONFIRM_READS.load(std::sync::atomic::Ordering::Relaxed),
                2,
                "one entry confirmation plus one post-wait recheck"
            );
            assert!(
                child.try_wait().expect("try_wait").is_none(),
                "the real process is still running: SIGKILL was never sent"
            );

            signal_group(child.id(), libc::SIGKILL).expect("kill stubborn task");
            child.wait().expect("reap stubborn task");
        }

        /// A session that stops cleanly — dead, with nothing outstanding —
        /// has its captured `stderr.log` reclaimed along with the record that
        /// was the only pointer to it.
        #[test]
        fn stop_session_reclaims_the_session_log_dir() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-session-logs");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            let log_dir = session_logs_dir(&dir.path, "svc-1");
            fs::create_dir_all(&log_dir).expect("create session logs");
            fs::write(log_dir.join("stderr.log"), b"boom").expect("write captured stderr");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                |_, _| {},
            )
            .expect("stop session");
            drop(admission);

            assert!(residue.is_empty(), "nothing is outstanding: {residue:?}");
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_none(),
                "the session record is removed"
            );
            assert!(
                !log_dir.exists(),
                "the session log directory is reclaimed with its record"
            );
        }

        /// A session whose cleanup leaves residue keeps its record, so it
        /// keeps its logs too — the operator can still read why the cleanup
        /// did not finish. Linux-only for the same reason as the sibling
        /// rescan tests: the racing admission needs a real live task child.
        #[cfg(target_os = "linux")]
        #[test]
        fn stop_session_keeps_the_log_dir_when_cleanup_leaves_residue() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("stop-session-logs-residue");
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            let log_dir = session_logs_dir(&dir.path, "svc-1");
            fs::create_dir_all(&log_dir).expect("create session logs");
            fs::write(log_dir.join("stderr.log"), b"boom").expect("write captured stderr");

            // A live record the reaper does see, so it reaches its grace wait
            // — the point at which the racing admission lands and becomes the
            // outstanding task that keeps the session record alive.
            let mut reaped_child = spawn_live_task_child(&dir.path, "task-reaped");
            let reaped = live_task_record("svc-1", "task-reaped", reaped_child.id());
            write_task_record(&dir.path, &reaped).expect("write reaped task");
            let mut child = spawn_live_task_child(&dir.path, "task-racer");
            let racer = live_task_record("svc-1", "task-racer", child.id());

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let residue = stop_session_record_with_wait(
                &mut admission,
                &session_record,
                false,
                |_, _| {},
                |_, _| {
                    write_task_record(&dir.path, &racer).expect("admit racing task");
                },
            )
            .expect("stop session");
            drop(admission);

            assert!(
                residue.iter().any(|entry| entry.id == "task-racer"),
                "the racing task is outstanding: {residue:?}"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read")
                    .is_some(),
                "the session record is retained while an owned task is outstanding"
            );
            assert!(
                log_dir.join("stderr.log").is_file(),
                "its captured stderr is retained with it"
            );

            signal_group(child.id(), libc::SIGKILL).expect("kill racer");
            child.wait().expect("reap racer");
            reaped_child
                .wait()
                .expect("reap the terminated fixture task");
        }

        /// Aborting an admission is the last reference to the task's captured
        /// output, so the log tree goes with the record rather than being
        /// orphaned under `task-logs/`.
        #[test]
        fn abort_task_admission_reclaims_the_task_log_dir() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("abort-task-logs");
            // A terminal record needs no liveness probe, so this fixture stays
            // portable: `abort_task_admission` removes it outright.
            let record = TaskRecord {
                id: "task-aborted".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", Vec::new(), Vec::new(), 1_000, "/tmp/cb"),
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
            write_task_record(&dir.path, &record).expect("write task record");
            let log_dir = task_logs_dir(&dir.path, "task-aborted");
            fs::create_dir_all(&log_dir).expect("create task logs");
            fs::write(log_dir.join("stdout.log"), b"out").expect("write captured stdout");

            assert!(
                admission::abort_task_admission::<UnixServicePlatform>(&dir.path, &record)
                    .expect("abort admission"),
                "a terminal record is removed outright"
            );
            assert!(
                read_task_record(&dir.path, "task-aborted")
                    .expect("read")
                    .is_none(),
                "the task record is removed"
            );
            assert!(
                !log_dir.exists(),
                "the task log directory is reclaimed with its record"
            );
        }

        /// `force=true` skips every wait, so a racing admission can never
        /// land through the `task_wait`/`session_wait` callbacks the
        /// non-force rescan tests above use. `rescan_owned_tasks` is
        /// exercised directly instead: a live task record present when the
        /// rescan runs must be force-terminated and its record removed, not
        /// merely reported as residue.
        #[cfg(target_os = "linux")]
        #[test]
        fn rescan_force_terminates_and_removes_a_racing_task_record() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("rescan-force-racer");
            let mut child = spawn_live_task_child(&dir.path, "task-racer");
            let racer = live_task_record("svc-1", "task-racer", child.id());
            write_task_record(&dir.path, &racer).expect("write racing task");

            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
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
                read_task_record(&dir.path, "task-racer")
                    .expect("read")
                    .is_none(),
                "rescan_owned_tasks' force branch removes the racing task's record"
            );

            // The force branch signals the process group asynchronously, so
            // an immediate probe can observe `Live` while signal delivery and
            // child reaping are still in flight. Poll until the kill is
            // observable, with the same bounded grace used by cleanup.
            wait_while_task_alive(&racer, KILL_GRACE_MS);
            let liveness = task_execution_liveness_after_retry(&racer, KILL_GRACE_MS);
            assert_eq!(
                liveness,
                Liveness::Dead,
                "rescan_owned_tasks' force branch terminates the racing task's process"
            );

            child.wait().expect("reap racer");
        }

        /// Multiple unresolved task records are retained without entering a
        /// grace wait or signalling their uncorroborated process groups.
        #[cfg(target_os = "linux")]
        #[test]
        fn reap_session_tasks_retains_multiple_unresolved_without_waiting() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("reap-unresolved-fast");
            let mut children = Vec::new();
            let mut task_ids = Vec::new();

            for index in 0..3 {
                let task_id = format!("task-unresolved-{index}");
                let log_dir = task_logs_dir(&dir.path, &task_id);
                fs::create_dir_all(&log_dir).expect("create task log dir");
                let stdout_path = log_dir.join("stdout.log");
                let stderr_path = log_dir.join("stderr.log");
                let (child, task_specification) =
                    spawn_unresolved_identity_child(&stdout_path, &stderr_path);
                let task_record = TaskRecord {
                    id: task_id.clone(),
                    request_id: None,
                    admission: TaskAdmissionPhase::Committed,
                    spec: task_specification,
                    pid: child.id(),
                    started_at: None,
                    start_epoch_secs: None,
                    job: None,
                    started_ms: None,
                    state: TaskState::Running,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout_path: stdout_path.display().to_string(),
                    stderr_path: stderr_path.display().to_string(),
                    delivered_milestones: 0,
                    terminal_delivered_at_ms: None,
                };
                write_task_record(&dir.path, &task_record).expect("write unresolved task");

                assert_eq!(
                    is_task_alive(&task_record),
                    Liveness::Unresolved,
                    "fixture is in the unresolved identity state"
                );

                task_ids.push(task_id);
                children.push(child);
            }

            let wait_calls = std::cell::Cell::new(0);
            let mut admission = AdmissionGuard::acquire(&dir.path).expect("admission lock");
            let (residue, _handled) =
                reap_session_tasks_with_wait(&mut admission, "svc-1", false, |_, _| {
                    wait_calls.set(wait_calls.get() + 1)
                })
                .expect("reap unresolved tasks");

            assert_eq!(wait_calls.get(), 0, "unresolved tasks skip grace waits");
            assert_eq!(residue.len(), task_ids.len());
            assert!(
                residue
                    .iter()
                    .all(|item| { item.kind == "task" && item.liveness == Liveness::Unresolved })
            );
            for (task_id, child) in task_ids.iter().zip(children.iter_mut()) {
                assert!(
                    read_task_record(&dir.path, task_id)
                        .expect("read retained task")
                        .is_some(),
                    "unresolved task record remains durable"
                );
                assert!(
                    child.try_wait().expect("poll unresolved task").is_none(),
                    "unresolved task process remains unsignalled"
                );
            }

            for mut child in children {
                signal_group(child.id(), libc::SIGKILL).expect("kill unresolved task");
                child.wait().expect("wait for unresolved task");
            }
        }

        /// Ordinary teardown keeps an unresolved rehydrated task and its
        /// owning session record so a later forced teardown still has a
        /// durable cleanup boundary. `--force` then removes the record and
        /// signals the asserted PID group.
        #[cfg(target_os = "linux")]
        #[test]
        fn teardown_preserves_unresolved_task_until_force() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("teardown-unresolved");
            let request_id = "task-unresolved-request";
            let log_dir = task_logs_dir(&dir.path, "task-unresolved");
            fs::create_dir_all(&log_dir).expect("create log dir");
            let stdout_path = log_dir.join("stdout.log");
            let stderr_path = log_dir.join("stderr.log");
            let (mut child, task_specification) =
                spawn_unresolved_identity_child(&stdout_path, &stderr_path);
            let session_record = SessionRecord {
                id: "svc-1".to_string(),
                spec: spec("/tmp/in", "/tmp/out"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                stderr_path: String::new(),
            };
            let task_record = TaskRecord {
                id: "task-unresolved".to_string(),
                request_id: Some(request_id.to_string()),
                admission: TaskAdmissionPhase::Prepared,
                spec: task_specification,
                pid: child.id(),
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Running,
                exit_code: None,
                elapsed_ms: None,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
            };
            write_session_record(&dir.path, &session_record).expect("write session");
            write_task_record(&dir.path, &task_record).expect("write unresolved task");
            write_task_start_response(
                &dir.path,
                request_id,
                &TaskStartResponse {
                    task_id: Some(task_record.id.clone()),
                    error: None,
                },
            )
            .expect("write unresolved task response");
            mark_task_start_ack(&dir.path, request_id).expect("write unresolved task ack");
            mark_task_start_rollback(&dir.path, request_id)
                .expect("write unresolved task rollback");

            assert_eq!(is_task_alive(&task_record), Liveness::Unresolved);

            let mut status = Vec::new();
            execute_task_status(&dir.path, Some("task-unresolved"), &mut status)
                .expect("status unresolved task");
            let status: serde_json::Value = serde_json::from_slice(&status).expect("status JSON");
            assert_eq!(status["tasks"][0]["live"], false);
            assert_eq!(status["tasks"][0]["liveness"], "unresolved");

            let mut out = Vec::new();
            assert!(
                execute_teardown(&dir.path, false, &mut out).is_err(),
                "ordinary teardown reports unresolved residue"
            );
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read preserved session")
                    .is_some()
            );
            assert!(
                read_task_record(&dir.path, "task-unresolved")
                    .expect("read preserved task")
                    .is_some()
            );
            assert!(
                child.try_wait().expect("poll unresolved task").is_none(),
                "ordinary teardown does not signal unresolved identity"
            );

            execute_teardown(&dir.path, true, &mut Vec::new()).expect("forced teardown");
            assert!(
                read_session_record(&dir.path, "svc-1")
                    .expect("read forced session")
                    .is_none()
            );
            assert!(
                read_task_record(&dir.path, "task-unresolved")
                    .expect("read forced task")
                    .is_none()
            );
            assert!(
                !task_start_rollback_exists(&dir.path, request_id).expect("read forced rollback"),
                "forced teardown removes the rollback marker"
            );
            assert!(
                !task_start_response_boundary_exists(&dir.path, request_id)
                    .expect("read forced response boundary"),
                "forced teardown removes the response boundary"
            );
            let _ = child.wait();
        }

        // -- Task CLI-facing operations -----------------------------------

        /// `execute_task_status` reports a written task record's liveness
        /// and fields.
        #[test]
        fn execute_task_status_reports_task_fields() {
            let _guard = serialize_forks_and_locks();
            let dir = TempDir::new("task-status");
            let record = TaskRecord {
                id: "task-1".to_string(),
                request_id: None,
                admission: TaskAdmissionPhase::Committed,
                spec: task_spec("svc-1", "true", vec![], vec![], 1_000, "/tmp/cb"),
                pid: u32::MAX - 1,
                started_at: None,
                start_epoch_secs: None,
                job: None,
                started_ms: None,
                state: TaskState::Completed,
                exit_code: Some(0),
                elapsed_ms: Some(42),
                stdout_path: String::new(),
                stderr_path: String::new(),
                delivered_milestones: 0,
                terminal_delivered_at_ms: None,
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
            assert_eq!(tasks[0]["liveness"], "dead");
            assert_eq!(tasks[0]["exit_code"], 0);
            assert_eq!(tasks[0]["elapsed_ms"], 42);
        }

        /// `execute_task_cancel` on an unknown task id is a no-op success
        /// (idempotent).
        #[test]
        fn execute_task_cancel_unknown_task_is_idempotent_success() {
            let _guard = serialize_forks_and_locks();
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
                    other => panic!("unexpected terminal delivery outcome: {other:?}"),
                }
            }
            assert_eq!(running.record.state, TaskState::Cancelled);
        }
    }
}

#[cfg(windows)]
#[path = "service_windows.rs"]
mod imp;

#[cfg(windows)]
pub fn adopt_windows_service_job() -> Result<()> {
    imp::adopt_service_job()
}
