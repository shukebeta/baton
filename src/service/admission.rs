//! The task-admission transactional core shared by both platform adapters:
//! the `Prepared` -> `Committed` -> `Responded` state machine a task-start
//! request moves through, its startup reconciliation, and the rollback path
//! that aborts and reclaims an admission a restart interrupted. Generic over
//! [`ServicePlatform`] so [`super::imp::UnixServicePlatform`] and
//! [`super::imp::WindowsServicePlatform`] route spawn/liveness/kill through
//! the trait instead of keeping a private copy each.

use std::fs;
use std::path::Path;
use std::process::Child;

use serde::{Deserialize, Serialize};

use super::records::{
    TEST_TASK_ROLLBACK_RECONCILE_BARRIER, TEST_TASK_ROLLBACK_REQUEST_BARRIER, TaskStartResponse,
    discard_pending_task_start_request, fresh_task_id, list_task_records, list_task_start_acks,
    list_task_start_response_claims, list_task_start_rollbacks, read_session_record,
    remove_task_logs_dir, remove_task_record, remove_task_start_ack,
    remove_task_start_response_files, remove_task_start_rollback,
    restore_task_start_response_claim, task_channel, task_logs_dir, task_start_ack_exists,
    task_start_response_claim_path, task_start_response_path, task_start_rollback_exists,
    wait_for_test_task_admission_barrier, wait_for_test_task_response_phase_barrier,
    wait_for_test_task_rollback_cleanup_barrier, write_task_record, write_task_start_response,
};
use super::task_tick::{Liveness, RunningTask, ServicePlatform};
use super::{BatonError, Result};
use crate::mailbox;
use crate::task::{
    Clock, TaskAdmissionPhase, TaskRecord, TaskSpec, TaskState, first_non_ascending_milestone,
};

/// Bound on the kill-signal (or Windows Job Object) escalation grace
/// [`abort_task_admission`] gives a running task before giving up on it.
const KILL_GRACE_MS: u64 = 2_000;

/// A freshly spawned, not-yet-delivered task: its durable record, its child
/// process, its platform-specific handle, and the clock reading taken at
/// spawn time.
type SpawnedTask<P> = (TaskRecord, Child, <P as ServicePlatform>::TaskHandle, u64);

/// Directory holding one durable "a stop owns this session" marker per
/// session, read by [`session_stop_in_progress`].
pub(super) fn session_stop_markers_dir(control: &Path) -> std::path::PathBuf {
    control.join("session-stopping")
}

pub(super) fn session_stop_marker_path(control: &Path, id: &str) -> Result<std::path::PathBuf> {
    if !mailbox::is_safe_key(id) {
        return Err(BatonError::Io(format!(
            "session id is not usable as a filename: {id:?}"
        )));
    }
    Ok(session_stop_markers_dir(control).join(mailbox::file_name(id)))
}

/// The durable "a stop owns this session" marker a live `service
/// stop`/`teardown` writes under the admission lock for the length of one
/// session's cleanup (see each platform's `SessionStopGuard`), read here so
/// a racing task start rejects a session that is mid-stop even while its
/// process still probes `Live`.
#[derive(Serialize, Deserialize)]
struct SessionStopMarker {
    pid: u32,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    start_epoch_secs: Option<i64>,
}

/// Whether some live `service stop`/`teardown` currently owns `id`'s
/// cleanup. Removes a stale marker as a side effect, so one orphaned by a
/// killed stop costs at most one rejected start.
pub(super) fn session_stop_in_progress<P: ServicePlatform>(
    control: &Path,
    id: &str,
) -> Result<bool> {
    let path = session_stop_marker_path(control, id)?;
    let data = match fs::read_to_string(&path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
    };
    // A malformed marker is not evidence of a live stop, and leaving it
    // would wedge admission for good.
    let Ok(marker) = serde_json::from_str::<SessionStopMarker>(&data) else {
        let _ = fs::remove_file(&path);
        return Ok(false);
    };
    let (started_at, start_epoch_secs) = P::recorded_start_identity(marker.pid);
    if started_at == marker.started_at && start_epoch_secs == marker.start_epoch_secs {
        return Ok(true);
    }
    let _ = fs::remove_file(&path);
    Ok(false)
}

/// Renders `err` for a start-response `error` field. The client re-wraps
/// that text in [`BatonError::Io`], so the rendered form must not repeat the
/// kind prefix `Display` already adds.
pub(super) fn admission_error_text(err: &BatonError) -> String {
    match err {
        BatonError::Io(msg) => msg.clone(),
        other => other.to_string(),
    }
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
/// Returns the `tasks/` records observed at the start of the pass,
/// alongside whether the pass mutated any of them (removed one via
/// [`abort_task_admission`] or promoted `Committed` to `Responded`). A
/// caller that finds `mutated == false` may reuse the returned records
/// as an accurate post-reconciliation snapshot instead of re-listing
/// `tasks/`.
pub(super) fn reconcile_task_admissions<P: ServicePlatform>(
    control: &Path,
) -> Result<(Vec<TaskRecord>, bool)> {
    let rollback_ids = list_task_start_rollbacks(control)?;
    let ack_ids = list_task_start_acks(control)?;
    let claim_ids = list_task_start_response_claims(control)?;
    let records = list_task_records(control)?;
    let mut mutated = false;
    let mut seen_rollbacks = std::collections::HashSet::new();
    let mut retained_rollbacks = std::collections::HashSet::new();
    let mut seen_acks = std::collections::HashSet::new();

    for record in &records {
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
            // `abort_task_admission` may durably rewrite the record (e.g.
            // a macOS legacy-record epoch upgrade) even when it reports
            // the admission unresolved, so it counts as a mutation
            // regardless of its return value.
            let removed = abort_task_admission::<P>(control, record)?;
            mutated = true;
            if !removed {
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
            // See the Prepared-admission arm above: this call may
            // durably rewrite the record even on an unresolved outcome.
            let removed = abort_task_admission::<P>(control, record)?;
            mutated = true;
            if !removed {
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
                mutated = true;
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
            } else {
                mutated = true;
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
    Ok((records, mutated))
}

pub(super) fn abort_task_admission<P: ServicePlatform>(
    control: &Path,
    record: &TaskRecord,
) -> Result<bool> {
    if record.state == TaskState::Running {
        let mut record = record.clone();
        P::upgrade_legacy_task_record(control, &mut record)?;
        let liveness = P::escalate_task_to_death(&record, KILL_GRACE_MS);
        if liveness != Liveness::Dead {
            return Ok(false);
        }
    }
    remove_task_record(control, &record.id)?;
    // The aborted admission is the last reference to this task's captured
    // output, so its log tree goes with the record rather than becoming
    // unidentifiable garbage under `task-logs/`.
    remove_task_logs_dir(control, &record.id);
    Ok(true)
}

/// Returns any `task-processing/` entry a crash left mid-request to
/// `task-requests/` through the shared request channel.
pub(super) fn reclaim_stale_task_requests(control: &Path) -> Result<()> {
    task_channel(control).reclaim_stale()
}

/// Claims the next task-start request through the shared request channel,
/// then applies task-specific admission and lifecycle handling.
pub(super) fn process_one_task_request<P: ServicePlatform>(
    control: &Path,
    clock: &dyn Clock,
) -> Result<Option<(String, RunningTask<P>)>> {
    task_channel(control).process_one(|request_id, claimed_path| {
        // The lock is intentionally acquired after the request is
        // claimed but before owner validation and spawn. If session
        // cleanup wins the race, validation observes the removed/dead
        // owner; if admission wins, cleanup waits and reaps the newly
        // recorded task.
        let outcome = P::acquire_admission_lock(control).and_then(|_admission| {
            if task_start_rollback_exists(control, request_id)? {
                discard_pending_task_start_request(control, request_id)?;
                wait_for_test_task_rollback_cleanup_barrier(TEST_TASK_ROLLBACK_REQUEST_BARRIER);
                remove_task_start_rollback(control, request_id)?;
                return Ok(None);
            }
            handle_task_start_request::<P>(control, request_id, claimed_path, clock)
        });
        let Some((record, child, task_handle, started_ms)) = outcome? else {
            return Ok(None);
        };
        let id = record.id.clone();
        let running = RunningTask::new(record, Some(child), Some(task_handle), started_ms);
        Ok(Some((id, running)))
    })
}

/// Answers a claimed task-start request with an admission failure the
/// supervisor can name, mirroring `reject_start_request`.
pub(super) fn reject_task_start_request<P: ServicePlatform>(
    control: &Path,
    request_id: &str,
    error: String,
) -> Result<Option<SpawnedTask<P>>> {
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
/// creation, spawn, post-spawn corroboration, record write — is answered
/// as an error response and reported as `Ok(None)`, so the client fails
/// immediately with the real reason instead of waiting out `START_AWAIT_MS`.
/// Only a failure to deliver a response at all is propagated as `Err`.
pub(super) fn handle_task_start_request<P: ServicePlatform>(
    control: &Path,
    request_id: &str,
    spec_path: &Path,
    clock: &dyn Clock,
) -> Result<Option<SpawnedTask<P>>> {
    let data = fs::read_to_string(spec_path)
        .map_err(|err| BatonError::Io(format!("could not read {spec_path:?}: {err}")))?;
    let spec: TaskSpec = serde_json::from_str(&data)
        .map_err(|err| BatonError::Decode(format!("malformed task spec {spec_path:?}: {err}")))?;
    if let Some((previous, current)) = first_non_ascending_milestone(&spec.milestones_ms) {
        return reject_task_start_request::<P>(
            control,
            request_id,
            format!(
                "task start rejected: --milestone-ms values must be strictly ascending: got {previous} followed by {current}"
            ),
        );
    }
    // A session being stopped is not an admissible owner, even while its
    // process is still live. `service stop` releases the admission lock
    // across its grace windows (so it never freezes this loop), which
    // leaves a window where the owner still probes `Live`; without this
    // gate a start racing that window would be answered with a task id
    // for a process the very same stop is about to kill.
    let owner_live = if mailbox::is_safe_key(&spec.session) {
        read_session_record(control, &spec.session)?
            .map(|record| P::session_liveness(&record) == Liveness::Live)
            .unwrap_or(false)
            && !session_stop_in_progress::<P>(control, &spec.session)?
    } else {
        false
    };
    if !owner_live {
        let error = format!(
            "task start rejected: --session {:?} does not name a live managed session on {:?} (the session record is absent, its process is no longer live, or it is draining a stop request)",
            spec.session, control
        );
        return reject_task_start_request::<P>(control, request_id, error);
    }
    let task_id = fresh_task_id();
    let log_dir = task_logs_dir(control, &task_id);
    if let Err(err) = fs::create_dir_all(&log_dir) {
        return reject_task_start_request::<P>(
            control,
            request_id,
            format!("could not create {log_dir:?}: {err}"),
        );
    }
    let stdout_path = log_dir.join("stdout.log");
    let stderr_path = log_dir.join("stderr.log");
    let (mut child, task_handle) = match P::spawn_task(&spec, &stdout_path, &stderr_path) {
        Ok(spawned) => spawned,
        Err(err) => {
            // Nothing ever ran under this id, so its just-created log
            // directory holds two empty files and no record refers to
            // it: drop it rather than leaking one per failed start.
            let _ = fs::remove_dir_all(&log_dir);
            return reject_task_start_request::<P>(control, request_id, admission_error_text(&err));
        }
    };
    let pid = child.id();
    let (started_at, start_epoch_secs) = P::recorded_start_identity(pid);
    if !P::start_identity_is_valid(&started_at, &start_epoch_secs) {
        let _ = P::abort_uncommitted_spawn(pid, &task_handle);
        let _ = child.wait();
        return reject_task_start_request::<P>(
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
        job: P::task_handle_identity(&task_handle),
        started_ms: Some(started_ms),
        state: TaskState::Running,
        exit_code: None,
        elapsed_ms: None,
        stdout_path: stdout_path.display().to_string(),
        stderr_path: stderr_path.display().to_string(),
        delivered_milestones: 0,
        terminal_delivered_at_ms: None,
    };
    if let Err(err) = write_task_record(control, &record) {
        let _ = P::abort_uncommitted_spawn(pid, &task_handle);
        let _ = child.wait();
        return reject_task_start_request::<P>(control, request_id, admission_error_text(&err));
    }
    wait_for_test_task_admission_barrier();
    record.admission = TaskAdmissionPhase::Committed;
    if let Err(err) = write_task_record(control, &record) {
        let _ = P::abort_uncommitted_spawn(pid, &task_handle);
        let _ = child.wait();
        let _ = remove_task_record(control, &record.id);
        remove_task_logs_dir(control, &record.id);
        return reject_task_start_request::<P>(control, request_id, admission_error_text(&err));
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
        return Ok(Some((record, child, task_handle, started_ms)));
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
    Ok(Some((record, child, task_handle, started_ms)))
}
