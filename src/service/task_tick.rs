use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::records::{
    SessionRecord, list_task_records, read_task_record, remove_task_logs_dir, remove_task_record,
    remove_task_start_transaction, task_cancel_dir, task_record_exists, write_task_record,
};
use super::{BatonError, Result, SessionSpec};
use crate::mailbox;
use crate::message::{MessageEnvelope, MessageKind};
use crate::task::{
    Clock, TaskAdmissionPhase, TaskEventBody, TaskEventKind, TaskRecord, TaskSpec, TaskState,
    max_duration_exceeded, milestones_due, task_event_id,
};

const EVENT_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
const EVENT_RETRY_MAX_DELAY_MS: u64 = 60_000;
const MAX_EVENT_DELIVERY_ATTEMPTS: u32 = 10;
const KILL_GRACE_MS: u64 = 2_000;
/// Minimum interval between liveness samples for one rehydrated task's
/// process group. The supervisor still ticks every 100 ms, but the costly
/// Linux `/proc` table scan, non-Linux `ps` probe, and Windows Job Object
/// probe are each allowed at most twice per second in the steady state.
pub(super) const REHYDRATED_LIVENESS_CACHE_MS: u64 = 500;
/// Default milliseconds a delivered terminal task record is kept before
/// automatic runtime reaping, when `baton service run` omits
/// `--task-retention`.
pub(super) const DEFAULT_TASK_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;

/// Whether a `(checked_ms, _, checked_at)` liveness sample is still within
/// [`REHYDRATED_LIVENESS_CACHE_MS`] of both the tick's software clock and
/// the wall clock. Shared by every platform arm of `task_liveness_for_tick`
/// so the dual-clock staleness rule isn't hand-mirrored per platform.
pub(super) fn liveness_sample_is_fresh(checked_ms: u64, checked_at: Instant, now_ms: u64) -> bool {
    now_ms.saturating_sub(checked_ms) < REHYDRATED_LIVENESS_CACHE_MS
        && checked_at.elapsed() < Duration::from_millis(REHYDRATED_LIVENESS_CACHE_MS)
}

/// Result of corroborating a durable PID against the process currently
/// occupying it. `Unresolved` is deliberately distinct from `Dead`: the
/// former means the PID exists but baton cannot prove its identity, so it is
/// never signalled or removed without an explicit operator override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Liveness {
    Live,
    Dead,
    Unresolved,
}

impl Liveness {
    pub(super) fn is_live(self) -> bool {
        self == Self::Live
    }
}

/// The lifecycle operation requested from a platform adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminationSignal {
    Term,
    Kill,
}

#[allow(dead_code)]
impl TerminationSignal {
    pub(super) fn phase(self) -> &'static str {
        match self {
            Self::Term => "-TERM",
            Self::Kill => "-KILL",
        }
    }
}

/// Identifies the task liveness path the shared tick is observing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskLivenessMode {
    Owned { leader_exited: bool },
    Rehydrated,
}

/// Controls whether a platform may use its bounded liveness sample or must
/// take a fresh observation before authorizing termination/reaping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskLivenessRefresh {
    Cached,
    Forced,
}

/// Shared platform seam used by the task tick and the existing service
/// process adapters. The task tick only depends on the associated handle and
/// cache types; platform-specific imports stay in the cfg-selected adapter.
#[allow(dead_code)]
pub(super) trait ServicePlatform {
    type SessionHandle;
    type TaskHandle;
    type TaskLivenessCache: Default;

    fn spawn_session(
        spec: &SessionSpec,
        stderr_path: &Path,
    ) -> Result<(Child, Self::SessionHandle)>;
    fn spawn_task(
        spec: &TaskSpec,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(Child, Self::TaskHandle)>;
    /// The durable identity string a spawned task handle should persist in
    /// [`TaskRecord::job`] for later rehydration, or `None` on platforms that
    /// rehydrate by PID alone.
    fn task_handle_identity(handle: &Self::TaskHandle) -> Option<String>;
    /// Kills a just-spawned task that has not yet been committed to a durable
    /// record (no [`TaskRecord`] exists yet to route through
    /// [`Self::terminate_owned_task`]).
    fn abort_uncommitted_spawn(pid: u32, handle: &Self::TaskHandle) -> Result<()>;
    fn recorded_start_identity(pid: u32) -> (Option<String>, Option<i64>);
    fn start_identity_is_valid(started_at: &Option<String>, start_epoch_secs: &Option<i64>)
    -> bool;
    fn session_liveness(record: &SessionRecord) -> Liveness;
    fn task_liveness(record: &TaskRecord) -> Liveness;
    fn task_liveness_for_tick(
        record: &TaskRecord,
        owner: Option<&Self::TaskHandle>,
        mode: TaskLivenessMode,
        cache: &mut Self::TaskLivenessCache,
        now_ms: u64,
        refresh: TaskLivenessRefresh,
    ) -> Liveness;
    fn terminate_session(
        record: &SessionRecord,
        signal: TerminationSignal,
        force: bool,
    ) -> Result<()>;
    fn terminate_task(record: &TaskRecord, signal: TerminationSignal, force: bool) -> Result<()>;
    fn terminate_owned_task(
        owner: Option<&Self::TaskHandle>,
        record: &TaskRecord,
        signal: TerminationSignal,
        force: bool,
    ) -> Result<()>;
    fn pid_is_gone(pid: u32) -> bool;
    fn unresolved_task_is_gone(
        control: &Path,
        id: &str,
        record: &TaskRecord,
        term_sent_at_ms: Option<u64>,
    ) -> Result<bool>;
    fn rehydrate_task(record: &TaskRecord) -> Result<Option<Self::TaskHandle>>;
    /// Upgrades a legacy durable record in place before its admission is
    /// evaluated (a no-op on every platform but macOS, which backfills a
    /// missing start epoch from a live process).
    fn upgrade_legacy_task_record(control: &Path, record: &mut TaskRecord) -> Result<()>;
    /// Escalates a running task toward termination and returns the settled
    /// [`Liveness`] once dead, still live after both signals, or unresolved
    /// (an unresolved probe short-circuits before any signal is sent). Each
    /// platform retains its own probe-retry and signal-escalation shape.
    fn escalate_task_to_death(record: &TaskRecord, grace_ms: u64) -> Liveness;
    /// Takes the short-lived lock shared by task admission and session
    /// cleanup. Each platform opens its own lock file with its own
    /// close-on-exec/sharing discipline, so this cannot be a plain shared
    /// function.
    fn acquire_admission_lock(control: &Path) -> Result<File>;
    fn persist_terminal_task(
        control: &Path,
        record: &mut TaskRecord,
        state: TaskState,
        exit_code: Option<i32>,
        elapsed_ms: u64,
    ) -> Result<bool>;
    fn keep_child_handle_while_draining() -> bool;
}

/// Exit outcome retained when a platform parks the task after its direct
/// child exits while descendants remain in the owned boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChildExit {
    succeeded: bool,
    code: Option<i32>,
}

/// One task currently tracked by the shared supervisor state machine.
pub(super) struct RunningTask<P: ServicePlatform> {
    pub(super) record: TaskRecord,
    pub(super) child: Option<Child>,
    pub(super) task_handle: Option<P::TaskHandle>,
    pub(super) liveness_cache: P::TaskLivenessCache,
    pub(super) started_ms: u64,
    pub(super) term_sent_at_ms: Option<u64>,
    pub(super) kill_sent: bool,
    pub(super) terminal_delivery_attempts: u32,
    pub(super) next_terminal_retry_ms: Option<u64>,
    pub(super) terminal_retry_delay_ms: u64,
    pub(super) milestone_delivery_attempts: u32,
    pub(super) next_milestone_retry_ms: Option<u64>,
    pub(super) milestone_retry_delay_ms: u64,
    pub(super) child_exit: Option<ChildExit>,
    /// Milliseconds a delivered terminal record is retained before
    /// [`deliver_terminal_event`] reaps it. Defaults to
    /// [`DEFAULT_TASK_RETENTION_MS`]; overridden via [`Self::with_retention_ms`]
    /// at the two sites that know the configured `--task-retention` value
    /// (fresh spawn and boot rehydration).
    pub(super) retention_ms: u64,
}

impl<P: ServicePlatform> RunningTask<P> {
    pub(super) fn new(
        record: TaskRecord,
        child: Option<Child>,
        task_handle: Option<P::TaskHandle>,
        started_ms: u64,
    ) -> Self {
        Self {
            record,
            child,
            task_handle,
            liveness_cache: P::TaskLivenessCache::default(),
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
            retention_ms: DEFAULT_TASK_RETENTION_MS,
        }
    }

    pub(super) fn with_retention_ms(mut self, retention_ms: u64) -> Self {
        self.retention_ms = retention_ms;
        self
    }
}

/// Restores every durable task before the request loop accepts new work.
///
/// `records`, when given, is reused as the post-reconciliation `tasks/`
/// snapshot instead of re-listing the directory — safe only when the caller
/// knows nothing wrote to `tasks/` since that snapshot was taken (the
/// steady-state boot path, once admission reconciliation reports no
/// mutation). `None` always re-lists.
pub(super) fn rehydrate_tasks<P: ServicePlatform>(
    control: &Path,
    clock: &dyn Clock,
    retention_ms: u64,
    records: Option<Vec<TaskRecord>>,
) -> Result<HashMap<String, RunningTask<P>>> {
    let mut tasks = HashMap::new();
    let records = match records {
        Some(records) => records,
        None => list_task_records(control)?,
    };
    for mut record in records {
        if record.admission == TaskAdmissionPhase::Prepared {
            continue;
        }
        let started_ms = match record.started_ms {
            Some(started_ms) => started_ms,
            None if record.state == TaskState::Running => {
                let started_ms = clock.now_ms();
                record.started_ms = Some(started_ms);
                write_task_record(control, &record)?;
                started_ms
            }
            None => clock.now_ms(),
        };
        let id = record.id.clone();
        let task_handle = P::rehydrate_task(&record)?;
        tasks.insert(
            id,
            RunningTask::new(record, None, task_handle, started_ms).with_retention_ms(retention_ms),
        );
    }
    Ok(tasks)
}

/// Outcome of one [`tick_one_task`] call.
#[derive(Debug)]
pub(super) enum TaskTick {
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

/// Advances one tracked task by one loop tick: delivers milestones, enforces
/// max duration, and finalizes once the direct process and its owned boundary
/// have exited. All liveness and termination decisions are delegated to the
/// selected platform adapter.
pub(super) fn tick_one_task<P: ServicePlatform>(
    control: &Path,
    id: &str,
    running: &mut RunningTask<P>,
    clock: &dyn Clock,
) -> Result<TaskTick> {
    if !task_record_exists(control, id)? {
        return Ok(TaskTick::Finished);
    }
    if running.record.state != TaskState::Running {
        return deliver_terminal_event(control, running, clock);
    }

    let now_ms = clock.now_ms();
    let elapsed_ms = now_ms.saturating_sub(running.started_ms);
    deliver_due_milestones(control, id, running, elapsed_ms, clock)?;

    let rehydrated_liveness = if running.child.is_none() {
        let signal_due = (running.term_sent_at_ms.is_none()
            && max_duration_exceeded(elapsed_ms, running.record.spec.max_duration_ms))
            || (running.term_sent_at_ms.is_some()
                && !running.kill_sent
                && now_ms.saturating_sub(running.term_sent_at_ms.unwrap()) >= KILL_GRACE_MS);
        let liveness = P::task_liveness_for_tick(
            &running.record,
            running.task_handle.as_ref(),
            TaskLivenessMode::Rehydrated,
            &mut running.liveness_cache,
            now_ms,
            if signal_due {
                TaskLivenessRefresh::Forced
            } else {
                TaskLivenessRefresh::Cached
            },
        );
        match liveness {
            Liveness::Dead => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
            }
            Liveness::Live => {}
            Liveness::Unresolved
                if P::unresolved_task_is_gone(
                    control,
                    id,
                    &running.record,
                    running.term_sent_at_ms,
                )? =>
            {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                return finalize_task(control, running, state, exit_code, elapsed_ms, clock);
            }
            Liveness::Unresolved => return Ok(TaskTick::StillRunning),
        }
        Some(liveness)
    } else {
        None
    };

    if running.term_sent_at_ms.is_none()
        && max_duration_exceeded(elapsed_ms, running.record.spec.max_duration_ms)
    {
        let liveness = rehydrated_liveness.unwrap_or_else(|| {
            P::task_liveness_for_tick(
                &running.record,
                running.task_handle.as_ref(),
                TaskLivenessMode::Owned {
                    leader_exited: false,
                },
                &mut running.liveness_cache,
                now_ms,
                TaskLivenessRefresh::Forced,
            )
        });
        match liveness {
            Liveness::Unresolved => return Ok(TaskTick::StillRunning),
            Liveness::Live => {
                let _ = P::terminate_owned_task(
                    running.task_handle.as_ref(),
                    &running.record,
                    TerminationSignal::Term,
                    false,
                );
                running.term_sent_at_ms = Some(clock.now_ms());
            }
            Liveness::Dead => {}
        }
    } else if let Some(term_at) = running.term_sent_at_ms
        && !running.kill_sent
        && clock.now_ms().saturating_sub(term_at) >= KILL_GRACE_MS
    {
        let liveness = rehydrated_liveness.unwrap_or_else(|| {
            P::task_liveness_for_tick(
                &running.record,
                running.task_handle.as_ref(),
                TaskLivenessMode::Owned {
                    leader_exited: false,
                },
                &mut running.liveness_cache,
                now_ms,
                TaskLivenessRefresh::Forced,
            )
        });
        match liveness {
            Liveness::Unresolved => return Ok(TaskTick::StillRunning),
            Liveness::Live => {
                let _ = P::terminate_owned_task(
                    running.task_handle.as_ref(),
                    &running.record,
                    TerminationSignal::Kill,
                    false,
                );
                running.kill_sent = true;
            }
            Liveness::Dead => {}
        }
    }

    let child_status = match running.child.as_mut() {
        Some(child) => Some(
            child
                .try_wait()
                .map_err(|err| BatonError::Io(format!("could not poll task {id}: {err}")))?,
        ),
        None => None,
    };
    match child_status {
        None => match rehydrated_liveness.expect("rehydrated liveness cached above") {
            Liveness::Live | Liveness::Unresolved => Ok(TaskTick::StillRunning),
            Liveness::Dead => {
                let cancelled = consume_task_cancel_sentinel(control, id)?;
                let (state, exit_code) = parked_terminal(running, cancelled);
                finalize_task(control, running, state, exit_code, elapsed_ms, clock)
            }
        },
        Some(None) => Ok(TaskTick::StillRunning),
        Some(Some(status)) => {
            let liveness = P::task_liveness_for_tick(
                &running.record,
                running.task_handle.as_ref(),
                TaskLivenessMode::Owned {
                    leader_exited: true,
                },
                &mut running.liveness_cache,
                now_ms,
                TaskLivenessRefresh::Cached,
            );
            match liveness {
                Liveness::Live | Liveness::Unresolved => {
                    if !P::keep_child_handle_while_draining() {
                        running.child_exit = Some(ChildExit {
                            succeeded: status.success(),
                            code: status.code(),
                        });
                        running.child = None;
                    }
                    return Ok(TaskTick::StillRunning);
                }
                Liveness::Dead => {}
            }
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
    }
}

/// Resolves the terminal state and exit code for a task whose Child handle is
/// gone after its platform-owned boundary has drained.
fn parked_terminal<P: ServicePlatform>(
    running: &RunningTask<P>,
    cancelled: bool,
) -> (TaskState, Option<i32>) {
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

fn deliver_due_milestones<P: ServicePlatform>(
    control: &Path,
    id: &str,
    running: &mut RunningTask<P>,
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
                    running.record.delivered_milestones = index + 1;
                    if let Err(write_err) = write_task_record(control, &running.record) {
                        running.record.delivered_milestones = index;
                        return Err(write_err);
                    }
                    running.milestone_delivery_attempts = 0;
                    running.next_milestone_retry_ms = None;
                    running.milestone_retry_delay_ms = 0;
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
                break;
            }
        }
    }
    Ok(())
}

/// Delivers a task's terminal callback exactly once, then retains its
/// record for [`RunningTask::retention_ms`] before reaping it. A record
/// whose `terminal_delivered_at_ms` is already set (delivered earlier this
/// run, or before a restart) skips straight to the retention check —
/// delivery is never repeated once it has succeeded.
fn deliver_terminal_event<P: ServicePlatform>(
    control: &Path,
    running: &mut RunningTask<P>,
    clock: &dyn Clock,
) -> Result<TaskTick> {
    let now_ms = clock.now_ms();
    if running.record.terminal_delivered_at_ms.is_none() {
        if let Some(next_retry_ms) = running.next_terminal_retry_ms
            && now_ms < next_retry_ms
        {
            return Ok(TaskTick::StillRunning);
        }

        match deliver_task_event(&running.record, TaskEventKind::Terminal) {
            Ok(()) => {
                running.record.terminal_delivered_at_ms = Some(now_ms);
                if let Err(err) = write_task_record(control, &running.record) {
                    running.record.terminal_delivered_at_ms = None;
                    return Err(err);
                }
            }
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
                return Ok(TaskTick::TerminalDeliveryRetry {
                    error,
                    attempt,
                    delay_ms,
                });
            }
        }
    }

    let delivered_at_ms = running
        .record
        .terminal_delivered_at_ms
        .expect("just delivered or already recorded above");
    if now_ms.saturating_sub(delivered_at_ms) >= running.retention_ms {
        remove_reaped_task_record(control, &running.record)?;
        Ok(TaskTick::Finished)
    } else {
        Ok(TaskTick::StillRunning)
    }
}

pub(super) fn finalize_task<P: ServicePlatform>(
    control: &Path,
    running: &mut RunningTask<P>,
    state: TaskState,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    clock: &dyn Clock,
) -> Result<TaskTick> {
    let previous = running.record.clone();
    let persisted = match P::persist_terminal_task(
        control,
        &mut running.record,
        state,
        exit_code,
        elapsed_ms,
    ) {
        Ok(persisted) => persisted,
        Err(err) => {
            running.record = previous;
            return Err(err);
        }
    };
    if !persisted {
        running.record = previous;
        return Ok(TaskTick::Finished);
    }
    deliver_terminal_event(control, running, clock)
}

/// Ticks every tracked task once, dropping any that finished.
pub(super) fn tick_tasks<P: ServicePlatform>(
    control: &Path,
    tasks: &mut HashMap<String, RunningTask<P>>,
    clock: &dyn Clock,
) {
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
            Err(err) => eprintln!("warning: baton service failed to tick task {id}: {err}"),
        }
    }
    for id in finished {
        tasks.remove(&id);
    }
}

pub(super) fn deliver_task_event(record: &TaskRecord, kind: TaskEventKind) -> Result<()> {
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

pub(super) fn task_cancel_sentinel_path(control: &Path, task_id: &str) -> std::path::PathBuf {
    task_cancel_dir(control).join(mailbox::file_name(task_id))
}

/// Remove every durable trace of a task whose record is being reaped:
/// its start transaction, its `tasks/` record, any lingering cancel sentinel,
/// and its captured `task-logs/<task-id>/` output. Shared by both platforms'
/// stop/teardown paths and by the runtime terminal-record reaper, so the logs
/// live exactly as long as the record that names them.
pub(super) fn remove_reaped_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
    remove_task_start_transaction(control, record)?;
    remove_task_record(control, &record.id)?;
    let _ = std::fs::remove_file(task_cancel_sentinel_path(control, &record.id));
    remove_task_logs_dir(control, &record.id);
    Ok(())
}

pub(super) fn consume_task_cancel_sentinel(control: &Path, task_id: &str) -> Result<bool> {
    match std::fs::remove_file(task_cancel_sentinel_path(control, task_id)) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(BatonError::Io(format!(
            "could not consume task cancel sentinel: {err}"
        ))),
    }
}
