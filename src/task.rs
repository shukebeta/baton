//! `baton task`: a service-owned asynchronous job, run and reaped by
//! [`crate::service`]'s control-plane loop on behalf of a headless
//! `baton serve --agent-cmd` turn that has already exited.
//!
//! This module holds the wire-format contract only — [`TaskSpec`],
//! [`TaskCommand`], [`TaskRecord`], [`TaskEventKind`], deterministic event-id
//! derivation, and the injectable [`Clock`] seam. The actual spawn/track/reap
//! loop lives in [`crate::service`] (see `run_service`), which already owns
//! the durable control-plane directories and process-group kill primitives a
//! task reuses.
//!
//! ## Deterministic event ids
//!
//! An event id is composed only from the task id, its event kind, and — for a
//! milestone — that milestone's configured index: no wall-clock or fire-time
//! input (see [`task_event_id`]). Delivering the same id twice is safe without
//! any hashing dependency: the mailbox's own `done/` dedup
//! ([`crate::mailbox::Mailbox::claim_next`]) drops an exact-id redelivery once
//! the first delivery has been consumed.
//!
//! ## Injectable clock
//!
//! Milestone and max-duration checks compare elapsed milliseconds against a
//! [`Clock`], not a bare `SystemTime`/`Instant` call, so a test can drive the
//! task loop's timing decisions deterministically (see [`FakeClock`]) instead
//! of sleeping in wall-clock time.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Schema tag for a [`TaskSpec`], stamped for forward-compatible parsing.
pub const TASK_SPEC_SCHEMA: &str = "baton.task-spec/v1";

/// Where a task's lifecycle events are delivered.
///
/// `inbox` is a mailbox root — the same addressing every other `baton`
/// surface uses ([`crate::mailbox::deliver_to`] delivers into
/// `<inbox>/pending/`). `role` is an optional identity tag carried on the
/// delivered envelope for the recipient's own framing (mirrors
/// [`crate::service::SessionSpec::role`]); it is never used to resolve
/// delivery — the callback is a delivery target only, not an ownership or
/// reaping boundary (see the module's owning-session scoping in
/// `crate::service`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCallback {
    /// Mailbox root events are delivered to.
    pub inbox: String,
    /// Optional identity tag carried on delivered envelopes.
    pub role: Option<String>,
}

/// A versioned specification for one task, submitted to `baton task start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    /// [`TASK_SPEC_SCHEMA`].
    pub schema: String,
    /// The `baton service` session id this task is owned by. Session
    /// teardown cancels and reaps every task it owns, independent of the
    /// task's callback target.
    pub session: String,
    /// The command to execute.
    pub command: String,
    /// Arguments to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory for the command; `None` inherits the service's own.
    pub cwd: Option<String>,
    /// Additional environment variables for the command.
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Durations (ms elapsed since spawn) at which a milestone event fires.
    /// Opaque to `baton`: no default set and no cadence semantics — the
    /// caller supplies whichever durations it wants an event at, or none.
    #[serde(default)]
    pub milestones_ms: Vec<u64>,
    /// Maximum duration (ms elapsed since spawn) before the whole process
    /// group is terminated and a `timeout` terminal event is delivered.
    pub max_duration_ms: u64,
    /// Delivery target for this task's lifecycle events.
    pub callback: TaskCallback,
}

/// A parsed `baton task` invocation.
#[derive(Debug, PartialEq, Eq)]
pub enum TaskCommand {
    /// Submit a task spec to a live `baton service run` and return its task id.
    Start {
        /// The `--control <dir>` root.
        control: String,
        /// The task to start.
        spec: Box<TaskSpec>,
    },
    /// Report one task's (or every known task's) status.
    Status {
        /// The `--control <dir>` root.
        control: String,
        /// `--task <id>`; `None` reports every known task.
        task: Option<String>,
    },
    /// Cancel a task: idempotent. Kills the whole process group if still
    /// running, so a later reap delivers a `cancelled` terminal event.
    Cancel {
        /// The `--control <dir>` root.
        control: String,
        /// The task id to cancel.
        task: String,
    },
}

/// A task's lifecycle state, as reported by `baton task status` and stamped
/// on its terminal event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Still running.
    Running,
    /// Exited zero before `max_duration_ms` and before being cancelled.
    Completed,
    /// Exited non-zero before `max_duration_ms` and before being cancelled.
    Failed,
    /// Killed for exceeding `max_duration_ms`.
    Timeout,
    /// Killed by `baton task cancel`.
    Cancelled,
}

/// Durable admission phase for a task-start request.
///
/// A prepared task has a durable record but has not yet reached the response
/// boundary. A committed task has crossed the record boundary but may still
/// be waiting for response publication or phase persistence; a responded task
/// has a durable response and needs no response restoration on restart. A
/// consumed response also leaves a durable request acknowledgement until
/// reconciliation finalizes the phase. Older task records omit this field and
/// deserialize as responded, preserving tasks admitted before the transaction
/// marker was introduced without creating orphan responses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAdmissionPhase {
    /// The task record was persisted, but the successful admission response
    /// has not crossed the durable commit boundary.
    Prepared,
    /// The task record and admission decision are durable; the response may
    /// still be pending or awaiting phase persistence.
    Committed,
    /// The task-start response was durably written.
    #[default]
    Responded,
}

/// A durable on-disk record of one task the service has spawned: its
/// effective spec, live PID, and terminal state once known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Stable task id, minted at spawn.
    pub id: String,
    /// Request id that admitted this task. `None` identifies a legacy record
    /// written before durable task admission was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Durable admission phase used during supervisor restart reconciliation.
    #[serde(default)]
    pub admission: TaskAdmissionPhase,
    /// The effective spec this task was spawned from.
    pub spec: TaskSpec,
    /// OS pid of the task's process-group leader.
    pub pid: u32,
    /// Linux `/proc/<pid>/stat` starttime field, corroborating `pid` against
    /// reuse; `None` where it could not be read (non-Linux Unix).
    pub started_at: Option<String>,
    /// Canonical Unix epoch seconds parsed from macOS `ps lstart`; its
    /// presence marks a post-upgrade record that can use the epoch fast path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_epoch_secs: Option<i64>,
    /// Unix epoch milliseconds when the task child was spawned. Records
    /// written before restart reconciliation was introduced may omit this;
    /// the service fills it from its first post-upgrade observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_ms: Option<u64>,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Exit code, once terminal and the process exited normally.
    pub exit_code: Option<i32>,
    /// Elapsed milliseconds from spawn to terminal state, once known.
    pub elapsed_ms: Option<u64>,
    /// Path to the captured stdout tail.
    pub stdout_path: String,
    /// Path to the captured stderr tail.
    pub stderr_path: String,
    /// Highest milestone index already delivered (exclusive upper bound);
    /// `0` means none yet.
    pub delivered_milestones: usize,
}

/// What a task lifecycle event reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEventKind {
    /// One configured milestone duration elapsed.
    Milestone {
        /// Index into `TaskSpec::milestones_ms`.
        index: usize,
    },
    /// The task reached a terminal state.
    Terminal,
}

/// The JSON payload carried in a delivered task event's
/// [`crate::message::MessageEnvelope::body`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventBody {
    /// The task this event reports on.
    pub task_id: String,
    /// `"milestone"` or `"terminal"`.
    pub kind: String,
    /// Present for a milestone event: its index into `milestones_ms`.
    pub milestone_index: Option<usize>,
    /// Present for a terminal event: the task's final state.
    pub state: Option<TaskState>,
    /// Present for a terminal event whose process exited normally.
    pub exit_code: Option<i32>,
    /// Present for a terminal event: elapsed milliseconds from spawn.
    pub elapsed_ms: Option<u64>,
}

impl TaskEventBody {
    /// Builds the body for a milestone event.
    pub fn milestone(task_id: impl Into<String>, index: usize) -> Self {
        Self {
            task_id: task_id.into(),
            kind: "milestone".to_string(),
            milestone_index: Some(index),
            state: None,
            exit_code: None,
            elapsed_ms: None,
        }
    }

    /// Builds the body for a terminal event.
    pub fn terminal(
        task_id: impl Into<String>,
        state: TaskState,
        exit_code: Option<i32>,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            kind: "terminal".to_string(),
            milestone_index: None,
            state: Some(state),
            exit_code,
            elapsed_ms: Some(elapsed_ms),
        }
    }
}

/// Derives an event's deterministic, content-addressable id from `task_id`
/// and `kind` alone — no wall-clock or fire-time input, so a redelivery
/// always regenerates the same id and the mailbox's own dedup
/// (`done/`-membership) recognizes it as the same event.
pub fn task_event_id(task_id: &str, kind: TaskEventKind) -> String {
    match kind {
        TaskEventKind::Milestone { index } => format!("{task_id}-milestone-{index}"),
        TaskEventKind::Terminal => format!("{task_id}-terminal"),
    }
}

/// Returns the indices of every milestone newly due at `elapsed_ms`, given
/// `already_fired` milestones have already been delivered (the lowest
/// `already_fired` entries of `milestones_ms` are skipped). Each index is
/// returned at most once across the task's lifetime, in ascending order.
///
/// Pure and process-free: exercised directly by unit tests via a
/// [`Clock`]-derived `elapsed_ms`, without spawning anything.
pub fn milestones_due(elapsed_ms: u64, milestones_ms: &[u64], already_fired: usize) -> Vec<usize> {
    milestones_ms
        .iter()
        .enumerate()
        .skip(already_fired)
        .filter(|&(_, &threshold)| elapsed_ms >= threshold)
        .map(|(index, _)| index)
        .collect()
}

/// Whether `elapsed_ms` has crossed `max_duration_ms`.
pub fn max_duration_exceeded(elapsed_ms: u64, max_duration_ms: u64) -> bool {
    elapsed_ms >= max_duration_ms
}

/// A source of "now", in milliseconds, injectable so task-loop timing
/// decisions can be driven deterministically in tests instead of by real
/// sleeps.
pub trait Clock: Send + Sync {
    /// The current Unix epoch time in milliseconds. Must be monotonically
    /// non-decreasing for a given `Clock` instance. [`FakeClock`] produces
    /// synthetic readings in the same unit (its default starts at epoch zero)
    /// so persisted [`TaskRecord::started_ms`] values can be compared with an
    /// OS process start time after a supervisor restart.
    fn now_ms(&self) -> u64;
}

/// The real-time [`Clock`], backed by [`crate::events::now_ms`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        crate::events::now_ms()
    }
}

/// A manually-advanced [`Clock`] for deterministic tests: starts at `0` and
/// only moves when [`FakeClock::advance`] is called, so a test can assert a
/// milestone or max-duration decision at an exact elapsed time without any
/// real sleep.
#[derive(Debug, Default)]
pub struct FakeClock {
    now_ms: AtomicU64,
}

impl FakeClock {
    /// A fresh clock reading `0`.
    pub fn new() -> Self {
        Self {
            now_ms: AtomicU64::new(0),
        }
    }

    /// A clock initialized to a specific Unix epoch millisecond reading.
    pub fn at(epoch_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(epoch_ms),
        }
    }

    /// Moves the clock forward by `delta_ms`.
    pub fn advance(&self, delta_ms: u64) {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(milestones_ms: Vec<u64>, max_duration_ms: u64) -> TaskSpec {
        TaskSpec {
            schema: TASK_SPEC_SCHEMA.to_string(),
            session: "svc-1".to_string(),
            command: "true".to_string(),
            args: vec![],
            cwd: None,
            env: vec![],
            milestones_ms,
            max_duration_ms,
            callback: TaskCallback {
                inbox: "/tmp/inbox".to_string(),
                role: None,
            },
        }
    }

    #[test]
    fn task_spec_round_trips() {
        let original = spec(vec![100, 200], 1_000);
        let json = serde_json::to_string(&original).expect("serializes");
        let back: TaskSpec = serde_json::from_str(&json).expect("parses");
        assert_eq!(original, back);
    }

    #[test]
    fn event_id_is_deterministic_and_kind_specific() {
        let a = task_event_id("t-1", TaskEventKind::Milestone { index: 0 });
        let b = task_event_id("t-1", TaskEventKind::Milestone { index: 0 });
        assert_eq!(a, b, "same task+kind+index must yield the same id");
        assert_ne!(
            a,
            task_event_id("t-1", TaskEventKind::Milestone { index: 1 }),
            "different milestone index must yield a different id"
        );
        assert_ne!(
            a,
            task_event_id("t-1", TaskEventKind::Terminal),
            "milestone and terminal ids must never collide"
        );
        assert_eq!(
            task_event_id("t-1", TaskEventKind::Terminal),
            "t-1-terminal"
        );
    }

    #[test]
    fn milestones_due_fires_each_index_once() {
        let milestones = vec![100, 200, 300];
        assert_eq!(milestones_due(50, &milestones, 0), Vec::<usize>::new());
        assert_eq!(milestones_due(150, &milestones, 0), vec![0]);
        assert_eq!(milestones_due(150, &milestones, 1), Vec::<usize>::new());
        assert_eq!(milestones_due(999, &milestones, 0), vec![0, 1, 2]);
        assert_eq!(milestones_due(999, &milestones, 2), vec![2]);
        assert_eq!(milestones_due(999, &milestones, 3), Vec::<usize>::new());
    }

    #[test]
    fn max_duration_exceeded_is_inclusive_boundary() {
        assert!(!max_duration_exceeded(999, 1_000));
        assert!(max_duration_exceeded(1_000, 1_000));
        assert!(max_duration_exceeded(1_001, 1_000));
    }

    #[test]
    fn fake_clock_only_moves_on_advance() {
        let clock = FakeClock::new();
        assert_eq!(clock.now_ms(), 0);
        clock.advance(250);
        assert_eq!(clock.now_ms(), 250);
        clock.advance(750);
        assert_eq!(clock.now_ms(), 1_000);
    }

    #[test]
    fn fake_clock_can_start_at_epoch_milliseconds() {
        let clock = FakeClock::at(1_700_000_000_000);
        assert_eq!(clock.now_ms(), 1_700_000_000_000);
        clock.advance(250);
        assert_eq!(clock.now_ms(), 1_700_000_000_250);
    }

    #[test]
    fn task_event_body_round_trips() {
        let milestone = TaskEventBody::milestone("t-1", 2);
        let json = serde_json::to_string(&milestone).expect("serializes");
        let back: TaskEventBody = serde_json::from_str(&json).expect("parses");
        assert_eq!(milestone, back);

        let terminal = TaskEventBody::terminal("t-1", TaskState::Completed, Some(0), 1_234);
        let json = serde_json::to_string(&terminal).expect("serializes");
        let back: TaskEventBody = serde_json::from_str(&json).expect("parses");
        assert_eq!(terminal, back);
    }

    #[test]
    fn system_clock_is_monotonically_non_decreasing_across_two_reads() {
        let clock = SystemClock;
        let first = clock.now_ms();
        let second = clock.now_ms();
        assert!(second >= first);
    }
}
