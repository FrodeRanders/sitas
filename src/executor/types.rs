#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;

/// Identifier assigned to an executor scheduling group.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchedulingGroupId(pub usize);

/// Identifier of the default scheduling group used by ordinary spawns.
pub const DEFAULT_SCHEDULING_GROUP_ID: SchedulingGroupId = SchedulingGroupId(0);

/// Relative weight assigned to the default scheduling group.
pub const DEFAULT_SCHEDULING_GROUP_SHARES: u32 = 100;

/// Identifier assigned to a task when it is spawned.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub usize);

/// Coarse lifecycle state for a task in an executor snapshot.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// The task is in the ready queue.
    Queued,
    /// The task is currently being polled.
    Polling,
    /// The task is pending and waiting for another wakeup.
    Waiting,
    /// The task completed normally.
    Completed,
    /// The task was canceled before completing.
    Cancelled,
}

/// What a pending task last registered interest in.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWait {
    /// The task yielded or is waiting on an opaque waker.
    Unknown,
    /// The task is waiting for an executor timer.
    Timer {
        /// The instant at which the timer becomes ready.
        deadline: Instant,
    },
    /// The task is waiting for a file descriptor to become readable.
    #[cfg(unix)]
    Readable {
        /// File descriptor registered for readability.
        fd: RawFd,
    },
    /// The task is waiting for a file descriptor to become writable.
    #[cfg(unix)]
    Writable {
        /// File descriptor registered for writability.
        fd: RawFd,
    },
}

/// Owned point-in-time summary of one task.
#[must_use]
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    /// Executor-local task identifier.
    pub id: TaskId,
    /// Optional human-readable task name supplied by the spawner.
    pub name: Option<String>,
    /// Scheduling group this task belongs to.
    pub scheduling_group_id: SchedulingGroupId,
    /// Name of the scheduling group this task belongs to, if known.
    pub scheduling_group_name: Option<String>,
    /// Current coarse task lifecycle state.
    pub status: TaskStatus,
    /// Last wait interest registered by this task, if known.
    pub waiting_for: Option<TaskWait>,
    /// Number of times the task future has been polled.
    pub poll_count: u64,
    /// Total wall-clock time spent polling this task.
    pub total_poll_time: Duration,
    /// When this task was created.
    pub created_at: Instant,
    /// When this task was last placed on the ready queue.
    pub last_scheduled_at: Option<Instant>,
    /// When this task's most recent poll started.
    pub last_poll_started_at: Option<Instant>,
    /// When this task's most recent poll finished.
    pub last_poll_finished_at: Option<Instant>,
}

/// Owned point-in-time summary of one scheduling group.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulingGroupSnapshot {
    /// Executor-local scheduling group identifier.
    pub id: SchedulingGroupId,
    /// Human-readable group name.
    pub name: String,
    /// Relative scheduling weight.
    pub shares: u32,
    /// Number of ready tasks currently queued in this group.
    pub ready_queue_len: usize,
    /// Weighted virtual runtime accumulated by this group.
    pub virtual_runtime: u128,
    /// Total wall-clock poll time charged to this group.
    pub total_poll_time: Duration,
}

/// Owned point-in-time summary of one executor.
#[must_use]
#[derive(Debug, Clone)]
pub struct ExecutorSnapshot {
    /// Whether this executor still accepts new tasks.
    pub accepting: bool,
    /// Number of live spawner handles.
    pub spawner_count: usize,
    /// Number of tasks the scheduler still considers unfinished.
    pub task_count: usize,
    /// Number of tasks currently queued for polling.
    pub ready_queue_len: usize,
    /// Owned snapshots of executor scheduling groups.
    pub scheduling_groups: Vec<SchedulingGroupSnapshot>,
    /// Number of registered timers.
    pub timer_count: usize,
    /// Number of registered read-readiness interests.
    #[cfg(unix)]
    pub read_interest_count: usize,
    /// Number of registered write-readiness interests.
    #[cfg(unix)]
    pub write_interest_count: usize,
    /// Snapshot of the executor-owned Linux `io_uring` dispatcher, if installed.
    #[cfg(target_os = "linux")]
    pub io_uring: Option<IoUringDispatcherSnapshot>,
    /// Maximum number of ready tasks polled before timers and readiness are checked.
    pub ready_poll_budget: usize,
    /// Number of tasks accepted by this executor since startup.
    pub total_spawned_tasks: u64,
    /// Number of tasks that have completed, panicked, or been canceled since startup.
    pub total_completed_tasks: u64,
    /// Number of spawned task polls performed since startup.
    pub total_task_polls: u64,
    /// Number of ready-poll batches that consumed the full ready-poll budget.
    pub ready_poll_budget_exhaustions: u64,
    /// Number of idle driver events observed by the executor.
    pub total_driver_events: u64,
    /// Number of readiness driver events observed by the executor.
    #[cfg(unix)]
    pub total_readiness_events: u64,
    /// Number of readiness driver events that reported at least one readable fd.
    #[cfg(unix)]
    pub total_readable_events: u64,
    /// Number of readiness driver events that reported at least one writable fd.
    #[cfg(unix)]
    pub total_writable_events: u64,
    /// Number of Linux completion driver events observed by the executor.
    #[cfg(target_os = "linux")]
    pub total_completion_events: u64,
    /// Owned snapshots for tasks that are still externally observable.
    pub tasks: Vec<TaskSnapshot>,
}
