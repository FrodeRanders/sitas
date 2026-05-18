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

impl TaskSnapshot {
    /// Returns how long this task had existed at `now`.
    pub fn age_at(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.created_at)
    }

    /// Returns how long ago this task was last queued, if it has been queued.
    pub fn time_since_last_scheduled_at(&self, now: Instant) -> Option<Duration> {
        self.last_scheduled_at
            .map(|instant| now.saturating_duration_since(instant))
    }

    /// Returns how long ago this task's most recent poll started, if it has
    /// been polled.
    pub fn time_since_last_poll_started_at(&self, now: Instant) -> Option<Duration> {
        self.last_poll_started_at
            .map(|instant| now.saturating_duration_since(instant))
    }

    /// Returns how long ago this task's most recent poll finished, if a poll
    /// has finished.
    pub fn time_since_last_poll_finished_at(&self, now: Instant) -> Option<Duration> {
        self.last_poll_finished_at
            .map(|instant| now.saturating_duration_since(instant))
    }

    /// Returns how long this task had been in its current coarse state at
    /// `now`.
    pub fn state_duration_at(&self, now: Instant) -> Duration {
        let entered_state_at = match self.status {
            TaskStatus::Queued => self.last_scheduled_at,
            TaskStatus::Polling => self.last_poll_started_at,
            TaskStatus::Waiting | TaskStatus::Completed | TaskStatus::Cancelled => {
                self.last_poll_finished_at
            }
        }
        .unwrap_or(self.created_at);

        now.saturating_duration_since(entered_state_at)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn task_snapshot(status: TaskStatus) -> TaskSnapshot {
        let created_at = Instant::now();
        TaskSnapshot {
            id: TaskId(7),
            name: Some(String::from("worker")),
            scheduling_group_id: DEFAULT_SCHEDULING_GROUP_ID,
            scheduling_group_name: Some(String::from("default")),
            status,
            waiting_for: None,
            poll_count: 3,
            total_poll_time: Duration::from_millis(2),
            created_at,
            last_scheduled_at: Some(created_at + Duration::from_millis(10)),
            last_poll_started_at: Some(created_at + Duration::from_millis(20)),
            last_poll_finished_at: Some(created_at + Duration::from_millis(30)),
        }
    }

    #[test]
    fn task_snapshot_duration_helpers_report_elapsed_times() {
        let task = task_snapshot(TaskStatus::Waiting);
        let now = task.created_at + Duration::from_millis(45);

        assert_eq!(task.age_at(now), Duration::from_millis(45));
        assert_eq!(
            task.time_since_last_scheduled_at(now),
            Some(Duration::from_millis(35))
        );
        assert_eq!(
            task.time_since_last_poll_started_at(now),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            task.time_since_last_poll_finished_at(now),
            Some(Duration::from_millis(15))
        );
    }

    #[test]
    fn task_snapshot_state_duration_uses_current_state_timestamp() {
        let queued = task_snapshot(TaskStatus::Queued);
        let now = queued.created_at + Duration::from_millis(45);
        assert_eq!(queued.state_duration_at(now), Duration::from_millis(35));

        let polling = task_snapshot(TaskStatus::Polling);
        let now = polling.created_at + Duration::from_millis(45);
        assert_eq!(polling.state_duration_at(now), Duration::from_millis(25));

        for status in [
            TaskStatus::Waiting,
            TaskStatus::Completed,
            TaskStatus::Cancelled,
        ] {
            let task = task_snapshot(status);
            let now = task.created_at + Duration::from_millis(45);
            assert_eq!(task.state_duration_at(now), Duration::from_millis(15));
        }
    }

    #[test]
    fn task_snapshot_state_duration_falls_back_to_created_at() {
        let created_at = Instant::now();
        let task = TaskSnapshot {
            id: TaskId(7),
            name: None,
            scheduling_group_id: DEFAULT_SCHEDULING_GROUP_ID,
            scheduling_group_name: None,
            status: TaskStatus::Queued,
            waiting_for: None,
            poll_count: 0,
            total_poll_time: Duration::ZERO,
            created_at,
            last_scheduled_at: None,
            last_poll_started_at: None,
            last_poll_finished_at: None,
        };
        let now = created_at + Duration::from_millis(5);

        assert_eq!(task.state_duration_at(now), Duration::from_millis(5));
    }
}
