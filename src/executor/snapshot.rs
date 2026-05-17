use super::counters::SchedulerCounters;
use super::task_set::SchedulerTaskSnapshot;
use super::{ExecutorSnapshot, READY_POLL_BUDGET};

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;

pub(super) struct ExecutorSnapshotParts {
    pub(super) tasks: SchedulerTaskSnapshot,
    pub(super) timer_count: usize,
    pub(super) counters: SchedulerCounters,
    #[cfg(unix)]
    pub(super) read_interest_count: usize,
    #[cfg(unix)]
    pub(super) write_interest_count: usize,
    #[cfg(target_os = "linux")]
    pub(super) io_uring: Option<IoUringDispatcherSnapshot>,
}

pub(super) fn build_executor_snapshot(parts: ExecutorSnapshotParts) -> ExecutorSnapshot {
    let mut tasks = parts
        .tasks
        .tasks
        .into_iter()
        .filter_map(|task| task.upgrade())
        .map(|task| task.snapshot())
        .collect::<Vec<_>>();
    tasks.sort_by_key(|task| task.id);

    ExecutorSnapshot {
        accepting: parts.tasks.accepting,
        spawner_count: parts.tasks.spawner_count,
        task_count: parts.tasks.task_count,
        ready_queue_len: parts.tasks.ready_queue_len,
        timer_count: parts.timer_count,
        #[cfg(unix)]
        read_interest_count: parts.read_interest_count,
        #[cfg(unix)]
        write_interest_count: parts.write_interest_count,
        #[cfg(target_os = "linux")]
        io_uring: parts.io_uring,
        ready_poll_budget: READY_POLL_BUDGET,
        total_spawned_tasks: parts.counters.total_spawned_tasks,
        total_completed_tasks: parts.counters.total_completed_tasks,
        total_task_polls: parts.counters.total_task_polls,
        ready_poll_budget_exhaustions: parts.counters.ready_poll_budget_exhaustions,
        total_driver_events: parts.counters.total_driver_events,
        #[cfg(unix)]
        total_readiness_events: parts.counters.total_readiness_events,
        #[cfg(unix)]
        total_readable_events: parts.counters.total_readable_events,
        #[cfg(unix)]
        total_writable_events: parts.counters.total_writable_events,
        #[cfg(target_os = "linux")]
        total_completion_events: parts.counters.total_completion_events,
        tasks,
    }
}
