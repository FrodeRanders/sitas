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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::os::OsReactor;

    use super::super::scheduler::Scheduler;
    use super::super::task::Task;
    use super::super::{BoxFuture, TaskId};
    use super::*;

    fn scheduler() -> Arc<Scheduler> {
        let reactor = OsReactor::new().expect("failed to create test reactor");
        Arc::new(Scheduler::new(reactor.waker()))
    }

    fn task(id: usize) -> Arc<Task> {
        Arc::new(Task::new(
            TaskId(id),
            None,
            Box::pin(async {}) as BoxFuture,
            scheduler(),
            None,
        ))
    }

    fn snapshot_parts(task_snapshot: SchedulerTaskSnapshot) -> ExecutorSnapshotParts {
        let counters = SchedulerCounters {
            total_spawned_tasks: 10,
            total_completed_tasks: 8,
            total_task_polls: 24,
            ready_poll_budget_exhaustions: 2,
            total_driver_events: 6,
            #[cfg(unix)]
            total_readiness_events: 4,
            #[cfg(unix)]
            total_readable_events: 3,
            #[cfg(unix)]
            total_writable_events: 2,
            #[cfg(target_os = "linux")]
            total_completion_events: 2,
        };

        ExecutorSnapshotParts {
            tasks: task_snapshot,
            timer_count: 3,
            counters,
            #[cfg(unix)]
            read_interest_count: 5,
            #[cfg(unix)]
            write_interest_count: 7,
            #[cfg(target_os = "linux")]
            io_uring: None,
        }
    }

    #[test]
    fn snapshot_builder_copies_scheduler_fields_and_counters() {
        let task_snapshot = SchedulerTaskSnapshot {
            accepting: false,
            spawner_count: 2,
            task_count: 4,
            ready_queue_len: 1,
            tasks: Vec::new(),
        };

        let snapshot = build_executor_snapshot(snapshot_parts(task_snapshot));

        assert!(!snapshot.accepting);
        assert_eq!(snapshot.spawner_count, 2);
        assert_eq!(snapshot.task_count, 4);
        assert_eq!(snapshot.ready_queue_len, 1);
        assert_eq!(snapshot.timer_count, 3);
        assert_eq!(snapshot.ready_poll_budget, super::super::READY_POLL_BUDGET);
        assert_eq!(snapshot.total_spawned_tasks, 10);
        assert_eq!(snapshot.total_completed_tasks, 8);
        assert_eq!(snapshot.total_task_polls, 24);
        assert_eq!(snapshot.ready_poll_budget_exhaustions, 2);
        assert_eq!(snapshot.total_driver_events, 6);
        #[cfg(unix)]
        {
            assert_eq!(snapshot.read_interest_count, 5);
            assert_eq!(snapshot.write_interest_count, 7);
            assert_eq!(snapshot.total_readiness_events, 4);
            assert_eq!(snapshot.total_readable_events, 3);
            assert_eq!(snapshot.total_writable_events, 2);
        }
        #[cfg(target_os = "linux")]
        {
            assert_eq!(snapshot.total_completion_events, 2);
            assert!(snapshot.io_uring.is_none());
        }
    }

    #[test]
    fn snapshot_builder_filters_dropped_tasks_and_sorts_live_tasks_by_id() {
        let high = task(9);
        let low = task(1);
        let dropped = task(5);
        let dropped_weak = Arc::downgrade(&dropped);
        drop(dropped);

        let task_snapshot = SchedulerTaskSnapshot {
            accepting: true,
            spawner_count: 1,
            task_count: 2,
            ready_queue_len: 0,
            tasks: vec![Arc::downgrade(&high), dropped_weak, Arc::downgrade(&low)],
        };

        let snapshot = build_executor_snapshot(snapshot_parts(task_snapshot));

        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, TaskId(1));
        assert_eq!(snapshot.tasks[1].id, TaskId(9));
    }
}
