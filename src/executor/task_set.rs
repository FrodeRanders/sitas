use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use super::task::Task;
use super::{SpawnError, TaskId};

#[derive(Debug)]
pub(super) struct SchedulerTaskSet {
    queue: VecDeque<Arc<Task>>,
    tasks: Vec<Weak<Task>>,
    accepting: bool,
    spawner_count: usize,
    task_count: usize,
    next_task_id: usize,
}

#[derive(Debug)]
pub(super) struct SchedulerTaskSnapshot {
    pub(super) accepting: bool,
    pub(super) spawner_count: usize,
    pub(super) task_count: usize,
    pub(super) ready_queue_len: usize,
    pub(super) tasks: Vec<Weak<Task>>,
}

impl SchedulerTaskSet {
    pub(super) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            tasks: Vec::new(),
            accepting: true,
            spawner_count: 1,
            task_count: 0,
            next_task_id: 0,
        }
    }

    pub(super) fn allocate_task_id(&mut self) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id = self.next_task_id.wrapping_add(1);
        TaskId(id)
    }

    pub(super) fn add_spawner(&mut self) {
        self.spawner_count += 1;
    }

    pub(super) fn remove_spawner(&mut self) -> bool {
        self.spawner_count = self.spawner_count.saturating_sub(1);
        self.spawner_count == 0
    }

    pub(super) fn schedule_new(&mut self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !self.accepting {
            task.clear_queued();
            return Err(SpawnError);
        }

        self.task_count += 1;
        self.tasks.push(Arc::downgrade(&task));
        self.queue.push_back(task);
        Ok(())
    }

    pub(super) fn schedule_existing(&mut self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !self.accepting {
            task.clear_queued();
            return Err(SpawnError);
        }

        self.queue.push_back(task);
        Ok(())
    }

    pub(super) fn next_task(&mut self) -> Option<Arc<Task>> {
        self.queue.pop_front()
    }

    pub(super) fn is_drained(&self) -> bool {
        self.queue.is_empty() && self.spawner_count == 0 && self.task_count == 0
    }

    pub(super) fn has_ready_tasks(&self) -> bool {
        !self.queue.is_empty()
    }

    pub(super) fn close(&mut self) -> Vec<Arc<Task>> {
        self.accepting = false;
        self.task_count = 0;
        self.queue.clear();

        self.tasks
            .drain(..)
            .filter_map(|task| task.upgrade())
            .collect()
    }

    pub(super) fn finish_task(&mut self) -> bool {
        self.task_count = self.task_count.saturating_sub(1);
        self.is_drained()
    }

    pub(super) fn snapshot(&self) -> SchedulerTaskSnapshot {
        SchedulerTaskSnapshot {
            accepting: self.accepting,
            spawner_count: self.spawner_count,
            task_count: self.task_count,
            ready_queue_len: self.queue.len(),
            tasks: self.tasks.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::os::OsReactor;

    use super::*;
    use crate::executor::scheduler::Scheduler;

    fn scheduler() -> Arc<Scheduler> {
        let reactor = OsReactor::new().expect("failed to create test reactor");
        Arc::new(Scheduler::new(reactor.waker()))
    }

    fn task(id: usize) -> Arc<Task> {
        Arc::new(Task::new(
            TaskId(id),
            None,
            Box::pin(async {}),
            scheduler(),
            None,
        ))
    }

    #[test]
    fn task_ids_are_allocated_monotonically() {
        let mut tasks = SchedulerTaskSet::new();

        assert_eq!(tasks.allocate_task_id(), TaskId(0));
        assert_eq!(tasks.allocate_task_id(), TaskId(1));
        assert_eq!(tasks.allocate_task_id(), TaskId(2));
    }

    #[test]
    fn new_tasks_are_counted_and_queued_in_order() {
        let mut tasks = SchedulerTaskSet::new();
        let first = task(0);
        let second = task(1);

        tasks.schedule_new(Arc::clone(&first)).unwrap();
        tasks.schedule_new(Arc::clone(&second)).unwrap();

        assert_eq!(tasks.snapshot().task_count, 2);
        assert_eq!(tasks.snapshot().ready_queue_len, 2);
        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &first));
        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &second));
        assert!(tasks.next_task().is_none());
    }

    #[test]
    fn existing_tasks_are_requeued_without_changing_task_count() {
        let mut tasks = SchedulerTaskSet::new();
        let task = task(0);

        tasks.schedule_new(Arc::clone(&task)).unwrap();
        tasks.schedule_existing(Arc::clone(&task)).unwrap();

        assert_eq!(tasks.snapshot().task_count, 1);
        assert_eq!(tasks.snapshot().ready_queue_len, 2);
    }

    #[test]
    fn finishing_tasks_and_dropping_spawners_drains_the_set() {
        let mut tasks = SchedulerTaskSet::new();
        let task = task(0);

        tasks.schedule_new(task).unwrap();
        assert!(tasks.next_task().is_some());

        assert!(!tasks.finish_task());
        assert!(!tasks.is_drained());
        assert!(tasks.remove_spawner());
        assert!(tasks.is_drained());
    }

    #[test]
    fn closing_stops_accepting_and_returns_observable_tasks() {
        let mut tasks = SchedulerTaskSet::new();
        let task = task(0);
        assert!(task.mark_queued());

        tasks.schedule_new(Arc::clone(&task)).unwrap();
        let closed = tasks.close();

        assert_eq!(closed.len(), 1);
        assert!(Arc::ptr_eq(&closed[0], &task));
        assert!(!tasks.snapshot().accepting);
        assert_eq!(tasks.snapshot().task_count, 0);
        assert_eq!(tasks.snapshot().ready_queue_len, 0);

        assert!(tasks.schedule_new(Arc::clone(&task)).is_err());
        assert!(task.mark_queued());
    }
}
