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
