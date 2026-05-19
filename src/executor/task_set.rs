use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::Duration;

use super::task::Task;
use super::types::{
    DEFAULT_SCHEDULING_GROUP_ID, DEFAULT_SCHEDULING_GROUP_SHARES, SchedulingGroupId,
    SchedulingGroupSnapshot,
};
use super::{SpawnError, TaskId};

#[derive(Debug)]
pub(super) struct SchedulerTaskSet {
    groups: Vec<SchedulerGroup>,
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
    pub(super) groups: Vec<SchedulingGroupSnapshot>,
    pub(super) tasks: Vec<Weak<Task>>,
}

#[derive(Debug)]
struct SchedulerGroup {
    id: SchedulingGroupId,
    name: String,
    shares: u32,
    virtual_runtime: u128,
    total_polls: u64,
    total_poll_time: Duration,
    queue: VecDeque<Arc<Task>>,
}

impl SchedulerTaskSet {
    pub(super) fn new() -> Self {
        Self {
            groups: vec![SchedulerGroup {
                id: DEFAULT_SCHEDULING_GROUP_ID,
                name: String::from("default"),
                shares: DEFAULT_SCHEDULING_GROUP_SHARES,
                virtual_runtime: 0,
                total_polls: 0,
                total_poll_time: Duration::ZERO,
                queue: VecDeque::new(),
            }],
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

    pub(super) fn create_scheduling_group(
        &mut self,
        name: String,
        shares: u32,
    ) -> SchedulingGroupId {
        let id = SchedulingGroupId(self.groups.len());
        let virtual_runtime = self.minimum_virtual_runtime();
        self.groups.push(SchedulerGroup {
            id,
            name,
            shares,
            virtual_runtime,
            total_polls: 0,
            total_poll_time: Duration::ZERO,
            queue: VecDeque::new(),
        });
        id
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
            return Err(SpawnError::Closed);
        }

        let Some(group_idx) = self.group_index(task.scheduling_group_id()) else {
            task.clear_queued();
            return Err(SpawnError::Closed);
        };

        self.task_count += 1;
        self.tasks.push(Arc::downgrade(&task));
        self.groups[group_idx].queue.push_back(task);
        Ok(())
    }

    pub(super) fn schedule_existing(&mut self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !self.accepting {
            task.clear_queued();
            return Err(SpawnError::Closed);
        }

        let Some(group_idx) = self.group_index(task.scheduling_group_id()) else {
            task.clear_queued();
            return Err(SpawnError::Closed);
        };

        self.groups[group_idx].queue.push_back(task);
        Ok(())
    }

    pub(super) fn next_task(&mut self) -> Option<Arc<Task>> {
        let group = self
            .groups
            .iter_mut()
            .filter(|group| !group.queue.is_empty())
            .min_by_key(|group| (group.virtual_runtime, group.id))?;

        group.queue.pop_front()
    }

    pub(super) fn record_task_poll(
        &mut self,
        group_id: SchedulingGroupId,
        poll_duration: Duration,
    ) {
        let Some(group) = self.group_mut(group_id) else {
            return;
        };

        group.total_poll_time += poll_duration;
        group.total_polls += 1;
        let nanos = poll_duration.as_nanos().max(1);
        group.virtual_runtime +=
            nanos * u128::from(DEFAULT_SCHEDULING_GROUP_SHARES) / u128::from(group.shares);
    }

    pub(super) fn is_drained(&self) -> bool {
        self.ready_queue_len() == 0 && self.spawner_count == 0 && self.task_count == 0
    }

    pub(super) fn has_ready_tasks(&self) -> bool {
        self.ready_queue_len() > 0
    }

    pub(super) fn close(&mut self) -> Vec<Arc<Task>> {
        self.accepting = false;
        self.task_count = 0;
        for group in &mut self.groups {
            group.queue.clear();
        }

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
            ready_queue_len: self.ready_queue_len(),
            groups: self.group_snapshots(),
            tasks: self.tasks.clone(),
        }
    }

    fn group_mut(&mut self, id: SchedulingGroupId) -> Option<&mut SchedulerGroup> {
        self.groups.get_mut(id.0).filter(|group| group.id == id)
    }

    fn group_index(&self, id: SchedulingGroupId) -> Option<usize> {
        self.groups.get(id.0).filter(|group| group.id == id)?;
        Some(id.0)
    }

    fn ready_queue_len(&self) -> usize {
        self.groups.iter().map(|group| group.queue.len()).sum()
    }

    fn minimum_virtual_runtime(&self) -> u128 {
        self.groups
            .iter()
            .map(|group| group.virtual_runtime)
            .min()
            .unwrap_or(0)
    }

    fn group_snapshots(&self) -> Vec<SchedulingGroupSnapshot> {
        self.groups
            .iter()
            .map(|group| SchedulingGroupSnapshot {
                id: group.id,
                name: group.name.clone(),
                shares: group.shares,
                ready_queue_len: group.queue.len(),
                virtual_runtime: group.virtual_runtime,
                total_polls: group.total_polls,
                total_poll_time: group.total_poll_time,
            })
            .collect()
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
        task_in_group(id, DEFAULT_SCHEDULING_GROUP_ID)
    }

    fn task_in_group(id: usize, group_id: SchedulingGroupId) -> Arc<Task> {
        Arc::new(Task::new_in_group(
            TaskId(id),
            None,
            group_id,
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
        assert_eq!(tasks.snapshot().groups[0].ready_queue_len, 2);
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

    #[test]
    fn scheduling_groups_are_reported_in_snapshots() {
        let mut tasks = SchedulerTaskSet::new();
        let group_id = tasks.create_scheduling_group("bulk".to_string(), 25);
        let task = task_in_group(1, group_id);

        tasks.schedule_new(Arc::clone(&task)).unwrap();
        let snapshot = tasks.snapshot();

        assert_eq!(snapshot.groups.len(), 2);
        assert_eq!(snapshot.groups[0].name, "default");
        assert_eq!(snapshot.groups[0].shares, DEFAULT_SCHEDULING_GROUP_SHARES);
        assert_eq!(snapshot.groups[1].id, group_id);
        assert_eq!(snapshot.groups[1].name, "bulk");
        assert_eq!(snapshot.groups[1].shares, 25);
        assert_eq!(snapshot.groups[1].ready_queue_len, 1);
        assert_eq!(snapshot.groups[1].total_polls, 0);
    }

    #[test]
    fn weighted_selection_prefers_lower_virtual_runtime() {
        let mut tasks = SchedulerTaskSet::new();
        let low_share = tasks.create_scheduling_group("low".to_string(), 10);
        let high_share = tasks.create_scheduling_group("high".to_string(), 100);

        let first_low = task_in_group(1, low_share);
        let first_high = task_in_group(2, high_share);
        tasks.schedule_new(Arc::clone(&first_low)).unwrap();
        tasks.schedule_new(Arc::clone(&first_high)).unwrap();

        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &first_low));
        tasks.record_task_poll(low_share, Duration::from_millis(1));
        assert_eq!(tasks.snapshot().groups[low_share.0].total_polls, 1);

        let second_low = task_in_group(3, low_share);
        let second_high = task_in_group(4, high_share);
        tasks.schedule_new(Arc::clone(&second_low)).unwrap();
        tasks.schedule_new(Arc::clone(&second_high)).unwrap();

        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &first_high));
        tasks.record_task_poll(high_share, Duration::from_millis(1));
        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &second_high));
    }

    #[test]
    fn late_created_group_starts_at_current_minimum_virtual_runtime() {
        let mut tasks = SchedulerTaskSet::new();
        let first_group = tasks.create_scheduling_group("first".to_string(), 100);

        tasks.record_task_poll(DEFAULT_SCHEDULING_GROUP_ID, Duration::from_millis(2));
        tasks.record_task_poll(first_group, Duration::from_millis(1));

        let late_group = tasks.create_scheduling_group("late".to_string(), 100);
        let snapshot = tasks.snapshot();
        let first_vruntime = snapshot
            .groups
            .iter()
            .find(|group| group.id == first_group)
            .unwrap()
            .virtual_runtime;
        let late_vruntime = snapshot
            .groups
            .iter()
            .find(|group| group.id == late_group)
            .unwrap()
            .virtual_runtime;

        assert_eq!(first_vruntime, late_vruntime);

        let first_task = task_in_group(1, first_group);
        let late_task = task_in_group(2, late_group);
        tasks.schedule_new(Arc::clone(&first_task)).unwrap();
        tasks.schedule_new(Arc::clone(&late_task)).unwrap();

        assert!(Arc::ptr_eq(&tasks.next_task().unwrap(), &first_task));
    }
}
