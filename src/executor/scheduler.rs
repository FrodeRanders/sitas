use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;
#[cfg(unix)]
use crate::os::OsWaker;

#[cfg(target_os = "linux")]
use super::IoUringExecutorStatus;
use super::counters::SchedulerCounters;
use super::current::set_current_task_waiting_for;
#[cfg(unix)]
use super::io_interest::ReadinessInterests;
use super::scheduling_group::ExecutorId;
use super::snapshot::{ExecutorSnapshotParts, build_executor_snapshot};
use super::task::Task;
use super::task_set::SchedulerTaskSet;
use super::timer::TimerSet;
use super::{ExecutorSnapshot, SchedulingGroupId, SpawnError, TaskId, TaskWait};

#[derive(Debug)]
pub(super) struct Scheduler {
    id: ExecutorId,
    state: Mutex<SchedulerState>,
    #[cfg(unix)]
    waker: OsWaker,
}

#[derive(Debug)]
struct SchedulerState {
    tasks: SchedulerTaskSet,
    pub(super) timers: TimerSet,
    #[cfg(unix)]
    io_interests: ReadinessInterests,
    #[cfg(target_os = "linux")]
    io_uring: Option<IoUringDispatcherSnapshot>,
    #[cfg(target_os = "linux")]
    io_uring_status: IoUringExecutorStatus,
    counters: SchedulerCounters,
}

impl Scheduler {
    pub(super) fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            id: ExecutorId::allocate(),
            state: Mutex::new(SchedulerState {
                tasks: SchedulerTaskSet::new(),
                timers: TimerSet::new(),
                #[cfg(unix)]
                io_interests: ReadinessInterests::new(),
                #[cfg(target_os = "linux")]
                io_uring: None,
                #[cfg(target_os = "linux")]
                io_uring_status: IoUringExecutorStatus::NotStarted,
                counters: SchedulerCounters::default(),
            }),
            #[cfg(unix)]
            waker,
        }
    }

    pub(super) fn id(&self) -> ExecutorId {
        self.id
    }

    pub(super) fn allocate_task_id(&self) -> TaskId {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.allocate_task_id()
    }

    pub(super) fn create_scheduling_group(&self, name: String, shares: u32) -> SchedulingGroupId {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.create_scheduling_group(name, shares)
    }

    pub(super) fn add_spawner(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.add_spawner();
    }

    pub(super) fn remove_spawner(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.tasks.remove_spawner()
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    pub(super) fn schedule(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.tasks.schedule_new(task)?;
            state.counters.record_spawned_task();
        }

        self.wake_reactor();
        Ok(())
    }

    pub(super) fn schedule_existing(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.tasks.schedule_existing(task)?;
        }

        self.wake_reactor();
        Ok(())
    }

    pub(super) fn next_task(&self) -> Option<Arc<Task>> {
        self.state
            .lock()
            .expect("scheduler state mutex poisoned")
            .tasks
            .next_task()
    }

    pub(super) fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.is_drained()
    }

    pub(super) fn has_ready_tasks(&self) -> bool {
        self.state
            .lock()
            .expect("scheduler state mutex poisoned")
            .tasks
            .has_ready_tasks()
    }

    pub(super) fn close(&self) {
        let tasks = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.timers.clear();
            #[cfg(unix)]
            state.io_interests.clear();

            state.tasks.close()
        };

        for task in tasks {
            task.drop_future();
        }

        self.wake_reactor();
    }

    pub(super) fn snapshot(&self) -> ExecutorSnapshot {
        let parts = {
            let state = self.state.lock().expect("scheduler state mutex poisoned");
            ExecutorSnapshotParts {
                tasks: state.tasks.snapshot(),
                timer_count: state.timers.len(),
                counters: state.counters,
                #[cfg(unix)]
                read_interest_count: state.io_interests.read_len(),
                #[cfg(unix)]
                write_interest_count: state.io_interests.write_len(),
                #[cfg(target_os = "linux")]
                io_uring: state.io_uring,
                #[cfg(target_os = "linux")]
                io_uring_status: state.io_uring_status,
            }
        };

        build_executor_snapshot(parts)
    }

    pub(super) fn finish_task(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let should_wake = state.tasks.finish_task();
            state.counters.record_completed_task();
            should_wake
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    pub(super) fn record_ready_poll_batch(&self, polled: usize, exhausted_budget: bool) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state
            .counters
            .record_ready_poll_batch(polled, exhausted_budget);
    }

    pub(super) fn record_task_poll(&self, group_id: SchedulingGroupId, poll_duration: Duration) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.record_task_poll(group_id, poll_duration);
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    pub(super) fn record_readiness_driver_event(&self, readable: bool, writable: bool) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state
            .counters
            .record_readiness_driver_event(readable, writable);
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_driver_event(
        &self,
        readiness: bool,
        readable: bool,
        writable: bool,
        completion: bool,
    ) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state
            .counters
            .record_driver_event(readiness, readable, writable, completion);
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_completion_dispatch_batch(
        &self,
        dispatched: usize,
        exhausted_budget: bool,
    ) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state
            .counters
            .record_completion_dispatch_batch(dispatched, exhausted_budget);
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_io_uring_snapshot(
        &self,
        status: IoUringExecutorStatus,
        snapshot: Option<IoUringDispatcherSnapshot>,
    ) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_uring_status = status;
        state.io_uring = snapshot;
    }

    pub(super) fn allocate_timer_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.allocate_id()
    }

    pub(super) fn register_timer(&self, id: usize, deadline: Instant, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Timer { deadline });
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.timers.register(id, deadline, waker)
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    pub(super) fn remove_timer(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.remove(id);
    }

    pub(super) fn wake_expired_timers(&self) {
        let expired = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.timers.expired(Instant::now())
        };

        for waker in expired {
            waker.wake();
        }
    }

    pub(super) fn time_until_next_timer(&self) -> Option<Duration> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.time_until_next(Instant::now())
    }

    #[cfg(unix)]
    pub(super) fn allocate_read_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.allocate_read_id()
    }

    #[cfg(unix)]
    pub(super) fn register_read_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Readable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.io_interests.register_read(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    pub(super) fn remove_read_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.remove_read(id);
    }

    #[cfg(unix)]
    pub(super) fn read_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.read_fds()
    }

    #[cfg(unix)]
    pub(super) fn wake_readable_fds(&self, readable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.io_interests.wake_readable(readable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    pub(super) fn take_ready_read_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.take_ready_read(id)
    }

    #[cfg(unix)]
    pub(super) fn allocate_write_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.allocate_write_id()
    }

    #[cfg(unix)]
    pub(super) fn register_write_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Writable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.io_interests.register_write(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    pub(super) fn remove_write_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.remove_write(id);
    }

    #[cfg(unix)]
    pub(super) fn write_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.write_fds()
    }

    #[cfg(unix)]
    pub(super) fn wake_writable_fds(&self, writable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.io_interests.wake_writable(writable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    pub(super) fn take_ready_write_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_interests.take_ready_write(id)
    }

    pub(super) fn wake_reactor(&self) {
        #[cfg(unix)]
        let _ = self.waker.wake();
    }
}
