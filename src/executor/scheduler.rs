use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;
#[cfg(unix)]
use crate::os::OsWaker;

use super::counters::SchedulerCounters;
#[cfg(unix)]
use super::io_interest::InterestSet;
use super::task::{Task, set_current_task_waiting_for};
use super::task_set::SchedulerTaskSet;
use super::timer::TimerSet;
use super::{ExecutorSnapshot, READY_POLL_BUDGET, SpawnError, TaskId, TaskWait};

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
}

#[derive(Debug)]
pub(super) struct Scheduler {
    pub(super) state: Mutex<SchedulerState>,
    #[cfg(unix)]
    waker: OsWaker,
}

#[derive(Debug)]
pub(super) struct SchedulerState {
    tasks: SchedulerTaskSet,
    pub(super) timers: TimerSet,
    #[cfg(unix)]
    pub(super) read_interests: InterestSet,
    #[cfg(unix)]
    pub(super) write_interests: InterestSet,
    #[cfg(target_os = "linux")]
    io_uring: Option<IoUringDispatcherSnapshot>,
    counters: SchedulerCounters,
    next_timer_id: usize,
}

impl Scheduler {
    pub(super) fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                tasks: SchedulerTaskSet::new(),
                timers: TimerSet::new(),
                #[cfg(unix)]
                read_interests: InterestSet::new(),
                #[cfg(unix)]
                write_interests: InterestSet::new(),
                #[cfg(target_os = "linux")]
                io_uring: None,
                counters: SchedulerCounters::default(),
                next_timer_id: 0,
            }),
            #[cfg(unix)]
            waker,
        }
    }

    pub(super) fn allocate_task_id(&self) -> TaskId {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.tasks.allocate_task_id()
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
            {
                state.read_interests.clear();
                state.write_interests.clear();
            }

            state.tasks.close()
        };

        for task in tasks {
            task.drop_future();
        }

        self.wake_reactor();
    }

    pub(super) fn snapshot(&self) -> ExecutorSnapshot {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let task_set = state.tasks.snapshot();
        let timer_count = state.timers.len();
        let counters = state.counters;
        #[cfg(unix)]
        let read_interest_count = state.read_interests.len();
        #[cfg(unix)]
        let write_interest_count = state.write_interests.len();
        #[cfg(target_os = "linux")]
        let io_uring = state.io_uring;
        let tasks = task_set.tasks;
        drop(state);

        let mut tasks = tasks
            .into_iter()
            .filter_map(|task| task.upgrade())
            .map(|task| task.snapshot())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.id);

        ExecutorSnapshot {
            accepting: task_set.accepting,
            spawner_count: task_set.spawner_count,
            task_count: task_set.task_count,
            ready_queue_len: task_set.ready_queue_len,
            timer_count,
            #[cfg(unix)]
            read_interest_count,
            #[cfg(unix)]
            write_interest_count,
            #[cfg(target_os = "linux")]
            io_uring,
            ready_poll_budget: READY_POLL_BUDGET,
            total_spawned_tasks: counters.total_spawned_tasks,
            total_completed_tasks: counters.total_completed_tasks,
            total_task_polls: counters.total_task_polls,
            ready_poll_budget_exhaustions: counters.ready_poll_budget_exhaustions,
            total_driver_events: counters.total_driver_events,
            #[cfg(unix)]
            total_readiness_events: counters.total_readiness_events,
            #[cfg(unix)]
            total_readable_events: counters.total_readable_events,
            #[cfg(unix)]
            total_writable_events: counters.total_writable_events,
            #[cfg(target_os = "linux")]
            total_completion_events: counters.total_completion_events,
            tasks,
        }
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

    #[cfg(unix)]
    pub(super) fn record_readiness_driver_event(&self, readable: bool, writable: bool) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state
            .counters
            .record_readiness_driver_event(readable, writable);
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_completion_driver_event(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.counters.record_completion_driver_event();
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_io_uring_snapshot(&self, snapshot: Option<IoUringDispatcherSnapshot>) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.io_uring = snapshot;
    }

    pub(super) fn allocate_timer_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        let id = state.next_timer_id;
        state.next_timer_id = state.next_timer_id.wrapping_add(1);
        id
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
        state.read_interests.allocate_id()
    }

    #[cfg(unix)]
    pub(super) fn register_read_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Readable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    pub(super) fn remove_read_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.remove(id);
    }

    #[cfg(unix)]
    pub(super) fn read_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.fds()
    }

    #[cfg(unix)]
    pub(super) fn wake_readable_fds(&self, readable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.wake_ready(readable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    pub(super) fn take_ready_read_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.take_ready(id)
    }

    #[cfg(unix)]
    pub(super) fn allocate_write_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.allocate_id()
    }

    #[cfg(unix)]
    pub(super) fn register_write_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Writable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    pub(super) fn remove_write_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.remove(id);
    }

    #[cfg(unix)]
    pub(super) fn write_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.fds()
    }

    #[cfg(unix)]
    pub(super) fn wake_writable_fds(&self, writable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.wake_ready(writable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    pub(super) fn take_ready_write_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.take_ready(id)
    }

    pub(super) fn wake_reactor(&self) {
        #[cfg(unix)]
        let _ = self.waker.wake();
    }
}

pub(super) fn current_scheduler() -> Arc<Scheduler> {
    CURRENT_SCHEDULER
        .with(|current| current.borrow().as_ref().cloned())
        .expect("executor futures must be polled by sitas::executor::Executor")
}

pub(super) fn set_current_scheduler(scheduler: Option<Arc<Scheduler>>) {
    CURRENT_SCHEDULER.with(|current| {
        *current.borrow_mut() = scheduler;
    });
}
