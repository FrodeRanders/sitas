use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::task::Waker;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;
#[cfg(unix)]
use crate::os::OsWaker;

#[cfg(unix)]
use super::io_interest::InterestSet;
use super::task::{Task, set_current_task_waiting_for};
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
    pub(super) queue: VecDeque<Arc<Task>>,
    pub(super) tasks: Vec<Weak<Task>>,
    pub(super) timers: Vec<TimerEntry>,
    #[cfg(unix)]
    pub(super) read_interests: InterestSet,
    #[cfg(unix)]
    pub(super) write_interests: InterestSet,
    #[cfg(target_os = "linux")]
    io_uring: Option<IoUringDispatcherSnapshot>,
    accepting: bool,
    spawner_count: usize,
    task_count: usize,
    total_spawned_tasks: u64,
    total_completed_tasks: u64,
    total_task_polls: u64,
    ready_poll_budget_exhaustions: u64,
    total_driver_events: u64,
    #[cfg(unix)]
    total_readiness_events: u64,
    #[cfg(unix)]
    total_readable_events: u64,
    #[cfg(unix)]
    total_writable_events: u64,
    #[cfg(target_os = "linux")]
    total_completion_events: u64,
    next_task_id: usize,
    next_timer_id: usize,
}

#[derive(Debug)]
pub(super) struct TimerEntry {
    id: usize,
    deadline: Instant,
    waker: Waker,
}

impl Scheduler {
    pub(super) fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                queue: VecDeque::new(),
                tasks: Vec::new(),
                timers: Vec::new(),
                #[cfg(unix)]
                read_interests: InterestSet::new(),
                #[cfg(unix)]
                write_interests: InterestSet::new(),
                #[cfg(target_os = "linux")]
                io_uring: None,
                accepting: true,
                spawner_count: 1,
                task_count: 0,
                total_spawned_tasks: 0,
                total_completed_tasks: 0,
                total_task_polls: 0,
                ready_poll_budget_exhaustions: 0,
                total_driver_events: 0,
                #[cfg(unix)]
                total_readiness_events: 0,
                #[cfg(unix)]
                total_readable_events: 0,
                #[cfg(unix)]
                total_writable_events: 0,
                #[cfg(target_os = "linux")]
                total_completion_events: 0,
                next_task_id: 0,
                next_timer_id: 0,
            }),
            #[cfg(unix)]
            waker,
        }
    }

    pub(super) fn allocate_task_id(&self) -> TaskId {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        let id = state.next_task_id;
        state.next_task_id = state.next_task_id.wrapping_add(1);
        TaskId(id)
    }

    pub(super) fn add_spawner(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.spawner_count += 1;
    }

    pub(super) fn remove_spawner(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.spawner_count = state.spawner_count.saturating_sub(1);
            state.spawner_count == 0
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
            if !state.accepting {
                task.clear_queued();
                return Err(SpawnError);
            }
            state.task_count += 1;
            state.total_spawned_tasks += 1;
            state.tasks.push(Arc::downgrade(&task));
            state.queue.push_back(task);
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
            if !state.accepting {
                task.clear_queued();
                return Err(SpawnError);
            }
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    pub(super) fn next_task(&self) -> Option<Arc<Task>> {
        self.state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .pop_front()
    }

    pub(super) fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
    }

    pub(super) fn has_ready_tasks(&self) -> bool {
        !self
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .is_empty()
    }

    pub(super) fn close(&self) {
        let tasks = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.accepting = false;
            state.task_count = 0;
            state.queue.clear();
            state.timers.clear();
            #[cfg(unix)]
            {
                state.read_interests.clear();
                state.write_interests.clear();
            }

            state
                .tasks
                .drain(..)
                .filter_map(|task| task.upgrade())
                .collect::<Vec<_>>()
        };

        for task in tasks {
            task.drop_future();
        }

        self.wake_reactor();
    }

    pub(super) fn snapshot(&self) -> ExecutorSnapshot {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let accepting = state.accepting;
        let spawner_count = state.spawner_count;
        let task_count = state.task_count;
        let ready_queue_len = state.queue.len();
        let timer_count = state.timers.len();
        let total_spawned_tasks = state.total_spawned_tasks;
        let total_completed_tasks = state.total_completed_tasks;
        let total_task_polls = state.total_task_polls;
        let ready_poll_budget_exhaustions = state.ready_poll_budget_exhaustions;
        let total_driver_events = state.total_driver_events;
        #[cfg(unix)]
        let total_readiness_events = state.total_readiness_events;
        #[cfg(unix)]
        let total_readable_events = state.total_readable_events;
        #[cfg(unix)]
        let total_writable_events = state.total_writable_events;
        #[cfg(target_os = "linux")]
        let total_completion_events = state.total_completion_events;
        #[cfg(unix)]
        let read_interest_count = state.read_interests.len();
        #[cfg(unix)]
        let write_interest_count = state.write_interests.len();
        #[cfg(target_os = "linux")]
        let io_uring = state.io_uring;
        let tasks = state.tasks.clone();
        drop(state);

        let mut tasks = tasks
            .into_iter()
            .filter_map(|task| task.upgrade())
            .map(|task| task.snapshot())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.id);

        ExecutorSnapshot {
            accepting,
            spawner_count,
            task_count,
            ready_queue_len,
            timer_count,
            #[cfg(unix)]
            read_interest_count,
            #[cfg(unix)]
            write_interest_count,
            #[cfg(target_os = "linux")]
            io_uring,
            ready_poll_budget: READY_POLL_BUDGET,
            total_spawned_tasks,
            total_completed_tasks,
            total_task_polls,
            ready_poll_budget_exhaustions,
            total_driver_events,
            #[cfg(unix)]
            total_readiness_events,
            #[cfg(unix)]
            total_readable_events,
            #[cfg(unix)]
            total_writable_events,
            #[cfg(target_os = "linux")]
            total_completion_events,
            tasks,
        }
    }

    pub(super) fn finish_task(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.task_count = state.task_count.saturating_sub(1);
            state.total_completed_tasks += 1;
            state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    pub(super) fn record_ready_poll_batch(&self, polled: usize, exhausted_budget: bool) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.total_task_polls += polled as u64;
        if exhausted_budget {
            state.ready_poll_budget_exhaustions += 1;
        }
    }

    #[cfg(unix)]
    pub(super) fn record_readiness_driver_event(&self, readable: bool, writable: bool) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.total_driver_events += 1;
        state.total_readiness_events += 1;
        if readable {
            state.total_readable_events += 1;
        }
        if writable {
            state.total_writable_events += 1;
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_completion_driver_event(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.total_driver_events += 1;
        state.total_completion_events += 1;
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
            let previous_next = next_timer_deadline(&state.timers);

            match state.timers.iter_mut().find(|timer| timer.id == id) {
                Some(timer) => {
                    timer.deadline = deadline;
                    timer.waker = waker;
                }
                None => state.timers.push(TimerEntry {
                    id,
                    deadline,
                    waker,
                }),
            }

            previous_next.is_none_or(|previous| deadline < previous)
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    pub(super) fn remove_timer(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.retain(|timer| timer.id != id);
    }

    pub(super) fn wake_expired_timers(&self) {
        let expired = {
            let now = Instant::now();
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let mut expired = Vec::new();
            let mut pending = Vec::with_capacity(state.timers.len());

            for timer in state.timers.drain(..) {
                if timer.deadline <= now {
                    expired.push(timer.waker);
                } else {
                    pending.push(timer);
                }
            }

            state.timers = pending;
            expired
        };

        for waker in expired {
            waker.wake();
        }
    }

    pub(super) fn time_until_next_timer(&self) -> Option<Duration> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let deadline = next_timer_deadline(&state.timers)?;
        Some(deadline.saturating_duration_since(Instant::now()))
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

fn next_timer_deadline(timers: &[TimerEntry]) -> Option<Instant> {
    timers.iter().map(|timer| timer.deadline).min()
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
