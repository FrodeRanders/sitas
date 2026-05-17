use std::cell::RefCell;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use super::current::enter_scheduler;
use super::scheduler::Scheduler;
use super::{BoxFuture, PanicHandler, TaskId, TaskSnapshot, TaskStatus, TaskWait};

thread_local! {
    static CURRENT_TASK: RefCell<Option<Arc<Task>>> = const { RefCell::new(None) };
}

pub(super) struct Task {
    id: TaskId,
    name: Option<String>,
    created_at: Instant,
    state: Mutex<TaskState>,
    scheduler: Arc<Scheduler>,
}

struct TaskState {
    future: Option<BoxFuture>,
    panic_handler: Option<PanicHandler>,
    queued: bool,
    polling: bool,
    cancel_requested: bool,
    completed: bool,
    poll_count: u64,
    total_poll_time: Duration,
    waiting_for: Option<TaskWait>,
    last_scheduled_at: Option<Instant>,
    last_poll_started_at: Option<Instant>,
    last_poll_finished_at: Option<Instant>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").finish_non_exhaustive()
    }
}

impl Task {
    pub(super) fn new(
        id: TaskId,
        name: Option<String>,
        future: BoxFuture,
        scheduler: Arc<Scheduler>,
        panic_handler: Option<PanicHandler>,
    ) -> Self {
        Self {
            id,
            name,
            created_at: Instant::now(),
            state: Mutex::new(TaskState {
                future: Some(future),
                panic_handler,
                queued: false,
                polling: false,
                cancel_requested: false,
                completed: false,
                poll_count: 0,
                total_poll_time: Duration::ZERO,
                waiting_for: None,
                last_scheduled_at: None,
                last_poll_started_at: None,
                last_poll_finished_at: None,
            }),
            scheduler,
        }
    }

    pub(super) fn poll(self: Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut context = Context::from_waker(&waker);
        let poll_started_at = Instant::now();
        let mut future = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            state.queued = false;

            let Some(future) = state.future.take() else {
                return;
            };
            state.polling = true;
            state.waiting_for = None;
            state.last_poll_started_at = Some(poll_started_at);
            state.poll_count += 1;
            future
        };

        let current_scheduler = enter_scheduler(Arc::clone(&self.scheduler));
        CURRENT_TASK.with(|current| {
            *current.borrow_mut() = Some(Arc::clone(&self));
        });

        let poll_result =
            panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        CURRENT_TASK.with(|current| {
            *current.borrow_mut() = None;
        });
        drop(current_scheduler);

        let poll_finished_at = Instant::now();
        let poll_duration = poll_finished_at.saturating_duration_since(poll_started_at);

        match poll_result {
            Ok(Poll::Ready(())) => {
                let mut state = self.state.lock().expect("task state mutex poisoned");
                state.polling = false;
                state.completed = true;
                state.waiting_for = None;
                state.total_poll_time += poll_duration;
                state.last_poll_finished_at = Some(poll_finished_at);
                drop(state);
                self.scheduler.finish_task();
            }
            Ok(Poll::Pending) => {
                let cancelled = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    state.total_poll_time += poll_duration;
                    state.last_poll_finished_at = Some(poll_finished_at);
                    if state.waiting_for.is_none() {
                        state.waiting_for = Some(TaskWait::Unknown);
                    }
                    if state.cancel_requested {
                        state.completed = true;
                        state.waiting_for = None;
                        true
                    } else {
                        state.future = Some(future);
                        false
                    }
                };

                if cancelled {
                    self.scheduler.finish_task();
                    self.scheduler.wake_reactor();
                }
            }
            Err(payload) => {
                let panic_handler = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    state.future = None;
                    state.completed = true;
                    state.waiting_for = None;
                    state.total_poll_time += poll_duration;
                    state.last_poll_finished_at = Some(poll_finished_at);
                    state.panic_handler.take()
                };

                if let Some(panic_handler) = panic_handler {
                    panic_handler(payload);
                }
                self.scheduler.finish_task();
            }
        }
    }

    pub(super) fn mark_queued(&self) -> bool {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        if state.queued || (state.future.is_none() && !state.polling) {
            return false;
        }

        state.queued = true;
        state.waiting_for = None;
        state.last_scheduled_at = Some(Instant::now());
        true
    }

    pub(super) fn cancel(&self) -> bool {
        let should_finish = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            if state.future.is_none() && !state.polling {
                return false;
            }

            state.cancel_requested = true;

            if state.polling {
                false
            } else {
                state.future = None;
                state.queued = false;
                state.completed = true;
                state.waiting_for = None;
                true
            }
        };

        if should_finish {
            self.scheduler.finish_task();
        }
        self.scheduler.wake_reactor();
        true
    }

    pub(super) fn drop_future(&self) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        state.cancel_requested = true;
        if !state.polling {
            state.future = None;
            state.queued = false;
            state.completed = true;
            state.waiting_for = None;
        }
    }

    pub(super) fn clear_queued(&self) {
        self.state.lock().expect("task state mutex poisoned").queued = false;
    }

    pub(super) fn set_waiting_for(&self, waiting_for: TaskWait) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        if state.polling && !state.completed {
            state.waiting_for = Some(waiting_for);
        }
    }

    pub(super) fn snapshot(&self) -> TaskSnapshot {
        let state = self.state.lock().expect("task state mutex poisoned");
        let status = if state.completed && state.cancel_requested {
            TaskStatus::Cancelled
        } else if state.completed {
            TaskStatus::Completed
        } else if state.polling {
            TaskStatus::Polling
        } else if state.queued {
            TaskStatus::Queued
        } else {
            TaskStatus::Waiting
        };

        TaskSnapshot {
            id: self.id,
            name: self.name.clone(),
            status,
            waiting_for: state.waiting_for,
            poll_count: state.poll_count,
            total_poll_time: state.total_poll_time,
            created_at: self.created_at,
            last_scheduled_at: state.last_scheduled_at,
            last_poll_started_at: state.last_poll_started_at,
            last_poll_finished_at: state.last_poll_finished_at,
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let scheduler = Arc::clone(&self.scheduler);
        let _ = scheduler.schedule_existing(self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.scheduler.schedule_existing(self.clone());
    }
}

pub(super) fn set_current_task_waiting_for(waiting_for: TaskWait) {
    CURRENT_TASK.with(|current| {
        if let Some(task) = current.borrow().as_ref() {
            task.set_waiting_for(waiting_for);
        }
    });
}
