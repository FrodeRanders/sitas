use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use super::current::{enter_scheduler, enter_task};
use super::scheduler::Scheduler;
use super::task_state::TaskState;
use super::{BoxFuture, PanicHandler, TaskId, TaskSnapshot, TaskWait};

pub(super) struct Task {
    id: TaskId,
    name: Option<String>,
    created_at: Instant,
    state: Mutex<TaskState>,
    scheduler: Arc<Scheduler>,
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
            state: Mutex::new(TaskState::new(future, panic_handler)),
            scheduler,
        }
    }

    pub(super) fn poll(self: Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut context = Context::from_waker(&waker);
        let poll_started_at = Instant::now();
        let mut future = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            let Some(future) = state.take_future_for_poll(poll_started_at) else {
                return;
            };
            future
        };

        let current_scheduler = enter_scheduler(Arc::clone(&self.scheduler));
        let current_task = enter_task(Arc::clone(&self));

        let poll_result =
            panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        drop(current_task);
        drop(current_scheduler);

        let poll_finished_at = Instant::now();
        let poll_duration = poll_finished_at.saturating_duration_since(poll_started_at);

        match poll_result {
            Ok(Poll::Ready(())) => {
                let mut state = self.state.lock().expect("task state mutex poisoned");
                state.finish_ready(poll_duration, poll_finished_at);
                drop(state);
                self.scheduler.finish_task();
            }
            Ok(Poll::Pending) => {
                let cancelled = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.finish_pending(future, poll_duration, poll_finished_at)
                };

                if cancelled {
                    self.scheduler.finish_task();
                    self.scheduler.wake_reactor();
                }
            }
            Err(payload) => {
                let panic_handler = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.finish_panicked(poll_duration, poll_finished_at)
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
        state.mark_queued(Instant::now())
    }

    pub(super) fn cancel(&self) -> bool {
        let should_finish = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            let Some(should_finish) = state.cancel() else {
                return false;
            };
            should_finish
        };

        if should_finish {
            self.scheduler.finish_task();
        }
        self.scheduler.wake_reactor();
        true
    }

    pub(super) fn drop_future(&self) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        state.drop_future();
    }

    pub(super) fn clear_queued(&self) {
        self.state
            .lock()
            .expect("task state mutex poisoned")
            .clear_queued();
    }

    pub(super) fn set_waiting_for(&self, waiting_for: TaskWait) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        state.set_waiting_for(waiting_for);
    }

    pub(super) fn snapshot(&self) -> TaskSnapshot {
        let state = self.state.lock().expect("task state mutex poisoned");
        state.snapshot(self.id, self.name.clone(), self.created_at)
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
