//! Thread-local executor and task tracking.
//!
//! Two thread-locals store the currently active [`Scheduler`] and [`Task`].
//! Guard types restore the previous state on drop, enabling nested polling
//! (e.g. `block_on` within a task) without losing the outer context. The
//! scheduler must be entered before any task is polled and is used by
//! [`set_current_task_waiting_for`] to record wait interest for snapshots.

use std::cell::RefCell;
use std::sync::Arc;

use super::TaskWait;
use super::scheduler::Scheduler;
use super::task::Task;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
    static CURRENT_TASK: RefCell<Option<Arc<Task>>> = const { RefCell::new(None) };
}

pub(super) struct CurrentSchedulerGuard {
    previous: Option<Arc<Scheduler>>,
}

pub(super) struct CurrentTaskGuard {
    previous: Option<Arc<Task>>,
}

pub(super) fn current_scheduler() -> Arc<Scheduler> {
    CURRENT_SCHEDULER
        .with(|current| current.borrow().as_ref().cloned())
        .expect("executor futures must be polled by sitas_core::executor::Executor")
}

pub(super) fn enter_scheduler(scheduler: Arc<Scheduler>) -> CurrentSchedulerGuard {
    let previous = CURRENT_SCHEDULER.with(|current| current.borrow_mut().replace(scheduler));
    CurrentSchedulerGuard { previous }
}

impl Drop for CurrentSchedulerGuard {
    fn drop(&mut self) {
        CURRENT_SCHEDULER.with(|current| {
            *current.borrow_mut() = self.previous.take();
        });
    }
}

pub(super) fn enter_task(task: Arc<Task>) -> CurrentTaskGuard {
    let previous = CURRENT_TASK.with(|current| current.borrow_mut().replace(task));
    CurrentTaskGuard { previous }
}

impl Drop for CurrentTaskGuard {
    fn drop(&mut self) {
        CURRENT_TASK.with(|current| {
            *current.borrow_mut() = self.previous.take();
        });
    }
}

pub(super) fn set_current_task_waiting_for(waiting_for: TaskWait) {
    CURRENT_TASK.with(|current| {
        if let Some(task) = current.borrow().as_ref() {
            task.set_waiting_for(waiting_for);
        }
    });
}
