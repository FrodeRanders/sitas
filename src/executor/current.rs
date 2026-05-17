use std::cell::RefCell;
use std::sync::Arc;

use super::scheduler::Scheduler;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
}

pub(super) struct CurrentSchedulerGuard {
    previous: Option<Arc<Scheduler>>,
}

pub(super) fn current_scheduler() -> Arc<Scheduler> {
    CURRENT_SCHEDULER
        .with(|current| current.borrow().as_ref().cloned())
        .expect("executor futures must be polled by sitas::executor::Executor")
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
