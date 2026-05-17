use std::cell::RefCell;
use std::sync::Arc;

use super::scheduler::Scheduler;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
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
