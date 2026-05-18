use std::fmt;
use std::sync::{Arc, Mutex};
use std::task::Wake;

use super::scheduler::Scheduler;

pub(super) struct RootWaker {
    ready: Mutex<bool>,
    scheduler: Arc<Scheduler>,
}

impl RootWaker {
    pub(super) fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            ready: Mutex::new(true),
            scheduler,
        }
    }

    pub(super) fn take_ready(&self) -> bool {
        let mut ready = self.ready.lock().expect("root waker mutex poisoned");
        let was_ready = *ready;
        *ready = false;
        was_ready
    }

    pub(super) fn is_ready(&self) -> bool {
        *self.ready.lock().expect("root waker mutex poisoned")
    }

    fn mark_ready(&self) {
        *self.ready.lock().expect("root waker mutex poisoned") = true;
        self.scheduler.wake_reactor();
    }
}

impl fmt::Debug for RootWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootWaker").finish_non_exhaustive()
    }
}

impl Wake for RootWaker {
    fn wake(self: Arc<Self>) {
        self.mark_ready();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark_ready();
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use crate::os::OsReactor;

    use super::*;

    fn root_waker() -> Arc<RootWaker> {
        let reactor = OsReactor::new().expect("failed to create test reactor");
        Arc::new(RootWaker::new(Arc::new(Scheduler::new(reactor.waker()))))
    }

    #[test]
    fn root_waker_starts_ready_and_take_ready_clears_it() {
        let root = root_waker();

        assert!(root.is_ready());
        assert!(root.take_ready());
        assert!(!root.is_ready());
        assert!(!root.take_ready());
    }

    #[test]
    fn wake_by_ref_marks_root_ready() {
        let root = root_waker();
        assert!(root.take_ready());

        let waker = Waker::from(Arc::clone(&root));
        waker.wake_by_ref();

        assert!(root.is_ready());
        assert!(root.take_ready());
    }

    #[test]
    fn wake_marks_root_ready() {
        let root = root_waker();
        assert!(root.take_ready());

        let waker = Waker::from(Arc::clone(&root));
        waker.wake();

        assert!(root.is_ready());
        assert!(root.take_ready());
    }
}
