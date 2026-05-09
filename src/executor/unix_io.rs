#![cfg(unix)]

use std::future::Future;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{Scheduler, current_scheduler};

/// Returns a future that completes when `fd` is readable.
pub fn readable(fd: RawFd) -> Readable {
    Readable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Returns a future that completes when `fd` is writable.
pub fn writable(fd: RawFd) -> Writable {
    Writable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Future returned by [`readable`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Readable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

impl Future for Readable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_read_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_read_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_read_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for Readable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_read_interest(interest_id);
        }
    }
}

/// Future returned by [`writable`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Writable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

impl Future for Writable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_write_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_write_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_write_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for Writable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_write_interest(interest_id);
        }
    }
}
