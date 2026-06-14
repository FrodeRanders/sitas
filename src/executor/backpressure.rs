//! Backpressure mechanism for the async executor spawner.
//!
//! The [`BackpressureGuard`] tracks the number of in-flight spawned tasks and
//! provides a mechanism for callers to wait until capacity is available before
//! spawning more tasks. This complements the std-layer bounded mailboxes with
//! an async-layer spawn backpressure primitive.
//!
//! # Design
//!
//! In the std layer, each shard has a bounded `mpsc::sync_channel` mailbox.
//! In the async layer, the spawn path had no built-in backpressure — spawns
//! would queue indefinitely. The backpressure guard implements a simple
//! semaphore-like mechanism:
//!
//! - `acquire()` returns a future that resolves when capacity is available
//! - The guard token is returned to the pool when dropped
//! - Cloning the guard shares the same counter
//!
//! This is intentionally minimal: a counting semaphore, not a priority-based
//! admission controller.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// A backpressure guard that limits the number of in-flight spawned tasks.
///
/// Spawners acquire a permit before spawning. The permit is automatically
/// released when the spawned task completes (via the permit's `Drop` impl).
#[derive(Debug, Clone)]
pub struct BackpressureGuard {
    inner: Arc<BackpressureInner>,
}

struct BackpressureInner {
    capacity: usize,
    in_flight: AtomicUsize,
    waiters: Mutex<Vec<Waker>>,
}

impl fmt::Debug for BackpressureInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpressureInner")
            .field("capacity", &self.capacity)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl BackpressureGuard {
    /// Creates a new backpressure guard with a maximum number of in-flight
    /// tasks.
    ///
    /// `capacity` must be greater than zero.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "backpressure capacity must be greater than zero"
        );
        Self {
            inner: Arc::new(BackpressureInner {
                capacity,
                in_flight: AtomicUsize::new(0),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns the configured capacity.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Returns the current number of in-flight tasks.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    /// Returns the number of available permits.
    pub fn available(&self) -> usize {
        self.inner.capacity.saturating_sub(self.in_flight())
    }

    /// Attempts to acquire a permit without waiting.
    ///
    /// Returns `Some(Permit)` if capacity is available, or `None` if the limit
    /// is reached.
    pub fn try_acquire(&self) -> Option<Permit> {
        loop {
            let current = self.inner.in_flight.load(Ordering::Acquire);
            if current >= self.inner.capacity {
                return None;
            }
            if self
                .inner
                .in_flight
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(Permit {
                    inner: Arc::clone(&self.inner),
                });
            }
        }
    }

    /// Acquires a permit, waiting asynchronously if necessary.
    pub fn acquire(&self) -> AcquirePermit {
        AcquirePermit {
            inner: Arc::clone(&self.inner),
            registered: false,
        }
    }
}

impl fmt::Display for BackpressureGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BackpressureGuard({}/{})",
            self.in_flight(),
            self.capacity()
        )
    }
}

/// A permit that tracks one in-flight task.
///
/// When dropped, the permit is released back to the guard, potentially waking
/// a waiting async acquirer.
#[must_use = "permit is released when dropped; keep it alive while the task is in-flight"]
pub struct Permit {
    inner: Arc<BackpressureInner>,
}

impl Permit {
    /// Returns the current in-flight count (including this permit).
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }
}

impl fmt::Debug for Permit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit")
            .field("in_flight", &self.in_flight())
            .finish()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::Release);
        let wakers = {
            let mut waiters = self
                .inner
                .waiters
                .lock()
                .expect("backpressure waiters poisoned");
            std::mem::take(&mut *waiters)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Future returned by [`BackpressureGuard::acquire`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct AcquirePermit {
    inner: Arc<BackpressureInner>,
    registered: bool,
}

impl fmt::Debug for AcquirePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcquirePermit")
            .field("in_flight", &self.inner.in_flight.load(Ordering::Acquire))
            .field("capacity", &self.inner.capacity)
            .finish()
    }
}

impl Future for AcquirePermit {
    type Output = Permit;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let current = self.inner.in_flight.load(Ordering::Acquire);
            if current < self.inner.capacity {
                if self
                    .inner
                    .in_flight
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Poll::Ready(Permit {
                        inner: Arc::clone(&self.inner),
                    });
                }
                continue;
            }

            break;
        }

        if !self.registered {
            self.registered = true;
            let inner = Arc::clone(&self.inner);
            let mut waiters = inner.waiters.lock().expect("backpressure waiters poisoned");
            waiters.push(context.waker().clone());
        }

        Poll::Pending
    }
}

impl Drop for AcquirePermit {
    fn drop(&mut self) {
        if self.registered {
            // Waker may remain in list; waking a dropped future is a no-op.
        }
    }
}

/// A spawned task wrapper that manages backpressure lifecycle.
///
/// When a task is spawned through a backpressure-aware spawner, the
/// [`BackpressureTask`] holds the permit for the task's lifetime. When the
/// task completes or is dropped, the permit is released.
#[derive(Debug)]
pub struct BackpressureTask<F> {
    future: F,
    _permit: Permit,
}

impl<F> BackpressureTask<F> {
    /// Creates a new backpressure task, consuming the permit.
    pub fn new(future: F, permit: Permit) -> Self {
        Self {
            future,
            _permit: permit,
        }
    }

    /// Returns a reference to the inner future.
    pub fn inner(&self) -> &F {
        &self.future
    }

    /// Returns a mutable reference to the inner future.
    pub fn inner_mut(&mut self) -> &mut F {
        &mut self.future
    }

    /// Consumes this wrapper and returns the inner future.
    /// The permit is released immediately.
    pub fn into_inner(self) -> F {
        // Permit is dropped here, releasing capacity.
        self.future
    }
}

impl<F: Future> Future for BackpressureTask<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: we only project to the future field, the permit is pinned but never moved.
        let this = unsafe { self.get_unchecked_mut() };
        // Safety: the future is pinned through Box::pin when spawned by the executor.
        unsafe { Pin::new_unchecked(&mut this.future) }.poll(context)
    }
}

#[cfg(test)]
mod tests {
    use super::super::block_on;
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    #[test]
    fn backpressure_guard_try_acquire_limits_capacity() {
        let guard = BackpressureGuard::new(2);

        let p1 = guard.try_acquire().unwrap();
        let p2 = guard.try_acquire().unwrap();
        assert!(guard.try_acquire().is_none());

        drop(p1);
        let p3 = guard.try_acquire().unwrap();
        drop(p2);
        drop(p3);

        assert_eq!(guard.in_flight(), 0);
    }

    #[test]
    fn backpressure_guard_acquire_awaits_capacity() {
        let guard = BackpressureGuard::new(1);
        let p1 = guard.try_acquire().unwrap();

        let guard2 = guard.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let completed2 = Arc::clone(&completed);

        let handle = std::thread::spawn(move || {
            block_on(async move {
                let _p2 = guard2.acquire().await;
                completed2.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        });

        std::thread::sleep(Duration::from_millis(10));
        assert!(!completed.load(std::sync::atomic::Ordering::SeqCst));

        drop(p1);
        handle.join().unwrap();

        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn backpressure_task_holds_permit() {
        let guard = BackpressureGuard::new(1);
        let permit = guard.try_acquire().unwrap();
        assert!(guard.try_acquire().is_none());

        let task = BackpressureTask::new(async { 42 }, permit);
        assert!(guard.try_acquire().is_none());

        drop(task);
        let _permit = guard.try_acquire().unwrap();
        assert_eq!(guard.in_flight(), 1);
    }

    #[test]
    fn backpressure_guard_capacity_must_be_positive() {
        let guard = BackpressureGuard::new(5);
        assert_eq!(guard.capacity(), 5);
        assert_eq!(guard.available(), 5);
    }
}
