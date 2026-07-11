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

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use core::task::{Context, Poll, Waker};

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
    waiters: Mutex<Vec<(usize, Waker)>>,
    next_wait_id: AtomicUsize,
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
                next_wait_id: AtomicUsize::new(0),
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
            wait_id: self.inner.next_wait_id.fetch_add(1, Ordering::Relaxed),
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
            let taken: Vec<(usize, Waker)> = std::mem::take(&mut *waiters);
            taken
                .into_iter()
                .map(|(_, waker)| waker)
                .collect::<Vec<_>>()
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
    wait_id: usize,
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
            waiters.push((self.wait_id, context.waker().clone()));
        }

        Poll::Pending
    }
}

impl Drop for AcquirePermit {
    fn drop(&mut self) {
        if self.registered {
            let mut waiters = self
                .inner
                .waiters
                .lock()
                .expect("backpressure waiters poisoned");
            waiters.retain(|(id, _)| *id != self.wait_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::block_on;
    use super::*;
    use core::sync::atomic::AtomicBool;
    use core::time::Duration;

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
                completed2.store(true, core::sync::atomic::Ordering::SeqCst);
            });
        });

        std::thread::sleep(Duration::from_millis(10));
        assert!(!completed.load(core::sync::atomic::Ordering::SeqCst));

        drop(p1);
        handle.join().unwrap();

        assert!(completed.load(core::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn spawner_with_backpressure_limits_concurrent_tasks() {
        use super::super::executor_and_spawner;

        let (_executor, spawner) = executor_and_spawner();
        let spawner = spawner.with_backpressure(1);

        // First spawn succeeds, consuming the only permit.
        spawner.spawn(async {}).unwrap();
        // Second spawn fails because backpressure limit is reached.
        assert_eq!(
            spawner.spawn(async {}),
            Err(super::super::SpawnError::Backpressure)
        );
    }

    #[test]
    fn backpressure_guard_capacity_must_be_positive() {
        let guard = BackpressureGuard::new(5);
        assert_eq!(guard.capacity(), 5);
        assert_eq!(guard.available(), 5);
    }
}
