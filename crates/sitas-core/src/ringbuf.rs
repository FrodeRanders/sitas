//! Bounded owned-value queue for the portable runtime.
//!
//! Both ends may be called concurrently. A short runtime-internal spin lock
//! serializes queue bookkeeping; application service state is never stored in
//! this lock. The original SPSC ring exposed shared safe methods without
//! enforcing its producer/consumer restriction. Keeping the same small API
//! over safe storage also guarantees that queued values are dropped normally.

use alloc::collections::VecDeque;
use spin::Mutex;

pub struct RingBuffer<T> {
    queue: Mutex<VecDeque<T>>,
    limit: usize,
}

impl<T> RingBuffer<T> {
    /// Allocate `capacity` ring slots. One slot remains reserved, preserving
    /// the original ring's usable capacity of `capacity - 1`.
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity >= 2, "ring capacity must be at least two");
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity - 1)),
            limit: capacity - 1,
        }
    }

    /// Legacy spelling for a bounded 256-slot ring.
    #[deprecated(note = "this queue is bounded; use RingBuffer::bounded explicitly")]
    pub fn unbounded() -> Self {
        Self::bounded(256)
    }

    pub fn try_push(&self, item: T) -> Result<(), T> {
        let mut queue = self.queue.lock();
        if queue.len() == self.limit {
            return Err(item);
        }
        queue.push_back(item);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        self.queue.lock().pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering};

    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn queued_and_rejected_values_are_dropped_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = RingBuffer::bounded(3);
        assert!(queue.try_push(CountDrop(Arc::clone(&drops))).is_ok());
        assert!(queue.try_push(CountDrop(Arc::clone(&drops))).is_ok());
        let rejected = queue.try_push(CountDrop(Arc::clone(&drops))).err().unwrap();
        drop(queue.pop());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(queue);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        drop(rejected);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    #[should_panic(expected = "ring capacity must be at least two")]
    fn zero_capacity_is_rejected() {
        let _ = RingBuffer::<u8>::bounded(0);
    }

    #[cfg(feature = "std")]
    #[test]
    fn concurrent_producers_and_consumers_transfer_each_value_once() {
        let queue = Arc::new(RingBuffer::bounded(33));
        let received = Arc::new(std::sync::Mutex::new(alloc::vec::Vec::new()));
        std::thread::scope(|scope| {
            for producer in 0..4 {
                let queue = Arc::clone(&queue);
                scope.spawn(move || {
                    for value in producer * 100..(producer + 1) * 100 {
                        while queue.try_push(value).is_err() {
                            std::thread::yield_now();
                        }
                    }
                });
            }
            for _ in 0..4 {
                let queue = Arc::clone(&queue);
                let received = Arc::clone(&received);
                scope.spawn(move || {
                    for _ in 0..100 {
                        loop {
                            if let Some(value) = queue.pop() {
                                received.lock().unwrap().push(value);
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                });
            }
        });
        let mut received = received.lock().unwrap();
        received.sort_unstable();
        assert_eq!(*received, (0..400).collect::<alloc::vec::Vec<_>>());
        assert!(queue.is_empty());
    }
}
