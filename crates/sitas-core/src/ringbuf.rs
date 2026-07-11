//! Minimal lock-free bounded SPSC ring buffer for `sitas-core`.
//!
//! The original `sitas` crate uses `concurrent_queue::ConcurrentQueue` (which
//! depends on `std` for condition-variable signalling). For the `no_std` core
//! we provide a simple atomic-based ring buffer that the executor's reactor
//! (and the `ShardMailbox`) can use. It is single-producer, single-consumer:
//! the kernel writes completions (CQ ring), the reactor reads them.
//!
//! This is NOT a general-purpose MPMC queue — it is just enough for the
//! executor's wake path and the CharlotteOS reactor.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicUsize, Ordering};

pub struct RingBuffer<T> {
    buffer: UnsafeCell<alloc::boxed::Box<[UnsafeCell<core::mem::MaybeUninit<T>>]>>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn bounded(capacity: usize) -> Self {
        let mut v = alloc::vec::Vec::with_capacity(capacity);
        v.resize_with(capacity, || UnsafeCell::new(core::mem::MaybeUninit::uninit()));
        Self {
            buffer: UnsafeCell::new(v.into_boxed_slice()),
            capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    pub fn unbounded() -> Self {
        Self::bounded(256) // "unbounded" approximation
    }

    pub fn try_push(&self, item: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let next = (head + 1) % self.capacity;
        let tail = self.tail.load(Ordering::Acquire);
        if next == tail {
            return Err(item);
        }
        let buf = unsafe { &mut *self.buffer.get() };
        unsafe { buf[head].get().write(core::mem::MaybeUninit::new(item)) };
        self.head.store(next, Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail == head {
            return None;
        }
        let buf = unsafe { &mut *self.buffer.get() };
        let item = unsafe { buf[tail].get().read().assume_init() };
        self.tail.store((tail + 1) % self.capacity, Ordering::Release);
        Some(item)
    }

    pub fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Relaxed);
        if h >= t { h - t } else { h + self.capacity - t }
    }
}
