//! # ShardRuntime trait — OS-thread abstraction for the sharded executor.
//!
//! The sharded executor spawns one OS thread per shard. On Unix this delegates
//! to `std::thread::spawn`; on `no_std` targets (CharlotteOS) it delegates to
//! the kernel's `spawn_thread` via SVC. This trait is the seam.
//!
//! ## Design
//!
//! Only three operations are needed:
//!
//! - `spawn_shard(shard_id, placement, entry)` — spawn a thread pinned to a
//!   specific core/LP and run the given closure.
//! - `channel<M>(capacity)` — create a typed owned-message channel between
//!   shards (backed by `RingBuffer`).
//! - `sleep(duration)` — block the calling thread for the given duration.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use crate::ringbuf::RingBuffer;
use crate::shard::ShardId;

/// A handle to a spawned shard thread, equivalent to `std::thread::JoinHandle`.
/// The handle can be waited on (joining the thread) and checked for completion.
pub struct ShardJoinHandle<T> {
    inner: JoinHandleInner<T>,
}

enum JoinHandleInner<T> {
    #[cfg(not(feature = "std"))]
    Raw(RawJoinHandle),
    #[cfg(feature = "std")]
    Std(std::thread::JoinHandle<T>),
}

impl<T> ShardJoinHandle<T> {
    #[cfg(feature = "std")]
    pub fn from_std(handle: std::thread::JoinHandle<T>) -> Self {
        Self { inner: JoinHandleInner::Std(handle) }
    }

    #[cfg(not(feature = "std"))]
    pub fn from_raw(handle: RawJoinHandle) -> Self {
        Self { inner: JoinHandleInner::Raw(handle) }
    }

    /// Blocks until the spawned thread exits and returns its result.
    pub fn join(self) -> core::result::Result<T, Box<dyn core::error::Error + Send + Sync>> {
        match self.inner {
            #[cfg(feature = "std")]
            JoinHandleInner::Std(handle) => handle.join().map_err(|e| Box::new(e.to_string().as_str())),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::Raw(handle) => handle.join(),
        }
    }

    pub fn is_finished(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "std")]
            JoinHandleInner::Std(handle) => handle.is_finished(),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::Raw(handle) => handle.is_finished(),
        }
    }
}

impl<T> fmt::Debug for ShardJoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardJoinHandle").finish()
    }
}

/// A raw join handle for no_std targets. On CharlotteOS this represents a
/// thread that was spawned via the kernel's `spawn_thread` syscall; joining
/// is not yet implemented (the kernel's thread lifecycle is cooperative).
#[derive(Debug)]
pub struct RawJoinHandle;

impl RawJoinHandle {
    pub fn is_finished(&self) -> bool { false }
    pub fn join(self) -> core::result::Result<(), Box<dyn core::error::Error + Send + Sync>> {
        Err(Box::new(crate::io::ErrorKind::Other) as Box<dyn core::error::Error + Send + Sync>)
    }
}

/// The result type for `ShardRuntime::channel`.
pub type ShardChannelResult<M> = core::result::Result<(ShardSender<M>, ShardReceiver<M>), ()>;

/// A cloneable sender for a typed inter-shard channel.
#[derive(Debug)]
pub struct ShardSender<M> {
    queue: Arc<RingBuffer<M>>,
}

impl<M> Clone for ShardSender<M> {
    fn clone(&self) -> Self {
        Self { queue: Arc::clone(&self.queue) }
    }
}

impl<M> ShardSender<M> {
    pub fn try_send(&self, msg: M) -> Result<(), M> {
        self.queue.try_push(msg)
    }
}

/// A single-consumer receiver for a typed inter-shard channel.
#[derive(Debug)]
pub struct ShardReceiver<M> {
    queue: Arc<RingBuffer<M>>,
}

impl<M> ShardReceiver<M> {
    pub fn try_recv(&mut self) -> Option<M> {
        self.queue.pop()
    }
}

impl<M> Future for ShardReceiver<M> {
    type Output = Option<M>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.get_mut().try_recv())
    }
}

/// The OS thread-spawning interface the sharded executor requires.
pub trait ShardRuntime: Send + Sync {
    type JoinHandle<T: Send>: Send;

    /// Spawn a new shard worker thread, pinned to the given core/placement.
    fn spawn_shard<T: Send + 'static>(
        &self,
        shard_id: ShardId,
        placement: crate::placement::Placement,
        entry: Box<dyn FnOnce() -> T + Send>,
    ) -> Self::JoinHandle<T>;

    /// Create a typed bounded channel between shards.
    fn channel<M: Send + 'static>(&self, capacity: usize) -> ShardChannelResult<M>;

    /// Block the calling thread for at least `duration`.
    fn sleep(&self, duration: core::time::Duration);
}
