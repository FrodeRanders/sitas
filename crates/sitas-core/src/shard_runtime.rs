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
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::placement::ShardPlacement;
use crate::ringbuf::RingBuffer;
use crate::shard::ShardId;

/// A handle to a spawned shard thread, equivalent to `std::thread::JoinHandle`.
/// The handle can be waited on (joining the thread) and checked for completion.
pub struct ShardJoinHandle<T> {
    inner: JoinHandleInner<T>,
    _result: PhantomData<fn() -> T>,
}

enum JoinHandleInner<T> {
    #[cfg(not(feature = "std"))]
    Raw(RawJoinHandle),
    #[cfg(feature = "std")]
    Std(std::thread::JoinHandle<T>),
    #[cfg(not(feature = "std"))]
    _Result(PhantomData<fn() -> T>),
}

impl<T> ShardJoinHandle<T> {
    #[cfg(feature = "std")]
    pub fn from_std(handle: std::thread::JoinHandle<T>) -> Self {
        Self {
            inner: JoinHandleInner::Std(handle),
            _result: PhantomData,
        }
    }

    #[cfg(not(feature = "std"))]
    pub fn from_raw(handle: RawJoinHandle) -> Self {
        Self {
            inner: JoinHandleInner::Raw(handle),
            _result: PhantomData,
        }
    }

    /// Blocks until the spawned thread exits and returns its result.
    pub fn join(self) -> core::result::Result<T, Box<dyn core::error::Error + Send + Sync>> {
        match self.inner {
            #[cfg(feature = "std")]
            JoinHandleInner::Std(handle) => handle.join().map_err(|_| {
                Box::new(crate::io::ErrorKind::Other) as Box<dyn core::error::Error + Send + Sync>
            }),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::Raw(handle) => handle.join(),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::_Result(_) => unreachable!(),
        }
    }

    pub fn is_finished(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "std")]
            JoinHandleInner::Std(handle) => handle.is_finished(),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::Raw(handle) => handle.is_finished(),
            #[cfg(not(feature = "std"))]
            JoinHandleInner::_Result(_) => unreachable!(),
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
    pub fn is_finished(&self) -> bool {
        false
    }
    pub fn join<T>(self) -> core::result::Result<T, Box<dyn core::error::Error + Send + Sync>> {
        Err(Box::new(crate::io::ErrorKind::Other) as Box<dyn core::error::Error + Send + Sync>)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardChannelError {
    InvalidCapacity,
}

impl fmt::Display for ShardChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "channel capacity must be at least two"),
        }
    }
}

impl core::error::Error for ShardChannelError {}

/// The result type for `ShardRuntime::channel`.
pub type ShardChannelResult<M> =
    core::result::Result<(ShardSender<M>, ShardReceiver<M>), ShardChannelError>;

pub fn channel<M: Send + 'static>(capacity: usize) -> ShardChannelResult<M> {
    if capacity < 2 {
        return Err(ShardChannelError::InvalidCapacity);
    }
    let queue = Arc::new(RingBuffer::bounded(capacity));
    Ok((
        ShardSender {
            queue: Arc::clone(&queue),
        },
        ShardReceiver { queue },
    ))
}

/// A cloneable sender for a typed inter-shard channel.
pub struct ShardSender<M> {
    queue: Arc<RingBuffer<M>>,
}

impl<M> fmt::Debug for ShardSender<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSender").finish_non_exhaustive()
    }
}

impl<M> Clone for ShardSender<M> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<M> ShardSender<M> {
    pub fn try_send(&self, msg: M) -> Result<(), M> {
        self.queue.try_push(msg)
    }
}

/// A single-consumer receiver for a typed inter-shard channel.
pub struct ShardReceiver<M> {
    queue: Arc<RingBuffer<M>>,
}

impl<M> fmt::Debug for ShardReceiver<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardReceiver").finish_non_exhaustive()
    }
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
        placement: ShardPlacement,
        entry: Box<dyn FnOnce() -> T + Send>,
    ) -> Self::JoinHandle<T>;

    /// Create a typed bounded channel between shards.
    fn channel<M: Send + 'static>(&self, capacity: usize) -> ShardChannelResult<M>;

    /// Block the calling thread for at least `duration`.
    fn sleep(&self, duration: core::time::Duration);

    /// Obtain a shared [`ShardParker`] for this runtime. A shard waiting for
    /// a message or a reply parks through it — sleeping with no CPU cost —
    /// instead of busy-spinning, and a peer shard releases it with
    /// [`ShardParker::unpark`].
    fn parker(&self) -> Arc<dyn ShardParker>;
}

/// A shareable handle to park the calling shard and to release parked shards.
///
/// This is the seam that lets sitas services block instead of busy-spinning
/// while waiting for an inter-shard message or reply. `park` sleeps the caller
/// (with no CPU cost) until a peer calls `unpark`, until an optional deadline
/// elapses, or spuriously; `unpark` releases parked shards so they re-check
/// their state. Callers must always re-check their condition after `park`
/// returns, because spurious and coalesced wakeups are permitted.
///
/// On a co-designed asynchronous OS the implementation is the kernel's
/// completion-queue wait/wake (`CQ_WAIT`/`CQ_WAKE`); a hosted backend would
/// use a futex or condition variable.
pub trait ShardParker: Send + Sync {
    /// Park the calling shard until [`unpark`](ShardParker::unpark) is called,
    /// `timeout` elapses (if given), or a spurious wakeup occurs.
    fn park(&self, timeout: Option<core::time::Duration>);

    /// Release shards parked on this runtime so they re-check their state.
    /// Waking more shards than strictly necessary is permitted (they simply
    /// re-check and re-park).
    fn unpark(&self);
}
