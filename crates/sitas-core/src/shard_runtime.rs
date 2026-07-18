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
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};

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
    Raw(RawJoinHandle),
    #[cfg(feature = "std")]
    Std(std::thread::JoinHandle<T>),
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

    /// Wraps a raw (foreign-runtime) join handle. Always available so that
    /// `no_std` backends such as `sitas-charlotte` keep compiling when a
    /// std-enabled `sitas-core` is unified into the same build graph.
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
            JoinHandleInner::Raw(handle) => handle.join(),
            JoinHandleInner::_Result(_) => unreachable!(),
        }
    }

    pub fn is_finished(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "std")]
            JoinHandleInner::Std(handle) => handle.is_finished(),
            JoinHandleInner::Raw(handle) => handle.is_finished(),
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
    let shared = Arc::new(ChannelShared {
        queue: RingBuffer::bounded(capacity),
        recv_waker: spin::Mutex::new(None),
        closed: AtomicBool::new(false),
    });
    Ok((
        ShardSender {
            shared: Arc::clone(&shared),
        },
        ShardReceiver { shared },
    ))
}

/// State shared by both channel endpoints: the message ring, the receiving
/// task's registered waker (taken and invoked on send/close so the receiver's
/// executor re-polls it), and the closed flag.
struct ChannelShared<M> {
    queue: RingBuffer<M>,
    recv_waker: spin::Mutex<Option<Waker>>,
    closed: AtomicBool,
}

impl<M> ChannelShared<M> {
    fn wake_receiver(&self) {
        let waker = self.recv_waker.lock().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A cloneable sender for a typed inter-shard channel.
pub struct ShardSender<M> {
    shared: Arc<ChannelShared<M>>,
}

impl<M> fmt::Debug for ShardSender<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSender").finish_non_exhaustive()
    }
}

impl<M> Clone for ShardSender<M> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<M> ShardSender<M> {
    pub fn try_send(&self, msg: M) -> Result<(), M> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(msg);
        }
        self.shared.queue.try_push(msg)?;
        // Re-poll a receiver task parked in `recv().await`.
        self.shared.wake_receiver();
        Ok(())
    }

    /// Close the channel. A receiver awaiting [`ShardReceiver::recv`] first
    /// drains any queued messages and then resolves to `None`, so closing is
    /// the shutdown signal for a shard's message loop.
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.wake_receiver();
    }
}

/// A single-consumer receiver for a typed inter-shard channel.
pub struct ShardReceiver<M> {
    shared: Arc<ChannelShared<M>>,
}

impl<M> fmt::Debug for ShardReceiver<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardReceiver").finish_non_exhaustive()
    }
}

impl<M> ShardReceiver<M> {
    pub fn try_recv(&mut self) -> Option<M> {
        self.shared.queue.pop()
    }

    /// Await the next message. Resolves to `None` once the channel is closed
    /// and drained. While pending, the task's waker is registered with the
    /// channel; `try_send`/`close` invoke it, which re-queues the task on its
    /// shard executor and releases the shard's blocked reactor wait.
    pub fn recv(&mut self) -> Recv<'_, M> {
        Recv { receiver: self }
    }
}

/// Future returned by [`ShardReceiver::recv`].
pub struct Recv<'a, M> {
    receiver: &'a mut ShardReceiver<M>,
}

impl<M> Future for Recv<'_, M> {
    type Output = Option<M>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<M>> {
        let shared = &self.get_mut().receiver.shared;
        if let Some(msg) = shared.queue.pop() {
            return Poll::Ready(Some(msg));
        }
        // Drain before reporting closure so queued messages are not lost.
        if shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }
        *shared.recv_waker.lock() = Some(cx.waker().clone());
        // Re-check after registering: a send or close racing the
        // registration may have missed the waker.
        if let Some(msg) = shared.queue.pop() {
            return Poll::Ready(Some(msg));
        }
        if shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

/// The OS thread-spawning interface the sharded executor requires.
pub trait ShardRuntime: Send + Sync {
    type JoinHandle<T: Send>: Send;

    /// The per-shard reactor this runtime provides (see
    /// [`shard_reactor`](ShardRuntime::shard_reactor)).
    type Reactor: crate::reactor_backend::ReactorBackend + Send + 'static;

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

    /// Construct the reactor a shard's executor blocks on. Each shard owns
    /// one reactor (one wait point, §7 of the co-designed architecture); on
    /// CharlotteOS this is the completion-queue reactor for that shard's LP.
    fn shard_reactor(&self, shard_id: ShardId) -> Self::Reactor;
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
