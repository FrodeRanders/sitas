//! Standard-library shard runtime primitives.
//!
//! This module provides the reusable kernel for the shared-nothing service
//! model: bounded shard mailboxes, one-shot reply handles, shard set
//! startup, non-blocking enqueue, and shutdown support. It does not know
//! about concrete service state or key-value commands.
//!
//! The reply primitive is waker-aware so blocking code can wait
//! synchronously while executor tasks can await replies through
//! [`ReplyFuture`] without external runtime dependencies.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use core::task::{Context, Poll, Waker};
use std::thread;
use core::time::{Duration, Instant};

use crate::{ShardError, ShardId};

/// Default number of pending commands each shard mailbox can hold.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Runtime shard configuration shared by sharded services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    /// Number of shard threads to start.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
}

impl ShardConfig {
    /// Creates a config with the default bounded mailbox capacity.
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
        }
    }

    /// Sets the bounded mailbox capacity per shard.
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    pub(crate) fn validate(self) -> Result<Self, ShardError> {
        if self.shard_count == 0 {
            return Err(ShardError::InvalidShardCount);
        }
        if self.mailbox_capacity == 0 {
            return Err(ShardError::InvalidMailboxCapacity);
        }

        Ok(self)
    }
}

/// Owned snapshot of a running shard set's runtime shape.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    /// Number of shard threads in the service.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
    /// Whether the service handle has begun shutdown.
    pub stopped: bool,
}

/// A one-shot reply handle for an accepted shard command.
///
/// This is not a future and does not require an async runtime. Calling
/// [`Reply::wait`] blocks the current thread until the owning shard sends the
/// response or the reply channel is disconnected. [`Reply::try_wait`] polls the
/// reply channel once without blocking.
#[must_use]
pub struct Reply<T> {
    shared: Arc<ReplyShared<T>>,
}

impl<T> Reply<T> {
    fn new(shared: Arc<ReplyShared<T>>) -> Self {
        Self { shared }
    }

    /// Waits for the shard reply and returns the owned response value.
    pub fn wait(self) -> Result<T, ShardError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("reply state mutex poisoned");

        loop {
            if let Some(value) = state.value.take() {
                return Ok(value);
            }
            if !state.sender_alive {
                return Err(ShardError::ReplyFailed);
            }

            state = self
                .shared
                .ready
                .wait(state)
                .expect("reply state mutex poisoned");
        }
    }

    /// Waits for the shard reply until `timeout` expires.
    pub fn wait_timeout(self, timeout: Duration) -> Result<T, ShardError> {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .shared
            .state
            .lock()
            .expect("reply state mutex poisoned");

        loop {
            if let Some(value) = state.value.take() {
                return Ok(value);
            }
            if !state.sender_alive {
                return Err(ShardError::ReplyFailed);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(ShardError::ReplyTimeout);
            }

            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout_result) = self
                .shared
                .ready
                .wait_timeout(state, remaining)
                .expect("reply state mutex poisoned");
            state = next_state;

            if timeout_result.timed_out() && state.value.is_none() {
                return Err(ShardError::ReplyTimeout);
            }
        }
    }

    /// Polls the reply channel once without blocking.
    ///
    /// Returns `Ok(None)` when the shard has not replied yet. Returns
    /// `Ok(Some(value))` when the reply is ready.
    pub fn try_wait(&self) -> Result<Option<T>, ShardError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("reply state mutex poisoned");

        if let Some(value) = state.value.take() {
            Ok(Some(value))
        } else if state.sender_alive {
            Ok(None)
        } else {
            Err(ShardError::ReplyFailed)
        }
    }
}

impl<T: Send + 'static> Reply<T> {
    /// Converts this reply handle into a future.
    ///
    /// The future registers its task waker directly in this reply's one-shot
    /// state and is woken when the shard sends the response.
    pub fn wait_async(self) -> ReplyFuture<T> {
        ReplyFuture {
            shared: self.shared,
        }
    }
}

impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reply").finish_non_exhaustive()
    }
}

/// The sending side of a one-shot reply channel.
///
/// Created together with a [`Reply`] by the channel constructor. The sender
/// is moved into the shard thread's command and used to send the response
/// back to the waiting caller.
pub struct ReplySender<T> {
    shared: Arc<ReplyShared<T>>,
}

impl<T> ReplySender<T> {
    pub(crate) fn send(self, value: T) -> Result<(), T> {
        let waker = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("reply state mutex poisoned");
            if state.value.is_some() {
                return Err(value);
            }

            state.value = Some(value);
            state.sender_alive = false;
            state.waker.take()
        };

        self.shared.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }

        Ok(())
    }
}

impl<T> Drop for ReplySender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("reply state mutex poisoned");
            if !state.sender_alive {
                return;
            }

            state.sender_alive = false;
            state.waker.take()
        };

        self.shared.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub(crate) fn reply_channel<T>() -> (ReplySender<T>, Reply<T>) {
    let shared = Arc::new(ReplyShared {
        state: Mutex::new(ReplyState {
            value: None,
            sender_alive: true,
            waker: None,
        }),
        ready: Condvar::new(),
    });

    (
        ReplySender {
            shared: Arc::clone(&shared),
        },
        Reply::new(shared),
    )
}

struct ReplyShared<T> {
    state: Mutex<ReplyState<T>>,
    ready: Condvar,
}

struct ReplyState<T> {
    value: Option<T>,
    sender_alive: bool,
    waker: Option<Waker>,
}

/// Future returned by [`Reply::wait_async`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct ReplyFuture<T> {
    shared: Arc<ReplyShared<T>>,
}

impl<T> fmt::Debug for ReplyFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyFuture").finish_non_exhaustive()
    }
}

impl<T> Future for ReplyFuture<T> {
    type Output = Result<T, ShardError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("reply state mutex poisoned");

        if let Some(value) = state.value.take() {
            Poll::Ready(Ok(value))
        } else if state.sender_alive {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Err(ShardError::ReplyFailed))
        }
    }
}

pub(crate) struct ShardMailbox<C> {
    sender: mpsc::SyncSender<C>,
}

impl<C> ShardMailbox<C> {
    pub(crate) fn new(sender: mpsc::SyncSender<C>) -> Self {
        Self { sender }
    }

    /// Construct a mailbox that wraps a `ShardSender` (ringbuf-based,
    /// used on the runtime / no\_std path).
    pub(crate) fn from_shard_sender(sender: ShardSender<C>) -> Self
    where
        C: Send + 'static,
    {
        // Store the raw boxed sender; we call it via the trait method below.
        // Since ShardMailbox is generic over C and we need type erasure, we
        // box the sender and cast via an unsafe pointer transmute. This is
        // safe because the type C is known to the caller and the boxing
        // preserves the vtable.
        let raw: *mut ShardSender<C> = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(sender));
        // We reuse the `sender` field as a raw pointer. The mpsc::SyncSender
        // has the same size as a pointer.
        let ptr = raw as usize as i64;
        Self { sender: unsafe { core::mem::transmute::<i64, mpsc::SyncSender<C>>(ptr) } }
    }

    fn as_shard_sender_ptr(&self) -> *mut ShardSender<C> {
        let val: i64 = unsafe { core::mem::transmute_copy(&self.sender) };
        val as usize as *mut ShardSender<C>
    }

    pub(crate) fn send_runtime(&self, command: C) -> Result<(), ShardError>
    where
        C: Send + 'static,
    {
        let ptr = self.as_shard_sender_ptr();
        if ptr.is_null() {
            return Err(ShardError::SendFailed);
        }
        unsafe { &*ptr }.try_send(command).map_err(|_| ShardError::SendFailed)
    }

    pub(crate) fn try_send_runtime(&self, command: C) -> Result<(), ShardError>
    where
        C: Send + 'static,
    {
        self.send_runtime(command)
    }

    pub(crate) fn send(&self, command: C) -> Result<(), ShardError> {
        self.sender
            .send(command)
            .map_err(|_| ShardError::SendFailed)
    }

    pub(crate) fn send_stopped(&self, command: C) -> Result<(), ShardError> {
        self.sender
            .send(command)
            .map_err(|_| ShardError::ShardStopped)
    }

    pub(crate) fn try_send(&self, command: C) -> Result<(), ShardError> {
        self.sender.try_send(command).map_err(map_try_send_error)
    }

    pub(crate) fn request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        let (reply, receiver) = reply_channel();
        self.send(build(reply))?;
        Ok(receiver)
    }

    pub(crate) fn try_request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        let (reply, receiver) = reply_channel();
        self.try_send(build(reply))?;
        Ok(receiver)
    }

    pub(crate) fn request_stopped<T, F>(&self, build: F) -> Result<T, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        let (reply, receiver) = reply_channel();
        self.send_stopped(build(reply))?;
        receiver.wait()
    }
}

/// A ringbuf-based command receiver for the runtime / no\_std path.
/// Mirrors the API of `mpsc::Receiver` enough for `run_shard` to work.
pub(crate) struct ShardReceiver<C> {
    queue: alloc::sync::Arc<crate::ringbuf::RingBuffer<C>>,
}

impl<C> ShardReceiver<C> {
    pub(crate) fn recv(&self) -> Result<C, ShardError> {
        loop {
            if let Some(cmd) = self.queue.pop() {
                return Ok(cmd);
            }
            core::hint::spin_loop();
        }
    }
}

/// Runtime-path mailbox using `ShardSender` from shard_runtime, wrapping it
/// for the existing `ShardMailbox<C>` API.
pub(crate) fn bounded_mailbox_runtime<C: Send + 'static>(capacity: usize) -> (ShardSender<C>, ShardReceiver<C>) {
    use crate::shard_runtime::ShardSender;
    let q = alloc::sync::Arc::new(crate::ringbuf::RingBuffer::bounded(capacity));
    let sender = ShardSender { queue: alloc::sync::Arc::clone(&q) };
    let receiver = ShardReceiver { queue: q };
    (sender, receiver)
}
    let (sender, receiver) = mpsc::sync_channel(capacity);
    (ShardMailbox::new(sender), receiver)
}

pub(crate) trait HasShardId {
    fn shard_id(&self) -> ShardId;
}

pub(crate) struct ShardSet<H> {
    handles: Vec<H>,
    /// Join handles for std threads. `None` when the runtime spawn path was
    /// used (threads are self-managed).
    joins: Option<Vec<thread::JoinHandle<()>>>,
    mailbox_capacity: usize,
}

impl<H: 'static> ShardSet<H> {
    /// Starts a shard set using the given [`ShardRuntime`] for thread spawning
    /// (the CharlotteOS / no\_std path). Threads are spawned via
    /// `runtime.spawn_shard()` instead of `std::thread::spawn()`.
    pub(crate) fn start_with_runtime<C, BuildHandle, RunShard, R>(
        shard_count: usize,
        _mailbox_capacity: usize,
        mut build_handle: BuildHandle,
        run_shard: RunShard,
        runtime: &R,
    ) -> Self
    where
        C: Send + 'static,
        BuildHandle: FnMut(usize, ShardSender<C>) -> H,
        RunShard: Fn(ShardId, ShardReceiver<C>) + Copy + Send + 'static,
        R: crate::shard_runtime::ShardRuntime + ?Sized,
    {
        use crate::shard_runtime::ShardSender;
        let mut handles = Vec::with_capacity(shard_count);
        for shard_idx in 0..shard_count {
            let (mailbox, receiver) = bounded_mailbox_runtime(256);
            let _join = runtime.spawn_shard(
                ShardId(shard_idx),
                crate::placement::Placement::Sequential,
                alloc::boxed::Box::new(move || {
                    run_shard(ShardId(shard_idx), receiver);
                }),
            );
            handles.push(build_handle(shard_idx, mailbox));
        }
        Self { handles, joins: None, mailbox_capacity: 256 }
    }
}

impl<H> ShardSet<H> {
    pub(crate) fn start<C, BuildHandle, RunShard>(
        shard_count: usize,
        mailbox_capacity: usize,
        mut build_handle: BuildHandle,
        run_shard: RunShard,
    ) -> Self
    where
        C: Send + 'static,
        BuildHandle: FnMut(usize, ShardMailbox<C>) -> H,
        RunShard: Fn(ShardId, mpsc::Receiver<C>) + Copy + Send + 'static,
    {
        let mut handles = Vec::with_capacity(shard_count);
        let mut joins = Vec::with_capacity(shard_count);

        for shard_idx in 0..shard_count {
            let (mailbox, receiver) = bounded_mailbox(mailbox_capacity);
            let join = thread::spawn(move || run_shard(ShardId(shard_idx), receiver));

            handles.push(build_handle(shard_idx, mailbox));
            joins.push(join);
        }

        Self {
            handles,
            joins: Some(joins),
            mailbox_capacity,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.handles.len()
    }

    pub(crate) fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    pub(crate) fn snapshot(&self, stopped: bool) -> RuntimeSnapshot {
        RuntimeSnapshot {
            shard_count: self.len(),
            mailbox_capacity: self.mailbox_capacity,
            stopped,
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&H> {
        self.handles.get(index)
    }

    pub(crate) fn request_all<T, F>(&self, request: F) -> Result<Vec<T>, ShardError>
    where
        F: FnMut(&H) -> Result<T, ShardError>,
    {
        self.handles.iter().map(request).collect()
    }

    pub(crate) fn join_drained(&mut self) -> Result<(), ShardError> {
        match self.joins.take() {
            Some(joins) => join_all(joins),
            None => Ok(()), // Runtime path: threads are self-managed.
        }
    }

    pub(crate) fn stop_and_join<F>(&mut self, mut stop_one: F) -> Result<(), ShardError>
    where
        F: FnMut(&H) -> Result<(), ShardError>,
    {
        let mut stop_result = Ok(());

        for handle in &self.handles {
            if let Err(error) = stop_one(handle) {
                stop_result = Err(error);
            }
        }

        let join_result = self.join_drained();
        stop_result?;
        join_result
    }
}

impl<H: HasShardId> ShardSet<H> {
    pub(crate) fn get_by_id(&self, shard_id: ShardId) -> Result<&H, ShardError> {
        let handle = self
            .get(shard_id.0)
            .ok_or(ShardError::InvalidShardId(shard_id.0))?;
        debug_assert_eq!(handle.shard_id(), shard_id);
        Ok(handle)
    }

    pub(crate) fn request_all_with_ids<T, F>(
        &self,
        mut request: F,
    ) -> Result<Vec<(ShardId, T)>, ShardError>
    where
        F: FnMut(&H) -> Result<T, ShardError>,
    {
        self.handles
            .iter()
            .map(|handle| Ok((handle.shard_id(), request(handle)?)))
            .collect()
    }
}

pub(crate) fn join_all(joins: Vec<thread::JoinHandle<()>>) -> Result<(), ShardError> {
    let mut join_result = Ok(());

    for join in joins {
        if join.join().is_err() {
            join_result = Err(ShardError::ThreadJoinFailed);
        }
    }

    join_result
}

fn map_try_send_error<C>(error: mpsc::TrySendError<C>) -> ShardError {
    match error {
        mpsc::TrySendError::Full(_) => ShardError::MailboxFull,
        mpsc::TrySendError::Disconnected(_) => ShardError::SendFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_MAILBOX_CAPACITY, ShardConfig, reply_channel};
    use crate::ShardError;
    use crate::executor::block_on;
    use std::thread;
    use core::time::Duration;

    #[test]
    fn shard_config_uses_default_mailbox_capacity() {
        let config = ShardConfig::new(3);

        assert_eq!(config.shard_count, 3);
        assert_eq!(config.mailbox_capacity, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn shard_config_rejects_zero_shards() {
        assert_eq!(
            ShardConfig::new(0).validate().unwrap_err(),
            ShardError::InvalidShardCount
        );
    }

    #[test]
    fn shard_config_rejects_zero_mailbox_capacity() {
        assert_eq!(
            ShardConfig::new(1)
                .with_mailbox_capacity(0)
                .validate()
                .unwrap_err(),
            ShardError::InvalidMailboxCapacity
        );
    }

    #[test]
    fn reply_future_wakes_after_sender_replies() {
        let (sender, reply) = reply_channel();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            sender.send(17).unwrap();
        });

        assert_eq!(block_on(async move { reply.wait_async().await }), Ok(17));
    }
}
