use alloc::string::String;
use alloc::vec::Vec;
use alloc::boxed::Box;
//! Streaming reply channels for sharded services.
//!
//! A [`StreamReply<T>`] bridges between a shard producing multiple values and a
//! consumer that receives them. Unlike a one-shot [`Reply<T>`](crate::runtime::Reply),
//! a stream reply delivers a sequence of owned values followed by a terminal
//! completion signal. Blocking consumers use [`StreamReply::recv`],
//! [`StreamReply::recv_batch`], [`StreamReply::collect`], or
//! [`StreamReply::fold`]. Async consumers call [`StreamReply::next_batch`]
//! which returns a [`StreamBatch`] future for waker-integrated polling.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use core::task::{Context, Poll, Waker};

/// Error returned when a streaming reply is dropped before completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamError {
    /// The stream sender was dropped before all values were consumed.
    SenderDropped,
    /// The stream receiver was dropped while the sender was still producing.
    ReceiverDropped,
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::SenderDropped => write!(f, "stream sender dropped unexpectedly"),
            StreamError::ReceiverDropped => write!(f, "stream receiver dropped early"),
        }
    }
}

impl core::error::Error for StreamError {}

/// The sending side of a streaming reply channel.
///
/// Shard threads push owned values and then drop the sender to signal
/// completion. When an async consumer is waiting via [`StreamBatch`], the
/// sender wakes the registered waker.
pub struct StreamSender<T> {
    shared: Arc<StreamShared<T>>,
}

impl<T> StreamSender<T> {
    /// Pushes one value to the stream receiver.
    ///
    /// Returns `Err(StreamError::ReceiverDropped)` if the consumer has already
    /// stopped receiving.
    pub fn send(&self, value: T) -> Result<(), StreamError> {
        let waker = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("stream state mutex poisoned");

            if state.done {
                return Err(StreamError::ReceiverDropped);
            }

            state.values.push(value);
            state.waker.take()
        };

        self.shared.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for StreamSender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        let waker = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("stream state mutex poisoned");
            state.waker.take()
        };
        self.shared.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Clone for StreamSender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// The receiving side of a streaming reply channel.
///
/// Callers drain values from the stream until the sender signals completion.
#[must_use = "stream replies do nothing unless consumed"]
pub struct StreamReply<T> {
    shared: Arc<StreamShared<T>>,
    index: usize,
}

struct StreamShared<T> {
    state: Mutex<StreamState<T>>,
    ready: Condvar,
    sender_count: AtomicUsize,
}

struct StreamState<T> {
    values: Vec<T>,
    done: bool,
    waker: Option<Waker>,
}

/// Create a streaming reply channel pair.
pub fn stream_channel<T>() -> (StreamSender<T>, StreamReply<T>) {
    let shared = Arc::new(StreamShared {
        state: Mutex::new(StreamState {
            values: Vec::new(),
            done: false,
            waker: None,
        }),
        ready: Condvar::new(),
        sender_count: AtomicUsize::new(1),
    });

    (
        StreamSender {
            shared: Arc::clone(&shared),
        },
        StreamReply { shared, index: 0 },
    )
}

impl<T> StreamReply<T> {
    /// Waits until at least one value is available, then returns all available
    /// values up to `max_items`.
    ///
    /// Returns an empty vector when the sender is dropped and no values remain.
    pub fn recv_batch(&mut self, max_items: usize) -> Result<Vec<T>, StreamError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("stream state mutex poisoned");

        loop {
            if self.index < state.values.len() {
                let available = state.values.len() - self.index;
                let count = available.min(max_items);
                let batch: Vec<T> = state.values.drain(self.index..self.index + count).collect();
                return Ok(batch);
            }

            if self.shared.sender_count.load(Ordering::Acquire) == 0 {
                state.done = true;
                return Ok(Vec::new());
            }

            state = self
                .shared
                .ready
                .wait(state)
                .expect("stream state mutex poisoned");
        }
    }

    /// Returns the next single value, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Option<T>, StreamError> {
        let batch = self.recv_batch(1)?;
        Ok(batch.into_iter().next())
    }

    /// Attempts to receive values without blocking.
    ///
    /// Returns `Ok(None)` when no values are available but the sender is still
    /// alive. Returns `Ok(Some(values))` when values are available. Returns
    /// `Err(StreamError::SenderDropped)` when the sender is gone and no values
    /// remain.
    pub fn try_recv(&mut self, max_items: usize) -> Result<Option<Vec<T>>, StreamError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("stream state mutex poisoned");

        if self.index < state.values.len() {
            let available = state.values.len() - self.index;
            let count = available.min(max_items);
            let batch: Vec<T> = state.values.drain(self.index..self.index + count).collect();
            return Ok(Some(batch));
        }

        if self.shared.sender_count.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }

        Ok(None)
    }

    /// Collects all remaining values into a single vector.
    pub fn collect(mut self) -> Result<Vec<T>, StreamError> {
        let mut all = Vec::new();
        loop {
            let batch = self.recv_batch(usize::MAX)?;
            if batch.is_empty() {
                break;
            }
            all.extend(batch);
        }
        Ok(all)
    }

    /// Folds all remaining values into an accumulator.
    pub fn fold<Acc, F>(mut self, mut acc: Acc, mut f: F) -> Result<Acc, StreamError>
    where
        F: FnMut(Acc, T) -> Acc,
    {
        loop {
            let batch = self.recv_batch(usize::MAX)?;
            if batch.is_empty() {
                break;
            }
            for value in batch {
                acc = f(acc, value);
            }
        }
        Ok(acc)
    }

    /// Returns true if all values have been received and the sender is done.
    pub fn is_done(&self) -> bool {
        let state = self
            .shared
            .state
            .lock()
            .expect("stream state mutex poisoned");
        self.shared.sender_count.load(Ordering::Acquire) == 0 && self.index >= state.values.len()
    }

    /// Returns a future that yields the next batch of values.
    ///
    /// This borrows the reply mutably, so it can be called repeatedly in a
    /// loop. Each call polls the shared channel, registers the executor's
    /// waker, and returns when values arrive or the stream is exhausted.
    ///
    /// ```
    /// # use sitas::stream_reply::{stream_channel, StreamBatch};
    /// # use sitas::executor::block_on;
    /// # block_on(async {
    /// let (sender, mut reply) = stream_channel::<i32>();
    /// sender.send(1).unwrap();
    /// sender.send(2).unwrap();
    /// drop(sender);
    ///
    /// let mut values = Vec::new();
    /// while let Ok(Some(batch)) = reply.next_batch().await {
    ///     values.extend(batch);
    /// }
    /// assert_eq!(values, vec![1, 2]);
    /// # });
    /// ```
    pub fn next_batch(&mut self) -> StreamBatch<'_, T> {
        StreamBatch { reply: self }
    }

    /// Consumes this reply and returns a [`StreamFuture`] that owns it.
    ///
    /// `StreamFuture` provides the same `next_batch()` looping pattern plus
    /// convenience async methods like [`StreamFuture::collect`] and
    /// [`StreamFuture::fold`]. This is useful when you want to pass the
    /// stream to another task or function without keeping a separate
    /// `StreamReply` handle alive.
    pub fn into_stream(self) -> StreamFuture<T> {
        StreamFuture { reply: self }
    }
}

impl<T> fmt::Debug for StreamReply<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamReply").finish_non_exhaustive()
    }
}

impl<T> Drop for StreamReply<T> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("stream state mutex poisoned");
        state.done = true;
        self.shared.ready.notify_all();
    }
}

/// Future returned by [`StreamReply::next_batch`].
///
/// Borrows the reply mutably and yields one batch of available values.
/// Returns `Ok(Some(batch))` when values are ready, `Ok(None)` when the
/// stream is exhausted, or `Err(StreamError)` on failure. Integrates with
/// executor wakers so the task sleeps until new values arrive.
///
/// This is a one-shot per call — to receive multiple batches, call
/// `next_batch()` repeatedly.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct StreamBatch<'a, T> {
    reply: &'a mut StreamReply<T>,
}

impl<T> Unpin for StreamBatch<'_, T> {}

impl<T> fmt::Debug for StreamBatch<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamBatch").finish_non_exhaustive()
    }
}

impl<T> Future for StreamBatch<'_, T> {
    type Output = Result<Option<Vec<T>>, StreamError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.reply.try_recv(usize::MAX) {
            Ok(Some(batch)) => Poll::Ready(Ok(Some(batch))),
            Ok(None) => {
                let mut state = this
                    .reply
                    .shared
                    .state
                    .lock()
                    .expect("stream state mutex poisoned");

                if this.reply.shared.sender_count.load(Ordering::Acquire) == 0
                    && this.reply.index >= state.values.len()
                {
                    return Poll::Ready(Ok(None));
                }

                if state.values.len() > this.reply.index {
                    drop(state);
                    return Pin::new(&mut *this).poll(context);
                }

                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

/// Owning multi-yield stream future returned by [`StreamReply::into_stream`].
///
/// `StreamFuture<T>` owns the [`StreamReply<T>`] and provides the same
/// `next_batch()` pattern as the borrowed variant. It also offers async
/// convenience methods like [`StreamFuture::collect`] and
/// [`StreamFuture::fold`] that drain the entire stream.
///
/// ```
/// # use sitas::stream_reply::{stream_channel, StreamFuture};
/// # use sitas::executor::block_on;
/// # block_on(async {
/// let (sender, reply) = stream_channel::<i32>();
/// sender.send(1).unwrap();
/// sender.send(2).unwrap();
/// drop(sender);
///
/// let mut stream = reply.into_stream();
/// let total = stream.fold(0, |acc, v| acc + v).await.unwrap();
/// assert_eq!(total, 3);
/// # });
/// ```
pub struct StreamFuture<T> {
    reply: StreamReply<T>,
}

impl<T> StreamFuture<T> {
    /// Returns a future yielding the next batch from this owned stream.
    pub fn next_batch(&mut self) -> StreamBatch<'_, T> {
        StreamBatch {
            reply: &mut self.reply,
        }
    }

    /// Collects all remaining values in the stream into a single vector.
    pub async fn collect(mut self) -> Result<Vec<T>, StreamError> {
        let mut all = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            all.extend(batch);
        }
        Ok(all)
    }

    /// Folds all remaining values into an accumulator.
    pub async fn fold<Acc, F>(mut self, mut acc: Acc, mut f: F) -> Result<Acc, StreamError>
    where
        F: FnMut(Acc, T) -> Acc,
    {
        while let Some(batch) = self.next_batch().await? {
            for value in batch {
                acc = f(acc, value);
            }
        }
        Ok(acc)
    }
}

impl<T> fmt::Debug for StreamFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFuture").finish_non_exhaustive()
    }
}

/// A shard-local streaming producer.
///
/// When a shard service responds with many values (e.g., scanning a range of
/// keys), it can push values through a [`StreamSender`] instead of collecting
/// them into a single `Vec`. The caller receives values incrementally via
/// [`StreamReply`].
pub struct StreamProducer<T> {
    /// The sending end used by the shard to push values.
    pub sender: StreamSender<T>,
    /// The receiving end returned to the caller.
    pub reply: StreamReply<T>,
}

impl<T> StreamProducer<T> {
    /// Creates a new streaming producer/reply pair.
    pub fn new() -> Self {
        let (sender, reply) = stream_channel();
        Self { sender, reply }
    }
}

impl<T> Default for StreamProducer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for StreamProducer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamProducer").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::block_on;
    use std::thread;
    use core::time::Duration;

    #[test]
    fn stream_reply_collects_all_values() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        drop(sender);

        let result = reply.collect().unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn stream_reply_recv_batch_limits_items() {
        let (sender, mut reply) = stream_channel::<i32>();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        sender.send(4).unwrap();
        drop(sender);

        let batch = reply.recv_batch(2).unwrap();
        assert_eq!(batch, vec![1, 2]);

        let batch = reply.recv_batch(2).unwrap();
        assert_eq!(batch, vec![3, 4]);

        let batch = reply.recv_batch(10).unwrap();
        assert!(batch.is_empty());
    }

    #[test]
    fn stream_reply_recv_returns_none_after_completion() {
        let (sender, mut reply) = stream_channel::<i32>();

        sender.send(42).unwrap();
        drop(sender);

        assert_eq!(reply.recv().unwrap(), Some(42));
        assert_eq!(reply.recv().unwrap(), None);
    }

    #[test]
    fn stream_reply_fold_accumulates_all_values() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        drop(sender);

        let sum = reply.fold(0, |acc, v| acc + v).unwrap();
        assert_eq!(sum, 6);
    }

    #[test]
    fn stream_reply_handles_empty_stream() {
        let (sender, reply) = stream_channel::<i32>();
        drop(sender);

        let result = reply.collect().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn stream_sender_rejects_after_receiver_drop() {
        let (sender, reply) = stream_channel::<i32>();
        drop(reply);

        assert_eq!(sender.send(1), Err(StreamError::ReceiverDropped));
    }

    #[test]
    fn stream_reply_handles_concurrent_producer() {
        let (sender, reply) = stream_channel::<i32>();
        let sender2 = sender.clone();
        let sender_main = sender.clone();

        thread::spawn(move || {
            sender.send(1).unwrap();
            sender.send(2).unwrap();
        });

        thread::spawn(move || {
            sender2.send(3).unwrap();
        });

        thread::sleep(Duration::from_millis(50));
        drop(sender_main);

        let mut result = reply.collect().unwrap();
        result.sort();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn stream_batch_awaits_multiple_batches() {
        let (sender, mut reply) = stream_channel::<i32>();

        sender.send(10).unwrap();
        sender.send(20).unwrap();
        drop(sender);

        let result = block_on(async {
            let mut values = Vec::new();
            while let Ok(Some(batch)) = reply.next_batch().await {
                values.extend(batch);
            }
            values
        });
        assert_eq!(result, vec![10, 20]);
    }

    #[test]
    fn stream_batch_handles_empty_stream() {
        let (_sender, mut reply) = stream_channel::<i32>();
        drop(_sender);

        let result = block_on(async { reply.next_batch().await }).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn stream_batch_waker_receives_values_from_other_thread() {
        let (sender, mut reply) = stream_channel::<i32>();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            sender.send(99).unwrap();
        });

        let result = block_on(async {
            match reply.next_batch().await {
                Ok(Some(batch)) => batch,
                Ok(None) => Vec::new(),
                Err(_) => Vec::new(),
            }
        });
        assert_eq!(result, vec![99]);
    }

    #[test]
    fn stream_future_collect_drains_all_values() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        drop(sender);

        let result = block_on(reply.into_stream().collect()).unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn stream_future_fold_accumulates_values() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(10).unwrap();
        sender.send(20).unwrap();
        sender.send(30).unwrap();
        drop(sender);

        let total = block_on(reply.into_stream().fold(0, |acc, v| acc + v)).unwrap();
        assert_eq!(total, 60);
    }

    #[test]
    fn stream_future_next_batch_loops_like_reply() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(100).unwrap();
        sender.send(200).unwrap();
        drop(sender);

        let result = block_on(async {
            let mut stream = reply.into_stream();
            let mut values = Vec::new();
            while let Ok(Some(batch)) = stream.next_batch().await {
                values.extend(batch);
            }
            values
        });
        assert_eq!(result, vec![100, 200]);
    }

    #[test]
    fn stream_future_handles_empty_stream() {
        let (_sender, reply) = stream_channel::<i32>();
        drop(_sender);

        let result = block_on(reply.into_stream().collect()).unwrap();
        assert!(result.is_empty());
    }
}
