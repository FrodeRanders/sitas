//! Streaming reply channels for sharded services.
//!
//! A [`StreamReply<T>`] bridges between a shard producing multiple values and a
//! consumer that receives them. Unlike a one-shot [`Reply<T>`](crate::runtime::Reply),
//! a stream reply delivers a sequence of owned values followed by a terminal
//! completion signal. Blocking consumers use `recv`, `recv_batch`, `collect`,
//! or `fold`. Async consumers wrap the reply in a [`StreamFuture`] for
//! waker-integrated polling.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};

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

impl std::error::Error for StreamError {}

/// The sending side of a streaming reply channel.
///
/// Shard threads push owned values and then drop the sender to signal
/// completion. When an async consumer is waiting via [`StreamFuture`], the
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
        let waker = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("stream state mutex poisoned");
            state.sender_alive = false;
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
}

struct StreamState<T> {
    values: Vec<T>,
    sender_alive: bool,
    done: bool,
    waker: Option<Waker>,
}

/// Create a streaming reply channel pair.
pub fn stream_channel<T>() -> (StreamSender<T>, StreamReply<T>) {
    let shared = Arc::new(StreamShared {
        state: Mutex::new(StreamState {
            values: Vec::new(),
            sender_alive: true,
            done: false,
            waker: None,
        }),
        ready: Condvar::new(),
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

            if !state.sender_alive {
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

        if !state.sender_alive {
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
        !state.sender_alive && self.index >= state.values.len()
    }

    /// Converts this blocking stream reply into an awaitable future.
    ///
    /// The returned future integrates with executor wakers: when a value is
    /// available, the waker registered by the future is invoked. The future
    /// yields one batch at a time. When the stream is exhausted, the future
    /// returns `Ok(None)`.
    pub fn into_async(self) -> StreamFuture<T> {
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
        state.sender_alive = false;
        self.shared.ready.notify_all();
    }
}

/// Future wrapper for [`StreamReply`] that integrates with executor wakers.
///
/// Each call to `poll` returns one batch of available values. When the stream
/// is exhausted, the future returns `Poll::Ready(Ok(None))`.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct StreamFuture<T> {
    reply: StreamReply<T>,
}

impl<T> fmt::Debug for StreamFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFuture").finish_non_exhaustive()
    }
}

impl<T> Future for StreamFuture<T> {
    type Output = Result<Option<Vec<T>>, StreamError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.reply.try_recv(usize::MAX) {
            Ok(Some(batch)) => Poll::Ready(Ok(Some(batch))),
            Ok(None) => {
                // No values available, register waker for notification.
                let mut state = self
                    .reply
                    .shared
                    .state
                    .lock()
                    .expect("stream state mutex poisoned");

                if !state.sender_alive && self.reply.index >= state.values.len() {
                    return Poll::Ready(Ok(None));
                }

                if state.values.len() > self.reply.index {
                    drop(state);
                    return self.poll(context);
                }

                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
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
    use std::time::Duration;

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
    fn stream_future_integrates_with_executor_waker() {
        let (sender, reply) = stream_channel::<i32>();

        sender.send(10).unwrap();
        sender.send(20).unwrap();
        drop(sender);

        let result = block_on(async { reply.into_async().await.unwrap().unwrap() });
        assert_eq!(result, vec![10, 20]);
    }

    #[test]
    fn stream_future_handles_empty_stream() {
        let (_sender, reply) = stream_channel::<i32>();
        drop(_sender);

        let result = block_on(reply.into_async()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn stream_future_waker_receives_values_from_other_thread() {
        let (sender, reply) = stream_channel::<i32>();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            sender.send(99).unwrap();
        });

        let result = block_on(async {
            let future = reply.into_async();
            match future.await {
                Ok(Some(batch)) => batch,
                Ok(None) => Vec::new(),
                Err(_) => Vec::new(),
            }
        });
        assert_eq!(result, vec![99]);
    }
}
