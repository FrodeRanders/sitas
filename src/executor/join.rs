use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::PanicPayload;
use super::task::Task;

type JoinResult<T> = Result<T, JoinError>;

/// Future returned by [`super::Spawner::spawn_with_handle`].
#[must_use = "join handles do nothing unless polled or awaited"]
pub struct JoinHandle<T> {
    pub(super) shared: Arc<Mutex<JoinState<T>>>,
    pub(super) task: Arc<Task>,
}

pub(super) struct JoinState<T> {
    pub(super) result: Option<JoinResult<T>>,
    pub(super) waker: Option<Waker>,
}

/// Error returned by a [`JoinHandle`] when a spawned task did not produce a
/// value.
pub enum JoinError {
    /// The task was aborted before it completed.
    Cancelled,
    /// The task panicked while it was being polled.
    Panic(PanicPayload),
}

impl JoinError {
    /// Returns true if the task was aborted before completion.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, JoinError::Cancelled)
    }

    /// Returns true if the task panicked while it was being polled.
    pub fn is_panic(&self) -> bool {
        matches!(self, JoinError::Panic(_))
    }

    /// Consumes the error and returns the panic payload if the task panicked.
    pub fn into_panic(self) -> Option<PanicPayload> {
        match self {
            JoinError::Cancelled => None,
            JoinError::Panic(payload) => Some(payload),
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => f.write_str("Cancelled"),
            JoinError::Panic(_) => f.write_str("Panic(..)"),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => write!(f, "task was cancelled"),
            JoinError::Panic(_) => write!(f, "task panicked"),
        }
    }
}

impl Error for JoinError {}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> JoinHandle<T> {
    pub(super) fn new(shared: Arc<Mutex<JoinState<T>>>, task: Arc<Task>) -> Self {
        Self { shared, task }
    }

    /// Aborts the task if it has not completed yet.
    ///
    /// Awaiting this handle after a successful abort returns
    /// [`JoinError::Cancelled`].
    pub fn abort(&self) -> bool {
        if !self.task.cancel() {
            return false;
        }

        complete_join(&self.shared, Err(JoinError::Cancelled));
        true
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = JoinResult<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .shared
            .lock()
            .expect("join handle state mutex poisoned");

        match state.result.take() {
            Some(result) => Poll::Ready(result),
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub(super) fn complete_join<T>(shared: &Arc<Mutex<JoinState<T>>>, result: JoinResult<T>) {
    let waker = {
        let mut state = shared.lock().expect("join handle state mutex poisoned");
        if state.result.is_some() {
            None
        } else {
            state.result = Some(result);
            state.waker.take()
        }
    };

    if let Some(waker) = waker {
        waker.wake();
    }
}
