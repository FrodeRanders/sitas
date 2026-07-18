//! Grouped child tasks with cooperative shutdown.
//!
//! [`TaskScope`] owns a set of spawned tasks and a shared stop token.
//! Dropping a scope signals stop and aborts remaining children. Explicit
//! [`shutdown`](TaskScope::shutdown) and
//! [`shutdown_timeout`](TaskScope::shutdown_timeout) support cooperative
//! cancellation with bounded abort.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::time::{Duration, Instant};

use super::{
    JoinError, JoinHandle, SchedulingGroup, SpawnError, Spawner, StopSource, StopToken,
    TimeoutError, stop_pair, timeout,
};

/// Error returned when a task scope cannot shut down cleanly.
pub enum TaskScopeError {
    /// A child task failed while the scope was waiting for shutdown.
    Join(JoinError),
    /// The shutdown deadline elapsed before all child tasks completed.
    TimedOut,
}

impl TaskScopeError {
    /// Returns true if shutdown timed out.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, TaskScopeError::TimedOut)
    }

    /// Returns true if a child task failed while shutting down.
    pub fn is_join_error(&self) -> bool {
        matches!(self, TaskScopeError::Join(_))
    }
}

impl From<JoinError> for TaskScopeError {
    fn from(error: JoinError) -> Self {
        TaskScopeError::Join(error)
    }
}

impl fmt::Debug for TaskScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskScopeError::Join(error) => f.debug_tuple("Join").field(error).finish(),
            TaskScopeError::TimedOut => f.write_str("TimedOut"),
        }
    }
}

impl fmt::Display for TaskScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskScopeError::Join(error) => write!(f, "task scope child failed: {error}"),
            TaskScopeError::TimedOut => write!(f, "task scope shutdown timed out"),
        }
    }
}

impl Error for TaskScopeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TaskScopeError::Join(error) => Some(error),
            TaskScopeError::TimedOut => None,
        }
    }
}

/// Owns a group of spawned tasks and a shared cooperative stop signal.
///
/// Dropping a scope requests stop and aborts any children that are still owned
/// by the scope. Use [`TaskScope::shutdown`] when children should observe the
/// stop token and finish cooperatively.
#[must_use = "task scopes abort their children when dropped"]
pub struct TaskScope {
    spawner: Spawner,
    stop_source: StopSource,
    stop_token: StopToken,
    handles: Vec<JoinHandle<()>>,
}

impl TaskScope {
    /// Creates a new scope that spawns tasks on `spawner`.
    pub fn new(spawner: Spawner) -> Self {
        let (stop_source, stop_token) = stop_pair();

        Self {
            spawner,
            stop_source,
            stop_token,
            handles: Vec::new(),
        }
    }

    /// Returns a clone of the scope's stop token.
    pub fn stop_token(&self) -> StopToken {
        self.stop_token.clone()
    }

    /// Returns true if this scope has already been asked to stop.
    pub fn is_stopped(&self) -> bool {
        self.stop_source.is_stopped()
    }

    /// Requests cooperative stop for tasks in this scope.
    pub fn stop(&self) -> bool {
        self.stop_source.stop()
    }

    /// Spawns a child task owned by this scope.
    pub fn spawn<F>(&mut self, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.push(self.spawner.spawn_with_handle(future)?);
        Ok(())
    }

    /// Spawns a child task into a scheduling group owned by this scope.
    pub fn spawn_in_group<F>(
        &mut self,
        group: &SchedulingGroup,
        future: F,
    ) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles
            .push(self.spawner.spawn_with_handle_in_group(group, future)?);
        Ok(())
    }

    /// Spawns a child task that receives this scope's stop token.
    pub fn spawn_with_stop<F, Fut>(&mut self, make_future: F) -> Result<(), SpawnError>
    where
        F: FnOnce(StopToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn(make_future(self.stop_token()))
    }

    /// Spawns a child task into a scheduling group with this scope's stop token.
    pub fn spawn_with_stop_in_group<F, Fut>(
        &mut self,
        group: &SchedulingGroup,
        make_future: F,
    ) -> Result<(), SpawnError>
    where
        F: FnOnce(StopToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_in_group(group, make_future(self.stop_token()))
    }

    /// Aborts all child tasks still owned by this scope.
    pub fn abort_all(&self) -> usize {
        self.handles.iter().filter(|handle| handle.abort()).count()
    }

    /// Waits for all child tasks to finish.
    pub async fn wait(mut self) -> Result<(), JoinError> {
        for handle in self.handles.drain(..) {
            handle.await?;
        }

        Ok(())
    }

    /// Requests cooperative stop and waits for all child tasks to finish.
    pub async fn shutdown(self) -> Result<(), JoinError> {
        self.stop();
        self.wait().await
    }

    /// Requests cooperative stop and waits up to `duration` for children to
    /// finish before aborting the still-owned tasks.
    pub async fn shutdown_timeout(mut self, duration: Duration) -> Result<(), TaskScopeError> {
        self.stop();
        let deadline = Instant::now() + duration;

        while let Some(mut handle) = self.handles.pop() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                handle.abort();
                self.abort_all();
                return Err(TaskScopeError::TimedOut);
            }

            match timeout(remaining, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(TaskScopeError::Join(error)),
                Err(TimeoutError) => {
                    handle.abort();
                    self.abort_all();
                    return Err(TaskScopeError::TimedOut);
                }
            }
        }

        Ok(())
    }
}

impl fmt::Debug for TaskScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskScope")
            .field("stopped", &self.is_stopped())
            .field("children", &self.handles.len())
            .finish()
    }
}

impl Drop for TaskScope {
    fn drop(&mut self) {
        self.stop();
        self.abort_all();
    }
}
