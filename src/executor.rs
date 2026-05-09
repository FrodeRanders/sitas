//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::error::Error;
use std::fmt;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

#[cfg(unix)]
use crate::os::OsReactor;

mod future;
mod scheduler;
mod sync;
mod task;
#[cfg(unix)]
mod tcp;
#[cfg(unix)]
mod unix_io;

pub use future::{
    Race, RaceOutput, Sleep, Timeout, TimeoutError, YieldNow, race, sleep, timeout, yield_now,
};
use scheduler::{Scheduler, set_current_scheduler};
pub use sync::{Notified, Notify, StopSource, StopToken, stop_pair};
use task::Task;
#[cfg(unix)]
pub use tcp::{
    serve_tcp_n, serve_tcp_n_timeout, serve_tcp_until_idle, serve_tcp_until_idle_timeout,
    serve_tcp_until_stopped, serve_tcp_until_stopped_scoped,
    serve_tcp_until_stopped_scoped_timeout, serve_tcp_until_stopped_timeout,
};
#[cfg(unix)]
pub use unix_io::{
    Readable, Writable, accept_async, accept_timeout_async, connect_async, connect_timeout_async,
    copy_async, copy_timeout_async, read_exact_async, read_exact_timeout_async, readable, writable,
    write_all_async, write_all_timeout_async,
};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type PanicPayload = Box<dyn std::any::Any + Send + 'static>;
type PanicHandler = Box<dyn FnOnce(PanicPayload) + Send + 'static>;
type JoinResult<T> = Result<T, JoinError>;

const READY_POLL_BUDGET: usize = 64;

/// Identifier assigned to a task when it is spawned.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub usize);

/// Coarse lifecycle state for a task in an executor snapshot.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// The task is in the ready queue.
    Queued,
    /// The task is currently being polled.
    Polling,
    /// The task is pending and waiting for another wakeup.
    Waiting,
    /// The task completed normally.
    Completed,
    /// The task was cancelled before completing.
    Cancelled,
}

/// What a pending task last registered interest in.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWait {
    /// The task yielded or is waiting on an opaque waker.
    Unknown,
    /// The task is waiting for an executor timer.
    Timer {
        /// The instant at which the timer becomes ready.
        deadline: Instant,
    },
    /// The task is waiting for a file descriptor to become readable.
    #[cfg(unix)]
    Readable {
        /// File descriptor registered for readability.
        fd: RawFd,
    },
    /// The task is waiting for a file descriptor to become writable.
    #[cfg(unix)]
    Writable {
        /// File descriptor registered for writability.
        fd: RawFd,
    },
}

/// Owned point-in-time summary of one task.
#[must_use]
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    /// Executor-local task identifier.
    pub id: TaskId,
    /// Optional human-readable task name supplied by the spawner.
    pub name: Option<String>,
    /// Current coarse task lifecycle state.
    pub status: TaskStatus,
    /// Last wait interest registered by this task, if known.
    pub waiting_for: Option<TaskWait>,
    /// Number of times the task future has been polled.
    pub poll_count: u64,
    /// Total wall-clock time spent polling this task.
    pub total_poll_time: Duration,
    /// When this task was created.
    pub created_at: Instant,
    /// When this task was last placed on the ready queue.
    pub last_scheduled_at: Option<Instant>,
    /// When this task's most recent poll started.
    pub last_poll_started_at: Option<Instant>,
    /// When this task's most recent poll finished.
    pub last_poll_finished_at: Option<Instant>,
}

/// Owned point-in-time summary of one executor.
#[must_use]
#[derive(Debug, Clone)]
pub struct ExecutorSnapshot {
    /// Whether this executor still accepts new tasks.
    pub accepting: bool,
    /// Number of live spawner handles.
    pub spawner_count: usize,
    /// Number of tasks the scheduler still considers unfinished.
    pub task_count: usize,
    /// Number of tasks currently queued for polling.
    pub ready_queue_len: usize,
    /// Number of registered timers.
    pub timer_count: usize,
    /// Number of registered read-readiness interests.
    #[cfg(unix)]
    pub read_interest_count: usize,
    /// Number of registered write-readiness interests.
    #[cfg(unix)]
    pub write_interest_count: usize,
    /// Owned snapshots for tasks that are still externally observable.
    pub tasks: Vec<TaskSnapshot>,
}

/// Weak observer handle for an executor.
///
/// Unlike [`Spawner`], this handle does not keep the executor alive and does
/// not count as a live spawner. It is intended for monitoring code that should
/// observe runtime state without affecting shutdown.
#[derive(Debug, Clone)]
pub struct ExecutorObserver {
    scheduler: Weak<Scheduler>,
}

impl ExecutorObserver {
    /// Returns an executor snapshot if the executor is still alive.
    pub fn snapshot(&self) -> Option<ExecutorSnapshot> {
        self.scheduler
            .upgrade()
            .map(|scheduler| scheduler.snapshot())
    }
}

/// Error returned when a task cannot be submitted to an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpawnError;

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "executor is not accepting tasks")
    }
}

impl Error for SpawnError {}

/// Handle used to submit futures to an [`Executor`].
#[derive(Debug)]
pub struct Spawner {
    scheduler: Arc<Scheduler>,
}

impl Clone for Spawner {
    fn clone(&self) -> Self {
        self.scheduler.add_spawner();

        Self {
            scheduler: Arc::clone(&self.scheduler),
        }
    }
}

impl Drop for Spawner {
    fn drop(&mut self) {
        self.scheduler.remove_spawner();
    }
}

impl Spawner {
    /// Spawns a future onto the executor's ready queue.
    pub fn spawn<F>(&self, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_name(None, future)
    }

    /// Spawns a named future onto the executor's ready queue.
    pub fn spawn_named<F>(&self, name: impl Into<String>, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_name(Some(name.into()), future)
    }

    fn spawn_with_name<F>(&self, name: Option<String>, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_panic_handler(name, future, None)
            .map(|_| ())
    }

    /// Spawns a future and returns a handle that can await its output.
    pub fn spawn_with_handle<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with_handle_and_name(None, future)
    }

    /// Spawns a named future and returns a handle that can await its output.
    pub fn spawn_with_handle_named<F>(
        &self,
        name: impl Into<String>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with_handle_and_name(Some(name.into()), future)
    }

    fn spawn_with_handle_and_name<F>(
        &self,
        name: Option<String>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let shared = Arc::new(Mutex::new(JoinState {
            result: None,
            waker: None,
        }));
        let shared_for_task = Arc::clone(&shared);
        let shared_for_panic = Arc::clone(&shared);

        let task = self.spawn_with_panic_handler(
            name,
            async move {
                let output = future.await;
                complete_join(&shared_for_task, Ok(output));
            },
            Some(Box::new(move |payload| {
                complete_join(&shared_for_panic, Err(JoinError::Panic(payload)));
            })),
        )?;

        Ok(JoinHandle { shared, task })
    }

    fn spawn_with_panic_handler<F>(
        &self,
        name: Option<String>,
        future: F,
        panic_handler: Option<PanicHandler>,
    ) -> Result<Arc<Task>, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = self.scheduler.allocate_task_id();
        let task = Arc::new(Task::new(
            id,
            name,
            Box::pin(future),
            Arc::clone(&self.scheduler),
            panic_handler,
        ));

        self.scheduler.schedule(Arc::clone(&task))?;
        Ok(task)
    }

    /// Returns an owned snapshot of this spawner's executor.
    pub fn snapshot(&self) -> ExecutorSnapshot {
        self.scheduler.snapshot()
    }

    /// Returns a weak observer handle for this spawner's executor.
    pub fn observer(&self) -> ExecutorObserver {
        ExecutorObserver {
            scheduler: Arc::downgrade(&self.scheduler),
        }
    }
}

/// Future returned by [`Spawner::spawn_with_handle`].
#[must_use = "join handles do nothing unless polled or awaited"]
pub struct JoinHandle<T> {
    shared: Arc<Mutex<JoinState<T>>>,
    task: Arc<Task>,
}

struct JoinState<T> {
    result: Option<JoinResult<T>>,
    waker: Option<Waker>,
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

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> JoinHandle<T> {
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

fn complete_join<T>(shared: &Arc<Mutex<JoinState<T>>>, result: JoinResult<T>) {
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

    /// Spawns a child task that receives this scope's stop token.
    pub fn spawn_with_stop<F, Fut>(&mut self, make_future: F) -> Result<(), SpawnError>
    where
        F: FnOnce(StopToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn(make_future(self.stop_token()))
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

/// Single-threaded executor that polls tasks from a ready queue.
#[derive(Debug)]
pub struct Executor {
    scheduler: Arc<Scheduler>,
    #[cfg(unix)]
    reactor: OsReactor,
}

impl Executor {
    /// Returns an owned snapshot of this executor's scheduler and tasks.
    pub fn snapshot(&self) -> ExecutorSnapshot {
        self.scheduler.snapshot()
    }

    /// Returns a weak observer handle for this executor.
    pub fn observer(&self) -> ExecutorObserver {
        ExecutorObserver {
            scheduler: Arc::downgrade(&self.scheduler),
        }
    }

    /// Runs tasks until all spawners and runnable tasks are gone.
    pub fn run(&self) {
        loop {
            self.poll_ready_tasks();

            self.scheduler.wake_expired_timers();

            if self.scheduler.is_drained() {
                break;
            }

            if self.scheduler.has_ready_tasks() {
                continue;
            }

            #[cfg(unix)]
            {
                let event = self
                    .reactor
                    .wait_io(
                        &self.scheduler.read_interest_fds(),
                        &self.scheduler.write_interest_fds(),
                        self.scheduler.time_until_next_timer(),
                    )
                    .expect("OS reactor wait failed while running executor");
                self.scheduler.wake_readable_fds(&event.readable);
                self.scheduler.wake_writable_fds(&event.writable);
            }

            self.scheduler.wake_expired_timers();
        }
    }

    /// Runs `future` to completion while also driving spawned executor tasks.
    pub fn run_until<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let root = Arc::new(RootWaker::new(Arc::clone(&self.scheduler)));
        let waker = Waker::from(Arc::clone(&root));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            if root.take_ready() {
                set_current_scheduler(Some(Arc::clone(&self.scheduler)));

                let poll_result =
                    panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

                set_current_scheduler(None);

                match poll_result {
                    Ok(Poll::Ready(output)) => return output,
                    Ok(Poll::Pending) => {}
                    Err(payload) => panic::resume_unwind(payload),
                }
            }

            self.poll_ready_tasks();

            self.scheduler.wake_expired_timers();

            if root.is_ready() || self.scheduler.has_ready_tasks() {
                continue;
            }

            #[cfg(unix)]
            {
                let event = self
                    .reactor
                    .wait_io(
                        &self.scheduler.read_interest_fds(),
                        &self.scheduler.write_interest_fds(),
                        self.scheduler.time_until_next_timer(),
                    )
                    .expect("OS reactor wait failed while running root future");
                self.scheduler.wake_readable_fds(&event.readable);
                self.scheduler.wake_writable_fds(&event.writable);
            }

            self.scheduler.wake_expired_timers();
        }
    }

    fn poll_ready_tasks(&self) {
        for _ in 0..READY_POLL_BUDGET {
            let Some(task) = self.scheduler.next_task() else {
                break;
            };
            task.poll();
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.scheduler.close();
    }
}

struct RootWaker {
    ready: Mutex<bool>,
    scheduler: Arc<Scheduler>,
}

impl RootWaker {
    fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            ready: Mutex::new(true),
            scheduler,
        }
    }

    fn take_ready(&self) -> bool {
        let mut ready = self.ready.lock().expect("root waker mutex poisoned");
        let was_ready = *ready;
        *ready = false;
        was_ready
    }

    fn is_ready(&self) -> bool {
        *self.ready.lock().expect("root waker mutex poisoned")
    }

    fn mark_ready(&self) {
        *self.ready.lock().expect("root waker mutex poisoned") = true;
        self.scheduler.wake_reactor();
    }
}

impl fmt::Debug for RootWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootWaker").finish_non_exhaustive()
    }
}

impl Wake for RootWaker {
    fn wake(self: Arc<Self>) {
        self.mark_ready();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark_ready();
    }
}

/// Creates a paired executor and spawner.
pub fn executor_and_spawner() -> (Executor, Spawner) {
    #[cfg(unix)]
    {
        let reactor = OsReactor::new().expect("failed to create OS reactor for executor");
        let scheduler = Arc::new(Scheduler::new(reactor.waker()));

        (
            Executor {
                scheduler: Arc::clone(&scheduler),
                reactor,
            },
            Spawner { scheduler },
        )
    }
}

/// Runs one future to completion on a fresh single-threaded executor.
///
/// The root future is polled directly by the executor, so it may borrow from
/// the caller's stack.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let (executor, spawner) = executor_and_spawner();
    drop(spawner);

    executor.run_until(future)
}

#[cfg(test)]
mod tests;
