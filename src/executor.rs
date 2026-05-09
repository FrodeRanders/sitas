//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::cell::RefCell;
use std::collections::VecDeque;
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
use crate::os::{OsReactor, OsWaker};

mod future;
mod sync;
#[cfg(unix)]
mod tcp;
#[cfg(unix)]
mod unix_io;

pub use future::{
    Race, RaceOutput, Sleep, Timeout, TimeoutError, YieldNow, race, sleep, timeout, yield_now,
};
pub use sync::{Notified, Notify, StopSource, StopToken, stop_pair};
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

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
    static CURRENT_TASK: RefCell<Option<Arc<Task>>> = const { RefCell::new(None) };
}

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
                CURRENT_SCHEDULER.with(|current| {
                    *current.borrow_mut() = Some(Arc::clone(&self.scheduler));
                });

                let poll_result =
                    panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

                CURRENT_SCHEDULER.with(|current| {
                    *current.borrow_mut() = None;
                });

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

struct Task {
    id: TaskId,
    name: Option<String>,
    created_at: Instant,
    state: Mutex<TaskState>,
    scheduler: Arc<Scheduler>,
}

struct TaskState {
    future: Option<BoxFuture>,
    panic_handler: Option<PanicHandler>,
    queued: bool,
    polling: bool,
    cancel_requested: bool,
    completed: bool,
    poll_count: u64,
    total_poll_time: Duration,
    waiting_for: Option<TaskWait>,
    last_scheduled_at: Option<Instant>,
    last_poll_started_at: Option<Instant>,
    last_poll_finished_at: Option<Instant>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").finish_non_exhaustive()
    }
}

impl Task {
    fn new(
        id: TaskId,
        name: Option<String>,
        future: BoxFuture,
        scheduler: Arc<Scheduler>,
        panic_handler: Option<PanicHandler>,
    ) -> Self {
        Self {
            id,
            name,
            created_at: Instant::now(),
            state: Mutex::new(TaskState {
                future: Some(future),
                panic_handler,
                queued: false,
                polling: false,
                cancel_requested: false,
                completed: false,
                poll_count: 0,
                total_poll_time: Duration::ZERO,
                waiting_for: None,
                last_scheduled_at: None,
                last_poll_started_at: None,
                last_poll_finished_at: None,
            }),
            scheduler,
        }
    }

    fn poll(self: Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut context = Context::from_waker(&waker);
        let poll_started_at = Instant::now();
        let mut future = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            state.queued = false;

            let Some(future) = state.future.take() else {
                return;
            };
            state.polling = true;
            state.waiting_for = None;
            state.last_poll_started_at = Some(poll_started_at);
            state.poll_count += 1;
            future
        };

        let scheduler = Arc::clone(&self.scheduler);
        CURRENT_SCHEDULER.with(|current| {
            *current.borrow_mut() = Some(scheduler);
        });
        CURRENT_TASK.with(|current| {
            *current.borrow_mut() = Some(Arc::clone(&self));
        });

        let poll_result =
            panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        CURRENT_TASK.with(|current| {
            *current.borrow_mut() = None;
        });
        CURRENT_SCHEDULER.with(|current| {
            *current.borrow_mut() = None;
        });

        let poll_finished_at = Instant::now();
        let poll_duration = poll_finished_at.saturating_duration_since(poll_started_at);

        match poll_result {
            Ok(Poll::Ready(())) => {
                let mut state = self.state.lock().expect("task state mutex poisoned");
                state.polling = false;
                state.completed = true;
                state.waiting_for = None;
                state.total_poll_time += poll_duration;
                state.last_poll_finished_at = Some(poll_finished_at);
                drop(state);
                self.scheduler.finish_task();
            }
            Ok(Poll::Pending) => {
                let cancelled = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    state.total_poll_time += poll_duration;
                    state.last_poll_finished_at = Some(poll_finished_at);
                    if state.waiting_for.is_none() {
                        state.waiting_for = Some(TaskWait::Unknown);
                    }
                    if state.cancel_requested {
                        state.completed = true;
                        state.waiting_for = None;
                        true
                    } else {
                        state.future = Some(future);
                        false
                    }
                };

                if cancelled {
                    self.scheduler.finish_task();
                    self.scheduler.wake_reactor();
                }
            }
            Err(payload) => {
                let panic_handler = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    state.future = None;
                    state.completed = true;
                    state.waiting_for = None;
                    state.total_poll_time += poll_duration;
                    state.last_poll_finished_at = Some(poll_finished_at);
                    state.panic_handler.take()
                };

                if let Some(panic_handler) = panic_handler {
                    panic_handler(payload);
                }
                self.scheduler.finish_task();
            }
        }
    }

    fn mark_queued(&self) -> bool {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        if state.queued || (state.future.is_none() && !state.polling) {
            return false;
        }

        state.queued = true;
        state.waiting_for = None;
        state.last_scheduled_at = Some(Instant::now());
        true
    }

    fn cancel(&self) -> bool {
        let should_finish = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            if state.future.is_none() && !state.polling {
                return false;
            }

            state.cancel_requested = true;

            if state.polling {
                false
            } else {
                state.future = None;
                state.queued = false;
                state.completed = true;
                state.waiting_for = None;
                true
            }
        };

        if should_finish {
            self.scheduler.finish_task();
        }
        self.scheduler.wake_reactor();
        true
    }

    fn drop_future(&self) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        state.cancel_requested = true;
        if !state.polling {
            state.future = None;
            state.queued = false;
            state.completed = true;
            state.waiting_for = None;
        }
    }

    fn clear_queued(&self) {
        self.state.lock().expect("task state mutex poisoned").queued = false;
    }

    fn set_waiting_for(&self, waiting_for: TaskWait) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        if state.polling && !state.completed {
            state.waiting_for = Some(waiting_for);
        }
    }

    fn snapshot(&self) -> TaskSnapshot {
        let state = self.state.lock().expect("task state mutex poisoned");
        let status = if state.completed && state.cancel_requested {
            TaskStatus::Cancelled
        } else if state.completed {
            TaskStatus::Completed
        } else if state.polling {
            TaskStatus::Polling
        } else if state.queued {
            TaskStatus::Queued
        } else {
            TaskStatus::Waiting
        };

        TaskSnapshot {
            id: self.id,
            name: self.name.clone(),
            status,
            waiting_for: state.waiting_for,
            poll_count: state.poll_count,
            total_poll_time: state.total_poll_time,
            created_at: self.created_at,
            last_scheduled_at: state.last_scheduled_at,
            last_poll_started_at: state.last_poll_started_at,
            last_poll_finished_at: state.last_poll_finished_at,
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let scheduler = Arc::clone(&self.scheduler);
        let _ = scheduler.schedule_existing(self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.scheduler.schedule_existing(self.clone());
    }
}

#[derive(Debug)]
struct Scheduler {
    state: Mutex<SchedulerState>,
    #[cfg(unix)]
    waker: OsWaker,
}

#[derive(Debug)]
struct SchedulerState {
    queue: VecDeque<Arc<Task>>,
    tasks: Vec<Weak<Task>>,
    timers: Vec<TimerEntry>,
    #[cfg(unix)]
    read_interests: InterestSet,
    #[cfg(unix)]
    write_interests: InterestSet,
    accepting: bool,
    spawner_count: usize,
    task_count: usize,
    next_task_id: usize,
    next_timer_id: usize,
}

#[derive(Debug)]
struct TimerEntry {
    id: usize,
    deadline: Instant,
    waker: Waker,
}

#[cfg(unix)]
#[derive(Debug)]
struct InterestSet {
    interests: Vec<IoInterest>,
    ready: Vec<usize>,
    next_id: usize,
}

#[cfg(unix)]
#[derive(Debug)]
struct IoInterest {
    id: usize,
    fd: RawFd,
    waker: Waker,
}

#[cfg(unix)]
impl InterestSet {
    fn new() -> Self {
        Self {
            interests: Vec::new(),
            ready: Vec::new(),
            next_id: 0,
        }
    }

    fn allocate_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    fn register(&mut self, id: usize, fd: RawFd, waker: Waker) {
        match self.interests.iter_mut().find(|interest| interest.id == id) {
            Some(interest) => {
                interest.fd = fd;
                interest.waker = waker;
            }
            None => self.interests.push(IoInterest { id, fd, waker }),
        }
    }

    fn remove(&mut self, id: usize) {
        self.interests.retain(|interest| interest.id != id);
        self.ready.retain(|ready_id| *ready_id != id);
    }

    fn clear(&mut self) {
        self.interests.clear();
        self.ready.clear();
    }

    fn fds(&self) -> Vec<RawFd> {
        self.interests.iter().map(|interest| interest.fd).collect()
    }

    fn len(&self) -> usize {
        self.interests.len()
    }

    fn wake_ready(&mut self, ready_fds: &[RawFd]) -> Vec<Waker> {
        let mut wakers = Vec::new();
        let mut ready_ids = Vec::new();
        let mut pending = Vec::with_capacity(self.interests.len());

        for interest in self.interests.drain(..) {
            if ready_fds.contains(&interest.fd) {
                ready_ids.push(interest.id);
                wakers.push(interest.waker);
            } else {
                pending.push(interest);
            }
        }

        self.interests = pending;
        self.ready.extend(ready_ids);
        wakers
    }

    fn take_ready(&mut self, id: usize) -> bool {
        let Some(position) = self.ready.iter().position(|ready_id| *ready_id == id) else {
            return false;
        };

        self.ready.swap_remove(position);
        true
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.interests.is_empty() && self.ready.is_empty()
    }
}

impl Scheduler {
    fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                queue: VecDeque::new(),
                tasks: Vec::new(),
                timers: Vec::new(),
                #[cfg(unix)]
                read_interests: InterestSet::new(),
                #[cfg(unix)]
                write_interests: InterestSet::new(),
                accepting: true,
                spawner_count: 1,
                task_count: 0,
                next_task_id: 0,
                next_timer_id: 0,
            }),
            #[cfg(unix)]
            waker,
        }
    }

    fn allocate_task_id(&self) -> TaskId {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        let id = state.next_task_id;
        state.next_task_id = state.next_task_id.wrapping_add(1);
        TaskId(id)
    }

    fn add_spawner(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.spawner_count += 1;
    }

    fn remove_spawner(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.spawner_count = state.spawner_count.saturating_sub(1);
            state.spawner_count == 0
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn schedule(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                task.clear_queued();
                return Err(SpawnError);
            }
            state.task_count += 1;
            state.tasks.push(Arc::downgrade(&task));
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    fn schedule_existing(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                task.clear_queued();
                return Err(SpawnError);
            }
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    fn next_task(&self) -> Option<Arc<Task>> {
        self.state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .pop_front()
    }

    fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
    }

    fn has_ready_tasks(&self) -> bool {
        !self
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .is_empty()
    }

    fn close(&self) {
        let tasks = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.accepting = false;
            state.task_count = 0;
            state.queue.clear();
            state.timers.clear();
            #[cfg(unix)]
            {
                state.read_interests.clear();
                state.write_interests.clear();
            }

            state
                .tasks
                .drain(..)
                .filter_map(|task| task.upgrade())
                .collect::<Vec<_>>()
        };

        for task in tasks {
            task.drop_future();
        }

        self.wake_reactor();
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let accepting = state.accepting;
        let spawner_count = state.spawner_count;
        let task_count = state.task_count;
        let ready_queue_len = state.queue.len();
        let timer_count = state.timers.len();
        #[cfg(unix)]
        let read_interest_count = state.read_interests.len();
        #[cfg(unix)]
        let write_interest_count = state.write_interests.len();
        let tasks = state.tasks.clone();
        drop(state);

        let mut tasks = tasks
            .into_iter()
            .filter_map(|task| task.upgrade())
            .map(|task| task.snapshot())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.id);

        ExecutorSnapshot {
            accepting,
            spawner_count,
            task_count,
            ready_queue_len,
            timer_count,
            #[cfg(unix)]
            read_interest_count,
            #[cfg(unix)]
            write_interest_count,
            tasks,
        }
    }

    fn finish_task(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.task_count = state.task_count.saturating_sub(1);
            state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn allocate_timer_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        let id = state.next_timer_id;
        state.next_timer_id = state.next_timer_id.wrapping_add(1);
        id
    }

    fn register_timer(&self, id: usize, deadline: Instant, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Timer { deadline });
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let previous_next = next_timer_deadline(&state.timers);

            match state.timers.iter_mut().find(|timer| timer.id == id) {
                Some(timer) => {
                    timer.deadline = deadline;
                    timer.waker = waker;
                }
                None => state.timers.push(TimerEntry {
                    id,
                    deadline,
                    waker,
                }),
            }

            previous_next.is_none_or(|previous| deadline < previous)
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn remove_timer(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.retain(|timer| timer.id != id);
    }

    fn wake_expired_timers(&self) {
        let expired = {
            let now = Instant::now();
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let mut expired = Vec::new();
            let mut pending = Vec::with_capacity(state.timers.len());

            for timer in state.timers.drain(..) {
                if timer.deadline <= now {
                    expired.push(timer.waker);
                } else {
                    pending.push(timer);
                }
            }

            state.timers = pending;
            expired
        };

        for waker in expired {
            waker.wake();
        }
    }

    fn time_until_next_timer(&self) -> Option<Duration> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let deadline = next_timer_deadline(&state.timers)?;
        Some(deadline.saturating_duration_since(Instant::now()))
    }

    #[cfg(unix)]
    fn allocate_read_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.allocate_id()
    }

    #[cfg(unix)]
    fn register_read_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Readable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    fn remove_read_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.remove(id);
    }

    #[cfg(unix)]
    fn read_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.fds()
    }

    #[cfg(unix)]
    fn wake_readable_fds(&self, readable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.wake_ready(readable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    fn take_ready_read_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.take_ready(id)
    }

    #[cfg(unix)]
    fn allocate_write_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.allocate_id()
    }

    #[cfg(unix)]
    fn register_write_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        set_current_task_waiting_for(TaskWait::Writable { fd });
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    fn remove_write_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.remove(id);
    }

    #[cfg(unix)]
    fn write_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.fds()
    }

    #[cfg(unix)]
    fn wake_writable_fds(&self, writable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.wake_ready(writable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    fn take_ready_write_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.take_ready(id)
    }

    fn wake_reactor(&self) {
        #[cfg(unix)]
        let _ = self.waker.wake();
    }
}

fn next_timer_deadline(timers: &[TimerEntry]) -> Option<Instant> {
    timers.iter().map(|timer| timer.deadline).min()
}

fn current_scheduler() -> Arc<Scheduler> {
    CURRENT_SCHEDULER
        .with(|current| current.borrow().as_ref().cloned())
        .expect("executor futures must be polled by sitas::executor::Executor")
}

fn set_current_task_waiting_for(waiting_for: TaskWait) {
    CURRENT_TASK.with(|current| {
        if let Some(task) = current.borrow().as_ref() {
            task.set_waiting_for(waiting_for);
        }
    });
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
