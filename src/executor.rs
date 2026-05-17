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

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;
#[cfg(unix)]
use crate::os::OsReactor;

mod driver;
mod future;
mod join;
mod scheduler;
mod scope;
mod sync;
mod task;
#[cfg(unix)]
mod tcp;
#[cfg(unix)]
mod unix_io;
#[cfg(target_os = "linux")]
mod uring;

pub use future::{
    Race, RaceOutput, Sleep, Timeout, TimeoutError, YieldNow, race, sleep, timeout, yield_now,
};
pub use join::{JoinError, JoinHandle};
use join::{JoinState, complete_join};
use scheduler::{Scheduler, set_current_scheduler};
pub use scope::{TaskScope, TaskScopeError};
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
#[cfg(target_os = "linux")]
pub use uring::{ReadAtUring, read_at_uring, read_exact_at_uring, write_all_at_uring};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type PanicPayload = Box<dyn std::any::Any + Send + 'static>;
type PanicHandler = Box<dyn FnOnce(PanicPayload) + Send + 'static>;

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
    /// Snapshot of the executor-owned Linux `io_uring` dispatcher, if installed.
    #[cfg(target_os = "linux")]
    pub io_uring: Option<IoUringDispatcherSnapshot>,
    /// Maximum number of ready tasks polled before timers and readiness are checked.
    pub ready_poll_budget: usize,
    /// Number of tasks accepted by this executor since startup.
    pub total_spawned_tasks: u64,
    /// Number of tasks that have completed, panicked, or been cancelled since startup.
    pub total_completed_tasks: u64,
    /// Number of spawned task polls performed since startup.
    pub total_task_polls: u64,
    /// Number of ready-poll batches that consumed the full ready-poll budget.
    pub ready_poll_budget_exhaustions: u64,
    /// Number of idle driver events observed by the executor.
    pub total_driver_events: u64,
    /// Number of readiness driver events observed by the executor.
    #[cfg(unix)]
    pub total_readiness_events: u64,
    /// Number of readiness driver events that reported at least one readable fd.
    #[cfg(unix)]
    pub total_readable_events: u64,
    /// Number of readiness driver events that reported at least one writable fd.
    #[cfg(unix)]
    pub total_writable_events: u64,
    /// Number of Linux completion driver events observed by the executor.
    #[cfg(target_os = "linux")]
    pub total_completion_events: u64,
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

        Ok(JoinHandle::new(shared, task))
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
        #[cfg(target_os = "linux")]
        let _io_uring_scope = IoUringScope::enter();
        self.refresh_io_uring_snapshot();

        loop {
            self.poll_ready_tasks();

            self.scheduler.wake_expired_timers();
            driver::dispatch_available(&self.scheduler);

            if self.scheduler.is_drained() {
                break;
            }

            if self.scheduler.has_ready_tasks() {
                continue;
            }

            let event = self.wait_for_driver_event("running executor");
            driver::apply_event(&self.scheduler, event);

            self.scheduler.wake_expired_timers();
        }
    }

    /// Runs `future` to completion while also driving spawned executor tasks.
    pub fn run_until<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        #[cfg(target_os = "linux")]
        let _io_uring_scope = IoUringScope::enter();
        self.refresh_io_uring_snapshot();

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
            driver::dispatch_available(&self.scheduler);

            if root.is_ready() || self.scheduler.has_ready_tasks() {
                continue;
            }

            let event = self.wait_for_driver_event("running root future");
            driver::apply_event(&self.scheduler, event);

            self.scheduler.wake_expired_timers();
        }
    }

    fn poll_ready_tasks(&self) {
        let mut polled = 0;

        for _ in 0..READY_POLL_BUDGET {
            let Some(task) = self.scheduler.next_task() else {
                break;
            };
            polled += 1;
            task.poll();
        }

        self.scheduler.record_ready_poll_batch(
            polled,
            polled == READY_POLL_BUDGET && self.scheduler.has_ready_tasks(),
        );
    }

    fn wait_for_driver_event(&self, context: &str) -> Option<driver::DriverEvent> {
        #[cfg(unix)]
        return driver::wait_for_event(&self.scheduler, &self.reactor, context);

        #[cfg(not(unix))]
        return driver::wait_for_event(&self.scheduler, context);
    }

    fn refresh_io_uring_snapshot(&self) {
        #[cfg(target_os = "linux")]
        self.scheduler.record_io_uring_snapshot(uring::snapshot());
    }
}

#[cfg(target_os = "linux")]
struct IoUringScope;

#[cfg(target_os = "linux")]
impl IoUringScope {
    fn enter() -> Self {
        uring::install_current_io_uring();
        Self
    }
}

#[cfg(target_os = "linux")]
impl Drop for IoUringScope {
    fn drop(&mut self) {
        uring::clear_current_io_uring();
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
