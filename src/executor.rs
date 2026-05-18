//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::future::Future;
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use crate::os::IoUringDispatcherSnapshot;
#[cfg(unix)]
use crate::os::OsReactor;

mod counters;
mod current;
mod driver;
mod future;
#[cfg(unix)]
mod io_interest;
mod join;
mod root;
mod scheduler;
mod scope;
mod snapshot;
mod spawner;
mod sync;
mod task;
mod task_set;
mod task_state;
#[cfg(unix)]
mod tcp;
mod timer;
#[cfg(unix)]
mod unix_io;
#[cfg(target_os = "linux")]
mod uring;

use current::enter_scheduler;
pub use future::{
    Race, RaceOutput, Sleep, Timeout, TimeoutError, YieldNow, race, sleep, timeout, yield_now,
};
pub use join::{JoinError, JoinHandle};
use root::RootWaker;
use scheduler::Scheduler;
pub use scope::{TaskScope, TaskScopeError};
pub use spawner::{ExecutorObserver, SpawnError, Spawner};
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
    /// The task was canceled before completing.
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
    /// Number of tasks that have completed, panicked, or been canceled since startup.
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
        ExecutorObserver::new(Arc::downgrade(&self.scheduler))
    }

    /// Runs tasks until all spawners and runnable tasks are gone.
    pub fn run(&self) {
        #[cfg(target_os = "linux")]
        let _io_uring_scope = IoUringScope::enter();
        self.refresh_io_uring_snapshot();

        loop {
            self.drive_ready_work();

            if self.scheduler.is_drained() {
                break;
            }

            if self.scheduler.has_ready_tasks() {
                continue;
            }

            self.wait_for_idle_driver_event("running executor");
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
                let current_scheduler = enter_scheduler(Arc::clone(&self.scheduler));

                let poll_result =
                    panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
                drop(current_scheduler);

                match poll_result {
                    Ok(Poll::Ready(output)) => return output,
                    Ok(Poll::Pending) => {}
                    Err(payload) => panic::resume_unwind(payload),
                }
            }

            self.drive_ready_work();

            if root.is_ready() || self.scheduler.has_ready_tasks() {
                continue;
            }

            self.wait_for_idle_driver_event("running root future");
        }
    }

    fn drive_ready_work(&self) {
        self.poll_ready_tasks();
        self.scheduler.wake_expired_timers();
        driver::dispatch_available(&self.scheduler);
    }

    fn wait_for_idle_driver_event(&self, context: &str) {
        let event = self.wait_for_driver_event(context);
        driver::apply_event(&self.scheduler, event);
        self.scheduler.wake_expired_timers();
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
            Spawner::new(scheduler),
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
