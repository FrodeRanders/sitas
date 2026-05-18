//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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
mod scheduling_group;
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
mod types;
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
pub use scheduling_group::{SchedulingGroup, SchedulingGroupError};
pub use scope::{TaskScope, TaskScopeError};
pub use spawner::{ExecutorObserver, SpawnError, Spawner};
pub use sync::{Notified, Notify, StopSource, StopToken, stop_pair};
#[cfg(unix)]
pub use tcp::{
    serve_tcp_n, serve_tcp_n_in_group, serve_tcp_n_timeout, serve_tcp_n_timeout_in_group,
    serve_tcp_until_idle, serve_tcp_until_idle_in_group, serve_tcp_until_idle_timeout,
    serve_tcp_until_idle_timeout_in_group, serve_tcp_until_stopped,
    serve_tcp_until_stopped_in_group, serve_tcp_until_stopped_scoped,
    serve_tcp_until_stopped_scoped_in_group, serve_tcp_until_stopped_scoped_timeout,
    serve_tcp_until_stopped_scoped_timeout_in_group, serve_tcp_until_stopped_timeout,
    serve_tcp_until_stopped_timeout_in_group,
};
pub use types::{
    DEFAULT_SCHEDULING_GROUP_ID, DEFAULT_SCHEDULING_GROUP_SHARES, ExecutorSnapshot,
    SchedulingGroupId, SchedulingGroupSnapshot, TaskId, TaskSnapshot, TaskStatus, TaskWait,
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
        let _io_uring_scope = uring::ExecutorIoUringScope::enter();
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
        let _io_uring_scope = uring::ExecutorIoUringScope::enter();
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
            if let Some((group_id, poll_duration)) = task.poll() {
                self.scheduler.record_task_poll(group_id, poll_duration);
            }
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
