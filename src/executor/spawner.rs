//! Task spawning and executor monitoring.
//!
//! [`Spawner`] submits futures to the executor with optional naming,
//! scheduling group placement, join handles, and optional backpressure
//! limiting. [`ExecutorObserver`] provides a weak monitoring handle that
//! does not keep the executor alive.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use super::join::{JoinState, complete_join};
use super::scheduler::Scheduler;
use super::scheduling_group::{SchedulingGroup, SchedulingGroupError};
use super::task::Task;
use super::{ExecutorSnapshot, JoinError, JoinHandle, PanicHandler, SchedulingGroupId};

use crate::executor::backpressure::{BackpressureGuard, Permit};

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
    pub(super) fn new(scheduler: Weak<Scheduler>) -> Self {
        Self { scheduler }
    }

    /// Returns an executor snapshot if the executor is still alive.
    pub fn snapshot(&self) -> Option<ExecutorSnapshot> {
        self.scheduler
            .upgrade()
            .map(|scheduler| scheduler.snapshot())
    }

    /// Requests that the observed executor stop its run loop as soon as
    /// possible, even if tasks are still pending.
    ///
    /// Returns `true` if the executor was still alive and the stop was
    /// signaled, or `false` if it has already shut down. This is used by forced
    /// shutdown paths to unblock an executor whose tasks are not cooperating.
    pub fn request_stop(&self) -> bool {
        match self.scheduler.upgrade() {
            Some(scheduler) => {
                scheduler.request_stop();
                true
            }
            None => false,
        }
    }
}

/// Error returned when a task cannot be submitted to an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    /// The executor is no longer accepting tasks.
    Closed,
    /// The scheduling group belongs to a different executor.
    SchedulingGroupExecutorMismatch,
    /// Backpressure limit reached; no spawn capacity available.
    Backpressure,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpawnError::Closed => write!(f, "executor is not accepting tasks"),
            SpawnError::SchedulingGroupExecutorMismatch => {
                write!(f, "scheduling group belongs to a different executor")
            }
            SpawnError::Backpressure => write!(f, "spawn backpressure limit reached"),
        }
    }
}

impl Error for SpawnError {}

/// Handle used to submit futures to an [`super::Executor`].
pub struct Spawner {
    scheduler: Arc<Scheduler>,
    backpressure: Option<BackpressureGuard>,
}

impl fmt::Debug for Spawner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spawner")
            .field("backpressure", &self.backpressure)
            .finish()
    }
}

impl Clone for Spawner {
    fn clone(&self) -> Self {
        self.scheduler.add_spawner();

        Self {
            scheduler: Arc::clone(&self.scheduler),
            backpressure: self.backpressure.clone(),
        }
    }
}

impl Drop for Spawner {
    fn drop(&mut self) {
        self.scheduler.remove_spawner();
    }
}

impl Spawner {
    pub(super) fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            scheduler,
            backpressure: None,
        }
    }

    /// Attaches a backpressure guard to this spawner, limiting concurrent
    /// in-flight tasks. Cloned spawners share the same guard.
    pub fn with_backpressure(mut self, capacity: usize) -> Self {
        self.backpressure = Some(BackpressureGuard::new(capacity));
        self
    }

    /// Returns the current backpressure in-flight count, if configured.
    pub fn backpressure_in_flight(&self) -> Option<usize> {
        self.backpressure.as_ref().map(|g| g.in_flight())
    }

    /// Returns the configured backpressure capacity, if any.
    pub fn backpressure_capacity(&self) -> Option<usize> {
        self.backpressure.as_ref().map(|g| g.capacity())
    }

    /// Creates an executor-local scheduling group with a relative weight.
    pub fn create_scheduling_group(
        &self,
        name: impl Into<String>,
        shares: u32,
    ) -> Result<SchedulingGroup, SchedulingGroupError> {
        if shares == 0 {
            return Err(SchedulingGroupError::ZeroShares);
        }

        let name = name.into();
        let id = self.scheduler.create_scheduling_group(name.clone(), shares);
        Ok(SchedulingGroup::new(self.scheduler.id(), id, name, shares))
    }

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

    /// Spawns a future into a scheduling group.
    pub fn spawn_in_group<F>(&self, group: &SchedulingGroup, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.ensure_group_belongs_to_executor(group)?;
        self.spawn_with_name_and_group(None, group.id(), future)
    }

    /// Spawns a named future into a scheduling group.
    pub fn spawn_named_in_group<F>(
        &self,
        group: &SchedulingGroup,
        name: impl Into<String>,
        future: F,
    ) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.ensure_group_belongs_to_executor(group)?;
        self.spawn_with_name_and_group(Some(name.into()), group.id(), future)
    }

    fn spawn_with_name<F>(&self, name: Option<String>, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_name_and_group(name, SchedulingGroup::default().id(), future)
    }

    fn spawn_with_name_and_group<F>(
        &self,
        name: Option<String>,
        scheduling_group_id: SchedulingGroupId,
        future: F,
    ) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _permit = self.acquire_backpressure()?;
        self.spawn_inner(name, scheduling_group_id, future, _permit, None)
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

    /// Spawns a future into a scheduling group and returns an awaitable handle.
    pub fn spawn_with_handle_in_group<F>(
        &self,
        group: &SchedulingGroup,
        future: F,
    ) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.ensure_group_belongs_to_executor(group)?;
        self.spawn_with_handle_name_and_group(None, group.id(), future)
    }

    /// Spawns a named future into a scheduling group and returns an awaitable
    /// handle.
    pub fn spawn_with_handle_named_in_group<F>(
        &self,
        group: &SchedulingGroup,
        name: impl Into<String>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.ensure_group_belongs_to_executor(group)?;
        self.spawn_with_handle_name_and_group(Some(name.into()), group.id(), future)
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
        self.spawn_with_handle_name_and_group(name, SchedulingGroup::default().id(), future)
    }

    fn spawn_with_handle_name_and_group<F>(
        &self,
        name: Option<String>,
        scheduling_group_id: SchedulingGroupId,
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

        let permit = self.acquire_backpressure()?;

        let task = self.spawn_inner(
            name,
            scheduling_group_id,
            async move {
                let output = future.await;
                complete_join(&shared_for_task, Ok(output));
            },
            permit,
            Some(Box::new(move |payload| {
                complete_join(&shared_for_panic, Err(JoinError::Panic(payload)));
            })),
        )?;

        Ok(JoinHandle::new(shared, task))
    }

    fn acquire_backpressure(&self) -> Result<Option<Permit>, SpawnError> {
        match &self.backpressure {
            Some(guard) => guard
                .try_acquire()
                .map(Some)
                .ok_or(SpawnError::Backpressure),
            None => Ok(None),
        }
    }

    fn spawn_inner<F>(
        &self,
        name: Option<String>,
        scheduling_group_id: SchedulingGroupId,
        future: F,
        permit: Option<Permit>,
        panic_handler: Option<PanicHandler>,
    ) -> Result<Arc<Task>, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = self.scheduler.allocate_task_id();
        let wrapped = async move {
            // Hold the permit for the task's lifetime by moving it into the
            // async block. It is dropped when the task completes or is cancelled.
            let _permit = permit;
            future.await;
        };
        let task = Arc::new(Task::new_in_group(
            id,
            name,
            scheduling_group_id,
            Box::pin(wrapped),
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
        ExecutorObserver::new(Arc::downgrade(&self.scheduler))
    }

    fn ensure_group_belongs_to_executor(&self, group: &SchedulingGroup) -> Result<(), SpawnError> {
        if group.belongs_to(self.scheduler.id()) {
            Ok(())
        } else {
            Err(SpawnError::SchedulingGroupExecutorMismatch)
        }
    }
}
