use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use super::join::{JoinState, complete_join};
use super::scheduler::Scheduler;
use super::scheduling_group::{SchedulingGroup, SchedulingGroupError};
use super::task::Task;
use super::{ExecutorSnapshot, JoinError, JoinHandle, PanicHandler, SchedulingGroupId};

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
}

/// Error returned when a task cannot be submitted to an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    /// The executor is no longer accepting tasks.
    Closed,
    /// The scheduling group belongs to a different executor.
    SchedulingGroupExecutorMismatch,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpawnError::Closed => write!(f, "executor is not accepting tasks"),
            SpawnError::SchedulingGroupExecutorMismatch => {
                write!(f, "scheduling group belongs to a different executor")
            }
        }
    }
}

impl Error for SpawnError {}

/// Handle used to submit futures to an [`super::Executor`].
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
    pub(super) fn new(scheduler: Arc<Scheduler>) -> Self {
        Self { scheduler }
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
        self.spawn_with_panic_handler(name, scheduling_group_id, future, None)
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

        let task = self.spawn_with_panic_handler(
            name,
            scheduling_group_id,
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
        scheduling_group_id: SchedulingGroupId,
        future: F,
        panic_handler: Option<PanicHandler>,
    ) -> Result<Arc<Task>, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = self.scheduler.allocate_task_id();
        let task = Arc::new(Task::new_in_group(
            id,
            name,
            scheduling_group_id,
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
