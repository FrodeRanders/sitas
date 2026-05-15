//! Shard-local state for the shard-per-thread async runtime.
//!
//! A `ShardLocal<T>` owns one `T` per shard. Access happens by submitting a
//! closure to the owning shard executor. The closure receives `&mut T` while it
//! is being polled on that shard, and no reference to `T` can escape the
//! closure.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::executor::{StopSource, StopToken, stop_pair};
use crate::shard::ShardId;
use crate::sharded_executor::{
    ShardedJoinError, ShardedJoinHandle, ShardedJoinTimeoutError, ShardedOperationError,
    ShardedSpawnError, ShardedSubmitter, current_executor_shard, join_all_shards,
    join_all_shards_timeout,
};

/// One value per shard, accessed only on the owning shard executor.
///
/// Cloning this handle does not clone the underlying values. It creates another
/// handle to the same per-shard cells and keeps the runtime accepting
/// submissions through a cloned [`ShardedSubmitter`].
#[must_use]
pub struct ShardLocal<T> {
    shards: Vec<ShardLocalSlot<T>>,
    submitter: ShardedSubmitter,
}

impl<T> Clone for ShardLocal<T> {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            submitter: self.submitter.clone(),
        }
    }
}

struct ShardLocalSlot<T> {
    shard_id: ShardId,
    cell: Arc<ShardLocalCell<T>>,
}

impl<T> Clone for ShardLocalSlot<T> {
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id,
            cell: Arc::clone(&self.cell),
        }
    }
}

struct ShardLocalCell<T> {
    owner: ShardId,
    value: UnsafeCell<T>,
}

// Safety: `ShardLocalCell` only exposes access through `with_mut`, which checks
// that the current executor shard is the owner. The public `ShardLocal` API
// schedules every access closure onto that owner shard and does not let
// references to `T` escape the closure.
unsafe impl<T: Send> Send for ShardLocalCell<T> {}
unsafe impl<T: Send> Sync for ShardLocalCell<T> {}

impl<T> ShardLocal<T>
where
    T: Send + 'static,
{
    /// Creates one shard-local value per shard.
    pub fn new<MakeValue>(submitter: ShardedSubmitter, mut make_value: MakeValue) -> Self
    where
        MakeValue: FnMut(ShardId) -> T,
    {
        let shards = (0..submitter.shard_count())
            .map(|idx| {
                let shard_id = ShardId(idx);
                ShardLocalSlot {
                    shard_id,
                    cell: Arc::new(ShardLocalCell {
                        owner: shard_id,
                        value: UnsafeCell::new(make_value(shard_id)),
                    }),
                }
            })
            .collect();

        Self { shards, submitter }
    }

    /// Returns the number of shard-local values.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Runs `operation` directly against the value owned by the current shard.
    ///
    /// This is the fast path for code that is already being polled by a
    /// [`ShardedExecutor`](crate::ShardedExecutor) shard thread. The closure is
    /// synchronous, so the borrowed local value cannot cross an `.await`.
    pub fn with_current<R, F>(&self, operation: F) -> Result<(ShardId, R), ShardLocalAccessError>
    where
        F: FnOnce(ShardId, &mut T) -> R,
    {
        let shard_id = current_executor_shard().ok_or(ShardLocalAccessError::NotOnShard)?;
        let cell = self.cell_for_current(shard_id)?;

        Ok((shard_id, cell.with_mut(|value| operation(shard_id, value))))
    }

    /// Runs `operation` against the value owned by `shard_id`.
    pub fn with_on<R, F>(
        &self,
        shard_id: ShardId,
        operation: F,
    ) -> Result<ShardedJoinHandle<R>, ShardedSpawnError>
    where
        R: Send + 'static,
        F: FnOnce(&mut T) -> R + Send + 'static,
    {
        let cell = Arc::clone(&self.cell_for(shard_id)?.cell);

        self.submitter
            .submit_with_handle_named_to(
                shard_id,
                format!("shard-local-{}", shard_id.0),
                async move { cell.with_mut(operation) },
            )
            .map(|handle| ShardedJoinHandle::new(shard_id, handle))
    }

    /// Runs one operation per shard and returns shard-tagged handles.
    pub fn with_all<R, F>(
        &self,
        operation: F,
    ) -> Result<Vec<ShardedJoinHandle<R>>, ShardedSpawnError>
    where
        R: Send + 'static,
        F: FnMut(ShardId, &mut T) -> R + Send + Clone + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for slot in &self.shards {
            let shard_id = slot.shard_id;
            let cell = Arc::clone(&slot.cell);
            let mut operation = operation.clone();
            let handle = self
                .submitter
                .submit_with_handle_named_to(
                    shard_id,
                    format!("shard-local-{}", shard_id.0),
                    async move { cell.with_mut(|value| operation(shard_id, value)) },
                )
                .map(|handle| ShardedJoinHandle::new(shard_id, handle))?;
            handles.push(handle);
        }

        Ok(handles)
    }

    /// Starts one async worker on each shard.
    ///
    /// Each worker receives its [`ShardId`] and a clone of this handle. Worker
    /// futures can call [`ShardLocal::with_current`] for direct access to their
    /// own shard-local value, or use the regular submission helpers when they
    /// need to work with other shards.
    pub fn spawn_workers<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<ShardLocalWorkers<Fut::Output>, ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId, ShardLocal<T>) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        self.spawn_named_workers(
            |shard_id| format!("shard-local-worker-{}", shard_id.0),
            make_future,
        )
    }

    /// Starts one named async worker on each shard.
    pub fn spawn_named_workers<MakeName, MakeFuture, Fut>(
        &self,
        mut make_name: MakeName,
        mut make_future: MakeFuture,
    ) -> Result<ShardLocalWorkers<Fut::Output>, ShardedSpawnError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId, ShardLocal<T>) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for slot in &self.shards {
            let shard_id = slot.shard_id;
            let local = ShardLocal::clone(self);
            let handle = self
                .submitter
                .submit_with_handle_named_to(
                    shard_id,
                    make_name(shard_id),
                    make_future(shard_id, local),
                )
                .map(|handle| ShardedJoinHandle::new(shard_id, handle))?;
            handles.push(handle);
        }

        Ok(ShardLocalWorkers { handles })
    }

    /// Starts one stoppable async worker on each shard.
    ///
    /// Every worker receives the same cooperative stop token. Calling
    /// [`StoppableShardLocalWorkers::stop`] wakes workers waiting on that token.
    pub fn spawn_stoppable_workers<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<StoppableShardLocalWorkers<Fut::Output>, ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId, ShardLocal<T>, StopToken) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        self.spawn_named_stoppable_workers(
            |shard_id| format!("shard-local-worker-{}", shard_id.0),
            make_future,
        )
    }

    /// Starts one named stoppable async worker on each shard.
    pub fn spawn_named_stoppable_workers<MakeName, MakeFuture, Fut>(
        &self,
        mut make_name: MakeName,
        mut make_future: MakeFuture,
    ) -> Result<StoppableShardLocalWorkers<Fut::Output>, ShardedSpawnError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId, ShardLocal<T>, StopToken) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let (stop_source, stop_token) = stop_pair();
        let mut handles = Vec::with_capacity(self.shard_count());

        for slot in &self.shards {
            let shard_id = slot.shard_id;
            let local = ShardLocal::clone(self);
            let token = stop_token.clone();
            let handle = self
                .submitter
                .submit_with_handle_named_to(
                    shard_id,
                    make_name(shard_id),
                    make_future(shard_id, local, token),
                )
                .map(|handle| ShardedJoinHandle::new(shard_id, handle))?;
            handles.push(handle);
        }

        Ok(StoppableShardLocalWorkers {
            stop_source,
            workers: ShardLocalWorkers { handles },
        })
    }

    /// Runs one operation per shard and collects shard-tagged outputs.
    pub async fn map_all<R, F>(
        &self,
        operation: F,
    ) -> Result<Vec<(ShardId, R)>, ShardedOperationError>
    where
        R: Send + 'static,
        F: FnMut(ShardId, &mut T) -> R + Send + Clone + 'static,
    {
        let handles = self
            .with_all(operation)
            .map_err(ShardedOperationError::Submit)?;
        join_all_shards(handles)
            .await
            .map_err(ShardedOperationError::Join)
    }

    /// Runs one operation per shard and reduces the shard-tagged outputs into
    /// one value.
    pub async fn map_reduce_all<R, F, Acc, Reduce>(
        &self,
        operation: F,
        mut initial: Acc,
        mut reduce: Reduce,
    ) -> Result<Acc, ShardedOperationError>
    where
        R: Send + 'static,
        F: FnMut(ShardId, &mut T) -> R + Send + Clone + 'static,
        Reduce: FnMut(Acc, ShardId, R) -> Acc,
    {
        let outputs = join_all_shards(
            self.with_all(operation)
                .map_err(ShardedOperationError::Submit)?,
        )
        .await
        .map_err(ShardedOperationError::Join)?;

        for (shard_id, output) in outputs {
            initial = reduce(initial, shard_id, output);
        }

        Ok(initial)
    }

    fn cell_for(&self, shard_id: ShardId) -> Result<&ShardLocalSlot<T>, ShardedSpawnError> {
        self.shards
            .get(shard_id.0)
            .ok_or(ShardedSpawnError::InvalidShardId(shard_id.0))
    }

    fn cell_for_current(
        &self,
        shard_id: ShardId,
    ) -> Result<&ShardLocalCell<T>, ShardLocalAccessError> {
        self.shards
            .get(shard_id.0)
            .map(|slot| slot.cell.as_ref())
            .ok_or(ShardLocalAccessError::InvalidShardId(shard_id.0))
    }
}

/// Join handle set for one shard-local worker per shard.
#[must_use = "shard-local workers do nothing useful unless joined"]
pub struct ShardLocalWorkers<T> {
    handles: Vec<ShardedJoinHandle<T>>,
}

impl<T> ShardLocalWorkers<T> {
    /// Returns the number of worker handles.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Returns true when there are no worker handles.
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Returns the shard ids represented by this worker set.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.handles.iter().map(ShardedJoinHandle::shard_id)
    }

    /// Consumes this worker set and returns the underlying shard-tagged handles.
    pub fn into_handles(self) -> Vec<ShardedJoinHandle<T>> {
        self.handles
    }

    /// Aborts all still-owned workers and returns how many were newly aborted.
    pub fn abort_all(&self) -> usize {
        self.handles.iter().filter(|handle| handle.abort()).count()
    }

    /// Waits for every worker and returns shard-tagged outputs in shard order.
    pub async fn join(self) -> Result<Vec<(ShardId, T)>, ShardedJoinError> {
        join_all_shards(self.handles).await
    }

    /// Waits up to `duration` for all workers to finish, aborting still-owned
    /// workers if the deadline elapses.
    pub async fn join_timeout(
        self,
        duration: Duration,
    ) -> Result<Vec<(ShardId, T)>, ShardLocalWorkerTimeoutError> {
        join_all_shards_timeout(self.handles, duration)
            .await
            .map_err(ShardLocalWorkerTimeoutError::from)
    }

    /// Waits for every worker and reduces the shard-tagged outputs into one
    /// value.
    pub async fn map_reduce<Acc, Reduce>(
        self,
        mut initial: Acc,
        mut reduce: Reduce,
    ) -> Result<Acc, ShardedJoinError>
    where
        Reduce: FnMut(Acc, ShardId, T) -> Acc,
    {
        for (shard_id, output) in self.join().await? {
            initial = reduce(initial, shard_id, output);
        }

        Ok(initial)
    }

    /// Waits up to `duration` for every worker and reduces the shard-tagged
    /// outputs into one value.
    pub async fn map_reduce_timeout<Acc, Reduce>(
        self,
        duration: Duration,
        mut initial: Acc,
        mut reduce: Reduce,
    ) -> Result<Acc, ShardLocalWorkerTimeoutError>
    where
        Reduce: FnMut(Acc, ShardId, T) -> Acc,
    {
        for (shard_id, output) in self.join_timeout(duration).await? {
            initial = reduce(initial, shard_id, output);
        }

        Ok(initial)
    }
}

impl<T> fmt::Debug for ShardLocalWorkers<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardLocalWorkers")
            .field("len", &self.handles.len())
            .field(
                "shard_ids",
                &self
                    .handles
                    .iter()
                    .map(ShardedJoinHandle::shard_id)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Error returned when a shard-local worker set fails or times out.
#[derive(Debug)]
pub enum ShardLocalWorkerTimeoutError {
    /// A worker failed while being joined.
    Join(ShardedJoinError),
    /// The timeout elapsed and still-owned workers were aborted.
    TimedOut {
        /// Shard whose worker timed out.
        shard_id: ShardId,
    },
}

impl ShardLocalWorkerTimeoutError {
    /// Returns the shard whose worker failed or timed out.
    pub fn shard_id(&self) -> ShardId {
        match self {
            ShardLocalWorkerTimeoutError::Join(error) => error.shard_id(),
            ShardLocalWorkerTimeoutError::TimedOut { shard_id } => *shard_id,
        }
    }

    /// Returns true if shutdown timed out.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, ShardLocalWorkerTimeoutError::TimedOut { .. })
    }

    /// Returns true if a worker failed while being joined.
    pub fn is_join_error(&self) -> bool {
        matches!(self, ShardLocalWorkerTimeoutError::Join(_))
    }
}

impl From<ShardedJoinTimeoutError> for ShardLocalWorkerTimeoutError {
    fn from(error: ShardedJoinTimeoutError) -> Self {
        match error {
            ShardedJoinTimeoutError::Join(error) => Self::Join(error),
            ShardedJoinTimeoutError::TimedOut { shard_id } => Self::TimedOut { shard_id },
        }
    }
}

impl fmt::Display for ShardLocalWorkerTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardLocalWorkerTimeoutError::Join(error) => write!(f, "{error}"),
            ShardLocalWorkerTimeoutError::TimedOut { shard_id } => {
                write!(f, "shard-local worker on shard {} timed out", shard_id.0)
            }
        }
    }
}

impl std::error::Error for ShardLocalWorkerTimeoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardLocalWorkerTimeoutError::Join(error) => Some(error),
            ShardLocalWorkerTimeoutError::TimedOut { .. } => None,
        }
    }
}

/// Stoppable join handle set for one shard-local worker per shard.
#[must_use = "stoppable shard-local workers do nothing useful unless stopped or joined"]
pub struct StoppableShardLocalWorkers<T> {
    stop_source: StopSource,
    workers: ShardLocalWorkers<T>,
}

impl<T> StoppableShardLocalWorkers<T> {
    /// Returns the number of worker handles.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns true when there are no worker handles.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Returns true if cooperative stop has already been requested.
    pub fn is_stopped(&self) -> bool {
        self.stop_source.is_stopped()
    }

    /// Requests cooperative stop for all workers.
    pub fn stop(&self) -> bool {
        self.stop_source.stop()
    }

    /// Returns the shard ids represented by this worker set.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.workers.shard_ids()
    }

    /// Consumes this value and returns the non-stoppable worker join set.
    pub fn into_workers(self) -> ShardLocalWorkers<T> {
        self.workers
    }

    /// Waits for every worker and returns shard-tagged outputs in shard order.
    pub async fn join(self) -> Result<Vec<(ShardId, T)>, ShardedJoinError> {
        self.workers.join().await
    }

    /// Requests cooperative stop, then waits for every worker.
    pub async fn stop_and_join(self) -> Result<Vec<(ShardId, T)>, ShardedJoinError> {
        self.stop();
        self.join().await
    }

    /// Requests cooperative stop, then waits up to `duration` for every worker.
    pub async fn stop_and_join_timeout(
        self,
        duration: Duration,
    ) -> Result<Vec<(ShardId, T)>, ShardLocalWorkerTimeoutError> {
        self.stop();
        self.workers.join_timeout(duration).await
    }

    /// Waits for every worker and reduces the shard-tagged outputs into one
    /// value.
    pub async fn map_reduce<Acc, Reduce>(
        self,
        initial: Acc,
        reduce: Reduce,
    ) -> Result<Acc, ShardedJoinError>
    where
        Reduce: FnMut(Acc, ShardId, T) -> Acc,
    {
        self.workers.map_reduce(initial, reduce).await
    }

    /// Requests cooperative stop, then reduces all worker outputs.
    pub async fn stop_and_map_reduce<Acc, Reduce>(
        self,
        initial: Acc,
        reduce: Reduce,
    ) -> Result<Acc, ShardedJoinError>
    where
        Reduce: FnMut(Acc, ShardId, T) -> Acc,
    {
        self.stop();
        self.map_reduce(initial, reduce).await
    }

    /// Requests cooperative stop, then waits up to `duration` and reduces all
    /// worker outputs.
    pub async fn stop_and_map_reduce_timeout<Acc, Reduce>(
        self,
        duration: Duration,
        initial: Acc,
        reduce: Reduce,
    ) -> Result<Acc, ShardLocalWorkerTimeoutError>
    where
        Reduce: FnMut(Acc, ShardId, T) -> Acc,
    {
        self.stop();
        self.workers
            .map_reduce_timeout(duration, initial, reduce)
            .await
    }
}

impl<T> fmt::Debug for StoppableShardLocalWorkers<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoppableShardLocalWorkers")
            .field("stopped", &self.is_stopped())
            .field("workers", &self.workers)
            .finish()
    }
}

impl<T> fmt::Debug for ShardLocal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardLocal")
            .field("shard_count", &self.shards.len())
            .finish_non_exhaustive()
    }
}

/// Error returned when shard-local state cannot be accessed directly from the
/// current thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLocalAccessError {
    /// The caller is not currently running on a sharded executor thread.
    NotOnShard,
    /// The current shard id is outside this shard-local handle's shard set.
    InvalidShardId(usize),
}

impl fmt::Display for ShardLocalAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardLocalAccessError::NotOnShard => {
                write!(f, "not running on a sharded executor thread")
            }
            ShardLocalAccessError::InvalidShardId(shard_id) => {
                write!(f, "invalid shard id: {shard_id}")
            }
        }
    }
}

impl std::error::Error for ShardLocalAccessError {}

impl<T> ShardLocalCell<T> {
    fn with_mut<R, F>(&self, operation: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        assert_eq!(
            current_executor_shard(),
            Some(self.owner),
            "shard-local value accessed from the wrong executor shard"
        );

        // Safety: access is routed to the owner shard by ShardLocal. The
        // closure is synchronous, so `&mut T` cannot be held across an await.
        operation(unsafe { &mut *self.value.get() })
    }
}

#[cfg(test)]
mod tests {
    use super::{ShardLocal, ShardLocalAccessError};
    use crate::ShardId;
    use crate::executor::{block_on, sleep};
    use crate::sharded_executor::{ShardedExecutor, join_all_shards};
    use std::time::Duration;

    #[test]
    fn shard_local_values_are_accessed_on_owning_shards() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

        let handles = local
            .with_all(|shard_id, value| {
                *value += 10;
                (shard_id, *value)
            })
            .unwrap();
        let outputs = block_on(join_all_shards(handles)).unwrap();

        assert_eq!(
            outputs,
            vec![
                (ShardId(0), (ShardId(0), 10)),
                (ShardId(1), (ShardId(1), 11)),
                (ShardId(2), (ShardId(2), 12))
            ]
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn shard_local_rejects_invalid_shard() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let error = local
            .with_on(ShardId(3), |value| *value)
            .expect_err("invalid shard should fail");

        assert_eq!(
            error,
            crate::sharded_executor::ShardedSpawnError::InvalidShardId(3)
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn shard_local_map_reduce_runs_on_owning_shards() {
        let runtime = ShardedExecutor::start(4).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

        let total = block_on(local.map_reduce_all(
            |shard_id, value| {
                *value += 1;
                shard_id.0 + *value
            },
            0usize,
            |sum, _shard_id, value| sum + value,
        ))
        .unwrap();

        assert_eq!(total, 16);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn cloned_shard_local_handle_shares_shard_values() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);
        let task_local = local.clone();

        let remote_total = block_on(
            submitter
                .submit_with_handle_to(ShardId(0), async move {
                    task_local
                        .map_reduce_all(
                            |_shard_id, value| {
                                *value += 5;
                                *value
                            },
                            0usize,
                            |sum, _shard_id, value| sum + value,
                        )
                        .await
                        .unwrap()
                })
                .unwrap(),
        )
        .unwrap();

        assert_eq!(remote_total, 10);

        let values = block_on(local.map_all(|_shard_id, value| *value)).unwrap();
        assert_eq!(values, vec![(ShardId(0), 5), (ShardId(1), 5)]);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn with_current_accesses_current_shard_without_submission() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);
        let task_local = local.clone();

        let output = block_on(
            submitter
                .submit_with_handle_to(ShardId(2), async move {
                    task_local
                        .with_current(|shard_id, value| {
                            *value += 11;
                            (shard_id, *value)
                        })
                        .unwrap()
                })
                .unwrap(),
        )
        .unwrap();

        assert_eq!(output, (ShardId(2), (ShardId(2), 11)));

        let values = block_on(local.map_all(|_shard_id, value| *value)).unwrap();
        assert_eq!(
            values,
            vec![(ShardId(0), 0), (ShardId(1), 0), (ShardId(2), 11)]
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn with_current_rejects_non_shard_thread() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let error = local
            .with_current(|_shard_id, value| {
                *value += 1;
                *value
            })
            .expect_err("non-shard caller should fail");

        assert_eq!(error, ShardLocalAccessError::NotOnShard);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn shard_local_workers_run_on_each_owning_shard() {
        let runtime = ShardedExecutor::start(4).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

        let workers = local
            .spawn_workers(|expected_shard, task_local| async move {
                task_local
                    .with_current(|current_shard, value| {
                        assert_eq!(current_shard, expected_shard);
                        *value += 100;
                        *value
                    })
                    .unwrap()
            })
            .unwrap();

        assert_eq!(workers.len(), 4);
        assert_eq!(
            workers.shard_ids().collect::<Vec<_>>(),
            vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)]
        );

        let outputs = block_on(workers.join()).unwrap();
        assert_eq!(
            outputs,
            vec![
                (ShardId(0), (ShardId(0), 100)),
                (ShardId(1), (ShardId(1), 101)),
                (ShardId(2), (ShardId(2), 102)),
                (ShardId(3), (ShardId(3), 103))
            ]
        );

        let values = block_on(local.map_all(|_shard_id, value| *value)).unwrap();
        assert_eq!(
            values,
            vec![
                (ShardId(0), 100),
                (ShardId(1), 101),
                (ShardId(2), 102),
                (ShardId(3), 103)
            ]
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn shard_local_workers_can_reduce_outputs() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0 + 1);

        let workers = local
            .spawn_workers(|_expected_shard, task_local| async move {
                task_local
                    .with_current(|_current_shard, value| {
                        *value *= 10;
                        *value
                    })
                    .unwrap()
            })
            .unwrap();

        let total = block_on(workers.map_reduce(0usize, |sum, _shard_id, output| {
            let (_current_shard, value) = output;
            sum + value
        }))
        .unwrap();

        assert_eq!(total, 60);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn stoppable_shard_local_workers_stop_and_join() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let workers = local
            .spawn_stoppable_workers(|_expected_shard, task_local, stop| async move {
                let (_current_shard, value) = task_local
                    .with_current(|_current_shard, value| {
                        *value += 1;
                        *value
                    })
                    .unwrap();
                stop.await;
                value
            })
            .unwrap();

        assert_eq!(workers.len(), 3);
        assert!(!workers.is_stopped());
        assert!(workers.stop());
        assert!(!workers.stop());
        assert!(workers.is_stopped());

        let outputs = block_on(workers.join()).unwrap();
        assert_eq!(
            outputs,
            vec![(ShardId(0), 1), (ShardId(1), 1), (ShardId(2), 1)]
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn stoppable_shard_local_workers_can_stop_and_reduce() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let workers = local
            .spawn_stoppable_workers(|_expected_shard, task_local, stop| async move {
                let (_current_shard, value) = task_local
                    .with_current(|_current_shard, value| {
                        *value += 5;
                        *value
                    })
                    .unwrap();
                stop.await;
                value
            })
            .unwrap();

        let total =
            block_on(workers.stop_and_map_reduce(0usize, |sum, _shard_id, value| sum + value))
                .unwrap();

        assert_eq!(total, 10);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn stoppable_shard_local_workers_can_stop_and_join_with_timeout() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let workers = local
            .spawn_stoppable_workers(|_expected_shard, task_local, stop| async move {
                let (_current_shard, value) = task_local
                    .with_current(|_current_shard, value| {
                        *value += 2;
                        *value
                    })
                    .unwrap();
                stop.await;
                value
            })
            .unwrap();

        let outputs = block_on(workers.stop_and_join_timeout(Duration::from_secs(1))).unwrap();
        assert_eq!(outputs, vec![(ShardId(0), 2), (ShardId(1), 2)]);

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn stoppable_shard_local_worker_timeout_aborts_uncooperative_workers() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let workers = local
            .spawn_stoppable_workers(|_expected_shard, _task_local, _stop| async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                }
            })
            .unwrap();

        let error = block_on(workers.stop_and_join_timeout(Duration::from_millis(5)))
            .expect_err("uncooperative workers should time out");

        assert!(error.is_timed_out());
        assert!(matches!(error.shard_id(), ShardId(0) | ShardId(1)));

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn named_shard_local_workers_are_observable() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let observer = runtime.observer();
        let submitter = runtime.submitter();
        let local = ShardLocal::new(submitter.clone(), |_| 0usize);

        let workers = local
            .spawn_named_workers(
                |shard_id| format!("local-worker-{}", shard_id.0),
                |_shard_id, task_local| async move {
                    let output = task_local
                        .with_current(|current_shard, value| {
                            *value += 1;
                            (current_shard, *value)
                        })
                        .unwrap();
                    sleep(Duration::from_millis(50)).await;
                    output
                },
            )
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));
        let snapshot = observer.snapshot();
        let mut task_names = snapshot
            .shards
            .iter()
            .flat_map(|shard| shard.executor.iter())
            .flat_map(|executor| executor.tasks.iter())
            .filter_map(|task| task.name.as_deref())
            .collect::<Vec<_>>();
        task_names.sort_unstable();

        assert_eq!(task_names, vec!["local-worker-0", "local-worker-1"]);

        let outputs = block_on(workers.join()).unwrap();
        assert_eq!(
            outputs,
            vec![
                (ShardId(0), (ShardId(0), (ShardId(0), 1))),
                (ShardId(1), (ShardId(1), (ShardId(1), 1)))
            ]
        );

        drop(local);
        drop(submitter);
        runtime.stop().unwrap();
    }
}
