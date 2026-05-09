//! Shard-local state for the shard-per-thread async runtime.
//!
//! A `ShardLocal<T>` owns one `T` per shard. Access happens by submitting a
//! closure to the owning shard executor. The closure receives `&mut T` while it
//! is being polled on that shard, and no reference to `T` can escape the
//! closure.

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;

use crate::shard::ShardId;
use crate::sharded_executor::{
    ShardedJoinHandle, ShardedOperationError, ShardedSpawnError, ShardedSubmitter,
    current_executor_shard, join_all_shards,
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
    use crate::executor::block_on;
    use crate::sharded_executor::{ShardedExecutor, join_all_shards};

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
}
