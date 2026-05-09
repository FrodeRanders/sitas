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
    ShardedJoinHandle, ShardedSpawnError, ShardedSubmitter, current_executor_shard,
};

/// One value per shard, accessed only on the owning shard executor.
#[must_use]
pub struct ShardLocal<T> {
    shards: Vec<ShardLocalSlot<T>>,
    submitter: ShardedSubmitter,
}

struct ShardLocalSlot<T> {
    shard_id: ShardId,
    cell: Arc<ShardLocalCell<T>>,
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

    fn cell_for(&self, shard_id: ShardId) -> Result<&ShardLocalSlot<T>, ShardedSpawnError> {
        self.shards
            .get(shard_id.0)
            .ok_or(ShardedSpawnError::InvalidShardId(shard_id.0))
    }
}

impl<T> fmt::Debug for ShardLocal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardLocal")
            .field("shard_count", &self.shards.len())
            .finish_non_exhaustive()
    }
}

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
    use super::ShardLocal;
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
}
