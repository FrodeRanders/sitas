//! Shard-per-thread async executor runtime.
//!
//! This module is the first bridge between the single-threaded async executor
//! and the project's shard-local service model. Each shard owns one executor
//! running on one OS thread. Callers place work explicitly with [`ShardId`],
//! and spawned tasks stay on that shard for their whole lifetime.

use std::fmt;
use std::future::Future;
use std::sync::mpsc;
use std::thread;

use crate::error::ShardError;
use crate::executor::{JoinHandle, SpawnError, Spawner, executor_and_spawner};
use crate::runtime::join_all;
use crate::shard::ShardId;

thread_local! {
    static CURRENT_EXECUTOR_SHARD: std::cell::Cell<Option<ShardId>> = const { std::cell::Cell::new(None) };
}

/// Returns the shard currently polling this task, if the caller is running on a
/// [`ShardedExecutor`] shard thread.
pub fn current_executor_shard() -> Option<ShardId> {
    CURRENT_EXECUTOR_SHARD.with(std::cell::Cell::get)
}

/// A small shard-per-thread async runtime.
///
/// Each shard owns one [`crate::executor::Executor`] and one OS thread. Work is
/// submitted to an explicit shard with [`ShardedExecutor::spawn_on`] or
/// [`ShardedExecutor::spawn_with_handle_on`]. Dropping or stopping the runtime
/// drops the last owned spawners, allowing idle executor threads to drain and
/// exit.
#[must_use = "dropping the sharded executor stops all owned shard threads"]
pub struct ShardedExecutor {
    shards: Vec<AsyncShard>,
    joins: Vec<thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct AsyncShard {
    shard_id: ShardId,
    spawner: Option<Spawner>,
}

impl ShardedExecutor {
    /// Starts `shard_count` async executor shards.
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        if shard_count == 0 {
            return Err(ShardError::InvalidShardCount);
        }

        let mut shards = Vec::with_capacity(shard_count);
        let mut joins = Vec::with_capacity(shard_count);

        for shard_idx in 0..shard_count {
            let shard_id = ShardId(shard_idx);
            let (executor, spawner) = executor_and_spawner();
            let (started_sender, started_receiver) = mpsc::sync_channel(1);

            let join = thread::spawn(move || {
                CURRENT_EXECUTOR_SHARD.with(|current| current.set(Some(shard_id)));
                let _ = started_sender.send(());
                executor.run();
                CURRENT_EXECUTOR_SHARD.with(|current| current.set(None));
            });

            started_receiver
                .recv()
                .map_err(|_| ShardError::ThreadJoinFailed)?;

            shards.push(AsyncShard {
                shard_id,
                spawner: Some(spawner),
            });
            joins.push(join);
        }

        Ok(Self { shards, joins })
    }

    /// Returns the number of async executor shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Spawns a task onto a specific executor shard.
    pub fn spawn_on<F>(&self, shard_id: ShardId, future: F) -> Result<(), ShardedSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Spawns a task onto a specific executor shard and returns an awaitable
    /// handle for its output.
    pub fn spawn_with_handle_on<F>(
        &self,
        shard_id: ShardId,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ShardedSpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_with_handle(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Stops all owned shard executors and joins their threads.
    pub fn stop(mut self) -> Result<(), ShardError> {
        self.shutdown()
    }

    /// Stops all owned shard executors while keeping the runtime handle
    /// inspectable.
    pub fn shutdown(&mut self) -> Result<(), ShardError> {
        for shard in &mut self.shards {
            shard.spawner.take();
        }

        join_all(self.joins.drain(..).collect())
    }

    fn spawner_for(&self, shard_id: ShardId) -> Result<&Spawner, ShardedSpawnError> {
        let shard = self
            .shards
            .get(shard_id.0)
            .ok_or(ShardedSpawnError::InvalidShardId(shard_id.0))?;

        debug_assert_eq!(shard.shard_id, shard_id);
        shard
            .spawner
            .as_ref()
            .ok_or(ShardedSpawnError::Stopped(shard_id))
    }
}

impl fmt::Debug for ShardedExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedExecutor")
            .field("shard_count", &self.shard_count())
            .field("running", &!self.joins.is_empty())
            .finish()
    }
}

impl Drop for ShardedExecutor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Error returned when work cannot be placed on a sharded executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardedSpawnError {
    /// A caller addressed a shard index that does not exist.
    InvalidShardId(usize),
    /// The addressed shard has already stopped.
    Stopped(ShardId),
    /// The addressed shard executor rejected the task.
    Spawn(SpawnError),
}

impl fmt::Display for ShardedSpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardedSpawnError::InvalidShardId(id) => write!(f, "invalid shard id: {id}"),
            ShardedSpawnError::Stopped(shard_id) => {
                write!(f, "executor shard {} has stopped", shard_id.0)
            }
            ShardedSpawnError::Spawn(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ShardedSpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardedSpawnError::Spawn(error) => Some(error),
            ShardedSpawnError::InvalidShardId(_) | ShardedSpawnError::Stopped(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ShardedExecutor, ShardedSpawnError, current_executor_shard};
    use crate::ShardId;
    use crate::executor::block_on;
    use std::sync::mpsc;

    #[test]
    fn start_rejects_zero_shards() {
        assert_eq!(
            ShardedExecutor::start(0).unwrap_err().to_string(),
            "shard count must be greater than zero"
        );
    }

    #[test]
    fn spawn_on_runs_task_on_requested_shard() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let (sender, receiver) = mpsc::sync_channel(3);

        for shard_idx in 0..runtime.shard_count() {
            let sender = sender.clone();
            runtime
                .spawn_on(ShardId(shard_idx), async move {
                    sender.send(current_executor_shard()).unwrap();
                })
                .unwrap();
        }

        drop(sender);

        let mut seen = receiver.into_iter().collect::<Vec<_>>();
        seen.sort_by_key(|shard| shard.map(|id| id.0));

        assert_eq!(
            seen,
            vec![Some(ShardId(0)), Some(ShardId(1)), Some(ShardId(2))]
        );
        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_with_handle_on_returns_task_output() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let handle = runtime
            .spawn_with_handle_on(ShardId(1), async {
                assert_eq!(current_executor_shard(), Some(ShardId(1)));
                42
            })
            .unwrap();

        assert_eq!(block_on(handle).unwrap(), 42);
        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_on_rejects_invalid_shard() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let error = runtime
            .spawn_on(ShardId(7), async {})
            .expect_err("invalid shard should fail");

        assert_eq!(error, ShardedSpawnError::InvalidShardId(7));
        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_on_rejects_stopped_shard() {
        let mut runtime = ShardedExecutor::start(1).unwrap();

        runtime.shutdown().unwrap();
        let error = runtime
            .spawn_on(ShardId(0), async {})
            .expect_err("stopped shard should fail");

        assert_eq!(error, ShardedSpawnError::Stopped(ShardId(0)));
    }
}
