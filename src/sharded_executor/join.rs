use std::fmt;
use std::time::Duration;

use crate::executor::{JoinError, JoinHandle, SpawnError, TimeoutError, timeout};
use crate::shard::ShardId;

/// Join handle tagged with the shard on which the task is running.
#[must_use = "sharded join handles do nothing unless joined"]
pub struct ShardedJoinHandle<T> {
    shard_id: ShardId,
    handle: JoinHandle<T>,
}

impl<T> ShardedJoinHandle<T> {
    pub(crate) fn new(shard_id: ShardId, handle: JoinHandle<T>) -> Self {
        Self { shard_id, handle }
    }

    /// Returns the shard on which this task is running.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Aborts the task if it has not completed yet.
    pub fn abort(&self) -> bool {
        self.handle.abort()
    }

    /// Waits for this task and returns the shard-tagged output.
    pub async fn join(self) -> Result<(ShardId, T), ShardedJoinError> {
        self.handle
            .await
            .map(|output| (self.shard_id, output))
            .map_err(|error| ShardedJoinError {
                shard_id: self.shard_id,
                error,
            })
    }

    /// Waits up to `duration` for this task and aborts it if the timeout
    /// elapses.
    pub async fn join_timeout(
        mut self,
        duration: Duration,
    ) -> Result<(ShardId, T), ShardedJoinTimeoutError> {
        match timeout(duration, &mut self.handle).await {
            Ok(Ok(output)) => Ok((self.shard_id, output)),
            Ok(Err(error)) => Err(ShardedJoinTimeoutError::Join(ShardedJoinError {
                shard_id: self.shard_id,
                error,
            })),
            Err(TimeoutError) => {
                self.handle.abort();
                Err(ShardedJoinTimeoutError::TimedOut {
                    shard_id: self.shard_id,
                })
            }
        }
    }
}

impl<T> fmt::Debug for ShardedJoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedJoinHandle")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

/// Error returned when a shard-tagged join handle fails.
pub struct ShardedJoinError {
    shard_id: ShardId,
    error: JoinError,
}

impl ShardedJoinError {
    /// Returns the shard whose task failed.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Returns the underlying join error.
    pub fn error(&self) -> &JoinError {
        &self.error
    }

    /// Consumes this error and returns the underlying join error.
    pub fn into_error(self) -> JoinError {
        self.error
    }
}

impl fmt::Debug for ShardedJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedJoinError")
            .field("shard_id", &self.shard_id)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ShardedJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "task on shard {} failed: {}",
            self.shard_id.0, self.error
        )
    }
}

impl std::error::Error for ShardedJoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Error returned when a shard-tagged join handle fails or times out.
#[derive(Debug)]
pub enum ShardedJoinTimeoutError {
    /// The task failed while being joined.
    Join(ShardedJoinError),
    /// The timeout elapsed and the task was aborted.
    TimedOut {
        /// Shard whose task timed out.
        shard_id: ShardId,
    },
}

impl ShardedJoinTimeoutError {
    /// Returns the shard whose task failed or timed out.
    pub fn shard_id(&self) -> ShardId {
        match self {
            ShardedJoinTimeoutError::Join(error) => error.shard_id(),
            ShardedJoinTimeoutError::TimedOut { shard_id } => *shard_id,
        }
    }

    /// Returns true if the join timed out.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, ShardedJoinTimeoutError::TimedOut { .. })
    }

    /// Returns true if the task failed while being joined.
    pub fn is_join_error(&self) -> bool {
        matches!(self, ShardedJoinTimeoutError::Join(_))
    }
}

impl fmt::Display for ShardedJoinTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardedJoinTimeoutError::Join(error) => write!(f, "{error}"),
            ShardedJoinTimeoutError::TimedOut { shard_id } => {
                write!(f, "task on shard {} timed out", shard_id.0)
            }
        }
    }
}

impl std::error::Error for ShardedJoinTimeoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardedJoinTimeoutError::Join(error) => Some(error),
            ShardedJoinTimeoutError::TimedOut { .. } => None,
        }
    }
}

/// Error returned by higher-level sharded operations.
#[derive(Debug)]
pub enum ShardedOperationError {
    /// Failed while submitting work to a shard.
    Submit(ShardedSpawnError),
    /// Failed while joining shard work.
    Join(ShardedJoinError),
}

impl fmt::Display for ShardedOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardedOperationError::Submit(error) => write!(f, "{error}"),
            ShardedOperationError::Join(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ShardedOperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardedOperationError::Submit(error) => Some(error),
            ShardedOperationError::Join(error) => Some(error),
        }
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

/// Awaits shard-tagged join handles in input order.
pub async fn join_all_shards<T>(
    handles: Vec<ShardedJoinHandle<T>>,
) -> Result<Vec<(ShardId, T)>, ShardedJoinError> {
    let mut outputs = Vec::with_capacity(handles.len());

    for handle in handles {
        outputs.push(handle.join().await?);
    }

    Ok(outputs)
}
