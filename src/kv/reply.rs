//! Typed reply handles for key-value store operations.
//!
//! Each reply type wraps a [`Reply<T>`] for a specific KV command. They
//! provide `wait`, `wait_timeout`, and `wait_async` methods so callers can
//! consume results both synchronously and through the custom executor.

use std::time::Duration;

use crate::runtime::Reply;
use crate::{ShardError, ShardId, ShardSnapshot};

/// Reply handle for an accepted key-value command.
pub type KvReply<T> = Reply<T>;

/// Reply handle for a total length request across all shards.
///
/// Calling [`KvTotalLenReply::wait`] waits for every shard reply and sums the
/// returned lengths.
#[must_use]
#[derive(Debug)]
pub struct KvTotalLenReply {
    replies: Vec<KvReply<usize>>,
}

impl KvTotalLenReply {
    pub(crate) fn new(replies: Vec<KvReply<usize>>) -> Self {
        Self { replies }
    }

    /// Waits for all shard length replies and returns their sum.
    pub fn wait(self) -> Result<usize, ShardError> {
        self.replies
            .into_iter()
            .try_fold(0usize, |total, reply| Ok(total + reply.wait()?))
    }

    /// Waits for all shard length replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<usize, ShardError> {
        self.replies.into_iter().try_fold(0usize, |total, reply| {
            Ok(total + reply.wait_timeout(timeout)?)
        })
    }

    /// Awaits all shard length replies and returns their sum.
    pub async fn wait_async(self) -> Result<usize, ShardError> {
        let mut total = 0usize;
        for reply in self.replies {
            total += reply.wait_async().await?;
        }
        Ok(total)
    }
}

/// Reply handle for a per-shard snapshot request.
///
/// Calling [`KvShardSnapshotsReply::wait`] waits for every shard reply and
/// returns owned snapshots in shard order.
#[must_use]
#[derive(Debug)]
pub struct KvShardSnapshotsReply {
    replies: Vec<(ShardId, KvReply<usize>)>,
}

impl KvShardSnapshotsReply {
    pub(crate) fn new(replies: Vec<(ShardId, KvReply<usize>)>) -> Self {
        Self { replies }
    }

    /// Waits for all shard snapshot replies.
    pub fn wait(self) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(ShardSnapshot {
                    shard_id,
                    len: reply.wait()?,
                })
            })
            .collect()
    }

    /// Waits for all shard snapshot replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(ShardSnapshot {
                    shard_id,
                    len: reply.wait_timeout(timeout)?,
                })
            })
            .collect()
    }

    /// Awaits all shard snapshot replies.
    pub async fn wait_async(self) -> Result<Vec<ShardSnapshot>, ShardError> {
        let mut snapshots = Vec::with_capacity(self.replies.len());
        for (shard_id, reply) in self.replies {
            snapshots.push(ShardSnapshot {
                shard_id,
                len: reply.wait_async().await?,
            });
        }
        Ok(snapshots)
    }
}

/// Reply handle for an all-keys request across all shards.
///
/// Calling [`KvAllKeysReply::wait`] waits for every shard reply and returns all
/// keys sorted lexicographically.
#[must_use]
#[derive(Debug)]
pub struct KvAllKeysReply {
    replies: Vec<KvReply<Vec<String>>>,
}

impl KvAllKeysReply {
    pub(crate) fn new(replies: Vec<KvReply<Vec<String>>>) -> Self {
        Self { replies }
    }

    /// Waits for all shard key replies and returns sorted owned keys.
    pub fn wait(self) -> Result<Vec<String>, ShardError> {
        let mut keys = self.replies.into_iter().try_fold(
            Vec::new(),
            |mut keys, reply| -> Result<Vec<String>, ShardError> {
                keys.extend(reply.wait()?);
                Ok(keys)
            },
        )?;
        keys.sort();
        Ok(keys)
    }

    /// Waits for all shard key replies until `timeout` expires for one reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<String>, ShardError> {
        let mut keys = self.replies.into_iter().try_fold(
            Vec::new(),
            |mut keys, reply| -> Result<Vec<String>, ShardError> {
                keys.extend(reply.wait_timeout(timeout)?);
                Ok(keys)
            },
        )?;
        keys.sort();
        Ok(keys)
    }

    /// Awaits all shard key replies and returns sorted owned keys.
    pub async fn wait_async(self) -> Result<Vec<String>, ShardError> {
        let mut keys = Vec::new();
        for reply in self.replies {
            keys.extend(reply.wait_async().await?);
        }
        keys.sort();
        Ok(keys)
    }
}

/// Reply handle for a multi-key get request.
///
/// Calling [`KvGetManyReply::wait`] waits for each accepted get command and
/// returns owned key/value pairs in the same order the keys were submitted.
#[must_use]
#[derive(Debug)]
pub struct KvGetManyReply {
    replies: Vec<(String, KvReply<Option<String>>)>,
}

impl KvGetManyReply {
    pub(crate) fn new(replies: Vec<(String, KvReply<Option<String>>)>) -> Self {
        Self { replies }
    }

    /// Waits for all key replies and returns results in input order.
    pub fn wait(self) -> Result<Vec<(String, Option<String>)>, ShardError> {
        self.replies
            .into_iter()
            .map(|(key, reply)| Ok((key, reply.wait()?)))
            .collect()
    }

    /// Waits for all key replies until `timeout` expires for one reply.
    ///
    /// The timeout is applied per pending key reply.
    pub fn wait_timeout(
        self,
        timeout: Duration,
    ) -> Result<Vec<(String, Option<String>)>, ShardError> {
        self.replies
            .into_iter()
            .map(|(key, reply)| Ok((key, reply.wait_timeout(timeout)?)))
            .collect()
    }

    /// Awaits all key replies and returns results in input order.
    pub async fn wait_async(self) -> Result<Vec<(String, Option<String>)>, ShardError> {
        let mut values = Vec::with_capacity(self.replies.len());
        for (key, reply) in self.replies {
            values.push((key, reply.wait_async().await?));
        }
        Ok(values)
    }
}

/// Reply handle for a multi-key delete request.
///
/// Calling [`KvDeleteManyReply::wait`] waits for each accepted delete command
/// and returns owned key/previous-value pairs in the same order the keys were
/// submitted.
#[must_use]
#[derive(Debug)]
pub struct KvDeleteManyReply {
    replies: Vec<(String, KvReply<Option<String>>)>,
}

impl KvDeleteManyReply {
    pub(crate) fn new(replies: Vec<(String, KvReply<Option<String>>)>) -> Self {
        Self { replies }
    }

    /// Waits for all delete replies and returns results in input order.
    pub fn wait(self) -> Result<Vec<(String, Option<String>)>, ShardError> {
        self.replies
            .into_iter()
            .map(|(key, reply)| Ok((key, reply.wait()?)))
            .collect()
    }

    /// Waits for all delete replies until `timeout` expires for one reply.
    ///
    /// The timeout is applied per pending delete reply.
    pub fn wait_timeout(
        self,
        timeout: Duration,
    ) -> Result<Vec<(String, Option<String>)>, ShardError> {
        self.replies
            .into_iter()
            .map(|(key, reply)| Ok((key, reply.wait_timeout(timeout)?)))
            .collect()
    }

    /// Awaits all delete replies and returns results in input order.
    pub async fn wait_async(self) -> Result<Vec<(String, Option<String>)>, ShardError> {
        let mut values = Vec::with_capacity(self.replies.len());
        for (key, reply) in self.replies {
            values.push((key, reply.wait_async().await?));
        }
        Ok(values)
    }
}
