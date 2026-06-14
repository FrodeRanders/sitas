//! Async-service bridge for the shard-per-thread model.
//!
//! This module provides adapters that wrap std-layer sharded services
//! ([`ShardedKv`](crate::ShardedKv), [`ShardedCounter`](crate::ShardedCounter))
//! for use inside async tasks running on a [`ShardedExecutor`](crate::ShardedExecutor).
//!
//! The key insight is that [`Reply::wait_async`](crate::runtime::Reply::wait_async) already
//! integrates with the custom executor's waker. This module provides ergonomic
//! async wrappers that call `submit_*` and then `wait_async().await` in a single
//! async method.
//!
//! # Example
//!
//! ```ignore
//! use sitas::{ShardedKv, AsyncShardedKv};
//!
//! let kv = ShardedKv::start(4).unwrap();
//! let async_kv = AsyncShardedKv::new(&kv);
//!
//! // Inside an async task on a ShardedExecutor:
//! let value = async_kv.get("my-key").await.unwrap();
//! ```

use crate::ShardError;
use crate::kv::ShardedKv;

/// Async wrapper around a [`ShardedKv`] reference.
///
/// All methods are `&self` and return futures that wait for shard replies
/// through the custom executor's waker system.
///
/// The `'a` lifetime ties this wrapper to the underlying [`ShardedKv`].
/// For an owned variant, wrap the kv in an `Arc` and use [`OwnedAsyncShardedKv`].
pub struct AsyncShardedKv<'a> {
    kv: &'a ShardedKv,
}

impl<'a> AsyncShardedKv<'a> {
    /// Creates an async wrapper around a shared reference to a kv store.
    pub fn new(kv: &'a ShardedKv) -> Self {
        Self { kv }
    }

    /// Returns the underlying kv reference.
    pub fn inner(&self) -> &ShardedKv {
        self.kv
    }

    /// Gets the value for a key.
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.kv.submit_get(key)?.wait_async().await
    }

    /// Inserts or replaces a key-value pair.
    pub async fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ShardError> {
        self.kv.submit_put(key, value)?.wait_async().await
    }

    /// Atomically replaces a key only if its current value matches `expected`.
    pub async fn compare_and_put(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<bool, ShardError> {
        self.kv
            .submit_compare_and_put(key, expected, value)?
            .wait_async()
            .await
    }

    /// Returns the existing value for `key`, or inserts and returns `value`.
    pub async fn get_or_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<String, ShardError> {
        self.kv.submit_get_or_put(key, value)?.wait_async().await
    }

    /// Deletes a key and returns the previous value.
    pub async fn delete(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.kv.submit_delete(key)?.wait_async().await
    }

    /// Gets multiple key values in input order.
    pub async fn get_many<I, K>(&self, keys: I) -> Result<Vec<(String, Option<String>)>, ShardError>
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        self.kv.submit_get_many(keys)?.wait_async().await
    }

    /// Deletes multiple keys and returns previous values in input order.
    pub async fn delete_many<I, K>(
        &self,
        keys: I,
    ) -> Result<Vec<(String, Option<String>)>, ShardError>
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        self.kv.submit_delete_many(keys)?.wait_async().await
    }

    /// Returns the total number of keys across all shards.
    pub async fn total_len(&self) -> Result<usize, ShardError> {
        self.kv.submit_total_len()?.wait_async().await
    }

    /// Returns owned per-shard snapshots in shard order.
    pub async fn shard_snapshots(&self) -> Result<Vec<crate::ShardSnapshot>, ShardError> {
        self.kv.submit_shard_snapshots()?.wait_async().await
    }

    /// Returns all keys sorted lexicographically.
    pub async fn all_keys(&self) -> Result<Vec<String>, ShardError> {
        self.kv.submit_all_keys()?.wait_async().await
    }

    /// Returns the number of keys on a specific shard.
    pub async fn len_on_shard(&self, shard_id: crate::ShardId) -> Result<usize, ShardError> {
        self.kv.submit_len_on_shard(shard_id)?.wait_async().await
    }

    /// Returns sorted owned keys from a specific shard.
    pub async fn keys_on_shard(&self, shard_id: crate::ShardId) -> Result<Vec<String>, ShardError> {
        self.kv.submit_keys_on_shard(shard_id)?.wait_async().await
    }
}

impl<'a> std::fmt::Debug for AsyncShardedKv<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncShardedKv")
            .field("shard_count", &self.kv.shard_count())
            .finish_non_exhaustive()
    }
}

/// Owned async wrapper around a [`ShardedKv`].
///
/// Unlike [`AsyncShardedKv`], this owns the kv store and manages its lifecycle.
/// Dropping this handle stops and joins all shard threads.
pub struct OwnedAsyncShardedKv {
    kv: Option<ShardedKv>,
}

impl OwnedAsyncShardedKv {
    /// Starts an owned async kv store.
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        let kv = ShardedKv::start(shard_count)?;
        Ok(Self { kv: Some(kv) })
    }

    /// Returns a borrowed async wrapper.
    pub fn as_async(&self) -> AsyncShardedKv<'_> {
        AsyncShardedKv::new(
            self.kv
                .as_ref()
                .expect("OwnedAsyncShardedKv already consumed"),
        )
    }

    /// Consumes this wrapper and returns the underlying store.
    pub fn into_inner(mut self) -> ShardedKv {
        self.kv
            .take()
            .expect("OwnedAsyncShardedKv already consumed")
    }

    /// Returns the underlying kv reference.
    pub fn inner(&self) -> &ShardedKv {
        self.kv
            .as_ref()
            .expect("OwnedAsyncShardedKv already consumed")
    }

    /// Gets the value for a key.
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.as_async().get(key).await
    }

    /// Inserts or replaces a key-value pair.
    pub async fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ShardError> {
        self.as_async().put(key, value).await
    }

    /// Deletes a key and returns the previous value.
    pub async fn delete(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.as_async().delete(key).await
    }

    /// Returns the total number of keys across all shards.
    pub async fn total_len(&self) -> Result<usize, ShardError> {
        self.as_async().total_len().await
    }
}

impl std::fmt::Debug for OwnedAsyncShardedKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedAsyncShardedKv")
            .field("shard_count", &self.kv.as_ref().map(|kv| kv.shard_count()))
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedAsyncShardedKv {
    fn drop(&mut self) {
        if let Some(ref mut kv) = self.kv {
            let _ = kv.shutdown();
        }
    }
}

/// Async wrapper around a [`ShardedCounter`](crate::ShardedCounter) reference.
pub struct AsyncShardedCounter<'a> {
    counter: &'a crate::counter::ShardedCounter,
}

impl<'a> AsyncShardedCounter<'a> {
    /// Creates an async wrapper around a shared reference to a counter.
    pub fn new(counter: &'a crate::counter::ShardedCounter) -> Self {
        Self { counter }
    }

    /// Returns the underlying counter reference.
    pub fn inner(&self) -> &crate::counter::ShardedCounter {
        self.counter
    }

    /// Adds `delta` to a specific shard counter and returns the new value.
    pub async fn add_on_shard(
        &self,
        shard_id: crate::ShardId,
        delta: i64,
    ) -> Result<i64, ShardError> {
        self.counter
            .submit_add_on_shard(shard_id, delta)?
            .wait_async()
            .await
    }

    /// Returns the current value on a specific shard.
    pub async fn get_on_shard(&self, shard_id: crate::ShardId) -> Result<i64, ShardError> {
        self.counter
            .submit_get_on_shard(shard_id)?
            .wait_async()
            .await
    }

    /// Returns the sum of all shard counters.
    pub async fn total(&self) -> Result<i64, ShardError> {
        self.counter.submit_total()?.wait_async().await
    }

    /// Returns owned per-shard counter snapshots.
    pub async fn shard_snapshots(
        &self,
    ) -> Result<Vec<crate::counter::CounterShardSnapshot>, ShardError> {
        self.counter.submit_shard_snapshots()?.wait_async().await
    }
}

impl<'a> std::fmt::Debug for AsyncShardedCounter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncShardedCounter")
            .field("shard_count", &self.counter.shard_count())
            .finish_non_exhaustive()
    }
}
