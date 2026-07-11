//! Key-to-shard placement strategies.
//!
//! The [`crate::placement::Placement`] trait lets callers provide custom routing from keys
//! to shards. [`crate::placement::HashPlacement`] is the default strategy, using the
//! standard library's hasher. The free function [`crate::placement::shard_for_hash`] provides
//! the same hash-based mapping without the trait.

use alloc::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::ShardId;

/// A key-to-shard placement strategy.
///
/// Placement implementations should return a shard ID inside
/// `0..shard_count`. Callers must pass a non-zero shard count.
pub trait Placement<K: ?Sized> {
    /// Returns the shard that should own `key`.
    fn shard_for(&self, key: &K, shard_count: usize) -> ShardId;
}

/// Default hash-based placement strategy.
///
/// This is a simple first-milestone placement strategy, not a stable
/// consistent-hashing contract.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HashPlacement;

impl<K: Hash + ?Sized> Placement<K> for HashPlacement {
    fn shard_for(&self, key: &K, shard_count: usize) -> ShardId {
        shard_for_hash(key, shard_count)
    }
}

/// Routes a hashable key to a shard using Rust's default hasher.
///
/// This is a simple first-milestone placement strategy, not a stable
/// consistent-hashing contract. Callers must pass a non-zero shard count.
pub fn shard_for_hash<K: Hash + ?Sized>(key: &K, shard_count: usize) -> ShardId {
    debug_assert!(shard_count > 0);

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);

    ShardId((hasher.finish() as usize) % shard_count)
}
