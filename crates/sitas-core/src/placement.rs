//! Key-to-shard placement strategies.
//!
//! The [`crate::placement::Placement`] trait lets callers provide custom routing from keys
//! to shards. [`crate::placement::HashPlacement`] is the default strategy, using the
//! standard library's hasher. The free function [`crate::placement::shard_for_hash`] provides
//! the same hash-based mapping without the trait.

use core::hash::{Hash, Hasher};

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

/// Runtime-level shard placement request passed to [`crate::shard_runtime::ShardRuntime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPlacement {
    /// Let the backend choose where the shard runs.
    Unpinned,
    /// Place shard `n` on logical processor `n` when the backend supports it.
    Sequential,
}

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

    let mut hasher = FnvHasher::default();
    key.hash(&mut hasher);

    ShardId((hasher.finish() as usize) % shard_count)
}

#[derive(Debug, Clone, Copy)]
struct FnvHasher(u64);

impl Default for FnvHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}
