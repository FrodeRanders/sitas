use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::ShardId;

/// Routes a hashable key to a shard using Rust's default hasher.
///
/// This is a simple first-milestone placement strategy, not a stable
/// consistent-hashing contract. Callers must pass a non-zero shard count.
pub fn shard_for_hash<K: Hash>(key: &K, shard_count: usize) -> ShardId {
    debug_assert!(shard_count > 0);

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);

    ShardId((hasher.finish() as usize) % shard_count)
}
