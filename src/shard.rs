//! Shard identifiers and snapshot types.
//!
//! [`ShardId`] is a newtype index identifying a shard within a running
//! service. [`ShardSnapshot`] is an owned point-in-time summary of one
//! shard, used by the observability layer.

/// Identifier for a shard within a running sharded service.
///
/// `ShardId(0)` identifies the first shard.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub usize);

/// A point-in-time, owned summary of one shard.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardSnapshot {
    /// The shard this snapshot describes.
    pub shard_id: ShardId,
    /// Number of keys stored on the shard when the snapshot command ran.
    pub len: usize,
}
