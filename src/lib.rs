//! `shardstar` is an experiment in Rust-native shard-local service ownership.
//!
//! The project is inspired by Seastar's shard-per-core, shared-nothing model,
//! but this first milestone is not an async runtime. It deliberately uses only
//! the Rust standard library to validate a small architectural kernel:
//! shard-local ownership, bounded mailboxes, reply handles, and typed message
//! passing.
//!
//! Application state is owned by a shard thread. Other threads interact with
//! that state only by sending typed messages to the owning shard. No mutex
//! protects the service state because the service state is not shared.
//! Cross-shard values are owned values, so no references into shard-local state
//! escape the shard.
//!
//! This first milestone deliberately does not include:
//!
//! - async/await
//! - non-blocking I/O
//! - a custom executor
//! - a network server
//! - persistence
//! - CPU pinning
//! - scheduling classes
//! - procedural macro service generation
//!
//! Later milestones may add async I/O, custom executors, CPU pinning,
//! backpressure, and runtime backends.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Sharded counter service implementation.
pub mod counter;
/// Error types returned by shard operations.
pub mod error;
/// Sharded key-value store implementation.
pub mod kv;
/// Key-to-shard placement helpers.
pub mod placement;
/// Standard-library shard runtime primitives.
pub mod runtime;
/// Shard identifiers and shard-level types.
pub mod shard;

pub use counter::{
    CounterReply, CounterShardSnapshot, CounterShardSnapshotsReply, CounterTotalReply,
    ShardedCounter, ShardedCounterConfig,
};
pub use error::ShardError;
pub use kv::{
    KvAllKeysReply, KvReply, KvShardSnapshotsReply, KvTotalLenReply, ShardedKv, ShardedKvConfig,
};
pub use runtime::{RuntimeSnapshot, DEFAULT_MAILBOX_CAPACITY};
pub use shard::{ShardId, ShardSnapshot};
