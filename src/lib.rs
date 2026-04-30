//! `sitas` is an experiment in Rust-native shard-local service ownership.
//!
//! The project is inspired by Seastar's shard-per-core, shared-nothing model,
//! but this first milestone deliberately uses only the Rust standard library to
//! validate small architectural kernels: shard-local ownership, bounded
//! mailboxes, blocking and awaitable reply handles, typed message passing, and a
//! minimal executor experiment.
//!
//! Application state is owned by a shard thread. Other threads interact with
//! that state only by sending typed messages to the owning shard. No mutex
//! protects the service state because the service state is not shared.
//! Cross-shard values are owned values, so no references into shard-local state
//! escape the shard.
//!
//! The baseline std-only milestone deliberately does not include:
//!
//! - non-blocking I/O
//! - a network server
//! - persistence
//! - CPU pinning
//! - scheduling classes
//! - procedural macro service generation
//!
//! The `non-std-runtime` branch starts introducing small Unix runtime backend
//! pieces directly through OS syscalls.

#![warn(missing_docs)]
#![warn(unsafe_op_in_unsafe_fn)]

/// Sharded counter service implementation.
pub mod counter;
/// Error types returned by shard operations.
pub mod error;
/// Minimal standard-library async executor experiment.
pub mod executor;
/// Sharded key-value store implementation.
pub mod kv;
/// Unix runtime backend experiments.
#[cfg(unix)]
pub mod os;
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
    KvAllKeysReply, KvDeleteManyReply, KvGetManyReply, KvReply, KvShardSnapshotsReply,
    KvTotalLenReply, ShardedKv, ShardedKvConfig,
};
pub use runtime::{ReplyFuture, RuntimeSnapshot, DEFAULT_MAILBOX_CAPACITY};
pub use shard::{ShardId, ShardSnapshot};
