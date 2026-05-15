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
/// Minimal async executor experiment.
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
/// Shard-local async state helpers.
pub mod shard_local;
/// Shard-per-thread async executor runtime.
pub mod sharded_executor;

pub use counter::{
    CounterReply, CounterShardSnapshot, CounterShardSnapshotsReply, CounterTotalReply,
    ShardedCounter, ShardedCounterConfig,
};
pub use error::ShardError;
pub use executor::{
    ExecutorObserver, ExecutorSnapshot, TaskId, TaskSnapshot, TaskStatus, TaskWait,
};
pub use kv::{
    KvAllKeysReply, KvDeleteManyReply, KvGetManyReply, KvReply, KvShardSnapshotsReply,
    KvTotalLenReply, ShardedKv, ShardedKvConfig,
};
pub use runtime::{DEFAULT_MAILBOX_CAPACITY, ReplyFuture, RuntimeSnapshot};
pub use shard::{ShardId, ShardSnapshot};
pub use shard_local::{
    ShardLocal, ShardLocalAccessError, ShardLocalWorkerTimeoutError, ShardLocalWorkers,
    StoppableShardLocalWorkers,
};
pub use sharded_executor::{
    ShardedExecutor, ShardedExecutorConfig, ShardedExecutorObserver, ShardedExecutorShardSnapshot,
    ShardedExecutorSnapshot, ShardedJoinError, ShardedJoinHandle, ShardedJoinTimeoutError,
    ShardedOperationError, ShardedSpawnError, ShardedSubmitter, available_parallelism,
    current_executor_shard, join_all_shards,
};
