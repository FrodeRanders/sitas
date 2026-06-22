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
//! The crate now contains three layers:
//!
//! 1. **Baseline std-only sharded services** (`runtime`, `kv`, `counter`,
//!    `placement`, `sharded`): bounded mailboxes, typed command/reply APIs,
//!    blocking and submit/wait-later handles, owned snapshots, clean
//!    startup/shutdown, streaming replies, and a generic sharded service trait.
//!
//! 2. **Custom single-threaded async executor** (`executor`, `os`): pinned
//!    futures, ready-queue scheduling, timers, timeouts, cooperative stop
//!    tokens, task scopes, join handles, scheduling groups, Unix readiness I/O
//!    (`epoll`/`kqueue`/`poll`), TCP accept/connect/copy helpers, UDP socket
//!    support, and experimental Linux `io_uring` file-I/O futures with
//!    extended opcode support.
//!
//! 3. **Shard-per-thread async runtime** (`sharded_executor`, `shard_local`):
//!    one executor thread per shard, cross-shard submission via
//!    `ShardedSubmitter`, shard-local state via `ShardLocal<T>`, CPU placement,
//!    owned message transfer via `shard_mailbox`, backpressure-aware spawning,
//!    sharded TCP server, sharded scheduling groups, async-std bridge, and
//!    snapshot-based observability with metrics collection.

#![warn(missing_docs)]
#![warn(unsafe_op_in_unsafe_fn)]

/// Async-std service bridge adapters.
pub mod async_service;
/// Sharded counter service implementation.
pub mod counter;
/// Error types returned by shard operations.
pub mod error;
/// Minimal async executor experiment.
pub mod executor;
/// Sharded key-value store implementation.
pub mod kv;
/// Runtime metrics collection.
pub mod metrics;
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
/// Typed owned-message transfer between executor shards.
pub mod shard_mailbox;
/// Generic sharded service trait and runtime.
pub mod sharded;
/// Shard-per-thread async executor runtime.
pub mod sharded_executor;
/// Network-facing sharded TCP server.
#[cfg(unix)]
pub mod sharded_tcp;
/// Streaming reply channels for sharded services.
pub mod stream_reply;

/// Running statistics for streaming samples.
pub mod running_stats;

pub use async_service::{AsyncShardedCounter, AsyncShardedKv, OwnedAsyncShardedKv};
pub use counter::{
    CounterReply, CounterShardSnapshot, CounterShardSnapshotsReply, CounterTotalReply,
    ShardedCounter, ShardedCounterConfig,
};
pub use error::ShardError;
pub use executor::{
    BackpressureGuard, ExecutorObserver, ExecutorSnapshot, Permit, SchedulingGroup,
    SchedulingGroupError, SchedulingGroupId, SchedulingGroupSnapshot, TaskId, TaskSnapshot,
    TaskStatus, TaskWait,
};
pub use kv::{
    KvAllKeysReply, KvDeleteManyReply, KvGetManyReply, KvReply, KvShardSnapshotsReply,
    KvTotalLenReply, ShardedKv, ShardedKvConfig,
};
pub use metrics::{MetricsSnapshot, RuntimeMetrics};
pub use running_stats::RunningStatistics;
pub use runtime::{DEFAULT_MAILBOX_CAPACITY, ReplyFuture, RuntimeSnapshot};
pub use shard::{ShardId, ShardSnapshot};
pub use shard_local::{
    ShardLocal, ShardLocalAccessError, ShardLocalWorkerTimeoutError, ShardLocalWorkers,
    StoppableShardLocalWorkers,
};
pub use shard_mailbox::{
    KeyRouterCreateError, RouteByKey, ShardMailbox, ShardMailboxAddressError, ShardMailboxConfig,
    ShardMailboxCreateError, ShardMailboxSet, ShardMailboxSnapshot, ShardReceiver, ShardRecv,
    ShardRecvError, ShardSend, ShardSendError, ShardSender, UniformShardRouter,
    WorkUnitMailboxAddressError, WorkUnitMailboxCreateError, WorkUnitMailboxSet,
    WorkUnitMailboxSnapshot, WorkUnitRouter, WorkUnitSpec,
};
pub use sharded::{ShardService, Sharded, ShardedConfig};
pub use sharded_executor::{
    CpuId, CpuPlacement, CpuPlacementStatus, ShardedExecutor, ShardedExecutorConfig,
    ShardedExecutorObserver, ShardedExecutorShardSnapshot, ShardedExecutorSnapshot,
    ShardedJoinError, ShardedJoinHandle, ShardedJoinTimeoutError, ShardedOperationError,
    ShardedSchedulingGroup, ShardedSchedulingGroupError, ShardedShutdownOutcome, ShardedSpawnError,
    ShardedSubmitter, available_cpu_ids, available_parallelism, current_executor_cpu_placement,
    current_executor_shard, join_all_shards, join_all_shards_timeout,
};
#[cfg(unix)]
pub use sharded_tcp::{
    ShardedTcpConfig, ShardedTcpConnection, ShardedTcpEvent, ShardedTcpEventSink,
    ShardedTcpIncomingCpu, ShardedTcpServer, ShardedTcpServerHandle, ShardedTcpServerSnapshot,
    ShardedTcpStartError,
};
pub use stream_reply::{
    StreamBatch, StreamError, StreamFuture, StreamProducer, StreamReply, StreamSender,
    stream_channel,
};
