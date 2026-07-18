//! `sitas` core: a Rust-native, shard-per-core, shared-nothing runtime kernel.
//!
//! The crate has two lanes:
//!
//! - The default **`no_std` + `alloc` lane** used by foreign runtimes such as
//!   CharlotteOS: [`shard_runtime::ShardRuntime`] abstracts thread spawn,
//!   channels, parking, and reactors; [`shard_executor::ShardExecutor`] drives
//!   futures over a [`reactor_backend::ReactorBackend`]; [`kv`] and
//!   [`basic_kv`] provide the minimal sharded key-value service used by the
//!   `catten-user` smoke test.
//!
//! - The **`std` lane** (feature `std`) restores the original host runtime:
//!   bounded-mailbox sharded services ([`runtime`], [`kv_service`],
//!   [`counter`], [`sharded`]), the single-threaded async executor with
//!   readiness I/O, TCP/UDP helpers and Linux `io_uring` ([`executor`],
//!   [`os`]), and the shard-per-thread async runtime ([`sharded_executor`],
//!   [`shard_local`], [`shard_mailbox`], [`sharded_tcp`]).

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod basic_kv;
pub mod error;
pub mod instant;
pub mod io;
pub mod kv;
pub mod placement;
pub mod reactor_backend;
pub mod ringbuf;
pub mod shard;
pub mod shard_executor;
pub mod shard_runtime;

#[cfg(feature = "std")]
pub mod async_service;
#[cfg(feature = "std")]
pub mod charlotte_abi;
#[cfg(feature = "std")]
pub mod counter;
#[cfg(feature = "std")]
pub mod executor;
#[cfg(feature = "std")]
pub mod kv_service;
#[cfg(feature = "std")]
pub mod metrics;
#[cfg(all(feature = "std", unix))]
pub mod os;
#[cfg(feature = "std")]
pub mod running_stats;
#[cfg(feature = "std")]
pub mod runtime;
#[cfg(feature = "std")]
pub mod shard_local;
#[cfg(feature = "std")]
pub mod shard_mailbox;
#[cfg(feature = "std")]
pub mod sharded;
#[cfg(feature = "std")]
pub mod sharded_executor;
#[cfg(all(feature = "std", unix))]
pub mod sharded_tcp;
#[cfg(feature = "std")]
pub mod stream_reply;

pub use error::*;
pub use reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker, SchedulerWake};
pub use shard::*;

#[cfg(feature = "std")]
pub use async_service::{AsyncShardedCounter, AsyncShardedKv, OwnedAsyncShardedKv};
#[cfg(feature = "std")]
pub use counter::{
    CounterReply, CounterShardSnapshot, CounterShardSnapshotsReply, CounterTotalReply,
    ShardedCounter, ShardedCounterConfig,
};
#[cfg(feature = "std")]
pub use executor::{
    BackpressureGuard, ExecutorObserver, ExecutorSnapshot, Permit, SchedulingGroup,
    SchedulingGroupError, SchedulingGroupId, SchedulingGroupSnapshot, TaskId, TaskSnapshot,
    TaskStatus, TaskWait,
};
#[cfg(feature = "std")]
pub use kv_service::{
    KvAllKeysReply, KvDeleteManyReply, KvGetManyReply, KvReply, KvShardSnapshotsReply,
    KvTotalLenReply, ShardedKv, ShardedKvConfig,
};
#[cfg(feature = "std")]
pub use metrics::{MetricsSnapshot, RuntimeMetrics};
#[cfg(feature = "std")]
pub use running_stats::RunningStatistics;
#[cfg(feature = "std")]
pub use runtime::{DEFAULT_MAILBOX_CAPACITY, ReplyFuture, RuntimeSnapshot};
#[cfg(feature = "std")]
pub use shard_local::{
    ShardLocal, ShardLocalAccessError, ShardLocalWorkerTimeoutError, ShardLocalWorkers,
    StoppableShardLocalWorkers,
};
#[cfg(feature = "std")]
pub use shard_mailbox::{
    KeyRouterCreateError, RouteByKey, ShardMailbox, ShardMailboxAddressError, ShardMailboxConfig,
    ShardMailboxCreateError, ShardMailboxSet, ShardMailboxSnapshot, ShardReceiver, ShardRecv,
    ShardRecvError, ShardSend, ShardSendError, ShardSender, UniformShardRouter,
    WorkUnitMailboxAddressError, WorkUnitMailboxCreateError, WorkUnitMailboxSet,
    WorkUnitMailboxSnapshot, WorkUnitRouter, WorkUnitSpec,
};
#[cfg(feature = "std")]
pub use sharded::{ShardService, Sharded, ShardedConfig};
#[cfg(feature = "std")]
pub use sharded_executor::{
    CpuId, CpuPlacement, CpuPlacementStatus, MemoryPlacement, MemoryPlacementStatus, NumaNodeId,
    ShardedExecutor, ShardedExecutorConfig, ShardedExecutorObserver, ShardedExecutorShardSnapshot,
    ShardedExecutorSnapshot, ShardedJoinError, ShardedJoinHandle, ShardedJoinTimeoutError,
    ShardedOperationError, ShardedSchedulingGroup, ShardedSchedulingGroupError,
    ShardedShutdownOutcome, ShardedSpawnError, ShardedSubmitter, available_cpu_ids,
    available_parallelism, current_executor_cpu_placement, current_executor_memory_placement,
    current_executor_shard, join_all_shards, join_all_shards_timeout, numa_node_for_cpu,
};
#[cfg(all(feature = "std", unix))]
pub use sharded_tcp::{
    ShardedTcpConfig, ShardedTcpConnection, ShardedTcpEvent, ShardedTcpEventSink,
    ShardedTcpIncomingCpu, ShardedTcpServer, ShardedTcpServerHandle, ShardedTcpServerSnapshot,
    ShardedTcpStartError,
};
#[cfg(feature = "std")]
pub use stream_reply::{
    StreamBatch, StreamError, StreamFuture, StreamProducer, StreamReply, StreamSender,
    stream_channel,
};
