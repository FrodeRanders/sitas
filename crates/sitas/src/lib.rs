//! `sitas` is an experiment in Rust-native shard-local service ownership.
//!
//! The project is inspired by Seastar's shard-per-core, shared-nothing model.
//! Application state is owned by a shard thread. Other threads interact with
//! that state only by sending typed messages to the owning shard. No mutex
//! protects the service state because the service state is not shared.
//! Cross-shard values are owned values, so no references into shard-local
//! state escape the shard.
//!
//! This crate is the batteries-included host-platform facade over
//! [`sitas_core`] with the `std` feature enabled. It contains three layers:
//!
//! 1. **Baseline std-only sharded services** ([`runtime`], [`kv_service`],
//!    [`counter`], [`placement`], [`sharded`]): bounded mailboxes, typed
//!    command/reply APIs, blocking and submit/wait-later handles, owned
//!    snapshots, clean startup/shutdown, streaming replies, and a generic
//!    sharded service trait.
//!
//! 2. **Custom single-threaded async executor** ([`executor`], [`os`]): pinned
//!    futures, ready-queue scheduling, timers, timeouts, cooperative stop
//!    tokens, task scopes, join handles, scheduling groups, Unix readiness I/O
//!    (`epoll`/`kqueue`/`poll`), TCP accept/connect/copy helpers, UDP socket
//!    support, and experimental Linux `io_uring` file-I/O futures.
//!
//! 3. **Shard-per-thread async runtime** ([`sharded_executor`],
//!    [`shard_local`]): one executor thread per shard, cross-shard submission
//!    via `ShardedSubmitter`, shard-local state via `ShardLocal<T>`, CPU
//!    placement, owned message transfer via [`shard_mailbox`], sharded TCP
//!    server, and snapshot-based observability.
//!
//! The `no_std` + `alloc` lane of [`sitas_core`] (the [`kv`],
//! [`shard_executor`], [`shard_runtime`], and [`reactor_backend`] modules)
//! is re-exported unchanged; it is the surface foreign runtimes such as
//! CharlotteOS build against.

pub use sitas_core::*;

pub use sitas_core::{
    async_service, basic_kv, charlotte_abi, counter, error, executor, instant, io, kv, kv_service,
    metrics, placement, reactor_backend, ringbuf, running_stats, runtime, shard, shard_executor,
    shard_local, shard_mailbox, shard_runtime, sharded, sharded_executor, stream_reply,
};

#[cfg(unix)]
pub use sitas_core::{os, sharded_tcp};
