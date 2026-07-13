#![no_std]

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
pub mod shard_runtime;

#[cfg(feature = "std")]
pub mod async_service;
#[cfg(feature = "std")]
pub mod charlotte_abi;
#[cfg(feature = "std")]
pub mod metrics;
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
#[cfg(feature = "std")]
pub mod stream_reply;

pub use reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker, SchedulerWake};
#[cfg(feature = "std")]
pub use runtime::*;
pub use shard::*;
pub use error::*;
