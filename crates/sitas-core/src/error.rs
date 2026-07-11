//! Error types returned by shard and runtime operations.
//!
//! [`ShardError`] is the central error type for the std-only shard runtime
//! and the concrete services built on top of it.

use core::error::Error;
use core::fmt;

/// Errors returned by shard lifecycle and mailbox operations.
#[derive(Debug, PartialEq, Eq)]
pub enum ShardError {
    /// A sharded service cannot be started with zero shards.
    InvalidShardCount,
    /// A caller addressed a shard index that does not exist.
    InvalidShardId(usize),
    /// A bounded shard mailbox was configured with zero capacity.
    InvalidMailboxCapacity,
    /// A sharded executor CPU placement policy is invalid.
    InvalidCpuPlacement(String),
    /// Required CPU placement could not be applied.
    CpuPlacementFailed(String),
    /// A sharded executor memory placement policy is invalid.
    InvalidMemoryPlacement(String),
    /// Required memory placement could not be applied.
    MemoryPlacementFailed(String),
    /// A non-blocking send found the target shard mailbox full.
    MailboxFull,
    /// Sending a command to a shard mailbox failed.
    SendFailed,
    /// Waiting for a command reply failed.
    ReplyFailed,
    /// Timed out while waiting for a command reply.
    ReplyTimeout,
    /// The target shard had already stopped.
    ShardStopped,
    /// A shard thread panicked or otherwise could not be joined.
    ThreadJoinFailed,
}

impl fmt::Display for ShardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardError::InvalidShardCount => write!(f, "shard count must be greater than zero"),
            ShardError::InvalidShardId(id) => write!(f, "invalid shard id: {id}"),
            ShardError::InvalidMailboxCapacity => {
                write!(f, "mailbox capacity must be greater than zero")
            }
            ShardError::InvalidCpuPlacement(reason) => {
                write!(f, "invalid CPU placement: {reason}")
            }
            ShardError::CpuPlacementFailed(reason) => {
                write!(f, "required CPU placement failed: {reason}")
            }
            ShardError::InvalidMemoryPlacement(reason) => {
                write!(f, "invalid memory placement: {reason}")
            }
            ShardError::MemoryPlacementFailed(reason) => {
                write!(f, "required memory placement failed: {reason}")
            }
            ShardError::MailboxFull => write!(f, "shard mailbox is full"),
            ShardError::SendFailed => write!(f, "failed to send command to shard"),
            ShardError::ReplyFailed => write!(f, "failed to receive reply from shard"),
            ShardError::ReplyTimeout => write!(f, "timed out waiting for shard reply"),
            ShardError::ShardStopped => write!(f, "shard has stopped"),
            ShardError::ThreadJoinFailed => write!(f, "failed to join shard thread"),
        }
    }
}

impl Error for ShardError {}
