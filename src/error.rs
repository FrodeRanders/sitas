use std::error::Error;
use std::fmt;

/// Errors returned by shard lifecycle and mailbox operations.
#[derive(Debug, PartialEq, Eq)]
pub enum ShardError {
    /// A sharded service cannot be started with zero shards.
    InvalidShardCount,
    /// A caller addressed a shard index that does not exist.
    InvalidShardId(usize),
    /// A bounded shard mailbox was configured with zero capacity.
    InvalidMailboxCapacity,
    /// A sharded executor CPU placement policy does not cover all shards.
    InvalidCpuPlacement,
    /// Required CPU placement could not be applied.
    CpuPlacementFailed(String),
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
            ShardError::InvalidCpuPlacement => {
                write!(f, "CPU placement must provide a CPU for every shard")
            }
            ShardError::CpuPlacementFailed(reason) => {
                write!(f, "required CPU placement failed: {reason}")
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
