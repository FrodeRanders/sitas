//! Minimal no_std sharded key-value service for foreign runtimes.
//!
//! Waiting is parked, never spun: a shard with an empty mailbox and a
//! requester awaiting a reply both sleep through the runtime's
//! [`ShardParker`] (on CharlotteOS: the kernel's `CQ_WAIT`/`CQ_WAKE`), and
//! the peer that produces the awaited state releases them with `unpark`.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;

use crate::ShardError;
use crate::placement::{HashPlacement, Placement, ShardPlacement};
use crate::shard::ShardId;
use crate::shard_runtime::{ShardParker, ShardReceiver, ShardRuntime, ShardSender};

/// Upper bound on one park while waiting for a message or reply. Parks are a
/// latency optimisation, not a correctness dependency: a lost or stolen wake
/// costs at most one interval before the parked side re-checks its state.
const PARK_TIMEOUT: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedKvConfig {
    pub shard_count: usize,
    pub mailbox_capacity: usize,
}

impl ShardedKvConfig {
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            mailbox_capacity: 64,
        }
    }

    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    fn validate(self) -> Result<Self, ShardError> {
        if self.shard_count == 0 {
            return Err(ShardError::InvalidShardCount);
        }
        if self.mailbox_capacity == 0 {
            return Err(ShardError::InvalidMailboxCapacity);
        }
        Ok(self)
    }
}

pub struct ShardedKv {
    shards: Vec<KvShardHandle>,
    parker: Arc<dyn ShardParker>,
}

impl ShardedKv {
    pub fn start_with_runtime<R>(config: ShardedKvConfig, runtime: &R) -> Result<Self, ShardError>
    where
        R: ShardRuntime + ?Sized,
    {
        let config = config.validate()?;
        let parker = runtime.parker();
        let mut shards = Vec::with_capacity(config.shard_count);

        for index in 0..config.shard_count {
            let (sender, receiver) = runtime
                .channel(config.mailbox_capacity)
                .map_err(|_| ShardError::InvalidMailboxCapacity)?;
            let stopped = Arc::new(AtomicBool::new(false));
            let shard_stopped = Arc::clone(&stopped);
            let shard_parker = Arc::clone(&parker);
            let shard_id = ShardId(index);
            runtime.spawn_shard(
                shard_id,
                ShardPlacement::Sequential,
                Box::new(move || run_kv_shard(receiver, shard_stopped, shard_parker)),
            );
            shards.push(KvShardHandle {
                id: shard_id,
                sender,
                stopped,
                parker: Arc::clone(&parker),
            });
        }

        Ok(Self { shards, parker })
    }

    pub fn put(&self, key: &str, value: &str) -> Result<(), ShardError> {
        self.shard_for(key)?
            .request(KvCommand::Put {
                key: key.to_string(),
                value: value.to_string(),
            })?
            .recv_unit()
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, ShardError> {
        self.shard_for(key)?
            .request(KvCommand::Get {
                key: key.to_string(),
            })?
            .recv_string_option()
    }

    pub fn total_len(&self) -> Result<usize, ShardError> {
        let mut total = 0usize;
        for shard in &self.shards {
            total += shard.request(KvCommand::Len)?.recv_usize()?;
        }
        Ok(total)
    }

    fn shard_for(&self, key: &str) -> Result<&KvShardHandle, ShardError> {
        let shard_id = HashPlacement.shard_for(key, self.shards.len());
        self.shards
            .get(shard_id.0)
            .ok_or(ShardError::InvalidShardId(shard_id.0))
    }
}

struct KvShardHandle {
    id: ShardId,
    sender: ShardSender<KvEnvelope>,
    stopped: Arc<AtomicBool>,
    parker: Arc<dyn ShardParker>,
}

impl KvShardHandle {
    fn request(&self, command: KvCommand) -> Result<KvReply, ShardError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(ShardError::ShardStopped);
        }
        let reply = Arc::new(crate::ringbuf::RingBuffer::bounded(2));
        self.sender
            .try_send(KvEnvelope {
                command,
                reply: Arc::clone(&reply),
            })
            .map_err(|_| ShardError::MailboxFull)?;
        // Release the serving shard's park so it observes the new message.
        self.parker.unpark();
        let _ = self.id;
        Ok(KvReply {
            reply,
            parker: Arc::clone(&self.parker),
        })
    }
}

enum KvCommand {
    Get { key: String },
    Put { key: String, value: String },
    Len,
}

struct KvEnvelope {
    command: KvCommand,
    reply: Arc<crate::ringbuf::RingBuffer<KvReplyValue>>,
}

enum KvReplyValue {
    Unit,
    StringOption(Option<String>),
    Usize(usize),
}

struct KvReply {
    reply: Arc<crate::ringbuf::RingBuffer<KvReplyValue>>,
    parker: Arc<dyn ShardParker>,
}

impl KvReply {
    fn recv_unit(self) -> Result<(), ShardError> {
        match self.recv()? {
            KvReplyValue::Unit => Ok(()),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    fn recv_string_option(self) -> Result<Option<String>, ShardError> {
        match self.recv()? {
            KvReplyValue::StringOption(value) => Ok(value),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    fn recv_usize(self) -> Result<usize, ShardError> {
        match self.recv()? {
            KvReplyValue::Usize(value) => Ok(value),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    /// Wait for the serving shard's reply, parking (not spinning) between
    /// checks. The serving shard `unpark`s us after it pushes the reply, so a
    /// park normally lasts only until that wake; the bounded interval makes a
    /// stolen or coalesced wake self-healing.
    fn recv(self) -> Result<KvReplyValue, ShardError> {
        loop {
            if let Some(value) = self.reply.pop() {
                return Ok(value);
            }
            self.parker.park(Some(PARK_TIMEOUT));
        }
    }
}

fn run_kv_shard(
    mut receiver: ShardReceiver<KvEnvelope>,
    stopped: Arc<AtomicBool>,
    parker: Arc<dyn ShardParker>,
) {
    let mut map = BTreeMap::<String, String>::new();

    loop {
        match receiver.try_recv() {
            Some(envelope) => {
                let value = match envelope.command {
                    KvCommand::Get { key } => KvReplyValue::StringOption(map.get(&key).cloned()),
                    KvCommand::Put { key, value } => {
                        map.insert(key, value);
                        KvReplyValue::Unit
                    }
                    KvCommand::Len => KvReplyValue::Usize(map.len()),
                };
                let _ = envelope.reply.try_push(value);
                // Release the requester waiting on this reply.
                parker.unpark();
            }
            None => {
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                // Mailbox empty: park until a sender `unpark`s us (or the
                // bounded interval elapses so shutdown is observed promptly).
                parker.park(Some(PARK_TIMEOUT));
            }
        }
    }
}

impl Drop for ShardedKv {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.stopped.store(true, Ordering::Release);
        }
        // Release any shard parked on an empty mailbox so it observes the
        // stop flag and exits, rather than waiting out its park interval.
        self.parker.unpark();
    }
}
