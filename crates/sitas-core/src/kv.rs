//! Minimal no_std sharded key-value service for foreign runtimes.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::ShardError;
use crate::placement::{HashPlacement, Placement, ShardPlacement};
use crate::shard::ShardId;
use crate::shard_runtime::{ShardReceiver, ShardRuntime, ShardSender};

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
}

impl ShardedKv {
    pub fn start_with_runtime<R>(config: ShardedKvConfig, runtime: &R) -> Result<Self, ShardError>
    where
        R: ShardRuntime + ?Sized,
    {
        let config = config.validate()?;
        let mut shards = Vec::with_capacity(config.shard_count);

        for index in 0..config.shard_count {
            let (sender, receiver) = runtime
                .channel(config.mailbox_capacity)
                .map_err(|_| ShardError::InvalidMailboxCapacity)?;
            let stopped = Arc::new(AtomicBool::new(false));
            let shard_stopped = Arc::clone(&stopped);
            let shard_id = ShardId(index);
            runtime.spawn_shard(
                shard_id,
                ShardPlacement::Sequential,
                Box::new(move || run_kv_shard(receiver, shard_stopped)),
            );
            shards.push(KvShardHandle {
                id: shard_id,
                sender,
                stopped,
            });
        }

        Ok(Self { shards })
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
        let _ = self.id;
        Ok(KvReply { reply })
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
}

impl KvReply {
    fn recv_unit(self) -> Result<(), ShardError> {
        match self.spin_recv()? {
            KvReplyValue::Unit => Ok(()),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    fn recv_string_option(self) -> Result<Option<String>, ShardError> {
        match self.spin_recv()? {
            KvReplyValue::StringOption(value) => Ok(value),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    fn recv_usize(self) -> Result<usize, ShardError> {
        match self.spin_recv()? {
            KvReplyValue::Usize(value) => Ok(value),
            _ => Err(ShardError::ReplyFailed),
        }
    }

    fn spin_recv(self) -> Result<KvReplyValue, ShardError> {
        loop {
            if let Some(value) = self.reply.pop() {
                return Ok(value);
            }
            core::hint::spin_loop();
        }
    }
}

fn run_kv_shard(mut receiver: ShardReceiver<KvEnvelope>, stopped: Arc<AtomicBool>) {
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
            }
            None => {
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                core::hint::spin_loop();
            }
        }
    }
}

impl Drop for ShardedKv {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.stopped.store(true, Ordering::Release);
        }
    }
}
