//! Minimal no_std sharded key-value service for foreign runtimes.
//!
//! Each shard runs its message loop as a **future** on a
//! [`ShardExecutor`](crate::shard_executor::ShardExecutor): awaiting the next
//! envelope parks the shard in its reactor's single blocking wait (§7 of the
//! co-designed architecture), and a sender's wake re-queues exactly that
//! task. Requesters awaiting a reply park through the runtime's
//! [`ShardParker`]. Nothing spins.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;

use crate::ShardError;
use crate::placement::{HashPlacement, Placement, ShardPlacement};
use crate::reactor_backend::ReactorBackend;
use crate::shard::ShardId;
use crate::shard_executor::ShardExecutor;
use crate::shard_runtime::{ShardParker, ShardReceiver, ShardRuntime, ShardSender};

/// Upper bound on one park/wait while waiting for a message or reply. Parks
/// are a latency optimisation, not a correctness dependency: a lost or stolen
/// wake costs at most one interval before the parked side re-checks its state
/// (relevant while all shards share one process-wide completion queue).
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
            let shard_parker = Arc::clone(&parker);
            let shard_id = ShardId(index);
            let reactor = runtime.shard_reactor(shard_id);
            runtime.spawn_shard(
                shard_id,
                ShardPlacement::Sequential,
                Box::new(move || run_kv_shard(receiver, shard_parker, reactor)),
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

fn run_kv_shard<R>(
    receiver: ShardReceiver<KvEnvelope>,
    parker: Arc<dyn ShardParker>,
    reactor: R,
) where
    R: ReactorBackend + Send + 'static,
    R::Waker: 'static,
{
    // The shard's message loop is a task on its own executor. Awaiting the
    // next envelope parks the shard in the reactor's single blocking wait; a
    // sender's wake (the channel waker → reactor wake) re-polls exactly this
    // task. A bounded idle wait keeps the shard self-healing on the shared
    // process-wide completion queue.
    let mut executor = ShardExecutor::new(reactor).with_idle_wait(Some(PARK_TIMEOUT));
    executor.spawn(kv_shard_task(receiver, parker));
    executor.run();
}

async fn kv_shard_task(mut receiver: ShardReceiver<KvEnvelope>, parker: Arc<dyn ShardParker>) {
    let mut map = BTreeMap::<String, String>::new();
    while let Some(envelope) = receiver.recv().await {
        let value = match envelope.command {
            KvCommand::Get { key } => KvReplyValue::StringOption(map.get(&key).cloned()),
            KvCommand::Put { key, value } => {
                map.insert(key, value);
                KvReplyValue::Unit
            }
            KvCommand::Len => KvReplyValue::Usize(map.len()),
        };
        let _ = envelope.reply.try_push(value);
        // Release the requester parked on this reply.
        parker.unpark();
    }
}

impl Drop for ShardedKv {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.stopped.store(true, Ordering::Release);
            // Close the mailbox so the shard's `recv().await` resolves to
            // `None`, its task completes, and its executor returns.
            shard.sender.close();
        }
        // Release any shard or requester parked on the shared queue so they
        // observe the shutdown promptly rather than waiting out an interval.
        self.parker.unpark();
    }
}
