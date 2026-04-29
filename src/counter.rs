use std::fmt;
use std::sync::mpsc;
use std::time::Duration;

use crate::runtime::{HasShardId, Reply, RuntimeSnapshot, ShardConfig, ShardMailbox, ShardSet};
use crate::{ShardError, ShardId};

/// Configuration for starting a [`ShardedCounter`] instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedCounterConfig {
    /// Number of shard threads to start.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
}

impl ShardedCounterConfig {
    /// Creates a config with the default bounded mailbox capacity.
    pub fn new(shard_count: usize) -> Self {
        ShardConfig::new(shard_count).into()
    }

    /// Sets the bounded mailbox capacity per shard.
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    fn runtime_config(self) -> Result<ShardConfig, ShardError> {
        ShardConfig {
            shard_count: self.shard_count,
            mailbox_capacity: self.mailbox_capacity,
        }
        .validate()
    }
}

impl From<ShardConfig> for ShardedCounterConfig {
    fn from(config: ShardConfig) -> Self {
        Self {
            shard_count: config.shard_count,
            mailbox_capacity: config.mailbox_capacity,
        }
    }
}

/// Reply handle for an accepted counter command.
pub type CounterReply<T> = Reply<T>;

/// A point-in-time, owned summary of one counter shard.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterShardSnapshot {
    /// The shard this snapshot describes.
    pub shard_id: ShardId,
    /// Current counter value on the shard when the snapshot command ran.
    pub value: i64,
}

/// Reply handle for a total counter request across all shards.
///
/// Calling [`CounterTotalReply::wait`] waits for every shard reply and sums the
/// returned values.
#[must_use]
#[derive(Debug)]
pub struct CounterTotalReply {
    replies: Vec<CounterReply<i64>>,
}

impl CounterTotalReply {
    fn new(replies: Vec<CounterReply<i64>>) -> Self {
        Self { replies }
    }

    /// Waits for all shard counter replies and returns their sum.
    pub fn wait(self) -> Result<i64, ShardError> {
        self.replies
            .into_iter()
            .try_fold(0i64, |total, reply| Ok(total + reply.wait()?))
    }

    /// Waits for all shard counter replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<i64, ShardError> {
        self.replies.into_iter().try_fold(0i64, |total, reply| {
            Ok(total + reply.wait_timeout(timeout)?)
        })
    }
}

/// Reply handle for a per-shard counter snapshot request.
///
/// Calling [`CounterShardSnapshotsReply::wait`] waits for every shard reply and
/// returns owned snapshots in shard order.
#[must_use]
#[derive(Debug)]
pub struct CounterShardSnapshotsReply {
    replies: Vec<(ShardId, CounterReply<i64>)>,
}

impl CounterShardSnapshotsReply {
    fn new(replies: Vec<(ShardId, CounterReply<i64>)>) -> Self {
        Self { replies }
    }

    /// Waits for all shard snapshot replies.
    pub fn wait(self) -> Result<Vec<CounterShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(CounterShardSnapshot {
                    shard_id,
                    value: reply.wait()?,
                })
            })
            .collect()
    }

    /// Waits for all shard snapshot replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<CounterShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(CounterShardSnapshot {
                    shard_id,
                    value: reply.wait_timeout(timeout)?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Default)]
struct CounterService {
    value: i64,
}

impl CounterService {
    fn add(&mut self, delta: i64) -> i64 {
        self.value += delta;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }
}

enum CounterCommand {
    Add {
        delta: i64,
        reply: mpsc::Sender<i64>,
    },
    Get {
        reply: mpsc::Sender<i64>,
    },
    Stop {
        reply: mpsc::Sender<()>,
    },
    #[cfg(test)]
    Hold {
        release: mpsc::Receiver<()>,
        started: mpsc::Sender<()>,
    },
}

struct CounterShardHandle {
    id: ShardId,
    mailbox: ShardMailbox<CounterCommand>,
}

impl CounterShardHandle {
    fn new(id: ShardId, mailbox: ShardMailbox<CounterCommand>) -> Self {
        Self { id, mailbox }
    }

    fn submit_add(&self, delta: i64) -> Result<CounterReply<i64>, ShardError> {
        self.mailbox
            .request(|reply| CounterCommand::Add { delta, reply })
    }

    fn try_submit_add(&self, delta: i64) -> Result<CounterReply<i64>, ShardError> {
        self.mailbox
            .try_request(|reply| CounterCommand::Add { delta, reply })
    }

    fn submit_get(&self) -> Result<CounterReply<i64>, ShardError> {
        self.mailbox.request(|reply| CounterCommand::Get { reply })
    }

    fn try_submit_get(&self) -> Result<CounterReply<i64>, ShardError> {
        self.mailbox
            .try_request(|reply| CounterCommand::Get { reply })
    }

    fn send_stop(&self) -> Result<(), ShardError> {
        self.mailbox
            .request_stopped(|reply| CounterCommand::Stop { reply })
    }

    #[cfg(test)]
    fn send_hold(&self) -> Result<(mpsc::Sender<()>, mpsc::Receiver<()>), ShardError> {
        let (release_sender, release_receiver) = mpsc::channel();
        let (started_sender, started_receiver) = mpsc::channel();

        self.mailbox.send(CounterCommand::Hold {
            release: release_receiver,
            started: started_sender,
        })?;

        Ok((release_sender, started_receiver))
    }
}

impl HasShardId for CounterShardHandle {
    fn shard_id(&self) -> ShardId {
        self.id
    }
}

/// A sharded counter service with one owning thread per shard.
///
/// This service exists to prove the small runtime layer can support a second
/// shard-local service without sharing mutable state.
pub struct ShardedCounter {
    shards: ShardSet<CounterShardHandle>,
    stopped: bool,
}

impl fmt::Debug for ShardedCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedCounter")
            .field("shard_count", &self.shard_count())
            .field("mailbox_capacity", &self.mailbox_capacity())
            .field("stopped", &self.stopped)
            .finish_non_exhaustive()
    }
}

impl ShardedCounter {
    /// Starts a sharded counter service with `shard_count` shard threads.
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        Self::start_with_config(ShardedCounterConfig::new(shard_count))
    }

    /// Starts a sharded counter service from an explicit configuration.
    pub fn start_with_config(config: ShardedCounterConfig) -> Result<Self, ShardError> {
        let config = config.runtime_config()?;

        let shards = ShardSet::start(
            config.shard_count,
            config.mailbox_capacity,
            |shard_idx, mailbox| CounterShardHandle::new(ShardId(shard_idx), mailbox),
            run_counter_shard,
        );

        Ok(Self {
            shards,
            stopped: false,
        })
    }

    /// Returns the number of shards in this counter service.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the bounded mailbox capacity configured for each shard.
    pub fn mailbox_capacity(&self) -> usize {
        self.shards.mailbox_capacity()
    }

    /// Returns an owned snapshot of this counter's runtime shape.
    pub fn runtime_snapshot(&self) -> RuntimeSnapshot {
        self.shards.snapshot(self.stopped)
    }

    /// Adds `delta` to a specific shard counter and returns the new shard value.
    pub fn add_on_shard(&self, shard_id: ShardId, delta: i64) -> Result<i64, ShardError> {
        self.submit_add_on_shard(shard_id, delta)?.wait()
    }

    /// Attempts to add `delta` without waiting for mailbox capacity.
    pub fn try_add_on_shard(&self, shard_id: ShardId, delta: i64) -> Result<i64, ShardError> {
        self.try_submit_add_on_shard(shard_id, delta)?.wait()
    }

    /// Enqueues an add command and returns a reply handle.
    pub fn submit_add_on_shard(
        &self,
        shard_id: ShardId,
        delta: i64,
    ) -> Result<CounterReply<i64>, ShardError> {
        let shard = self.shard(shard_id)?;
        shard.submit_add(delta)
    }

    /// Attempts to enqueue an add command without waiting for mailbox capacity.
    pub fn try_submit_add_on_shard(
        &self,
        shard_id: ShardId,
        delta: i64,
    ) -> Result<CounterReply<i64>, ShardError> {
        let shard = self.shard(shard_id)?;
        shard.try_submit_add(delta)
    }

    /// Returns the current value on a specific shard.
    pub fn get_on_shard(&self, shard_id: ShardId) -> Result<i64, ShardError> {
        self.submit_get_on_shard(shard_id)?.wait()
    }

    /// Attempts to get a shard value without waiting for mailbox capacity.
    pub fn try_get_on_shard(&self, shard_id: ShardId) -> Result<i64, ShardError> {
        self.try_submit_get_on_shard(shard_id)?.wait()
    }

    /// Enqueues a get command and returns a reply handle.
    pub fn submit_get_on_shard(&self, shard_id: ShardId) -> Result<CounterReply<i64>, ShardError> {
        let shard = self.shard(shard_id)?;
        shard.submit_get()
    }

    /// Attempts to enqueue a get command without waiting for mailbox capacity.
    pub fn try_submit_get_on_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<CounterReply<i64>, ShardError> {
        let shard = self.shard(shard_id)?;
        shard.try_submit_get()
    }

    /// Returns the sum of all shard counters.
    pub fn total(&self) -> Result<i64, ShardError> {
        self.submit_total()?.wait()
    }

    /// Attempts to return the sum of all shard counters without waiting for
    /// mailbox capacity on any shard.
    ///
    /// If any shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// Accepted commands still block waiting for their shard replies.
    pub fn try_total(&self) -> Result<i64, ShardError> {
        self.try_submit_total()?.wait()
    }

    /// Enqueues get commands to all shards and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for shards to execute the commands.
    pub fn submit_total(&self) -> Result<CounterTotalReply, ShardError> {
        self.ensure_running()?;
        let replies = self.shards.request_all(CounterShardHandle::submit_get)?;
        Ok(CounterTotalReply::new(replies))
    }

    /// Attempts to enqueue get commands to all shards and return a reply handle
    /// without waiting for mailbox capacity.
    ///
    /// If this returns [`ShardError::MailboxFull`], earlier shard get commands
    /// may already have been accepted. Those commands are read-only.
    pub fn try_submit_total(&self) -> Result<CounterTotalReply, ShardError> {
        self.ensure_running()?;
        let replies = self
            .shards
            .request_all(CounterShardHandle::try_submit_get)?;
        Ok(CounterTotalReply::new(replies))
    }

    /// Returns owned per-shard counter snapshots in shard order.
    pub fn shard_snapshots(&self) -> Result<Vec<CounterShardSnapshot>, ShardError> {
        self.submit_shard_snapshots()?.wait()
    }

    /// Attempts to return owned per-shard counter snapshots without waiting for
    /// mailbox capacity on any shard.
    ///
    /// If any shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// Accepted commands still block waiting for their shard replies.
    pub fn try_shard_snapshots(&self) -> Result<Vec<CounterShardSnapshot>, ShardError> {
        self.try_submit_shard_snapshots()?.wait()
    }

    /// Enqueues snapshot commands to all shards and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for shards to execute the commands.
    pub fn submit_shard_snapshots(&self) -> Result<CounterShardSnapshotsReply, ShardError> {
        self.ensure_running()?;
        let replies = self
            .shards
            .request_all_with_ids(CounterShardHandle::submit_get)?;
        Ok(CounterShardSnapshotsReply::new(replies))
    }

    /// Attempts to enqueue snapshot commands to all shards and return a reply
    /// handle without waiting for mailbox capacity.
    ///
    /// If this returns [`ShardError::MailboxFull`], earlier shard snapshot
    /// commands may already have been accepted. Those commands are read-only.
    pub fn try_submit_shard_snapshots(&self) -> Result<CounterShardSnapshotsReply, ShardError> {
        self.ensure_running()?;
        let replies = self
            .shards
            .request_all_with_ids(CounterShardHandle::try_submit_get)?;
        Ok(CounterShardSnapshotsReply::new(replies))
    }

    /// Stops all shards and joins their threads.
    pub fn stop(mut self) -> Result<(), ShardError> {
        self.shutdown()
    }

    /// Stops all shards and joins their threads while retaining the handle.
    ///
    /// This is useful when callers need to inspect
    /// [`ShardedCounter::runtime_snapshot`] after shutdown. Calling this more
    /// than once is a no-op after the first successful shutdown.
    pub fn shutdown(&mut self) -> Result<(), ShardError> {
        if self.stopped {
            return Ok(());
        }

        self.stopped = true;

        self.shards.stop_and_join(CounterShardHandle::send_stop)
    }

    fn ensure_running(&self) -> Result<(), ShardError> {
        if self.stopped {
            Err(ShardError::ShardStopped)
        } else {
            Ok(())
        }
    }

    fn shard(&self, shard_id: ShardId) -> Result<&CounterShardHandle, ShardError> {
        self.ensure_running()?;
        self.shards.get_by_id(shard_id)
    }
}

impl Drop for ShardedCounter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_counter_shard(receiver: mpsc::Receiver<CounterCommand>) {
    let mut service = CounterService::default();

    loop {
        match receiver.recv() {
            Ok(CounterCommand::Add { delta, reply }) => {
                let _ = reply.send(service.add(delta));
            }
            Ok(CounterCommand::Get { reply }) => {
                let _ = reply.send(service.get());
            }
            Ok(CounterCommand::Stop { reply }) => {
                let _ = reply.send(());
                break;
            }
            #[cfg(test)]
            Ok(CounterCommand::Hold { release, started }) => {
                let _ = started.send(());
                let _ = release.recv();
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CounterCommand, CounterService, ShardedCounter, ShardedCounterConfig};
    use crate::{ShardError, ShardId, DEFAULT_MAILBOX_CAPACITY};
    use std::time::Duration;

    #[test]
    fn counter_service_adds_and_gets_values() {
        let mut service = CounterService::default();

        assert_eq!(service.get(), 0);
        assert_eq!(service.add(5), 5);
        assert_eq!(service.add(-2), 3);
        assert_eq!(service.get(), 3);
    }

    #[test]
    fn counter_config_uses_default_mailbox_capacity() {
        let config = ShardedCounterConfig::new(3);

        assert_eq!(config.shard_count, 3);
        assert_eq!(config.mailbox_capacity, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn try_methods_return_mailbox_full_without_waiting_for_capacity() {
        let counter = ShardedCounter::start_with_config(
            ShardedCounterConfig::new(1).with_mailbox_capacity(1),
        )
        .unwrap();
        let shard = counter.shards.get(0).unwrap();
        let (release, started) = shard.send_hold().unwrap();

        started.recv().unwrap();

        shard
            .mailbox
            .try_send(CounterCommand::Get {
                reply: std::sync::mpsc::channel().0,
            })
            .unwrap();

        assert_eq!(
            counter.try_add_on_shard(ShardId(0), 1),
            Err(ShardError::MailboxFull)
        );
        assert_eq!(
            counter.try_get_on_shard(ShardId(0)),
            Err(ShardError::MailboxFull)
        );
        assert_eq!(counter.try_total(), Err(ShardError::MailboxFull));
        assert_eq!(counter.try_shard_snapshots(), Err(ShardError::MailboxFull));

        release.send(()).unwrap();
        counter.stop().unwrap();
    }

    #[test]
    fn counter_reply_wait_timeout_returns_timeout_when_shard_has_not_replied() {
        let counter = ShardedCounter::start_with_config(
            ShardedCounterConfig::new(1).with_mailbox_capacity(2),
        )
        .unwrap();
        let (release, started) = counter.shards.get(0).unwrap().send_hold().unwrap();

        started.recv().unwrap();

        let reply = counter.submit_get_on_shard(ShardId(0)).unwrap();

        assert_eq!(
            reply.wait_timeout(Duration::from_millis(1)),
            Err(ShardError::ReplyTimeout)
        );

        release.send(()).unwrap();
        counter.stop().unwrap();
    }

    #[test]
    fn counter_total_reply_wait_timeout_returns_timeout_when_shard_has_not_replied() {
        let counter = ShardedCounter::start_with_config(
            ShardedCounterConfig::new(1).with_mailbox_capacity(2),
        )
        .unwrap();
        let (release, started) = counter.shards.get(0).unwrap().send_hold().unwrap();

        started.recv().unwrap();

        let reply = counter.submit_total().unwrap();

        assert_eq!(
            reply.wait_timeout(Duration::from_millis(1)),
            Err(ShardError::ReplyTimeout)
        );

        release.send(()).unwrap();
        counter.stop().unwrap();
    }
}
