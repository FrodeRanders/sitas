use std::fmt;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::{ShardError, ShardId};

/// Default number of pending commands each shard mailbox can hold.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Runtime shard configuration shared by sharded services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    /// Number of shard threads to start.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
}

impl ShardConfig {
    /// Creates a config with the default bounded mailbox capacity.
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
        }
    }

    /// Sets the bounded mailbox capacity per shard.
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    pub(crate) fn validate(self) -> Result<Self, ShardError> {
        if self.shard_count == 0 {
            return Err(ShardError::InvalidShardCount);
        }
        if self.mailbox_capacity == 0 {
            return Err(ShardError::InvalidMailboxCapacity);
        }

        Ok(self)
    }
}

/// Owned snapshot of a running shard set's runtime shape.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    /// Number of shard threads in the service.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
    /// Whether the service handle has begun shutdown.
    pub stopped: bool,
}

/// A one-shot reply handle for an accepted shard command.
///
/// This is not a future and does not require an async runtime. Calling
/// [`Reply::wait`] blocks the current thread until the owning shard sends the
/// response or the reply channel is disconnected. [`Reply::try_wait`] polls the
/// reply channel once without blocking.
#[must_use]
pub struct Reply<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T> Reply<T> {
    pub(crate) fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self { receiver }
    }

    /// Waits for the shard reply and returns the owned response value.
    pub fn wait(self) -> Result<T, ShardError> {
        self.receiver.recv().map_err(|_| ShardError::ReplyFailed)
    }

    /// Waits for the shard reply until `timeout` expires.
    pub fn wait_timeout(self, timeout: Duration) -> Result<T, ShardError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(value) => Ok(value),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ShardError::ReplyTimeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ShardError::ReplyFailed),
        }
    }

    /// Polls the reply channel once without blocking.
    ///
    /// Returns `Ok(None)` when the shard has not replied yet. Returns
    /// `Ok(Some(value))` when the reply is ready.
    pub fn try_wait(&self) -> Result<Option<T>, ShardError> {
        match self.receiver.try_recv() {
            Ok(value) => Ok(Some(value)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(ShardError::ReplyFailed),
        }
    }
}

impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reply").finish_non_exhaustive()
    }
}

pub(crate) fn reply_channel<T>() -> (mpsc::Sender<T>, Reply<T>) {
    let (sender, receiver) = mpsc::channel();
    (sender, Reply::new(receiver))
}

pub(crate) struct ShardMailbox<C> {
    sender: mpsc::SyncSender<C>,
}

impl<C> ShardMailbox<C> {
    pub(crate) fn new(sender: mpsc::SyncSender<C>) -> Self {
        Self { sender }
    }

    pub(crate) fn send(&self, command: C) -> Result<(), ShardError> {
        self.sender
            .send(command)
            .map_err(|_| ShardError::SendFailed)
    }

    pub(crate) fn send_stopped(&self, command: C) -> Result<(), ShardError> {
        self.sender
            .send(command)
            .map_err(|_| ShardError::ShardStopped)
    }

    pub(crate) fn try_send(&self, command: C) -> Result<(), ShardError> {
        self.sender.try_send(command).map_err(map_try_send_error)
    }

    pub(crate) fn request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(mpsc::Sender<T>) -> C,
    {
        let (reply, receiver) = reply_channel();
        self.send(build(reply))?;
        Ok(receiver)
    }

    pub(crate) fn try_request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(mpsc::Sender<T>) -> C,
    {
        let (reply, receiver) = reply_channel();
        self.try_send(build(reply))?;
        Ok(receiver)
    }

    pub(crate) fn request_stopped<T, F>(&self, build: F) -> Result<T, ShardError>
    where
        F: FnOnce(mpsc::Sender<T>) -> C,
    {
        let (reply, receiver) = mpsc::channel();
        self.send_stopped(build(reply))?;
        receiver.recv().map_err(|_| ShardError::ReplyFailed)
    }
}

pub(crate) fn bounded_mailbox<C>(capacity: usize) -> (ShardMailbox<C>, mpsc::Receiver<C>) {
    let (sender, receiver) = mpsc::sync_channel(capacity);
    (ShardMailbox::new(sender), receiver)
}

pub(crate) trait HasShardId {
    fn shard_id(&self) -> ShardId;
}

pub(crate) struct ShardSet<H> {
    handles: Vec<H>,
    joins: Vec<thread::JoinHandle<()>>,
    mailbox_capacity: usize,
}

impl<H> ShardSet<H> {
    pub(crate) fn start<C, BuildHandle, RunShard>(
        shard_count: usize,
        mailbox_capacity: usize,
        mut build_handle: BuildHandle,
        run_shard: RunShard,
    ) -> Self
    where
        C: Send + 'static,
        BuildHandle: FnMut(usize, ShardMailbox<C>) -> H,
        RunShard: Fn(mpsc::Receiver<C>) + Copy + Send + 'static,
    {
        let mut handles = Vec::with_capacity(shard_count);
        let mut joins = Vec::with_capacity(shard_count);

        for shard_idx in 0..shard_count {
            let (mailbox, receiver) = bounded_mailbox(mailbox_capacity);
            let join = thread::spawn(move || run_shard(receiver));

            handles.push(build_handle(shard_idx, mailbox));
            joins.push(join);
        }

        Self {
            handles,
            joins,
            mailbox_capacity,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.handles.len()
    }

    pub(crate) fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    pub(crate) fn snapshot(&self, stopped: bool) -> RuntimeSnapshot {
        RuntimeSnapshot {
            shard_count: self.len(),
            mailbox_capacity: self.mailbox_capacity,
            stopped,
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&H> {
        self.handles.get(index)
    }

    pub(crate) fn request_all<T, F>(&self, request: F) -> Result<Vec<T>, ShardError>
    where
        F: FnMut(&H) -> Result<T, ShardError>,
    {
        self.handles.iter().map(request).collect()
    }

    pub(crate) fn join_drained(&mut self) -> Result<(), ShardError> {
        join_all(self.joins.drain(..).collect())
    }

    pub(crate) fn stop_and_join<F>(&mut self, mut stop_one: F) -> Result<(), ShardError>
    where
        F: FnMut(&H) -> Result<(), ShardError>,
    {
        let mut stop_result = Ok(());

        for handle in &self.handles {
            if let Err(error) = stop_one(handle) {
                stop_result = Err(error);
            }
        }

        let join_result = self.join_drained();
        stop_result?;
        join_result
    }
}

impl<H: HasShardId> ShardSet<H> {
    pub(crate) fn get_by_id(&self, shard_id: ShardId) -> Result<&H, ShardError> {
        let handle = self
            .get(shard_id.0)
            .ok_or(ShardError::InvalidShardId(shard_id.0))?;
        debug_assert_eq!(handle.shard_id(), shard_id);
        Ok(handle)
    }

    pub(crate) fn request_all_with_ids<T, F>(
        &self,
        mut request: F,
    ) -> Result<Vec<(ShardId, T)>, ShardError>
    where
        F: FnMut(&H) -> Result<T, ShardError>,
    {
        self.handles
            .iter()
            .map(|handle| Ok((handle.shard_id(), request(handle)?)))
            .collect()
    }
}

pub(crate) fn join_all(joins: Vec<thread::JoinHandle<()>>) -> Result<(), ShardError> {
    let mut join_result = Ok(());

    for join in joins {
        if join.join().is_err() {
            join_result = Err(ShardError::ThreadJoinFailed);
        }
    }

    join_result
}

fn map_try_send_error<C>(error: mpsc::TrySendError<C>) -> ShardError {
    match error {
        mpsc::TrySendError::Full(_) => ShardError::MailboxFull,
        mpsc::TrySendError::Disconnected(_) => ShardError::SendFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::{ShardConfig, DEFAULT_MAILBOX_CAPACITY};
    use crate::ShardError;

    #[test]
    fn shard_config_uses_default_mailbox_capacity() {
        let config = ShardConfig::new(3);

        assert_eq!(config.shard_count, 3);
        assert_eq!(config.mailbox_capacity, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn shard_config_rejects_zero_shards() {
        assert_eq!(
            ShardConfig::new(0).validate().unwrap_err(),
            ShardError::InvalidShardCount
        );
    }

    #[test]
    fn shard_config_rejects_zero_mailbox_capacity() {
        assert_eq!(
            ShardConfig::new(1)
                .with_mailbox_capacity(0)
                .validate()
                .unwrap_err(),
            ShardError::InvalidMailboxCapacity
        );
    }
}
