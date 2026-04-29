use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc;
use std::time::Duration;

use crate::placement::shard_for_hash;
use crate::runtime::{HasShardId, Reply, RuntimeSnapshot, ShardConfig, ShardMailbox, ShardSet};
use crate::{ShardError, ShardId, ShardSnapshot};

/// Reply handle for an accepted key-value command.
pub type KvReply<T> = Reply<T>;

/// Configuration for starting a [`ShardedKv`] instance.
///
/// The shard mailbox capacity is per shard. When a mailbox is full, callers
/// block while sending the command, which provides the first backpressure
/// mechanism without introducing async runtime concepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedKvConfig {
    /// Number of shard threads to start.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
}

impl ShardedKvConfig {
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

impl From<ShardConfig> for ShardedKvConfig {
    fn from(config: ShardConfig) -> Self {
        Self {
            shard_count: config.shard_count,
            mailbox_capacity: config.mailbox_capacity,
        }
    }
}

/// Reply handle for a total length request across all shards.
///
/// Calling [`KvTotalLenReply::wait`] waits for every shard reply and sums the
/// returned lengths.
#[must_use]
#[derive(Debug)]
pub struct KvTotalLenReply {
    replies: Vec<KvReply<usize>>,
}

impl KvTotalLenReply {
    fn new(replies: Vec<KvReply<usize>>) -> Self {
        Self { replies }
    }

    /// Waits for all shard length replies and returns their sum.
    pub fn wait(self) -> Result<usize, ShardError> {
        self.replies
            .into_iter()
            .try_fold(0usize, |total, reply| Ok(total + reply.wait()?))
    }

    /// Waits for all shard length replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<usize, ShardError> {
        self.replies.into_iter().try_fold(0usize, |total, reply| {
            Ok(total + reply.wait_timeout(timeout)?)
        })
    }
}

/// Reply handle for a per-shard snapshot request.
///
/// Calling [`KvShardSnapshotsReply::wait`] waits for every shard reply and
/// returns owned snapshots in shard order.
#[must_use]
#[derive(Debug)]
pub struct KvShardSnapshotsReply {
    replies: Vec<(ShardId, KvReply<usize>)>,
}

impl KvShardSnapshotsReply {
    fn new(replies: Vec<(ShardId, KvReply<usize>)>) -> Self {
        Self { replies }
    }

    /// Waits for all shard snapshot replies.
    pub fn wait(self) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(ShardSnapshot {
                    shard_id,
                    len: reply.wait()?,
                })
            })
            .collect()
    }

    /// Waits for all shard snapshot replies until `timeout` expires for one
    /// reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.replies
            .into_iter()
            .map(|(shard_id, reply)| {
                Ok(ShardSnapshot {
                    shard_id,
                    len: reply.wait_timeout(timeout)?,
                })
            })
            .collect()
    }
}

/// Reply handle for an all-keys request across all shards.
///
/// Calling [`KvAllKeysReply::wait`] waits for every shard reply and returns all
/// keys sorted lexicographically.
#[must_use]
#[derive(Debug)]
pub struct KvAllKeysReply {
    replies: Vec<KvReply<Vec<String>>>,
}

impl KvAllKeysReply {
    fn new(replies: Vec<KvReply<Vec<String>>>) -> Self {
        Self { replies }
    }

    /// Waits for all shard key replies and returns sorted owned keys.
    pub fn wait(self) -> Result<Vec<String>, ShardError> {
        let mut keys = self.replies.into_iter().try_fold(
            Vec::new(),
            |mut keys, reply| -> Result<Vec<String>, ShardError> {
                keys.extend(reply.wait()?);
                Ok(keys)
            },
        )?;
        keys.sort();
        Ok(keys)
    }

    /// Waits for all shard key replies until `timeout` expires for one reply.
    ///
    /// The timeout is applied per pending shard reply.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<String>, ShardError> {
        let mut keys = self.replies.into_iter().try_fold(
            Vec::new(),
            |mut keys, reply| -> Result<Vec<String>, ShardError> {
                keys.extend(reply.wait_timeout(timeout)?);
                Ok(keys)
            },
        )?;
        keys.sort();
        Ok(keys)
    }
}

struct KvService {
    map: HashMap<String, String>,
}

impl KvService {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    fn put(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    fn compare_and_put(&mut self, key: String, expected: Option<String>, value: String) -> bool {
        if self.map.get(&key) == expected.as_ref() {
            self.map.insert(key, value);
            true
        } else {
            false
        }
    }

    fn get_or_put(&mut self, key: String, value: String) -> String {
        self.map.entry(key).or_insert(value).clone()
    }

    fn delete(&mut self, key: String) -> Option<String> {
        self.map.remove(&key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn keys(&self) -> Vec<String> {
        let mut keys = self.map.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        keys
    }
}

enum KvCommand {
    Get {
        key: String,
        reply: mpsc::Sender<Option<String>>,
    },
    Put {
        key: String,
        value: String,
        reply: mpsc::Sender<()>,
    },
    CompareAndPut {
        key: String,
        expected: Option<String>,
        value: String,
        reply: mpsc::Sender<bool>,
    },
    GetOrPut {
        key: String,
        value: String,
        reply: mpsc::Sender<String>,
    },
    Delete {
        key: String,
        reply: mpsc::Sender<Option<String>>,
    },
    Len {
        reply: mpsc::Sender<usize>,
    },
    Keys {
        reply: mpsc::Sender<Vec<String>>,
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

struct KvShardHandle {
    id: ShardId,
    mailbox: ShardMailbox<KvCommand>,
}

impl KvShardHandle {
    fn new(id: ShardId, mailbox: ShardMailbox<KvCommand>) -> Self {
        Self { id, mailbox }
    }

    fn submit_get(&self, key: String) -> Result<KvReply<Option<String>>, ShardError> {
        self.mailbox.request(|reply| KvCommand::Get { key, reply })
    }

    fn try_submit_get(&self, key: String) -> Result<KvReply<Option<String>>, ShardError> {
        self.mailbox
            .try_request(|reply| KvCommand::Get { key, reply })
    }

    fn submit_put(&self, key: String, value: String) -> Result<KvReply<()>, ShardError> {
        self.mailbox
            .request(|reply| KvCommand::Put { key, value, reply })
    }

    fn try_submit_put(&self, key: String, value: String) -> Result<KvReply<()>, ShardError> {
        self.mailbox
            .try_request(|reply| KvCommand::Put { key, value, reply })
    }

    fn submit_compare_and_put(
        &self,
        key: String,
        expected: Option<String>,
        value: String,
    ) -> Result<KvReply<bool>, ShardError> {
        self.mailbox.request(|reply| KvCommand::CompareAndPut {
            key,
            expected,
            value,
            reply,
        })
    }

    fn try_submit_compare_and_put(
        &self,
        key: String,
        expected: Option<String>,
        value: String,
    ) -> Result<KvReply<bool>, ShardError> {
        self.mailbox.try_request(|reply| KvCommand::CompareAndPut {
            key,
            expected,
            value,
            reply,
        })
    }

    fn submit_get_or_put(&self, key: String, value: String) -> Result<KvReply<String>, ShardError> {
        self.mailbox
            .request(|reply| KvCommand::GetOrPut { key, value, reply })
    }

    fn try_submit_get_or_put(
        &self,
        key: String,
        value: String,
    ) -> Result<KvReply<String>, ShardError> {
        self.mailbox
            .try_request(|reply| KvCommand::GetOrPut { key, value, reply })
    }

    fn submit_delete(&self, key: String) -> Result<KvReply<Option<String>>, ShardError> {
        self.mailbox
            .request(|reply| KvCommand::Delete { key, reply })
    }

    fn try_submit_delete(&self, key: String) -> Result<KvReply<Option<String>>, ShardError> {
        self.mailbox
            .try_request(|reply| KvCommand::Delete { key, reply })
    }

    fn send_len(&self) -> Result<usize, ShardError> {
        self.submit_len()?.wait()
    }

    fn submit_len(&self) -> Result<KvReply<usize>, ShardError> {
        self.mailbox.request(|reply| KvCommand::Len { reply })
    }

    fn try_submit_len(&self) -> Result<KvReply<usize>, ShardError> {
        self.mailbox.try_request(|reply| KvCommand::Len { reply })
    }

    fn submit_keys(&self) -> Result<KvReply<Vec<String>>, ShardError> {
        self.mailbox.request(|reply| KvCommand::Keys { reply })
    }

    fn try_submit_keys(&self) -> Result<KvReply<Vec<String>>, ShardError> {
        self.mailbox.try_request(|reply| KvCommand::Keys { reply })
    }

    fn send_stop(&self) -> Result<(), ShardError> {
        self.mailbox
            .request_stopped(|reply| KvCommand::Stop { reply })
    }

    #[cfg(test)]
    fn send_hold(&self) -> Result<(mpsc::Sender<()>, mpsc::Receiver<()>), ShardError> {
        let (release_sender, release_receiver) = mpsc::channel();
        let (started_sender, started_receiver) = mpsc::channel();

        self.mailbox.send(KvCommand::Hold {
            release: release_receiver,
            started: started_sender,
        })?;

        Ok((release_sender, started_receiver))
    }
}

impl HasShardId for KvShardHandle {
    fn shard_id(&self) -> ShardId {
        self.id
    }
}

/// A sharded key-value store with one owning thread per shard.
///
/// Each shard owns its local `KvService` state and receives typed commands over
/// a standard-library channel. Public methods route keys to the owning shard and
/// then block until that shard replies.
///
/// ```
/// use shardstar::ShardedKv;
///
/// let kv = ShardedKv::start(2)?;
///
/// kv.put("alpha", "one")?;
/// assert_eq!(kv.get("alpha")?, Some("one".to_string()));
///
/// kv.stop()?;
/// # Ok::<(), shardstar::ShardError>(())
/// ```
pub struct ShardedKv {
    shards: ShardSet<KvShardHandle>,
    stopped: bool,
}

impl fmt::Debug for ShardedKv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedKv")
            .field("shard_count", &self.shard_count())
            .field("mailbox_capacity", &self.mailbox_capacity())
            .field("stopped", &self.stopped)
            .finish_non_exhaustive()
    }
}

impl ShardedKv {
    /// Starts a sharded key-value store with `shard_count` shard threads.
    ///
    /// Returns [`ShardError::InvalidShardCount`] when `shard_count` is zero.
    ///
    /// ```
    /// use shardstar::{ShardError, ShardedKv};
    ///
    /// assert_eq!(
    ///     ShardedKv::start(0).unwrap_err(),
    ///     ShardError::InvalidShardCount
    /// );
    /// ```
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        Self::start_with_config(ShardedKvConfig::new(shard_count))
    }

    /// Starts a sharded key-value store from an explicit configuration.
    ///
    /// Command mailboxes are bounded. A full mailbox blocks callers at send
    /// time until the owning shard drains capacity.
    pub fn start_with_config(config: ShardedKvConfig) -> Result<Self, ShardError> {
        let config = config.runtime_config()?;

        let shards = ShardSet::start(
            config.shard_count,
            config.mailbox_capacity,
            |shard_idx, mailbox| KvShardHandle::new(ShardId(shard_idx), mailbox),
            run_kv_shard,
        );

        Ok(Self {
            shards,
            stopped: false,
        })
    }

    /// Returns the number of shards in this store.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the bounded mailbox capacity configured for each shard.
    pub fn mailbox_capacity(&self) -> usize {
        self.shards.mailbox_capacity()
    }

    /// Returns an owned snapshot of this store's runtime shape.
    pub fn runtime_snapshot(&self) -> RuntimeSnapshot {
        self.shards.snapshot(self.stopped)
    }

    /// Returns the shard that owns `key`.
    ///
    /// The exact shard ID is intentionally not a stable public contract.
    pub fn shard_for_key(&self, key: &str) -> ShardId {
        shard_for_hash(&key, self.shard_count())
    }

    /// Inserts or replaces a key-value pair on the shard that owns `key`.
    pub fn put(&self, key: impl Into<String>, value: impl Into<String>) -> Result<(), ShardError> {
        self.submit_put(key, value)?.wait()
    }

    /// Attempts to insert or replace a key-value pair without waiting for
    /// mailbox capacity.
    ///
    /// If the owning shard mailbox is full, this returns
    /// [`ShardError::MailboxFull`]. If the command is accepted, this method
    /// still blocks waiting for the shard's reply.
    pub fn try_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ShardError> {
        self.try_submit_put(key, value)?.wait()
    }

    /// Enqueues a put command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<()>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.submit_put(key, value.into())
    }

    /// Attempts to enqueue a put command and return a reply handle without
    /// waiting for mailbox capacity.
    pub fn try_submit_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<()>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.try_submit_put(key, value.into())
    }

    /// Atomically replaces a key only if its current value matches `expected`.
    ///
    /// This comparison and mutation run inside the owning shard. Use
    /// `expected = None` to insert only when the key is currently absent.
    pub fn compare_and_put(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<bool, ShardError> {
        self.submit_compare_and_put(key, expected, value)?.wait()
    }

    /// Attempts an atomic compare-and-put without waiting for mailbox capacity.
    ///
    /// If the owning shard mailbox is full, this returns
    /// [`ShardError::MailboxFull`]. If the command is accepted, this method
    /// still blocks waiting for the shard's reply.
    pub fn try_compare_and_put(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<bool, ShardError> {
        self.try_submit_compare_and_put(key, expected, value)?
            .wait()
    }

    /// Enqueues an atomic compare-and-put command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_compare_and_put(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<bool>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.submit_compare_and_put(key, expected, value.into())
    }

    /// Attempts to enqueue an atomic compare-and-put command and return a reply
    /// handle without waiting for mailbox capacity.
    pub fn try_submit_compare_and_put(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<bool>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.try_submit_compare_and_put(key, expected, value.into())
    }

    /// Returns the existing value for `key`, or inserts and returns `value`
    /// when the key is absent.
    ///
    /// The lookup and optional insertion run inside the owning shard.
    pub fn get_or_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<String, ShardError> {
        self.submit_get_or_put(key, value)?.wait()
    }

    /// Attempts a shard-local get-or-put without waiting for mailbox capacity.
    ///
    /// If the owning shard mailbox is full, this returns
    /// [`ShardError::MailboxFull`]. If the command is accepted, this method
    /// still blocks waiting for the shard's reply.
    pub fn try_get_or_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<String, ShardError> {
        self.try_submit_get_or_put(key, value)?.wait()
    }

    /// Enqueues a shard-local get-or-put command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_get_or_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<String>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.submit_get_or_put(key, value.into())
    }

    /// Attempts to enqueue a shard-local get-or-put command and return a reply
    /// handle without waiting for mailbox capacity.
    pub fn try_submit_get_or_put(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<KvReply<String>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.try_submit_get_or_put(key, value.into())
    }

    /// Gets an owned value from the shard that owns `key`.
    pub fn get(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.submit_get(key)?.wait()
    }

    /// Attempts to get a value without waiting for mailbox capacity.
    ///
    /// If the owning shard mailbox is full, this returns
    /// [`ShardError::MailboxFull`]. If the command is accepted, this method
    /// still blocks waiting for the shard's reply.
    pub fn try_get(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.try_submit_get(key)?.wait()
    }

    /// Enqueues a get command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_get(
        &self,
        key: impl Into<String>,
    ) -> Result<KvReply<Option<String>>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.submit_get(key)
    }

    /// Attempts to enqueue a get command and return a reply handle without
    /// waiting for mailbox capacity.
    pub fn try_submit_get(
        &self,
        key: impl Into<String>,
    ) -> Result<KvReply<Option<String>>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.try_submit_get(key)
    }

    /// Deletes a key from the shard that owns it and returns the previous value.
    pub fn delete(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.submit_delete(key)?.wait()
    }

    /// Attempts to delete a key without waiting for mailbox capacity.
    ///
    /// If the owning shard mailbox is full, this returns
    /// [`ShardError::MailboxFull`]. If the command is accepted, this method
    /// still blocks waiting for the shard's reply.
    pub fn try_delete(&self, key: impl Into<String>) -> Result<Option<String>, ShardError> {
        self.try_submit_delete(key)?.wait()
    }

    /// Enqueues a delete command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_delete(
        &self,
        key: impl Into<String>,
    ) -> Result<KvReply<Option<String>>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.submit_delete(key)
    }

    /// Attempts to enqueue a delete command and return a reply handle without
    /// waiting for mailbox capacity.
    pub fn try_submit_delete(
        &self,
        key: impl Into<String>,
    ) -> Result<KvReply<Option<String>>, ShardError> {
        let key = key.into();
        let shard = self.shard_for_owned_key(&key)?;
        shard.try_submit_delete(key)
    }

    /// Returns the number of keys stored on a specific shard.
    pub fn len_on_shard(&self, shard_id: ShardId) -> Result<usize, ShardError> {
        self.shard(shard_id)?.send_len()
    }

    /// Returns sorted owned keys stored on a specific shard.
    pub fn keys_on_shard(&self, shard_id: ShardId) -> Result<Vec<String>, ShardError> {
        self.submit_keys_on_shard(shard_id)?.wait()
    }

    /// Attempts to read a shard length without waiting for mailbox capacity.
    ///
    /// If the shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// If the command is accepted, this method still blocks waiting for the
    /// shard's reply.
    pub fn try_len_on_shard(&self, shard_id: ShardId) -> Result<usize, ShardError> {
        self.try_submit_len_on_shard(shard_id)?.wait()
    }

    /// Attempts to return sorted owned keys from a specific shard without
    /// waiting for mailbox capacity.
    pub fn try_keys_on_shard(&self, shard_id: ShardId) -> Result<Vec<String>, ShardError> {
        self.try_submit_keys_on_shard(shard_id)?.wait()
    }

    /// Enqueues a shard length command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_len_on_shard(&self, shard_id: ShardId) -> Result<KvReply<usize>, ShardError> {
        self.shard(shard_id)?.submit_len()
    }

    /// Enqueues a shard keys command and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for the shard to execute the command.
    pub fn submit_keys_on_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<KvReply<Vec<String>>, ShardError> {
        self.shard(shard_id)?.submit_keys()
    }

    /// Attempts to enqueue a shard length command and return a reply handle
    /// without waiting for mailbox capacity.
    pub fn try_submit_len_on_shard(&self, shard_id: ShardId) -> Result<KvReply<usize>, ShardError> {
        self.shard(shard_id)?.try_submit_len()
    }

    /// Attempts to enqueue a shard keys command and return a reply handle
    /// without waiting for mailbox capacity.
    pub fn try_submit_keys_on_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<KvReply<Vec<String>>, ShardError> {
        self.shard(shard_id)?.try_submit_keys()
    }

    /// Returns the total number of keys stored across all shards.
    pub fn total_len(&self) -> Result<usize, ShardError> {
        self.submit_total_len()?.wait()
    }

    /// Returns owned per-shard snapshots in shard order.
    pub fn shard_snapshots(&self) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.submit_shard_snapshots()?.wait()
    }

    /// Returns all keys sorted lexicographically.
    pub fn all_keys(&self) -> Result<Vec<String>, ShardError> {
        self.submit_all_keys()?.wait()
    }

    /// Attempts to return the total number of keys without waiting for mailbox
    /// capacity on any shard.
    ///
    /// If any shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// Accepted commands still block waiting for their shard replies.
    pub fn try_total_len(&self) -> Result<usize, ShardError> {
        self.try_submit_total_len()?.wait()
    }

    /// Attempts to return owned per-shard snapshots without waiting for mailbox
    /// capacity on any shard.
    ///
    /// If any shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// Accepted commands still block waiting for their shard replies.
    pub fn try_shard_snapshots(&self) -> Result<Vec<ShardSnapshot>, ShardError> {
        self.try_submit_shard_snapshots()?.wait()
    }

    /// Attempts to return all keys without waiting for mailbox capacity on any
    /// shard.
    ///
    /// If any shard mailbox is full, this returns [`ShardError::MailboxFull`].
    /// Accepted commands still block waiting for their shard replies.
    pub fn try_all_keys(&self) -> Result<Vec<String>, ShardError> {
        self.try_submit_all_keys()?.wait()
    }

    /// Enqueues length commands to all shards and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for shards to execute the commands.
    pub fn submit_total_len(&self) -> Result<KvTotalLenReply, ShardError> {
        self.ensure_running()?;
        let replies = self.shards.request_all(KvShardHandle::submit_len)?;
        Ok(KvTotalLenReply::new(replies))
    }

    /// Enqueues snapshot commands to all shards and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for shards to execute the commands.
    pub fn submit_shard_snapshots(&self) -> Result<KvShardSnapshotsReply, ShardError> {
        self.ensure_running()?;
        let replies = self
            .shards
            .request_all_with_ids(KvShardHandle::submit_len)?;
        Ok(KvShardSnapshotsReply::new(replies))
    }

    /// Enqueues key snapshot commands to all shards and returns a reply handle.
    ///
    /// This method may block while waiting for bounded mailbox capacity, but it
    /// does not wait for shards to execute the commands.
    pub fn submit_all_keys(&self) -> Result<KvAllKeysReply, ShardError> {
        self.ensure_running()?;
        let replies = self.shards.request_all(KvShardHandle::submit_keys)?;
        Ok(KvAllKeysReply::new(replies))
    }

    /// Attempts to enqueue length commands to all shards and return a reply
    /// handle without waiting for mailbox capacity.
    ///
    /// If this returns [`ShardError::MailboxFull`], earlier shard length
    /// commands may already have been accepted. Those commands are read-only.
    pub fn try_submit_total_len(&self) -> Result<KvTotalLenReply, ShardError> {
        self.ensure_running()?;
        let replies = self.shards.request_all(KvShardHandle::try_submit_len)?;
        Ok(KvTotalLenReply::new(replies))
    }

    /// Attempts to enqueue snapshot commands to all shards and return a reply
    /// handle without waiting for mailbox capacity.
    ///
    /// If this returns [`ShardError::MailboxFull`], earlier shard length
    /// commands may already have been accepted. Those commands are read-only.
    pub fn try_submit_shard_snapshots(&self) -> Result<KvShardSnapshotsReply, ShardError> {
        self.ensure_running()?;
        let replies = self
            .shards
            .request_all_with_ids(KvShardHandle::try_submit_len)?;
        Ok(KvShardSnapshotsReply::new(replies))
    }

    /// Attempts to enqueue key snapshot commands to all shards and return a
    /// reply handle without waiting for mailbox capacity.
    ///
    /// If this returns [`ShardError::MailboxFull`], earlier shard key snapshot
    /// commands may already have been accepted. Those commands are read-only.
    pub fn try_submit_all_keys(&self) -> Result<KvAllKeysReply, ShardError> {
        self.ensure_running()?;
        let replies = self.shards.request_all(KvShardHandle::try_submit_keys)?;
        Ok(KvAllKeysReply::new(replies))
    }

    /// Stops all shards and joins their threads.
    ///
    /// This consumes the store handle so callers cannot issue more requests
    /// after shutdown.
    pub fn stop(mut self) -> Result<(), ShardError> {
        self.shutdown()
    }

    /// Stops all shards and joins their threads while retaining the handle.
    ///
    /// This is useful when callers need to inspect [`ShardedKv::runtime_snapshot`]
    /// after shutdown. Calling this more than once is a no-op after the first
    /// successful shutdown.
    pub fn shutdown(&mut self) -> Result<(), ShardError> {
        if self.stopped {
            return Ok(());
        }

        self.stopped = true;

        self.shards.stop_and_join(KvShardHandle::send_stop)
    }

    fn ensure_running(&self) -> Result<(), ShardError> {
        if self.stopped {
            Err(ShardError::ShardStopped)
        } else {
            Ok(())
        }
    }

    fn shard_for_owned_key(&self, key: &str) -> Result<&KvShardHandle, ShardError> {
        self.ensure_running()?;
        let shard_id = self.shard_for_key(key);
        Ok(self
            .shards
            .get(shard_id.0)
            .expect("placement returned an invalid shard id"))
    }

    fn shard(&self, shard_id: ShardId) -> Result<&KvShardHandle, ShardError> {
        self.ensure_running()?;
        self.shards.get_by_id(shard_id)
    }
}

impl Drop for ShardedKv {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_kv_shard(receiver: mpsc::Receiver<KvCommand>) {
    let mut service = KvService::new();

    loop {
        match receiver.recv() {
            Ok(KvCommand::Get { key, reply }) => {
                let value = service.get(key);
                let _ = reply.send(value);
            }
            Ok(KvCommand::Put { key, value, reply }) => {
                service.put(key, value);
                let _ = reply.send(());
            }
            Ok(KvCommand::CompareAndPut {
                key,
                expected,
                value,
                reply,
            }) => {
                let replaced = service.compare_and_put(key, expected, value);
                let _ = reply.send(replaced);
            }
            Ok(KvCommand::GetOrPut { key, value, reply }) => {
                let value = service.get_or_put(key, value);
                let _ = reply.send(value);
            }
            Ok(KvCommand::Delete { key, reply }) => {
                let value = service.delete(key);
                let _ = reply.send(value);
            }
            Ok(KvCommand::Len { reply }) => {
                let _ = reply.send(service.len());
            }
            Ok(KvCommand::Keys { reply }) => {
                let _ = reply.send(service.keys());
            }
            Ok(KvCommand::Stop { reply }) => {
                let _ = reply.send(());
                break;
            }
            #[cfg(test)]
            Ok(KvCommand::Hold { release, started }) => {
                let _ = started.send(());
                let _ = release.recv();
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KvCommand, KvService, ShardedKv, ShardedKvConfig};
    use crate::{ShardError, ShardId, DEFAULT_MAILBOX_CAPACITY};
    use std::time::Duration;

    #[test]
    fn kv_service_starts_empty() {
        let service = KvService::new();

        assert_eq!(service.len(), 0);
    }

    #[test]
    fn kv_service_put_get_delete_round_trip() {
        let mut service = KvService::new();

        service.put("alpha".to_string(), "one".to_string());

        assert_eq!(service.len(), 1);
        assert_eq!(service.get("alpha".to_string()), Some("one".to_string()));
        assert_eq!(service.delete("alpha".to_string()), Some("one".to_string()));
        assert_eq!(service.get("alpha".to_string()), None);
        assert_eq!(service.len(), 0);
    }

    #[test]
    fn kv_service_overwrite_does_not_increase_len() {
        let mut service = KvService::new();

        service.put("alpha".to_string(), "one".to_string());
        service.put("alpha".to_string(), "updated".to_string());

        assert_eq!(service.len(), 1);
        assert_eq!(
            service.get("alpha".to_string()),
            Some("updated".to_string())
        );
    }

    #[test]
    fn kv_service_compare_and_put_is_atomic_local_logic() {
        let mut service = KvService::new();

        assert!(service.compare_and_put("alpha".to_string(), None, "one".to_string()));
        assert_eq!(service.get("alpha".to_string()), Some("one".to_string()));
        assert!(!service.compare_and_put(
            "alpha".to_string(),
            Some("wrong".to_string()),
            "two".to_string()
        ));
        assert_eq!(service.get("alpha".to_string()), Some("one".to_string()));
        assert!(service.compare_and_put(
            "alpha".to_string(),
            Some("one".to_string()),
            "two".to_string()
        ));
        assert_eq!(service.get("alpha".to_string()), Some("two".to_string()));
    }

    #[test]
    fn kv_service_get_or_put_returns_existing_or_inserted_value() {
        let mut service = KvService::new();

        assert_eq!(
            service.get_or_put("alpha".to_string(), "one".to_string()),
            "one".to_string()
        );
        assert_eq!(
            service.get_or_put("alpha".to_string(), "two".to_string()),
            "one".to_string()
        );
        assert_eq!(service.get("alpha".to_string()), Some("one".to_string()));
        assert_eq!(service.len(), 1);
    }

    #[test]
    fn kv_service_keys_are_sorted_owned_values() {
        let mut service = KvService::new();

        service.put("gamma".to_string(), "three".to_string());
        service.put("alpha".to_string(), "one".to_string());
        service.put("beta".to_string(), "two".to_string());

        assert_eq!(
            service.keys(),
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );
    }

    #[test]
    fn sharded_kv_config_uses_default_mailbox_capacity() {
        let config = ShardedKvConfig::new(4);

        assert_eq!(config.shard_count, 4);
        assert_eq!(config.mailbox_capacity, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn sharded_kv_config_accepts_custom_mailbox_capacity() {
        let config = ShardedKvConfig::new(4).with_mailbox_capacity(8);

        assert_eq!(config.shard_count, 4);
        assert_eq!(config.mailbox_capacity, 8);
    }

    #[test]
    fn try_methods_return_mailbox_full_without_waiting_for_capacity() {
        let kv =
            ShardedKv::start_with_config(ShardedKvConfig::new(1).with_mailbox_capacity(1)).unwrap();
        let shard = kv.shards.get(0).unwrap();
        let (release, started) = shard.send_hold().unwrap();

        started.recv().unwrap();

        shard
            .mailbox
            .try_send(KvCommand::Len {
                reply: std::sync::mpsc::channel().0,
            })
            .unwrap();

        assert_eq!(kv.try_put("alpha", "one"), Err(ShardError::MailboxFull));
        assert_eq!(kv.try_get("alpha"), Err(ShardError::MailboxFull));
        assert_eq!(kv.try_delete("alpha"), Err(ShardError::MailboxFull));
        assert_eq!(
            kv.try_len_on_shard(ShardId(0)),
            Err(ShardError::MailboxFull)
        );
        assert_eq!(kv.try_total_len(), Err(ShardError::MailboxFull));

        release.send(()).unwrap();
        kv.stop().unwrap();
    }

    #[test]
    fn reply_wait_timeout_returns_timeout_when_shard_has_not_replied() {
        let kv =
            ShardedKv::start_with_config(ShardedKvConfig::new(1).with_mailbox_capacity(2)).unwrap();
        let (release, started) = kv.shards.get(0).unwrap().send_hold().unwrap();

        started.recv().unwrap();

        let reply = kv.submit_len_on_shard(ShardId(0)).unwrap();

        assert_eq!(
            reply.wait_timeout(Duration::from_millis(1)),
            Err(ShardError::ReplyTimeout)
        );

        release.send(()).unwrap();
        kv.stop().unwrap();
    }
}
