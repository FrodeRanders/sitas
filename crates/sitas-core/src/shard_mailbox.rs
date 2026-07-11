//! Typed owned-message transfer between executor shards.
//!
//! A [`ShardMailboxSet`] owns one bounded inbound mailbox per shard. Producers
//! send owned messages to a target shard through [`ShardSender`], and the
//! target shard drains its single [`ShardReceiver`]. A [`WorkUnitMailboxSet`]
//! maps logical work-unit names to shard-assigned receivers for non-uniform
//! work assignment. [`UniformShardRouter`] and [`WorkUnitRouter`] make the
//! key-discriminator target explicit. The mailbox transfers ownership; it does
//! not share mutable application state between shards.

use alloc::collections::{HashMap, VecDeque};
use core::fmt;
use core::future::Future;
use std::hash::Hash;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use core::task::{Context, Poll, Waker};

use crate::placement::shard_for_hash;
use crate::shard::ShardId;
use crate::sharded_executor::{ShardedSubmitter, current_executor_shard};

/// Configuration for a [`ShardMailboxSet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMailboxConfig {
    capacity_per_shard: usize,
}

impl ShardMailboxConfig {
    /// Creates a mailbox configuration with the given per-shard capacity.
    pub fn new(capacity_per_shard: usize) -> Self {
        Self { capacity_per_shard }
    }

    /// Returns the configured per-shard inbound capacity.
    pub fn capacity_per_shard(&self) -> usize {
        self.capacity_per_shard
    }
}

/// Error returned when creating a key router fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRouterCreateError {
    /// The router needs at least one target.
    ZeroTargets,
}

impl fmt::Display for KeyRouterCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTargets => write!(f, "key router requires at least one target"),
        }
    }
}

impl core::error::Error for KeyRouterCreateError {}

/// Routes a key to a target.
///
/// Uniform routing uses [`ShardId`] targets. Non-uniform logical routing can
/// use work-unit names as targets.
pub trait RouteByKey<K: ?Sized> {
    /// Target selected for the key.
    type Target;

    /// Routes `key` to one target.
    fn route(&self, key: &K) -> Self::Target;
}

/// Uniform key router that maps keys directly to physical shard IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniformShardRouter {
    shard_count: usize,
}

impl UniformShardRouter {
    /// Creates a uniform shard router for `shard_count` shards.
    pub fn new(shard_count: usize) -> Result<Self, KeyRouterCreateError> {
        if shard_count == 0 {
            return Err(KeyRouterCreateError::ZeroTargets);
        }
        Ok(Self { shard_count })
    }

    /// Creates a uniform shard router from a sharded submitter.
    pub fn for_submitter(submitter: &ShardedSubmitter) -> Result<Self, KeyRouterCreateError> {
        Self::new(submitter.shard_count())
    }

    /// Returns the number of shard targets.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }
}

impl<K> RouteByKey<K> for UniformShardRouter
where
    K: Hash + ?Sized,
{
    type Target = ShardId;

    fn route(&self, key: &K) -> Self::Target {
        shard_for_hash(key, self.shard_count)
    }
}

/// Key router that maps keys to logical work-unit names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkUnitRouter<N> {
    work_units: Vec<N>,
}

impl<N> WorkUnitRouter<N> {
    /// Creates a router over the provided logical work-unit names.
    pub fn new(work_units: impl IntoIterator<Item = N>) -> Result<Self, KeyRouterCreateError> {
        let work_units: Vec<N> = work_units.into_iter().collect();
        if work_units.is_empty() {
            return Err(KeyRouterCreateError::ZeroTargets);
        }
        Ok(Self { work_units })
    }

    /// Returns the number of logical targets.
    pub fn work_unit_count(&self) -> usize {
        self.work_units.len()
    }

    /// Returns the configured logical targets in routing order.
    pub fn work_units(&self) -> &[N] {
        &self.work_units
    }
}

impl<K, N> RouteByKey<K> for WorkUnitRouter<N>
where
    K: Hash + ?Sized,
    N: Clone,
{
    type Target = N;

    fn route(&self, key: &K) -> Self::Target {
        let target = shard_for_hash(key, self.work_units.len()).0;
        self.work_units[target].clone()
    }
}

/// Error returned when constructing a [`ShardMailboxSet`] fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardMailboxCreateError {
    /// At least one shard is required.
    ZeroShards,
    /// Per-shard mailbox capacity must be greater than zero.
    ZeroCapacity,
}

impl fmt::Display for ShardMailboxCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroShards => write!(f, "shard mailbox set requires at least one shard"),
            Self::ZeroCapacity => write!(f, "shard mailbox capacity must be greater than zero"),
        }
    }
}

impl core::error::Error for ShardMailboxCreateError {}

/// Error returned when addressing a mailbox shard fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardMailboxAddressError {
    /// The requested shard does not exist in this mailbox set.
    InvalidShard {
        /// The invalid shard identifier.
        shard_id: ShardId,
    },
    /// The current task is not running on a sharded executor shard.
    NotOnShard,
    /// The requested shard's single receiver has already been taken.
    ReceiverAlreadyTaken {
        /// The shard whose receiver was already taken.
        shard_id: ShardId,
    },
}

impl fmt::Display for ShardMailboxAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShard { shard_id } => write!(f, "invalid shard {}", shard_id.0),
            Self::NotOnShard => write!(f, "current task is not running on a shard"),
            Self::ReceiverAlreadyTaken { shard_id } => {
                write!(
                    f,
                    "receiver for shard {} has already been taken",
                    shard_id.0
                )
            }
        }
    }
}

impl core::error::Error for ShardMailboxAddressError {}

/// Error returned by [`ShardSender::try_send`].
pub enum ShardSendError<M> {
    /// The target mailbox is full. The original message is returned.
    Full(M),
    /// The target receiver is closed. The original message is returned.
    Closed(M),
}

impl<M> ShardSendError<M> {
    /// Returns the message carried by this error.
    pub fn into_message(self) -> M {
        match self {
            Self::Full(message) | Self::Closed(message) => message,
        }
    }
}

impl<M> fmt::Debug for ShardSendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.debug_tuple("Full").field(&"<message>").finish(),
            Self::Closed(_) => f.debug_tuple("Closed").field(&"<message>").finish(),
        }
    }
}

impl<M> fmt::Display for ShardSendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => write!(f, "shard mailbox is full"),
            Self::Closed(_) => write!(f, "shard mailbox is closed"),
        }
    }
}

impl<M> core::error::Error for ShardSendError<M> {}

/// Error returned by [`ShardReceiver::try_recv`] and [`ShardReceiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRecvError {
    /// The mailbox is currently empty but may receive more messages.
    Empty,
    /// The mailbox is closed and no queued messages remain.
    Closed,
}

impl fmt::Display for ShardRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "shard mailbox is empty"),
            Self::Closed => write!(f, "shard mailbox is closed"),
        }
    }
}

impl core::error::Error for ShardRecvError {}

/// Owned point-in-time snapshot of one shard mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMailboxSnapshot {
    /// The shard that owns this inbound mailbox.
    pub shard_id: ShardId,
    /// Maximum number of queued messages.
    pub capacity: usize,
    /// Current number of queued messages.
    pub len: usize,
    /// Number of live sender handles.
    pub sender_count: usize,
    /// Whether the single receiver has been taken.
    pub receiver_taken: bool,
    /// Whether the receiver side has been closed.
    pub receiver_closed: bool,
    /// Total messages accepted by this mailbox.
    pub sent: u64,
    /// Total messages received from this mailbox.
    pub received: u64,
    /// Total send attempts rejected because the mailbox was full.
    pub full_rejections: u64,
    /// Total send attempts rejected because the receiver was closed.
    pub closed_rejections: u64,
    /// Number of senders currently parked waiting for capacity.
    pub send_waiter_count: usize,
}

/// A single bounded inbound mailbox owned by one shard.
pub struct ShardMailbox<M> {
    shared: Arc<SharedMailbox<M>>,
}

impl<M> ShardMailbox<M> {
    fn new(shard_id: ShardId, capacity: usize) -> Self {
        Self {
            shared: Arc::new(SharedMailbox {
                shard_id,
                state: Mutex::new(MailboxState {
                    queue: VecDeque::with_capacity(capacity),
                    capacity,
                    sender_count: 0,
                    sender_factory_open: true,
                    receiver_taken: false,
                    receiver_closed: false,
                    recv_waker: None,
                    send_wakers: VecDeque::new(),
                }),
                sent: AtomicU64::new(0),
                received: AtomicU64::new(0),
                full_rejections: AtomicU64::new(0),
                closed_rejections: AtomicU64::new(0),
            }),
        }
    }

    /// Returns this mailbox's owning shard.
    pub fn shard_id(&self) -> ShardId {
        self.shared.shard_id
    }

    /// Returns an owned snapshot of this mailbox.
    pub fn snapshot(&self) -> ShardMailboxSnapshot {
        self.shared.snapshot()
    }
}

impl<M> fmt::Debug for ShardMailbox<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardMailbox")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// One mailbox per shard for a single message type.
pub struct ShardMailboxSet<M> {
    mailboxes: Vec<ShardMailbox<M>>,
}

impl<M: Send + 'static> ShardMailboxSet<M> {
    /// Creates a mailbox set sized to the shard count of `submitter`.
    pub fn new(
        submitter: &ShardedSubmitter,
        config: ShardMailboxConfig,
    ) -> Result<Self, ShardMailboxCreateError> {
        Self::with_shard_count(submitter.shard_count(), config)
    }

    /// Creates a mailbox set with a fixed shard count.
    pub fn with_shard_count(
        shard_count: usize,
        config: ShardMailboxConfig,
    ) -> Result<Self, ShardMailboxCreateError> {
        if shard_count == 0 {
            return Err(ShardMailboxCreateError::ZeroShards);
        }
        if config.capacity_per_shard == 0 {
            return Err(ShardMailboxCreateError::ZeroCapacity);
        }

        let mailboxes = (0..shard_count)
            .map(|idx| ShardMailbox::new(ShardId(idx), config.capacity_per_shard))
            .collect();
        Ok(Self { mailboxes })
    }

    /// Returns the number of mailbox shards.
    pub fn shard_count(&self) -> usize {
        self.mailboxes.len()
    }

    /// Returns a sender for `shard_id`.
    pub fn sender_to(&self, shard_id: ShardId) -> Result<ShardSender<M>, ShardMailboxAddressError> {
        let mailbox = self
            .mailboxes
            .get(shard_id.0)
            .ok_or(ShardMailboxAddressError::InvalidShard { shard_id })?;

        {
            let mut state = mailbox.shared.state.lock().expect("mailbox mutex poisoned");
            state.sender_count += 1;
        }

        Ok(ShardSender {
            shared: Arc::clone(&mailbox.shared),
        })
    }

    /// Takes the receiver for the currently executing shard.
    pub fn receiver_for_current_shard(&self) -> Result<ShardReceiver<M>, ShardMailboxAddressError> {
        let shard_id = current_executor_shard().ok_or(ShardMailboxAddressError::NotOnShard)?;
        self.receiver_for(shard_id)
    }

    /// Takes the single receiver for `shard_id`.
    pub fn receiver_for(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardReceiver<M>, ShardMailboxAddressError> {
        let mailbox = self
            .mailboxes
            .get(shard_id.0)
            .ok_or(ShardMailboxAddressError::InvalidShard { shard_id })?;

        let mut state = mailbox.shared.state.lock().expect("mailbox mutex poisoned");
        if state.receiver_taken {
            return Err(ShardMailboxAddressError::ReceiverAlreadyTaken { shard_id });
        }
        state.receiver_taken = true;

        Ok(ShardReceiver {
            shared: Arc::clone(&mailbox.shared),
        })
    }

    /// Returns owned snapshots for all shard mailboxes.
    pub fn snapshots(&self) -> Vec<ShardMailboxSnapshot> {
        self.mailboxes.iter().map(ShardMailbox::snapshot).collect()
    }

    /// Returns an owned snapshot for `shard_id`.
    pub fn snapshot(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardMailboxSnapshot, ShardMailboxAddressError> {
        self.mailboxes
            .get(shard_id.0)
            .map(ShardMailbox::snapshot)
            .ok_or(ShardMailboxAddressError::InvalidShard { shard_id })
    }
}

impl<M> fmt::Debug for ShardMailboxSet<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardMailboxSet")
            .field("shard_count", &self.mailboxes.len())
            .finish()
    }
}

impl<M> Drop for ShardMailboxSet<M> {
    fn drop(&mut self) {
        for mailbox in &self.mailboxes {
            mailbox.shared.close_sender_factory();
        }
    }
}

/// Logical work-unit placement for a named mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkUnitSpec<N> {
    /// Logical work-unit name.
    pub name: N,
    /// Executor shard assigned to run this work unit's receiver.
    pub shard_id: ShardId,
}

impl<N> WorkUnitSpec<N> {
    /// Creates a named work-unit placement.
    pub fn new(name: N, shard_id: ShardId) -> Self {
        Self { name, shard_id }
    }
}

/// Error returned when constructing a [`WorkUnitMailboxSet`] fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkUnitMailboxCreateError<N> {
    /// At least one work unit is required.
    NoWorkUnits,
    /// Per-work-unit mailbox capacity must be greater than zero.
    ZeroCapacity,
    /// A work unit was assigned to a shard outside the runtime.
    InvalidShard {
        /// The work-unit name.
        name: N,
        /// The invalid assigned shard.
        shard_id: ShardId,
    },
    /// A work-unit name appeared more than once.
    DuplicateName {
        /// The duplicate work-unit name.
        name: N,
    },
}

impl<N> fmt::Display for WorkUnitMailboxCreateError<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoWorkUnits => write!(f, "work-unit mailbox set requires at least one work unit"),
            Self::ZeroCapacity => write!(f, "work-unit mailbox capacity must be greater than zero"),
            Self::InvalidShard { shard_id, .. } => {
                write!(f, "work unit assigned to invalid shard {}", shard_id.0)
            }
            Self::DuplicateName { .. } => write!(f, "duplicate work-unit name"),
        }
    }
}

impl<N: fmt::Debug> core::error::Error for WorkUnitMailboxCreateError<N> {}

/// Error returned when addressing a named work-unit mailbox fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkUnitMailboxAddressError<N> {
    /// No work unit exists for the requested name.
    UnknownWorkUnit {
        /// The unknown work-unit name.
        name: N,
    },
    /// The current task is not running on a sharded executor shard.
    NotOnShard {
        /// The requested work-unit name.
        name: N,
    },
    /// The receiver was requested from a shard other than its assigned shard.
    WrongShard {
        /// The requested work-unit name.
        name: N,
        /// The shard assigned to own the receiver.
        assigned_shard: ShardId,
        /// The shard currently polling the task.
        current_shard: ShardId,
    },
    /// The work unit's single receiver has already been taken.
    ReceiverAlreadyTaken {
        /// The requested work-unit name.
        name: N,
        /// The shard assigned to own the receiver.
        shard_id: ShardId,
    },
}

impl<N> fmt::Display for WorkUnitMailboxAddressError<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownWorkUnit { .. } => write!(f, "unknown work unit"),
            Self::NotOnShard { .. } => write!(f, "current task is not running on a shard"),
            Self::WrongShard {
                assigned_shard,
                current_shard,
                ..
            } => write!(
                f,
                "work-unit receiver assigned to shard {}, requested from shard {}",
                assigned_shard.0, current_shard.0
            ),
            Self::ReceiverAlreadyTaken { shard_id, .. } => {
                write!(
                    f,
                    "receiver for work unit on shard {} has already been taken",
                    shard_id.0
                )
            }
        }
    }
}

impl<N: fmt::Debug> core::error::Error for WorkUnitMailboxAddressError<N> {}

/// Owned point-in-time snapshot of one named work-unit mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkUnitMailboxSnapshot<N> {
    /// Logical work-unit name.
    pub name: N,
    /// Executor shard assigned to this work unit.
    pub shard_id: ShardId,
    /// Mailbox state for the work unit.
    pub mailbox: ShardMailboxSnapshot,
}

/// Named mailbox set for logical work units assigned to executor shards.
///
/// Unlike [`ShardMailboxSet`], this type does not assume one logical receiver
/// per shard. Multiple work units may be assigned to the same shard, and some
/// shards may have no assigned work units.
pub struct WorkUnitMailboxSet<N, M> {
    work_units: Vec<WorkUnitMailbox<N, M>>,
    by_name: HashMap<N, usize>,
}

struct WorkUnitMailbox<N, M> {
    name: N,
    mailbox: ShardMailbox<M>,
}

impl<N, M> WorkUnitMailboxSet<N, M>
where
    N: Eq + Hash + Clone,
    M: Send + 'static,
{
    /// Creates a named work-unit mailbox set for a sharded runtime.
    pub fn new(
        submitter: &ShardedSubmitter,
        work_units: impl IntoIterator<Item = WorkUnitSpec<N>>,
        config: ShardMailboxConfig,
    ) -> Result<Self, WorkUnitMailboxCreateError<N>> {
        if config.capacity_per_shard == 0 {
            return Err(WorkUnitMailboxCreateError::ZeroCapacity);
        }

        let mut by_name = HashMap::new();
        let mut named_mailboxes = Vec::new();
        for spec in work_units {
            if spec.shard_id.0 >= submitter.shard_count() {
                return Err(WorkUnitMailboxCreateError::InvalidShard {
                    name: spec.name,
                    shard_id: spec.shard_id,
                });
            }
            if by_name.contains_key(&spec.name) {
                return Err(WorkUnitMailboxCreateError::DuplicateName { name: spec.name });
            }

            let idx = named_mailboxes.len();
            by_name.insert(spec.name.clone(), idx);
            named_mailboxes.push(WorkUnitMailbox {
                name: spec.name,
                mailbox: ShardMailbox::new(spec.shard_id, config.capacity_per_shard),
            });
        }

        if named_mailboxes.is_empty() {
            return Err(WorkUnitMailboxCreateError::NoWorkUnits);
        }

        Ok(Self {
            work_units: named_mailboxes,
            by_name,
        })
    }

    /// Returns the number of logical work units in this set.
    pub fn work_unit_count(&self) -> usize {
        self.work_units.len()
    }

    /// Returns the shard assigned to `name`.
    pub fn assigned_shard(&self, name: &N) -> Result<ShardId, WorkUnitMailboxAddressError<N>> {
        Ok(self.work_unit(name)?.mailbox.shard_id())
    }

    /// Returns a sender addressed to logical work unit `name`.
    pub fn sender_to(&self, name: &N) -> Result<ShardSender<M>, WorkUnitMailboxAddressError<N>> {
        let work_unit = self.work_unit(name)?;
        {
            let mut state = work_unit
                .mailbox
                .shared
                .state
                .lock()
                .expect("mailbox mutex poisoned");
            state.sender_count += 1;
        }

        Ok(ShardSender {
            shared: Arc::clone(&work_unit.mailbox.shared),
        })
    }

    /// Takes the receiver for `name`, requiring the current task to run on the
    /// work unit's assigned shard.
    pub fn receiver_for_current_shard(
        &self,
        name: &N,
    ) -> Result<ShardReceiver<M>, WorkUnitMailboxAddressError<N>> {
        let work_unit = self.work_unit(name)?;
        let assigned_shard = work_unit.mailbox.shard_id();
        let current_shard = current_executor_shard()
            .ok_or_else(|| WorkUnitMailboxAddressError::NotOnShard { name: name.clone() })?;
        if current_shard != assigned_shard {
            return Err(WorkUnitMailboxAddressError::WrongShard {
                name: name.clone(),
                assigned_shard,
                current_shard,
            });
        }

        self.receiver_for(name)
    }

    /// Takes the receiver for `name` without checking the current shard.
    pub fn receiver_for(
        &self,
        name: &N,
    ) -> Result<ShardReceiver<M>, WorkUnitMailboxAddressError<N>> {
        let work_unit = self.work_unit(name)?;
        let shard_id = work_unit.mailbox.shard_id();
        let mut state = work_unit
            .mailbox
            .shared
            .state
            .lock()
            .expect("mailbox mutex poisoned");
        if state.receiver_taken {
            return Err(WorkUnitMailboxAddressError::ReceiverAlreadyTaken {
                name: name.clone(),
                shard_id,
            });
        }
        state.receiver_taken = true;

        Ok(ShardReceiver {
            shared: Arc::clone(&work_unit.mailbox.shared),
        })
    }

    /// Returns owned snapshots for all named work units.
    pub fn snapshots(&self) -> Vec<WorkUnitMailboxSnapshot<N>> {
        self.work_units
            .iter()
            .map(|work_unit| WorkUnitMailboxSnapshot {
                name: work_unit.name.clone(),
                shard_id: work_unit.mailbox.shard_id(),
                mailbox: work_unit.mailbox.snapshot(),
            })
            .collect()
    }

    /// Returns an owned snapshot for `name`.
    pub fn snapshot(
        &self,
        name: &N,
    ) -> Result<WorkUnitMailboxSnapshot<N>, WorkUnitMailboxAddressError<N>> {
        let work_unit = self.work_unit(name)?;
        Ok(WorkUnitMailboxSnapshot {
            name: work_unit.name.clone(),
            shard_id: work_unit.mailbox.shard_id(),
            mailbox: work_unit.mailbox.snapshot(),
        })
    }

    fn work_unit(
        &self,
        name: &N,
    ) -> Result<&WorkUnitMailbox<N, M>, WorkUnitMailboxAddressError<N>> {
        self.by_name
            .get(name)
            .map(|idx| &self.work_units[*idx])
            .ok_or_else(|| WorkUnitMailboxAddressError::UnknownWorkUnit { name: name.clone() })
    }
}

impl<N, M> fmt::Debug for WorkUnitMailboxSet<N, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkUnitMailboxSet")
            .field("work_unit_count", &self.work_units.len())
            .finish()
    }
}

impl<N, M> Drop for WorkUnitMailboxSet<N, M> {
    fn drop(&mut self) {
        for work_unit in &self.work_units {
            work_unit.mailbox.shared.close_sender_factory();
        }
    }
}

/// Cloneable producer handle for sending owned messages to one shard mailbox.
pub struct ShardSender<M> {
    shared: Arc<SharedMailbox<M>>,
}

impl<M: Send + 'static> ShardSender<M> {
    /// Returns the target shard for this sender.
    pub fn target_shard(&self) -> ShardId {
        self.shared.shard_id
    }

    /// Attempts to send one owned message without waiting for capacity.
    pub fn try_send(&self, message: M) -> Result<(), ShardSendError<M>> {
        self.shared.try_send(message)
    }

    /// Sends one owned message, waiting for capacity if the mailbox is full.
    pub fn send(&self, message: M) -> ShardSend<'_, M> {
        ShardSend {
            sender: self,
            message: Some(message),
        }
    }

    /// Closes the target receiver side and wakes any pending receiver.
    pub fn close(&self) {
        self.shared.close_receiver();
    }
}

impl<M> Clone for ShardSender<M> {
    fn clone(&self) -> Self {
        {
            let mut state = self.shared.state.lock().expect("mailbox mutex poisoned");
            state.sender_count += 1;
        }
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<M> Drop for ShardSender<M> {
    fn drop(&mut self) {
        self.shared.drop_sender();
    }
}

impl<M> fmt::Debug for ShardSender<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSender")
            .field("target_shard", &self.shared.shard_id)
            .finish()
    }
}

/// Single consumer handle for receiving owned messages on one shard.
pub struct ShardReceiver<M> {
    shared: Arc<SharedMailbox<M>>,
}

impl<M: Send + 'static> ShardReceiver<M> {
    /// Returns the shard that owns this receiver.
    pub fn shard_id(&self) -> ShardId {
        self.shared.shard_id
    }

    /// Attempts to receive one message without waiting.
    pub fn try_recv(&mut self) -> Result<M, ShardRecvError> {
        self.shared.try_recv()
    }

    /// Returns a future that receives the next message.
    pub fn recv(&mut self) -> ShardRecv<'_, M> {
        ShardRecv { receiver: self }
    }

    /// Closes this receiver and wakes any waiters.
    pub fn close(&mut self) {
        self.shared.close_receiver();
    }
}

impl<M> Drop for ShardReceiver<M> {
    fn drop(&mut self) {
        self.shared.close_receiver();
    }
}

impl<M> fmt::Debug for ShardReceiver<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardReceiver")
            .field("shard_id", &self.shared.shard_id)
            .finish()
    }
}

/// Future returned by [`ShardReceiver::recv`].
#[must_use = "mailbox receive futures do nothing unless polled or awaited"]
pub struct ShardRecv<'a, M> {
    receiver: &'a mut ShardReceiver<M>,
}

impl<M: Send + 'static> Future for ShardRecv<'_, M> {
    type Output = Result<M, ShardRecvError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.receiver.shared.poll_recv(context)
    }
}

impl<M> fmt::Debug for ShardRecv<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardRecv")
            .field("shard_id", &self.receiver.shared.shard_id)
            .finish()
    }
}

/// Future returned by [`ShardSender::send`].
#[must_use = "mailbox send futures do nothing unless polled or awaited"]
pub struct ShardSend<'a, M> {
    sender: &'a ShardSender<M>,
    message: Option<M>,
}

// Safety: the message field is accessed by value (take/replace), never pinned.
impl<M> Unpin for ShardSend<'_, M> {}

impl<M: Send + 'static> Future for ShardSend<'_, M> {
    type Output = Result<(), ShardSendError<M>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.sender.shared.poll_send(&mut this.message, context)
    }
}

impl<M> Drop for ShardSend<'_, M> {
    fn drop(&mut self) {
        if self.message.is_some() {
            self.sender.shared.wake_one_sender();
        }
    }
}

impl<M> fmt::Debug for ShardSend<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSend")
            .field("target_shard", &self.sender.shared.shard_id)
            .field("pending", &self.message.is_some())
            .finish()
    }
}

struct SharedMailbox<M> {
    shard_id: ShardId,
    state: Mutex<MailboxState<M>>,
    sent: AtomicU64,
    received: AtomicU64,
    full_rejections: AtomicU64,
    closed_rejections: AtomicU64,
}

struct MailboxState<M> {
    queue: VecDeque<M>,
    capacity: usize,
    sender_count: usize,
    sender_factory_open: bool,
    receiver_taken: bool,
    receiver_closed: bool,
    recv_waker: Option<Waker>,
    send_wakers: VecDeque<Waker>,
}

impl<M> SharedMailbox<M> {
    fn try_send(&self, message: M) -> Result<(), ShardSendError<M>> {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if state.receiver_closed {
                self.closed_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(ShardSendError::Closed(message));
            }
            if state.queue.len() == state.capacity {
                self.full_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(ShardSendError::Full(message));
            }

            state.queue.push_back(message);
            self.sent.fetch_add(1, Ordering::AcqRel);
            state.recv_waker.take()
        };

        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    fn try_recv(&self) -> Result<M, ShardRecvError> {
        let send_wake;
        let result;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if let Some(message) = state.queue.pop_front() {
                self.received.fetch_add(1, Ordering::AcqRel);
                send_wake = state.send_wakers.pop_front();
                result = Ok(message);
            } else if state.receiver_closed
                || (!state.sender_factory_open && state.sender_count == 0)
            {
                send_wake = None;
                result = Err(ShardRecvError::Closed);
            } else {
                send_wake = None;
                result = Err(ShardRecvError::Empty);
            }
        }
        if let Some(waker) = send_wake {
            waker.wake();
        }
        result
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Result<M, ShardRecvError>> {
        let send_wake;
        let result;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if let Some(message) = state.queue.pop_front() {
                self.received.fetch_add(1, Ordering::AcqRel);
                send_wake = state.send_wakers.pop_front();
                result = Poll::Ready(Ok(message));
            } else if state.receiver_closed
                || (!state.sender_factory_open && state.sender_count == 0)
            {
                send_wake = None;
                result = Poll::Ready(Err(ShardRecvError::Closed));
            } else {
                if !state
                    .recv_waker
                    .as_ref()
                    .is_some_and(|waker| waker.will_wake(context.waker()))
                {
                    state.recv_waker = Some(context.waker().clone());
                }
                send_wake = None;
                result = Poll::Pending;
            }
        }
        if let Some(waker) = send_wake {
            waker.wake();
        }
        result
    }

    fn close_receiver(&self) {
        let recv_wake;
        let send_wakes;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if state.receiver_closed {
                return;
            }
            state.receiver_closed = true;
            recv_wake = state.recv_waker.take();
            send_wakes = state.send_wakers.drain(..).collect::<Vec<_>>();
        }

        if let Some(waker) = recv_wake {
            waker.wake();
        }
        for waker in send_wakes {
            waker.wake();
        }
    }

    fn drop_sender(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            state.sender_count = state.sender_count.saturating_sub(1);
            if !state.sender_factory_open && state.sender_count == 0 && state.queue.is_empty() {
                state.recv_waker.take()
            } else {
                None
            }
        };

        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn close_sender_factory(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if !state.sender_factory_open {
                None
            } else {
                state.sender_factory_open = false;
                if state.sender_count == 0 && state.queue.is_empty() {
                    state.recv_waker.take()
                } else {
                    None
                }
            }
        };

        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn poll_send(
        &self,
        message: &mut Option<M>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), ShardSendError<M>>> {
        let recv_wake;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            let msg = message.take().expect("poll_send called after completion");
            if state.receiver_closed {
                self.closed_rejections.fetch_add(1, Ordering::AcqRel);
                return Poll::Ready(Err(ShardSendError::Closed(msg)));
            }
            if state.queue.len() < state.capacity {
                state.queue.push_back(msg);
                self.sent.fetch_add(1, Ordering::AcqRel);
                recv_wake = state.recv_waker.take();
            } else {
                *message = Some(msg);
                state.send_wakers.push_back(context.waker().clone());
                return Poll::Pending;
            }
        }
        if let Some(waker) = recv_wake {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }

    fn wake_one_sender(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            state.send_wakers.pop_front()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn snapshot(&self) -> ShardMailboxSnapshot {
        let state = self.state.lock().expect("mailbox mutex poisoned");
        ShardMailboxSnapshot {
            shard_id: self.shard_id,
            capacity: state.capacity,
            len: state.queue.len(),
            sender_count: state.sender_count,
            receiver_taken: state.receiver_taken,
            receiver_closed: state.receiver_closed,
            sent: self.sent.load(Ordering::Acquire),
            received: self.received.load(Ordering::Acquire),
            full_rejections: self.full_rejections.load(Ordering::Acquire),
            closed_rejections: self.closed_rejections.load(Ordering::Acquire),
            send_waiter_count: state.send_wakers.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use crate::executor::{block_on, yield_now};
    use crate::{ShardedExecutor, current_executor_shard};

    use super::*;

    #[derive(Debug)]
    struct DropCounter {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn construction_rejects_zero_values() {
        assert_eq!(
            ShardMailboxSet::<usize>::with_shard_count(0, ShardMailboxConfig::new(1)).unwrap_err(),
            ShardMailboxCreateError::ZeroShards
        );
        assert_eq!(
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(0)).unwrap_err(),
            ShardMailboxCreateError::ZeroCapacity
        );
    }

    #[test]
    fn uniform_router_maps_keys_to_physical_shards() {
        assert_eq!(
            UniformShardRouter::new(0).unwrap_err(),
            KeyRouterCreateError::ZeroTargets
        );

        let router = UniformShardRouter::new(4).unwrap();
        let target = router.route("alpha");

        assert!(target.0 < router.shard_count());
        assert_eq!(target, shard_for_hash("alpha", 4));
    }

    #[test]
    fn work_unit_router_maps_keys_to_logical_names() {
        assert_eq!(
            WorkUnitRouter::<String>::new(Vec::new()).unwrap_err(),
            KeyRouterCreateError::ZeroTargets
        );

        let router = WorkUnitRouter::new([
            String::from("assembler-a"),
            String::from("assembler-b"),
            String::from("assembler-c"),
        ])
        .unwrap();
        let target = router.route("alpha");

        assert_eq!(router.work_unit_count(), 3);
        assert!(router.work_units().contains(&target));
        assert_eq!(target, router.work_units()[shard_for_hash("alpha", 3).0]);
    }

    #[test]
    fn try_send_and_try_recv_transfer_owned_messages() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(String::from("hello")).unwrap();

        assert_eq!(receiver.try_recv().unwrap(), "hello");
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Empty));
    }

    #[test]
    fn messages_preserve_fifo_order() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(3)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        sender.try_send(3).unwrap();

        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Ok(3));
    }

    #[test]
    fn full_mailbox_returns_original_message() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();

        sender.try_send(String::from("first")).unwrap();
        let error = sender.try_send(String::from("second")).unwrap_err();

        assert!(matches!(error, ShardSendError::Full(_)));
        assert_eq!(error.into_message(), "second");
        assert_eq!(mailboxes.snapshot(ShardId(0)).unwrap().full_rejections, 1);
    }

    #[test]
    fn receiver_close_rejects_future_sends() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        receiver.close();
        let error = sender.try_send(String::from("closed")).unwrap_err();

        assert!(matches!(error, ShardSendError::Closed(_)));
        assert_eq!(error.into_message(), "closed");
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Closed));
        assert_eq!(mailboxes.snapshot(ShardId(0)).unwrap().closed_rejections, 1);
    }

    #[test]
    fn dropping_all_senders_closes_after_draining() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(7).unwrap();
        drop(sender);

        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Empty));
        drop(mailboxes);
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Closed));
    }

    #[test]
    fn taking_two_receivers_for_same_shard_fails() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let _receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        assert_eq!(
            mailboxes.receiver_for(ShardId(0)).unwrap_err(),
            ShardMailboxAddressError::ReceiverAlreadyTaken {
                shard_id: ShardId(0)
            }
        );
    }

    #[test]
    fn queued_messages_drop_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let mailboxes = ShardMailboxSet::with_shard_count(1, ShardMailboxConfig::new(2))
                .expect("mailbox set starts");
            let sender = mailboxes.sender_to(ShardId(0)).unwrap();
            sender
                .try_send(DropCounter {
                    drops: Arc::clone(&drops),
                })
                .unwrap();
            sender
                .try_send(DropCounter {
                    drops: Arc::clone(&drops),
                })
                .unwrap();
        }

        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn snapshots_report_state_and_counters() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert!(matches!(sender.try_send(3), Err(ShardSendError::Full(3))));
        assert_eq!(receiver.try_recv(), Ok(1));

        assert_eq!(
            mailboxes.snapshot(ShardId(0)).unwrap(),
            ShardMailboxSnapshot {
                shard_id: ShardId(0),
                capacity: 2,
                len: 1,
                sender_count: 1,
                receiver_taken: true,
                receiver_closed: false,
                sent: 2,
                received: 1,
                full_rejections: 1,
                closed_rejections: 0,
                send_waiter_count: 0,
            }
        );
    }

    #[test]
    fn work_unit_mailboxes_reject_invalid_definitions() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();

        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                Vec::new(),
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::NoWorkUnits
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("a"), ShardId(0))],
                ShardMailboxConfig::new(0),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::ZeroCapacity
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("a"), ShardId(9))],
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::InvalidShard {
                name: String::from("a"),
                shard_id: ShardId(9),
            }
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [
                    WorkUnitSpec::new(String::from("a"), ShardId(0)),
                    WorkUnitSpec::new(String::from("a"), ShardId(1)),
                ],
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::DuplicateName {
                name: String::from("a"),
            }
        );

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn work_unit_mailboxes_route_by_logical_name() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let mailboxes = WorkUnitMailboxSet::new(
            &submitter,
            [
                WorkUnitSpec::new(String::from("parse"), ShardId(0)),
                WorkUnitSpec::new(String::from("merge-a"), ShardId(1)),
                WorkUnitSpec::new(String::from("merge-b"), ShardId(1)),
            ],
            ShardMailboxConfig::new(2),
        )
        .unwrap();

        assert_eq!(
            mailboxes.assigned_shard(&String::from("parse")),
            Ok(ShardId(0))
        );
        assert_eq!(
            mailboxes.assigned_shard(&String::from("merge-b")),
            Ok(ShardId(1))
        );

        let sender_a = mailboxes.sender_to(&String::from("merge-a")).unwrap();
        let sender_b = mailboxes.sender_to(&String::from("merge-b")).unwrap();
        let mut receiver_a = mailboxes.receiver_for(&String::from("merge-a")).unwrap();
        let mut receiver_b = mailboxes.receiver_for(&String::from("merge-b")).unwrap();

        sender_a.try_send(10).unwrap();
        sender_b.try_send(20).unwrap();

        assert_eq!(receiver_a.try_recv(), Ok(10));
        assert_eq!(receiver_b.try_recv(), Ok(20));
        assert_eq!(
            mailboxes
                .snapshot(&String::from("merge-a"))
                .unwrap()
                .mailbox
                .received,
            1
        );

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn work_unit_receiver_for_current_shard_rejects_wrong_shard() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let mailboxes = Arc::new(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("owned-by-one"), ShardId(1))],
                ShardMailboxConfig::new(1),
            )
            .unwrap(),
        );

        let task_mailboxes = Arc::clone(&mailboxes);
        let handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                task_mailboxes.receiver_for_current_shard(&String::from("owned-by-one"))
            })
            .unwrap();

        assert_eq!(
            block_on(handle).unwrap().unwrap_err(),
            WorkUnitMailboxAddressError::WrongShard {
                name: String::from("owned-by-one"),
                assigned_shard: ShardId(1),
                current_shard: ShardId(0),
            }
        );

        drop(mailboxes);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn recv_await_wakes_when_another_shard_sends() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(2)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        let receiver_mailboxes = Arc::clone(&mailboxes);

        let receiver = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                let before = current_executor_shard();
                let value = receiver.recv().await.unwrap();
                let after = current_executor_shard();
                (before, after, value)
            })
            .unwrap();

        let sender_task = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                yield_now().await;
                sender.try_send(42).unwrap();
            })
            .unwrap();

        block_on(sender_task).unwrap();
        let (before, after, value) = block_on(receiver).unwrap();

        assert_eq!(before, Some(ShardId(1)));
        assert_eq!(after, Some(ShardId(1)));
        assert_eq!(value, 42);

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn all_shards_can_send_to_all_shards() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<(ShardId, usize)>::with_shard_count(3, ShardMailboxConfig::new(16))
                .unwrap(),
        );
        let mut receiver_handles = Vec::new();

        for shard_idx in 0..3 {
            let receiver_mailboxes = Arc::clone(&mailboxes);
            receiver_handles.push(
                runtime
                    .spawn_with_handle_on(ShardId(shard_idx), async move {
                        let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                        let mut seen = Vec::new();
                        for _ in 0..3 {
                            seen.push(receiver.recv().await.unwrap());
                        }
                        seen.sort_by_key(|(from, value)| (from.0, *value));
                        (current_executor_shard().unwrap(), seen)
                    })
                    .unwrap(),
            );
        }

        let mut sender_handles = Vec::new();
        for from_idx in 0..3 {
            let sender_mailboxes = Arc::clone(&mailboxes);
            sender_handles.push(
                runtime
                    .spawn_with_handle_on(ShardId(from_idx), async move {
                        for target_idx in 0..3 {
                            let sender = sender_mailboxes.sender_to(ShardId(target_idx)).unwrap();
                            sender.try_send((ShardId(from_idx), target_idx)).unwrap();
                        }
                    })
                    .unwrap(),
            );
        }

        for handle in sender_handles {
            block_on(handle).unwrap();
        }
        for handle in receiver_handles {
            let (shard_id, seen) = block_on(handle).unwrap();
            assert_eq!(seen.len(), 3);
            assert!(seen.iter().all(|(_, target)| *target == shard_id.0));
        }

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_waits_for_capacity() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        sender.try_send(1).unwrap();
        assert!(matches!(sender.try_send(2), Err(ShardSendError::Full(2))));

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let receiver = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                yield_now().await;
                let a = receiver.recv().await.unwrap();
                let b = receiver.recv().await.unwrap();
                (a, b)
            })
            .unwrap();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                sender.send(2).await.unwrap();
            })
            .unwrap();

        block_on(send_handle).unwrap();
        let (a, b) = block_on(receiver).unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_returns_closed_when_receiver_drops() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        sender.try_send(1).unwrap();

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let closer = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                let receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                yield_now().await;
                drop(receiver);
            })
            .unwrap();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(1), async move { sender.send(2).await })
            .unwrap();

        block_on(closer).unwrap();
        let result = block_on(send_handle).unwrap();
        assert!(matches!(result, Err(ShardSendError::Closed(2))));

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_snapshot_tracks_waiters() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        sender.try_send(1).unwrap();

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let (pending_sender, pending_receiver) = mpsc::channel();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                let mut pending_sender = Some(pending_sender);
                let mut send = Box::pin(sender.send(2));
                poll_fn(|context| match send.as_mut().poll(context) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        if let Some(pending_sender) = pending_sender.take() {
                            pending_sender.send(()).unwrap();
                        }
                        Poll::Pending
                    }
                })
                .await
                .unwrap();
            })
            .unwrap();

        pending_receiver.recv().unwrap();
        let waiters_before_drain = mailboxes.snapshot(ShardId(1)).unwrap().send_waiter_count;

        let snap_handle = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                receiver.recv().await.unwrap();
                receiver.recv().await.unwrap();
            })
            .unwrap();

        block_on(send_handle).unwrap();
        block_on(snap_handle).unwrap();

        assert!(waiters_before_drain >= 1);

        drop(mailboxes);
        runtime.stop().unwrap();
    }
}
