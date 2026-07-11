//! Generic sharded service trait and runtime.
//!
//! The [`ShardService`] trait captures the reusable pattern across sharded
//! services: a per-shard service state, a typed command enum, and a processing
//! loop. [`Sharded`] provides the lifecycle (start/stop/snapshot) while each
//! service defines its own commands and reply types.

use core::fmt;
use std::sync::mpsc;

use crate::placement::HashPlacement;
use crate::runtime::{
    HasShardId, Reply, ReplySender, RuntimeSnapshot, ShardConfig, ShardMailbox, ShardSet,
};
use crate::{ShardError, ShardId};

/// Configuration for starting a [`Sharded`] service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedConfig {
    /// Number of shard threads to start.
    pub shard_count: usize,
    /// Maximum pending commands per shard mailbox.
    pub mailbox_capacity: usize,
}

impl ShardedConfig {
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

impl From<ShardConfig> for ShardedConfig {
    fn from(config: ShardConfig) -> Self {
        Self {
            shard_count: config.shard_count,
            mailbox_capacity: config.mailbox_capacity,
        }
    }
}

/// A shard-service implementation.
///
/// Implementors define the per-shard state type, the command enum, an initial
/// state factory, and a command-processing function. The processing function
/// returns `true` to continue the shard loop and `false` to exit.
pub trait ShardService: Sized {
    /// Per-shard mutable state.
    type State: Send + 'static;

    /// Typed command routed to a single shard.
    type Command: Send + 'static;

    /// Creates the initial per-shard state.
    fn initial_state(shard_id: ShardId) -> Self::State;

    /// Processes one command against the shard state.
    ///
    /// Return `true` to continue the shard event loop, or `false` to exit
    /// (e.g., when a stop command is received).
    fn process(state: &mut Self::State, command: Self::Command) -> bool;

    /// Builds the command that tells a shard to stop.
    ///
    /// The generic infrastructure calls this once per shard during shutdown.
    /// Implementations should construct a command whose processing returns
    /// `false`, causing the shard loop to exit after sending the reply.
    fn stop_command(reply: ReplySender<()>) -> Self::Command;
}

/// A handle for sending commands to a single shard.
///
/// Each shard in a [`Sharded`] service exposes its handle through
/// [`Sharded::shard_handle`]. Callers use the handle to submit typed commands
/// and receive replies.
pub struct ShardHandle<C> {
    id: ShardId,
    mailbox: ShardMailbox<C>,
}

impl<C> ShardHandle<C> {
    fn new(id: ShardId, mailbox: ShardMailbox<C>) -> Self {
        Self { id, mailbox }
    }

    fn send(&self, command: C) -> Result<(), ShardError> {
        self.mailbox.send(command)
    }

    fn try_send(&self, command: C) -> Result<(), ShardError> {
        self.mailbox.try_send(command)
    }

    fn request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        self.mailbox.request(build)
    }

    fn try_request<T, F>(&self, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        self.mailbox.try_request(build)
    }

    fn request_stopped<T, F>(&self, build: F) -> Result<T, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> C,
    {
        self.mailbox.request_stopped(build)
    }
}

impl<C> HasShardId for ShardHandle<C> {
    fn shard_id(&self) -> ShardId {
        self.id
    }
}

/// A sharded service with one owning thread per shard.
///
/// `S` implements [`ShardService`] and defines the per-shard state, command
/// type, and processing logic. The generic handle provides lifecycle management
/// and raw command submission.
pub struct Sharded<S, P = HashPlacement>
where
    S: ShardService,
{
    shards: ShardSet<ShardHandle<S::Command>>,
    placement: P,
    stopped: bool,
}

impl<S> Sharded<S, HashPlacement>
where
    S: ShardService + 'static,
{
    /// Starts a sharded service with `shard_count` shard threads.
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        Self::start_with_config(ShardedConfig::new(shard_count))
    }

    /// Starts a sharded service from an explicit configuration.
    pub fn start_with_config(config: ShardedConfig) -> Result<Self, ShardError> {
        Self::start_with_placement(config, HashPlacement)
    }
}

impl<S, P> Sharded<S, P>
where
    S: ShardService + 'static,
{
    /// Starts a sharded service with a custom placement strategy.
    pub fn start_with_placement(config: ShardedConfig, placement: P) -> Result<Self, ShardError> {
        let config = config.runtime_config()?;

        let shards = ShardSet::start(
            config.shard_count,
            config.mailbox_capacity,
            |shard_idx, mailbox| ShardHandle::new(ShardId(shard_idx), mailbox),
            run_shard::<S>,
        );

        Ok(Self {
            shards,
            placement,
            stopped: false,
        })
    }

    /// Returns the number of shards in this service.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the bounded mailbox capacity configured for each shard.
    pub fn mailbox_capacity(&self) -> usize {
        self.shards.mailbox_capacity()
    }

    /// Returns an owned snapshot of this service's runtime shape.
    pub fn runtime_snapshot(&self) -> RuntimeSnapshot {
        self.shards.snapshot(self.stopped)
    }

    /// Sends a command to a specific shard, blocking if the mailbox is full.
    pub fn send(&self, shard_id: ShardId, command: S::Command) -> Result<(), ShardError> {
        self.shard(shard_id)?.send(command)
    }

    /// Attempts to send a command without waiting for mailbox capacity.
    pub fn try_send(&self, shard_id: ShardId, command: S::Command) -> Result<(), ShardError> {
        self.shard(shard_id)?.try_send(command)
    }

    /// Enqueues a request command and returns a reply handle.
    pub fn request<T, F>(&self, shard_id: ShardId, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> S::Command,
    {
        self.shard(shard_id)?.request(build)
    }

    /// Attempts to enqueue a request without waiting for mailbox capacity.
    pub fn try_request<T, F>(&self, shard_id: ShardId, build: F) -> Result<Reply<T>, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> S::Command,
    {
        self.shard(shard_id)?.try_request(build)
    }

    /// Sends a request to a shard and waits synchronously for the result.
    pub fn request_stopped<T, F>(&self, shard_id: ShardId, build: F) -> Result<T, ShardError>
    where
        F: FnOnce(ReplySender<T>) -> S::Command,
    {
        self.shard(shard_id)?.request_stopped(build)
    }

    /// Sends a command to every shard, blocking on each if mailboxes are full.
    pub fn send_all<MakeCommand>(&self, mut make_command: MakeCommand) -> Result<(), ShardError>
    where
        MakeCommand: FnMut(ShardId) -> S::Command,
    {
        for idx in 0..self.shard_count() {
            self.shards
                .get(idx)
                .unwrap()
                .send(make_command(ShardId(idx)))?;
        }
        Ok(())
    }

    /// Sends a command to every shard without blocking for mailbox capacity.
    pub fn try_send_all<MakeCommand>(&self, mut make_command: MakeCommand) -> Result<(), ShardError>
    where
        MakeCommand: FnMut(ShardId) -> S::Command,
    {
        for idx in 0..self.shard_count() {
            self.shards
                .get(idx)
                .unwrap()
                .try_send(make_command(ShardId(idx)))?;
        }
        Ok(())
    }

    /// Requests from all shards and returns reply handles.
    ///
    /// The caller controls blocking vs non-blocking behavior by passing either
    /// `handle.request(...)` or `handle.try_request(...)` as the closure.
    pub fn request_all<T, F>(&self, request: F) -> Result<Vec<Reply<T>>, ShardError>
    where
        F: FnMut(&ShardHandle<S::Command>) -> Result<Reply<T>, ShardError>,
    {
        self.ensure_running()?;
        self.shards.request_all(request)
    }

    /// Returns the placement strategy.
    pub fn placement(&self) -> &P {
        &self.placement
    }

    /// Returns a reference to the shard handle for a given shard id.
    pub fn shard_handle(&self, shard_id: ShardId) -> Result<&ShardHandle<S::Command>, ShardError> {
        self.shard(shard_id)
    }

    /// Stops all shards and joins their threads.
    pub fn stop(mut self) -> Result<(), ShardError> {
        self.shutdown()
    }

    /// Stops all shards and joins their threads while retaining the handle.
    ///
    /// Each shard receives a stop command built by
    /// [`ShardService::stop_command`]. The shard loop processes it and exits
    /// after sending the reply.
    pub fn shutdown(&mut self) -> Result<(), ShardError> {
        if self.stopped {
            return Ok(());
        }

        self.stopped = true;
        self.shards
            .stop_and_join(|handle| handle.request_stopped(|reply| S::stop_command(reply)))
    }

    fn ensure_running(&self) -> Result<(), ShardError> {
        if self.stopped {
            Err(ShardError::ShardStopped)
        } else {
            Ok(())
        }
    }

    fn shard(&self, shard_id: ShardId) -> Result<&ShardHandle<S::Command>, ShardError> {
        self.ensure_running()?;
        self.shards.get_by_id(shard_id)
    }
}

impl<S, P> fmt::Debug for Sharded<S, P>
where
    S: ShardService,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sharded")
            .field("shard_count", &self.shards.len())
            .field("mailbox_capacity", &self.shards.mailbox_capacity())
            .field("stopped", &self.stopped)
            .finish_non_exhaustive()
    }
}

impl<S, P> Drop for Sharded<S, P>
where
    S: ShardService,
{
    fn drop(&mut self) {
        if !self.stopped {
            self.stopped = true;
            let _ = self
                .shards
                .stop_and_join(|handle| handle.request_stopped(|reply| S::stop_command(reply)));
        }
    }
}

fn run_shard<S: ShardService>(shard_id: crate::ShardId, receiver: mpsc::Receiver<S::Command>) {
    let mut state = S::initial_state(shard_id);

    while let Ok(command) = receiver.recv() {
        if !S::process(&mut state, command) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum TestCommand {
        Add { delta: i64, reply: ReplySender<i64> },
        Get { reply: ReplySender<i64> },
        Stop { reply: ReplySender<()> },
    }

    struct TestService;

    impl ShardService for TestService {
        type State = i64;
        type Command = TestCommand;

        fn initial_state(_shard_id: ShardId) -> Self::State {
            0
        }

        fn process(state: &mut Self::State, command: Self::Command) -> bool {
            match command {
                TestCommand::Add { delta, reply } => {
                    *state += delta;
                    let _ = reply.send(*state);
                    true
                }
                TestCommand::Get { reply } => {
                    let _ = reply.send(*state);
                    true
                }
                TestCommand::Stop { reply } => {
                    let _ = reply.send(());
                    false
                }
            }
        }

        fn stop_command(reply: ReplySender<()>) -> Self::Command {
            TestCommand::Stop { reply }
        }
    }

    #[test]
    fn generic_sharded_can_start_and_stop() {
        let mut service: Sharded<TestService> = Sharded::start(2).unwrap();

        assert_eq!(service.shard_count(), 2);

        let get_reply = service
            .request(ShardId(0), |reply| TestCommand::Get { reply })
            .unwrap();
        assert_eq!(get_reply.wait().unwrap(), 0);

        let add_reply = service
            .request(ShardId(0), |reply| TestCommand::Add { delta: 5, reply })
            .unwrap();
        assert_eq!(add_reply.wait().unwrap(), 5);

        let get_reply = service
            .request(ShardId(0), |reply| TestCommand::Get { reply })
            .unwrap();
        assert_eq!(get_reply.wait().unwrap(), 5);

        service.shutdown().unwrap();
    }

    #[test]
    fn generic_sharded_supports_all_shards() {
        let mut service: Sharded<TestService> = Sharded::start(4).unwrap();

        let replies: Vec<Reply<i64>> = service
            .request_all(|handle| handle.request(|reply| TestCommand::Add { delta: 10, reply }))
            .unwrap();

        let results: Vec<i64> = replies.into_iter().map(|r| r.wait().unwrap()).collect();
        assert_eq!(results, vec![10, 10, 10, 10]);

        service.shutdown().unwrap();
    }

    #[test]
    fn generic_sharded_handles_channel_disconnect() {
        let mut service: Sharded<TestService> = Sharded::start(1).unwrap();

        let reply = service
            .request(ShardId(0), |reply| TestCommand::Get { reply })
            .unwrap();
        assert_eq!(reply.wait().unwrap(), 0);

        service.shutdown().unwrap();
    }
}
