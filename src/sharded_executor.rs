//! Shard-per-thread async executor runtime.
//!
//! This module is the first bridge between the single-threaded async executor
//! and the project's shard-local service model. Each shard owns one executor
//! running on one OS thread. Callers place work explicitly with [`ShardId`],
//! and spawned tasks stay on that shard for their whole lifetime.

use std::fmt;
use std::future::Future;
use std::sync::mpsc;
use std::thread;

use crate::error::ShardError;
use crate::executor::{
    ExecutorObserver, ExecutorSnapshot, JoinHandle, Spawner, executor_and_spawner,
};
use crate::runtime::join_all;
use crate::shard::ShardId;

mod affinity;
mod join;

pub use affinity::{CpuId, CpuPlacement, CpuPlacementStatus, available_cpu_ids};
pub use join::{
    ShardedJoinError, ShardedJoinHandle, ShardedJoinTimeoutError, ShardedOperationError,
    ShardedSpawnError, join_all_shards,
};

thread_local! {
    static CURRENT_EXECUTOR_SHARD: std::cell::Cell<Option<ShardId>> = const { std::cell::Cell::new(None) };
    static CURRENT_EXECUTOR_CPU_PLACEMENT: std::cell::RefCell<Option<CpuPlacementStatus>> = const { std::cell::RefCell::new(None) };
}

/// Returns the shard currently polling this task, if the caller is running on a
/// [`ShardedExecutor`] shard thread.
pub fn current_executor_shard() -> Option<ShardId> {
    CURRENT_EXECUTOR_SHARD.with(std::cell::Cell::get)
}

/// Returns the CPU placement status observed when the current
/// [`ShardedExecutor`] shard thread started.
///
/// This returns `None` outside sharded executor threads.
pub fn current_executor_cpu_placement() -> Option<CpuPlacementStatus> {
    CURRENT_EXECUTOR_CPU_PLACEMENT.with(|placement| placement.borrow().clone())
}

/// Configuration for starting a [`ShardedExecutor`].
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardedExecutorConfig {
    shard_count: usize,
    thread_name_prefix: String,
    cpu_placement: CpuPlacement,
    require_cpu_placement: bool,
}

impl ShardedExecutorConfig {
    /// Creates a config for `shard_count` executor shards.
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            thread_name_prefix: String::from("sitas-shard"),
            cpu_placement: CpuPlacement::Unpinned,
            require_cpu_placement: false,
        }
    }

    /// Creates a config sized to the host's reported parallelism.
    ///
    /// If the platform cannot report available parallelism, this falls back to
    /// one shard.
    pub fn for_available_parallelism() -> Self {
        Self::new(available_parallelism())
    }

    /// Creates a config sized to the process's available CPU set.
    ///
    /// This is the shard count counterpart to [`CpuPlacement::Sequential`]:
    /// Linux uses `sched_getaffinity`, while other platforms fall back to
    /// reported parallelism.
    pub fn for_available_cpus() -> Self {
        Self::new(available_cpu_ids().len())
    }

    /// Creates a config with one shard per available CPU and sequential CPU
    /// placement requested.
    ///
    /// Linux applies hard shard-thread affinity. Other platforms keep the
    /// placement request visible in snapshots as unsupported.
    pub fn for_pinned_available_cpus() -> Self {
        Self::for_available_cpus().with_cpu_placement(CpuPlacement::Sequential)
    }

    /// Creates a config with one shard per available CPU and required
    /// sequential CPU placement.
    ///
    /// This is the fail-fast form for deployments that depend on hard CPU
    /// affinity. It succeeds on Linux when every shard can be pinned, and
    /// returns [`ShardError::CpuPlacementFailed`] otherwise.
    pub fn for_required_pinned_available_cpus() -> Self {
        Self::for_pinned_available_cpus().require_cpu_placement()
    }

    /// Sets the OS thread-name prefix used for shard executor threads.
    ///
    /// Thread names are formatted as `{prefix}-{shard_index}`.
    pub fn with_thread_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.thread_name_prefix = prefix.into();
        self
    }

    /// Sets the CPU placement policy for shard executor threads.
    ///
    /// Linux applies hard CPU affinity. Other platforms keep the requested
    /// placement observable but report it as unsupported in shard snapshots.
    pub fn with_cpu_placement(mut self, placement: CpuPlacement) -> Self {
        self.cpu_placement = placement;
        self
    }

    /// Requires requested CPU placement to be applied successfully.
    ///
    /// Without this, failed or unsupported CPU placement is reported in shard
    /// snapshots but does not prevent the runtime from starting.
    pub fn require_cpu_placement(mut self) -> Self {
        self.require_cpu_placement = true;
        self
    }

    /// Returns the configured number of executor shards.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Returns the configured OS thread-name prefix.
    pub fn thread_name_prefix(&self) -> &str {
        &self.thread_name_prefix
    }

    /// Returns the configured CPU placement policy.
    pub fn cpu_placement(&self) -> &CpuPlacement {
        &self.cpu_placement
    }

    /// Returns whether requested CPU placement must be applied successfully.
    pub fn is_cpu_placement_required(&self) -> bool {
        self.require_cpu_placement
    }

    fn validate(&self) -> Result<(), ShardError> {
        if self.shard_count == 0 {
            return Err(ShardError::InvalidShardCount);
        }

        if !self.cpu_placement.validate(self.shard_count) {
            return Err(ShardError::InvalidCpuPlacement(format!(
                "explicit placement does not provide a CPU for every shard: {} shards requested",
                self.shard_count
            )));
        }

        Ok(())
    }

    fn validate_cpu_placement_against(&self, available_cpus: &[CpuId]) -> Result<(), ShardError> {
        self.cpu_placement
            .validate_against_available_cpus(self.shard_count, available_cpus)
            .map_err(ShardError::InvalidCpuPlacement)
    }

    fn thread_name(&self, shard_id: ShardId) -> String {
        format!("{}-{}", self.thread_name_prefix, shard_id.0)
    }
}

/// A small shard-per-thread async runtime.
///
/// Each shard owns one [`crate::executor::Executor`] and one OS thread. Work is
/// submitted to an explicit shard with [`ShardedExecutor::spawn_on`] or
/// [`ShardedExecutor::spawn_with_handle_on`]. Dropping or stopping the runtime
/// drops the last owned spawners, allowing idle executor threads to drain and
/// exit.
#[must_use = "dropping the sharded executor stops all owned shard threads"]
pub struct ShardedExecutor {
    shards: Vec<AsyncShard>,
    joins: Vec<thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct AsyncShard {
    shard_id: ShardId,
    thread_name: String,
    cpu_placement: CpuPlacementStatus,
    spawner: Option<Spawner>,
}

impl ShardedExecutor {
    /// Starts `shard_count` async executor shards.
    pub fn start(shard_count: usize) -> Result<Self, ShardError> {
        Self::start_with_config(ShardedExecutorConfig::new(shard_count))
    }

    /// Starts one async executor shard for each reported unit of host
    /// parallelism.
    pub fn start_on_available_parallelism() -> Result<Self, ShardError> {
        Self::start_with_config(ShardedExecutorConfig::for_available_parallelism())
    }

    /// Starts one async executor shard for each CPU available to this process.
    pub fn start_on_available_cpus() -> Result<Self, ShardError> {
        Self::start_with_config(ShardedExecutorConfig::for_available_cpus())
    }

    /// Starts one async executor shard for each available CPU and requests
    /// sequential CPU placement.
    pub fn start_pinned_on_available_cpus() -> Result<Self, ShardError> {
        Self::start_with_config(ShardedExecutorConfig::for_pinned_available_cpus())
    }

    /// Starts one async executor shard for each available CPU and requires
    /// sequential CPU placement to be applied.
    pub fn start_required_pinned_on_available_cpus() -> Result<Self, ShardError> {
        Self::start_with_config(ShardedExecutorConfig::for_required_pinned_available_cpus())
    }

    /// Starts async executor shards using `config`.
    pub fn start_with_config(config: ShardedExecutorConfig) -> Result<Self, ShardError> {
        config.validate()?;

        let available_cpus = available_cpu_ids();
        config.validate_cpu_placement_against(&available_cpus)?;

        let mut shards = Vec::with_capacity(config.shard_count);
        let mut joins = Vec::with_capacity(config.shard_count);

        for shard_idx in 0..config.shard_count {
            let shard_id = ShardId(shard_idx);
            let thread_name = config.thread_name(shard_id);
            let requested_cpu = config
                .cpu_placement
                .cpu_for_shard(shard_idx, &available_cpus);
            let (executor, spawner) = executor_and_spawner();
            let (started_sender, started_receiver) = mpsc::sync_channel(1);

            let join = match thread::Builder::new()
                .name(thread_name.clone())
                .spawn(move || {
                    let cpu_placement = affinity::apply_to_current_thread(requested_cpu);
                    CURRENT_EXECUTOR_SHARD.with(|current| current.set(Some(shard_id)));
                    CURRENT_EXECUTOR_CPU_PLACEMENT
                        .with(|current| current.replace(Some(cpu_placement.clone())));
                    let _ = started_sender.send(cpu_placement);
                    executor.run();
                    CURRENT_EXECUTOR_CPU_PLACEMENT.with(|current| current.replace(None));
                    CURRENT_EXECUTOR_SHARD.with(|current| current.set(None));
                }) {
                Ok(join) => join,
                Err(_) => {
                    Self::cleanup_started_shards(&mut shards, &mut joins);
                    return Err(ShardError::ThreadJoinFailed);
                }
            };

            let cpu_placement = match started_receiver.recv() {
                Ok(cpu_placement) => cpu_placement,
                Err(_) => {
                    joins.push(join);
                    Self::cleanup_started_shards(&mut shards, &mut joins);
                    return Err(ShardError::ThreadJoinFailed);
                }
            };

            if config.require_cpu_placement
                && requested_cpu.is_some()
                && !cpu_placement.is_applied()
            {
                drop(spawner);
                joins.push(join);
                Self::cleanup_started_shards(&mut shards, &mut joins);
                return Err(ShardError::CpuPlacementFailed(cpu_placement.to_string()));
            }

            shards.push(AsyncShard {
                shard_id,
                thread_name,
                cpu_placement,
                spawner: Some(spawner),
            });
            joins.push(join);
        }

        Ok(Self { shards, joins })
    }

    fn cleanup_started_shards(shards: &mut [AsyncShard], joins: &mut Vec<thread::JoinHandle<()>>) {
        for shard in shards {
            shard.spawner.take();
        }

        let _ = join_all(std::mem::take(joins));
    }

    /// Returns the number of async executor shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns a cloneable handle for submitting work to executor shards.
    ///
    /// The returned handle owns spawner clones, so it keeps submission open
    /// while it exists. This mirrors an explicit cross-shard capability: drop
    /// all submitters when the runtime should drain and shut down.
    pub fn submitter(&self) -> ShardedSubmitter {
        ShardedSubmitter {
            shards: self
                .shards
                .iter()
                .map(|shard| ShardSubmitter {
                    shard_id: shard.shard_id,
                    spawner: shard.spawner.clone(),
                })
                .collect(),
        }
    }

    /// Spawns one task onto each executor shard.
    pub fn spawn_on_all<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<(), ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_handle_on_all(make_future).map(|_| ())
    }

    /// Spawns one task onto each executor shard and returns shard-tagged join
    /// handles.
    pub fn spawn_with_handle_on_all<MakeFuture, Fut>(
        &self,
        mut make_future: MakeFuture,
    ) -> Result<Vec<ShardedJoinHandle<Fut::Output>>, ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for shard in &self.shards {
            let spawner = shard
                .spawner
                .as_ref()
                .ok_or(ShardedSpawnError::Stopped(shard.shard_id))?;
            let handle = spawner
                .spawn_with_handle(make_future(shard.shard_id))
                .map_err(ShardedSpawnError::Spawn)?;
            handles.push(ShardedJoinHandle::new(shard.shard_id, handle));
        }

        Ok(handles)
    }

    /// Spawns one named task onto each executor shard and returns shard-tagged
    /// join handles.
    pub fn spawn_with_handle_named_on_all<MakeName, MakeFuture, Fut>(
        &self,
        mut make_name: MakeName,
        mut make_future: MakeFuture,
    ) -> Result<Vec<ShardedJoinHandle<Fut::Output>>, ShardedSpawnError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for shard in &self.shards {
            let spawner = shard
                .spawner
                .as_ref()
                .ok_or(ShardedSpawnError::Stopped(shard.shard_id))?;
            let handle = spawner
                .spawn_with_handle_named(make_name(shard.shard_id), make_future(shard.shard_id))
                .map_err(ShardedSpawnError::Spawn)?;
            handles.push(ShardedJoinHandle::new(shard.shard_id, handle));
        }

        Ok(handles)
    }

    /// Runs one async computation per shard and collects shard-tagged outputs.
    pub async fn map_all<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<Vec<(ShardId, Fut::Output)>, ShardedOperationError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let handles = self
            .spawn_with_handle_on_all(make_future)
            .map_err(ShardedOperationError::Submit)?;
        join_all_shards(handles)
            .await
            .map_err(ShardedOperationError::Join)
    }

    /// Runs one named async computation per shard and collects shard-tagged
    /// outputs.
    pub async fn map_named_all<MakeName, MakeFuture, Fut>(
        &self,
        make_name: MakeName,
        make_future: MakeFuture,
    ) -> Result<Vec<(ShardId, Fut::Output)>, ShardedOperationError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let handles = self
            .spawn_with_handle_named_on_all(make_name, make_future)
            .map_err(ShardedOperationError::Submit)?;
        join_all_shards(handles)
            .await
            .map_err(ShardedOperationError::Join)
    }

    /// Runs one async computation per shard and reduces the shard-tagged
    /// outputs into one value.
    pub async fn map_reduce_all<MakeFuture, Fut, Acc, Reduce>(
        &self,
        make_future: MakeFuture,
        mut initial: Acc,
        mut reduce: Reduce,
    ) -> Result<Acc, ShardedOperationError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Reduce: FnMut(Acc, ShardId, Fut::Output) -> Acc,
    {
        for (shard_id, output) in self.map_all(make_future).await? {
            initial = reduce(initial, shard_id, output);
        }

        Ok(initial)
    }

    /// Spawns a task onto a specific executor shard.
    pub fn spawn_on<F>(&self, shard_id: ShardId, future: F) -> Result<(), ShardedSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Spawns a named task onto a specific executor shard.
    pub fn spawn_named_on<F>(
        &self,
        shard_id: ShardId,
        name: impl Into<String>,
        future: F,
    ) -> Result<(), ShardedSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_named(name, future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Spawns a task onto a specific executor shard and returns an awaitable
    /// handle for its output.
    pub fn spawn_with_handle_on<F>(
        &self,
        shard_id: ShardId,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ShardedSpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_with_handle(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Spawns a named task onto a specific executor shard and returns an
    /// awaitable handle for its output.
    pub fn spawn_with_handle_named_on<F>(
        &self,
        shard_id: ShardId,
        name: impl Into<String>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ShardedSpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_with_handle_named(name, future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Returns an owned point-in-time snapshot of all executor shards.
    pub fn snapshot(&self) -> ShardedExecutorSnapshot {
        ShardedExecutorSnapshot {
            shard_count: self.shard_count(),
            running: !self.joins.is_empty(),
            shards: self
                .shards
                .iter()
                .map(|shard| ShardedExecutorShardSnapshot {
                    shard_id: shard.shard_id,
                    thread_name: shard.thread_name.clone(),
                    cpu_placement: shard.cpu_placement.clone(),
                    executor: shard.spawner.as_ref().map(Spawner::snapshot),
                })
                .collect(),
        }
    }

    /// Returns a weak observer handle for this sharded executor.
    ///
    /// The returned handle can be moved to monitoring code without keeping the
    /// shard executors alive or counting as a live spawner.
    pub fn observer(&self) -> ShardedExecutorObserver {
        ShardedExecutorObserver {
            shard_count: self.shard_count(),
            shards: self
                .shards
                .iter()
                .map(|shard| ShardedExecutorShardObserver {
                    shard_id: shard.shard_id,
                    thread_name: shard.thread_name.clone(),
                    cpu_placement: shard.cpu_placement.clone(),
                    executor: shard.spawner.as_ref().map(Spawner::observer),
                })
                .collect(),
        }
    }

    /// Stops all owned shard executors and joins their threads.
    pub fn stop(mut self) -> Result<(), ShardError> {
        self.shutdown()
    }

    /// Stops all owned shard executors while keeping the runtime handle
    /// inspectable.
    pub fn shutdown(&mut self) -> Result<(), ShardError> {
        for shard in &mut self.shards {
            shard.spawner.take();
        }

        join_all(self.joins.drain(..).collect())
    }

    fn spawner_for(&self, shard_id: ShardId) -> Result<&Spawner, ShardedSpawnError> {
        let shard = self
            .shards
            .get(shard_id.0)
            .ok_or(ShardedSpawnError::InvalidShardId(shard_id.0))?;

        debug_assert_eq!(shard.shard_id, shard_id);
        shard
            .spawner
            .as_ref()
            .ok_or(ShardedSpawnError::Stopped(shard_id))
    }
}

/// Returns the host's reported available parallelism, falling back to one.
pub fn available_parallelism() -> usize {
    thread::available_parallelism().map_or(1, usize::from)
}

/// Cloneable handle for submitting work to a [`ShardedExecutor`].
///
/// A submitter is intentionally separate from the runtime owner. It can be
/// moved into tasks so one shard can submit work to another shard and await the
/// returned [`JoinHandle`]. Cloning a submitter clones the underlying shard
/// spawners, so submitters keep shard executors accepting work until dropped.
#[must_use = "dropping the submitter releases its shard spawners"]
#[derive(Debug, Clone)]
pub struct ShardedSubmitter {
    shards: Vec<ShardSubmitter>,
}

#[derive(Debug, Clone)]
struct ShardSubmitter {
    shard_id: ShardId,
    spawner: Option<Spawner>,
}

impl ShardedSubmitter {
    /// Returns the number of shards this submitter can address.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Submits a task to a specific shard.
    pub fn submit_to<F>(&self, shard_id: ShardId, future: F) -> Result<(), ShardedSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Submits a named task to a specific shard.
    pub fn submit_named_to<F>(
        &self,
        shard_id: ShardId,
        name: impl Into<String>,
        future: F,
    ) -> Result<(), ShardedSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_named(name, future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Submits a task to a specific shard and returns an awaitable handle for
    /// its output.
    pub fn submit_with_handle_to<F>(
        &self,
        shard_id: ShardId,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ShardedSpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_with_handle(future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Submits a named task to a specific shard and returns an awaitable handle
    /// for its output.
    pub fn submit_with_handle_named_to<F>(
        &self,
        shard_id: ShardId,
        name: impl Into<String>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ShardedSpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner_for(shard_id)?
            .spawn_with_handle_named(name, future)
            .map_err(ShardedSpawnError::Spawn)
    }

    /// Submits one task to each shard.
    pub fn submit_to_all<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<(), ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.submit_with_handle_to_all(make_future).map(|_| ())
    }

    /// Submits one task to each shard and returns shard-tagged join handles.
    pub fn submit_with_handle_to_all<MakeFuture, Fut>(
        &self,
        mut make_future: MakeFuture,
    ) -> Result<Vec<ShardedJoinHandle<Fut::Output>>, ShardedSpawnError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for shard in &self.shards {
            let spawner = shard
                .spawner
                .as_ref()
                .ok_or(ShardedSpawnError::Stopped(shard.shard_id))?;
            let handle = spawner
                .spawn_with_handle(make_future(shard.shard_id))
                .map_err(ShardedSpawnError::Spawn)?;
            handles.push(ShardedJoinHandle::new(shard.shard_id, handle));
        }

        Ok(handles)
    }

    /// Submits one named task to each shard and returns shard-tagged join
    /// handles.
    pub fn submit_with_handle_named_to_all<MakeName, MakeFuture, Fut>(
        &self,
        mut make_name: MakeName,
        mut make_future: MakeFuture,
    ) -> Result<Vec<ShardedJoinHandle<Fut::Output>>, ShardedSpawnError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.shard_count());

        for shard in &self.shards {
            let spawner = shard
                .spawner
                .as_ref()
                .ok_or(ShardedSpawnError::Stopped(shard.shard_id))?;
            let handle = spawner
                .spawn_with_handle_named(make_name(shard.shard_id), make_future(shard.shard_id))
                .map_err(ShardedSpawnError::Spawn)?;
            handles.push(ShardedJoinHandle::new(shard.shard_id, handle));
        }

        Ok(handles)
    }

    /// Runs one async computation per shard and collects shard-tagged outputs.
    pub async fn map_all<MakeFuture, Fut>(
        &self,
        make_future: MakeFuture,
    ) -> Result<Vec<(ShardId, Fut::Output)>, ShardedOperationError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let handles = self
            .submit_with_handle_to_all(make_future)
            .map_err(ShardedOperationError::Submit)?;
        join_all_shards(handles)
            .await
            .map_err(ShardedOperationError::Join)
    }

    /// Runs one named async computation per shard and collects shard-tagged
    /// outputs.
    pub async fn map_named_all<MakeName, MakeFuture, Fut>(
        &self,
        make_name: MakeName,
        make_future: MakeFuture,
    ) -> Result<Vec<(ShardId, Fut::Output)>, ShardedOperationError>
    where
        MakeName: FnMut(ShardId) -> String,
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let handles = self
            .submit_with_handle_named_to_all(make_name, make_future)
            .map_err(ShardedOperationError::Submit)?;
        join_all_shards(handles)
            .await
            .map_err(ShardedOperationError::Join)
    }

    /// Runs one async computation per shard and reduces the shard-tagged
    /// outputs into one value.
    pub async fn map_reduce_all<MakeFuture, Fut, Acc, Reduce>(
        &self,
        make_future: MakeFuture,
        mut initial: Acc,
        mut reduce: Reduce,
    ) -> Result<Acc, ShardedOperationError>
    where
        MakeFuture: FnMut(ShardId) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Reduce: FnMut(Acc, ShardId, Fut::Output) -> Acc,
    {
        for (shard_id, output) in self.map_all(make_future).await? {
            initial = reduce(initial, shard_id, output);
        }

        Ok(initial)
    }

    fn spawner_for(&self, shard_id: ShardId) -> Result<&Spawner, ShardedSpawnError> {
        let shard = self
            .shards
            .get(shard_id.0)
            .ok_or(ShardedSpawnError::InvalidShardId(shard_id.0))?;

        debug_assert_eq!(shard.shard_id, shard_id);
        shard
            .spawner
            .as_ref()
            .ok_or(ShardedSpawnError::Stopped(shard_id))
    }
}

/// Weak observer handle for a sharded executor runtime.
///
/// This handle is cloneable, can be moved to a monitoring thread, and does not
/// prevent shard executors from shutting down.
#[must_use]
#[derive(Debug, Clone)]
pub struct ShardedExecutorObserver {
    shard_count: usize,
    shards: Vec<ShardedExecutorShardObserver>,
}

#[derive(Debug, Clone)]
struct ShardedExecutorShardObserver {
    shard_id: ShardId,
    thread_name: String,
    cpu_placement: CpuPlacementStatus,
    executor: Option<ExecutorObserver>,
}

impl ShardedExecutorObserver {
    /// Returns an owned point-in-time snapshot of all observable executor
    /// shards.
    pub fn snapshot(&self) -> ShardedExecutorSnapshot {
        let mut running = false;
        let shards = self
            .shards
            .iter()
            .map(|shard| {
                let executor = shard.executor.as_ref().and_then(ExecutorObserver::snapshot);
                running |= executor.is_some();

                ShardedExecutorShardSnapshot {
                    shard_id: shard.shard_id,
                    thread_name: shard.thread_name.clone(),
                    cpu_placement: shard.cpu_placement.clone(),
                    executor,
                }
            })
            .collect();

        ShardedExecutorSnapshot {
            shard_count: self.shard_count,
            running,
            shards,
        }
    }
}

/// Owned point-in-time summary of a sharded executor runtime.
#[must_use]
#[derive(Debug, Clone)]
pub struct ShardedExecutorSnapshot {
    /// Number of configured executor shards.
    pub shard_count: usize,
    /// Whether the runtime still owns running shard threads.
    pub running: bool,
    /// Per-shard executor snapshots.
    pub shards: Vec<ShardedExecutorShardSnapshot>,
}

/// Owned point-in-time summary of one async executor shard.
#[must_use]
#[derive(Debug, Clone)]
pub struct ShardedExecutorShardSnapshot {
    /// The shard this snapshot describes.
    pub shard_id: ShardId,
    /// OS thread name assigned to this shard executor.
    pub thread_name: String,
    /// CPU placement status observed when the shard thread started.
    pub cpu_placement: CpuPlacementStatus,
    /// Executor snapshot, or `None` if the shard has already stopped.
    pub executor: Option<ExecutorSnapshot>,
}

impl fmt::Debug for ShardedExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedExecutor")
            .field("shard_count", &self.shard_count())
            .field("running", &!self.joins.is_empty())
            .finish()
    }
}

impl Drop for ShardedExecutor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CpuId, CpuPlacement, CpuPlacementStatus, ShardedExecutor, ShardedExecutorConfig,
        ShardedSpawnError, available_cpu_ids, available_parallelism,
        current_executor_cpu_placement, current_executor_shard,
    };
    use crate::ShardId;
    use crate::error::ShardError;
    use crate::executor::block_on;
    use crate::executor::{TaskStatus, TaskWait, sleep};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn start_rejects_zero_shards() {
        assert_eq!(
            ShardedExecutor::start(0).unwrap_err().to_string(),
            "shard count must be greater than zero"
        );
    }

    #[test]
    fn config_reports_shard_count_and_thread_prefix() {
        let config = ShardedExecutorConfig::new(3)
            .with_thread_name_prefix("worker")
            .with_cpu_placement(CpuPlacement::Explicit(vec![CpuId(1), CpuId(2), CpuId(3)]))
            .require_cpu_placement();

        assert_eq!(config.shard_count(), 3);
        assert_eq!(config.thread_name_prefix(), "worker");
        assert_eq!(
            config.cpu_placement(),
            &CpuPlacement::Explicit(vec![CpuId(1), CpuId(2), CpuId(3)])
        );
        assert!(config.is_cpu_placement_required());
    }

    #[test]
    fn config_rejects_explicit_cpu_placement_that_does_not_cover_all_shards() {
        let error = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(2)
                .with_cpu_placement(CpuPlacement::Explicit(vec![CpuId(0)])),
        )
        .unwrap_err();

        assert!(matches!(error, ShardError::InvalidCpuPlacement(_)));
        assert!(error.to_string().contains("does not provide"));
    }

    #[test]
    fn config_rejects_explicit_cpu_placement_outside_available_cpu_set() {
        let unavailable_cpu = CpuId(usize::MAX);
        let error = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(1)
                .with_cpu_placement(CpuPlacement::Explicit(vec![unavailable_cpu])),
        )
        .unwrap_err();

        assert!(matches!(error, ShardError::InvalidCpuPlacement(_)));
        assert!(
            error
                .to_string()
                .contains("not in the process available CPU set")
        );
    }

    #[test]
    fn available_parallelism_config_uses_reported_parallelism() {
        let reported = thread::available_parallelism().map_or(1, usize::from);
        let config = ShardedExecutorConfig::for_available_parallelism();

        assert_eq!(available_parallelism(), reported);
        assert_eq!(config.shard_count(), reported);
        assert!(config.shard_count() >= 1);
    }

    #[test]
    fn available_cpu_ids_reports_at_least_one_cpu() {
        assert!(!available_cpu_ids().is_empty());
    }

    #[test]
    fn available_cpu_config_uses_available_cpu_count() {
        let config = ShardedExecutorConfig::for_available_cpus();

        assert_eq!(config.shard_count(), available_cpu_ids().len());
        assert!(config.shard_count() >= 1);
    }

    #[test]
    fn pinned_available_cpu_config_requests_sequential_placement() {
        let config = ShardedExecutorConfig::for_pinned_available_cpus();

        assert_eq!(config.shard_count(), available_cpu_ids().len());
        assert_eq!(config.cpu_placement(), &CpuPlacement::Sequential);
        assert!(!config.is_cpu_placement_required());
    }

    #[test]
    fn required_pinned_available_cpu_config_requires_placement() {
        let config = ShardedExecutorConfig::for_required_pinned_available_cpus();

        assert_eq!(config.shard_count(), available_cpu_ids().len());
        assert_eq!(config.cpu_placement(), &CpuPlacement::Sequential);
        assert!(config.is_cpu_placement_required());
    }

    #[test]
    fn start_on_available_parallelism_starts_reported_shard_count() {
        let runtime = ShardedExecutor::start_on_available_parallelism().unwrap();

        assert_eq!(runtime.shard_count(), available_parallelism());
        assert!(runtime.shard_count() >= 1);
        runtime.stop().unwrap();
    }

    #[test]
    fn start_on_available_cpus_starts_available_cpu_count() {
        let runtime = ShardedExecutor::start_on_available_cpus().unwrap();

        assert_eq!(runtime.shard_count(), available_cpu_ids().len());
        assert!(runtime.shard_count() >= 1);
        runtime.stop().unwrap();
    }

    #[test]
    fn start_pinned_on_available_cpus_records_requested_placement() {
        let runtime = ShardedExecutor::start_pinned_on_available_cpus().unwrap();

        assert_eq!(runtime.shard_count(), available_cpu_ids().len());
        for shard in &runtime.snapshot().shards {
            assert!(shard.cpu_placement.requested_cpu().is_some());
            assert!(!matches!(shard.cpu_placement, CpuPlacementStatus::Unpinned));
        }

        runtime.stop().unwrap();
    }

    #[test]
    fn current_executor_cpu_placement_reports_shard_thread_status() {
        assert_eq!(current_executor_cpu_placement(), None);

        let runtime = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(2).with_cpu_placement(CpuPlacement::Sequential),
        )
        .unwrap();
        let (sender, receiver) = mpsc::sync_channel(2);

        for shard_idx in 0..runtime.shard_count() {
            let sender = sender.clone();
            runtime
                .spawn_on(ShardId(shard_idx), async move {
                    sender
                        .send((
                            ShardId(shard_idx),
                            current_executor_shard(),
                            current_executor_cpu_placement(),
                        ))
                        .unwrap();
                })
                .unwrap();
        }

        drop(sender);

        let snapshot = runtime.snapshot();
        let mut observed = receiver.into_iter().collect::<Vec<_>>();
        observed.sort_by_key(|(shard_id, _, _)| shard_id.0);

        for (shard_id, current_shard, cpu_placement) in observed {
            assert_eq!(current_shard, Some(shard_id));
            assert_eq!(
                cpu_placement,
                Some(snapshot.shards[shard_id.0].cpu_placement.clone())
            );
        }

        runtime.stop().unwrap();
        assert_eq!(current_executor_cpu_placement(), None);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn required_pinned_available_cpu_start_rejects_unsupported_platforms() {
        let error = ShardedExecutor::start_required_pinned_on_available_cpus().unwrap_err();

        assert!(matches!(error, ShardError::CpuPlacementFailed(_)));
        assert!(error.to_string().contains("unsupported"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn required_pinned_available_cpu_start_applies_placement() {
        let runtime = ShardedExecutor::start_required_pinned_on_available_cpus().unwrap();

        assert_eq!(runtime.shard_count(), available_cpu_ids().len());
        for shard in &runtime.snapshot().shards {
            assert!(matches!(
                shard.cpu_placement,
                CpuPlacementStatus::Applied(_)
            ));
        }

        runtime.stop().unwrap();
    }

    #[test]
    fn start_with_config_uses_custom_thread_name_prefix() {
        let runtime = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(2).with_thread_name_prefix("core"),
        )
        .unwrap();
        let (sender, receiver) = mpsc::sync_channel(2);

        for shard_idx in 0..runtime.shard_count() {
            let sender = sender.clone();
            runtime
                .spawn_on(ShardId(shard_idx), async move {
                    sender
                        .send((
                            current_executor_shard(),
                            thread::current().name().map(str::to_owned),
                        ))
                        .unwrap();
                })
                .unwrap();
        }

        drop(sender);

        let mut seen = receiver.into_iter().collect::<Vec<_>>();
        seen.sort_by_key(|(shard, _)| shard.map(|id| id.0));

        assert_eq!(
            seen,
            vec![
                (Some(ShardId(0)), Some(String::from("core-0"))),
                (Some(ShardId(1)), Some(String::from("core-1"))),
            ]
        );
        assert_eq!(runtime.snapshot().shards[0].thread_name, "core-0");
        runtime.stop().unwrap();
    }

    #[test]
    fn start_with_config_records_cpu_placement_status() {
        let runtime = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(1).with_cpu_placement(CpuPlacement::Sequential),
        )
        .unwrap();

        let status = &runtime.snapshot().shards[0].cpu_placement;
        assert!(status.requested_cpu().is_some());
        assert!(!matches!(status, CpuPlacementStatus::Unpinned));

        runtime.stop().unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn required_cpu_placement_rejects_unsupported_platforms() {
        let error = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(1)
                .with_cpu_placement(CpuPlacement::Sequential)
                .require_cpu_placement(),
        )
        .unwrap_err();

        assert!(matches!(error, ShardError::CpuPlacementFailed(_)));
        assert!(error.to_string().contains("unsupported"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_cpu_placement_pins_shard_thread_affinity_masks() {
        let cpus = available_cpu_ids();
        let shard_count = cpus.len().min(2);

        if shard_count == 0 {
            return;
        }

        let expected_cpus = cpus.into_iter().take(shard_count).collect::<Vec<_>>();
        let runtime = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(shard_count)
                .with_cpu_placement(CpuPlacement::Explicit(expected_cpus.clone()))
                .require_cpu_placement(),
        )
        .unwrap();
        let (sender, receiver) = mpsc::sync_channel(shard_count);

        for shard_idx in 0..shard_count {
            let sender = sender.clone();
            runtime
                .spawn_on(ShardId(shard_idx), async move {
                    sender
                        .send((
                            ShardId(shard_idx),
                            super::affinity::current_thread_cpu_ids(),
                        ))
                        .unwrap();
                })
                .unwrap();
        }

        drop(sender);

        let mut observed = receiver.into_iter().collect::<Vec<_>>();
        observed.sort_by_key(|(shard_id, _)| shard_id.0);

        for (shard_id, cpus) in observed {
            let expected_cpu = expected_cpus[shard_id.0];
            assert_eq!(cpus, Some(vec![expected_cpu]));
            assert_eq!(
                runtime.snapshot().shards[shard_id.0].cpu_placement,
                CpuPlacementStatus::Applied(expected_cpu)
            );
        }

        runtime.stop().unwrap();
    }

    #[test]
    fn default_cpu_placement_is_unpinned() {
        let runtime = ShardedExecutor::start(1).unwrap();

        assert_eq!(
            runtime.snapshot().shards[0].cpu_placement,
            CpuPlacementStatus::Unpinned
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_on_runs_task_on_requested_shard() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let (sender, receiver) = mpsc::sync_channel(3);

        for shard_idx in 0..runtime.shard_count() {
            let sender = sender.clone();
            runtime
                .spawn_on(ShardId(shard_idx), async move {
                    sender
                        .send((
                            current_executor_shard(),
                            thread::current().name().map(str::to_owned),
                        ))
                        .unwrap();
                })
                .unwrap();
        }

        drop(sender);

        let mut seen = receiver.into_iter().collect::<Vec<_>>();
        seen.sort_by_key(|(shard, _)| shard.map(|id| id.0));

        assert_eq!(
            seen,
            vec![
                (Some(ShardId(0)), Some(String::from("sitas-shard-0"))),
                (Some(ShardId(1)), Some(String::from("sitas-shard-1"))),
                (Some(ShardId(2)), Some(String::from("sitas-shard-2")))
            ]
        );
        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_with_handle_on_returns_task_output() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let handle = runtime
            .spawn_with_handle_on(ShardId(1), async {
                assert_eq!(current_executor_shard(), Some(ShardId(1)));
                42
            })
            .unwrap();

        assert_eq!(block_on(handle).unwrap(), 42);
        runtime.stop().unwrap();
    }

    #[test]
    fn runtime_can_spawn_on_all_shards_and_join_outputs() {
        let runtime = ShardedExecutor::start(4).unwrap();

        let handles = runtime
            .spawn_with_handle_named_on_all(
                |shard_id| format!("runtime-broadcast-{}", shard_id.0),
                |shard_id| async move {
                    assert_eq!(current_executor_shard(), Some(shard_id));
                    shard_id.0 * 10
                },
            )
            .unwrap();

        let outputs = block_on(super::join_all_shards(handles)).unwrap();
        assert_eq!(
            outputs,
            vec![
                (ShardId(0), 0),
                (ShardId(1), 10),
                (ShardId(2), 20),
                (ShardId(3), 30)
            ]
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn runtime_can_map_reduce_across_shards() {
        let runtime = ShardedExecutor::start(4).unwrap();

        let total = block_on(runtime.map_reduce_all(
            |shard_id| async move {
                assert_eq!(current_executor_shard(), Some(shard_id));
                shard_id.0 + 1
            },
            0usize,
            |sum, _shard_id, value| sum + value,
        ))
        .unwrap();

        assert_eq!(total, 10);
        runtime.stop().unwrap();
    }

    #[test]
    fn runtime_map_named_all_tasks_are_observable() {
        let runtime = ShardedExecutor::start(2).unwrap();

        let handles = runtime
            .spawn_with_handle_named_on_all(
                |shard_id| format!("runtime-map-{}", shard_id.0),
                |shard_id| async move {
                    sleep(Duration::from_millis(100)).await;
                    current_executor_shard().unwrap_or(shard_id)
                },
            )
            .unwrap();

        let expected_0 = String::from("runtime-map-0");
        let expected_1 = String::from("runtime-map-1");
        let deadline = Instant::now() + Duration::from_secs(1);
        let task_names = loop {
            let snapshot = runtime.snapshot();
            let task_names = snapshot
                .shards
                .iter()
                .flat_map(|shard| {
                    shard
                        .executor
                        .as_ref()
                        .into_iter()
                        .flat_map(|executor| executor.tasks.iter())
                })
                .filter_map(|task| task.name.clone())
                .collect::<Vec<_>>();

            if task_names.contains(&expected_0) && task_names.contains(&expected_1) {
                break task_names;
            }

            assert!(
                Instant::now() < deadline,
                "runtime map tasks were not observable: {task_names:?}"
            );
            thread::sleep(Duration::from_millis(1));
        };

        assert!(task_names.contains(&expected_0));
        assert!(task_names.contains(&expected_1));

        let outputs = block_on(super::join_all_shards(handles)).unwrap();
        assert_eq!(
            outputs,
            vec![(ShardId(0), ShardId(0)), (ShardId(1), ShardId(1))]
        );
        runtime.stop().unwrap();
    }

    #[test]
    fn runtime_spawn_on_all_rejects_stopped_shards() {
        let mut runtime = ShardedExecutor::start(1).unwrap();
        runtime.shutdown().unwrap();

        let error = runtime
            .spawn_on_all(|_shard_id| async {})
            .expect_err("stopped shard should fail");

        assert_eq!(error, ShardedSpawnError::Stopped(ShardId(0)));
    }

    #[test]
    fn spawn_on_rejects_invalid_shard() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let error = runtime
            .spawn_on(ShardId(7), async {})
            .expect_err("invalid shard should fail");

        assert_eq!(error, ShardedSpawnError::InvalidShardId(7));
        runtime.stop().unwrap();
    }

    #[test]
    fn spawn_on_rejects_stopped_shard() {
        let mut runtime = ShardedExecutor::start(1).unwrap();

        runtime.shutdown().unwrap();
        let error = runtime
            .spawn_on(ShardId(0), async {})
            .expect_err("stopped shard should fail");

        assert_eq!(error, ShardedSpawnError::Stopped(ShardId(0)));
    }

    #[test]
    fn snapshot_reports_named_waiting_tasks_by_shard() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);

        runtime
            .spawn_named_on(ShardId(1), "slow-worker", async move {
                sender.send(()).unwrap();
                sleep(Duration::from_millis(100)).await;
            })
            .unwrap();

        receiver.recv().unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let (snapshot, shard, task) = loop {
            let snapshot = runtime.snapshot();
            let shard = snapshot
                .shards
                .iter()
                .find(|shard| shard.shard_id == ShardId(1))
                .unwrap()
                .clone();
            let executor = shard.executor.as_ref().unwrap();
            let task = executor
                .tasks
                .iter()
                .find(|task| task.name.as_deref() == Some("slow-worker"))
                .unwrap()
                .clone();

            if task.status == TaskStatus::Waiting
                && matches!(task.waiting_for, Some(TaskWait::Timer { .. }))
            {
                break (snapshot, shard, task);
            }

            assert!(
                Instant::now() < deadline,
                "slow-worker did not enter timer wait state: {task:?}"
            );
            thread::sleep(Duration::from_millis(1));
        };

        let executor = shard.executor.as_ref().unwrap();
        let task_count = executor.task_count;
        let timer_count = executor.timer_count;

        let shard = snapshot
            .shards
            .iter()
            .find(|shard| shard.shard_id == ShardId(1))
            .unwrap();

        assert_eq!(snapshot.shard_count, 2);
        assert!(snapshot.running);
        assert_eq!(shard.thread_name, "sitas-shard-1");
        assert_eq!(task.status, TaskStatus::Waiting);
        assert!(matches!(task.waiting_for, Some(TaskWait::Timer { .. })));
        assert!(task.poll_count >= 1);
        assert_eq!(task_count, 1);
        assert_eq!(timer_count, 1);

        runtime.stop().unwrap();
    }

    #[test]
    fn observer_snapshots_do_not_keep_shards_alive() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let observer = runtime.observer();

        let snapshot = observer.snapshot();
        assert!(snapshot.running);
        assert_eq!(snapshot.shards[0].thread_name, "sitas-shard-0");
        assert!(snapshot.shards[0].executor.is_some());

        runtime.stop().unwrap();

        let snapshot = observer.snapshot();
        assert!(!snapshot.running);
        assert_eq!(snapshot.shards[0].thread_name, "sitas-shard-0");
        assert!(snapshot.shards[0].executor.is_none());
    }

    #[test]
    fn shard_task_can_submit_to_another_shard_and_await_result() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let task_submitter = submitter.clone();

        let handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                assert_eq!(current_executor_shard(), Some(ShardId(0)));

                let remote = task_submitter
                    .submit_with_handle_named_to(ShardId(1), "remote-work", async {
                        current_executor_shard().unwrap()
                    })
                    .unwrap();
                let remote_shard = remote.await.unwrap();

                (current_executor_shard().unwrap(), remote_shard)
            })
            .unwrap();

        assert_eq!(block_on(handle).unwrap(), (ShardId(0), ShardId(1)));
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn submitter_rejects_invalid_shard() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let error = submitter
            .submit_to(ShardId(3), async {})
            .expect_err("invalid shard should fail");

        assert_eq!(error, ShardedSpawnError::InvalidShardId(3));
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn submitter_can_submit_to_all_shards_and_join_outputs() {
        let runtime = ShardedExecutor::start(4).unwrap();
        let submitter = runtime.submitter();
        let task_submitter = submitter.clone();

        let handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                let handles = task_submitter
                    .submit_with_handle_named_to_all(
                        |shard_id| format!("broadcast-{}", shard_id.0),
                        |shard_id| async move {
                            assert_eq!(current_executor_shard(), Some(shard_id));
                            shard_id.0 * 10
                        },
                    )
                    .unwrap();

                super::join_all_shards(handles).await.unwrap()
            })
            .unwrap();

        let outputs = block_on(handle).unwrap();
        assert_eq!(
            outputs,
            vec![
                (ShardId(0), 0),
                (ShardId(1), 10),
                (ShardId(2), 20),
                (ShardId(3), 30)
            ]
        );

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn submitter_can_map_reduce_across_shards() {
        let runtime = ShardedExecutor::start(4).unwrap();
        let submitter = runtime.submitter();
        let task_submitter = submitter.clone();

        let handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                task_submitter
                    .map_reduce_all(
                        |shard_id| async move {
                            assert_eq!(current_executor_shard(), Some(shard_id));
                            shard_id.0 + 1
                        },
                        0usize,
                        |sum, _shard_id, value| sum + value,
                    )
                    .await
                    .unwrap()
            })
            .unwrap();

        assert_eq!(block_on(handle).unwrap(), 10);

        drop(submitter);
        runtime.stop().unwrap();
    }
}
