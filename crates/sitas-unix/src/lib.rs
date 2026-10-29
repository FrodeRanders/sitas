//! Unix backend for the `sitas-core` `no_std` lane.
//!
//! [`UnixRuntime`] implements [`ShardRuntime`] over the standard library:
//! `std::thread` for shard spawn, the core ring-buffer channel for messages,
//! a `Mutex`/`Condvar` parker for requester blocking, and the portable
//! [`OsReactor`] (`epoll`/`kqueue`/`poll`) as the per-shard reactor.
//!
//! This is the host-platform mirror of `sitas-charlotte`: the same
//! `sitas-core` service code (for example [`sitas_core::kv::ShardedKv`])
//! runs unmodified on Linux and macOS through this backend and on
//! CharlotteOS through the kernel-syscall backend. That makes the
//! CharlotteOS code path testable with plain `cargo test` on a Unix host.

use std::sync::{Condvar, Mutex};

use sitas_core::os::OsReactor;
use sitas_core::placement::ShardPlacement;
use sitas_core::shard::ShardId;
use sitas_core::shard_runtime::{
    ShardChannelResult, ShardJoinHandle, ShardParker, ShardRuntime, channel,
};
use std::boxed::Box;
use std::sync::Arc;
use std::time::Duration;

/// A [`ShardRuntime`] backed by `std::thread` and [`OsReactor`].
///
/// All parkers handed out by [`ShardRuntime::parker`] share one wake channel,
/// mirroring the process-wide completion-queue wake used by the CharlotteOS
/// backend.
#[derive(Debug, Clone)]
pub struct UnixRuntime {
    parker: Arc<UnixParker>,
}

impl UnixRuntime {
    /// Creates a Unix shard runtime.
    pub fn new() -> Self {
        Self {
            parker: Arc::new(UnixParker::new()),
        }
    }
}

impl Default for UnixRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardRuntime for UnixRuntime {
    type JoinHandle<T: Send + 'static> = ShardJoinHandle<T>;
    type Reactor = OsReactor;

    fn spawn_shard<T: Send + 'static>(
        &self,
        shard_id: ShardId,
        _placement: ShardPlacement,
        entry: Box<dyn FnOnce() -> T + Send>,
    ) -> ShardJoinHandle<T> {
        // Placement is advisory: the Unix backend does not pin threads yet.
        // CPU pinning stays an explicit experiment in the std sharded
        // executor (`sitas_core::sharded_executor`).
        let handle = std::thread::Builder::new()
            .name(format!("shard-{}", shard_id.0))
            .spawn(entry)
            .expect("spawning a shard thread failed");
        ShardJoinHandle::from_std(handle)
    }

    fn channel<M: Send + 'static>(&self, capacity: usize) -> ShardChannelResult<M> {
        channel(capacity)
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn parker(&self) -> Arc<dyn ShardParker> {
        Arc::clone(&self.parker) as Arc<dyn ShardParker>
    }

    fn shard_reactor(&self, shard_id: ShardId) -> OsReactor {
        // A reactor is the shard's single blocking wait primitive; failing to
        // create one means the shard cannot run at all, so fail fast with
        // context instead of limping along.
        OsReactor::new().unwrap_or_else(|error| {
            panic!(
                "creating the OS reactor for shard {} failed: {error}",
                shard_id.0
            )
        })
    }
}

/// Process-wide `Mutex`/`Condvar` parker.
///
/// `park` returns when `unpark` was called since the last wakeup or when the
/// timeout elapses; spurious wakeups are allowed by the [`ShardParker`]
/// contract (callers re-check their own condition and re-park).
#[derive(Debug)]
struct UnixParker {
    state: Mutex<bool>,
    wake: Condvar,
}

impl UnixParker {
    fn new() -> Self {
        Self {
            state: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
}

impl ShardParker for UnixParker {
    fn park(&self, timeout: Option<Duration>) {
        let mut notified = self.state.lock().expect("shard parker mutex poisoned");
        match timeout {
            Some(duration) => {
                let deadline = std::time::Instant::now() + duration;
                while !*notified {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return;
                    }
                    let (guard, wait) = self
                        .wake
                        .wait_timeout(notified, remaining)
                        .expect("shard parker mutex poisoned");
                    notified = guard;
                    if wait.timed_out() {
                        break;
                    }
                }
                *notified = false;
            }
            None => {
                while !*notified {
                    notified = self
                        .wake
                        .wait(notified)
                        .expect("shard parker mutex poisoned");
                }
                *notified = false;
            }
        }
    }

    fn unpark(&self) {
        let mut notified = self.state.lock().expect("shard parker mutex poisoned");
        *notified = true;
        self.wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sitas_core::kv::{ShardedKv, ShardedKvConfig};

    #[test]
    fn unix_runtime_runs_the_no_std_kv() {
        let runtime = UnixRuntime::new();
        let kv = ShardedKv::start_with_runtime(ShardedKvConfig::new(3), &runtime)
            .expect("kv start failed");

        kv.put("alpha", "one").expect("put alpha");
        kv.put("beta", "two").expect("put beta");
        kv.put("gamma", "three").expect("put gamma");

        assert_eq!(kv.get("alpha").expect("get alpha").as_deref(), Some("one"));
        assert_eq!(kv.get("beta").expect("get beta").as_deref(), Some("two"));
        assert_eq!(kv.get("missing").expect("get missing"), None);
        assert_eq!(kv.total_len().expect("total_len"), 3);
    }

    #[test]
    fn unix_runtime_builds_the_mailbox_index() {
        use sitas_core::mailbox_index::{
            FAILURE_SENTINEL, RECORD_COUNT, SUCCESS_SENTINEL, mailbox_index_test,
        };

        let runtime = UnixRuntime::new();
        let mut result = 0u32;
        unsafe { mailbox_index_test(&runtime, &mut result) };
        assert_ne!(
            result, FAILURE_SENTINEL,
            "mailbox index demo failed (failure sentinel written)"
        );
        assert_eq!(
            result, SUCCESS_SENTINEL,
            "expected verified record count {RECORD_COUNT}, got {result}"
        );
    }

    #[test]
    fn parker_wakes_a_parked_thread() {
        let runtime = UnixRuntime::new();
        let parker = runtime.parker();
        let unparker = runtime.parker();

        let waiter = std::thread::spawn(move || {
            parker.park(None);
        });

        std::thread::sleep(Duration::from_millis(20));
        unparker.unpark();
        waiter.join().expect("parked thread panicked");
    }

    #[test]
    fn park_with_timeout_returns() {
        let runtime = UnixRuntime::new();
        runtime.parker().park(Some(Duration::from_millis(10)));
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use sitas_core::{ShardError, shard_runtime::ShardChannelError};
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    struct ObservedRuntime {
        inner: UnixRuntime,
        completed: Arc<AtomicUsize>,
        channels: AtomicUsize,
        fail_channel_at: usize,
        exit_gate: Option<Arc<Barrier>>,
        exits: Option<mpsc::Sender<()>>,
        panic_worker: bool,
    }
    impl ObservedRuntime {
        fn new() -> Self {
            Self {
                inner: UnixRuntime::new(),
                completed: Arc::new(AtomicUsize::new(0)),
                channels: AtomicUsize::new(0),
                fail_channel_at: usize::MAX,
                exit_gate: None,
                exits: None,
                panic_worker: false,
            }
        }
    }
    impl ShardRuntime for ObservedRuntime {
        type JoinHandle<T: Send + 'static> = ShardJoinHandle<T>;
        type Reactor = OsReactor;
        fn spawn_shard<T: Send + 'static>(
            &self,
            shard: ShardId,
            placement: ShardPlacement,
            entry: Box<dyn FnOnce() -> T + Send>,
        ) -> ShardJoinHandle<T> {
            let completed = Arc::clone(&self.completed);
            let exit_gate = self.exit_gate.clone();
            let exits = self.exits.clone();
            let panic_worker = self.panic_worker;
            self.inner.spawn_shard(
                shard,
                placement,
                Box::new(move || {
                    struct Completed(Arc<AtomicUsize>);
                    impl Drop for Completed {
                        fn drop(&mut self) {
                            self.0.fetch_add(1, Ordering::Release);
                        }
                    }
                    let _completed = Completed(completed);
                    if panic_worker {
                        drop(entry);
                        panic!("injected worker failure");
                    }
                    let result = entry();
                    if let Some(exits) = exits {
                        exits.send(()).unwrap();
                    }
                    if let Some(gate) = exit_gate {
                        gate.wait();
                    }
                    result
                }),
            )
        }
        fn channel<M: Send + 'static>(&self, capacity: usize) -> ShardChannelResult<M> {
            if self.channels.fetch_add(1, Ordering::Relaxed) == self.fail_channel_at {
                Err(ShardChannelError::InvalidCapacity)
            } else {
                channel(capacity)
            }
        }
        fn sleep(&self, duration: Duration) {
            self.inner.sleep(duration);
        }
        fn parker(&self) -> Arc<dyn ShardParker> {
            self.inner.parker()
        }
        fn shard_reactor(&self, shard: ShardId) -> OsReactor {
            self.inner.shard_reactor(shard)
        }
    }

    #[test]
    fn kv_explicit_stop_and_drop_join_worker_destructors() {
        use sitas_core::kv::{ShardedKv, ShardedKvConfig};
        for explicit in [false, true] {
            let mut runtime = ObservedRuntime::new();
            let gate = Arc::new(Barrier::new(3));
            let (exit_tx, exit_rx) = mpsc::channel();
            runtime.exit_gate = Some(Arc::clone(&gate));
            runtime.exits = Some(exit_tx);
            let kv = ShardedKv::start_with_runtime(ShardedKvConfig::new(2), &runtime).unwrap();
            kv.put("key", "value").unwrap();
            let (done_tx, done_rx) = mpsc::channel();
            let shutdown = std::thread::spawn(move || {
                if explicit {
                    kv.stop().unwrap();
                } else {
                    drop(kv);
                }
                done_tx.send(()).unwrap();
            });
            exit_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            exit_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let returned_early = done_rx.try_recv().is_ok();
            gate.wait();
            shutdown.join().unwrap();
            assert!(
                !returned_early,
                "KV returned before worker destructors finished"
            );
            assert_eq!(runtime.completed.load(Ordering::Acquire), 2);
        }
    }

    #[test]
    fn kv_partial_startup_closes_and_joins_started_workers() {
        use sitas_core::kv::{ShardedKv, ShardedKvConfig};
        let mut runtime = ObservedRuntime::new();
        runtime.fail_channel_at = 1;
        let error = ShardedKv::start_with_runtime(ShardedKvConfig::new(2), &runtime).err();
        assert_eq!(error, Some(ShardError::InvalidMailboxCapacity));
        assert_eq!(runtime.completed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn kv_worker_failure_rejects_requests_and_reports_join_failure() {
        use sitas_core::kv::{ShardedKv, ShardedKvConfig};
        let mut runtime = ObservedRuntime::new();
        runtime.panic_worker = true;
        let mut kv = ShardedKv::start_with_runtime(ShardedKvConfig::new(1), &runtime).unwrap();
        let start = std::time::Instant::now();
        while runtime.completed.load(Ordering::Acquire) == 0
            && start.elapsed() < Duration::from_secs(2)
        {
            std::thread::yield_now();
        }
        assert_eq!(runtime.completed.load(Ordering::Acquire), 1);
        assert_eq!(kv.put("key", "value"), Err(ShardError::ShardStopped));
        assert_eq!(kv.shutdown(), Err(ShardError::ThreadJoinFailed));
        assert_eq!(kv.shutdown(), Ok(()));
    }

    #[test]
    fn mailbox_index_worker_failure_closes_channels_and_joins() {
        let mut runtime = ObservedRuntime::new();
        runtime.panic_worker = true;
        let mut result = 0;
        // SAFETY: result remains a live, uniquely borrowed u32 for this call.
        unsafe {
            sitas_core::mailbox_index::mailbox_index_test(&runtime, &mut result);
        }
        assert_eq!(result, sitas_core::mailbox_index::FAILURE_SENTINEL);
        assert_eq!(runtime.completed.load(Ordering::Acquire), 5);
    }
}
