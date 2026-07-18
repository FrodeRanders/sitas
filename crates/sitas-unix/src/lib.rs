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
    type JoinHandle<T: Send> = ShardJoinHandle<T>;
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
