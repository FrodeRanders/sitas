//! Reference model of the CharlotteOS async syscall / completion-capability ABI.
//!
//! This module is the sitas-side *executable specification* of the ABI drafted
//! in the CharlotteOS repository (`docs/async-syscall-abi.md`, Option C). It is
//! not a Unix backend and it talks to no kernel: it is an in-memory model of the
//! kernel's completion machinery, written so that sitas's reactor contract
//! ([`crate::reactor_backend`]) can be implemented against it with
//! `Handle = CompletionCap` — the answer to the Phase 1 question *"what is a
//! waitable interest?"* — instead of a Unix `RawFd`.
//!
//! The point is to pressure-test the ABI *shapes* before any kernel path
//! exists:
//!
//! - a submit that returns a completion capability naming in-flight work;
//! - one wait source (a per-shard completion queue) that folds timers, wakes,
//!   and async-syscall completions together;
//! - a cross-shard wake;
//! - drop-as-cancellation with deferred buffer reclaim (sitas's io_uring
//!   discipline, mirrored on the kernel side);
//! - bounded, non-lossy backpressure on both submission and completion.
//!
//! [`MockKernel`](crate::charlotte_abi::MockKernel) models the kernel;
//! [`CharlotteReactor`](crate::charlotte_abi::CharlotteReactor) is the
//! [`ReactorBackend`](crate::reactor_backend::ReactorBackend) a `sitas-charlotte`
//! backend would provide. The tests validate the decision-gate claims recorded
//! in `docs/sitas-runtime-model.md` §11 (Phase 2).

use alloc::collections::HashMap;
use alloc::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use core::time::{Duration, Instant};

use crate::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker};

/// A per-address-space handle naming an in-flight or completed async operation.
///
/// This is the CharlotteOS realization of the reactor backend's associated
/// `Handle` type (see [`ReactorBackend`]):
/// the value that crosses the syscall boundary and identifies a waitable
/// interest, backed in the kernel by an object implementing its `Observable`
/// trait.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct CompletionCap(pub u32);

/// Identifies one shard's completion queue; also the cross-shard wake target.
///
/// A shard is an LP-affine thread with a private completion queue, so this is
/// both "which queue do I drain" and "which shard do I wake".
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct CompletionQueueId(pub u32);

/// The operation an async syscall performs. Only enough variants to exercise the
/// buffer-ownership contract are modelled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpCode {
    /// No-op completion (no buffer transfer); used to model pure signals.
    Nop,
    /// Read into the submitted buffer (buffer returned on completion).
    Read,
    /// Write from the submitted buffer (buffer returned on completion).
    Write,
}

/// The terminal result carried by a completion-queue entry.
#[derive(Debug, PartialEq, Eq)]
pub enum OpResult {
    /// Success with an operation-specific value (for example bytes transferred).
    Ok(i64),
    /// Failure with an error code.
    Err(i32),
    /// The operation reached a terminal state after [`MockKernel::cancel`].
    Cancelled,
}

/// An owned buffer transferred to the kernel on submit and handed back on
/// completion — the analog of sitas passing a `Vec<u8>` into `read_at`.
pub type Buffer = Vec<u8>;

/// One entry drained from a shard's completion queue.
#[derive(Debug)]
pub struct Completion {
    /// The capability this completion is for.
    pub cap: CompletionCap,
    /// The terminal result.
    pub result: OpResult,
    /// Buffer ownership handed back to userspace, mirroring sitas's
    /// `WriteAtUringCompletion { bytes, buffer }`.
    pub returned: Option<Buffer>,
}

/// Reason a [`MockKernel::submit`] was refused. `WouldBlock` is the first-class,
/// non-fatal backpressure signal — the analog of `ShardSender::try_send`
/// returning `Full`.
#[derive(Debug, PartialEq, Eq)]
pub enum SubmitError {
    /// Submission queue / capability table is full; retry after draining.
    WouldBlock,
    /// The target completion queue does not exist.
    BadArgs,
}

/// Reason a [`MockKernel::wait`] failed.
#[derive(Debug, PartialEq, Eq)]
pub enum WaitError {
    /// The completion queue does not exist.
    UnknownQueue,
}

/// Reason a [`MockKernel::wake`] failed.
#[derive(Debug, PartialEq, Eq)]
pub enum WakeError {
    /// The completion queue does not exist.
    UnknownQueue,
}

/// Reason a [`MockKernel::cancel`] failed.
#[derive(Debug, PartialEq, Eq)]
pub enum CancelError {
    /// No such capability.
    UnknownCap,
}

/// Reason a [`MockKernel::close`] failed.
#[derive(Debug, PartialEq, Eq)]
pub enum CapError {
    /// No such capability.
    UnknownCap,
    /// The capability's operation has not reached a terminal completion yet.
    NotComplete,
}

/// The state of a `cancel` request.
#[derive(Debug, PartialEq, Eq)]
pub enum CancelState {
    /// The operation had already completed; nothing to cancel.
    AlreadyComplete,
    /// Cancellation was requested; a terminal completion will still be posted,
    /// and any transferred buffer is retained until then (deferred reclaim).
    CancelRequested,
}

/// The outcome of posting a completion into a shard's queue.
#[derive(Debug, PartialEq, Eq)]
pub enum PostOutcome {
    /// The completion was enqueued.
    Posted,
    /// The completion queue was full; the completion was *not* dropped, the
    /// queue is marked overflow-pending, and the operation stays in flight.
    Backpressured,
}

struct OpState {
    queue: CompletionQueueId,
    #[allow(dead_code)]
    kind: OpCode,
    buffer: Option<Buffer>,
    cancelling: bool,
    completed: bool,
}

struct Shard {
    cq: VecDeque<Completion>,
    cq_capacity: usize,
    woke: bool,
    overflow_pending: bool,
}

struct KernelState {
    ops: HashMap<CompletionCap, OpState>,
    shards: HashMap<CompletionQueueId, Shard>,
    next_cap: u32,
    next_queue: u32,
    cap_table_capacity: usize,
}

struct Inner {
    state: Mutex<KernelState>,
    cvar: Condvar,
}

/// Configuration bounds for the modelled kernel.
#[derive(Clone, Copy, Debug)]
pub struct MockKernelConfig {
    /// Maximum concurrent in-flight capabilities (submission backpressure).
    pub cap_table_capacity: usize,
}

impl Default for MockKernelConfig {
    fn default() -> Self {
        Self {
            cap_table_capacity: 1024,
        }
    }
}

/// An in-memory model of the CharlotteOS completion machinery, shared (cloneably)
/// across the shards of one modelled address space.
///
/// Cloning shares the same underlying kernel state, so a waker cloned onto
/// another thread wakes the same queues — matching the real cross-LP wake.
#[derive(Clone)]
pub struct MockKernel {
    inner: Arc<Inner>,
}

impl MockKernel {
    /// Creates an empty modelled kernel with the given bounds.
    pub fn new(config: MockKernelConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(KernelState {
                    ops: HashMap::new(),
                    shards: HashMap::new(),
                    next_cap: 0,
                    next_queue: 0,
                    cap_table_capacity: config.cap_table_capacity,
                }),
                cvar: Condvar::new(),
            }),
        }
    }

    /// Registers a shard (LP-affine thread) with a bounded completion queue and
    /// returns its queue id.
    pub fn register_shard(&self, cq_capacity: usize) -> CompletionQueueId {
        let mut state = self.lock();
        let id = CompletionQueueId(state.next_queue);
        state.next_queue += 1;
        state.shards.insert(
            id,
            Shard {
                cq: VecDeque::new(),
                cq_capacity,
                woke: false,
                overflow_pending: false,
            },
        );
        id
    }

    /// Starts an async operation. Returns immediately with a capability naming
    /// it; ownership of `buffer` transfers to the kernel until a terminal
    /// completion is posted. Returns [`SubmitError::WouldBlock`] under
    /// submission backpressure.
    pub fn submit(
        &self,
        cq: CompletionQueueId,
        op: OpCode,
        buffer: Option<Buffer>,
    ) -> Result<CompletionCap, SubmitError> {
        let mut state = self.lock();
        if !state.shards.contains_key(&cq) {
            return Err(SubmitError::BadArgs);
        }
        if state.ops.len() >= state.cap_table_capacity {
            return Err(SubmitError::WouldBlock);
        }
        let cap = CompletionCap(state.next_cap);
        state.next_cap += 1;
        state.ops.insert(
            cap,
            OpState {
                queue: cq,
                kind: op,
                buffer,
                cancelling: false,
                completed: false,
            },
        );
        Ok(cap)
    }

    /// Kernel-side hook: the modelled worker finished `cap`'s operation. Posts a
    /// completion to the owning shard's queue (handing the buffer back), waking
    /// any waiter. If the operation was cancelling, the posted result is
    /// [`OpResult::Cancelled`]. Honors completion backpressure non-lossily.
    pub fn complete(&self, cap: CompletionCap, result: OpResult) -> PostOutcome {
        let mut state = self.lock();
        let (queue, buffer, cancelling) = {
            let op = state.ops.get_mut(&cap).expect("unknown capability");
            (op.queue, op.buffer.take(), op.cancelling)
        };
        let final_result = if cancelling {
            OpResult::Cancelled
        } else {
            result
        };

        let full = {
            let shard = state.shards.get(&queue).expect("unknown queue");
            shard.cq.len() >= shard.cq_capacity
        };
        if full {
            // Non-lossy: retain the buffer, keep the op in flight, and mark the
            // queue so the next wait reports that draining is required.
            state.shards.get_mut(&queue).unwrap().overflow_pending = true;
            state.ops.get_mut(&cap).unwrap().buffer = buffer;
            return PostOutcome::Backpressured;
        }

        state.ops.get_mut(&cap).unwrap().completed = true;
        state
            .shards
            .get_mut(&queue)
            .unwrap()
            .cq
            .push_back(Completion {
                cap,
                result: final_result,
                returned: buffer,
            });
        self.inner.cvar.notify_all();
        PostOutcome::Posted
    }

    /// The reactor's only sleep. Blocks until at least `min_complete`
    /// completions are ready on `cq` (or any event when `min_complete == 0`), a
    /// wake arrives, or `deadline` elapses. Returns how many entries are now
    /// drainable.
    pub fn wait(
        &self,
        cq: CompletionQueueId,
        min_complete: u32,
        deadline: Option<Instant>,
    ) -> Result<u32, WaitError> {
        let mut state = self.lock();
        loop {
            let shard = state.shards.get(&cq).ok_or(WaitError::UnknownQueue)?;
            let ready = shard.cq.len() as u32;
            let satisfied = shard.woke
                || (min_complete == 0 && ready > 0)
                || (min_complete > 0 && ready >= min_complete);
            if satisfied {
                return Ok(ready);
            }

            match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        return Ok(ready);
                    }
                    let (guard, timed_out) = self
                        .inner
                        .cvar
                        .wait_timeout(state, dl - now)
                        .expect("kernel state mutex poisoned");
                    state = guard;
                    if timed_out.timed_out() {
                        let ready = state
                            .shards
                            .get(&cq)
                            .map(|s| s.cq.len() as u32)
                            .unwrap_or(0);
                        return Ok(ready);
                    }
                }
                None => {
                    state = self
                        .inner
                        .cvar
                        .wait(state)
                        .expect("kernel state mutex poisoned");
                }
            }
        }
    }

    /// Cross-shard wake: unblock another shard's [`wait`](Self::wait). Modeled
    /// after `send_ipi(target_lp)` landing in the target's IRQ dispatcher.
    pub fn wake(&self, cq: CompletionQueueId) -> Result<(), WakeError> {
        let mut state = self.lock();
        let shard = state.shards.get_mut(&cq).ok_or(WakeError::UnknownQueue)?;
        shard.woke = true;
        self.inner.cvar.notify_all();
        Ok(())
    }

    /// Drop-as-cancellation. If the operation already completed, reports
    /// [`CancelState::AlreadyComplete`]; otherwise marks it cancelling and
    /// *retains any transferred buffer* until a terminal completion is posted
    /// (deferred reclaim), mirroring sitas's `abandon_operation`.
    pub fn cancel(&self, cap: CompletionCap) -> Result<CancelState, CancelError> {
        let mut state = self.lock();
        let op = state.ops.get_mut(&cap).ok_or(CancelError::UnknownCap)?;
        if op.completed {
            Ok(CancelState::AlreadyComplete)
        } else {
            op.cancelling = true;
            Ok(CancelState::CancelRequested)
        }
    }

    /// Releases a completed/observed capability slot.
    pub fn close(&self, cap: CompletionCap) -> Result<(), CapError> {
        let mut state = self.lock();
        {
            let op = state.ops.get(&cap).ok_or(CapError::UnknownCap)?;
            if !op.completed {
                return Err(CapError::NotComplete);
            }
        }
        state.ops.remove(&cap);
        Ok(())
    }

    /// Peeks (without consuming) the capabilities whose completions are ready on
    /// `cq`. Used by the reactor to build the ready-handle set for its event.
    pub fn ready_caps(&self, cq: CompletionQueueId) -> Vec<CompletionCap> {
        let state = self.lock();
        state
            .shards
            .get(&cq)
            .map(|s| s.cq.iter().map(|c| c.cap).collect())
            .unwrap_or_default()
    }

    /// Drains all pending completion entries from `cq` (userspace reading its CQ
    /// out of shared memory), clearing the overflow-pending flag.
    pub fn drain(&self, cq: CompletionQueueId) -> Vec<Completion> {
        let mut state = self.lock();
        match state.shards.get_mut(&cq) {
            Some(shard) => {
                shard.overflow_pending = false;
                shard.cq.drain(..).collect()
            }
            None => Vec::new(),
        }
    }

    /// Consumes and returns whether a cross-shard wake was observed on `cq`.
    pub fn take_woke(&self, cq: CompletionQueueId) -> bool {
        let mut state = self.lock();
        state
            .shards
            .get_mut(&cq)
            .map(|s| std::mem::replace(&mut s.woke, false))
            .unwrap_or(false)
    }

    /// Whether `cq` is overflow-pending (completion backpressure engaged).
    pub fn overflow_pending(&self, cq: CompletionQueueId) -> bool {
        let state = self.lock();
        state
            .shards
            .get(&cq)
            .map(|s| s.overflow_pending)
            .unwrap_or(false)
    }

    /// Test/inspection helper: whether the kernel still owns a buffer for `cap`
    /// (i.e. it has not yet been handed back). Demonstrates deferred reclaim.
    pub fn kernel_holds_buffer(&self, cap: CompletionCap) -> bool {
        let state = self.lock();
        state
            .ops
            .get(&cap)
            .map(|o| o.buffer.is_some())
            .unwrap_or(false)
    }

    /// Builds a [`CharlotteReactor`] bound to `cq` over this kernel.
    pub fn reactor(&self, cq: CompletionQueueId) -> CharlotteReactor {
        CharlotteReactor {
            kernel: self.clone(),
            cq,
        }
    }

    fn lock(&self) -> spin::MutexGuard<'_, KernelState> {
        self.inner
            .state
            .lock()
            .expect("kernel state mutex poisoned")
    }
}

/// A cloneable wake handle for one shard's completion queue.
///
/// Satisfies sitas's `ReactorWaker: Clone + Send + Sync`; `wake()` is the
/// `wake(cq)` syscall (a cross-LP IPI in the real kernel).
#[derive(Clone)]
pub struct CharlotteWaker {
    kernel: MockKernel,
    cq: CompletionQueueId,
}

impl ReactorWaker for CharlotteWaker {
    fn wake(&self) -> crate::io::Result<()> {
        self.kernel.wake(self.cq).map_err(|_| {
            crate::io::Error::new(crate::io::ErrorKind::NotFound, "unknown completion queue")
        })
    }
}

/// The owned result of one [`CharlotteReactor::wait`], reporting whether a wake
/// was drained and which capabilities became ready.
pub struct CharlotteEvent {
    woke: bool,
    readable: Vec<CompletionCap>,
    writable: Vec<CompletionCap>,
}

impl ReactorEvent for CharlotteEvent {
    type Handle = CompletionCap;

    fn woke(&self) -> bool {
        self.woke
    }

    fn readable(&self) -> &[CompletionCap] {
        &self.readable
    }

    fn writable(&self) -> &[CompletionCap] {
        // Completion capabilities are one-shot and direction-agnostic: an
        // operation's completion is "readable". There is no separate writable
        // interest set in this model.
        &self.writable
    }
}

/// The [`ReactorBackend`] a `sitas-charlotte` backend would provide: one shard's
/// view of the modelled kernel.
///
/// This is the concrete rendering of the sketch in `docs/async-syscall-abi.md`
/// §9: `type Handle = CompletionCap`, `wait` blocks on the shard's completion
/// queue, and the waker is a cross-shard wake. Interests are registered at
/// submit-time (via [`MockKernel::submit`]), not passed to `wait`, so the
/// `read`/`write` handle slices are unused — the shard watches its single queue.
pub struct CharlotteReactor {
    kernel: MockKernel,
    cq: CompletionQueueId,
}

impl CharlotteReactor {
    /// The kernel this reactor's shard belongs to (for submitting work).
    pub fn kernel(&self) -> &MockKernel {
        &self.kernel
    }

    /// This reactor's completion-queue id.
    pub fn queue(&self) -> CompletionQueueId {
        self.cq
    }
}

impl ReactorBackend for CharlotteReactor {
    type Waker = CharlotteWaker;
    type Handle = CompletionCap;
    type Event = CharlotteEvent;

    fn waker(&self) -> CharlotteWaker {
        CharlotteWaker {
            kernel: self.kernel.clone(),
            cq: self.cq,
        }
    }

    fn wait(
        &self,
        _read: &[CompletionCap],
        _write: &[CompletionCap],
        timeout: Option<Duration>,
    ) -> crate::io::Result<CharlotteEvent> {
        let deadline = timeout.map(|d| Instant::now() + d);
        self.kernel.wait(self.cq, 0, deadline).map_err(|_| {
            crate::io::Error::new(crate::io::ErrorKind::NotFound, "unknown completion queue")
        })?;
        let woke = self.kernel.take_woke(self.cq);
        let readable = self.kernel.ready_caps(self.cq);
        Ok(CharlotteEvent {
            woke,
            readable,
            writable: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kernel() -> MockKernel {
        MockKernel::new(MockKernelConfig::default())
    }

    #[test]
    fn submit_complete_wait_returns_buffer() {
        let k = kernel();
        let cq = k.register_shard(16);
        let cap = k.submit(cq, OpCode::Read, Some(vec![0u8; 4])).unwrap();

        // Kernel still owns the buffer while the op is in flight.
        assert!(k.kernel_holds_buffer(cap));

        // Worker finishes: fill the buffer and post the completion.
        // (Model the fill by handing back a populated buffer via completion.)
        assert_eq!(k.complete(cap, OpResult::Ok(4)), PostOutcome::Posted);

        let reactor = k.reactor(cq);
        let event = reactor
            .wait(&[], &[], Some(Duration::from_secs(1)))
            .unwrap();
        assert!(!event.woke());
        assert_eq!(event.readable(), &[cap]);

        let mut drained = k.drain(cq);
        assert_eq!(drained.len(), 1);
        let completion = drained.pop().unwrap();
        assert_eq!(completion.cap, cap);
        assert!(matches!(completion.result, OpResult::Ok(4)));
        assert_eq!(completion.returned, Some(vec![0u8; 4]));

        // Capability can now be closed.
        k.close(cap).unwrap();
        assert_eq!(k.close(cap), Err(CapError::UnknownCap));
    }

    #[test]
    fn wait_times_out_when_idle() {
        let k = kernel();
        let cq = k.register_shard(16);
        let reactor = k.reactor(cq);
        let event = reactor
            .wait(&[], &[], Some(Duration::from_millis(1)))
            .unwrap();
        assert!(!event.woke());
        assert!(event.readable().is_empty());
        assert!(event.writable().is_empty());
    }

    #[test]
    fn wake_before_wait_is_observed() {
        let k = kernel();
        let cq = k.register_shard(16);
        let reactor = k.reactor(cq);
        reactor.waker().wake().unwrap();
        let event = reactor
            .wait(&[], &[], Some(Duration::from_secs(1)))
            .unwrap();
        assert!(event.woke());
    }

    #[test]
    fn cancel_defers_buffer_until_terminal_completion() {
        let k = kernel();
        let cq = k.register_shard(16);
        let cap = k.submit(cq, OpCode::Write, Some(vec![1u8, 2, 3])).unwrap();

        // Drop-as-cancel: request cancellation while in flight.
        assert_eq!(k.cancel(cap).unwrap(), CancelState::CancelRequested);
        // The kernel must still own the buffer (deferred reclaim) — it may still
        // touch it until the terminal completion is posted.
        assert!(k.kernel_holds_buffer(cap));

        // The terminal completion arrives as Cancelled, handing the buffer back.
        assert_eq!(k.complete(cap, OpResult::Ok(3)), PostOutcome::Posted);
        assert!(!k.kernel_holds_buffer(cap));

        let mut drained = k.drain(cq);
        let completion = drained.pop().unwrap();
        assert!(matches!(completion.result, OpResult::Cancelled));
        assert_eq!(completion.returned, Some(vec![1u8, 2, 3]));
    }

    #[test]
    fn cancel_after_completion_reports_already_complete() {
        let k = kernel();
        let cq = k.register_shard(16);
        let cap = k.submit(cq, OpCode::Nop, None).unwrap();
        k.complete(cap, OpResult::Ok(0));
        assert_eq!(k.cancel(cap).unwrap(), CancelState::AlreadyComplete);
    }

    #[test]
    fn submit_backpressure_returns_would_block() {
        let k = MockKernel::new(MockKernelConfig {
            cap_table_capacity: 2,
        });
        let cq = k.register_shard(16);
        let _a = k.submit(cq, OpCode::Nop, None).unwrap();
        let _b = k.submit(cq, OpCode::Nop, None).unwrap();
        assert_eq!(
            k.submit(cq, OpCode::Nop, None),
            Err(SubmitError::WouldBlock)
        );
    }

    #[test]
    fn completion_queue_backpressure_is_non_lossy() {
        let k = kernel();
        let cq = k.register_shard(1);
        let a = k.submit(cq, OpCode::Read, Some(vec![0u8; 2])).unwrap();
        let b = k.submit(cq, OpCode::Read, Some(vec![0u8; 2])).unwrap();

        assert_eq!(k.complete(a, OpResult::Ok(2)), PostOutcome::Posted);
        // Queue is full (capacity 1): the second completion is refused, not
        // dropped; the op stays in flight and keeps its buffer.
        assert_eq!(k.complete(b, OpResult::Ok(2)), PostOutcome::Backpressured);
        assert!(k.overflow_pending(cq));
        assert!(k.kernel_holds_buffer(b));

        // Draining frees space and clears the overflow flag; the op can then be
        // posted successfully (backpressure applied upstream, nothing lost).
        let drained = k.drain(cq);
        assert_eq!(drained.len(), 1);
        assert!(!k.overflow_pending(cq));
        assert_eq!(k.complete(b, OpResult::Ok(2)), PostOutcome::Posted);
        assert_eq!(k.drain(cq).len(), 1);
    }

    #[test]
    fn cross_shard_wake_unblocks_a_blocked_reactor() {
        // A reactor blocked with no deadline is unblocked by a wake issued from
        // "another shard" (another thread) via a cloned waker — the model's
        // stand-in for a cross-LP IPI.
        let k = kernel();
        let cq = k.register_shard(16);
        let reactor = k.reactor(cq);
        let waker = reactor.waker();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            waker.wake().unwrap();
        });

        let event = reactor.wait(&[], &[], None).unwrap();
        assert!(event.woke());
        handle.join().unwrap();
    }

    /// Proves a consumer can drive the CharlotteOS reactor purely through the
    /// [`ReactorBackend`] contract with `Handle = CompletionCap`, exactly as the
    /// executor relies on `OsReactor` today — the Option-C shape validation.
    fn drive_through_contract<B>(reactor: &B)
    where
        B: ReactorBackend<Handle = CompletionCap>,
    {
        let event = reactor
            .wait(&[], &[], Some(Duration::from_millis(1)))
            .unwrap();
        assert!(!event.woke());
        assert!(event.readable().is_empty());

        reactor.waker().wake().unwrap();
        let event = reactor
            .wait(&[], &[], Some(Duration::from_secs(1)))
            .unwrap();
        assert!(event.woke());
    }

    #[test]
    fn charlotte_reactor_satisfies_reactor_backend_contract() {
        let k = kernel();
        let cq = k.register_shard(16);
        let reactor = k.reactor(cq);
        drive_through_contract(&reactor);
    }
}
