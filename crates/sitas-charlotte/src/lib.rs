//! sitas-charlotte: CharlotteOS backend for the sitas shard-per-core runtime.
//!
//! Implements the `ReactorBackend` and `ShardRuntime` traits against the
//! CharlotteOS kernel's async syscall ABI. This is the runtime that actually
//! runs sitas's executor on CharlotteOS.
//!
//! ## Design
//!
//! The executor's idle wait (`wait(read, write, timeout)`) drains the kernel's
//! CQ ring — a shared-memory page mapped at a known virtual address in the
//! user AS, written by the kernel and read here without syscalls — and, when
//! the ring is empty, **blocks** in the kernel via `CQ_WAIT` /
//! `CQ_WAIT_TIMEOUT`. The kernel wait returns when a completion entry is
//! posted, when a peer thread posts an explicit `CQ_WAKE`, or (for the timed
//! variant) when the deadline fires. There is no busy polling.
//!
//! The waker (`ReactorWaker::wake`) posts `CQ_WAKE`, which releases any thread
//! of this address space blocked in a CQ wait. Until per-shard CQ partitioning
//! exists, the CQ (and therefore the wake) is per-process: all shards share
//! one ring, so a wake releases every blocked shard of the process rather than
//! one target LP.
//!
//! Thread spawn (`ShardRuntime::spawn_shard`) uses the kernel's
//! `spawn_thread` via SVC, pinned to a specific LP.

#![no_std]
#![deny(unsafe_op_in_unsafe_fn)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::time::Duration;

use sitas_core::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker};
use sitas_core::shard::ShardId;
use sitas_core::shard_runtime::{
    RawJoinHandle, ShardJoinHandle, ShardParker, ShardRuntime,
};
use spin::Mutex;

/// The virtual address where the default (queue 0) CQ ring is mapped.
const CQ_RING_VADDR: usize = 0x0000_0000_0001_1000;
/// The canonical config page (kernel↔user contract).
const CONFIG_VADDR: usize = 0x0000_0000_0001_0000;
/// Byte offset in the config page of the per-shard CQ ring base virtual
/// address (matches `catten_rt::config::SHARD_CQ_BASE_OFFSET`).
const SHARD_CQ_BASE_OFFSET: usize = 2064;
/// Byte offset of the per-shard CQ ring count
/// (matches `catten_rt::config::SHARD_CQ_COUNT_OFFSET`).
const SHARD_CQ_COUNT_OFFSET: usize = 2072;
const SYSCALL_COMPLETION_SUBMIT: u16 = 1;
const SYSCALL_SPAWN_THREAD: u16 = 7;
const SYSCALL_CQ_WAIT: u16 = 12;
const SYSCALL_CQ_WAKE: u16 = 41;
const SYSCALL_CQ_WAIT_TIMEOUT: u16 = 42;

/// Read the per-shard CQ ring layout the loader published in the config page:
/// `(base_vaddr, count)`, or `(0, 0)` if no per-shard rings were mapped.
fn shard_cq_layout() -> (usize, usize) {
    let base =
        unsafe { core::ptr::read_volatile((CONFIG_VADDR + SHARD_CQ_BASE_OFFSET) as *const u64) };
    let count =
        unsafe { core::ptr::read_volatile((CONFIG_VADDR + SHARD_CQ_COUNT_OFFSET) as *const u64) };
    (base as usize, count as usize)
}

static SHARD_ENTRIES: Mutex<Vec<Box<dyn FnOnce() + Send>>> = Mutex::new(Vec::new());

// ---- syscall wrappers -------------------------------------------------------

/// Invoke a syscall with the given SVC immediate and arguments.
#[inline(always)]
unsafe fn syscall(imm: u16, arg1: u64, arg2: u64, arg3: u64) -> u64 {
    macro_rules! svc {
        ($number:literal) => {{
            let ret: u64;
            unsafe {
                core::arch::asm!(
                    concat!("svc #", stringify!($number)),
                    lateout("x0") ret,
                    in("x1") arg1,
                    in("x2") arg2,
                    in("x3") arg3,
                    options(nostack, nomem, preserves_flags),
                );
            }
            ret
        }};
    }

    match imm {
        SYSCALL_COMPLETION_SUBMIT => svc!(1),
        SYSCALL_SPAWN_THREAD => svc!(7),
        SYSCALL_CQ_WAIT => svc!(12),
        SYSCALL_CQ_WAKE => svc!(41),
        _ => {
            let _ = (arg1, arg2, arg3);
            1
        }
    }
}

/// Syscall: spawn a thread pinned to a specific LP.
#[inline(always)]
unsafe fn sys_spawn(entry_vaddr: u64, target_lp: u64) -> u64 {
    unsafe { syscall(SYSCALL_SPAWN_THREAD, entry_vaddr, target_lp, 0) }
}

/// Post an explicit wake to queue `cq` (a per-shard wake releases only the
/// target shard's blocked `CQ_WAIT`).
#[inline(always)]
unsafe fn sys_cq_wake(cq: u32) -> u64 {
    unsafe { syscall(SYSCALL_CQ_WAKE, cq as u64, 0, 0) }
}

/// Block until the shard's CQ has at least `min_complete` entries or an
/// explicit wake is posted to it.
#[inline(always)]
unsafe fn sys_cq_wait(min_complete: u64, cq: u32) -> u64 {
    unsafe { syscall(SYSCALL_CQ_WAIT, min_complete, cq as u64, 0) }
}

/// Like [`sys_cq_wait`] but also returns when `timeout_ms` elapses.
/// Returns `(pending, timed_out)`.
#[inline(always)]
unsafe fn sys_cq_wait_timeout(min_complete: u64, timeout_ms: u64, cq: u32) -> (u64, u64) {
    let ret: u64;
    let timed_out: u64;
    unsafe {
        core::arch::asm!(
            "svc #42",
            lateout("x0") ret,
            inlateout("x1") min_complete => timed_out,
            in("x2") timeout_ms,
            in("x3") cq as u64,
            options(nostack, nomem, preserves_flags),
        );
    }
    let _ = SYSCALL_CQ_WAIT_TIMEOUT;
    (ret, timed_out)
}

extern "C" fn shard_entry_trampoline() {
    let entry = SHARD_ENTRIES.lock().pop();
    if let Some(entry) = entry {
        entry();
    }
}

// ---- reactor backend --------------------------------------------------------

/// A reactor for one LP (one shard). Where per-shard CQ rings are mapped,
/// one shard's executor waits on its own ring and only a wake targeted at
/// that ring's queue id releases it; where they are not (legacy path) the
/// process-wide default queue 0 is used. All state is in the CQ ring mapped
/// in the user AS and the per-LP kernel completion table.
pub struct CharlotteReactor {
    lp_id: u32,
    /// The virtual address of this shard's CQ ring page.
    ring_vaddr: usize,
    /// The kernel queue id the reactor waits on and the waker wakes.
    cq_id: u32,
}

impl CharlotteReactor {
    /// Default constructor: process-wide queue 0 at the canonical ring
    /// address. For single-shard services and the pre-per-shard legacy path.
    pub fn new(lp_id: u32) -> Self {
        Self {
            lp_id,
            ring_vaddr: CQ_RING_VADDR,
            cq_id: 0,
        }
    }

    /// Per-shard constructor: the reactor blocks on queue `cq_id` and drains
    /// the ring page at `ring_vaddr`. `lp_id` records the intended core
    /// affinity.
    pub fn with_cq(lp_id: u32, ring_vaddr: usize, cq_id: u32) -> Self {
        Self {
            lp_id,
            ring_vaddr,
            cq_id,
        }
    }

    /// Returns the underlying CQ ring header at the shard's ring address.
    pub fn cq(&self) -> &CqHeader {
        unsafe { &*(self.ring_vaddr as *const CqHeader) }
    }

    /// Submit an async operation and return a future that completes when the
    /// kernel posts the completion.
    pub fn submit_wait(&self, op_code: u64, buffer: Option<&[u8]>) -> u64 {
        let buf_ptr = buffer.map(|b| b.as_ptr() as u64).unwrap_or(0);
        let buf_len = buffer.map(|b| b.len() as u64).unwrap_or(0);
        // Syscall #1: COMPLETION_SUBMIT returns the kernel-owned completion cap.
        unsafe { syscall(SYSCALL_COMPLETION_SUBMIT, op_code, buf_ptr, buf_len) }
    }
}

// ---- CQ ring layout ---------------------------------------------------------

/// The header of the shared CQ ring, read by userspace and written by the
/// kernel. Layout must match `crates/catten/src/completion/cq.rs`.
/// The CQ header is public so userspace code (test binaries) can poll and drain.
#[repr(C)]
pub struct CqHeader {
    head: u32,
    tail: u32,
    capacity: u32,
    overflow: u32,
}

#[repr(C)]
pub struct CqEntry {
    pub cap: u64,
    pub result: i64,
}

impl CqHeader {
    pub fn pending(&self) -> u32 {
        let h = unsafe { core::ptr::read_volatile(&self.head) };
        let t = unsafe { core::ptr::read_volatile(&self.tail) };
        if h >= t { h - t } else { h + self.capacity - t }
    }

    pub fn read_one(&self) -> Option<CqEntry> {
        if self.pending() == 0 {
            return None;
        }
        let t = unsafe { core::ptr::read_volatile(&self.tail) };
        let entry_ptr = unsafe {
            let base = self as *const Self as *const u8;
            let entries_offset = core::mem::offset_of!(Self, overflow) + 4;
            base.add(entries_offset)
                .add(t as usize * core::mem::size_of::<CqEntry>()) as *const CqEntry
        };
        unsafe {
            let entry = core::ptr::read_volatile(entry_ptr);
            let this = self as *const Self as *mut Self;
            core::ptr::write_volatile(&raw mut (*this).tail, (t + 1) % self.capacity);
            Some(entry)
        }
    }
}

// ---- trait implementations --------------------------------------------------

/// A handle that can wake this reactor from another shard/LP.
#[derive(Clone)]
pub struct CharlWaker {
    /// The logical processor the owning shard runs on (for future affinity
    /// use; currently not targeted by the wake).
    target_lp: u32,
    /// The completion queue id blocked in `CQ_WAIT`; a wake targets only
    /// this queue so per-shard rings receive isolated, non-stealing wakes.
    cq_id: u32,
}

impl ReactorWaker for CharlWaker {
    fn wake(&self) -> Result<(), sitas_core::io::ErrorKind> {
        let result = unsafe { sys_cq_wake(self.cq_id) };
        if result == 0 {
            Ok(())
        } else {
            Err(sitas_core::io::ErrorKind::WouldBlock)
        }
    }
}

impl core::fmt::Debug for CharlWaker {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CharlWaker")
            .field("target_lp", &self.target_lp)
            .finish()
    }
}

pub struct CharlEvent {
    woke: bool,
    readable: Vec<u64>,
}

impl ReactorEvent for CharlEvent {
    type Handle = u64;
    fn woke(&self) -> bool {
        self.woke
    }
    fn readable(&self) -> &[u64] {
        &self.readable
    }
    fn writable(&self) -> &[u64] {
        &[]
    }
}

impl ReactorBackend for CharlotteReactor {
    type Waker = CharlWaker;
    type Handle = u64;
    type Event = CharlEvent;

    fn waker(&self) -> CharlWaker {
        CharlWaker {
            target_lp: self.lp_id,
            cq_id: self.cq_id,
        }
    }

    fn wait(
        &self,
        _read: &[u64],
        _write: &[u64],
        timeout: Option<Duration>,
    ) -> Result<CharlEvent, sitas_core::io::ErrorKind> {
        loop {
            // Drain whatever the kernel has already posted — no syscall needed.
            let pending = self.cq().pending();
            if pending > 0 {
                let mut caps = Vec::new();
                for _ in 0..pending {
                    if let Some(entry) = self.cq().read_one() {
                        caps.push(entry.cap);
                    }
                }
                return Ok(CharlEvent {
                    woke: false,
                    readable: caps,
                });
            }

            // Ring empty: block in the kernel until a completion entry is
            // posted, a peer posts CQ_WAKE to this shard's queue, or the
            // deadline fires.
            match timeout {
                Some(duration) => {
                    let timeout_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1);
                    let (_pending, timed_out) =
                        unsafe { sys_cq_wait_timeout(1, timeout_ms, self.cq_id) };
                    if self.cq().pending() > 0 {
                        continue; // completions arrived: drain them above
                    }
                    return Ok(CharlEvent {
                        woke: timed_out == 0,
                        readable: Vec::new(),
                    });
                }
                None => {
                    unsafe { sys_cq_wait(1, self.cq_id) };
                    if self.cq().pending() > 0 {
                        continue; // completions arrived: drain them above
                    }
                    return Ok(CharlEvent {
                        woke: true,
                        readable: Vec::new(),
                    });
                }
            }
        }
    }
}

// ---- ShardRuntime implementation -------------------------------------------

impl ShardRuntime for CharlotteReactor {
    type JoinHandle<T: Send> = ShardJoinHandle<T>;
    type Reactor = CharlotteReactor;

    fn spawn_shard<T: Send + 'static>(
        &self,
        shard_id: ShardId,
        _placement: sitas_core::placement::ShardPlacement,
        entry: alloc::boxed::Box<dyn FnOnce() -> T + Send>,
    ) -> ShardJoinHandle<T> {
        SHARD_ENTRIES.lock().push(Box::new(move || {
            let _ = entry();
        }));
        let entry_vaddr = shard_entry_trampoline as *const () as usize;
        unsafe {
            sys_spawn(entry_vaddr as u64, shard_id.0 as u64);
        }
        ShardJoinHandle::from_raw(RawJoinHandle)
    }

    fn channel<M: Send + 'static>(
        &self,
        capacity: usize,
    ) -> sitas_core::shard_runtime::ShardChannelResult<M> {
        sitas_core::shard_runtime::channel(capacity)
    }

    fn sleep(&self, duration: Duration) {
        // Block in the kernel for the requested duration instead of spinning.
        // A min_complete the small ring can never reach means only the
        // deadline (or a spurious peer wake) releases us.
        let timeout_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1);
        let _ = unsafe { sys_cq_wait_timeout(u64::from(u32::MAX), timeout_ms, self.cq_id) };
    }

    fn parker(&self) -> alloc::sync::Arc<dyn ShardParker> {
        // The parker always uses the process-wide default queue (0). The
        // requester thread (main / catten-user) parks there waiting for
        // replies, and each serving shard's reply code unpark()s CQ 0; only
        // one non-shard waiter exists per process so wake-stealing is not a
        // concern. Cross-shard wakes flow through the executor's TaskWaker →
        // ReactorWaker targeting each shard's own CQ.
        alloc::sync::Arc::new(CharlotteParker::new(0))
    }

    fn shard_reactor(&self, shard_id: ShardId) -> CharlotteReactor {
        let (base, count) = shard_cq_layout();
        let shard = shard_id.0;
        if base != 0 && shard < count {
            // Per-shard ring: `cq_id = shard + 1`, ring at `base + shard * 4096`.
            CharlotteReactor::with_cq(shard as u32, base + shard * 4096, (shard as u32) + 1)
        } else {
            // Fallback for shard counts beyond the pre-allocated rings, or when
            // the loader did not map per-shard CQ pages (legacy single-ring path).
            CharlotteReactor::new(shard as u32)
        }
    }
}

/// Parks and wakes shards through the kernel completion queue.
///
/// A shard waiting for an inter-shard message or reply parks in `CQ_WAIT`
/// (bounded by a short deadline so a stolen or coalesced wake self-heals while
/// the process-wide CQ is shared by all shards); a peer releases it with
/// `CQ_WAKE`. This replaces the KV service's former `spin_loop` busy-waits, so
/// a blocked shard consumes no CPU.
///
/// The deadline bound is a deliberate robustness choice until per-shard CQ
/// rings are mapped into user space: with one process-wide ring, a single
/// `CQ_WAKE` releases only one waiter, so a shard whose wake was consumed by a
/// peer still re-checks its state within one park interval rather than
/// stalling.
pub struct CharlotteParker {
    cq_id: u32,
}

impl CharlotteParker {
    pub fn new(cq_id: u32) -> Self {
        Self { cq_id }
    }
}

/// The park interval, chosen small enough that a stolen/coalesced wake costs
/// at most this much latency, and large enough that idle parking is genuinely
/// cheap (a single kernel wait, no spinning).
const PARK_INTERVAL_MS: u64 = 5;

impl ShardParker for CharlotteParker {
    fn park(&self, timeout: Option<Duration>) {
        let ms = match timeout {
            Some(duration) => {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(PARK_INTERVAL_MS).max(1)
            }
            None => PARK_INTERVAL_MS,
        };
        // A min_complete the small ring can never reach means the wait is
        // released only by a peer `CQ_WAKE` or by the deadline — never by a
        // stray completion entry meant for the reactor.
        let _ = unsafe { sys_cq_wait_timeout(u64::from(u32::MAX), ms, self.cq_id) };
    }

    fn unpark(&self) {
        let _ = unsafe { sys_cq_wake(self.cq_id) };
    }
}
