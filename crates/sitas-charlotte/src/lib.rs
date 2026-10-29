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
//! the ring is empty, **blocks** in the kernel via checked `CQ_WAIT_CHECKED` (88). The kernel wait returns when a completion entry is
//! posted, when a peer thread posts an explicit `CQ_WAKE`, or (for the timed
//! variant) when the deadline fires. There is no busy polling.
//!
//! The waker (`ReactorWaker::wake`) posts `CQ_WAKE`, which releases any thread
//! blocked on the exact selected queue. Mapped per-shard rings isolate reactor
//! wakes; explicitly shared/default parkers retain their bounded recovery policy.
//!
//! Thread spawn (`ShardRuntime::spawn_shard`) uses the kernel's
//! `spawn_thread` via SVC, pinned to a specific LP.

#![no_std]
#![deny(unsafe_op_in_unsafe_fn)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use sitas_core::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker};
use sitas_core::shard::ShardId;
use sitas_core::shard_runtime::{ShardParker, ShardRuntime, ShardThread};
use spin::Mutex;

/// The virtual address where the default (queue 0) CQ ring is mapped.
#[cfg(target_os = "none")]
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
const SYSCALL_COMPLETION_POLL: u16 = 3;
const SYSCALL_COMPLETION_WAIT: u16 = 4;
const SYSCALL_COMPLETION_CLOSE: u16 = 6;
const SYSCALL_COMPLETION_CANCEL: u16 = 5;
const SYSCALL_SPAWN_THREAD: u16 = 7;
const SYSCALL_THREAD_EXIT: u16 = 8;
const SYSCALL_CQ_WAIT_CHECKED: u16 = 88;
const SYSCALL_CQ_WAKE: u16 = 41;
const SYSCALL_MONOTONIC_CLOCK: u16 = 79;
const SYSCALL_DOMAIN_ABORT: u16 = 68;
const SYSCALL_LOG_STR: u16 = 74;
const SYSCALL_OBSERVE_THREAD_EXIT: u16 = 61;
const SYSCALL_GET_TID: u16 = 62;

/// Read the per-shard CQ ring layout the loader published in the config page:
/// `(base_vaddr, count)`, or `(0, 0)` if no per-shard rings were mapped.
fn shard_cq_layout() -> (usize, usize) {
    let base =
        unsafe { core::ptr::read_volatile((CONFIG_VADDR + SHARD_CQ_BASE_OFFSET) as *const u64) };
    let count =
        unsafe { core::ptr::read_volatile((CONFIG_VADDR + SHARD_CQ_COUNT_OFFSET) as *const u64) };
    (base as usize, count as usize)
}

/// Entry queue for shard threads: `(tid, closure)`. A spawned shard thread
/// removes the entry whose tid matches its own (the kernel reports it via
/// `GET_TID`), so the closure a thread runs — and therefore the result slot
/// its handle reads — is determined by the thread's identity, never by the
/// order threads happen to be scheduled in. (A plain pop would race: two
/// shards can be scheduled in either order, and the handle created for spawn
/// N must observe the value computed by exactly that spawn.)
type ShardEntry = (u64, Box<dyn FnOnce() + Send>);

static SHARD_ENTRIES: Mutex<Vec<ShardEntry>> = Mutex::new(Vec::new());
// Serializes userspace consumers, including legacy reactors sharing CQ 0.
// Never held across a syscall or a task poll; kernel producers do not use it.
static CQ_CONSUMER: Mutex<()> = Mutex::new(());

// ---- syscall wrappers -------------------------------------------------------

/// Invoke a syscall with the given SVC immediate and arguments.
#[inline(always)]
unsafe fn syscall(imm: u16, arg1: u64, arg2: u64, arg3: u64) -> u64 {
    #[cfg(target_arch = "aarch64")]
    macro_rules! svc {
        ($number:literal) => {{
            let ret: u64;
            unsafe {
                core::arch::asm!(
                    concat!("svc #", stringify!($number)),
                    lateout("x0") ret,
                    inlateout("x1") arg1 => _,
                    inlateout("x2") arg2 => _,
                    inlateout("x3") arg3 => _,
                    options(nostack, preserves_flags),
                );
            }
            ret
        }};
    }

    #[cfg(target_arch = "x86_64")]
    macro_rules! svc {
        ($number:literal) => {{
            let ret: u64;
            unsafe {
                core::arch::asm!(
                    "syscall",
                    inlateout("rax") ($number as u64) => ret,
                    inlateout("rdi") arg1 => _,
                    inlateout("rsi") arg2 => _,
                    inlateout("rdx") arg3 => _,
                    lateout("rcx") _,
                    lateout("r11") _,
                    options(preserves_flags),
                );
            }
            ret
        }};
    }

    match imm {
        SYSCALL_COMPLETION_SUBMIT => svc!(1),
        SYSCALL_COMPLETION_WAIT => svc!(4),
        SYSCALL_COMPLETION_CLOSE => svc!(6),
        SYSCALL_COMPLETION_CANCEL => svc!(5),
        SYSCALL_SPAWN_THREAD => svc!(7),
        SYSCALL_THREAD_EXIT => svc!(8),
        SYSCALL_DOMAIN_ABORT => svc!(68),
        SYSCALL_LOG_STR => svc!(74),
        SYSCALL_CQ_WAKE => svc!(41),
        SYSCALL_OBSERVE_THREAD_EXIT => svc!(61),
        SYSCALL_GET_TID => svc!(62),
        _ => {
            let _ = (arg1, arg2, arg3);
            1
        }
    }
}

/// Syscall: spawn a thread pinned to a specific LP.
#[inline(always)]
unsafe fn sys_spawn(entry_vaddr: u64, target_lp: u64) -> (u64, u64) {
    // SAFETY: the caller supplies this domain's executable trampoline.
    unsafe { syscall_pair(SYSCALL_SPAWN_THREAD, entry_vaddr, target_lp, 0) }
}

/// Terminate the calling thread; the kernel reaps it and fires any exit
/// observer registered against it. Never returns.
#[inline(always)]
unsafe fn sys_thread_exit() -> ! {
    let _ = unsafe { syscall(SYSCALL_THREAD_EXIT, 0, 0, 0) };
    // SAFETY: the kernel ABI is terminal. Trap if it is violated rather than
    // giving the optimizer permission to assume an impossible Rust execution.
    unsafe {
        #[cfg(target_arch = "aarch64")]
        core::arch::asm!("udf #0", options(noreturn));
        #[cfg(target_arch = "x86_64")]
        core::arch::asm!("ud2", options(noreturn));
    }
}

/// Syscall: report the calling thread's kernel thread id.
#[inline(always)]
unsafe fn sys_get_tid() -> u64 {
    unsafe { syscall(SYSCALL_GET_TID, 0, 0, 0) }
}

/// Register a completion capability that fires when thread `tid` exits.
/// Returns the capability, or `u64::MAX` when the thread id is invalid.
#[inline(always)]
unsafe fn sys_observe_exit(tid: u64, generation: u64) -> u64 {
    // SAFETY: retain the exact identity returned by SPAWN_THREAD, so a
    // recycled numeric tid cannot redirect the exit observation.
    unsafe { syscall(SYSCALL_OBSERVE_THREAD_EXIT, tid, generation, 0) }
}

/// Block the calling thread until the given completion capability reaches a
/// terminal state.
#[inline(always)]
unsafe fn sys_wait(cap: u64) {
    unsafe { syscall(SYSCALL_COMPLETION_WAIT, cap, 0, 0) };
}

/// Release a completed capability slot.
#[inline(always)]
unsafe fn sys_close(cap: u64) {
    unsafe { syscall(SYSCALL_COMPLETION_CLOSE, cap, 0, 0) };
}

/// Poll a completion capability: `(0, _)` once terminal, `(1, 0)` while
/// pending.
#[inline(always)]
unsafe fn sys_poll(cap: u64) -> (u64, u64) {
    let _ = SYSCALL_COMPLETION_POLL;
    let state: u64;
    let result: u64;
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "svc #3",
            lateout("x0") state,
            inlateout("x1") cap => result,
            options(nostack, preserves_flags),
        );
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::asm!(
            "syscall",
            inlateout("rax") (SYSCALL_COMPLETION_POLL as u64) => state,
            inlateout("rdi") cap => result,
            lateout("rcx") _,
            lateout("r11") _,
            options(preserves_flags),
        );
    }
    (state, result)
}

/// Post an explicit wake to queue `cq` (a per-shard wake releases only the
/// target shard's blocked checked CQ wait).
#[inline(always)]
unsafe fn sys_cq_wake(cq: u32) -> u64 {
    unsafe { syscall(SYSCALL_CQ_WAKE, cq as u64, 0, 0) }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CqWaitOutcome {
    Ready,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CqWaitError {
    AdmissionRejected,
    InvalidRequest,
    InvalidStatus,
}

fn decode_cq_wait(status: u64, timed: bool) -> Result<CqWaitOutcome, CqWaitError> {
    match status {
        0 => Ok(CqWaitOutcome::Ready),
        1 if timed => Ok(CqWaitOutcome::Deadline),
        2 => Err(CqWaitError::InvalidRequest),
        3 => Err(CqWaitError::AdmissionRejected),
        _ => Err(CqWaitError::InvalidStatus),
    }
}

/// Fixed two-result ABI boundaries; neither publishes an owning capability.
unsafe fn syscall_pair(imm: u16, arg1: u64, arg2: u64, arg3: u64) -> (u64, u64) {
    let ret: u64;
    let secondary: u64;
    #[cfg(target_arch = "aarch64")]
    {
        macro_rules! pair {
            ($number:literal) => {
                // SAFETY: only the fixed clock and checked CQ ABIs reach this
                // boundary. All result registers are declared; the wait is a
                // compiler memory barrier for subsequently inspected CQ state.
                unsafe { core::arch::asm!(
                    concat!("svc #", stringify!($number)),
                    lateout("x0") ret,
                    inlateout("x1") arg1 => secondary,
                    in("x2") arg2,
                    in("x3") arg3,
                    options(nostack, preserves_flags),
                ); }
            };
        }
        match imm {
            SYSCALL_SPAWN_THREAD => pair!(7),
            SYSCALL_MONOTONIC_CLOCK => pair!(79),
            SYSCALL_CQ_WAIT_CHECKED => pair!(88),
            _ => unreachable!(),
        }
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: the private callers supply the fixed clock/CQ ABI and retain
    // their mapped ring and pending operation owners through the wait.
    unsafe {
        core::arch::asm!(
            "syscall",
            inlateout("rax") u64::from(imm) => ret,
            inlateout("rdi") arg1 => secondary,
            in("rsi") arg2,
            in("rdx") arg3,
            lateout("rcx") _,
            lateout("r11") _,
            options(preserves_flags),
        );
    }
    (ret, secondary)
}

fn abort_wait(reason: CqWaitError) -> ! {
    let message: &[u8] = match reason {
        CqWaitError::AdmissionRejected => b"[sitas] CQ waiter admission rejected\n",
        CqWaitError::InvalidRequest => b"[sitas] invalid CQ wait request\n",
        CqWaitError::InvalidStatus => b"[sitas] invalid CQ wait outcome/clock\n",
    };
    abort_runtime(message)
}

fn abort_runtime(message: &[u8]) -> ! {
    // SAFETY: the static diagnostic stays borrowed through the synchronous
    // logging ABI. Domain abort is terminal: do not unwind pending operation
    // owners or turn rejection into a wake that the executor would retry.
    unsafe {
        syscall(
            SYSCALL_LOG_STR,
            message.as_ptr() as u64,
            message.len() as u64,
            0,
        );
        syscall(SYSCALL_DOMAIN_ABORT, 0, 0, 0);
        #[cfg(target_arch = "aarch64")]
        core::arch::asm!("udf #0", options(noreturn));
        #[cfg(target_arch = "x86_64")]
        core::arch::asm!("ud2", options(noreturn));
    }
}

fn wait_for_cq(timeout: Option<u64>, cq: u32) -> CqWaitOutcome {
    // SAFETY: this wait borrows the launch-owned CQ and returns no resources.
    // The explicit flag distinguishes every finite timeout from event-only.
    let (_, status) = unsafe {
        syscall_pair(
            SYSCALL_CQ_WAIT_CHECKED,
            timeout.unwrap_or(0),
            u64::from(cq),
            u64::from(timeout.is_some()),
        )
    };
    decode_cq_wait(status, timeout.is_some()).unwrap_or_else(|error| abort_wait(error))
}

struct WaitBudget {
    start: u64,
    frequency: u64,
    millis: u64,
}
impl WaitBudget {
    fn new(millis: u64) -> Self {
        // SAFETY: this fixed ABI returns monotonic metadata, not a resource.
        let (start, frequency) = unsafe { syscall_pair(SYSCALL_MONOTONIC_CLOCK, 0, 0, 0) };
        if frequency == 0 {
            abort_wait(CqWaitError::InvalidStatus);
        }
        Self {
            start,
            frequency,
            millis,
        }
    }
    fn remaining_at(&self, now: u64) -> Option<u64> {
        if self.frequency == 0 {
            return None;
        }
        let elapsed = u128::from(now.checked_sub(self.start)?) * 1000;
        let total = u128::from(self.millis) * u128::from(self.frequency);
        let left = total.saturating_sub(elapsed);
        left.div_ceil(u128::from(self.frequency)).try_into().ok()
    }
    fn remaining(&self) -> u64 {
        // SAFETY: this fixed ABI returns monotonic metadata, not a resource.
        let (now, frequency) = unsafe { syscall_pair(SYSCALL_MONOTONIC_CLOCK, 0, 0, 0) };
        if frequency != self.frequency {
            abort_wait(CqWaitError::InvalidStatus);
        }
        self.remaining_at(now)
            .unwrap_or_else(|| abort_wait(CqWaitError::InvalidStatus))
    }
}

extern "C" fn shard_entry_trampoline() {
    let tid = unsafe { sys_get_tid() };
    // The spawner stages this thread's entry right after SPAWN_THREAD
    // returns, so the thread can be scheduled and run before the entry
    // exists. Spin until it appears: the push is unconditional and
    // immediate, and falling through would re-enter the trampoline forever.
    let entry = loop {
        let mut entries = SHARD_ENTRIES.lock();
        match entries.iter().position(|(spawn_tid, _)| *spawn_tid == tid) {
            Some(index) => break entries.remove(index).1,
            None => drop(entries),
        }
    };
    entry();
    // The shard closure has run to completion. A user thread has no
    // "returned" trap: if this function simply returned, the thread would
    // re-enter the trampoline forever. Terminate explicitly so the kernel
    // reaps the thread and fires any exit observer registered against it
    // (this is what makes `CharlotteJoinHandle::join` complete). Never
    // returns.
    unsafe { sys_thread_exit() };
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
        #[cfg(not(target_os = "none"))]
        {
            let _ = lp_id;
            panic!("CharlotteReactor requires the CharlotteOS launch mappings");
        }
        #[cfg(target_os = "none")]
        {
            Self {
                lp_id,
                ring_vaddr: CQ_RING_VADDR,
                cq_id: 0,
            }
        }
    }

    /// Per-shard constructor: the reactor blocks on queue `cq_id` and drains
    /// the ring page at `ring_vaddr`. `lp_id` records the intended core
    /// affinity.
    /// # Safety
    /// `ring_vaddr` must identify a writable, aligned 4 KiB CQ page mapped by
    /// CharlotteOS for `cq_id`. The mapping must outlive the reactor and every
    /// borrowed header. The kernel owns head/entries; userspace owns tail.
    pub unsafe fn with_cq(lp_id: u32, ring_vaddr: usize, cq_id: u32) -> Self {
        Self {
            lp_id,
            ring_vaddr,
            cq_id,
        }
    }

    /// Returns the underlying CQ ring header at the shard's ring address.
    pub fn cq(&self) -> &CqHeader {
        // SAFETY: `new` uses the canonical Charlotte launch mapping;
        // `with_cq` requires the same mapping lifetime from its caller.
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
    head: AtomicU32,
    tail: AtomicU32,
    capacity: AtomicU32,
    overflow: AtomicU32,
}

#[repr(C)]
pub struct CqEntry {
    pub operation: u64,
    pub cookie: u64,
    pub status: u32,
    /// Bit 0: reserved for `returned_capability_present` (§8.2). When set,
    /// the following 8 bytes hold a `returned_capability: u64`.
    pub flags: u32,
    pub result: i64,
}

impl CqHeader {
    pub fn pending(&self) -> u32 {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Relaxed);
        let capacity = self.capacity.load(Ordering::Relaxed);
        if !(2..=127).contains(&capacity) || h >= capacity || t >= capacity {
            return 0;
        }
        if h >= t { h - t } else { h + capacity - t }
    }

    pub fn read_one(&self) -> Option<CqEntry> {
        let _consumer = CQ_CONSUMER.lock();
        if self.pending() == 0 {
            return None;
        }
        let t = self.tail.load(Ordering::Relaxed);
        let capacity = self.capacity.load(Ordering::Relaxed);
        // SAFETY: constructors establish a full CQ page, and pending checked
        // the slot index/capacity. The acquire head load observes publication
        // of this entry; the consumer gate excludes competing tail updates.
        let entry_ptr = unsafe {
            let base = self as *const Self as *const u8;
            let entries_offset = core::mem::offset_of!(Self, overflow) + 4;
            base.add(entries_offset)
                .add(t as usize * core::mem::size_of::<CqEntry>()) as *const CqEntry
        };
        // SAFETY: this slot is initialized by the kernel before publishing
        // head and cannot be reused until our release store advances tail.
        unsafe {
            let entry = core::ptr::read_volatile(entry_ptr);
            self.tail.store((t + 1) % capacity, Ordering::Release);
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
    /// The completion queue id used by the checked CQ wait; a wake targets only
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
        let budget = timeout.map(|duration| {
            WaitBudget::new(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        });
        loop {
            // Drain whatever the kernel has already posted — no syscall needed.
            let pending = self.cq().pending();
            if pending > 0 {
                let mut caps = Vec::new();
                for _ in 0..pending {
                    if let Some(entry) = self.cq().read_one() {
                        caps.push(entry.cookie);
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
            let outcome = wait_for_cq(budget.as_ref().map(WaitBudget::remaining), self.cq_id);
            if self.cq().pending() > 0 {
                continue; // drain above, retaining this same deadline
            }
            return Ok(CharlEvent {
                woke: outcome == CqWaitOutcome::Ready,
                readable: Vec::new(),
            });
        }
    }
}

// ---- ShardRuntime implementation -------------------------------------------

impl ShardRuntime for CharlotteReactor {
    type JoinHandle<T: Send + 'static> = CharlotteJoinHandle<T>;
    type Reactor = CharlotteReactor;

    fn spawn_shard<T: Send + 'static>(
        &self,
        shard_id: ShardId,
        _placement: sitas_core::placement::ShardPlacement,
        entry: alloc::boxed::Box<dyn FnOnce() -> T + Send>,
    ) -> CharlotteJoinHandle<T> {
        // The result slot is filled by the shard thread before it terminates;
        // the exit observer fires only after the closure completed, so `join`
        // can read the value without racing the thread.
        let result = alloc::sync::Arc::new(spin::Mutex::new(None::<T>));
        let result_for_thread = alloc::sync::Arc::clone(&result);
        let entry_vaddr = shard_entry_trampoline as *const () as usize;
        // SAFETY: the trampoline is executable for this domain; placement
        // failures are rejected by the kernel before publishing a thread.
        let (tid, generation) = unsafe { sys_spawn(entry_vaddr as u64, shard_id.0 as u64) };
        if tid == u64::MAX || generation == 0 {
            abort_runtime(b"[sitas] shard spawn rejected\n");
        }
        SHARD_ENTRIES.lock().push((
            tid,
            Box::new(move || {
                let value = entry();
                *result_for_thread.lock() = Some(value);
            }),
        ));
        // Observe the spawned thread's exit; the returned capability
        // completes when the kernel reaps the thread.
        // SAFETY: these are the exact spawn publication identity, not a later
        // lookup of a recyclable numeric tid.
        let cap = unsafe { sys_observe_exit(tid, generation) };
        if cap == u64::MAX {
            abort_runtime(b"[sitas] shard exit observation rejected\n");
        }
        CharlotteJoinHandle {
            cap: Some(cap),
            result,
        }
    }

    fn channel<M: Send + 'static>(
        &self,
        capacity: usize,
    ) -> sitas_core::shard_runtime::ShardChannelResult<M> {
        sitas_core::shard_runtime::channel(capacity)
    }

    fn sleep(&self, duration: Duration) {
        let budget = WaitBudget::new(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
        loop {
            let remaining = budget.remaining();
            if remaining == 0 {
                return;
            }
            // CQ wakes are hints, not proof that sleep's full deadline elapsed.
            wait_for_cq(Some(remaining), self.cq_id);
        }
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
            // SAFETY: the trusted launch config supplies this mapped CQ page
            // for the domain lifetime and its matching kernel queue id.
            unsafe {
                CharlotteReactor::with_cq(shard as u32, base + shard * 4096, (shard as u32) + 1)
            }
        } else {
            // Fallback for shard counts beyond the pre-allocated rings, or when
            // the loader did not map per-shard CQ pages (legacy single-ring path).
            CharlotteReactor::new(shard as u32)
        }
    }
}

// ---- join handles -----------------------------------------------------------

/// A kernel-backed join handle for a CharlotteOS shard thread.
///
/// `join` blocks on a completion capability that fires when the kernel reaps
/// the thread (the exit observer registered by `SPAWN_THREAD` + a dedicated
/// syscall); the shard closure's return value is delivered through the shared
/// result slot, which the thread fills before terminating. This replaces the
/// always-error `RawJoinHandle` placeholder: joining is real, and the waiting
/// side parks in the kernel's completion wait rather than spinning.
pub struct CharlotteJoinHandle<T> {
    cap: Option<u64>,
    result: alloc::sync::Arc<spin::Mutex<Option<T>>>,
}

impl<T> CharlotteJoinHandle<T> {
    /// Returns `true` once the shard thread has exited and been reaped.
    pub fn is_finished(&self) -> bool {
        // SAFETY: this handle exclusively owns its exit-observation cap.
        let (state, _) = unsafe { sys_poll(self.cap.expect("live join capability")) };
        state == 0
    }

    /// Blocks until the shard thread exits and returns its closure's value.
    pub fn join(mut self) -> core::result::Result<T, Box<dyn core::error::Error + Send + Sync>> {
        let cap = self.cap.take().expect("live join capability");
        // SAFETY: retain the exit capability through its terminal wait, then
        // release it exactly once. Only that wait establishes thread exit.
        unsafe {
            sys_wait(cap);
            sys_close(cap);
        }
        self.result.lock().take().ok_or_else(|| {
            Box::new(sitas_core::io::ErrorKind::Other) as Box<dyn core::error::Error + Send + Sync>
        })
    }
}

impl<T: Send> ShardThread<T> for CharlotteJoinHandle<T> {
    fn join(self) -> core::result::Result<T, Box<dyn core::error::Error + Send + Sync>> {
        CharlotteJoinHandle::join(self)
    }
    fn is_finished(&self) -> bool {
        CharlotteJoinHandle::is_finished(self)
    }
}

impl<T> Drop for CharlotteJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(cap) = self.cap.take() {
            // SAFETY: cancel only this handle's exit subscription, not the
            // thread. Its producer terminates the observation on cancellation.
            // Retain it through terminal wait before closing the capability;
            // cancellation is not evidence that the thread itself has exited.
            unsafe {
                syscall(SYSCALL_COMPLETION_CANCEL, cap, 0, 0);
                sys_wait(cap);
                sys_close(cap);
            }
        }
    }
}

/// Parks and wakes shards through the kernel completion queue.
///
/// A shard waiting for an inter-shard message or reply uses checked CQ wait 88
/// (bounded by a short deadline for potentially shared parkers); a peer releases it with
/// `CQ_WAKE`. This replaces the KV service's former `spin_loop` busy-waits, so
/// a blocked shard consumes no CPU.
///
/// The bounded backstop remains for public parkers sharing one CQ: the kernel
/// consumes a shared work-generation cursor, so a competing reader can consume
/// that generation. Private per-shard reactors use their own rings and can wait
/// without a timer. Removing this backstop needs a per-waiter wake contract.
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
            Some(duration) => u64::try_from(duration.as_millis())
                .unwrap_or(u64::MAX)
                .clamp(1, PARK_INTERVAL_MS),
            None => PARK_INTERVAL_MS,
        };
        // Readiness includes completions and peer wakes. The old min_complete
        // parameter was ignored by the kernel; it never filtered completions.
        wait_for_cq(Some(ms), self.cq_id);
    }

    fn unpark(&self) {
        let _ = unsafe { sys_cq_wake(self.cq_id) };
    }
}

#[cfg(test)]
mod checked_wait_tests {
    use super::*;
    #[test]
    fn rejection_is_neither_timeout_nor_peer_wake() {
        for timed in [false, true] {
            assert_eq!(
                decode_cq_wait(3, timed),
                Err(CqWaitError::AdmissionRejected)
            );
            assert_eq!(decode_cq_wait(0, timed), Ok(CqWaitOutcome::Ready));
        }
        assert_eq!(decode_cq_wait(1, true), Ok(CqWaitOutcome::Deadline));
        assert_eq!(decode_cq_wait(1, false), Err(CqWaitError::InvalidStatus));
        assert_eq!(decode_cq_wait(2, true), Err(CqWaitError::InvalidRequest));
    }
    #[test]
    fn wakes_cannot_restart_sleep_or_reactor_deadline() {
        let budget = WaitBudget {
            start: 100,
            frequency: 1000,
            millis: 10,
        };
        assert_eq!(budget.remaining_at(100), Some(10));
        assert_eq!(budget.remaining_at(105), Some(5));
        assert_eq!(budget.remaining_at(110), Some(0));
        assert_eq!(budget.remaining_at(99), None);
        let wide = WaitBudget {
            start: 0,
            frequency: u64::MAX,
            millis: u64::MAX,
        };
        assert_eq!(wide.remaining_at(0), Some(u64::MAX));
    }
}

#[cfg(test)]
mod cq_tests {
    use super::*;
    use alloc::sync::Arc;

    fn ring() -> (Vec<u64>, CharlotteReactor) {
        let mut page = alloc::vec![0u64; 512];
        let address = page.as_mut_ptr() as usize;
        // SAFETY: the allocation is aligned and contains a full writable page.
        // It stays alive in the caller while the reactor/header is used.
        unsafe {
            core::ptr::write(
                address as *mut CqHeader,
                CqHeader {
                    head: AtomicU32::new(0),
                    tail: AtomicU32::new(0),
                    capacity: AtomicU32::new(127),
                    overflow: AtomicU32::new(0),
                },
            );
        }
        // SAFETY: this fixture supplies the page normally mapped by the loader.
        let reactor = unsafe { CharlotteReactor::with_cq(0, address, 0) };
        (page, reactor)
    }

    #[test]
    fn cq_layout_matches_kernel_and_release_publishes_entries() {
        assert_eq!(core::mem::size_of::<CqHeader>(), 16);
        assert_eq!(core::mem::size_of::<CqEntry>(), 32);
        let (_page, reactor) = ring();
        let header = reactor.cq();
        let entries = (header as *const CqHeader)
            .cast::<u8>()
            .wrapping_add(16)
            .cast_mut()
            .cast::<CqEntry>();
        // SAFETY: slot 0 is inside this owned fixture's page, and no consumer
        // can read it before the release publication below.
        unsafe {
            entries.write(CqEntry {
                operation: 1,
                cookie: 42,
                status: 0,
                flags: 0,
                result: 7,
            });
        }
        header.head.store(1, Ordering::Release);
        assert_eq!(header.pending(), 1);
        let entry = header.read_one().unwrap();
        assert_eq!((entry.cookie, entry.result), (42, 7));
        assert_eq!(header.tail.load(Ordering::Acquire), 1);
        assert!(header.read_one().is_none());
        header.capacity.store(0, Ordering::Relaxed);
        assert!(header.read_one().is_none());
    }

    #[test]
    fn shared_reactors_consume_each_slot_once() {
        let (_page, reactor) = ring();
        let header = reactor.cq();
        let entries = (header as *const CqHeader)
            .cast::<u8>()
            .wrapping_add(16)
            .cast_mut()
            .cast::<CqEntry>();
        for cookie in 0..80 {
            // SAFETY: these initialized entries fit inside the owned CQ page;
            // publication and all consumers follow the initialization.
            unsafe {
                entries.add(cookie).write(CqEntry {
                    operation: 0,
                    cookie: cookie as u64,
                    status: 0,
                    flags: 0,
                    result: 0,
                });
            }
        }
        header.head.store(80, Ordering::Release);
        let reactor = Arc::new(reactor);
        let mut threads = Vec::new();
        for _ in 0..4 {
            let reactor = Arc::clone(&reactor);
            threads.push(std::thread::spawn(move || {
                let mut cookies = Vec::new();
                while let Some(entry) = reactor.cq().read_one() {
                    cookies.push(entry.cookie);
                }
                cookies
            }));
        }
        let mut cookies: Vec<_> = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect();
        cookies.sort_unstable();
        assert_eq!(cookies, (0..80).collect::<Vec<_>>());
    }
}
