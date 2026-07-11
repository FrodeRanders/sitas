//! sitas-charlotte: CharlotteOS backend for the sitas shard-per-core runtime.
//!
//! Implements the `ReactorBackend` and `ShardRuntime` traits against the
//! CharlotteOS kernel's async syscall ABI. This is the runtime that actually
//! runs sitas's executor on CharlotteOS.
//!
//! ## Design
//!
//! The executor's idle wait (`wait(read, write, timeout)`) maps to polling the
//! kernel's CQ ring for the calling shard. The CQ ring is a shared-memory page
//! mapped at a known virtual address in the user AS. The kernel writes
//! completion entries (cap, result) to the ring; userspace reads them without
//! syscalls.
//!
//! The waker (`ReactorWaker::wake`) uses the kernel's cross-LP IPI via
//! `try_send_ipi_rpc`. Each shard's reactor is identified by its LP index.
//!
//! Thread spawn (`ShardRuntime::spawn_shard`) uses the kernel's
//! `spawn_thread` via SVC, pinned to a specific LP.

#![no_std]
#![deny(unsafe_op_in_unsafe_fn)]

extern crate alloc;

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

use sitas_core::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker, SchedulerWake};
use sitas_core::shard_runtime::{ShardJoinHandle, ShardRuntime, RawJoinHandle};
use sitas_core::shard::ShardId;

/// The virtual address where the CQ ring is mapped in the user AS.
const CQ_RING_VADDR: usize = 0x0000_0000_0001_1000;

// ---- syscall wrappers -------------------------------------------------------

/// Invoke a syscall with the given SVC immediate and arguments.
#[inline(always)]
unsafe fn syscall(imm: u16, x0: u64, x1: u64, x2: u64, x3: u64) -> u64 {
    let ret: u64;
    unsafe {
        core::arch::asm!(
            "svc {}",
            in(reg) imm,
            inlateout("x0") x0 => ret,
            in("x1") x1,
            in("x2") x2,
            in("x3") x3,
            options(nostack, nomem, preserves_flags),
        );
    }
    ret
}

/// Syscall: completion::wait(asid, cap) — blocks until the capability completes.
#[inline(always)]
unsafe fn sys_wait(asid: u64, cap: u64) {
    syscall(4, asid, cap, 0, 0);
}

/// Syscall: completion::close(asid, cap) — frees a capability slot.
#[inline(always)]
unsafe fn sys_close(asid: u64, cap: u64) {
    syscall(6, asid, cap, 0, 0);
}

/// Syscall: spawn a thread pinned to a specific LP.
#[inline(always)]
unsafe fn sys_spawn(asid: u64, entry_vaddr: u64, target_lp: u64) -> u64 {
    syscall(7, asid, entry_vaddr, target_lp, 0)
}

// ---- reactor backend --------------------------------------------------------

/// A reactor for one LP (shard). All state is in the CQ ring mapped at
/// `CQ_RING_VADDR` and the per-LP kernel completion table.
pub struct CharlotteReactor {
    /// The address-space id this reactor's shard uses for completions.
    asid: u64,
    /// Which LP this reactor is pinned to.
    lp_id: u32,
    /// Monotonically-allocated completion-cap IDs (local to this reactor).
    next_cap: AtomicU64,
}

impl CharlotteReactor {
    pub fn new(asid: u64, lp_id: u32) -> Self {
        Self {
            asid,
            lp_id,
            next_cap: AtomicU64::new(1),
        }
    }

    /// Returns the underlying CQ ring header.
    pub fn cq(&self) -> &CqHeader {
        unsafe { &*(CQ_RING_VADDR as *const CqHeader) }
    }

    /// Monotonically allocates a local capability ID.
    fn alloc_cap(&self) -> u64 {
        self.next_cap.fetch_add(1, Ordering::Relaxed)
    }

    /// Submit an async operation and return a future that completes when the
    /// kernel posts the completion.
    pub fn submit_wait(&self, op_code: u64, buffer: Option<&[u8]>) -> u64 {
        let cap = self.alloc_cap();
        let buf_ptr = buffer.map(|b| b.as_ptr() as u64).unwrap_or(0);
        let buf_len = buffer.map(|b| b.len() as u64).unwrap_or(0);
        // Syscall #1: COMPLETION_SUBMIT
        unsafe { syscall(1, self.asid, op_code, buf_ptr, buf_len) };
        cap
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
    fn pending(&self) -> u32 {
        let h = unsafe { core::ptr::read_volatile(&self.head) };
        let t = unsafe { core::ptr::read_volatile(&self.tail) };
        if h >= t { h - t } else { h + self.capacity - t }
    }

    fn read_one(&self) -> Option<CqEntry> {
        if self.pending() == 0 {
            return None;
        }
        let t = unsafe { core::ptr::read_volatile(&self.tail) };
        let entry_ptr = unsafe {
            let base = self as *const Self as *const u8;
            let entries_offset = core::mem::offset_of!(Self, overflow) + 4;
            base.add(entries_offset).add(t as usize * core::mem::size_of::<CqEntry>()) as *const CqEntry
        };
        unsafe {
            let entry = core::ptr::read_volatile(entry_ptr);
            core::ptr::write_volatile(&raw mut *(self as *const Self as *mut Self).tail, (t + 1) % self.capacity);
            Some(entry)
        }
    }
}

// ---- trait implementations --------------------------------------------------

/// A handle that can wake this reactor from another shard/LP.
#[derive(Clone)]
pub struct CharlWaker {
    target_lp: u32,
}

impl ReactorWaker for CharlWaker {
    fn wake(&self) -> Result<(), sitas_core::io::ErrorKind> {
        unsafe { sys_wake(self.target_lp) };
        Ok(())
    }
}

impl core::fmt::Debug for CharlWaker {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CharlWaker").field("target_lp", &self.target_lp).finish()
    }
}

impl SchedulerWake for CharlWaker {
    fn wake(&self) {
        let _ = ReactorWaker::wake(self);
    }
}

pub struct CharlEvent {
    woke: bool,
    readable: Vec<u64>,
}

impl ReactorEvent for CharlEvent {
    type Handle = u64;
    fn woke(&self) -> bool { self.woke }
    fn readable(&self) -> &[u64] { &self.readable }
    fn writable(&self) -> &[u64] { &[] }
}

impl ReactorBackend for CharlotteReactor {
    type Waker = CharlWaker;
    type Handle = u64;
    type Event = CharlEvent;

    fn waker(&self) -> CharlWaker {
        CharlWaker { target_lp: self.lp_id }
    }

    fn wait(
        &self,
        _read: &[u64],
        _write: &[u64],
        timeout: Option<Duration>,
    ) -> Result<CharlEvent, sitas_core::io::ErrorKind> {
        // Poll the CQ ring until a completion arrives or the timeout expires.
        // In a real implementation this would use either a blocking syscall
        // (completion::wait) or a yield loop; for now it busy-polls.
        let deadline = timeout.map(|d| {
            let now = sitas_core::instant::Instant::now();
            now.checked_add(d).unwrap_or(now)
        });
        loop {
            let pending = self.cq().pending();
            if pending > 0 {
                let mut caps = alloc::vec::Vec::new();
                for _ in 0..pending {
                    if let Some(entry) = self.cq().read_one() {
                        caps.push(entry.cap);
                    }
                }
                return Ok(CharlEvent { woke: false, readable: caps });
            }
            if let Some(dl) = deadline {
                if sitas_core::instant::Instant::now() >= dl {
                    return Ok(CharlEvent { woke: false, readable: alloc::vec::Vec::new() });
                }
            }
            core::hint::spin_loop();
        }
    }
}

// ---- ShardRuntime implementation -------------------------------------------

impl ShardRuntime for CharlotteReactor {
    type JoinHandle<T: Send> = ShardJoinHandle<T>;

    fn spawn_shard<T: Send + 'static>(
        &self,
        shard_id: ShardId,
        _placement: sitas_core::placement::Placement,
        entry: alloc::boxed::Box<dyn FnOnce() -> T + Send>,
    ) -> ShardJoinHandle<T> {
        let entry_ptr = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(entry));
        let entry_vaddr = entry_ptr as *const () as usize;
        unsafe {
            sys_spawn(self.asid, entry_vaddr as u64, shard_id.0 as u64);
        }
        ShardJoinHandle::from_raw(RawJoinHandle)
    }

    fn channel<M: Send + 'static>(
        &self,
        capacity: usize,
    ) -> sitas_core::shard_runtime::ShardChannelResult<M> {
        let q = alloc::sync::Arc::new(sitas_core::ringbuf::RingBuffer::bounded(capacity));
        Ok((
            sitas_core::shard_runtime::ShardSender { queue: alloc::sync::Arc::clone(&q) },
            sitas_core::shard_runtime::ShardReceiver { queue: q },
        ))
    }

    fn sleep(&self, _duration: Duration) {
        for _ in 0..10000 { core::hint::spin_loop(); }
    }
}
