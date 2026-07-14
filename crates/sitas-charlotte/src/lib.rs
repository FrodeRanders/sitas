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

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::time::Duration;

use sitas_core::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker};
use sitas_core::shard::ShardId;
use sitas_core::shard_runtime::{RawJoinHandle, ShardJoinHandle, ShardRuntime};
use spin::Mutex;

/// The virtual address where the CQ ring is mapped in the user AS.
const CQ_RING_VADDR: usize = 0x0000_0000_0001_1000;
const SYSCALL_COMPLETION_SUBMIT: u16 = 1;
const SYSCALL_SPAWN_THREAD: u16 = 7;
const SYSCALL_MAILBOX_SEND: u16 = 9;

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
        SYSCALL_MAILBOX_SEND => svc!(9),
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

#[inline(always)]
unsafe fn sys_wake(target_lp: u32) -> u64 {
    unsafe { syscall(SYSCALL_MAILBOX_SEND, u64::from(target_lp), 1, 0) }
}

extern "C" fn shard_entry_trampoline() {
    let entry = SHARD_ENTRIES.lock().pop();
    if let Some(entry) = entry {
        entry();
    }
}

// ---- reactor backend --------------------------------------------------------

/// A reactor for one LP (shard). All state is in the CQ ring mapped at
/// `CQ_RING_VADDR` and the per-LP kernel completion table.
pub struct CharlotteReactor {
    /// Which LP this reactor is pinned to.
    lp_id: u32,
}

impl CharlotteReactor {
    pub fn new(lp_id: u32) -> Self {
        Self { lp_id }
    }

    /// Returns the underlying CQ ring header.
    pub fn cq(&self) -> &CqHeader {
        unsafe { &*(CQ_RING_VADDR as *const CqHeader) }
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
    target_lp: u32,
}

impl ReactorWaker for CharlWaker {
    fn wake(&self) -> Result<(), sitas_core::io::ErrorKind> {
        let result = unsafe { sys_wake(self.target_lp) };
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
        }
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
                return Ok(CharlEvent {
                    woke: false,
                    readable: caps,
                });
            }
            if let Some(dl) = deadline
                && sitas_core::instant::Instant::now() >= dl
            {
                return Ok(CharlEvent {
                    woke: false,
                    readable: alloc::vec::Vec::new(),
                });
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

    fn sleep(&self, _duration: Duration) {
        for _ in 0..10000 {
            core::hint::spin_loop();
        }
    }
}
