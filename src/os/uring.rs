use std::fmt;
use std::io;
use std::os::raw::{c_int, c_long, c_uint, c_void};
use std::os::unix::io::RawFd;
use std::ptr;

use super::{OwnedFd, last_os_error};

const SYS_IO_URING_SETUP: c_long = 425;
const SYS_IO_URING_ENTER: c_long = 426;

const IORING_OFF_SQ_RING: i64 = 0;
const IORING_OFF_CQ_RING: i64 = 0x0800_0000;
const IORING_OFF_SQES: i64 = 0x1000_0000;
const IORING_ENTER_GETEVENTS: c_uint = 1;

const IORING_OP_NOP: u8 = 0;
const SQE_SIZE: usize = 64;
const SQE_USER_DATA_OFFSET: usize = 32;

const PROT_READ: c_int = 0x1;
const PROT_WRITE: c_int = 0x2;
const MAP_SHARED: c_int = 0x01;
const MAP_FAILED: *mut c_void = !0usize as *mut c_void;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoSqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoCqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: IoSqringOffsets,
    cq_off: IoCqringOffsets,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoUringCqe {
    user_data: u64,
    res: i32,
    flags: u32,
}

unsafe extern "C" {
    fn mmap(
        addr: *mut c_void,
        length: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: i64,
    ) -> *mut c_void;
    fn munmap(addr: *mut c_void, length: usize) -> c_int;
    fn syscall(number: c_long, ...) -> c_long;
}

/// Minimal Linux `io_uring` completion backend experiment.
///
/// This type currently exposes only enough functionality to validate ring
/// setup, submission, and completion delivery. It intentionally does not yet
/// integrate with [`super::OsReactor`] or own real I/O buffers.
pub struct IoUring {
    fd: OwnedFd,
    _sq_ring: Mapping,
    _cq_ring: Mapping,
    sqes: Mapping,
    sq_head: *mut u32,
    sq_tail: *mut u32,
    sq_ring_mask: *mut u32,
    sq_array: *mut u32,
    cq_head: *mut u32,
    cq_tail: *mut u32,
    cq_ring_mask: *mut u32,
    cqes: *mut IoUringCqe,
}

impl IoUring {
    /// Creates an `io_uring` instance with at least `entries` submission slots.
    pub fn new(entries: u32) -> io::Result<Self> {
        let mut params = IoUringParams {
            sq_entries: 0,
            cq_entries: 0,
            flags: 0,
            sq_thread_cpu: 0,
            sq_thread_idle: 0,
            features: 0,
            wq_fd: 0,
            resv: [0; 3],
            sq_off: IoSqringOffsets {
                head: 0,
                tail: 0,
                ring_mask: 0,
                ring_entries: 0,
                flags: 0,
                dropped: 0,
                array: 0,
                resv1: 0,
                user_addr: 0,
            },
            cq_off: IoCqringOffsets {
                head: 0,
                tail: 0,
                ring_mask: 0,
                ring_entries: 0,
                overflow: 0,
                cqes: 0,
                flags: 0,
                resv1: 0,
                user_addr: 0,
            },
        };

        // SAFETY: `params` points to writable memory for the kernel to fill,
        // and the syscall does not retain the pointer after returning.
        let fd = unsafe { syscall(SYS_IO_URING_SETUP, entries, &mut params) };
        if fd < 0 {
            return Err(last_os_error());
        }
        let fd = OwnedFd::new(fd as RawFd);

        let sq_ring_len = params.sq_off.array as usize + params.sq_entries as usize * 4;
        let cq_ring_len =
            params.cq_off.cqes as usize + params.cq_entries as usize * size_of::<IoUringCqe>();
        let sqes_len = params.sq_entries as usize * SQE_SIZE;

        let sq_ring = Mapping::new(fd.raw(), sq_ring_len, IORING_OFF_SQ_RING)?;
        let cq_ring = Mapping::new(fd.raw(), cq_ring_len, IORING_OFF_CQ_RING)?;
        let sqes = Mapping::new(fd.raw(), sqes_len, IORING_OFF_SQES)?;

        let sq_base = sq_ring.as_u8_ptr();
        let cq_base = cq_ring.as_u8_ptr();

        Ok(Self {
            fd,
            sq_head: offset_ptr(sq_base, params.sq_off.head),
            sq_tail: offset_ptr(sq_base, params.sq_off.tail),
            sq_ring_mask: offset_ptr(sq_base, params.sq_off.ring_mask),
            sq_array: offset_ptr(sq_base, params.sq_off.array),
            cq_head: offset_ptr(cq_base, params.cq_off.head),
            cq_tail: offset_ptr(cq_base, params.cq_off.tail),
            cq_ring_mask: offset_ptr(cq_base, params.cq_off.ring_mask),
            cqes: offset_ptr(cq_base, params.cq_off.cqes),
            _sq_ring: sq_ring,
            _cq_ring: cq_ring,
            sqes,
        })
    }

    /// Submits a no-op operation tagged with `user_data`.
    pub fn submit_nop(&mut self, user_data: u64) -> io::Result<()> {
        let tail = read_u32(self.sq_tail);
        let head = read_u32(self.sq_head);
        let mask = read_u32(self.sq_ring_mask);
        if tail.wrapping_sub(head) > mask {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue is full",
            ));
        }

        let index = tail & mask;
        let sqe = unsafe { self.sqes.as_u8_ptr().add(index as usize * SQE_SIZE) };
        clear_sqe(sqe);
        write_u8(sqe, IORING_OP_NOP);
        write_u64(unsafe { sqe.add(SQE_USER_DATA_OFFSET) }, user_data);
        write_u32(unsafe { self.sq_array.add(index as usize) }, index);
        write_u32(self.sq_tail, tail.wrapping_add(1));

        // SAFETY: the ring fd is owned by `self`; no signal mask is supplied.
        let result = unsafe {
            syscall(
                SYS_IO_URING_ENTER,
                self.fd.raw(),
                1u32,
                0u32,
                0u32,
                ptr::null::<c_void>(),
                0usize,
            )
        };
        if result < 0 {
            Err(last_os_error())
        } else {
            Ok(())
        }
    }

    /// Waits for one completion queue entry.
    pub fn wait_completion(&mut self) -> io::Result<IoUringCompletion> {
        loop {
            if let Some(completion) = self.pop_completion() {
                return Ok(completion);
            }

            // SAFETY: the ring fd is owned by `self`; no signal mask is supplied.
            let result = unsafe {
                syscall(
                    SYS_IO_URING_ENTER,
                    self.fd.raw(),
                    0u32,
                    1u32,
                    IORING_ENTER_GETEVENTS,
                    ptr::null::<c_void>(),
                    0usize,
                )
            };
            if result < 0 {
                let error = last_os_error();
                if error.raw_os_error() == Some(super::EINTR) {
                    continue;
                }
                return Err(error);
            }
        }
    }

    fn pop_completion(&mut self) -> Option<IoUringCompletion> {
        let head = read_u32(self.cq_head);
        let tail = read_u32(self.cq_tail);
        if head == tail {
            return None;
        }

        let mask = read_u32(self.cq_ring_mask);
        let index = head & mask;
        let cqe = read_cqe(unsafe { self.cqes.add(index as usize) });
        write_u32(self.cq_head, head.wrapping_add(1));

        Some(IoUringCompletion {
            user_data: cqe.user_data,
            result: cqe.res,
            flags: cqe.flags,
        })
    }
}

impl fmt::Debug for IoUring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUring").finish_non_exhaustive()
    }
}

/// One `io_uring` completion queue entry.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoUringCompletion {
    /// Caller-provided operation tag.
    pub user_data: u64,
    /// Operation result.
    pub result: i32,
    /// Kernel completion flags.
    pub flags: u32,
}

#[derive(Debug)]
struct Mapping {
    ptr: *mut c_void,
    len: usize,
}

impl Mapping {
    fn new(fd: RawFd, len: usize, offset: i64) -> io::Result<Self> {
        // SAFETY: `fd` is an io_uring descriptor, and the returned mapping is
        // owned by `Mapping` until `Drop`.
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                offset,
            )
        };
        if ptr == MAP_FAILED {
            Err(last_os_error())
        } else {
            Ok(Self { ptr, len })
        }
    }

    fn as_u8_ptr(&self) -> *mut u8 {
        self.ptr.cast::<u8>()
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` describe the mapping owned by this value.
        let _ = unsafe { munmap(self.ptr, self.len) };
    }
}

fn offset_ptr<T>(base: *mut u8, offset: u32) -> *mut T {
    unsafe { base.add(offset as usize).cast::<T>() }
}

fn clear_sqe(sqe: *mut u8) {
    // SAFETY: `sqe` points to one 64-byte SQE slot in the mapped SQE array.
    unsafe { ptr::write_bytes(sqe, 0, SQE_SIZE) };
}

fn read_u32(ptr: *mut u32) -> u32 {
    // SAFETY: ring offset pointers point into live kernel-provided mappings.
    unsafe { ptr.read_volatile() }
}

fn write_u32(ptr: *mut u32, value: u32) {
    // SAFETY: ring offset pointers point into live kernel-provided mappings.
    unsafe { ptr.write_volatile(value) };
}

fn write_u8(ptr: *mut u8, value: u8) {
    // SAFETY: `ptr` points into a live SQE slot.
    unsafe { ptr.write_volatile(value) };
}

fn write_u64(ptr: *mut u8, value: u64) {
    // SAFETY: `ptr` points to the aligned `user_data` field inside an SQE.
    unsafe { ptr.cast::<u64>().write_volatile(value) };
}

fn read_cqe(ptr: *mut IoUringCqe) -> IoUringCqe {
    // SAFETY: `ptr` points to a CQE slot identified by the CQ head/mask.
    unsafe { ptr.read_volatile() }
}

#[cfg(test)]
mod tests {
    use super::IoUring;

    #[test]
    fn nop_completion_round_trip() {
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(1) | Some(22) | Some(38) | Some(95)
                ) =>
            {
                return;
            }
            Err(error) => panic!("failed to create io_uring: {error}"),
        };

        ring.submit_nop(0x51_7a_5).unwrap();
        let completion = ring.wait_completion().unwrap();

        assert_eq!(completion.user_data, 0x51_7a_5);
        assert_eq!(completion.result, 0);
    }
}
