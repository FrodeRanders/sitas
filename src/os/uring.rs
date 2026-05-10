use std::collections::{HashMap, VecDeque};
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
const IORING_OP_READ: u8 = 22;
const IORING_OP_WRITE: u8 = 23;
const SQE_SIZE: usize = 64;
const SQE_FD_OFFSET: usize = 4;
const SQE_OFF_OFFSET: usize = 8;
const SQE_ADDR_OFFSET: usize = 16;
const SQE_LEN_OFFSET: usize = 24;
const SQE_USER_DATA_OFFSET: usize = 32;
const OPERATION_USER_DATA_BASE: u64 = 1 << 63;

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
    completions: VecDeque<IoUringCompletion>,
    operations: HashMap<IoUringOperationId, IoUringOperationKind>,
    next_operation: u64,
    pending_submissions: u32,
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
            completions: VecDeque::new(),
            operations: HashMap::new(),
            next_operation: 0,
            pending_submissions: 0,
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
        self.queue_nop(user_data)?;
        self.submit_pending().map(|_| ())
    }

    /// Queues a no-op operation tagged with `user_data`.
    ///
    /// Queued operations are not visible to the kernel until
    /// [`IoUring::submit_pending`] is called.
    pub fn queue_nop(&mut self, user_data: u64) -> io::Result<()> {
        let sqe = self.prepare_sqe()?;
        write_u8(sqe, IORING_OP_NOP);
        write_u64(unsafe { sqe.add(SQE_USER_DATA_OFFSET) }, user_data);
        self.finish_sqe()
    }

    /// Queues a tracked no-op operation and returns its operation id.
    pub fn queue_nop_operation(&mut self) -> io::Result<IoUringOperationId> {
        let operation = self.allocate_operation(IoUringOperationKind::Nop)?;
        if let Err(error) = self.queue_nop(operation.raw()) {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Submits one read operation tagged with `user_data`.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and uniquely writable until the completion
    /// for `user_data` has been observed. Dropping or mutating the buffer
    /// before the completion arrives may let the kernel write through an
    /// invalid or aliased pointer.
    pub unsafe fn submit_read(
        &mut self,
        fd: RawFd,
        buffer: &mut [u8],
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        // SAFETY: the caller upholds the same buffer lifetime requirements for
        // queuing as for submitting.
        unsafe {
            self.queue_read(fd, buffer, offset, user_data)?;
        }
        self.submit_pending().map(|_| ())
    }

    /// Queues one read operation tagged with `user_data`.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and uniquely writable until the completion
    /// for `user_data` has been observed. Dropping or mutating the buffer
    /// before the completion arrives may let the kernel write through an
    /// invalid or aliased pointer. The operation must also eventually be
    /// submitted with [`IoUring::submit_pending`].
    pub unsafe fn queue_read(
        &mut self,
        fd: RawFd,
        buffer: &mut [u8],
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        self.queue_buffer_operation(
            IORING_OP_READ,
            fd,
            buffer.as_mut_ptr() as u64,
            buffer.len(),
            offset,
            user_data,
            "read",
        )
    }

    /// Queues a tracked read operation and returns its operation id.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and uniquely writable until the completion
    /// for the returned operation id has been observed. Dropping or mutating
    /// the buffer before completion may let the kernel write through an
    /// invalid or aliased pointer. The operation must also eventually be
    /// submitted with [`IoUring::submit_pending`].
    pub unsafe fn queue_read_operation(
        &mut self,
        fd: RawFd,
        buffer: &mut [u8],
        offset: u64,
    ) -> io::Result<IoUringOperationId> {
        let operation = self.allocate_operation(IoUringOperationKind::Read)?;
        // SAFETY: the caller keeps the read buffer valid until completion.
        if let Err(error) = unsafe { self.queue_read(fd, buffer, offset, operation.raw()) } {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Submits one write operation tagged with `user_data`.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and immutable until the completion for
    /// `user_data` has been observed. Dropping or mutating the buffer before
    /// the completion arrives may let the kernel read invalid or changing
    /// memory.
    pub unsafe fn submit_write(
        &mut self,
        fd: RawFd,
        buffer: &[u8],
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        // SAFETY: the caller upholds the same buffer lifetime requirements for
        // queuing as for submitting.
        unsafe {
            self.queue_write(fd, buffer, offset, user_data)?;
        }
        self.submit_pending().map(|_| ())
    }

    /// Queues one write operation tagged with `user_data`.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and immutable until the completion for
    /// `user_data` has been observed. Dropping or mutating the buffer before
    /// the completion arrives may let the kernel read invalid or changing
    /// memory. The operation must also eventually be submitted with
    /// [`IoUring::submit_pending`].
    pub unsafe fn queue_write(
        &mut self,
        fd: RawFd,
        buffer: &[u8],
        offset: u64,
        user_data: u64,
    ) -> io::Result<()> {
        self.queue_buffer_operation(
            IORING_OP_WRITE,
            fd,
            buffer.as_ptr() as u64,
            buffer.len(),
            offset,
            user_data,
            "write",
        )
    }

    /// Queues a tracked write operation and returns its operation id.
    ///
    /// # Safety
    ///
    /// `buffer` must remain valid and immutable until the completion for the
    /// returned operation id has been observed. Dropping or mutating the buffer
    /// before completion may let the kernel read invalid or changing memory.
    /// The operation must also eventually be submitted with
    /// [`IoUring::submit_pending`].
    pub unsafe fn queue_write_operation(
        &mut self,
        fd: RawFd,
        buffer: &[u8],
        offset: u64,
    ) -> io::Result<IoUringOperationId> {
        let operation = self.allocate_operation(IoUringOperationKind::Write)?;
        // SAFETY: the caller keeps the write buffer valid until completion.
        if let Err(error) = unsafe { self.queue_write(fd, buffer, offset, operation.raw()) } {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Reads once through `io_uring` and waits for the matching completion.
    ///
    /// This is a safe convenience wrapper around [`IoUring::submit_read`]
    /// because it does not return until the kernel has completed the operation.
    pub fn read_once(
        &mut self,
        fd: RawFd,
        buffer: &mut [u8],
        user_data: u64,
    ) -> io::Result<IoUringCompletion> {
        // SAFETY: this method waits for the completion before returning, so
        // the borrowed buffer remains live and uniquely borrowed for the whole
        // submitted operation.
        unsafe {
            self.submit_read(fd, buffer, u64::MAX, user_data)?;
        }

        self.wait_for_completion(user_data)
    }

    /// Writes once through `io_uring` and waits for the matching completion.
    ///
    /// This is a safe convenience wrapper around [`IoUring::submit_write`]
    /// because it does not return until the kernel has completed the operation.
    pub fn write_once(
        &mut self,
        fd: RawFd,
        buffer: &[u8],
        user_data: u64,
    ) -> io::Result<IoUringCompletion> {
        // SAFETY: this method waits for the completion before returning, so
        // the borrowed buffer remains live and immutable for the whole
        // submitted operation.
        unsafe {
            self.submit_write(fd, buffer, u64::MAX, user_data)?;
        }

        self.wait_for_completion(user_data)
    }

    /// Returns the number of SQEs queued but not yet submitted to the kernel.
    pub fn pending_submissions(&self) -> u32 {
        self.pending_submissions
    }

    /// Submits all currently queued SQEs to the kernel.
    pub fn submit_pending(&mut self) -> io::Result<u32> {
        let mut submitted = 0;
        while self.pending_submissions > 0 {
            let requested = self.pending_submissions;
            let result = self.enter(requested, 0, 0)?;
            if result == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "io_uring accepted no pending submissions",
                ));
            }

            let accepted = result.min(requested);
            self.pending_submissions -= accepted;
            submitted += accepted;
        }
        Ok(submitted)
    }

    /// Waits for the completion of a tracked operation.
    pub fn wait_operation_completion(
        &mut self,
        operation: IoUringOperationId,
    ) -> io::Result<IoUringOperationCompletion> {
        let completion = self.wait_for_completion(operation.raw())?;
        self.complete_operation(operation, completion)
    }

    /// Returns one completed tracked operation, if any, without blocking.
    ///
    /// Raw completions produced by untracked `queue_*` or `submit_*` calls are
    /// retained for [`IoUring::try_completion`] and [`IoUring::wait_completion`].
    pub fn try_operation_completion(&mut self) -> Option<IoUringOperationCompletion> {
        self.drain_completions();

        if let Some(index) = self.completions.iter().position(|completion| {
            self.operations
                .contains_key(&IoUringOperationId(completion.user_data))
        }) {
            let completion = self.completions.remove(index).expect("completion exists");
            return self
                .complete_operation(IoUringOperationId(completion.user_data), completion)
                .ok();
        }

        None
    }

    /// Drains all currently available kernel completions into the local queue.
    ///
    /// This does not block and does not submit pending SQEs. It is useful for
    /// executor loops that want to harvest a batch of completions after a wait
    /// has returned.
    pub fn drain_completions(&mut self) -> usize {
        let mut drained = 0;
        while let Some(completion) = self.pop_ring_completion() {
            self.completions.push_back(completion);
            drained += 1;
        }
        drained
    }

    fn queue_buffer_operation(
        &mut self,
        opcode: u8,
        fd: RawFd,
        buffer_addr: u64,
        buffer_len: usize,
        offset: u64,
        user_data: u64,
        operation_name: &str,
    ) -> io::Result<()> {
        let len = u32::try_from(buffer_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("io_uring {operation_name} buffer length exceeds u32::MAX"),
            )
        })?;

        let sqe = self.prepare_sqe()?;
        write_u8(sqe, opcode);
        write_i32(unsafe { sqe.add(SQE_FD_OFFSET) }, fd);
        write_u64(unsafe { sqe.add(SQE_OFF_OFFSET) }, offset);
        write_u64(unsafe { sqe.add(SQE_ADDR_OFFSET) }, buffer_addr);
        write_u32(unsafe { sqe.add(SQE_LEN_OFFSET).cast::<u32>() }, len);
        write_u64(unsafe { sqe.add(SQE_USER_DATA_OFFSET) }, user_data);
        self.finish_sqe()
    }

    fn prepare_sqe(&mut self) -> io::Result<*mut u8> {
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
        write_u32(unsafe { self.sq_array.add(index as usize) }, index);
        write_u32(self.sq_tail, tail.wrapping_add(1));
        Ok(sqe)
    }

    fn finish_sqe(&mut self) -> io::Result<()> {
        self.pending_submissions = self
            .pending_submissions
            .checked_add(1)
            .ok_or_else(|| io::Error::other("io_uring pending submission count overflow"))?;
        Ok(())
    }

    fn allocate_operation(&mut self, kind: IoUringOperationKind) -> io::Result<IoUringOperationId> {
        if self.next_operation == OPERATION_USER_DATA_BASE {
            return Err(io::Error::other("io_uring operation id space exhausted"));
        }

        let operation = IoUringOperationId(OPERATION_USER_DATA_BASE | self.next_operation);
        self.next_operation += 1;
        if self.operations.insert(operation, kind).is_some() {
            return Err(io::Error::other("io_uring operation id collision"));
        }
        Ok(operation)
    }

    fn complete_operation(
        &mut self,
        operation: IoUringOperationId,
        completion: IoUringCompletion,
    ) -> io::Result<IoUringOperationCompletion> {
        let kind = self.operations.remove(&operation).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "io_uring completion does not match a tracked operation",
            )
        })?;

        Ok(IoUringOperationCompletion {
            operation,
            kind,
            result: completion.result,
            flags: completion.flags,
        })
    }

    fn enter(&self, to_submit: u32, min_complete: u32, flags: c_uint) -> io::Result<u32> {
        // SAFETY: the ring fd is owned by `self`; no signal mask is supplied.
        let result = unsafe {
            syscall(
                SYS_IO_URING_ENTER,
                self.fd.raw(),
                to_submit,
                min_complete,
                flags,
                ptr::null::<c_void>(),
                0usize,
            )
        };
        if result < 0 {
            Err(last_os_error())
        } else {
            Ok(result as u32)
        }
    }

    /// Waits for one completion queue entry.
    pub fn wait_completion(&mut self) -> io::Result<IoUringCompletion> {
        if let Some(completion) = self.try_completion() {
            return Ok(completion);
        }
        self.wait_ring_completion()
    }

    /// Waits for the completion tagged with `user_data`.
    ///
    /// Completions for other operations are retained and will be returned by a
    /// later [`IoUring::try_completion`] or [`IoUring::wait_completion`] call.
    pub fn wait_for_completion(&mut self, user_data: u64) -> io::Result<IoUringCompletion> {
        if let Some(index) = self
            .completions
            .iter()
            .position(|completion| completion.user_data == user_data)
        {
            return Ok(self.completions.remove(index).expect("completion exists"));
        }

        loop {
            let completion = self.wait_ring_completion()?;
            if completion.user_data == user_data {
                return Ok(completion);
            }
            self.completions.push_back(completion);
        }
    }

    /// Returns one already available completion, if any, without blocking.
    pub fn try_completion(&mut self) -> Option<IoUringCompletion> {
        self.completions
            .pop_front()
            .or_else(|| self.pop_ring_completion())
    }

    fn wait_ring_completion(&mut self) -> io::Result<IoUringCompletion> {
        loop {
            if let Some(completion) = self.pop_ring_completion() {
                return Ok(completion);
            }

            let to_submit = self.pending_submissions;
            match self.enter(to_submit, 1, IORING_ENTER_GETEVENTS) {
                Ok(submitted) => {
                    self.pending_submissions = self.pending_submissions.saturating_sub(submitted);
                }
                Err(error) if error.raw_os_error() == Some(super::EINTR) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn pop_ring_completion(&mut self) -> Option<IoUringCompletion> {
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

/// Identifier for a tracked `io_uring` operation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IoUringOperationId(u64);

impl IoUringOperationId {
    /// Returns the raw `user_data` value placed in the SQE.
    pub fn raw(self) -> u64 {
        self.0
    }
}

/// Kind of tracked operation submitted through [`IoUring`].
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringOperationKind {
    /// No-op operation.
    Nop,
    /// Read operation.
    Read,
    /// Write operation.
    Write,
}

/// Completion for a tracked `io_uring` operation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoUringOperationCompletion {
    /// Operation id returned when the operation was queued.
    pub operation: IoUringOperationId,
    /// Kind of operation that completed.
    pub kind: IoUringOperationKind,
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

fn write_i32(ptr: *mut u8, value: i32) {
    // SAFETY: `ptr` points to an aligned i32 field inside an SQE.
    unsafe { ptr.cast::<i32>().write_volatile(value) };
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
    use super::{IoUring, IoUringOperationKind};
    use std::os::raw::c_void;
    use std::os::unix::io::RawFd;

    #[test]
    fn nop_completion_round_trip() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.submit_nop(0x51_7a_5).unwrap();
        let completion = ring.wait_completion().unwrap();

        assert_eq!(completion.user_data, 0x51_7a_5);
        assert_eq!(completion.result, 0);
    }

    #[test]
    fn queued_nops_submit_as_batch() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_7).unwrap();
        ring.queue_nop(0x51_7a_5_8).unwrap();

        assert_eq!(ring.pending_submissions(), 2);
        assert!(ring.try_completion().is_none());
        assert_eq!(ring.submit_pending().unwrap(), 2);
        assert_eq!(ring.pending_submissions(), 0);

        let first = ring.wait_completion().unwrap();
        let second = ring.wait_completion().unwrap();

        assert_eq!(first.result, 0);
        assert_eq!(second.result, 0);
        assert_eq!(
            [first.user_data, second.user_data],
            [0x51_7a_5_7, 0x51_7a_5_8]
        );
    }

    #[test]
    fn drain_completions_harvests_available_cqes() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_b).unwrap();
        ring.queue_nop(0x51_7a_5_c).unwrap();
        assert_eq!(ring.submit_pending().unwrap(), 2);

        let first = ring.wait_completion().unwrap();
        let drained = ring.drain_completions();
        let second = ring.wait_completion().unwrap();

        assert_eq!(first.user_data, 0x51_7a_5_b);
        assert_eq!(first.result, 0);
        assert_eq!(drained, 1);
        assert_eq!(second.user_data, 0x51_7a_5_c);
        assert_eq!(second.result, 0);
    }

    #[test]
    fn wait_completion_submits_queued_operations() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_9).unwrap();
        assert_eq!(ring.pending_submissions(), 1);

        let completion = ring.wait_completion().unwrap();

        assert_eq!(completion.user_data, 0x51_7a_5_9);
        assert_eq!(completion.result, 0);
        assert_eq!(ring.pending_submissions(), 0);
    }

    #[test]
    fn tracked_nop_completion_reports_operation_metadata() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        let operation = ring.queue_nop_operation().unwrap();
        assert!(operation.raw() & super::OPERATION_USER_DATA_BASE != 0);
        assert_eq!(ring.submit_pending().unwrap(), 1);

        let completion = ring.wait_operation_completion(operation).unwrap();

        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(completion.result, 0);
    }

    #[test]
    fn try_operation_completion_preserves_raw_completions() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_a).unwrap();
        let operation = ring.queue_nop_operation().unwrap();
        assert_eq!(ring.submit_pending().unwrap(), 2);

        let tracked = ring.wait_operation_completion(operation).unwrap();
        let raw = ring.wait_completion().unwrap();

        assert_eq!(tracked.operation, operation);
        assert_eq!(tracked.kind, IoUringOperationKind::Nop);
        assert_eq!(raw.user_data, 0x51_7a_5_a);
        assert_eq!(raw.result, 0);
    }

    #[test]
    fn try_operation_completion_scans_past_raw_cqes() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_d).unwrap();
        let operation = ring.queue_nop_operation().unwrap();
        assert_eq!(ring.submit_pending().unwrap(), 2);

        let raw = ring.wait_for_completion(0x51_7a_5_d).unwrap();
        let tracked = ring.try_operation_completion().unwrap();

        assert_eq!(raw.user_data, 0x51_7a_5_d);
        assert_eq!(raw.result, 0);
        assert_eq!(tracked.operation, operation);
        assert_eq!(tracked.kind, IoUringOperationKind::Nop);
        assert_eq!(tracked.result, 0);
    }

    #[test]
    fn wait_for_completion_preserves_other_completions() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.submit_nop(0x51_7a_5_4).unwrap();
        ring.submit_nop(0x51_7a_5_5).unwrap();

        let matched = ring.wait_for_completion(0x51_7a_5_5).unwrap();
        let preserved = ring.wait_completion().unwrap();

        assert_eq!(matched.user_data, 0x51_7a_5_5);
        assert_eq!(matched.result, 0);
        assert_eq!(preserved.user_data, 0x51_7a_5_4);
        assert_eq!(preserved.result, 0);
    }

    #[test]
    fn try_completion_returns_available_completion_without_blocking() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        assert!(ring.try_completion().is_none());

        ring.submit_nop(0x51_7a_5_6).unwrap();
        let completion = ring.wait_completion().unwrap();
        ring.completions.push_back(completion);

        assert_eq!(
            ring.try_completion().map(|completion| completion.user_data),
            Some(0x51_7a_5_6)
        );
        assert!(ring.try_completion().is_none());
    }

    #[test]
    fn read_once_reads_from_pipe() {
        let Some(mut ring) = available_ring() else {
            return;
        };
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();
        write_bytes(write_fd.raw(), b"uring");

        let mut buffer = [0u8; 5];
        let completion = ring
            .read_once(read_fd.raw(), &mut buffer, 0x51_7a_5_2)
            .unwrap();

        assert_eq!(completion.user_data, 0x51_7a_5_2);
        assert_eq!(completion.result, 5);
        assert_eq!(&buffer, b"uring");
    }

    #[test]
    fn tracked_read_operation_reads_from_pipe() {
        let Some(mut ring) = available_ring() else {
            return;
        };
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();
        write_bytes(write_fd.raw(), b"uring");

        let mut buffer = [0u8; 5];
        // SAFETY: the buffer is kept alive and uniquely borrowed until the
        // tracked completion has been observed below.
        let operation =
            unsafe { ring.queue_read_operation(read_fd.raw(), &mut buffer, u64::MAX) }.unwrap();
        let completion = ring.wait_operation_completion(operation).unwrap();

        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Read);
        assert_eq!(completion.result, 5);
        assert_eq!(&buffer, b"uring");
    }

    #[test]
    fn write_once_writes_to_pipe() {
        let Some(mut ring) = available_ring() else {
            return;
        };
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();

        let completion = ring
            .write_once(write_fd.raw(), b"uring", 0x51_7a_5_3)
            .unwrap();
        let mut buffer = [0u8; 5];
        read_bytes(read_fd.raw(), &mut buffer);

        assert_eq!(completion.user_data, 0x51_7a_5_3);
        assert_eq!(completion.result, 5);
        assert_eq!(&buffer, b"uring");
    }

    #[test]
    fn tracked_write_operation_writes_to_pipe() {
        let Some(mut ring) = available_ring() else {
            return;
        };
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();

        // SAFETY: the buffer is kept alive and immutable until the tracked
        // completion has been observed below.
        let operation =
            unsafe { ring.queue_write_operation(write_fd.raw(), b"uring", u64::MAX) }.unwrap();
        let completion = ring.wait_operation_completion(operation).unwrap();
        let mut buffer = [0u8; 5];
        read_bytes(read_fd.raw(), &mut buffer);

        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Write);
        assert_eq!(completion.result, 5);
        assert_eq!(&buffer, b"uring");
    }

    fn available_ring() -> Option<IoUring> {
        match IoUring::new(8) {
            Ok(ring) => Some(ring),
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(1) | Some(22) | Some(38) | Some(95)
                ) =>
            {
                None
            }
            Err(error) => panic!("failed to create io_uring: {error}"),
        }
    }

    fn write_bytes(fd: RawFd, bytes: &[u8]) {
        // SAFETY: `bytes` is valid readable memory for `bytes.len()` bytes,
        // and tests pass an open non-blocking pipe write descriptor.
        let result =
            unsafe { super::super::write(fd, bytes.as_ptr().cast::<c_void>(), bytes.len()) };
        assert_eq!(result, bytes.len() as isize);
    }

    fn read_bytes(fd: RawFd, bytes: &mut [u8]) {
        // SAFETY: `bytes` is valid writable memory for `bytes.len()` bytes,
        // and tests pass an open non-blocking pipe read descriptor containing
        // exactly that many bytes.
        let result =
            unsafe { super::super::read(fd, bytes.as_mut_ptr().cast::<c_void>(), bytes.len()) };
        assert_eq!(result, bytes.len() as isize);
    }
}
