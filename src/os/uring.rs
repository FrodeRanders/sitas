use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::os::raw::{c_int, c_long, c_uint, c_void};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use super::{OwnedFd, last_os_error};

const SYS_IO_URING_SETUP: c_long = 425;
const SYS_IO_URING_ENTER: c_long = 426;

const IORING_OFF_SQ_RING: i64 = 0;
const IORING_OFF_CQ_RING: i64 = 0x0800_0000;
const IORING_OFF_SQES: i64 = 0x1000_0000;
const IORING_ENTER_GETEVENTS: c_uint = 1;

const IORING_OP_NOP: u8 = 0;
const IORING_OP_TIMEOUT: u8 = 11;
const IORING_OP_ASYNC_CANCEL: u8 = 14;
const IORING_OP_READ: u8 = 22;
const IORING_OP_WRITE: u8 = 23;
const SQE_SIZE: usize = 64;
const SQE_FD_OFFSET: usize = 4;
const SQE_OFF_OFFSET: usize = 8;
const SQE_ADDR_OFFSET: usize = 16;
const SQE_LEN_OFFSET: usize = 24;
const SQE_TIMEOUT_FLAGS_OFFSET: usize = 28;
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

#[repr(C)]
#[derive(Debug)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

impl KernelTimespec {
    fn from_duration(duration: Duration) -> io::Result<Self> {
        let tv_sec = i64::try_from(duration.as_secs()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring timeout seconds exceed i64::MAX",
            )
        })?;

        Ok(Self {
            tv_sec,
            tv_nsec: i64::from(duration.subsec_nanos()),
        })
    }
}

#[derive(Debug)]
struct IoUringOperationState {
    kind: IoUringOperationKind,
    _timeout: Option<Box<KernelTimespec>>,
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
    operations: HashMap<IoUringOperationId, IoUringOperationState>,
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
        let operation = self.allocate_operation(IoUringOperationKind::Nop, None)?;
        if let Err(error) = self.queue_nop(operation.raw()) {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Queues a tracked relative timeout operation and returns its operation id.
    ///
    /// The ring owns the timeout storage until the completion for the returned
    /// operation id is observed.
    pub fn queue_timeout_operation(
        &mut self,
        duration: Duration,
    ) -> io::Result<IoUringOperationId> {
        let timeout = Box::new(KernelTimespec::from_duration(duration)?);
        let timeout_addr = timeout.as_ref() as *const KernelTimespec as u64;
        let operation = self.allocate_operation(IoUringOperationKind::Timeout, Some(timeout))?;

        if let Err(error) = self.queue_timeout(timeout_addr, operation.raw()) {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Submits a cancellation request for a tracked operation.
    ///
    /// The returned operation id identifies the cancellation request itself.
    /// The target operation still produces its own completion, commonly with a
    /// negative cancelled result if the kernel accepted the cancellation.
    pub fn cancel_operation(
        &mut self,
        target: IoUringOperationId,
    ) -> io::Result<IoUringOperationId> {
        let operation = self.queue_cancel_operation(target)?;
        if let Err(error) = self.submit_pending() {
            self.operations.remove(&operation);
            return Err(error);
        }
        Ok(operation)
    }

    /// Queues a cancellation request for a tracked operation.
    ///
    /// The returned operation id identifies the cancellation request itself.
    /// The target operation remains tracked until its own completion is
    /// observed.
    pub fn queue_cancel_operation(
        &mut self,
        target: IoUringOperationId,
    ) -> io::Result<IoUringOperationId> {
        let operation = self.allocate_operation(IoUringOperationKind::Cancel { target }, None)?;
        if let Err(error) = self.queue_cancel(target.raw(), operation.raw()) {
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
        let operation = self.allocate_operation(IoUringOperationKind::Read, None)?;
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
        let operation = self.allocate_operation(IoUringOperationKind::Write, None)?;
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

    /// Waits for a relative timeout through `io_uring`.
    pub fn timeout_once(&mut self, duration: Duration) -> io::Result<IoUringOperationCompletion> {
        let operation = self.queue_timeout_operation(duration)?;
        self.wait_operation_completion(operation)
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

    /// Returns the number of completions already drained into the local queue.
    ///
    /// This does not inspect the kernel completion queue. Call
    /// [`IoUring::drain_completions`] first when a fresh non-blocking sample is
    /// needed.
    pub fn pending_completions(&self) -> usize {
        self.completions.len()
    }

    /// Returns the number of tracked operations whose completions have not yet
    /// been consumed.
    pub fn tracked_operations(&self) -> usize {
        self.operations.len()
    }

    /// Returns a read-only snapshot of locally tracked ring state.
    ///
    /// The snapshot is intentionally local: it reports queued submissions,
    /// locally buffered completions, and tracked operation metadata owned by
    /// this `IoUring` value. It does not make a syscall or mutate the kernel
    /// completion queue.
    pub fn snapshot(&self) -> IoUringSnapshot {
        let mut operation_kinds = IoUringOperationKindCounts::default();
        for state in self.operations.values() {
            operation_kinds.add(state.kind);
        }

        IoUringSnapshot {
            pending_submissions: self.pending_submissions,
            pending_completions: self.completions.len(),
            tracked_operations: self.operations.len(),
            operation_kinds,
        }
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

    /// Waits until at least `min_complete` completions are locally available.
    ///
    /// Pending SQEs are submitted as part of the wait. Any available kernel
    /// completions are drained into the local completion queue and can then be
    /// consumed through [`IoUring::try_completion`],
    /// [`IoUring::wait_completion`], or [`IoUring::try_operation_completion`].
    pub fn wait_completions(&mut self, min_complete: usize) -> io::Result<usize> {
        let target = min_complete;
        u32::try_from(target).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring completion wait count exceeds u32::MAX",
            )
        })?;

        self.drain_completions();
        if self.completions.len() >= target {
            return Ok(self.completions.len());
        }

        loop {
            let needed = target - self.completions.len();
            let min_complete = u32::try_from(needed).expect("completion wait count fits u32");
            let to_submit = self.pending_submissions;
            match self.enter(to_submit, min_complete, IORING_ENTER_GETEVENTS) {
                Ok(submitted) => {
                    self.pending_submissions = self.pending_submissions.saturating_sub(submitted);
                    self.drain_completions();
                    if self.completions.len() >= target {
                        return Ok(self.completions.len());
                    }
                }
                Err(error) if error.raw_os_error() == Some(super::EINTR) => continue,
                Err(error) => return Err(error),
            }
        }
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

    fn queue_timeout(&mut self, timeout_addr: u64, user_data: u64) -> io::Result<()> {
        let sqe = self.prepare_sqe()?;
        write_u8(sqe, IORING_OP_TIMEOUT);
        write_u64(unsafe { sqe.add(SQE_ADDR_OFFSET) }, timeout_addr);
        write_u32(unsafe { sqe.add(SQE_LEN_OFFSET).cast::<u32>() }, 0);
        write_u32(
            unsafe { sqe.add(SQE_TIMEOUT_FLAGS_OFFSET).cast::<u32>() },
            0,
        );
        write_u64(unsafe { sqe.add(SQE_USER_DATA_OFFSET) }, user_data);
        self.finish_sqe()
    }

    fn queue_cancel(&mut self, target_user_data: u64, user_data: u64) -> io::Result<()> {
        let sqe = self.prepare_sqe()?;
        write_u8(sqe, IORING_OP_ASYNC_CANCEL);
        write_i32(unsafe { sqe.add(SQE_FD_OFFSET) }, -1);
        write_u64(unsafe { sqe.add(SQE_ADDR_OFFSET) }, target_user_data);
        write_u32(
            unsafe { sqe.add(SQE_TIMEOUT_FLAGS_OFFSET).cast::<u32>() },
            0,
        );
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

    fn allocate_operation(
        &mut self,
        kind: IoUringOperationKind,
        timeout: Option<Box<KernelTimespec>>,
    ) -> io::Result<IoUringOperationId> {
        if self.next_operation == OPERATION_USER_DATA_BASE {
            return Err(io::Error::other("io_uring operation id space exhausted"));
        }

        let operation = IoUringOperationId(OPERATION_USER_DATA_BASE | self.next_operation);
        self.next_operation += 1;
        let state = IoUringOperationState {
            kind,
            _timeout: timeout,
        };
        if self.operations.insert(operation, state).is_some() {
            return Err(io::Error::other("io_uring operation id collision"));
        }
        Ok(operation)
    }

    fn complete_operation(
        &mut self,
        operation: IoUringOperationId,
        completion: IoUringCompletion,
    ) -> io::Result<IoUringOperationCompletion> {
        let state = self.operations.remove(&operation).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "io_uring completion does not match a tracked operation",
            )
        })?;

        Ok(IoUringOperationCompletion {
            operation,
            kind: state.kind,
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

/// Dispatches tracked `io_uring` completions to registered task wakers.
///
/// This is the first bridge from the raw completion queue to async scheduling.
/// It owns an [`IoUring`], lets callers register interest in tracked
/// operation ids, and stores completed operations until the future or executor
/// consumes them.
pub struct IoUringDispatcher {
    ring: IoUring,
    waiters: HashMap<IoUringOperationId, Waker>,
    completions: HashMap<IoUringOperationId, IoUringOperationCompletion>,
    deferred_buffers: HashMap<IoUringOperationId, Vec<u8>>,
}

impl IoUringDispatcher {
    /// Creates a dispatcher around an existing ring.
    pub fn new(ring: IoUring) -> Self {
        Self {
            ring,
            waiters: HashMap::new(),
            completions: HashMap::new(),
            deferred_buffers: HashMap::new(),
        }
    }

    /// Wraps this dispatcher in single-threaded shared ownership.
    pub fn into_shared(self) -> SharedIoUringDispatcher {
        Rc::new(RefCell::new(self))
    }

    /// Returns shared access to the owned ring.
    pub fn ring(&self) -> &IoUring {
        &self.ring
    }

    /// Returns mutable access to the owned ring for queuing operations.
    pub fn ring_mut(&mut self) -> &mut IoUring {
        &mut self.ring
    }

    /// Registers or replaces the waker for a tracked operation.
    ///
    /// Returns `true` if the operation has already completed locally. In that
    /// case the waker is not stored and the caller can consume the completion
    /// immediately with [`IoUringDispatcher::take_completion`].
    pub fn register_waker(&mut self, operation: IoUringOperationId, waker: &Waker) -> bool {
        if self.completions.contains_key(&operation) {
            return true;
        }

        self.waiters.insert(operation, waker.clone());
        false
    }

    /// Removes any registered waker for an operation.
    pub fn clear_waker(&mut self, operation: IoUringOperationId) -> Option<Waker> {
        self.waiters.remove(&operation)
    }

    /// Returns and removes a completed tracked operation, if available.
    pub fn take_completion(
        &mut self,
        operation: IoUringOperationId,
    ) -> Option<IoUringOperationCompletion> {
        self.completions.remove(&operation)
    }

    /// Keeps an abandoned owned I/O buffer alive until its kernel operation
    /// completes.
    ///
    /// If the completion has already been dispatched, both the completion and
    /// buffer are discarded immediately because no future remains to consume
    /// them.
    pub fn defer_buffer_drop(&mut self, operation: IoUringOperationId, buffer: Vec<u8>) {
        self.clear_waker(operation);
        if self.completions.remove(&operation).is_none() {
            self.deferred_buffers.insert(operation, buffer);
        }
    }

    /// Dispatches all locally available tracked completions.
    ///
    /// Matching registered wakers are removed and woken. The completions remain
    /// available through [`IoUringDispatcher::take_completion`].
    pub fn dispatch_available(&mut self) -> usize {
        let mut dispatched = 0;
        while let Some(completion) = self.ring.try_operation_completion() {
            let operation = completion.operation;
            if self.deferred_buffers.remove(&operation).is_some() {
                dispatched += 1;
                continue;
            }

            self.completions.insert(operation, completion);
            if let Some(waker) = self.waiters.remove(&operation) {
                waker.wake();
            }
            dispatched += 1;
        }
        dispatched
    }

    /// Waits for completions through the ring and dispatches tracked ones.
    pub fn wait_and_dispatch(&mut self, min_complete: usize) -> io::Result<usize> {
        self.ring.wait_completions(min_complete)?;
        Ok(self.dispatch_available())
    }

    /// Returns a local dispatcher snapshot.
    pub fn snapshot(&self) -> IoUringDispatcherSnapshot {
        IoUringDispatcherSnapshot {
            ring: self.ring.snapshot(),
            registered_wakers: self.waiters.len(),
            completed_operations: self.completions.len(),
            deferred_buffers: self.deferred_buffers.len(),
        }
    }
}

impl fmt::Debug for IoUringDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringDispatcher")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Single-thread shared ownership for an [`IoUringDispatcher`].
pub type SharedIoUringDispatcher = Rc<RefCell<IoUringDispatcher>>;

/// Future that completes when a tracked `io_uring` operation is dispatched.
///
/// The future registers the current task waker with the shared dispatcher when
/// pending. An event loop still has to drive the dispatcher by calling
/// [`IoUringDispatcher::dispatch_available`] or
/// [`IoUringDispatcher::wait_and_dispatch`].
pub struct IoUringOperationFuture {
    dispatcher: SharedIoUringDispatcher,
    operation: IoUringOperationId,
}

impl IoUringOperationFuture {
    /// Creates a future for a tracked operation owned by `dispatcher`.
    pub fn new(dispatcher: SharedIoUringDispatcher, operation: IoUringOperationId) -> Self {
        Self {
            dispatcher,
            operation,
        }
    }

    /// Queues a tracked no-op operation and returns a future for its
    /// completion.
    pub fn queue_nop(dispatcher: SharedIoUringDispatcher) -> io::Result<Self> {
        let operation = dispatcher.borrow_mut().ring_mut().queue_nop_operation()?;
        Ok(Self::new(dispatcher, operation))
    }

    /// Queues a tracked relative timeout and returns a future for its
    /// completion.
    pub fn queue_timeout(
        dispatcher: SharedIoUringDispatcher,
        duration: Duration,
    ) -> io::Result<Self> {
        let operation = dispatcher
            .borrow_mut()
            .ring_mut()
            .queue_timeout_operation(duration)?;
        Ok(Self::new(dispatcher, operation))
    }

    /// Queues a tracked cancellation request and returns a future for the
    /// cancellation request completion.
    pub fn queue_cancel(
        dispatcher: SharedIoUringDispatcher,
        target: IoUringOperationId,
    ) -> io::Result<Self> {
        let operation = dispatcher
            .borrow_mut()
            .ring_mut()
            .queue_cancel_operation(target)?;
        Ok(Self::new(dispatcher, operation))
    }

    /// Returns the operation id this future waits for.
    pub fn operation(&self) -> IoUringOperationId {
        self.operation
    }
}

impl Future for IoUringOperationFuture {
    type Output = IoUringOperationCompletion;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut dispatcher = self.dispatcher.borrow_mut();
        dispatcher.dispatch_available();

        if let Some(completion) = dispatcher.take_completion(self.operation) {
            return Poll::Ready(completion);
        }

        if dispatcher.register_waker(self.operation, context.waker())
            && let Some(completion) = dispatcher.take_completion(self.operation)
        {
            return Poll::Ready(completion);
        }

        Poll::Pending
    }
}

impl Drop for IoUringOperationFuture {
    fn drop(&mut self) {
        self.dispatcher.borrow_mut().clear_waker(self.operation);
    }
}

impl fmt::Debug for IoUringOperationFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringOperationFuture")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

/// Result of an owned-buffer `io_uring` read future.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoUringReadCompletion {
    /// Completion metadata for the read operation.
    pub completion: IoUringOperationCompletion,
    /// Buffer owned by the read operation.
    pub buffer: Vec<u8>,
}

/// Future for a tracked read operation that owns its buffer.
///
/// The future keeps the buffer allocation alive until the kernel completion is
/// observed. Dropping it while pending transfers the buffer to the dispatcher,
/// which releases it after the matching completion is dispatched.
pub struct IoUringReadFuture {
    operation: IoUringOperationFuture,
    buffer: Option<Vec<u8>>,
}

impl IoUringReadFuture {
    /// Queues a tracked read operation using an owned buffer.
    pub fn queue(
        dispatcher: SharedIoUringDispatcher,
        fd: RawFd,
        mut buffer: Vec<u8>,
        offset: u64,
    ) -> io::Result<Self> {
        let operation = {
            let mut dispatcher_ref = dispatcher.borrow_mut();
            // SAFETY: `buffer` is moved into the returned future and kept
            // alive until completion. Moving the Vec value does not move the
            // heap allocation passed to the kernel.
            unsafe {
                dispatcher_ref
                    .ring_mut()
                    .queue_read_operation(fd, &mut buffer, offset)?
            }
        };

        Ok(Self {
            operation: IoUringOperationFuture::new(dispatcher, operation),
            buffer: Some(buffer),
        })
    }

    /// Returns the operation id this future waits for.
    pub fn operation(&self) -> IoUringOperationId {
        self.operation.operation()
    }
}

impl Future for IoUringReadFuture {
    type Output = IoUringReadCompletion;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.operation).poll(context) {
            Poll::Ready(completion) => Poll::Ready(IoUringReadCompletion {
                completion,
                buffer: this
                    .buffer
                    .take()
                    .expect("read buffer exists until completion"),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for IoUringReadFuture {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let operation = self.operation();
            self.operation
                .dispatcher
                .borrow_mut()
                .defer_buffer_drop(operation, buffer);
        }
    }
}

impl fmt::Debug for IoUringReadFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringReadFuture")
            .field("operation", &self.operation())
            .field("buffer_len", &self.buffer.as_ref().map_or(0, Vec::len))
            .finish_non_exhaustive()
    }
}

/// Result of an owned-buffer `io_uring` write future.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoUringWriteCompletion {
    /// Completion metadata for the write operation.
    pub completion: IoUringOperationCompletion,
    /// Buffer owned by the write operation.
    pub buffer: Vec<u8>,
}

/// Future for a tracked write operation that owns its buffer.
///
/// The future keeps the buffer allocation alive until the kernel completion is
/// observed. Dropping it while pending transfers the buffer to the dispatcher,
/// which releases it after the matching completion is dispatched.
pub struct IoUringWriteFuture {
    operation: IoUringOperationFuture,
    buffer: Option<Vec<u8>>,
}

impl IoUringWriteFuture {
    /// Queues a tracked write operation using an owned buffer.
    pub fn queue(
        dispatcher: SharedIoUringDispatcher,
        fd: RawFd,
        buffer: Vec<u8>,
        offset: u64,
    ) -> io::Result<Self> {
        let operation = {
            let mut dispatcher_ref = dispatcher.borrow_mut();
            // SAFETY: `buffer` is moved into the returned future and kept
            // alive until completion. Moving the Vec value does not move the
            // heap allocation passed to the kernel.
            unsafe {
                dispatcher_ref
                    .ring_mut()
                    .queue_write_operation(fd, &buffer, offset)?
            }
        };

        Ok(Self {
            operation: IoUringOperationFuture::new(dispatcher, operation),
            buffer: Some(buffer),
        })
    }

    /// Returns the operation id this future waits for.
    pub fn operation(&self) -> IoUringOperationId {
        self.operation.operation()
    }
}

impl Future for IoUringWriteFuture {
    type Output = IoUringWriteCompletion;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.operation).poll(context) {
            Poll::Ready(completion) => Poll::Ready(IoUringWriteCompletion {
                completion,
                buffer: this
                    .buffer
                    .take()
                    .expect("write buffer exists until completion"),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for IoUringWriteFuture {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let operation = self.operation();
            self.operation
                .dispatcher
                .borrow_mut()
                .defer_buffer_drop(operation, buffer);
        }
    }
}

impl fmt::Debug for IoUringWriteFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringWriteFuture")
            .field("operation", &self.operation())
            .field("buffer_len", &self.buffer.as_ref().map_or(0, Vec::len))
            .finish_non_exhaustive()
    }
}

/// Read-only locally observable state for an [`IoUringDispatcher`].
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoUringDispatcherSnapshot {
    /// Snapshot of the owned raw ring.
    pub ring: IoUringSnapshot,
    /// Number of operations with registered task wakers.
    pub registered_wakers: usize,
    /// Number of completed tracked operations buffered for consumption.
    pub completed_operations: usize,
    /// Number of abandoned owned buffers waiting for kernel completion.
    pub deferred_buffers: usize,
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
    /// Relative timeout operation.
    Timeout,
    /// Cancellation request for another tracked operation.
    Cancel {
        /// Target operation requested for cancellation.
        target: IoUringOperationId,
    },
}

/// Counts of tracked operations by kind in an [`IoUringSnapshot`].
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoUringOperationKindCounts {
    /// Number of tracked no-op operations.
    pub nops: usize,
    /// Number of tracked read operations.
    pub reads: usize,
    /// Number of tracked write operations.
    pub writes: usize,
    /// Number of tracked timeout operations.
    pub timeouts: usize,
    /// Number of tracked cancellation operations.
    pub cancellations: usize,
}

impl IoUringOperationKindCounts {
    fn add(&mut self, kind: IoUringOperationKind) {
        match kind {
            IoUringOperationKind::Nop => self.nops += 1,
            IoUringOperationKind::Read => self.reads += 1,
            IoUringOperationKind::Write => self.writes += 1,
            IoUringOperationKind::Timeout => self.timeouts += 1,
            IoUringOperationKind::Cancel { .. } => self.cancellations += 1,
        }
    }
}

/// Read-only locally observable state for an [`IoUring`] instance.
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoUringSnapshot {
    /// SQEs queued locally but not yet submitted to the kernel.
    pub pending_submissions: u32,
    /// CQEs drained into the local completion queue but not yet consumed.
    pub pending_completions: usize,
    /// Tracked operations whose completions have not yet been consumed.
    pub tracked_operations: usize,
    /// Tracked operations grouped by operation kind.
    pub operation_kinds: IoUringOperationKindCounts,
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
    use super::{
        IoUring, IoUringDispatcher, IoUringOperationFuture, IoUringOperationKind,
        IoUringReadFuture, IoUringWriteFuture,
    };
    use std::future::Future;
    use std::os::raw::c_void;
    use std::os::unix::io::RawFd;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    const ETIME: i32 = 62;
    const ECANCELED: i32 = 125;

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
    fn snapshot_reports_local_operation_state() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        let timeout = ring
            .queue_timeout_operation(Duration::from_secs(1))
            .unwrap();
        let cancel = ring.queue_cancel_operation(timeout).unwrap();

        let snapshot = ring.snapshot();
        assert_eq!(ring.pending_submissions(), 2);
        assert_eq!(ring.pending_completions(), 0);
        assert_eq!(ring.tracked_operations(), 2);
        assert_eq!(snapshot.pending_submissions, 2);
        assert_eq!(snapshot.pending_completions, 0);
        assert_eq!(snapshot.tracked_operations, 2);
        assert_eq!(snapshot.operation_kinds.timeouts, 1);
        assert_eq!(snapshot.operation_kinds.cancellations, 1);

        let cancel_completion = ring.wait_operation_completion(cancel).unwrap();
        let timeout_completion = ring.wait_operation_completion(timeout).unwrap();

        assert_eq!(cancel_completion.result, 0);
        assert_eq!(timeout_completion.result, -ECANCELED);
        assert_eq!(ring.snapshot().tracked_operations, 0);
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
    fn wait_completions_submits_and_drains_batch() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_5_e).unwrap();
        ring.queue_nop(0x51_7a_5_f).unwrap();
        assert_eq!(ring.pending_submissions(), 2);

        assert_eq!(ring.wait_completions(2).unwrap(), 2);
        assert_eq!(ring.pending_submissions(), 0);
        assert_eq!(ring.pending_completions(), 2);
        assert_eq!(ring.snapshot().pending_completions, 2);

        let first = ring.wait_completion().unwrap();
        let second = ring.wait_completion().unwrap();

        assert_eq!(first.user_data, 0x51_7a_5_e);
        assert_eq!(first.result, 0);
        assert_eq!(second.user_data, 0x51_7a_5_f);
        assert_eq!(second.result, 0);
    }

    #[test]
    fn wait_completions_keeps_tracked_and_raw_completions_available() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        ring.queue_nop(0x51_7a_60).unwrap();
        let operation = ring.queue_nop_operation().unwrap();

        assert_eq!(ring.wait_completions(2).unwrap(), 2);

        let tracked = ring.try_operation_completion().unwrap();
        let raw = ring.wait_completion().unwrap();

        assert_eq!(tracked.operation, operation);
        assert_eq!(tracked.kind, IoUringOperationKind::Nop);
        assert_eq!(tracked.result, 0);
        assert_eq!(raw.user_data, 0x51_7a_60);
        assert_eq!(raw.result, 0);
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
    fn dispatcher_wakes_registered_operation() {
        let Some(ring) = available_ring() else {
            return;
        };
        let mut dispatcher = IoUringDispatcher::new(ring);
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));

        let operation = dispatcher.ring_mut().queue_nop_operation().unwrap();
        assert!(!dispatcher.register_waker(operation, &waker));
        assert_eq!(dispatcher.snapshot().registered_wakers, 1);

        assert_eq!(dispatcher.wait_and_dispatch(1).unwrap(), 1);

        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        let completion = dispatcher.take_completion(operation).unwrap();
        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(completion.result, 0);
        assert_eq!(dispatcher.snapshot().registered_wakers, 0);
        assert_eq!(dispatcher.snapshot().completed_operations, 0);
    }

    #[test]
    fn dispatcher_reports_already_completed_operation() {
        let Some(ring) = available_ring() else {
            return;
        };
        let mut dispatcher = IoUringDispatcher::new(ring);
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));

        let operation = dispatcher.ring_mut().queue_nop_operation().unwrap();
        assert_eq!(dispatcher.wait_and_dispatch(1).unwrap(), 1);

        assert!(dispatcher.register_waker(operation, &waker));
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);
        assert_eq!(dispatcher.snapshot().registered_wakers, 0);
        assert_eq!(dispatcher.snapshot().completed_operations, 1);

        let completion = dispatcher.take_completion(operation).unwrap();
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(dispatcher.snapshot().completed_operations, 0);
    }

    #[test]
    fn operation_future_registers_and_resolves_after_dispatch() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let operation = dispatcher
            .borrow_mut()
            .ring_mut()
            .queue_nop_operation()
            .unwrap();
        let mut future = IoUringOperationFuture::new(Rc::clone(&dispatcher), operation);
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 1);

        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(completion) = Pin::new(&mut future).poll(&mut context) else {
            panic!("operation future should be ready after dispatch");
        };
        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(completion.result, 0);
    }

    #[test]
    fn operation_future_can_queue_tracked_nop() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let mut future = IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher)).unwrap();
        let operation = future.operation();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(completion) = Pin::new(&mut future).poll(&mut context) else {
            panic!("queued nop future should be ready after dispatch");
        };
        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(completion.result, 0);
    }

    #[test]
    fn operation_future_can_queue_tracked_timeout() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let mut future =
            IoUringOperationFuture::queue_timeout(Rc::clone(&dispatcher), Duration::from_millis(1))
                .unwrap();
        let operation = future.operation();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(completion) = Pin::new(&mut future).poll(&mut context) else {
            panic!("queued timeout future should be ready after dispatch");
        };
        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Timeout);
        assert_eq!(completion.result, -ETIME);
    }

    #[test]
    fn operation_future_can_queue_tracked_cancel() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let timeout = dispatcher
            .borrow_mut()
            .ring_mut()
            .queue_timeout_operation(Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            dispatcher.borrow_mut().ring_mut().submit_pending().unwrap(),
            1
        );

        let mut cancel_future =
            IoUringOperationFuture::queue_cancel(Rc::clone(&dispatcher), timeout).unwrap();
        let cancel = cancel_future.operation();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut cancel_future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(cancel_completion) = Pin::new(&mut cancel_future).poll(&mut context) else {
            panic!("queued cancel future should be ready after dispatch");
        };
        assert_eq!(cancel_completion.operation, cancel);
        assert_eq!(
            cancel_completion.kind,
            IoUringOperationKind::Cancel { target: timeout }
        );
        assert_eq!(cancel_completion.result, 0);

        let timeout_completion = dispatcher
            .borrow_mut()
            .ring_mut()
            .wait_operation_completion(timeout)
            .unwrap();
        assert_eq!(timeout_completion.kind, IoUringOperationKind::Timeout);
        assert_eq!(timeout_completion.result, -ECANCELED);
    }

    #[test]
    fn owned_read_future_returns_filled_buffer() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();
        write_bytes(write_fd.raw(), b"uring");

        let mut future =
            IoUringReadFuture::queue(Rc::clone(&dispatcher), read_fd.raw(), vec![0; 5], u64::MAX)
                .unwrap();
        let operation = future.operation();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(read) = Pin::new(&mut future).poll(&mut context) else {
            panic!("owned read future should be ready after dispatch");
        };
        assert_eq!(read.completion.operation, operation);
        assert_eq!(read.completion.kind, IoUringOperationKind::Read);
        assert_eq!(read.completion.result, 5);
        assert_eq!(&read.buffer, b"uring");
    }

    #[test]
    fn dropping_pending_owned_read_defers_buffer_until_completion() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();
        write_bytes(write_fd.raw(), b"uring");
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(wake_count);
        let mut context = Context::from_waker(&waker);

        {
            let mut future = IoUringReadFuture::queue(
                Rc::clone(&dispatcher),
                read_fd.raw(),
                vec![0; 5],
                u64::MAX,
            )
            .unwrap();
            assert!(matches!(
                Pin::new(&mut future).poll(&mut context),
                Poll::Pending
            ));
            assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 1);
        }

        assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 0);
        assert_eq!(dispatcher.borrow().snapshot().deferred_buffers, 1);

        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(dispatcher.borrow().snapshot().deferred_buffers, 0);
        assert_eq!(dispatcher.borrow().snapshot().completed_operations, 0);
    }

    #[test]
    fn owned_write_future_returns_written_buffer() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();

        let mut future = IoUringWriteFuture::queue(
            Rc::clone(&dispatcher),
            write_fd.raw(),
            b"uring".to_vec(),
            u64::MAX,
        )
        .unwrap();
        let operation = future.operation();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        let Poll::Ready(write) = Pin::new(&mut future).poll(&mut context) else {
            panic!("owned write future should be ready after dispatch");
        };
        let mut buffer = [0u8; 5];
        read_bytes(read_fd.raw(), &mut buffer);

        assert_eq!(write.completion.operation, operation);
        assert_eq!(write.completion.kind, IoUringOperationKind::Write);
        assert_eq!(write.completion.result, 5);
        assert_eq!(&write.buffer, b"uring");
        assert_eq!(&buffer, b"uring");
    }

    #[test]
    fn dropping_pending_owned_write_defers_buffer_until_completion() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let (read_fd, write_fd) = super::super::create_pipe().unwrap();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(wake_count);
        let mut context = Context::from_waker(&waker);

        {
            let mut future = IoUringWriteFuture::queue(
                Rc::clone(&dispatcher),
                write_fd.raw(),
                b"uring".to_vec(),
                u64::MAX,
            )
            .unwrap();
            assert!(matches!(
                Pin::new(&mut future).poll(&mut context),
                Poll::Pending
            ));
            assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 1);
        }

        assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 0);
        assert_eq!(dispatcher.borrow().snapshot().deferred_buffers, 1);

        assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(dispatcher.borrow().snapshot().deferred_buffers, 0);
        assert_eq!(dispatcher.borrow().snapshot().completed_operations, 0);

        let mut buffer = [0u8; 5];
        read_bytes(read_fd.raw(), &mut buffer);
        assert_eq!(&buffer, b"uring");
    }

    #[test]
    fn dropping_pending_operation_future_clears_registered_waker() {
        let Some(ring) = available_ring() else {
            return;
        };
        let dispatcher = IoUringDispatcher::new(ring).into_shared();
        let operation = dispatcher
            .borrow_mut()
            .ring_mut()
            .queue_nop_operation()
            .unwrap();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(wake_count);
        let mut context = Context::from_waker(&waker);

        {
            let mut future = IoUringOperationFuture::new(Rc::clone(&dispatcher), operation);
            assert!(matches!(
                Pin::new(&mut future).poll(&mut context),
                Poll::Pending
            ));
            assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 1);
        }

        assert_eq!(dispatcher.borrow().snapshot().registered_wakers, 0);
    }

    #[test]
    fn tracked_timeout_operation_completes_after_deadline() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        let operation = ring
            .queue_timeout_operation(Duration::from_millis(1))
            .unwrap();
        let completion = ring.wait_operation_completion(operation).unwrap();

        assert_eq!(completion.operation, operation);
        assert_eq!(completion.kind, IoUringOperationKind::Timeout);
        assert_eq!(completion.result, -ETIME);
    }

    #[test]
    fn timeout_once_waits_for_timeout_completion() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        let completion = ring.timeout_once(Duration::from_millis(1)).unwrap();

        assert_eq!(completion.kind, IoUringOperationKind::Timeout);
        assert_eq!(completion.result, -ETIME);
    }

    #[test]
    fn tracked_timeout_can_be_cancelled() {
        let Some(mut ring) = available_ring() else {
            return;
        };

        let timeout = ring
            .queue_timeout_operation(Duration::from_secs(1))
            .unwrap();
        assert_eq!(ring.submit_pending().unwrap(), 1);

        let cancel = ring.cancel_operation(timeout).unwrap();
        let first = ring.wait_operation_completion(cancel).unwrap();
        let second = ring.wait_operation_completion(timeout).unwrap();

        assert_eq!(first.operation, cancel);
        assert_eq!(first.kind, IoUringOperationKind::Cancel { target: timeout });
        assert_eq!(first.result, 0);
        assert_eq!(second.operation, timeout);
        assert_eq!(second.kind, IoUringOperationKind::Timeout);
        assert_eq!(second.result, -ECANCELED);
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

    fn counting_waker(counter: Arc<AtomicUsize>) -> Waker {
        Waker::from(Arc::new(CountingWake { counter }))
    }

    struct CountingWake {
        counter: Arc<AtomicUsize>,
    }

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}
