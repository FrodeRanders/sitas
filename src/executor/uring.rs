#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::os::{
    IoUringDispatcher, IoUringDispatcherSnapshot, IoUringOperationCompletion, IoUringOperationId,
    IoUringShutdownDrainSnapshot, IoUringShutdownDrainStatus, available_io_uring,
};

use super::scheduling_group::ExecutorId;

type SharedDispatcher = crate::os::SharedIoUringDispatcher;

const EXECUTOR_IO_URING_ENTRIES: u32 = 256;
pub(super) const EXECUTOR_IO_URING_COMPLETION_BUDGET: usize = 64;
const EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS: usize = 8;

thread_local! {
    static CURRENT_IO_URING: RefCell<Option<CurrentIoUring>> = const { RefCell::new(None) };
}

struct CurrentIoUring {
    owner: ExecutorId,
    dispatcher: SharedDispatcher,
}

pub(super) struct ExecutorIoUringScope;

impl ExecutorIoUringScope {
    pub(super) fn enter(owner: ExecutorId) -> Self {
        install_current_io_uring(owner);
        Self
    }
}

impl Drop for ExecutorIoUringScope {
    fn drop(&mut self) {
        shutdown_current_if_idle();
    }
}

/// Returns a future that reads up to `buffer.len()` bytes from `fd` at
/// `offset` through the current executor's Linux `io_uring` backend.
///
/// The returned future owns its buffer, so it is safe to move across threads
/// before it is first polled. It must be polled by a sitas executor running on
/// Linux with `io_uring` available.
pub fn read_at_uring(fd: RawFd, offset: u64, buffer: Vec<u8>) -> ReadAtUring {
    ReadAtUring {
        fd,
        offset,
        buffer: Some(buffer),
        operation: None,
    }
}

/// Reads exactly `len` bytes from `fd` at `offset` through the current
/// executor's Linux `io_uring` backend.
///
/// This retries short reads with updated offsets. Reaching EOF before `len`
/// bytes have been read returns [`io::ErrorKind::UnexpectedEof`].
pub async fn read_exact_at_uring(fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(len);

    while output.len() < len {
        let read_offset = offset + output.len() as u64;
        let remaining = len - output.len();
        let buffer = read_at_uring(fd, read_offset, vec![0; remaining]).await?;
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "io_uring read reached EOF before filling the requested buffer",
            ));
        }
        output.extend_from_slice(&buffer);
    }

    Ok(output)
}

/// Returns a future that writes `buffer` to `fd` at `offset` through the
/// current executor's Linux `io_uring` backend.
///
/// This future performs one kernel write. Use [`write_all_at_uring`] when the
/// full buffer must be written across possible partial completions.
fn write_at_uring(fd: RawFd, offset: u64, buffer: Vec<u8>) -> WriteAtUring {
    WriteAtUring {
        fd,
        offset,
        buffer: Some(buffer),
        operation: None,
    }
}

/// Writes an owned buffer completely at `offset`, retrying short `io_uring`
/// writes with updated offsets.
pub async fn write_all_at_uring(fd: RawFd, offset: u64, mut buffer: Vec<u8>) -> io::Result<()> {
    let mut write_offset = offset;

    while !buffer.is_empty() {
        let requested = buffer.len();
        let completion = write_at_uring(fd, write_offset, buffer).await?;
        let written = completion.bytes;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "io_uring write accepted zero bytes",
            ));
        }

        write_offset += written as u64;
        if written >= requested {
            buffer = Vec::new();
        } else {
            buffer = completion.buffer[written..].to_vec();
        }
    }

    Ok(())
}

/// Future returned by [`read_at_uring`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct ReadAtUring {
    fd: RawFd,
    offset: u64,
    buffer: Option<Vec<u8>>,
    operation: Option<IoUringOperationId>,
}

impl Future for ReadAtUring {
    type Output = io::Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let dispatcher = match current_dispatcher() {
            Some(dispatcher) => dispatcher,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "executor io_uring backend is unavailable",
                )));
            }
        };

        if let Err(error) = this.ensure_queued(Rc::clone(&dispatcher)) {
            return Poll::Ready(Err(error));
        }

        match poll_operation(Rc::clone(&dispatcher), this.operation.unwrap(), context) {
            Poll::Ready(Ok(completion)) => {
                let read = match completion_result_to_usize(completion.result, "read") {
                    Ok(read) => read,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                let mut buffer = this
                    .buffer
                    .take()
                    .expect("read buffer exists until completion");
                buffer.truncate(read);
                this.operation = None;
                Poll::Ready(Ok(buffer))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ReadAtUring {
    fn ensure_queued(&mut self, dispatcher: SharedDispatcher) -> io::Result<()> {
        if self.operation.is_some() {
            return Ok(());
        }

        let buffer = self
            .buffer
            .as_mut()
            .expect("read buffer exists until queued");
        let operation = {
            let mut dispatcher = dispatcher.borrow_mut();
            // SAFETY: the buffer is owned by this future and remains alive
            // until the operation completes or is transferred to the dispatcher
            // on drop.
            unsafe {
                dispatcher
                    .ring_mut()
                    .queue_read_operation(self.fd, buffer, self.offset)?
            }
        };
        self.operation = Some(operation);
        Ok(())
    }
}

impl Drop for ReadAtUring {
    fn drop(&mut self) {
        let (Some(operation), Some(buffer)) = (self.operation, self.buffer.take()) else {
            return;
        };
        if let Some(dispatcher) = current_dispatcher() {
            dispatcher.borrow_mut().defer_buffer_drop(operation, buffer);
        }
    }
}

/// Completion returned by a single write operation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAtUringCompletion {
    /// Number of bytes accepted by the kernel.
    pub bytes: usize,
    /// Original owned write buffer.
    pub buffer: Vec<u8>,
}

/// Future returned by the internal one-shot write helper.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct WriteAtUring {
    fd: RawFd,
    offset: u64,
    buffer: Option<Vec<u8>>,
    operation: Option<IoUringOperationId>,
}

impl Future for WriteAtUring {
    type Output = io::Result<WriteAtUringCompletion>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let dispatcher = match current_dispatcher() {
            Some(dispatcher) => dispatcher,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "executor io_uring backend is unavailable",
                )));
            }
        };

        if let Err(error) = this.ensure_queued(Rc::clone(&dispatcher)) {
            return Poll::Ready(Err(error));
        }

        match poll_operation(Rc::clone(&dispatcher), this.operation.unwrap(), context) {
            Poll::Ready(Ok(completion)) => {
                let bytes = match completion_result_to_usize(completion.result, "write") {
                    Ok(bytes) => bytes,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                let buffer = this
                    .buffer
                    .take()
                    .expect("write buffer exists until completion");
                this.operation = None;
                Poll::Ready(Ok(WriteAtUringCompletion { bytes, buffer }))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl WriteAtUring {
    fn ensure_queued(&mut self, dispatcher: SharedDispatcher) -> io::Result<()> {
        if self.operation.is_some() {
            return Ok(());
        }

        let buffer = self
            .buffer
            .as_ref()
            .expect("write buffer exists until queued");
        let operation = {
            let mut dispatcher = dispatcher.borrow_mut();
            // SAFETY: the buffer is owned by this future and remains alive
            // until the operation completes or is transferred to the dispatcher
            // on drop.
            unsafe {
                dispatcher
                    .ring_mut()
                    .queue_write_operation(self.fd, buffer, self.offset)?
            }
        };
        self.operation = Some(operation);
        Ok(())
    }
}

impl Drop for WriteAtUring {
    fn drop(&mut self) {
        let (Some(operation), Some(buffer)) = (self.operation, self.buffer.take()) else {
            return;
        };
        if let Some(dispatcher) = current_dispatcher() {
            dispatcher.borrow_mut().defer_buffer_drop(operation, buffer);
        }
    }
}

fn poll_operation(
    dispatcher: SharedDispatcher,
    operation: IoUringOperationId,
    context: &mut Context<'_>,
) -> Poll<io::Result<IoUringOperationCompletion>> {
    let mut dispatcher = dispatcher.borrow_mut();
    dispatcher.dispatch_available();

    if let Some(completion) = dispatcher.take_completion(operation) {
        return Poll::Ready(Ok(completion));
    }

    if dispatcher.register_waker(operation, context.waker())
        && let Some(completion) = dispatcher.take_completion(operation)
    {
        return Poll::Ready(Ok(completion));
    }

    Poll::Pending
}

fn completion_result_to_usize(result: i32, operation: &str) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }

    usize::try_from(result).map_err(|_| {
        io::Error::other(format!(
            "io_uring {operation} completion result did not fit usize"
        ))
    })
}

pub(super) fn install_current_io_uring(owner: ExecutorId) {
    CURRENT_IO_URING.with(|current| {
        if let Some(installed) = current.borrow().as_ref() {
            if installed.owner == owner {
                return;
            }

            let snapshot = installed.dispatcher.borrow().snapshot();
            assert!(
                snapshot.is_idle(),
                "executor io_uring dispatcher for another executor still has live state: {snapshot:?}"
            );
        }

        let dispatcher = available_io_uring(EXECUTOR_IO_URING_ENTRIES)
            .ok()
            .flatten()
            .map(|ring| IoUringDispatcher::new(ring).into_shared());
        *current.borrow_mut() = dispatcher.map(|dispatcher| CurrentIoUring { owner, dispatcher });
    });
}

pub(super) fn shutdown_current(owner: ExecutorId) -> Option<IoUringDispatcherSnapshot> {
    CURRENT_IO_URING.with(|current| {
        {
            let current_ref = current.borrow();
            let installed = current_ref.as_ref()?;
            if installed.owner != owner {
                return None;
            }

            if installed.dispatcher.borrow().snapshot().registered_wakers > 0 {
                let mut snapshot = installed.dispatcher.borrow().snapshot();
                snapshot.shutdown_drain = Some(IoUringShutdownDrainSnapshot {
                    status: IoUringShutdownDrainStatus::SkippedLiveWakers,
                    max_waits: EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS,
                    dispatched: 0,
                });
                return Some(snapshot);
            }
        }

        let installed = current.borrow_mut().take().expect("dispatcher exists");
        let mut dispatcher = installed.dispatcher.borrow_mut();
        Some(shutdown_dispatcher(&mut dispatcher))
    })
}

fn shutdown_current_if_idle() {
    CURRENT_IO_URING.with(|current| {
        let should_shutdown = current.borrow().as_ref().is_some_and(|installed| {
            installed.dispatcher.borrow().snapshot().registered_wakers == 0
        });
        if should_shutdown {
            let installed = current.borrow_mut().take().expect("dispatcher exists");
            let mut dispatcher = installed.dispatcher.borrow_mut();
            let _ = shutdown_dispatcher(&mut dispatcher);
        }
    });
}

fn shutdown_dispatcher(dispatcher: &mut IoUringDispatcher) -> IoUringDispatcherSnapshot {
    let mut dispatched = 0;
    let status = if dispatcher.snapshot().registered_wakers > 0 {
        IoUringShutdownDrainStatus::SkippedLiveWakers
    } else {
        dispatched += dispatcher.dispatch_available();
        if dispatcher.is_idle() {
            IoUringShutdownDrainStatus::Completed
        } else {
            drain_shutdown_dispatcher(dispatcher, &mut dispatched)
        }
    };

    let mut snapshot = dispatcher.snapshot();
    snapshot.shutdown_drain = Some(IoUringShutdownDrainSnapshot {
        status,
        max_waits: EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS,
        dispatched,
    });
    snapshot
}

fn drain_shutdown_dispatcher(
    dispatcher: &mut IoUringDispatcher,
    dispatched: &mut usize,
) -> IoUringShutdownDrainStatus {
    for _ in 0..EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS {
        let snapshot = dispatcher.snapshot();
        if snapshot.ring.pending_submissions == 0
            && snapshot.ring.pending_completions == 0
            && snapshot.ring.tracked_operations == 0
        {
            break;
        }

        match dispatcher.wait_and_dispatch(1) {
            Ok(count) => *dispatched += count,
            Err(_) => return IoUringShutdownDrainStatus::TimedOut,
        }

        if dispatcher.is_idle() {
            return IoUringShutdownDrainStatus::Completed;
        }
    }

    if dispatcher.is_idle() {
        IoUringShutdownDrainStatus::Completed
    } else {
        IoUringShutdownDrainStatus::TimedOut
    }
}

pub(super) fn dispatch_available() -> usize {
    CURRENT_IO_URING.with(|current| {
        current.borrow().as_ref().map_or(0, |installed| {
            installed
                .dispatcher
                .borrow_mut()
                .dispatch_available_limit(EXECUTOR_IO_URING_COMPLETION_BUDGET)
        })
    })
}

pub(super) fn completion_budget() -> usize {
    EXECUTOR_IO_URING_COMPLETION_BUDGET
}

pub(super) fn should_wait() -> bool {
    CURRENT_IO_URING.with(|current| {
        current.borrow().as_ref().is_some_and(|installed| {
            let snapshot = installed.dispatcher.borrow().snapshot();
            snapshot.registered_wakers > 0 || snapshot.ring.pending_submissions > 0
        })
    })
}

pub(super) fn completion_fd() -> Option<RawFd> {
    CURRENT_IO_URING.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|installed| installed.dispatcher.borrow().raw_fd())
    })
}

pub(super) fn submit_pending() -> io::Result<u32> {
    CURRENT_IO_URING.with(|current| {
        let Some(dispatcher) = current
            .borrow()
            .as_ref()
            .map(|installed| Rc::clone(&installed.dispatcher))
        else {
            return Ok(0);
        };
        dispatcher.borrow_mut().submit_pending()
    })
}

pub(super) fn snapshot() -> Option<IoUringDispatcherSnapshot> {
    CURRENT_IO_URING.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|installed| installed.dispatcher.borrow().snapshot())
    })
}

fn current_dispatcher() -> Option<SharedDispatcher> {
    CURRENT_IO_URING.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|installed| Rc::clone(&installed.dispatcher))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_dispatcher_reports_timed_out_when_buffered_completion_remains() {
        let Some(ring) = available_io_uring(8).unwrap() else {
            return;
        };
        let mut dispatcher = IoUringDispatcher::new(ring);
        let _operation = dispatcher.ring_mut().queue_nop_operation().unwrap();

        assert_eq!(dispatcher.wait_and_dispatch(1).unwrap(), 1);
        assert_eq!(dispatcher.snapshot().completed_operations, 1);

        let snapshot = shutdown_dispatcher(&mut dispatcher);
        let drain = snapshot
            .shutdown_drain
            .expect("shutdown drain outcome is recorded");

        assert_eq!(drain.status, IoUringShutdownDrainStatus::TimedOut);
        assert_eq!(drain.dispatched, 0);
        assert_eq!(snapshot.completed_operations, 1);
    }
}
