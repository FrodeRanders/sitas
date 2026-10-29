//! Executor integration for Linux `io_uring`.
//!
//! Installs a thread-local [`IoUringDispatcher`] on each executor,
//! providing `read_at_uring`, `read_exact_at_uring`, and
//! `write_all_at_uring` futures. Completion dispatch shares the executor's
//! idle-wait path but is not yet unified with timers or readiness I/O.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake, Waker};

use crate::os::{
    IoUringDispatcher, IoUringDispatcherSnapshot, IoUringShutdownDrainSnapshot,
    IoUringShutdownDrainStatus, OwnedIoOperation, available_io_uring,
};

use super::current::current_scheduler;
use super::scheduler::Scheduler;
use super::{IoUringExecutorStatus, scheduling_group::ExecutorId};

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
/// First submission requires a sitas executor on Linux with `io_uring`
/// available. The originating dispatcher then owns the buffer until completion;
/// the future may be moved, polled, or dropped on another thread. The origin
/// must continue driving completions. Destroying it fails pending result handles
/// with [`io::ErrorKind::BrokenPipe`] while retaining any in-flight buffer safely.
/// The raw descriptor is borrowed, not owned: keep it open through the operation.
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
    operation: Option<Arc<OwnedIoOperation>>,
}

impl Future for ReadAtUring {
    type Output = io::Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.operation.is_none() {
            this.operation = Some(
                match queue_owned(this.fd, this.offset, &mut this.buffer, false) {
                    Ok(operation) => operation,
                    Err(error) => return Poll::Ready(Err(error)),
                },
            );
        }
        match this.operation.as_ref().unwrap().poll(context) {
            Poll::Ready(result) => {
                this.operation = None;
                Poll::Ready(result.and_then(|(completion, mut buffer)| {
                    let read = completion_result_to_usize(completion.result, "read")?;
                    buffer.truncate(read);
                    Ok(buffer)
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ReadAtUring {
    fn drop(&mut self) {
        if let Some(operation) = self.operation.take() {
            operation.abandon();
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
    operation: Option<Arc<OwnedIoOperation>>,
}

impl Future for WriteAtUring {
    type Output = io::Result<WriteAtUringCompletion>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.operation.is_none() {
            this.operation = Some(
                match queue_owned(this.fd, this.offset, &mut this.buffer, true) {
                    Ok(operation) => operation,
                    Err(error) => return Poll::Ready(Err(error)),
                },
            );
        }
        match this.operation.as_ref().unwrap().poll(context) {
            Poll::Ready(result) => {
                this.operation = None;
                Poll::Ready(result.and_then(|(completion, buffer)| {
                    let bytes = completion_result_to_usize(completion.result, "write")?;
                    Ok(WriteAtUringCompletion { bytes, buffer })
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WriteAtUring {
    fn drop(&mut self) {
        if let Some(operation) = self.operation.take() {
            operation.abandon();
        }
    }
}

struct OwnerWake(Weak<Scheduler>);
impl Wake for OwnerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(scheduler) = self.0.upgrade() {
            scheduler.wake_reactor();
        }
    }
}

fn queue_owned(
    fd: RawFd,
    offset: u64,
    buffer: &mut Option<Vec<u8>>,
    write: bool,
) -> io::Result<Arc<OwnedIoOperation>> {
    let dispatcher = current_dispatcher().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "executor io_uring backend is unavailable",
        )
    })?;
    let waker = Waker::from(Arc::new(OwnerWake(Arc::downgrade(&current_scheduler()))));
    dispatcher.borrow_mut().queue_owned_buffer(
        fd,
        offset,
        buffer.take().expect("buffer exists until submission"),
        write,
        waker,
    )
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

        let dispatcher = match available_io_uring(EXECUTOR_IO_URING_ENTRIES) {
            Ok(Some(ring)) => Some(IoUringDispatcher::new(ring).into_shared()),
            Ok(None) => None,
            Err(error) => panic!("executor io_uring setup failed: {error}"),
        };
        *current.borrow_mut() = dispatcher.map(|dispatcher| CurrentIoUring { owner, dispatcher });
    });
}

pub(super) fn shutdown_current(
    owner: ExecutorId,
) -> (IoUringExecutorStatus, Option<IoUringDispatcherSnapshot>) {
    CURRENT_IO_URING.with(|current| {
        {
            let current_ref = current.borrow();
            let Some(installed) = current_ref.as_ref() else {
                return (IoUringExecutorStatus::Unavailable, None);
            };
            if installed.owner != owner {
                return (IoUringExecutorStatus::Installed, None);
            }

            if installed.dispatcher.borrow().snapshot().registered_wakers > 0 {
                let mut snapshot = installed.dispatcher.borrow().snapshot();
                snapshot.shutdown_drain = Some(IoUringShutdownDrainSnapshot {
                    status: IoUringShutdownDrainStatus::SkippedLiveWakers,
                    max_waits: EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS,
                    dispatched: 0,
                    leaked_buffers: 0,
                });
                return (IoUringExecutorStatus::Installed, Some(snapshot));
            }
        }

        let installed = current.borrow_mut().take().expect("dispatcher exists");
        let mut dispatcher = installed.dispatcher.borrow_mut();
        (
            IoUringExecutorStatus::Shutdown,
            Some(shutdown_dispatcher(&mut dispatcher)),
        )
    })
}

/// Final executor destruction differs from a resumable run_until return:
/// no later run can drive its pending operations. Fail result handles and
/// transfer every still-live buffer through the dispatcher's drain-or-leak path.
pub(super) fn close_current(
    owner: ExecutorId,
) -> (IoUringExecutorStatus, Option<IoUringDispatcherSnapshot>) {
    let installed = CURRENT_IO_URING.with(|current| {
        let mut current = current.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|installed| installed.owner == owner)
        {
            current.take()
        } else {
            None
        }
    });
    let Some(installed) = installed else {
        return (IoUringExecutorStatus::Unavailable, None);
    };
    let mut dispatcher = installed.dispatcher.borrow_mut();
    dispatcher.retire_owned_operations();
    (
        IoUringExecutorStatus::Shutdown,
        Some(shutdown_dispatcher(&mut dispatcher)),
    )
}

pub(super) fn status() -> IoUringExecutorStatus {
    CURRENT_IO_URING.with(|current| {
        if current.borrow().is_some() {
            IoUringExecutorStatus::Installed
        } else {
            IoUringExecutorStatus::Unavailable
        }
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
    // Delegate to the dispatcher's single drain-or-leak routine. The outcome
    // (including any leaked-buffer count) is recorded into the dispatcher and
    // surfaced through its snapshot, so executor teardown, explicit shutdown,
    // and `Drop` all share one implementation.
    let _ = dispatcher.shutdown_drain(EXECUTOR_IO_URING_SHUTDOWN_DRAIN_WAITS);
    dispatcher.snapshot()
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

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::executor::{executor_and_spawner, sleep};
    use std::io::{Read, Write};
    use std::os::unix::{io::AsRawFd, net::UnixStream};
    use std::time::Duration;

    fn suspend<F: Future + Unpin>(executor: &crate::executor::Executor, mut future: F) -> F {
        executor.run_until(std::future::poll_fn(|cx| {
            assert!(Pin::new(&mut future).poll(cx).is_pending());
            Poll::Ready(())
        }));
        future
    }

    #[test]
    fn pending_read_can_be_dropped_on_a_foreign_executor() {
        if available_io_uring(8).unwrap().is_none() {
            return;
        }
        let (reader, _writer) = UnixStream::pair().unwrap();
        let (executor, spawner) = executor_and_spawner();
        drop(spawner);
        let future = suspend(
            &executor,
            read_at_uring(reader.as_raw_fd(), u64::MAX, vec![0; 32]),
        );
        executor.run_until(sleep(Duration::from_millis(1)));
        assert_eq!(
            executor
                .snapshot()
                .io_uring
                .unwrap()
                .ring
                .pending_submissions,
            0
        );
        std::thread::spawn(move || {
            crate::executor::block_on(async move {
                drop(future);
            })
        })
        .join()
        .unwrap();
        executor.run();
        let snapshot = executor.snapshot().io_uring.unwrap();
        assert_eq!(snapshot.registered_wakers, 0);
        assert_eq!(snapshot.ring.tracked_operations, 0);
        assert_eq!(snapshot.deferred_buffers, 0);
    }

    #[test]
    fn completed_read_can_be_consumed_on_another_thread_after_origin_shutdown() {
        if available_io_uring(8).unwrap().is_none() {
            return;
        }
        let (reader, mut writer) = UnixStream::pair().unwrap();
        writer.write_all(b"owned").unwrap();
        let (executor, spawner) = executor_and_spawner();
        drop(spawner);
        let future = suspend(
            &executor,
            read_at_uring(reader.as_raw_fd(), u64::MAX, vec![0; 5]),
        );
        executor.run_until(async {
            crate::executor::timeout(Duration::from_secs(2), async {
                while executor
                    .snapshot()
                    .io_uring
                    .is_some_and(|snapshot| snapshot.registered_wakers > 0)
                {
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        });
        drop(executor);
        let output = std::thread::spawn(move || crate::executor::block_on(future))
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(output, b"owned");
    }

    #[test]
    fn pending_write_can_be_dropped_on_another_thread() {
        if available_io_uring(8).unwrap().is_none() {
            return;
        }
        let (mut reader, writer) = UnixStream::pair().unwrap();
        let (executor, spawner) = executor_and_spawner();
        drop(spawner);
        let future = suspend(
            &executor,
            write_at_uring(writer.as_raw_fd(), u64::MAX, b"owned".to_vec()),
        );
        std::thread::spawn(move || drop(future)).join().unwrap();
        executor.run();
        let snapshot = executor.snapshot().io_uring.unwrap();
        assert_eq!(snapshot.registered_wakers, 0);
        assert_eq!(snapshot.ring.tracked_operations, 0);
        assert_eq!(snapshot.deferred_buffers, 0);
        // A submitted write may win its cancellation race. Closing the writer
        // makes either result observable without waiting on a missing payload.
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty() || bytes == b"owned");
    }

    #[test]
    fn origin_drop_fails_a_live_future_without_releasing_an_inflight_buffer() {
        if available_io_uring(8).unwrap().is_none() {
            return;
        }
        let (reader, _writer) = UnixStream::pair().unwrap();
        let (executor, spawner) = executor_and_spawner();
        drop(spawner);
        let future = suspend(
            &executor,
            read_at_uring(reader.as_raw_fd(), u64::MAX, vec![0; 32]),
        );
        executor.run_until(sleep(Duration::from_millis(1)));
        drop(executor);
        let error = crate::executor::block_on(future).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}
