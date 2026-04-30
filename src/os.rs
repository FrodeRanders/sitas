//! Unix runtime backend experiments.
//!
//! This module is the first step outside the pure standard-library runtime.
//! It uses direct Unix FFI for a small reactor wake primitive that works on
//! Linux and macOS: a non-blocking pipe provides the wake source, and `poll(2)`
//! waits for readiness.

use std::fmt;
use std::io;
use std::os::raw::{c_int, c_short, c_void};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

#[cfg(target_os = "linux")]
type Nfds = std::os::raw::c_ulong;
#[cfg(not(target_os = "linux"))]
type Nfds = u32;

const POLLIN: c_short = 0x0001;
const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;

#[cfg(target_os = "linux")]
const O_NONBLOCK: c_int = 0o4000;
#[cfg(not(target_os = "linux"))]
const O_NONBLOCK: c_int = 0x0004;

const EINTR: c_int = 4;
const EAGAIN: c_int = 11;
#[cfg(not(target_os = "linux"))]
const EWOULDBLOCK: c_int = 35;

#[repr(C)]
struct PollFd {
    fd: c_int,
    events: c_short,
    revents: c_short,
}

extern "C" {
    fn close(fd: c_int) -> c_int;
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
    fn pipe(fds: *mut c_int) -> c_int;
    fn poll(fds: *mut PollFd, nfds: Nfds, timeout: c_int) -> c_int;
    fn read(fd: c_int, buffer: *mut c_void, count: usize) -> isize;
    fn write(fd: c_int, buffer: *const c_void, count: usize) -> isize;
}

#[cfg(target_os = "linux")]
extern "C" {
    fn __errno_location() -> *mut c_int;
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
extern "C" {
    fn __error() -> *mut c_int;
}

/// OS-backed reactor wake source.
///
/// `OsReactor` owns the read side of a non-blocking pipe. Cloned [`OsWaker`]
/// values own the write side and can wake a thread blocked in [`OsReactor::wait`].
pub struct OsReactor {
    read_fd: OwnedFd,
    write_fd: Arc<OwnedFd>,
}

impl OsReactor {
    /// Creates a reactor wake source backed by a non-blocking pipe.
    pub fn new() -> io::Result<Self> {
        let (read_fd, write_fd) = create_pipe()?;

        Ok(Self {
            read_fd,
            write_fd: Arc::new(write_fd),
        })
    }

    /// Returns a cloneable waker for this reactor.
    pub fn waker(&self) -> OsWaker {
        OsWaker {
            write_fd: Arc::clone(&self.write_fd),
        }
    }

    /// Waits until the reactor is woken or the optional timeout expires.
    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<OsEvent> {
        let timeout_ms = timeout_to_poll_ms(timeout);

        loop {
            let mut fd = PollFd {
                fd: self.read_fd.raw(),
                events: POLLIN,
                revents: 0,
            };

            // SAFETY: `fd` points to one initialized `PollFd` for the duration
            // of the call, and the raw file descriptor is owned by `self`.
            let result = unsafe { poll(&mut fd, 1, timeout_ms) };
            if result > 0 {
                return Ok(OsEvent {
                    woke: fd.revents & POLLIN != 0 && self.drain_wakes()?,
                });
            }
            if result == 0 {
                return Ok(OsEvent { woke: false });
            }

            let error = last_os_error();
            if error.raw_os_error() == Some(EINTR) {
                continue;
            }
            return Err(error);
        }
    }

    fn drain_wakes(&self) -> io::Result<bool> {
        let mut drained = false;
        let mut buffer = [0u8; 64];

        loop {
            // SAFETY: `buffer` is valid writable memory for `buffer.len()`
            // bytes, and the descriptor is a non-blocking pipe read end.
            let result = unsafe {
                read(
                    self.read_fd.raw(),
                    buffer.as_mut_ptr().cast::<c_void>(),
                    buffer.len(),
                )
            };

            if result > 0 {
                drained = true;
                continue;
            }
            if result == 0 {
                return Ok(drained);
            }

            let error = last_os_error();
            if is_would_block(&error) {
                return Ok(drained);
            }
            match error.raw_os_error() {
                Some(EINTR) => continue,
                _ => return Err(error),
            }
        }
    }
}

impl fmt::Debug for OsReactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsReactor").finish_non_exhaustive()
    }
}

/// Cloneable handle that wakes an [`OsReactor`].
#[derive(Clone)]
pub struct OsWaker {
    write_fd: Arc<OwnedFd>,
}

impl OsWaker {
    /// Wakes the reactor.
    ///
    /// If the pipe is already full, the wake is considered delivered because
    /// the reactor will observe the existing byte.
    pub fn wake(&self) -> io::Result<()> {
        let byte = [1u8; 1];

        loop {
            // SAFETY: `byte` is valid readable memory for one byte, and the
            // descriptor is a non-blocking pipe write end.
            let result = unsafe {
                write(
                    self.write_fd.raw(),
                    byte.as_ptr().cast::<c_void>(),
                    byte.len(),
                )
            };

            if result >= 0 {
                return Ok(());
            }

            let error = last_os_error();
            if is_would_block(&error) {
                return Ok(());
            }
            match error.raw_os_error() {
                Some(EINTR) => continue,
                _ => return Err(error),
            }
        }
    }
}

impl fmt::Debug for OsWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsWaker").finish_non_exhaustive()
    }
}

/// Result of waiting on an [`OsReactor`].
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OsEvent {
    /// Whether the reactor observed and drained a wake.
    pub woke: bool,
}

#[derive(Debug)]
struct OwnedFd {
    fd: RawFd,
}

impl OwnedFd {
    fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    fn raw(&self) -> RawFd {
        self.fd
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        // SAFETY: `fd` is owned by this value and closed at most once here.
        let _ = unsafe { close(self.fd) };
    }
}

fn create_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];

    // SAFETY: `fds` points to two valid `c_int` slots for `pipe` to fill.
    let result = unsafe { pipe(fds.as_mut_ptr()) };
    if result < 0 {
        return Err(last_os_error());
    }

    let read_fd = OwnedFd::new(fds[0]);
    let write_fd = OwnedFd::new(fds[1]);

    set_nonblocking(read_fd.raw())?;
    set_nonblocking(write_fd.raw())?;

    Ok((read_fd, write_fd))
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an open file descriptor and `F_GETFL` does not require
    // the variadic argument.
    let flags = unsafe { fcntl(fd, F_GETFL) };
    if flags < 0 {
        return Err(last_os_error());
    }

    // SAFETY: `fd` is an open file descriptor and `F_SETFL` expects one integer
    // flag argument.
    let result = unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

fn timeout_to_poll_ms(timeout: Option<Duration>) -> c_int {
    match timeout {
        Some(duration) => {
            let millis = duration.as_millis();
            if millis > c_int::MAX as u128 {
                c_int::MAX
            } else {
                millis as c_int
            }
        }
        None => -1,
    }
}

fn last_os_error() -> io::Error {
    io::Error::from_raw_os_error(errno())
}

#[cfg(target_os = "linux")]
fn is_would_block(error: &io::Error) -> bool {
    error.raw_os_error() == Some(EAGAIN)
}

#[cfg(not(target_os = "linux"))]
fn is_would_block(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(EAGAIN) | Some(EWOULDBLOCK))
}

#[cfg(target_os = "linux")]
fn errno() -> c_int {
    // SAFETY: libc exposes a thread-local errno pointer on Linux.
    unsafe { *__errno_location() }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> c_int {
    // SAFETY: libc exposes a thread-local errno pointer on Apple platforms.
    unsafe { *__error() }
}

#[cfg(test)]
mod tests {
    use super::OsReactor;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn wait_times_out_without_wake() {
        let reactor = OsReactor::new().unwrap();

        assert!(!reactor.wait(Some(Duration::from_millis(1))).unwrap().woke);
    }

    #[test]
    fn wake_before_wait_is_observed() {
        let reactor = OsReactor::new().unwrap();
        let waker = reactor.waker();

        waker.wake().unwrap();

        assert!(reactor.wait(Some(Duration::from_secs(1))).unwrap().woke);
    }

    #[test]
    fn wake_from_thread_unblocks_wait() {
        let reactor = OsReactor::new().unwrap();
        let waker = reactor.waker();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            waker.wake().unwrap();
        });

        let started = Instant::now();
        assert!(reactor.wait(Some(Duration::from_secs(1))).unwrap().woke);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn multiple_wakes_are_coalesced_by_drain() {
        let reactor = OsReactor::new().unwrap();
        let waker = reactor.waker();

        waker.wake().unwrap();
        waker.wake().unwrap();

        assert!(reactor.wait(Some(Duration::from_secs(1))).unwrap().woke);
        assert!(!reactor.wait(Some(Duration::from_millis(1))).unwrap().woke);
    }
}
