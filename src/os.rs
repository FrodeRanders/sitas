//! Unix runtime backend experiments.
//!
//! This module is the first step outside the pure standard-library runtime.
//! It uses direct Unix FFI for a small reactor wake primitive and descriptor
//! readiness waiting. A non-blocking pipe provides the wake source. Linux uses
//! `epoll(7)` for readiness waits; other Unix targets currently use `poll(2)`.

#[cfg(target_os = "linux")]
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, TcpStream};
#[cfg(not(target_os = "linux"))]
use std::os::raw::c_short;
use std::os::raw::{c_int, c_void};
use std::os::unix::io::{FromRawFd, RawFd};
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Mutex;
use std::time::Duration;

#[cfg(not(target_os = "linux"))]
type Nfds = u32;
type SockLen = u32;

#[cfg(not(target_os = "linux"))]
const POLLIN: c_short = 0x0001;
#[cfg(not(target_os = "linux"))]
const POLLOUT: c_short = 0x0004;
const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const AF_INET: c_int = 2;
#[cfg(target_os = "linux")]
const AF_INET6: c_int = 10;
#[cfg(not(target_os = "linux"))]
const AF_INET6: c_int = 30;
const SOCK_STREAM: c_int = 1;

#[cfg(target_os = "linux")]
const EPOLLIN: u32 = 0x0001;
#[cfg(target_os = "linux")]
const EPOLLOUT: u32 = 0x0004;
#[cfg(target_os = "linux")]
const EPOLLERR: u32 = 0x0008;
#[cfg(target_os = "linux")]
const EPOLLHUP: u32 = 0x0010;
#[cfg(target_os = "linux")]
const EPOLL_CTL_ADD: c_int = 1;
#[cfg(target_os = "linux")]
const EPOLL_CTL_DEL: c_int = 2;

#[cfg(target_os = "linux")]
const O_NONBLOCK: c_int = 0o4000;
#[cfg(not(target_os = "linux"))]
const O_NONBLOCK: c_int = 0x0004;

const EINTR: c_int = 4;
const EAGAIN: c_int = 11;
#[cfg(target_os = "linux")]
const EINPROGRESS: c_int = 115;
#[cfg(not(target_os = "linux"))]
const EINPROGRESS: c_int = 36;
#[cfg(not(target_os = "linux"))]
const EWOULDBLOCK: c_int = 35;

#[cfg(not(target_os = "linux"))]
#[repr(C)]
struct PollFd {
    fd: c_int,
    events: c_short,
    revents: c_short,
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct EpollEvent {
    events: u32,
    data: u64,
}

#[repr(C)]
struct InAddr {
    s_addr: u32,
}

#[repr(C)]
struct In6Addr {
    s6_addr: [u8; 16],
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct SockAddrIn {
    sin_family: u16,
    sin_port: u16,
    sin_addr: InAddr,
    sin_zero: [u8; 8],
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct SockAddrIn6 {
    sin6_family: u16,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: In6Addr,
    sin6_scope_id: u32,
}

#[cfg(not(target_os = "linux"))]
#[repr(C)]
struct SockAddrIn6 {
    sin6_len: u8,
    sin6_family: u8,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: In6Addr,
    sin6_scope_id: u32,
}

#[cfg(not(target_os = "linux"))]
#[repr(C)]
struct SockAddrIn {
    sin_len: u8,
    sin_family: u8,
    sin_port: u16,
    sin_addr: InAddr,
    sin_zero: [u8; 8],
}

#[cfg(not(target_os = "linux"))]
impl PollFd {
    fn readable(fd: RawFd) -> Self {
        Self {
            fd,
            events: POLLIN,
            revents: 0,
        }
    }

    fn writable(fd: RawFd) -> Self {
        Self {
            fd,
            events: POLLOUT,
            revents: 0,
        }
    }
}

unsafe extern "C" {
    fn close(fd: c_int) -> c_int;
    fn connect(fd: c_int, address: *const c_void, length: SockLen) -> c_int;
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
    fn pipe(fds: *mut c_int) -> c_int;
    #[cfg(not(target_os = "linux"))]
    fn poll(fds: *mut PollFd, nfds: Nfds, timeout: c_int) -> c_int;
    fn read(fd: c_int, buffer: *mut c_void, count: usize) -> isize;
    fn socket(domain: c_int, socket_type: c_int, protocol: c_int) -> c_int;
    fn write(fd: c_int, buffer: *const c_void, count: usize) -> isize;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn __errno_location() -> *mut c_int;
    fn epoll_create1(flags: c_int) -> c_int;
    fn epoll_ctl(epoll_fd: c_int, op: c_int, fd: c_int, event: *mut EpollEvent) -> c_int;
    fn epoll_wait(
        epoll_fd: c_int,
        events: *mut EpollEvent,
        maxevents: c_int,
        timeout: c_int,
    ) -> c_int;
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe extern "C" {
    fn __error() -> *mut c_int;
}

/// OS-backed reactor wake source.
///
/// `OsReactor` owns the read side of a non-blocking pipe. Cloned [`OsWaker`]
/// values own the write side and can wake a thread blocked in [`OsReactor::wait`].
pub struct OsReactor {
    read_fd: OwnedFd,
    write_fd: Arc<OwnedFd>,
    #[cfg(target_os = "linux")]
    epoll_fd: OwnedFd,
    #[cfg(target_os = "linux")]
    epoll_registry: Mutex<EpollRegistry>,
}

impl OsReactor {
    /// Creates a reactor wake source backed by a non-blocking pipe.
    pub fn new() -> io::Result<Self> {
        let (read_fd, write_fd) = create_pipe()?;
        #[cfg(target_os = "linux")]
        let epoll_fd = create_epoll()?;
        #[cfg(target_os = "linux")]
        let epoll_registry = Mutex::new(EpollRegistry::new());
        #[cfg(target_os = "linux")]
        register_epoll_fd(epoll_fd.raw(), read_fd.raw(), EPOLLIN, 0)?;
        #[cfg(target_os = "linux")]
        epoll_registry
            .lock()
            .expect("epoll registry mutex poisoned")
            .insert_wake(read_fd.raw());

        Ok(Self {
            read_fd,
            write_fd: Arc::new(write_fd),
            #[cfg(target_os = "linux")]
            epoll_fd,
            #[cfg(target_os = "linux")]
            epoll_registry,
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
        self.wait_io(&[], &[], timeout)
    }

    /// Waits until the reactor is woken, one of `read_fds` becomes readable,
    /// or the optional timeout expires.
    pub fn wait_readable(
        &self,
        read_fds: &[RawFd],
        timeout: Option<Duration>,
    ) -> io::Result<OsEvent> {
        self.wait_io(read_fds, &[], timeout)
    }

    /// Waits until the reactor is woken, one of `write_fds` becomes writable,
    /// or the optional timeout expires.
    pub fn wait_writable(
        &self,
        write_fds: &[RawFd],
        timeout: Option<Duration>,
    ) -> io::Result<OsEvent> {
        self.wait_io(&[], write_fds, timeout)
    }

    /// Waits until the reactor is woken, a read descriptor becomes readable, a
    /// write descriptor becomes writable, or the optional timeout expires.
    pub fn wait_io(
        &self,
        read_fds: &[RawFd],
        write_fds: &[RawFd],
        timeout: Option<Duration>,
    ) -> io::Result<OsEvent> {
        self.wait_io_backend(read_fds, write_fds, timeout)
    }

    #[cfg(target_os = "linux")]
    fn wait_io_backend(
        &self,
        read_fds: &[RawFd],
        write_fds: &[RawFd],
        timeout: Option<Duration>,
    ) -> io::Result<OsEvent> {
        let timeout_ms = timeout_to_wait_ms(timeout);
        let interests = EpollInterests::new(read_fds, write_fds);
        let registration =
            register_epoll_interests(self.epoll_fd.raw(), &self.epoll_registry, &interests)?;

        loop {
            let max_events = interests.len() + 1;
            let mut events = vec![EpollEvent { events: 0, data: 0 }; max_events];

            // SAFETY: `events` points to initialized storage for `max_events`
            // event values, and the reactor-owned epoll descriptor remains
            // open for the call.
            let result = unsafe {
                epoll_wait(
                    self.epoll_fd.raw(),
                    events.as_mut_ptr(),
                    max_events as c_int,
                    timeout_ms,
                )
            };
            if result > 0 {
                let mut woke = false;
                let mut readable = Vec::new();
                let mut writable = Vec::new();

                for event in events.iter().take(result as usize) {
                    if event.data == 0 {
                        woke = event.events & EPOLLIN != 0 && self.drain_wakes()?;
                        continue;
                    }
                    let interest = registration
                        .interest(event.data)
                        .expect("epoll returned an unknown interest index");
                    if interest.read && event.events & (EPOLLIN | EPOLLERR | EPOLLHUP) != 0 {
                        readable.push(interest.fd);
                    }
                    if interest.write && event.events & (EPOLLOUT | EPOLLERR | EPOLLHUP) != 0 {
                        writable.push(interest.fd);
                    }
                }

                return Ok(OsEvent {
                    woke,
                    readable,
                    writable,
                });
            }
            if result == 0 {
                return Ok(OsEvent {
                    woke: false,
                    readable: Vec::new(),
                    writable: Vec::new(),
                });
            }

            let error = last_os_error();
            if error.raw_os_error() == Some(EINTR) {
                continue;
            }
            return Err(error);
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn wait_io_backend(
        &self,
        read_fds: &[RawFd],
        write_fds: &[RawFd],
        timeout: Option<Duration>,
    ) -> io::Result<OsEvent> {
        let timeout_ms = timeout_to_wait_ms(timeout);

        loop {
            let mut fds = Vec::with_capacity(read_fds.len() + write_fds.len() + 1);
            fds.push(PollFd::readable(self.read_fd.raw()));
            fds.extend(read_fds.iter().copied().map(PollFd::readable));
            fds.extend(write_fds.iter().copied().map(PollFd::writable));

            // SAFETY: `fds` points to initialized `PollFd` values for the
            // duration of the call, and all raw descriptors are borrowed only
            // for this wait operation.
            let result = unsafe { poll(fds.as_mut_ptr(), fds.len() as Nfds, timeout_ms) };
            if result > 0 {
                let woke = fds[0].revents & POLLIN != 0 && self.drain_wakes()?;
                let readable = fds
                    .iter()
                    .skip(1)
                    .take(read_fds.len())
                    .filter(|fd| fd.revents & POLLIN != 0)
                    .map(|fd| fd.fd)
                    .collect();
                let writable = fds
                    .iter()
                    .skip(1 + read_fds.len())
                    .filter(|fd| fd.revents & POLLOUT != 0)
                    .map(|fd| fd.fd)
                    .collect();

                return Ok(OsEvent {
                    woke,
                    readable,
                    writable,
                });
            }
            if result == 0 {
                return Ok(OsEvent {
                    woke: false,
                    readable: Vec::new(),
                    writable: Vec::new(),
                });
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

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
struct EpollInterest {
    fd: RawFd,
    read: bool,
    write: bool,
}

#[cfg(target_os = "linux")]
impl EpollInterest {
    fn events(&self) -> u32 {
        let mut events = 0;
        if self.read {
            events |= EPOLLIN;
        }
        if self.write {
            events |= EPOLLOUT;
        }
        events
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct EpollInterests {
    interests: Vec<EpollInterest>,
}

#[cfg(target_os = "linux")]
impl EpollInterests {
    fn new(read_fds: &[RawFd], write_fds: &[RawFd]) -> Self {
        let mut interests = Vec::with_capacity(read_fds.len() + write_fds.len());

        for fd in read_fds {
            add_epoll_interest(&mut interests, *fd, true, false);
        }
        for fd in write_fds {
            add_epoll_interest(&mut interests, *fd, false, true);
        }

        Self { interests }
    }

    fn iter(&self) -> impl Iterator<Item = &EpollInterest> {
        self.interests.iter()
    }

    fn len(&self) -> usize {
        self.interests.len()
    }
}

#[cfg(target_os = "linux")]
fn add_epoll_interest(interests: &mut Vec<EpollInterest>, fd: RawFd, read: bool, write: bool) {
    if let Some(interest) = interests.iter_mut().find(|interest| interest.fd == fd) {
        interest.read |= read;
        interest.write |= write;
    } else {
        interests.push(EpollInterest { fd, read, write });
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct EpollRegistry {
    next_token: u64,
    interests: HashMap<u64, EpollInterest>,
}

#[cfg(target_os = "linux")]
impl EpollRegistry {
    fn new() -> Self {
        Self {
            next_token: 1,
            interests: HashMap::new(),
        }
    }

    fn insert_wake(&mut self, fd: RawFd) {
        self.interests.insert(
            0,
            EpollInterest {
                fd,
                read: true,
                write: false,
            },
        );
    }

    fn insert_temporary(&mut self, interest: EpollInterest) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        self.interests.insert(token, interest);
        token
    }

    fn remove(&mut self, token: u64) -> Option<EpollInterest> {
        self.interests.remove(&token)
    }

    fn get(&self, token: u64) -> Option<EpollInterest> {
        self.interests.get(&token).copied()
    }
}

#[cfg(target_os = "linux")]
struct EpollRegistration<'a> {
    epoll_fd: RawFd,
    registry: &'a Mutex<EpollRegistry>,
    tokens: Vec<u64>,
}

#[cfg(target_os = "linux")]
impl Drop for EpollRegistration<'_> {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().expect("epoll registry mutex poisoned");

        for token in self.tokens.drain(..) {
            let Some(interest) = registry.remove(token) else {
                continue;
            };
            // SAFETY: `epoll_fd` is owned by the reactor and `fd` was
            // previously registered by this guard. The event pointer is unused
            // for `EPOLL_CTL_DEL` on Linux.
            let _ = unsafe {
                epoll_ctl(
                    self.epoll_fd,
                    EPOLL_CTL_DEL,
                    interest.fd,
                    std::ptr::null_mut::<EpollEvent>(),
                )
            };
        }
    }
}

#[cfg(target_os = "linux")]
impl EpollRegistration<'_> {
    fn interest(&self, token: u64) -> Option<EpollInterest> {
        self.registry
            .lock()
            .expect("epoll registry mutex poisoned")
            .get(token)
    }
}

#[cfg(target_os = "linux")]
fn register_epoll_interests<'a>(
    epoll_fd: RawFd,
    registry: &'a Mutex<EpollRegistry>,
    interests: &EpollInterests,
) -> io::Result<EpollRegistration<'a>> {
    let mut registration = EpollRegistration {
        epoll_fd,
        registry,
        tokens: Vec::with_capacity(interests.len()),
    };

    for interest in interests.iter().copied() {
        let token = {
            let mut registry = registry.lock().expect("epoll registry mutex poisoned");
            registry.insert_temporary(interest)
        };

        if let Err(error) = register_epoll_fd(epoll_fd, interest.fd, interest.events(), token) {
            registry
                .lock()
                .expect("epoll registry mutex poisoned")
                .remove(token);
            return Err(error);
        }

        registration.tokens.push(token);
    }

    Ok(registration)
}

#[cfg(target_os = "linux")]
fn register_epoll_fd(epoll_fd: RawFd, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
    let mut event = EpollEvent { events, data };

    // SAFETY: `epoll_fd` is an open epoll descriptor, `fd` is borrowed for
    // readiness observation, and `event` points to an initialized event value
    // for the duration of the call.
    let result = unsafe { epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &mut event) };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

/// Result of waiting on an [`OsReactor`].
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OsEvent {
    /// Whether the reactor observed and drained a wake.
    pub woke: bool,
    /// File descriptors that were readable when the reactor returned.
    pub readable: Vec<RawFd>,
    /// File descriptors that were writable when the reactor returned.
    pub writable: Vec<RawFd>,
}

/// Starts a non-blocking TCP connection.
///
/// If the connection is still in progress, the returned stream should be
/// awaited for writability and then checked with `TcpStream::take_error`.
pub fn tcp_connect_start(address: SocketAddr) -> io::Result<TcpStream> {
    match address {
        SocketAddr::V4(address) => tcp_connect_start_v4(address),
        SocketAddr::V6(address) => tcp_connect_start_v6(address),
    }
}

fn tcp_connect_start_v4(address: SocketAddrV4) -> io::Result<TcpStream> {
    let socket_address = socket_addr_v4(address);
    tcp_connect_start_with_address(
        AF_INET,
        (&socket_address as *const SockAddrIn).cast::<c_void>(),
        std::mem::size_of::<SockAddrIn>() as SockLen,
    )
}

fn tcp_connect_start_v6(address: SocketAddrV6) -> io::Result<TcpStream> {
    let socket_address = socket_addr_v6(address);
    tcp_connect_start_with_address(
        AF_INET6,
        (&socket_address as *const SockAddrIn6).cast::<c_void>(),
        std::mem::size_of::<SockAddrIn6>() as SockLen,
    )
}

fn tcp_connect_start_with_address(
    address_family: c_int,
    socket_address: *const c_void,
    socket_address_len: SockLen,
) -> io::Result<TcpStream> {
    // SAFETY: `socket` is called with constant address family/type values and
    // no borrowed memory.
    let fd = unsafe { socket(address_family, SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(last_os_error());
    }

    let fd = OwnedFd::new(fd);
    set_nonblocking(fd.raw())?;

    // SAFETY: `socket_address` points to a properly initialized socket address
    // whose pointer is valid for the duration of the call.
    let result = unsafe { connect(fd.raw(), socket_address, socket_address_len) };

    if result == 0 {
        return Ok(fd.into_tcp_stream());
    }

    let error = last_os_error();
    if error.raw_os_error() == Some(EINPROGRESS) || is_would_block(&error) {
        return Ok(fd.into_tcp_stream());
    }

    Err(error)
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

    fn into_tcp_stream(self) -> TcpStream {
        let fd = self.fd;
        std::mem::forget(self);
        // SAFETY: `fd` is an owned TCP socket descriptor and ownership is
        // transferred to `TcpStream`.
        unsafe { TcpStream::from_raw_fd(fd) }
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

#[cfg(target_os = "linux")]
fn create_epoll() -> io::Result<OwnedFd> {
    // SAFETY: `epoll_create1` is called with no flags and does not borrow
    // memory from Rust.
    let fd = unsafe { epoll_create1(0) };
    if fd < 0 {
        Err(last_os_error())
    } else {
        Ok(OwnedFd::new(fd))
    }
}

#[cfg(target_os = "linux")]
fn socket_addr_v4(address: SocketAddrV4) -> SockAddrIn {
    SockAddrIn {
        sin_family: AF_INET as u16,
        sin_port: address.port().to_be(),
        sin_addr: InAddr {
            s_addr: u32::from_be_bytes(address.ip().octets()).to_be(),
        },
        sin_zero: [0; 8],
    }
}

#[cfg(not(target_os = "linux"))]
fn socket_addr_v4(address: SocketAddrV4) -> SockAddrIn {
    SockAddrIn {
        sin_len: std::mem::size_of::<SockAddrIn>() as u8,
        sin_family: AF_INET as u8,
        sin_port: address.port().to_be(),
        sin_addr: InAddr {
            s_addr: u32::from_be_bytes(address.ip().octets()).to_be(),
        },
        sin_zero: [0; 8],
    }
}

#[cfg(target_os = "linux")]
fn socket_addr_v6(address: SocketAddrV6) -> SockAddrIn6 {
    SockAddrIn6 {
        sin6_family: AF_INET6 as u16,
        sin6_port: address.port().to_be(),
        sin6_flowinfo: address.flowinfo(),
        sin6_addr: In6Addr {
            s6_addr: address.ip().octets(),
        },
        sin6_scope_id: address.scope_id(),
    }
}

#[cfg(not(target_os = "linux"))]
fn socket_addr_v6(address: SocketAddrV6) -> SockAddrIn6 {
    SockAddrIn6 {
        sin6_len: std::mem::size_of::<SockAddrIn6>() as u8,
        sin6_family: AF_INET6 as u8,
        sin6_port: address.port().to_be(),
        sin6_flowinfo: address.flowinfo(),
        sin6_addr: In6Addr {
            s6_addr: address.ip().octets(),
        },
        sin6_scope_id: address.scope_id(),
    }
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

fn timeout_to_wait_ms(timeout: Option<Duration>) -> c_int {
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
    use super::{OsReactor, create_pipe};
    use std::os::raw::c_void;
    use std::os::unix::io::RawFd;
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

    #[test]
    fn wait_readable_times_out_without_fd_readiness() {
        let reactor = OsReactor::new().unwrap();
        let (read_fd, _write_fd) = create_pipe().unwrap();

        let event = reactor
            .wait_readable(&[read_fd.raw()], Some(Duration::from_millis(1)))
            .unwrap();

        assert!(!event.woke);
        assert!(event.readable.is_empty());
        assert!(event.writable.is_empty());
    }

    #[test]
    fn wait_readable_reports_external_fd_readiness() {
        let reactor = OsReactor::new().unwrap();
        let (read_fd, write_fd) = create_pipe().unwrap();

        write_one(write_fd.raw());

        let event = reactor
            .wait_readable(&[read_fd.raw()], Some(Duration::from_secs(1)))
            .unwrap();

        assert!(!event.woke);
        assert_eq!(event.readable, vec![read_fd.raw()]);
        assert!(event.writable.is_empty());
    }

    #[test]
    fn wait_readable_reports_wake_and_fd_readiness_together() {
        let reactor = OsReactor::new().unwrap();
        let waker = reactor.waker();
        let (read_fd, write_fd) = create_pipe().unwrap();

        write_one(write_fd.raw());
        waker.wake().unwrap();

        let event = reactor
            .wait_readable(&[read_fd.raw()], Some(Duration::from_secs(1)))
            .unwrap();

        assert!(event.woke);
        assert_eq!(event.readable, vec![read_fd.raw()]);
        assert!(event.writable.is_empty());
    }

    #[test]
    fn wait_writable_reports_external_fd_readiness() {
        let reactor = OsReactor::new().unwrap();
        let (_read_fd, write_fd) = create_pipe().unwrap();

        let event = reactor
            .wait_writable(&[write_fd.raw()], Some(Duration::from_secs(1)))
            .unwrap();

        assert!(!event.woke);
        assert!(event.readable.is_empty());
        assert_eq!(event.writable, vec![write_fd.raw()]);
    }

    #[test]
    fn wait_io_reports_readable_and_writable_fds_together() {
        let reactor = OsReactor::new().unwrap();
        let (read_fd, first_write_fd) = create_pipe().unwrap();
        let (_unused_read_fd, second_write_fd) = create_pipe().unwrap();

        write_one(first_write_fd.raw());

        let event = reactor
            .wait_io(
                &[read_fd.raw()],
                &[second_write_fd.raw()],
                Some(Duration::from_secs(1)),
            )
            .unwrap();

        assert!(!event.woke);
        assert_eq!(event.readable, vec![read_fd.raw()]);
        assert_eq!(event.writable, vec![second_write_fd.raw()]);
    }

    fn write_one(fd: RawFd) {
        let byte = [1u8; 1];
        // SAFETY: `byte` is valid readable memory for one byte, and tests pass
        // an open non-blocking pipe write descriptor.
        let result = unsafe { super::write(fd, byte.as_ptr().cast::<c_void>(), byte.len()) };
        assert_eq!(result, 1);
    }
}
