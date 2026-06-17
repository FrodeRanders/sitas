//! Network-facing sharded TCP service.
//!
//! This module integrates the executor-layer TCP helpers with the
//! shard-per-thread model. [`ShardedTcpServer`] binds a listening socket and
//! routes accepted connections to shards based on a configurable placement
//! strategy (hash of remote address by default).
//!
//! On Linux with `SO_REUSEPORT`, every shard binds its own listener and the
//! kernel distributes connections across shards. On other platforms, a single
//! accept shard distributes connections to worker shards via the submitter.
//!
//! This module uses direct Unix FFI following the same pattern as the `os`
//! module to keep the project dependency-free.

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::raw::c_int;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::executor::{RaceOutput, StopSource, accept_async, race, stop_pair};
use crate::shard::ShardId;
use crate::sharded_executor::{ShardedSpawnError, ShardedSubmitter};

// FFI declarations (same pattern as src/os.rs)
#[allow(dead_code)]
const AF_INET: c_int = 2;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
const AF_INET6: c_int = 10;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const AF_INET6: c_int = 30;
#[allow(dead_code)]
const SOCK_STREAM: c_int = 1;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
const SOCK_CLOEXEC: c_int = 0o2000000;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const SOCK_CLOEXEC: c_int = 0;
#[allow(dead_code)]
#[cfg(target_os = "linux")]
const SOL_SOCKET: c_int = 1;
#[cfg(not(target_os = "linux"))]
const SOL_SOCKET: c_int = 0xffff;
#[allow(dead_code)]
#[cfg(target_os = "linux")]
const SO_REUSEADDR: c_int = 2;
#[cfg(not(target_os = "linux"))]
const SO_REUSEADDR: c_int = 0x0004;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
const SO_REUSEPORT: c_int = 15;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const SO_REUSEPORT: c_int = 0x0200;
#[allow(dead_code)]
const F_GETFL: c_int = 3;
#[allow(dead_code)]
const F_SETFL: c_int = 4;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const F_SETFD: c_int = 2;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const FD_CLOEXEC: c_int = 1;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
const O_NONBLOCK: c_int = 0o4000;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const O_NONBLOCK: c_int = 0x0004;

#[allow(dead_code)]
type SockLen = u32;

#[allow(dead_code)]
#[repr(C)]
struct InAddr {
    s_addr: u32,
}

#[cfg(target_os = "linux")]
#[allow(dead_code)]
#[repr(C)]
struct SockAddrIn {
    sin_family: u16,
    sin_port: u16,
    sin_addr: InAddr,
    sin_zero: [u8; 8],
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
#[repr(C)]
struct SockAddrIn {
    sin_len: u8,
    sin_family: u8,
    sin_port: u16,
    sin_addr: InAddr,
    sin_zero: [u8; 8],
}

#[allow(dead_code)]
#[repr(C)]
struct In6Addr {
    s6_addr: [u8; 16],
}

#[cfg(target_os = "linux")]
#[allow(dead_code)]
#[repr(C)]
struct SockAddrIn6 {
    sin6_family: u16,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: In6Addr,
    sin6_scope_id: u32,
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
#[repr(C)]
struct SockAddrIn6 {
    sin6_len: u8,
    sin6_family: u8,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: In6Addr,
    sin6_scope_id: u32,
}

#[allow(dead_code)]
unsafe extern "C" {
    fn bind(fd: c_int, address: *const SockAddrIn, length: SockLen) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
    fn listen(fd: c_int, backlog: c_int) -> c_int;
    fn setsockopt(
        fd: c_int,
        level: c_int,
        option_name: c_int,
        option_value: *const c_int,
        option_len: SockLen,
    ) -> c_int;
    fn socket(domain: c_int, socket_type: c_int, protocol: c_int) -> c_int;
}

/// A TCP connection routed to a specific shard.
#[derive(Debug)]
pub struct ShardedTcpConnection {
    /// The accepted TCP stream.
    pub stream: TcpStream,
    /// The remote address of the peer.
    pub remote_addr: SocketAddr,
    /// The shard this connection was routed to.
    pub shard_id: ShardId,
}

/// Configuration for a sharded TCP server.
#[derive(Debug, Clone)]
pub struct ShardedTcpConfig {
    /// Maximum concurrent connections per shard.
    pub max_connections_per_shard: usize,
    /// Whether to use `SO_REUSEPORT` (Linux only, enables per-shard accept).
    pub reuse_port: bool,
    /// Connection backlog.
    pub backlog: u32,
}

impl Default for ShardedTcpConfig {
    fn default() -> Self {
        Self {
            max_connections_per_shard: 128,
            reuse_port: true,
            backlog: 128,
        }
    }
}

impl ShardedTcpConfig {
    /// Creates a new config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum concurrent connections per shard.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections_per_shard = max;
        self
    }

    /// Sets whether to use `SO_REUSEPORT`.
    pub fn with_reuse_port(mut self, reuse: bool) -> Self {
        self.reuse_port = reuse;
        self
    }

    /// Sets the TCP backlog.
    pub fn with_backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }
}

/// Error returned when a sharded TCP server cannot be started.
#[derive(Debug)]
pub enum ShardedTcpStartError {
    /// The configuration is invalid.
    InvalidConfig(&'static str),
    /// A listener could not be created for the accept path.
    Listen {
        /// The shard that would own this listener, or `None` for the
        /// single-accept path.
        shard_id: Option<ShardId>,
        /// The underlying OS error.
        source: io::Error,
    },
    /// The accept task could not be submitted to a shard executor.
    Spawn(ShardedSpawnError),
}

impl fmt::Display for ShardedTcpStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid TCP server config: {message}"),
            Self::Listen {
                shard_id: Some(shard_id),
                source,
            } => write!(
                f,
                "failed to create TCP listener for shard {}: {source}",
                shard_id.0
            ),
            Self::Listen {
                shard_id: None,
                source,
            } => write!(f, "failed to create TCP listener: {source}"),
            Self::Spawn(error) => write!(f, "{error}"),
        }
    }
}

impl Error for ShardedTcpStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Listen { source, .. } => Some(source),
            Self::Spawn(error) => Some(error),
            Self::InvalidConfig(_) => None,
        }
    }
}

impl From<ShardedSpawnError> for ShardedTcpStartError {
    fn from(error: ShardedSpawnError) -> Self {
        Self::Spawn(error)
    }
}

/// A sharded TCP server.
///
/// Binds a TCP listener and spawns one accept-loop task per shard. Accepted
/// connections are routed to their owning shard's handler.
pub struct ShardedTcpServer {
    bind_addr: SocketAddr,
    #[allow(dead_code)]
    config: ShardedTcpConfig,
}

/// Handle for a running [`ShardedTcpServer`].
#[derive(Debug, Clone)]
#[must_use = "dropping the handle does not stop the server; call stop first"]
pub struct ShardedTcpServerHandle {
    stop_source: StopSource,
}

impl ShardedTcpServerHandle {
    /// Requests all accept loops to stop.
    pub fn stop(&self) -> bool {
        self.stop_source.stop()
    }

    /// Returns whether stop has already been requested.
    pub fn is_stopped(&self) -> bool {
        self.stop_source.is_stopped()
    }
}

impl ShardedTcpServer {
    /// Creates a new server that binds to `bind_addr`.
    pub fn new(bind_addr: SocketAddr, config: ShardedTcpConfig) -> Self {
        Self { bind_addr, config }
    }

    /// Starts the server, spawning one accept-loop task on each shard.
    ///
    /// On Linux with `SO_REUSEPORT`, each shard binds its own socket. On other
    /// platforms, only shard 0 listens and distributes connections.
    ///
    /// `handle_connection` receives the connection and a [`ShardedSubmitter`]
    /// clone for spawning handler tasks. The handler runs on the same shard
    /// that received the connection.
    pub fn start<MakeHandler, HandlerFut>(
        &self,
        submitter: &ShardedSubmitter,
        make_handler: MakeHandler,
    ) -> Result<ShardedTcpServerHandle, ShardedTcpStartError>
    where
        MakeHandler: Fn(ShardedTcpConnection, ShardedSubmitter) -> HandlerFut
            + Send
            + Sync
            + Clone
            + 'static,
        HandlerFut: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.config.max_connections_per_shard == 0 {
            return Err(ShardedTcpStartError::InvalidConfig(
                "max_connections_per_shard must be greater than zero",
            ));
        }

        #[cfg(target_os = "linux")]
        {
            if self.config.reuse_port {
                self.start_with_reuseport(submitter, make_handler)
            } else {
                self.start_with_single_accept(submitter, make_handler)
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.start_with_single_accept(submitter, make_handler)
        }
    }

    #[cfg(target_os = "linux")]
    fn start_with_reuseport<MakeHandler, HandlerFut>(
        &self,
        submitter: &ShardedSubmitter,
        make_handler: MakeHandler,
    ) -> Result<ShardedTcpServerHandle, ShardedTcpStartError>
    where
        MakeHandler: Fn(ShardedTcpConnection, ShardedSubmitter) -> HandlerFut
            + Send
            + Sync
            + Clone
            + 'static,
        HandlerFut: std::future::Future<Output = ()> + Send + 'static,
    {
        let backlog = self.config.backlog;
        let max_connections_per_shard = self.config.max_connections_per_shard;
        let connection_counts = connection_counts(submitter.shard_count());
        let (stop_source, stop_token) = stop_pair();
        let mut listener_addr = self.bind_addr;
        let mut listeners = Vec::with_capacity(submitter.shard_count());

        for shard_idx in 0..submitter.shard_count() {
            let shard_id = ShardId(shard_idx);
            let listener = create_listener(listener_addr, backlog, true).map_err(|source| {
                ShardedTcpStartError::Listen {
                    shard_id: Some(shard_id),
                    source,
                }
            })?;
            if shard_idx == 0 && self.bind_addr.port() == 0 {
                listener_addr =
                    listener
                        .local_addr()
                        .map_err(|source| ShardedTcpStartError::Listen {
                            shard_id: Some(shard_id),
                            source,
                        })?;
            }
            listeners.push((shard_idx, shard_id, listener));
        }

        for (shard_idx, shard_id, listener) in listeners {
            let make_handler = make_handler.clone();
            let shard_submitter = submitter.clone();
            let accept_stop = stop_token.clone();
            let connection_counts = Arc::clone(&connection_counts);

            if let Err(error) = submitter.submit_named_to(
                shard_id,
                format!("tcp-accept-{}", shard_idx),
                async move {
                    loop {
                        match race(accept_async(&listener), accept_stop.clone()).await {
                            RaceOutput::First(Ok((stream, remote_addr))) => {
                                let Some(permit) = ConnectionPermit::try_acquire(
                                    Arc::clone(&connection_counts),
                                    shard_id,
                                    max_connections_per_shard,
                                ) else {
                                    continue;
                                };
                                let conn = ShardedTcpConnection {
                                    stream,
                                    remote_addr,
                                    shard_id,
                                };

                                if let Err(error) = shard_submitter.submit_to(
                                    shard_id,
                                    run_handler_with_permit(
                                        make_handler(conn, shard_submitter.clone()),
                                        permit,
                                    ),
                                ) {
                                    eprintln!(
                                        "[sitas-tcp] shard {} handler submit failed: {}",
                                        shard_idx, error
                                    );
                                }
                            }
                            RaceOutput::First(Err(e)) => {
                                eprintln!("[sitas-tcp] shard {} accept error: {}", shard_idx, e);
                                break;
                            }
                            RaceOutput::Second(()) => break,
                        }
                    }
                },
            ) {
                stop_source.stop();
                return Err(error.into());
            }
        }

        Ok(ShardedTcpServerHandle { stop_source })
    }

    fn start_with_single_accept<MakeHandler, HandlerFut>(
        &self,
        submitter: &ShardedSubmitter,
        make_handler: MakeHandler,
    ) -> Result<ShardedTcpServerHandle, ShardedTcpStartError>
    where
        MakeHandler: Fn(ShardedTcpConnection, ShardedSubmitter) -> HandlerFut
            + Send
            + Sync
            + Clone
            + 'static,
        HandlerFut: std::future::Future<Output = ()> + Send + 'static,
    {
        let bind_addr = self.bind_addr;
        let shard_count = submitter.shard_count();
        let shard_submitter = submitter.clone();
        let max_connections_per_shard = self.config.max_connections_per_shard;
        let connection_counts = connection_counts(shard_count);
        let (stop_source, stop_token) = stop_pair();
        let listener = create_single_listener(bind_addr)?;

        if let Err(error) = submitter.submit_named_to(ShardId(0), "tcp-accept-0", async move {
            loop {
                match race(accept_async(&listener), stop_token.clone()).await {
                    RaceOutput::First(Ok((stream, remote_addr))) => {
                        let target_shard = hash_addr_to_shard(&remote_addr, shard_count);
                        let Some(permit) = ConnectionPermit::try_acquire(
                            Arc::clone(&connection_counts),
                            target_shard,
                            max_connections_per_shard,
                        ) else {
                            continue;
                        };
                        let conn = ShardedTcpConnection {
                            stream,
                            remote_addr,
                            shard_id: target_shard,
                        };
                        let handler = make_handler(conn, shard_submitter.clone());
                        if let Err(error) = shard_submitter
                            .submit_to(target_shard, run_handler_with_permit(handler, permit))
                        {
                            eprintln!(
                                "[sitas-tcp] shard {} handler submit failed: {}",
                                target_shard.0, error
                            );
                        }
                    }
                    RaceOutput::First(Err(e)) => {
                        eprintln!("[sitas-tcp] accept error: {}", e);
                        break;
                    }
                    RaceOutput::Second(()) => break,
                }
            }
        }) {
            stop_source.stop();
            return Err(error.into());
        }

        Ok(ShardedTcpServerHandle { stop_source })
    }
}

#[derive(Debug)]
struct ConnectionPermit {
    counts: Arc<Vec<AtomicUsize>>,
    shard_id: ShardId,
}

impl ConnectionPermit {
    fn try_acquire(counts: Arc<Vec<AtomicUsize>>, shard_id: ShardId, limit: usize) -> Option<Self> {
        let counter = counts.get(shard_id.0)?;
        loop {
            let current = counter.load(Ordering::Acquire);
            if current >= limit {
                return None;
            }
            if counter
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(Self { counts, shard_id });
            }
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        if let Some(counter) = self.counts.get(self.shard_id.0) {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

async fn run_handler_with_permit<F>(future: F, _permit: ConnectionPermit)
where
    F: std::future::Future<Output = ()>,
{
    future.await;
}

fn connection_counts(shard_count: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..shard_count).map(|_| AtomicUsize::new(0)).collect())
}

fn create_single_listener(addr: SocketAddr) -> Result<TcpListener, ShardedTcpStartError> {
    let listener = TcpListener::bind(addr).map_err(|source| ShardedTcpStartError::Listen {
        shard_id: None,
        source,
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| ShardedTcpStartError::Listen {
            shard_id: None,
            source,
        })?;
    Ok(listener)
}

#[allow(dead_code)]
fn create_listener(addr: SocketAddr, backlog: u32, reuse_port: bool) -> io::Result<TcpListener> {
    let backlog: c_int = backlog.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP backlog does not fit platform c_int",
        )
    })?;

    let domain = match addr {
        SocketAddr::V4(_) => AF_INET,
        SocketAddr::V6(_) => AF_INET6,
    };

    #[cfg(target_os = "linux")]
    let sock_type = SOCK_STREAM | SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let sock_type = SOCK_STREAM;

    // SAFETY: `socket` is called with constant AF_INET/AF_INET6 domain,
    // SOCK_STREAM type (and SOCK_CLOEXEC on Linux), and protocol 0.
    // Returns a valid descriptor or -1, checked immediately below.
    let fd = unsafe { socket(domain, sock_type, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `fd` is an open socket descriptor from `socket()` above.
    // F_GETFL reads flags and F_SETFL writes O_NONBLOCK. All fds and flag
    // values are valid; return values are checked immediately.
    let flags = unsafe { fcntl(fd, F_GETFL, 0) };
    if flags < 0 {
        close_owned_fd(fd);
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is open and `flags | O_NONBLOCK` is a valid flag set
    // derived from the descriptor's current status flags.
    if unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) } < 0 {
        close_owned_fd(fd);
        return Err(io::Error::last_os_error());
    }

    #[cfg(not(target_os = "linux"))]
    unsafe {
        // SAFETY: `fd` is open. F_SETFD with FD_CLOEXEC only updates the
        // descriptor flag used to prevent leaking the listener across exec.
        if fcntl(fd, F_SETFD, FD_CLOEXEC) < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }
    }

    // SAFETY: `setsockopt` on an open socket fd with well-known SOL_SOCKET
    // level, SO_REUSEADDR option, and a pointer to a valid c_int value.
    let optval: c_int = 1;
    let reuse_addr_result = unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &optval,
            std::mem::size_of::<c_int>() as SockLen,
        )
    };
    if reuse_addr_result < 0 {
        close_owned_fd(fd);
        return Err(io::Error::last_os_error());
    }

    if reuse_port {
        // SAFETY: `setsockopt` on an open socket fd with SOL_SOCKET level
        // and SO_REUSEPORT option. The pointer and size are valid.
        let reuse_port_result = unsafe {
            setsockopt(
                fd,
                SOL_SOCKET,
                SO_REUSEPORT,
                &optval,
                std::mem::size_of::<c_int>() as SockLen,
            )
        };
        if reuse_port_result < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }
    }

    let bind_addr = BindSockAddr::new(&addr);
    let (bind_ptr, bind_len) = bind_addr.as_ptr_len();
    // SAFETY: `bind` is called with the open socket fd and a pointer to a
    // properly initialized sockaddr value owned by `bind_addr`, which remains
    // alive for the duration of this call.
    // The length matches the struct size.
    let bind_result = unsafe { bind(fd, bind_ptr, bind_len) };
    if bind_result < 0 {
        close_owned_fd(fd);
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `listen` marks the bound socket as passive with the given backlog.
    // `fd` is a valid bound socket descriptor.
    let listen_result = unsafe { listen(fd, backlog) };
    if listen_result < 0 {
        close_owned_fd(fd);
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `fd` is a valid, bound, listening socket descriptor. Ownership
    // transfers to the `TcpListener`, which will close it on drop.
    Ok(unsafe { TcpListener::from_raw_fd(fd) })
}

fn close_owned_fd(fd: c_int) {
    // SAFETY: callers only pass descriptors still owned by the current setup
    // path before they are transferred to `TcpListener::from_raw_fd`.
    unsafe {
        let _ = close(fd);
    }
}

#[allow(dead_code)]
enum BindSockAddr {
    V4(SockAddrIn),
    V6(SockAddrIn6),
}

impl BindSockAddr {
    fn new(addr: &SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                let octets = v4.ip().octets();
                Self::V4(SockAddrIn {
                    #[cfg(not(target_os = "linux"))]
                    sin_len: std::mem::size_of::<SockAddrIn>() as u8,
                    #[cfg(target_os = "linux")]
                    sin_family: AF_INET as u16,
                    #[cfg(not(target_os = "linux"))]
                    sin_family: AF_INET as u8,
                    sin_port: v4.port().to_be(),
                    sin_addr: InAddr {
                        s_addr: u32::from_ne_bytes(octets),
                    },
                    sin_zero: [0; 8],
                })
            }
            SocketAddr::V6(v6) => Self::V6(SockAddrIn6 {
                #[cfg(not(target_os = "linux"))]
                sin6_len: std::mem::size_of::<SockAddrIn6>() as u8,
                #[cfg(target_os = "linux")]
                sin6_family: AF_INET6 as u16,
                #[cfg(not(target_os = "linux"))]
                sin6_family: AF_INET6 as u8,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: In6Addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            }),
        }
    }

    fn as_ptr_len(&self) -> (*const SockAddrIn, SockLen) {
        match self {
            Self::V4(sin) => (
                sin as *const SockAddrIn,
                std::mem::size_of::<SockAddrIn>() as SockLen,
            ),
            Self::V6(sin6) => (
                sin6 as *const SockAddrIn6 as *const SockAddrIn,
                std::mem::size_of::<SockAddrIn6>() as SockLen,
            ),
        }
    }
}

/// Routes a socket address to a shard using hash-based placement.
#[cfg_attr(target_os = "linux", allow(dead_code))]
fn hash_addr_to_shard(addr: &SocketAddr, shard_count: usize) -> ShardId {
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    ShardId((hasher.finish() as usize) % shard_count)
}

#[cfg(test)]
mod bind_sockaddr_tests {
    use super::*;
    use crate::ShardedExecutor;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    #[test]
    fn bind_sockaddr_builds_ipv4_address_with_stable_pointer() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let bind_addr = BindSockAddr::new(&addr);
        let (ptr, len) = bind_addr.as_ptr_len();

        assert!(!ptr.is_null());
        assert_eq!(len, std::mem::size_of::<SockAddrIn>() as SockLen);
        if let BindSockAddr::V4(sin) = bind_addr {
            assert_eq!(sin.sin_port, 8080u16.to_be());
        } else {
            panic!("expected ipv4 sockaddr");
        }
    }

    #[test]
    fn bind_sockaddr_builds_ipv6_address_with_stable_pointer() {
        let addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9090, 0, 0));
        let bind_addr = BindSockAddr::new(&addr);
        let (ptr, len) = bind_addr.as_ptr_len();

        assert!(!ptr.is_null());
        assert_eq!(len, std::mem::size_of::<SockAddrIn6>() as SockLen);
        if let BindSockAddr::V6(sin6) = bind_addr {
            assert_eq!(sin6.sin6_port, 9090u16.to_be());
        } else {
            panic!("expected ipv6 sockaddr");
        }
    }

    #[test]
    fn connection_permit_enforces_per_shard_limit() {
        let counts = connection_counts(1);
        let first = ConnectionPermit::try_acquire(Arc::clone(&counts), ShardId(0), 1);
        let second = ConnectionPermit::try_acquire(Arc::clone(&counts), ShardId(0), 1);

        assert!(first.is_some());
        assert!(second.is_none());
        drop(first);
        assert!(ConnectionPermit::try_acquire(counts, ShardId(0), 1).is_some());
    }

    #[test]
    fn server_handle_stops_accept_loop() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let server = ShardedTcpServer::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ShardedTcpConfig::new(),
        );

        let handle = server
            .start(&submitter, |_conn, _submitter| async move {})
            .unwrap();
        assert!(handle.stop());
        assert!(handle.is_stopped());

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn server_start_rejects_zero_connection_limit() {
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let server = ShardedTcpServer::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ShardedTcpConfig::new().with_max_connections(0),
        );

        let result = server.start(&submitter, |_conn, _submitter| async move {});

        assert!(matches!(
            result,
            Err(ShardedTcpStartError::InvalidConfig(
                "max_connections_per_shard must be greater than zero"
            ))
        ));
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn server_start_reports_listener_bind_error() {
        let occupied =
            TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let addr = occupied.local_addr().unwrap();
        let runtime = ShardedExecutor::start(1).unwrap();
        let submitter = runtime.submitter();
        let server = ShardedTcpServer::new(addr, ShardedTcpConfig::new().with_reuse_port(false));

        let result = server.start(&submitter, |_conn, _submitter| async move {});

        assert!(matches!(
            result,
            Err(ShardedTcpStartError::Listen { shard_id: None, .. })
        ));
        drop(submitter);
        runtime.stop().unwrap();
    }
}
