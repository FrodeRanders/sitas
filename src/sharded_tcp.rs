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
use std::hash::{Hash, Hasher};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::raw::c_int;

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
const SOL_SOCKET: c_int = 0xffff;
#[allow(dead_code)]
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

/// A sharded TCP server.
///
/// Binds a TCP listener and spawns one accept-loop task per shard. Accepted
/// connections are routed to their owning shard's handler.
pub struct ShardedTcpServer {
    bind_addr: SocketAddr,
    #[allow(dead_code)]
    config: ShardedTcpConfig,
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
    ) -> Result<(), ShardedSpawnError>
    where
        MakeHandler: Fn(ShardedTcpConnection, ShardedSubmitter) -> HandlerFut
            + Send
            + Sync
            + Clone
            + 'static,
        HandlerFut: std::future::Future<Output = ()> + Send + 'static,
    {
        #[cfg(target_os = "linux")]
        {
            self.start_with_reuseport(submitter, make_handler)
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
    ) -> Result<(), ShardedSpawnError>
    where
        MakeHandler: Fn(ShardedTcpConnection, ShardedSubmitter) -> HandlerFut
            + Send
            + Sync
            + Clone
            + 'static,
        HandlerFut: std::future::Future<Output = ()> + Send + 'static,
    {
        let bind_addr = self.bind_addr;
        let backlog = self.config.backlog;
        let reuse_port = self.config.reuse_port;

        for shard_idx in 0..submitter.shard_count() {
            let shard_id = ShardId(shard_idx);
            let make_handler = make_handler.clone();
            let shard_submitter = submitter.clone();

            submitter.submit_named_to(
                shard_id,
                format!("tcp-accept-{}", shard_idx),
                async move {
                    let listener = match create_listener(bind_addr, backlog, reuse_port) {
                        Ok(l) => l,
                        Err(e) => {
                            eprintln!("[sitas-tcp] shard {} failed to bind: {}", shard_idx, e);
                            return;
                        }
                    };

                    loop {
                        match listener.accept() {
                            Ok((stream, remote_addr)) => {
                                let conn = ShardedTcpConnection {
                                    stream,
                                    remote_addr,
                                    shard_id,
                                };
                                let _ = shard_submitter.submit_to(
                                    shard_id,
                                    make_handler(conn, shard_submitter.clone()),
                                );
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                continue;
                            }
                            Err(e) => {
                                eprintln!("[sitas-tcp] shard {} accept error: {}", shard_idx, e);
                                break;
                            }
                        }
                    }
                },
            )?;
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn start_with_single_accept<MakeHandler, HandlerFut>(
        &self,
        submitter: &ShardedSubmitter,
        make_handler: MakeHandler,
    ) -> Result<(), ShardedSpawnError>
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

        submitter.submit_named_to(ShardId(0), "tcp-accept-0", async move {
            let listener = match std::net::TcpListener::bind(bind_addr) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[sitas-tcp] failed to bind: {}", e);
                    return;
                }
            };
            let _ = listener.set_nonblocking(true);

            loop {
                match listener.accept() {
                    Ok((stream, remote_addr)) => {
                        let target_shard = hash_addr_to_shard(&remote_addr, shard_count);
                        let conn = ShardedTcpConnection {
                            stream,
                            remote_addr,
                            shard_id: target_shard,
                        };
                        let handler = make_handler(conn, shard_submitter.clone());
                        let _ = shard_submitter.submit_to(target_shard, handler);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(e) => {
                        eprintln!("[sitas-tcp] accept error: {}", e);
                        break;
                    }
                }
            }
        })?;

        Ok(())
    }
}

#[allow(dead_code)]
fn create_listener(addr: SocketAddr, backlog: u32, reuse_port: bool) -> io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => AF_INET,
        SocketAddr::V6(_) => AF_INET6,
    };

    #[cfg(target_os = "linux")]
    let sock_type = SOCK_STREAM | SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let sock_type = SOCK_STREAM;

    let fd = unsafe { socket(domain, sock_type, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    #[cfg(not(target_os = "linux"))]
    unsafe {
        let flags = fcntl(fd, F_GETFL, 0);
        if flags >= 0 {
            fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        // FD_CLOEXEC
        fcntl(fd, 2, 1);
    }

    let optval: c_int = 1;
    unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &optval,
            std::mem::size_of::<c_int>() as SockLen,
        );
    }

    if reuse_port {
        unsafe {
            setsockopt(
                fd,
                SOL_SOCKET,
                SO_REUSEPORT,
                &optval,
                std::mem::size_of::<c_int>() as SockLen,
            );
        }
    }

    let (bind_ptr, bind_len) = build_sockaddr_for_bind(&addr);
    let bind_result = unsafe { bind(fd, bind_ptr, bind_len) };
    if bind_result < 0 {
        unsafe { close(fd) };
        return Err(io::Error::last_os_error());
    }

    let listen_result = unsafe { listen(fd, backlog as c_int) };
    if listen_result < 0 {
        unsafe { close(fd) };
        return Err(io::Error::last_os_error());
    }

    Ok(unsafe { TcpListener::from_raw_fd(fd) })
}

#[allow(dead_code)]
fn build_sockaddr_for_bind(addr: &SocketAddr) -> (*const SockAddrIn, SockLen) {
    match addr {
        SocketAddr::V4(v4) => {
            let octets = v4.ip().octets();
            let sin = SockAddrIn {
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
            };
            (
                &sin as *const SockAddrIn,
                std::mem::size_of::<SockAddrIn>() as SockLen,
            )
        }
        SocketAddr::V6(v6) => {
            let sin6 = SockAddrIn6 {
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
            };
            (
                &sin6 as *const SockAddrIn6 as *const SockAddrIn,
                std::mem::size_of::<SockAddrIn6>() as SockLen,
            )
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
