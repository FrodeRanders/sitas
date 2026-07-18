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

mod socket_options;

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::raw::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::executor::{RaceOutput, StopSource, accept_async, race, stop_pair};
use crate::shard::ShardId;
use crate::sharded_executor::{CpuId, ShardedSpawnError, ShardedSubmitter, available_cpu_ids};

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

impl ShardedTcpConnection {
    /// Enables Linux kTLS TCP ULP on this accepted stream.
    ///
    /// This only attaches the kernel TLS upper-layer protocol to the socket.
    /// A complete kTLS handoff also requires a TLS implementation to complete
    /// the handshake and install TX/RX crypto state with Linux `SOL_TLS`
    /// options. `ShardedTcpServer` intentionally leaves that protocol-specific
    /// key material boundary to the connection handler.
    #[cfg(target_os = "linux")]
    pub fn enable_kernel_tls_ulp(&self) -> io::Result<()> {
        socket_options::enable_kernel_tls_ulp(self.stream.as_raw_fd())
    }

    /// Returns unsupported on non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    pub fn enable_kernel_tls_ulp(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Linux kTLS TCP ULP is only available on Linux",
        ))
    }
}

/// Policy for applying Linux `SO_INCOMING_CPU` to sharded TCP listeners.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ShardedTcpIncomingCpu {
    /// Do not set `SO_INCOMING_CPU`.
    #[default]
    Disabled,
    /// Use the process-available CPU list, assigning shard `N` to CPU
    /// `available_cpu_ids()[N % len]`.
    SequentialAvailable,
    /// Use the explicit CPU id for each shard.
    Explicit(Vec<CpuId>),
}

impl ShardedTcpIncomingCpu {
    fn cpu_for_shard(&self, shard_idx: usize, available_cpus: &[CpuId]) -> Option<CpuId> {
        match self {
            Self::Disabled => None,
            Self::SequentialAvailable => {
                if available_cpus.is_empty() {
                    Some(CpuId(shard_idx))
                } else {
                    Some(available_cpus[shard_idx % available_cpus.len()])
                }
            }
            Self::Explicit(cpus) => cpus.get(shard_idx).copied(),
        }
    }
}

/// Configuration for a sharded TCP server.
#[derive(Clone)]
pub struct ShardedTcpConfig {
    /// Maximum concurrent connections per shard.
    pub max_connections_per_shard: usize,
    /// Whether to use `SO_REUSEPORT` (Linux only, enables per-shard accept).
    pub reuse_port: bool,
    /// Linux `SO_INCOMING_CPU` placement for listener sockets.
    pub incoming_cpu: ShardedTcpIncomingCpu,
    /// Connection backlog.
    pub backlog: u32,
    /// Optional structured event sink for server-level runtime events.
    pub event_sink: Option<Arc<dyn ShardedTcpEventSink>>,
}

impl Default for ShardedTcpConfig {
    fn default() -> Self {
        Self {
            max_connections_per_shard: 128,
            reuse_port: true,
            incoming_cpu: ShardedTcpIncomingCpu::Disabled,
            backlog: 128,
            event_sink: None,
        }
    }
}

impl fmt::Debug for ShardedTcpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedTcpConfig")
            .field("max_connections_per_shard", &self.max_connections_per_shard)
            .field("reuse_port", &self.reuse_port)
            .field("incoming_cpu", &self.incoming_cpu)
            .field("backlog", &self.backlog)
            .field("event_sink", &self.event_sink.as_ref().map(|_| "<sink>"))
            .finish()
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

    /// Sets Linux `SO_INCOMING_CPU` placement for listener sockets.
    ///
    /// This option is Linux-only. On other platforms the setting remains
    /// visible in configuration but has no socket effect.
    pub fn with_incoming_cpu(mut self, incoming_cpu: ShardedTcpIncomingCpu) -> Self {
        self.incoming_cpu = incoming_cpu;
        self
    }

    /// Sets the TCP backlog.
    pub fn with_backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Sets a structured event sink for server-level runtime events.
    pub fn with_event_sink<S>(mut self, sink: S) -> Self
    where
        S: ShardedTcpEventSink,
    {
        self.event_sink = Some(Arc::new(sink));
        self
    }

    /// Sets a shared structured event sink for server-level runtime events.
    pub fn with_shared_event_sink(mut self, sink: Arc<dyn ShardedTcpEventSink>) -> Self {
        self.event_sink = Some(sink);
        self
    }
}

/// Structured event emitted by a [`ShardedTcpServer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardedTcpEvent {
    /// An accepted connection was dropped because its target shard was already
    /// at the configured connection limit.
    ConnectionLimitRejected {
        /// The target shard for the connection.
        shard_id: ShardId,
    },
    /// An accept-loop error stopped an accept task.
    AcceptError {
        /// The shard that owned the failing listener, or `None` for the
        /// single-accept path.
        shard_id: Option<ShardId>,
        /// The stable `io::ErrorKind` for the accept failure.
        kind: io::ErrorKind,
    },
    /// An accepted connection's handler could not be submitted.
    HandlerSubmitError {
        /// The target shard for the handler.
        shard_id: ShardId,
        /// The submit error returned by the sharded executor.
        error: ShardedSpawnError,
    },
}

/// Sink for structured [`ShardedTcpEvent`] values.
pub trait ShardedTcpEventSink: Send + Sync + 'static {
    /// Records one server event.
    ///
    /// Panics from this method are isolated by the server event recorder after
    /// built-in snapshot counters have been updated.
    fn record(&self, event: ShardedTcpEvent);
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
    stats: Arc<ShardedTcpServerStats>,
}

/// Owned observability snapshot for a [`ShardedTcpServer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedTcpServerSnapshot {
    /// Number of accepted connections dropped because their target shard was
    /// already at the configured connection limit.
    pub connection_limit_rejections: u64,
    /// Number of accept-loop errors that stopped an accept task.
    pub accept_errors: u64,
    /// Number of accepted connections whose handler could not be submitted.
    pub handler_submit_errors: u64,
}

#[derive(Debug, Default)]
struct ShardedTcpServerStats {
    connection_limit_rejections: AtomicU64,
    accept_errors: AtomicU64,
    handler_submit_errors: AtomicU64,
}

impl ShardedTcpServerStats {
    fn record(&self, event: ShardedTcpEvent) {
        match event {
            ShardedTcpEvent::ConnectionLimitRejected { .. } => {
                self.connection_limit_rejections
                    .fetch_add(1, Ordering::AcqRel);
            }
            ShardedTcpEvent::AcceptError { .. } => {
                self.accept_errors.fetch_add(1, Ordering::AcqRel);
            }
            ShardedTcpEvent::HandlerSubmitError { .. } => {
                self.handler_submit_errors.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn snapshot(&self) -> ShardedTcpServerSnapshot {
        ShardedTcpServerSnapshot {
            connection_limit_rejections: self.connection_limit_rejections.load(Ordering::Acquire),
            accept_errors: self.accept_errors.load(Ordering::Acquire),
            handler_submit_errors: self.handler_submit_errors.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone)]
struct ShardedTcpEventRecorder {
    stats: Arc<ShardedTcpServerStats>,
    event_sink: Option<Arc<dyn ShardedTcpEventSink>>,
}

impl ShardedTcpEventRecorder {
    fn new(
        stats: Arc<ShardedTcpServerStats>,
        event_sink: Option<Arc<dyn ShardedTcpEventSink>>,
    ) -> Self {
        Self { stats, event_sink }
    }

    fn record(&self, event: ShardedTcpEvent) {
        self.stats.record(event);
        if let Some(sink) = &self.event_sink {
            let _ = catch_unwind(AssertUnwindSafe(|| sink.record(event)));
        }
    }
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

    /// Returns an owned snapshot of server-level accept-loop counters.
    pub fn snapshot(&self) -> ShardedTcpServerSnapshot {
        self.stats.snapshot()
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
        if let ShardedTcpIncomingCpu::Explicit(cpus) = &self.config.incoming_cpu
            && cpus.len() < submitter.shard_count()
        {
            return Err(ShardedTcpStartError::InvalidConfig(
                "incoming CPU placement must provide a CPU for every shard",
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
        let stats = Arc::new(ShardedTcpServerStats::default());
        let recorder =
            ShardedTcpEventRecorder::new(Arc::clone(&stats), self.config.event_sink.clone());
        let (stop_source, stop_token) = stop_pair();
        let mut listener_addr = self.bind_addr;
        let mut listeners = Vec::with_capacity(submitter.shard_count());
        let available_cpus = available_cpu_ids();

        for shard_idx in 0..submitter.shard_count() {
            let shard_id = ShardId(shard_idx);
            let incoming_cpu = self
                .config
                .incoming_cpu
                .cpu_for_shard(shard_idx, &available_cpus);
            let listener =
                create_listener(listener_addr, backlog, true, incoming_cpu).map_err(|source| {
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
            let recorder = recorder.clone();

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
                                    recorder.record(ShardedTcpEvent::ConnectionLimitRejected {
                                        shard_id,
                                    });
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
                                    recorder.record(ShardedTcpEvent::HandlerSubmitError {
                                        shard_id,
                                        error,
                                    });
                                }
                            }
                            RaceOutput::First(Err(e)) => {
                                recorder.record(ShardedTcpEvent::AcceptError {
                                    shard_id: Some(shard_id),
                                    kind: e.kind(),
                                });
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

        Ok(ShardedTcpServerHandle { stop_source, stats })
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
        let stats = Arc::new(ShardedTcpServerStats::default());
        let recorder =
            ShardedTcpEventRecorder::new(Arc::clone(&stats), self.config.event_sink.clone());
        let (stop_source, stop_token) = stop_pair();
        let incoming_cpu = self
            .config
            .incoming_cpu
            .cpu_for_shard(0, &available_cpu_ids());
        let listener = create_single_listener(bind_addr, self.config.backlog, incoming_cpu)?;

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
                            recorder.record(ShardedTcpEvent::ConnectionLimitRejected {
                                shard_id: target_shard,
                            });
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
                            recorder.record(ShardedTcpEvent::HandlerSubmitError {
                                shard_id: target_shard,
                                error,
                            });
                        }
                    }
                    RaceOutput::First(Err(e)) => {
                        recorder.record(ShardedTcpEvent::AcceptError {
                            shard_id: None,
                            kind: e.kind(),
                        });
                        break;
                    }
                    RaceOutput::Second(()) => break,
                }
            }
        }) {
            stop_source.stop();
            return Err(error.into());
        }

        Ok(ShardedTcpServerHandle { stop_source, stats })
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

fn create_single_listener(
    addr: SocketAddr,
    backlog: u32,
    incoming_cpu: Option<CpuId>,
) -> Result<TcpListener, ShardedTcpStartError> {
    create_listener(addr, backlog, false, incoming_cpu).map_err(|source| {
        ShardedTcpStartError::Listen {
            shard_id: None,
            source,
        }
    })
}

#[allow(dead_code)]
fn create_listener(
    addr: SocketAddr,
    backlog: u32,
    reuse_port: bool,
    incoming_cpu: Option<CpuId>,
) -> io::Result<TcpListener> {
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

    if let Err(error) = socket_options::set_reuse_addr(fd) {
        close_owned_fd(fd);
        return Err(error);
    }

    if reuse_port && let Err(error) = socket_options::set_reuse_port(fd) {
        close_owned_fd(fd);
        return Err(error);
    }

    if let Err(error) = socket_options::set_incoming_cpu(fd, incoming_cpu) {
        close_owned_fd(fd);
        return Err(error);
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
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<ShardedTcpEvent>>,
    }

    impl ShardedTcpEventSink for RecordingSink {
        fn record(&self, event: ShardedTcpEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    struct PanickingSink;

    impl ShardedTcpEventSink for PanickingSink {
        fn record(&self, _event: ShardedTcpEvent) {
            panic!("sink panic");
        }
    }

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
    fn incoming_cpu_policy_maps_shards_to_available_cpus() {
        let available = vec![CpuId(2), CpuId(4)];

        assert_eq!(
            ShardedTcpIncomingCpu::Disabled.cpu_for_shard(0, &available),
            None
        );
        assert_eq!(
            ShardedTcpIncomingCpu::SequentialAvailable.cpu_for_shard(0, &available),
            Some(CpuId(2))
        );
        assert_eq!(
            ShardedTcpIncomingCpu::SequentialAvailable.cpu_for_shard(2, &available),
            Some(CpuId(2))
        );
        assert_eq!(
            ShardedTcpIncomingCpu::Explicit(vec![CpuId(7)]).cpu_for_shard(0, &available),
            Some(CpuId(7))
        );
        assert_eq!(
            ShardedTcpIncomingCpu::Explicit(vec![CpuId(7)]).cpu_for_shard(1, &available),
            None
        );
    }

    #[test]
    fn event_recorder_updates_snapshot_and_custom_sink() {
        let stats = Arc::new(ShardedTcpServerStats::default());
        let sink = Arc::new(RecordingSink::default());
        let sink_for_recorder: Arc<dyn ShardedTcpEventSink> = sink.clone();
        let recorder = ShardedTcpEventRecorder::new(Arc::clone(&stats), Some(sink_for_recorder));
        let event = ShardedTcpEvent::ConnectionLimitRejected {
            shard_id: ShardId(0),
        };

        recorder.record(event);

        assert_eq!(
            stats.snapshot(),
            ShardedTcpServerSnapshot {
                connection_limit_rejections: 1,
                accept_errors: 0,
                handler_submit_errors: 0,
            }
        );
        assert_eq!(sink.events.lock().unwrap().as_slice(), &[event]);
    }

    #[test]
    fn event_recorder_isolates_sink_panics() {
        let stats = Arc::new(ShardedTcpServerStats::default());
        let sink = Arc::new(PanickingSink);
        let recorder = ShardedTcpEventRecorder::new(Arc::clone(&stats), Some(sink));

        recorder.record(ShardedTcpEvent::AcceptError {
            shard_id: Some(ShardId(0)),
            kind: io::ErrorKind::ConnectionAborted,
        });

        assert_eq!(
            stats.snapshot(),
            ShardedTcpServerSnapshot {
                connection_limit_rejections: 0,
                accept_errors: 1,
                handler_submit_errors: 0,
            }
        );
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
        assert_eq!(
            handle.snapshot(),
            ShardedTcpServerSnapshot {
                connection_limit_rejections: 0,
                accept_errors: 0,
                handler_submit_errors: 0,
            }
        );
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
    fn server_start_rejects_explicit_incoming_cpu_that_does_not_cover_shards() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let server = ShardedTcpServer::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ShardedTcpConfig::new()
                .with_reuse_port(false)
                .with_incoming_cpu(ShardedTcpIncomingCpu::Explicit(vec![CpuId(0)])),
        );

        let result = server.start(&submitter, |_conn, _submitter| async move {});

        assert!(matches!(
            result,
            Err(ShardedTcpStartError::InvalidConfig(
                "incoming CPU placement must provide a CPU for every shard"
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
