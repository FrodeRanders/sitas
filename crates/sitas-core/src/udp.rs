//! UDP socket support for the custom async executor.
//!
//! Provides non-blocking UDP socket creation, send, and receive operations
//! integrated with the executor's readiness-based I/O model.
//!
//! This module uses direct Unix FFI, following the same pattern as the `os`
//! module, to keep the project dependency-free.

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::raw::c_int;

// FFI declarations (same pattern as src/os.rs)
const AF_INET: c_int = 2;
#[cfg(target_os = "linux")]
const AF_INET6: c_int = 10;
#[cfg(not(target_os = "linux"))]
const AF_INET6: c_int = 30;
const SOCK_DGRAM: c_int = 2;
#[cfg(target_os = "linux")]
const SOCK_CLOEXEC: c_int = 0o2000000;
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
const SOCK_CLOEXEC: c_int = 0;
#[cfg(target_os = "linux")]
const SOL_SOCKET: c_int = 1;
#[cfg(not(target_os = "linux"))]
const SOL_SOCKET: c_int = 0xffff;
#[cfg(target_os = "linux")]
const SO_REUSEADDR: c_int = 2;
#[cfg(not(target_os = "linux"))]
const SO_REUSEADDR: c_int = 0x0004;
const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
#[cfg(not(target_os = "linux"))]
const F_SETFD: c_int = 2;
#[cfg(not(target_os = "linux"))]
const FD_CLOEXEC: c_int = 1;
#[cfg(target_os = "linux")]
const O_NONBLOCK: c_int = 0o4000;
#[cfg(not(target_os = "linux"))]
const O_NONBLOCK: c_int = 0x0004;

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

#[cfg(not(target_os = "linux"))]
#[repr(C)]
struct SockAddrIn {
    sin_len: u8,
    sin_family: u8,
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

#[repr(C)]
struct SockAddrStorage {
    ss_family: u16,
    __data: [u8; 126],
}

type SockLen = u32;

unsafe extern "C" {
    fn bind(fd: c_int, address: *const SockAddrIn, length: SockLen) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
    fn getsockname(fd: c_int, address: *mut SockAddrStorage, length: *mut SockLen) -> c_int;
    fn recvfrom(
        fd: c_int,
        buf: *mut u8,
        len: usize,
        flags: c_int,
        address: *mut SockAddrStorage,
        address_len: *mut SockLen,
    ) -> isize;
    fn sendto(
        fd: c_int,
        buf: *const u8,
        len: usize,
        flags: c_int,
        address: *const SockAddrIn,
        address_len: SockLen,
    ) -> isize;
    fn setsockopt(
        fd: c_int,
        level: c_int,
        option_name: c_int,
        option_value: *const c_int,
        option_len: SockLen,
    ) -> c_int;
    fn socket(domain: c_int, socket_type: c_int, protocol: c_int) -> c_int;
}

/// A non-blocking UDP socket.
#[derive(Debug)]
pub struct UdpSocket {
    fd: OwnedFd,
    local_addr: SocketAddr,
}

impl UdpSocket {
    /// Creates a new non-blocking UDP socket bound to `addr`.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };

        #[cfg(target_os = "linux")]
        let sock_type = SOCK_DGRAM | SOCK_CLOEXEC;
        #[cfg(not(target_os = "linux"))]
        let sock_type = SOCK_DGRAM;

        // SAFETY: `socket` is called with constant AF_INET/AF_INET6,
        // SOCK_DGRAM (and SOCK_CLOEXEC on Linux) values, and protocol 0.
        // The returned descriptor is valid or -1 on error, which we check.
        let fd = unsafe { socket(domain, sock_type, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `fd` is an open socket descriptor. F_GETFL reads descriptor
        // flags, and F_SETFL writes the same flags plus O_NONBLOCK.
        let flags = unsafe { fcntl(fd, F_GETFL, 0) };
        if flags < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is open and `flags | O_NONBLOCK` is derived from the
        // descriptor's current flags.
        if unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) } < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }

        #[cfg(not(target_os = "linux"))]
        // SAFETY: `fd` is open. F_SETFD with FD_CLOEXEC only updates the
        // descriptor flag used to prevent leaking the socket across exec.
        if unsafe { fcntl(fd, F_SETFD, FD_CLOEXEC) } < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `setsockopt` is called on an open socket descriptor with the
        // well-known SOL_SOCKET level and SO_REUSEADDR option. The option value
        // is a pointer to a valid `c_int` with size `sizeof(c_int)`.
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

        let bind_addr = BindSockAddr::new(&addr);
        let (bind_ptr, bind_len) = bind_addr.as_ptr_len();
        // SAFETY: `bind` is called on an open socket descriptor with a pointer
        // to a properly initialized sockaddr value owned by `bind_addr`, which
        // remains alive for the duration of this call. The length matches the
        // concrete sockaddr struct.
        let bind_result = unsafe { bind(fd, bind_ptr, bind_len) };
        if bind_result < 0 {
            close_owned_fd(fd);
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `fd` is a valid, open, bound socket descriptor.
        // Ownership is transferred to `OwnedFd` below. The raw `fd`
        // value is only used for `getsockname` before any drop can close it.
        let local_addr = match get_sock_name(fd, addr) {
            Ok(local_addr) => local_addr,
            Err(err) => {
                close_owned_fd(fd);
                return Err(err);
            }
        };
        // SAFETY: `fd` is a valid, open socket descriptor. Ownership is
        // transferred to the `OwnedFd`, which will call `close()` on drop.
        let fd_owned = unsafe { OwnedFd::from_raw_fd(fd) };

        Ok(Self {
            fd: fd_owned,
            local_addr,
        })
    }

    /// Receives a datagram and the sender's address.
    ///
    /// Returns `io::ErrorKind::WouldBlock` when no data is available.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        // SAFETY: zero-initializing sockaddr storage produces a valid writable
        // buffer for the kernel to fill.
        let mut addr_storage: SockAddrStorage = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<SockAddrStorage>() as SockLen;

        // SAFETY: `recvfrom` reads into `buf` (valid writable memory of
        // `buf.len()` bytes) and writes the sender address into
        // `addr_storage`. `addr_len` is initialized and passed by mutable
        // reference. The socket fd is valid and owned.
        let n = unsafe {
            recvfrom(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr(),
                buf.len(),
                0,
                &mut addr_storage,
                &mut addr_len,
            )
        };

        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(err);
            }
            return Err(err);
        }

        let addr = decode_sockaddr_storage(&addr_storage)
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "invalid address"))?;

        Ok((n as usize, addr))
    }

    /// Sends a datagram to `addr`.
    ///
    /// Returns `io::ErrorKind::WouldBlock` when the socket is not writable.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        let sockaddr = BindSockAddr::new(&addr);
        let (sockaddr_ptr, sockaddr_len) = sockaddr.as_ptr_len();

        // SAFETY: `sendto` reads from `buf` (valid readable memory of
        // `buf.len()` bytes) and writes to the socket. `sockaddr` owns a
        // properly initialized sockaddr value that remains alive for this call.
        // The socket fd is valid.
        let n = unsafe {
            sendto(
                self.fd.as_raw_fd(),
                buf.as_ptr(),
                buf.len(),
                0,
                sockaddr_ptr,
                sockaddr_len,
            )
        };

        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(err);
            }
            return Err(err);
        }

        Ok(n as usize)
    }

    /// Returns the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Converts into an owned file descriptor.
    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }

    /// Returns the local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

fn close_owned_fd(fd: c_int) {
    // SAFETY: callers only pass descriptors still owned by the current setup
    // path before they are transferred to `OwnedFd::from_raw_fd`.
    unsafe {
        let _ = close(fd);
    }
}

/// Creates a UDP socket pair for testing and local communication.
pub fn udp_pair() -> io::Result<(UdpSocket, UdpSocket)> {
    let addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("hardcoded localhost address is valid");
    let a = UdpSocket::bind(addr)?;
    let b = UdpSocket::bind(addr)?;
    Ok((a, b))
}

fn get_sock_name(fd: c_int, addr: SocketAddr) -> io::Result<SocketAddr> {
    match addr {
        SocketAddr::V4(_) => {
            // SAFETY: zero-initializing a `SockAddrIn` (repr(C), all integer fields)
            // produces a valid value for `getsockname` to overwrite.
            let mut sin: SockAddrIn = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<SockAddrIn>() as SockLen;
            // SAFETY: `getsockname` writes the bound address into `sin`.
            // `fd` is a valid bound socket, `len` is correctly initialized.
            let result = unsafe {
                getsockname(
                    fd,
                    &mut sin as *mut SockAddrIn as *mut SockAddrStorage,
                    &mut len,
                )
            };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        SocketAddr::V6(_) => {
            // SAFETY: zero-initializing a `SockAddrIn6` (repr(C), all integer
            // fields) produces a valid value for `getsockname` to overwrite.
            let mut sin6: SockAddrIn6 = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<SockAddrIn6>() as SockLen;
            // SAFETY: `getsockname` writes the bound address into `sin6`.
            // `fd` is a valid bound socket.
            let result = unsafe {
                getsockname(
                    fd,
                    &mut sin6 as *mut SockAddrIn6 as *mut SockAddrStorage,
                    &mut len,
                )
            };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            let scope_id = sin6.sin6_scope_id;
            Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
    }
}

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

fn decode_sockaddr_storage(storage: &SockAddrStorage) -> Option<SocketAddr> {
    #[cfg(target_os = "linux")]
    let family = storage.ss_family;
    #[cfg(not(target_os = "linux"))]
    let family = {
        // SAFETY: `storage` is a valid sockaddr_storage object. BSD-family
        // sockaddrs store length at byte 0 and family at byte 1.
        unsafe { *(storage as *const SockAddrStorage as *const u8).add(1) as u16 }
    };

    match family as c_int {
        AF_INET => {
            // SAFETY: the kernel reported AF_INET. Use an unaligned read from
            // storage because `sockaddr_storage` alignment varies by platform.
            let sin = unsafe {
                std::ptr::read_unaligned(storage as *const SockAddrStorage as *const SockAddrIn)
            };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            // SAFETY: the kernel reported AF_INET6. Use an unaligned read from
            // storage because `sockaddr_storage` alignment varies by platform.
            let sin6 = unsafe {
                std::ptr::read_unaligned(storage as *const SockAddrStorage as *const SockAddrIn6)
            };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            let scope_id = sin6.sin6_scope_id;
            Some(SocketAddr::V6(std::net::SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn udp_socket_bind_and_send_recv() {
        let a = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let b = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let b_addr = b.local_addr().unwrap();
        let a_addr = a.local_addr().unwrap();

        let msg = b"hello udp";
        let sent = a.send_to(msg, b_addr).unwrap();
        assert_eq!(sent, msg.len());

        let mut buf = [0u8; 1024];
        let (n, from_addr) = recv_with_retry(&b, &mut buf).unwrap();
        assert_eq!(&buf[..n], msg);
        assert_eq!(from_addr, a_addr);
    }

    #[test]
    fn udp_pair_communicates() {
        let (a, b) = udp_pair().unwrap();
        let a_addr = a.local_addr().unwrap();

        b.send_to(b"ping", a_addr).unwrap();
        let mut buf = [0u8; 1024];
        let (n, _from) = recv_with_retry(&a, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"ping");
    }

    #[test]
    fn udp_would_block_on_empty_socket() {
        let a = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let mut buf = [0u8; 1024];
        let result = a.recv_from(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    fn recv_with_retry(socket: &UdpSocket, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        for _ in 0..100 {
            match socket.recv_from(buf) {
                Ok(result) => return Ok(result),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(core::time::Duration::from_micros(10));
                }
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "recv timed out after retries",
        ))
    }
}
