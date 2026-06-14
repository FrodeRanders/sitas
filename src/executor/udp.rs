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
const SOL_SOCKET: c_int = 0xffff;
const SO_REUSEADDR: c_int = 0x0004;
const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
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
        address: *const SockAddrStorage,
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

        // SAFETY: `fd` is an open socket descriptor. `fcntl` with F_GETFL and
        // F_SETFL is safe to call on any open descriptor. O_NONBLOCK makes
        // the socket non-blocking. On non-Linux, also set FD_CLOEXEC.
        {
            let flags = unsafe { fcntl(fd, F_GETFL, 0) };
            if flags >= 0 {
                unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) };
            }
            #[cfg(not(target_os = "linux"))]
            unsafe {
                fcntl(fd, 2 /* F_SETFD */, 1 /* FD_CLOEXEC */);
            }
        }

        // SAFETY: `setsockopt` is called on an open socket descriptor with the
        // well-known SOL_SOCKET level and SO_REUSEADDR option. The option value
        // is a pointer to a valid `c_int` with size `sizeof(c_int)`.
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
                // SAFETY: `bind` is called on an open socket descriptor with
                // a pointer to a properly initialized `SockAddrIn` struct.
                // `sizeof(SockAddrIn)` is passed as the address length.
                let bind_result =
                    unsafe { bind(fd, &sin, std::mem::size_of::<SockAddrIn>() as SockLen) };
                if bind_result < 0 {
                    // SAFETY: `fd` is the owned descriptor from `socket()` above.
                    // It is closed at most once on this error path.
                    unsafe { close(fd) };
                    return Err(io::Error::last_os_error());
                }
            }
            SocketAddr::V6(v6) => {
                let sin6 = create_sockaddr_in6(v6);
                // SAFETY: `bind` with a pointer to a properly initialized
                // `SockAddrIn6` struct. The cast through `*const SockAddrIn` is
                // safe because the kernel reads only the first `sizeof(SockAddrIn6)`
                // bytes, which are correctly laid out for the AF_INET6 family.
                let bind_result = unsafe {
                    bind(
                        fd,
                        &sin6 as *const SockAddrIn6 as *const SockAddrIn,
                        std::mem::size_of::<SockAddrIn6>() as SockLen,
                    )
                };
                if bind_result < 0 {
                    // SAFETY: `fd` is owned by this scope; closing on error.
                    unsafe { close(fd) };
                    return Err(io::Error::last_os_error());
                }
            }
        }

        // SAFETY: `fd` is a valid, open socket descriptor from `socket()` above.
        // Ownership is transferred to the `OwnedFd`, which will call `close()`
        // on drop. No other code holds a reference to this fd.
        let fd_owned = unsafe { OwnedFd::from_raw_fd(fd) };
        let local_addr = get_sock_name(fd, addr)?;

        Ok(Self {
            fd: fd_owned,
            local_addr,
        })
    }

    /// Receives a datagram and the sender's address.
    ///
    /// Returns `io::ErrorKind::WouldBlock` when no data is available.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut addr_buf = [0u8; 128];
        let mut addr_len = 128u32;

        // SAFETY: `recvfrom` reads into `buf` (valid writable memory of
        // `buf.len()` bytes) and writes the sender address into `addr_buf`
        // (a 128-byte stack buffer). `addr_len` is initialized and passed
        // by mutable reference. The socket fd is valid and owned.
        let n = unsafe {
            recvfrom(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr(),
                buf.len(),
                0,
                &mut addr_buf as *mut u8 as *mut SockAddrStorage,
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

        let addr = decode_sockaddr_from_buf(&addr_buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "invalid address"))?;

        Ok((n as usize, addr))
    }

    /// Sends a datagram to `addr`.
    ///
    /// Returns `io::ErrorKind::WouldBlock` when the socket is not writable.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        let sockaddr_storage = socket_addr_to_sockaddr_storage(&addr);
        let sockaddr_len = socket_addr_len(&addr);

        // SAFETY: `sendto` reads from `buf` (valid readable memory of
        // `buf.len()` bytes) and writes to the socket. `sockaddr_storage`
        // contains a properly encoded sockaddr. The socket fd is valid.
        let n = unsafe {
            sendto(
                self.fd.as_raw_fd(),
                buf.as_ptr(),
                buf.len(),
                0,
                &sockaddr_storage,
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

/// Creates a UDP socket pair for testing and local communication.
pub fn udp_pair() -> io::Result<(UdpSocket, UdpSocket)> {
    let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap())?;
    let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap())?;
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

// Helper: convert SocketAddr to a raw byte buffer suitable for sendto/connect
fn socket_addr_to_sockaddr_storage(addr: &SocketAddr) -> SockAddrStorage {
    let mut buf = [0u8; 128];
    encode_sockaddr_to_buf(addr, &mut buf);
    // Copy to SockAddrStorage for FFI
    let mut storage = SockAddrStorage {
        ss_family: 0,
        __data: [0; 126],
    };
    // SAFETY: copying at most 128 bytes from `buf` (a stack buffer containing
    // a properly encoded sockaddr) into `storage` (a `SockAddrStorage` of
    // equal or larger size). Both pointers are valid and the regions do not
    // overlap.
    unsafe {
        std::ptr::copy_nonoverlapping(
            buf.as_ptr(),
            &mut storage as *mut SockAddrStorage as *mut u8,
            128.min(std::mem::size_of::<SockAddrStorage>()),
        );
    }
    storage
}

fn encode_sockaddr_to_buf(addr: &SocketAddr, buf: &mut [u8; 128]) {
    buf.fill(0);
    match addr {
        SocketAddr::V4(v4) => {
            #[cfg(target_os = "linux")]
            {
                buf[0..2].copy_from_slice(&(AF_INET as u16).to_ne_bytes());
            }
            #[cfg(not(target_os = "linux"))]
            {
                buf[0] = std::mem::size_of::<SockAddrIn>() as u8;
                buf[1] = AF_INET as u8;
            }
            buf[2..4].copy_from_slice(&v4.port().to_be_bytes());
            buf[4..8].copy_from_slice(&v4.ip().octets());
        }
        SocketAddr::V6(v6) => {
            #[cfg(target_os = "linux")]
            {
                buf[0..2].copy_from_slice(&(AF_INET6 as u16).to_ne_bytes());
            }
            #[cfg(not(target_os = "linux"))]
            {
                buf[0] = std::mem::size_of::<SockAddrIn6>() as u8;
                buf[1] = AF_INET6 as u8;
            }
            buf[2..4].copy_from_slice(&v6.port().to_be_bytes());
            buf[4..8].copy_from_slice(&v6.flowinfo().to_be_bytes());
            buf[8..24].copy_from_slice(&v6.ip().octets());
            buf[24..28].copy_from_slice(&v6.scope_id().to_be_bytes());
        }
    }
}

fn create_sockaddr_in6(v6: std::net::SocketAddrV6) -> SockAddrIn6 {
    SockAddrIn6 {
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
    }
}

fn socket_addr_len(addr: &SocketAddr) -> SockLen {
    match addr {
        SocketAddr::V4(_) => std::mem::size_of::<SockAddrIn>() as SockLen,
        SocketAddr::V6(_) => std::mem::size_of::<SockAddrIn6>() as SockLen,
    }
}

fn decode_sockaddr_from_buf(buf: &[u8; 128]) -> Option<SocketAddr> {
    // On Linux, family is at offset 0 as u16. On macOS/BSD, length at offset 0 (u8), family at offset 1 (u8).
    #[cfg(target_os = "linux")]
    let family = u16::from_ne_bytes([buf[0], buf[1]]);
    #[cfg(not(target_os = "linux"))]
    let family = buf[1] as u16;

    match family as c_int {
        AF_INET => {
            let port = u16::from_be_bytes([buf[2], buf[3]]);
            let ip = std::net::Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]);
            Some(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            let port = u16::from_be_bytes([buf[2], buf[3]]);
            let flowinfo = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let mut ip_bytes = [0u8; 16];
            ip_bytes.copy_from_slice(&buf[8..24]);
            let ip = std::net::Ipv6Addr::from(ip_bytes);
            let scope_id = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
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
    use std::net::{Ipv4Addr, SocketAddrV4};

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
                    std::thread::sleep(std::time::Duration::from_micros(10));
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
