//! Socket option helpers for the sharded TCP server.
//!
//! Linux-specific options such as `SO_INCOMING_CPU` and `TCP_ULP=tls` live
//! here so the accept-loop code does not carry Linux socket-option constants
//! inline. Cross-platform options used by listener setup stay behind the same
//! helper boundary.

use std::io;
use std::os::raw::c_int;

use crate::sharded_executor::CpuId;

type SockLen = u32;

#[cfg(target_os = "linux")]
const SOL_SOCKET: c_int = 1;
#[cfg(not(target_os = "linux"))]
const SOL_SOCKET: c_int = 0xffff;
#[cfg(target_os = "linux")]
const SO_REUSEADDR: c_int = 2;
#[cfg(not(target_os = "linux"))]
const SO_REUSEADDR: c_int = 0x0004;
#[cfg(target_os = "linux")]
const SO_REUSEPORT: c_int = 15;
#[cfg(not(target_os = "linux"))]
const SO_REUSEPORT: c_int = 0x0200;
#[cfg(target_os = "linux")]
const SO_INCOMING_CPU: c_int = 49;
#[cfg(target_os = "linux")]
const SOL_TCP: c_int = 6;
#[cfg(target_os = "linux")]
const TCP_ULP: c_int = 31;

unsafe extern "C" {
    fn setsockopt(
        fd: c_int,
        level: c_int,
        option_name: c_int,
        option_value: *const c_int,
        option_len: SockLen,
    ) -> c_int;
}

pub(crate) fn set_reuse_addr(fd: c_int) -> io::Result<()> {
    set_int_option(fd, SOL_SOCKET, SO_REUSEADDR, 1)
}

pub(crate) fn set_reuse_port(fd: c_int) -> io::Result<()> {
    set_int_option(fd, SOL_SOCKET, SO_REUSEPORT, 1)
}

#[cfg(target_os = "linux")]
pub(crate) fn set_incoming_cpu(fd: c_int, incoming_cpu: Option<CpuId>) -> io::Result<()> {
    let Some(cpu) = incoming_cpu else {
        return Ok(());
    };
    let cpu: c_int = cpu.0.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "SO_INCOMING_CPU value does not fit platform c_int",
        )
    })?;

    set_int_option(fd, SOL_SOCKET, SO_INCOMING_CPU, cpu)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn set_incoming_cpu(_fd: c_int, _incoming_cpu: Option<CpuId>) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn enable_kernel_tls_ulp(fd: c_int) -> io::Result<()> {
    let name = b"tls\0";
    // SAFETY: `fd` is an open TCP stream descriptor borrowed from TcpStream.
    // TCP_ULP expects a NUL-terminated protocol name buffer; `name` remains
    // live for the duration of the call and the kernel copies the value.
    let result = unsafe {
        setsockopt(
            fd,
            SOL_TCP,
            TCP_ULP,
            name.as_ptr().cast::<c_int>(),
            name.len() as SockLen,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn set_int_option(fd: c_int, level: c_int, option_name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: `fd` is an open socket descriptor and the option accepts a
    // `c_int` value. The pointer and length describe the local `value` for
    // the duration of this call.
    let result = unsafe {
        setsockopt(
            fd,
            level,
            option_name,
            &value,
            std::mem::size_of::<c_int>() as SockLen,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}
