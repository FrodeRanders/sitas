#![cfg(unix)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::os::tcp_connect_start;

use super::scheduler::{Scheduler, current_scheduler};
use super::{TimeoutError, timeout};

/// Returns a future that completes when `fd` is readable.
pub fn readable(fd: RawFd) -> Readable {
    Readable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Returns a future that completes when `fd` is writable.
pub fn writable(fd: RawFd) -> Writable {
    Writable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Reads exactly enough bytes to fill `buffer`, awaiting read readiness when
/// the reader would otherwise block.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
pub async fn read_exact_async<R>(reader: &mut R, buffer: &mut [u8]) -> io::Result<()>
where
    R: Read + AsRawFd,
{
    let mut filled = 0usize;

    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "reader reached EOF before buffer was filled",
                ));
            }
            Ok(bytes_read) => {
                filled += bytes_read;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(reader.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Reads exactly enough bytes to fill `buffer`, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
pub async fn read_exact_timeout_async<R>(
    reader: &mut R,
    buffer: &mut [u8],
    duration: Duration,
) -> io::Result<()>
where
    R: Read + AsRawFd,
{
    timeout_io(duration, read_exact_async(reader, buffer)).await
}

/// Writes the entire buffer, awaiting write readiness when the writer would
/// otherwise block.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
pub async fn write_all_async<W>(writer: &mut W, buffer: &[u8]) -> io::Result<()>
where
    W: Write + AsRawFd,
{
    let mut written = 0usize;

    while written < buffer.len() {
        match writer.write(&buffer[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "writer accepted zero bytes before buffer was written",
                ));
            }
            Ok(bytes_written) => {
                written += bytes_written;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                writable(writer.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Writes the entire buffer, failing with `io::ErrorKind::TimedOut` if
/// `duration` elapses first.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
pub async fn write_all_timeout_async<W>(
    writer: &mut W,
    buffer: &[u8],
    duration: Duration,
) -> io::Result<()>
where
    W: Write + AsRawFd,
{
    timeout_io(duration, write_all_async(writer, buffer)).await
}

/// Copies bytes from `reader` to `writer` until `reader` reaches EOF, awaiting
/// descriptor readiness whenever either side would otherwise block.
///
/// The caller is responsible for putting both underlying descriptors in
/// non-blocking mode before using this helper.
pub async fn copy_async<R, W>(reader: &mut R, writer: &mut W, buffer: &mut [u8]) -> io::Result<u64>
where
    R: Read + AsRawFd,
    W: Write + AsRawFd,
{
    if buffer.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy buffer must not be empty",
        ));
    }

    let mut copied = 0u64;

    loop {
        match reader.read(buffer) {
            Ok(0) => return Ok(copied),
            Ok(bytes_read) => {
                write_all_async(writer, &buffer[..bytes_read]).await?;
                copied += bytes_read as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(reader.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Copies bytes from `reader` to `writer`, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The caller is responsible for putting both underlying descriptors in
/// non-blocking mode before using this helper.
pub async fn copy_timeout_async<R, W>(
    reader: &mut R,
    writer: &mut W,
    buffer: &mut [u8],
    duration: Duration,
) -> io::Result<u64>
where
    R: Read + AsRawFd,
    W: Write + AsRawFd,
{
    timeout_io(duration, copy_async(reader, writer, buffer)).await
}

/// Accepts one TCP connection, awaiting listener readiness when accepting
/// would otherwise block.
///
/// The caller is responsible for putting the listener in non-blocking mode
/// before using this helper. The returned stream is placed in non-blocking mode
/// before it is returned.
pub async fn accept_async(listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                stream.set_nonblocking(true)?;
                return Ok((stream, peer));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(listener.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Accepts one TCP connection, failing with `io::ErrorKind::TimedOut` if
/// `duration` elapses first.
///
/// The caller is responsible for putting the listener in non-blocking mode
/// before using this helper. The returned stream is placed in non-blocking mode
/// before it is returned.
pub async fn accept_timeout_async(
    listener: &TcpListener,
    duration: Duration,
) -> io::Result<(TcpStream, SocketAddr)> {
    timeout_io(duration, accept_async(listener)).await
}

/// Connects to a TCP address without blocking the executor.
///
/// The returned stream is non-blocking.
pub async fn connect_async(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = tcp_connect_start(address)?;
    writable(stream.as_raw_fd()).await;

    match stream.take_error()? {
        Some(error) => Err(error),
        None => Ok(stream),
    }
}

/// Connects to a TCP address without blocking the executor, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The returned stream is non-blocking.
pub async fn connect_timeout_async(
    address: SocketAddr,
    duration: Duration,
) -> io::Result<TcpStream> {
    timeout_io(duration, connect_async(address)).await
}

async fn timeout_io<F, T>(duration: Duration, future: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout(duration, future).await {
        Ok(result) => result,
        Err(TimeoutError) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "async operation timed out",
        )),
    }
}

/// Future returned by [`readable`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Readable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

impl Future for Readable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_read_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_read_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_read_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for Readable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_read_interest(interest_id);
        }
    }
}

/// Future returned by [`writable`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Writable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

impl Future for Writable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_write_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_write_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_write_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for Writable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_write_interest(interest_id);
        }
    }
}
