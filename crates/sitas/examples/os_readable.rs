//! Waits for file-descriptor readability through the OS reactor.
//!
//! This sits below the executor layer and shows the raw readiness primitive
//! that `readable` and the TCP helpers are built on.
mod support;
use sitas::os::OsReactor;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    support::announce("os_readable");
    let reactor = OsReactor::new()?;
    let (reader, mut writer) = UnixStream::pair()?;

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        writer.write_all(b"x").unwrap();
    });

    let event = reactor.wait_readable(&[reader.as_raw_fd()], Some(Duration::from_secs(1)))?;
    println!("readable fds: {:?}", event.readable);

    Ok(())
}
