//! Writes asynchronously to a non-blocking Unix stream.
//!
//! The read side stays blocking because the lesson is only that executor
//! write readiness can drive `write_all_async` to completion.
use sitas::executor::{block_on, write_all_async};
use std::io::Read;
use std::os::unix::net::UnixStream;

fn main() -> std::io::Result<()> {
    let (mut reader, mut writer) = UnixStream::pair()?;
    writer.set_nonblocking(true)?;

    block_on(async move {
        write_all_async(&mut writer, b"x").await?;
        Ok::<(), std::io::Error>(())
    })?;

    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte)?;
    println!("wrote byte: {}", byte[0] as char);

    Ok(())
}
