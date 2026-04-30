use sitas::executor::{block_on, copy_async};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let (mut source_reader, mut source_writer) = UnixStream::pair()?;
    let (mut sink_reader, mut sink_writer) = UnixStream::pair()?;
    source_reader.set_nonblocking(true)?;
    sink_writer.set_nonblocking(true)?;

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        source_writer.write_all(b"copied").unwrap();
    });

    let copied = block_on(async move {
        let mut buffer = [0u8; 4];
        copy_async(&mut source_reader, &mut sink_writer, &mut buffer).await
    })?;

    let mut output = String::new();
    sink_reader.read_to_string(&mut output)?;
    println!("copied {copied} bytes: {output}");

    Ok(())
}
