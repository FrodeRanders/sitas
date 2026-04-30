use sitas::executor::{block_on, readable};
use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let (mut reader, mut writer) = UnixStream::pair()?;
    reader.set_nonblocking(true)?;
    let reader_fd = reader.as_raw_fd();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        writer.write_all(b"x").unwrap();
    });

    block_on(async move {
        readable(reader_fd).await;

        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        println!("read byte: {}", byte[0] as char);

        Ok::<(), std::io::Error>(())
    })
}
