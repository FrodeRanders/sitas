use sitas::executor::{block_on, read_exact_async};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let (mut reader, mut writer) = UnixStream::pair()?;
    reader.set_nonblocking(true)?;

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        writer.write_all(b"x").unwrap();
    });

    block_on(async move {
        let mut byte = [0u8; 1];
        read_exact_async(&mut reader, &mut byte).await?;
        println!("read byte: {}", byte[0] as char);

        Ok::<(), std::io::Error>(())
    })
}
