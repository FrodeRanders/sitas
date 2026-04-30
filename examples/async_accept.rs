use sitas::executor::{accept_async, block_on};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(b"x").unwrap();
    });

    let (mut stream, peer) = block_on(async move { accept_async(&listener).await })?;
    let mut byte = [0u8; 1];
    stream.read_exact(&mut byte)?;
    println!("accepted {peer}, read byte: {}", byte[0] as char);

    Ok(())
}
