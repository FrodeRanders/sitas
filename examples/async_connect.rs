//! Opens a TCP client connection with the custom executor.
//!
//! The server stays on a plain blocking thread so the example isolates the
//! async client-side connect/read/write path without needing a second executor.
mod support;
use sitas::executor::{block_on, connect_async, read_exact_async, write_all_async};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() -> std::io::Result<()> {
    support::announce("async_connect");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;

    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, peer) = listener.accept()?;
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        stream.write_all(&byte)?;
        println!("server echoed byte for {peer}");
        Ok(())
    });

    let echoed = block_on(async move {
        let mut stream = connect_async(address).await?;
        write_all_async(&mut stream, b"x").await?;

        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await?;
        Ok::<_, std::io::Error>(byte[0])
    })?;

    server.join().expect("server thread panicked")?;
    println!("client received echo: {}", echoed as char);

    Ok(())
}
