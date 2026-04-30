use sitas::executor::{accept_async, block_on, read_exact_async, write_all_async};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;

    let client = thread::spawn(move || -> std::io::Result<u8> {
        thread::sleep(Duration::from_millis(25));
        let mut stream = TcpStream::connect(address)?;
        stream.write_all(b"x")?;

        let mut echo = [0u8; 1];
        stream.read_exact(&mut echo)?;
        Ok(echo[0])
    });

    let peer = block_on(async move {
        let (mut stream, peer) = accept_async(&listener).await?;
        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await?;
        write_all_async(&mut stream, &byte).await?;
        Ok::<_, std::io::Error>(peer)
    })?;

    let echoed = client.join().expect("client thread panicked")?;
    println!("echoed byte to {peer}: {}", echoed as char);

    Ok(())
}
