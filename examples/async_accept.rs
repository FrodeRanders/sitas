use sitas::executor::{accept_async, block_on, read_exact_async};
use std::io::Write;
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

    let (peer, byte) = block_on(async move {
        let (mut stream, peer) = accept_async(&listener).await?;
        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await?;
        Ok::<_, std::io::Error>((peer, byte[0]))
    })?;

    println!("accepted {peer}, read byte: {}", byte as char);

    Ok(())
}
