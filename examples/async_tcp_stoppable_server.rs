//! Runs a TCP server until a cooperative stop token fires.
//!
//! This is closer to a service lifecycle than the fixed-count examples: work
//! continues until another task decides the runtime should stop accepting.
use sitas::executor::{
    executor_and_spawner, read_exact_async, serve_tcp_until_stopped_timeout, sleep, stop_pair,
    write_all_async,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

const CLIENT_COUNT: u8 = 3;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (stop_source, stop_token) = stop_pair();

    let clients = (0..CLIENT_COUNT)
        .map(|value| {
            thread::spawn(move || -> std::io::Result<u8> {
                thread::sleep(Duration::from_millis(25 + u64::from(value) * 5));
                let mut stream = TcpStream::connect(address)?;
                stream.write_all(&[b'a' + value])?;

                let mut echo = [0u8; 1];
                stream.read_exact(&mut echo)?;
                Ok(echo[0])
            })
        })
        .collect::<Vec<_>>();

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(75)).await;
            stop_source.stop();
        })
        .unwrap();

    let accepted = executor.run_until(async move {
        serve_tcp_until_stopped_timeout(
            listener,
            server_spawner,
            stop_token,
            Duration::from_secs(1),
            |mut stream, _peer| async move {
                let mut byte = [0u8; 1];
                read_exact_async(&mut stream, &mut byte).await?;
                write_all_async(&mut stream, &byte).await
            },
        )
        .await
    })?;

    drop(spawner);

    let mut echoed = clients
        .into_iter()
        .map(|client| client.join().expect("client thread panicked"))
        .collect::<std::io::Result<Vec<_>>>()?;
    echoed.sort();

    println!(
        "stopped after {accepted}, echoed bytes: {}",
        String::from_utf8_lossy(&echoed)
    );
    Ok(())
}
