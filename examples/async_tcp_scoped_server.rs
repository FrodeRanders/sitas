//! Uses scoped TCP handler shutdown with a stop token.
//!
//! The handler receives its own stop future so server shutdown can first ask
//! children to finish cooperatively, then bound that wait with a timeout.
mod support;
use sitas::executor::{
    executor_and_spawner, serve_tcp_until_stopped_scoped_timeout, sleep, stop_pair, write_all_async,
};
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    support::announce("async_tcp_scoped_server");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (stop_source, stop_token) = stop_pair();

    let client = thread::spawn(move || -> std::io::Result<u8> {
        let mut stream = TcpStream::connect(address)?;
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        Ok(byte[0])
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(25)).await;
            stop_source.stop();
        })
        .unwrap();

    let accepted = executor.run_until(async move {
        serve_tcp_until_stopped_scoped_timeout(
            listener,
            server_spawner,
            stop_token,
            Duration::from_secs(1),
            |mut stream, _peer, handler_stop| async move {
                handler_stop.await;
                write_all_async(&mut stream, b"x").await
            },
        )
        .await
    })?;

    drop(spawner);

    let echoed = client.join().expect("client thread panicked")?;
    println!(
        "scoped server accepted {accepted}, handler stopped with byte: {}",
        char::from(echoed)
    );
    Ok(())
}
