use sitas::executor::{executor_and_spawner, serve_tcp_until_idle_timeout, sleep};
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;

    let client = thread::spawn(move || -> std::io::Result<std::io::ErrorKind> {
        let mut stream = TcpStream::connect(address)?;
        let mut byte = [0u8; 1];
        Ok(stream.read_exact(&mut byte).unwrap_err().kind())
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();

    let error = executor.run_until(async move {
        serve_tcp_until_idle_timeout(
            listener,
            server_spawner,
            Duration::from_millis(10),
            Duration::from_millis(10),
            |_stream, _peer| async move {
                sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        )
        .await
        .unwrap_err()
    });

    drop(spawner);

    let client_error = client.join().expect("client thread panicked")?;
    println!(
        "idle server shutdown: {server:?}, client observed: {client_error:?}",
        server = error.kind()
    );

    Ok(())
}
