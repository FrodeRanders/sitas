use sitas::executor::{
    accept_async, connect_async, executor_and_spawner, read_exact_async, write_all_async,
};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;

    let (executor, spawner) = executor_and_spawner();
    let output = Arc::new(Mutex::new(None));

    let server = spawner
        .spawn_with_handle(async move {
            let (mut stream, peer) = accept_async(&listener).await?;
            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await?;
            write_all_async(&mut stream, &byte).await?;
            Ok::<SocketAddr, std::io::Error>(peer)
        })
        .unwrap();

    let client = spawner
        .spawn_with_handle(async move {
            let mut stream = connect_async(address).await?;
            write_all_async(&mut stream, b"x").await?;

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await?;
            Ok::<u8, std::io::Error>(byte[0])
        })
        .unwrap();

    let output_for_task = Arc::clone(&output);
    spawner
        .spawn(async move {
            let peer = server.await.unwrap();
            let echoed = client.await.unwrap();
            *output_for_task.lock().unwrap() = Some((peer, echoed));
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let (peer, echoed) = output.lock().unwrap().take().unwrap();
    println!("same-executor TCP echo from {peer}: {}", echoed as char);

    Ok(())
}
