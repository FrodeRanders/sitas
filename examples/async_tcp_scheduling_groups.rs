//! Runs TCP handlers in explicit scheduling groups.
//!
//! The accept loops stay in ordinary executor tasks. Each accepted connection is
//! handed to a handler task in the selected scheduling group, which lets network
//! services classify accepted work without changing the listener/accept code.

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use sitas::executor::{
    JoinError, Notify, executor_and_spawner, serve_tcp_n_in_group, write_all_async, yield_now,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let foreground_listener = TcpListener::bind("127.0.0.1:0")?;
    let foreground_address = foreground_listener.local_addr()?;
    let background_listener = TcpListener::bind("127.0.0.1:0")?;
    let background_address = background_listener.local_addr()?;

    let foreground_client = spawn_client(foreground_address);
    let background_client = spawn_client(background_address);

    let (executor, spawner) = executor_and_spawner();
    let foreground = spawner.create_scheduling_group("foreground-tcp", 100)?;
    let background = spawner.create_scheduling_group("background-tcp", 25)?;

    let started = Arc::new(Mutex::new(0usize));
    let release_handlers = Notify::new();

    let foreground_handle = {
        let server_spawner = spawner.clone();
        let group = foreground.clone();
        let started = Arc::clone(&started);
        let release_handlers = release_handlers.clone();
        spawner.spawn_with_handle(async move {
            serve_tcp_n_in_group(
                foreground_listener,
                server_spawner,
                &group,
                1,
                move |mut stream, _peer| {
                    let started = Arc::clone(&started);
                    let release_handlers = release_handlers.clone();
                    async move {
                        mark_started(&started);
                        release_handlers.notified().await;
                        write_all_async(&mut stream, b"f").await
                    }
                },
            )
            .await
        })?
    };

    let background_handle = {
        let server_spawner = spawner.clone();
        let group = background.clone();
        let started = Arc::clone(&started);
        let release_handlers = release_handlers.clone();
        spawner.spawn_with_handle(async move {
            serve_tcp_n_in_group(
                background_listener,
                server_spawner,
                &group,
                1,
                move |mut stream, _peer| {
                    let started = Arc::clone(&started);
                    let release_handlers = release_handlers.clone();
                    async move {
                        mark_started(&started);
                        release_handlers.notified().await;
                        write_all_async(&mut stream, b"b").await
                    }
                },
            )
            .await
        })?
    };

    let snapshot = executor.run_until(async {
        wait_for_started(&started, 2).await;
        executor_snapshot(&spawner)
    });

    println!("live task scheduling groups:");
    for task in &snapshot.tasks {
        let group_name = task.scheduling_group_name.as_deref().unwrap_or("<unknown>");
        println!(
            "  task {:>2} {:<24} status={:?}",
            task.id.0, group_name, task.status
        );
    }

    let result = executor.run_until(async move {
        release_handlers.notify_waiters();
        foreground_handle.await??;
        background_handle.await??;
        Ok::<_, ExampleError>(())
    });
    result?;

    drop(spawner);
    executor.run();

    println!(
        "client bytes: {}{}",
        char::from(
            foreground_client
                .join()
                .expect("foreground client panicked")?
        ),
        char::from(
            background_client
                .join()
                .expect("background client panicked")?
        )
    );

    Ok(())
}

fn spawn_client(address: std::net::SocketAddr) -> thread::JoinHandle<std::io::Result<u8>> {
    thread::spawn(move || {
        let mut stream = TcpStream::connect(address)?;
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        Ok(byte[0])
    })
}

fn mark_started(started: &Arc<Mutex<usize>>) {
    *started.lock().expect("started counter poisoned") += 1;
}

async fn wait_for_started(started: &Arc<Mutex<usize>>, expected: usize) {
    while *started.lock().expect("started counter poisoned") < expected {
        yield_now().await;
    }
}

fn executor_snapshot(spawner: &sitas::executor::Spawner) -> sitas::ExecutorSnapshot {
    spawner.snapshot()
}

#[derive(Debug)]
enum ExampleError {
    Io(std::io::Error),
    Join(JoinError),
}

impl std::fmt::Display for ExampleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExampleError::Io(error) => write!(f, "{error}"),
            ExampleError::Join(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ExampleError {}

impl From<std::io::Error> for ExampleError {
    fn from(error: std::io::Error) -> Self {
        ExampleError::Io(error)
    }
}

impl From<JoinError> for ExampleError {
    fn from(error: JoinError) -> Self {
        ExampleError::Join(error)
    }
}
