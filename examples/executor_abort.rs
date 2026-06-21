//! Demonstrates task abortion through a join handle.
//!
//! The task yields in a loop so cancellation can be observed at an executor
//! polling boundary instead of relying on a blocking sleep.
mod support;
use std::time::Duration;

use sitas::executor::{block_on, executor_and_spawner, sleep, yield_now};

fn main() {
    support::announce("executor_abort");
    let (executor, spawner) = executor_and_spawner();

    let worker = spawner
        .spawn_with_handle(async {
            sleep(Duration::from_secs(60)).await;
            "finished"
        })
        .unwrap();

    spawner
        .spawn(async move {
            yield_now().await;
            println!("aborted: {}", worker.abort());
            println!("cancelled: {}", worker.await.unwrap_err().is_cancelled());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let output = block_on(async { "root futures still complete" });
    println!("{output}");
}
