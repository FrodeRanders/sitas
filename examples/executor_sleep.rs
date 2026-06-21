//! Shows the executor timer path with `sleep`.
//!
//! A timer future registers a deadline, yields, and is later woken by the
//! executor rather than by an OS thread sleeping inside the task.
mod support;
use sitas::executor::{block_on, sleep};
use std::time::{Duration, Instant};

fn main() {
    support::announce("executor_sleep");
    let started = Instant::now();

    block_on(async {
        sleep(Duration::from_millis(25)).await;
    });

    println!(
        "slept for at least 25ms: {}",
        started.elapsed() >= Duration::from_millis(25)
    );
}
