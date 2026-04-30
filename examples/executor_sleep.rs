use sitas::executor::{block_on, sleep};
use std::time::{Duration, Instant};

fn main() {
    let started = Instant::now();

    block_on(async {
        sleep(Duration::from_millis(25)).await;
    });

    println!(
        "slept for at least 25ms: {}",
        started.elapsed() >= Duration::from_millis(25)
    );
}
