//! Stops shard-local workers with a bounded join.
//!
//! The timeout variant is important for service shutdown because cooperative
//! stop requests should not be able to hang the caller forever.
use sitas::{ShardLocal, ShardedExecutor, executor::sleep};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(2)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |_| 0usize);

    let workers =
        local_counts.spawn_stoppable_workers(|_shard_id, task_counts, stop| async move {
            let (_current_shard, value) = task_counts.with_current(|_current_shard, value| {
                *value += 1;
                *value
            })?;

            stop.await;
            sleep(Duration::from_millis(10)).await;

            Ok::<usize, sitas::ShardLocalAccessError>(value)
        })?;

    let outputs = sitas::executor::block_on(workers.stop_and_join_timeout(Duration::from_secs(1)))?;

    for (shard_id, output) in outputs {
        println!("shard {} stopped with value {}", shard_id.0, output?);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
