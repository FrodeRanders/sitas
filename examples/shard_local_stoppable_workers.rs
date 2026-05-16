//! Runs one cooperative worker per shard-local value.
//!
//! The stop token gives each worker a normal async way to leave its loop before
//! the owning runtime is stopped.
use sitas::{ShardLocal, ShardedExecutor, executor::sleep};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |_| 0usize);

    let workers = local_counts.spawn_named_stoppable_workers(
        |shard_id| format!("stoppable-local-worker-{}", shard_id.0),
        |_expected_shard, task_counts, stop| async move {
            let mut ticks = 0usize;
            while !stop.is_stopped() {
                let (_current_shard, value) =
                    task_counts.with_current(|_current_shard, value| {
                        *value += 1;
                        *value
                    })?;
                ticks = value;
                sleep(Duration::from_millis(10)).await;
            }
            Ok::<usize, sitas::ShardLocalAccessError>(ticks)
        },
    )?;

    std::thread::sleep(Duration::from_millis(35));
    let outputs = sitas::executor::block_on(workers.stop_and_join())?;

    for (shard_id, output) in outputs {
        println!("shard {} stopped after {} ticks", shard_id.0, output?);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
