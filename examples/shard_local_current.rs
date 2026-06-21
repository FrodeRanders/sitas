//! Uses direct shard-local access from code already running on the owner shard.
//!
//! `with_current` avoids a round-trip through the submitter, but only works
//! because the task is already executing on the shard that owns the value.
mod support;
use sitas::{ShardId, ShardLocal, ShardedExecutor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("shard_local_current");
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |_| 0usize);
    let task_counts = local_counts.clone();

    let (current_shard, (reported_shard, value)) =
        sitas::executor::block_on(submitter.submit_with_handle_to(ShardId(2), async move {
            task_counts.with_current(|shard_id, value| {
                *value += 42;
                (shard_id, *value)
            })
        })?)??;

    println!(
        "direct current-shard update ran on shard {}, reported shard {}, value {}",
        current_shard.0, reported_shard.0, value
    );

    for (shard_id, value) in sitas::executor::block_on(local_counts.map_all(|_, value| *value))? {
        println!("shard {} local value {}", shard_id.0, value);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
