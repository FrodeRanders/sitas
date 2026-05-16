//! Creates one value per executor shard and mutates it through routed access.
//!
//! `ShardLocal` is the core shared-nothing pattern: each shard owns its value,
//! and callers interact with it by submitting synchronous closures to the owner.
use sitas::{ShardLocal, ShardedExecutor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

    let outputs = sitas::executor::block_on(local_counts.map_all(|shard_id, value| {
        *value += 100;
        format!("shard {} local value {}", shard_id.0, *value)
    }))?;

    for (shard_id, message) in outputs {
        println!("{}: {message}", shard_id.0);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
