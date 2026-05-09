use sitas::{ShardLocal, ShardedExecutor, join_all_shards};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

    let handles = local_counts.with_all(|shard_id, value| {
        *value += 100;
        format!("shard {} local value {}", shard_id.0, *value)
    })?;

    for (shard_id, message) in sitas::executor::block_on(join_all_shards(handles))? {
        println!("{}: {message}", shard_id.0);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
