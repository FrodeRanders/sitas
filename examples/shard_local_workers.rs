use sitas::{ShardLocal, ShardedExecutor, join_all_shards};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |shard_id| shard_id.0);

    let handles = local_counts.spawn_workers(|expected_shard, task_counts| async move {
        task_counts.with_current(|current_shard, value| {
            assert_eq!(current_shard, expected_shard);
            *value += 10;
            format!(
                "worker on shard {} updated value to {}",
                current_shard.0, *value
            )
        })
    })?;

    for (shard_id, result) in sitas::executor::block_on(join_all_shards(handles))? {
        let (_current_shard, message) = result?;
        println!("{}: {message}", shard_id.0);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
