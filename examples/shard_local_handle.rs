use sitas::{ShardId, ShardLocal, ShardedExecutor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |_| 0usize);
    let task_counts = local_counts.clone();

    let total =
        sitas::executor::block_on(submitter.submit_with_handle_to(ShardId(0), async move {
            task_counts
                .map_reduce_all(
                    |_shard_id, value| {
                        *value += 1;
                        *value
                    },
                    0usize,
                    |sum, _shard_id, value| sum + value,
                )
                .await
        })?)??;

    println!("total after remote shard-local update: {total}");

    for (shard_id, value) in sitas::executor::block_on(local_counts.map_all(|_, value| *value))? {
        println!("shard {} local value {}", shard_id.0, value);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}
