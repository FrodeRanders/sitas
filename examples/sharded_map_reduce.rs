use sitas::{ShardId, ShardedExecutor, current_executor_shard};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let task_submitter = submitter.clone();

    let handle = runtime.spawn_with_handle_on(ShardId(0), async move {
        task_submitter
            .map_reduce_all(
                |shard_id| async move {
                    assert_eq!(current_executor_shard(), Some(shard_id));
                    (shard_id.0 + 1) * 100
                },
                0usize,
                |total, _shard_id, value| total + value,
            )
            .await
    })?;

    let total = sitas::executor::block_on(handle)??;
    println!("map-reduce total: {total}");

    drop(submitter);
    runtime.stop()?;
    Ok(())
}
