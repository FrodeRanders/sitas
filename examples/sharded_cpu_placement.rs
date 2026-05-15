use sitas::{
    ShardId, ShardedExecutor, available_cpu_ids, current_executor_cpu_placement,
    current_executor_shard, executor::block_on,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("available CPUs: {:?}", available_cpu_ids());

    let runtime = ShardedExecutor::start_pinned_on_available_cpus().unwrap();

    for shard in runtime.snapshot().shards {
        println!(
            "shard {} ({}) CPU placement: {}",
            shard.shard_id.0, shard.thread_name, shard.cpu_placement
        );
    }

    for shard_idx in 0..runtime.shard_count() {
        let handle = runtime.spawn_with_handle_on(ShardId(shard_idx), async move {
            (
                current_executor_shard().unwrap(),
                current_executor_cpu_placement().unwrap(),
            )
        })?;
        let (shard_id, cpu_placement) = block_on(handle)?;

        println!(
            "task on shard {} observed CPU placement: {}",
            shard_id.0, cpu_placement
        );
    }

    runtime.stop()?;
    Ok(())
}
