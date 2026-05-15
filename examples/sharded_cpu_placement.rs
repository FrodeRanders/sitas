use sitas::{CpuPlacement, ShardedExecutor, ShardedExecutorConfig, available_cpu_ids};

fn main() {
    println!("available CPUs: {:?}", available_cpu_ids());

    let runtime = ShardedExecutor::start_with_config(
        ShardedExecutorConfig::for_available_parallelism()
            .with_cpu_placement(CpuPlacement::Sequential),
    )
    .unwrap();

    for shard in runtime.snapshot().shards {
        println!(
            "shard {} ({}) CPU placement: {}",
            shard.shard_id.0, shard.thread_name, shard.cpu_placement
        );
    }

    runtime.stop().unwrap();
}
