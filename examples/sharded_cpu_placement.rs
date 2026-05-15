use sitas::{ShardedExecutor, available_cpu_ids};

fn main() {
    println!("available CPUs: {:?}", available_cpu_ids());

    let runtime = ShardedExecutor::start_pinned_on_available_cpus().unwrap();

    for shard in runtime.snapshot().shards {
        println!(
            "shard {} ({}) CPU placement: {}",
            shard.shard_id.0, shard.thread_name, shard.cpu_placement
        );
    }

    runtime.stop().unwrap();
}
