use sitas::{ShardId, ShardedExecutor, current_executor_shard};
use std::sync::mpsc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let (sender, receiver) = mpsc::sync_channel(4);

    for shard_idx in 0..runtime.shard_count() {
        let sender = sender.clone();
        runtime.spawn_on(ShardId(shard_idx), async move {
            sender
                .send(current_executor_shard().expect("task is running on a shard"))
                .expect("receiver is alive");
        })?;
    }

    drop(sender);

    let mut shards = receiver.into_iter().collect::<Vec<_>>();
    shards.sort_by_key(|shard_id| shard_id.0);

    for shard_id in shards {
        println!("task ran on shard {}", shard_id.0);
    }

    runtime.stop()?;
    Ok(())
}
