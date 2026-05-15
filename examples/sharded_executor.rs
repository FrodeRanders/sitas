use sitas::{ShardId, ShardedExecutor, current_executor_shard};
use std::sync::mpsc;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(4)?;
    let (sender, receiver) = mpsc::sync_channel(4);

    for shard_idx in 0..runtime.shard_count() {
        let sender = sender.clone();
        runtime.spawn_on(ShardId(shard_idx), async move {
            sender
                .send((
                    current_executor_shard().expect("task is running on a shard"),
                    thread::current().name().unwrap_or("unnamed").to_owned(),
                ))
                .expect("receiver is alive");
        })?;
    }

    drop(sender);

    let mut shards = receiver.into_iter().collect::<Vec<_>>();
    shards.sort_by_key(|(shard_id, _)| shard_id.0);

    for (shard_id, thread_name) in shards {
        println!("task ran on shard {} ({thread_name})", shard_id.0);
    }

    runtime.stop()?;
    Ok(())
}
