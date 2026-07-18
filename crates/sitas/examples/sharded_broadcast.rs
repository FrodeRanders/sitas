//! Broadcasts work from one shard to every shard.
//!
//! The origin task submits explicit remote futures instead of reaching into
//! other shards, making cross-shard fan-out visible in the code.
mod support;
use sitas::{ShardId, ShardedExecutor, current_executor_shard, join_all_shards};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("sharded_broadcast");
    let runtime = ShardedExecutor::start(4)?;
    let submitter = runtime.submitter();
    let task_submitter = submitter.clone();

    let handle = runtime.spawn_with_handle_on(ShardId(0), async move {
        let handles = task_submitter
            .submit_with_handle_named_to_all(
                |shard_id| format!("broadcast-{}", shard_id.0),
                |shard_id| async move {
                    assert_eq!(current_executor_shard(), Some(shard_id));
                    format!("hello from shard {}", shard_id.0)
                },
            )
            .expect("broadcast work was submitted");

        join_all_shards(handles).await
    })?;

    for (shard_id, message) in sitas::executor::block_on(handle)?? {
        println!("shard {} replied: {message}", shard_id.0);
    }

    drop(submitter);
    runtime.stop()?;
    Ok(())
}
