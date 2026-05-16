//! Submits work from one shard to another and awaits the remote result.
//!
//! The awaiting task resumes on its original shard, which is the key affinity
//! rule that keeps shard-local reasoning tractable.
use sitas::{ShardId, ShardedExecutor, current_executor_shard};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(2)?;
    let submitter = runtime.submitter();
    let task_submitter = submitter.clone();

    let handle = runtime.spawn_with_handle_on(ShardId(0), async move {
        let origin = current_executor_shard().expect("origin task is running on a shard");

        let remote = task_submitter
            .submit_with_handle_named_to(ShardId(1), "remote-calculation", async {
                current_executor_shard().expect("remote task is running on a shard")
            })
            .expect("remote work was submitted");

        let remote_shard = remote.await.expect("remote work completed");
        (origin, current_executor_shard().unwrap(), remote_shard)
    })?;

    let (origin, resumed, remote) = sitas::executor::block_on(handle)?;
    println!(
        "origin shard {}, resumed on shard {}, remote work ran on shard {}",
        origin.0, resumed.0, remote.0
    );

    drop(submitter);
    runtime.stop()?;
    Ok(())
}
