use sitas::{
    ShardLocal, ShardedExecutor, TaskSnapshot, TaskStatus, TaskWait, executor::sleep,
    join_all_shards,
};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(2)?;
    let observer = runtime.observer();
    let submitter = runtime.submitter();
    let local_counts = ShardLocal::new(submitter.clone(), |_| 0usize);

    let handles = local_counts.spawn_named_workers(
        |shard_id| format!("local-worker-{}", shard_id.0),
        |_shard_id, task_counts| async move {
            for _ in 0..3 {
                let (_current_shard, _value) =
                    task_counts.with_current(|_current_shard, value| {
                        *value += 1;
                        *value
                    })?;
                sleep(Duration::from_millis(30)).await;
            }
            task_counts.with_current(|_current_shard, value| *value)
        },
    )?;

    for sample_idx in 0..4 {
        std::thread::sleep(Duration::from_millis(20));
        let snapshot = observer.snapshot();

        println!("sample {sample_idx}: running={}", snapshot.running);
        for shard in &snapshot.shards {
            let Some(executor) = shard.executor.as_ref() else {
                println!("  shard {} stopped", shard.shard_id.0);
                continue;
            };

            println!(
                "  shard {}: ready={} tasks={} timers={}",
                shard.shard_id.0,
                executor.ready_queue_len,
                executor.task_count,
                executor.timer_count
            );

            for task in &executor.tasks {
                println!(
                    "    task {} {} status={} polls={} wait={}",
                    task.id.0,
                    task.name.as_deref().unwrap_or("<unnamed>"),
                    status_name(task.status),
                    task.poll_count,
                    wait_name(task)
                );
            }
        }
    }

    for (shard_id, output) in sitas::executor::block_on(join_all_shards(handles))? {
        let (_current_shard, value) = output?;
        println!("shard {} final value {}", shard_id.0, value);
    }

    drop(local_counts);
    drop(submitter);
    runtime.stop()?;
    Ok(())
}

fn status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Polling => "polling",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Completed => "completed",
        TaskStatus::Cancelled => "cancelled",
    }
}

fn wait_name(task: &TaskSnapshot) -> &'static str {
    match task.waiting_for {
        Some(TaskWait::Unknown) => "unknown",
        Some(TaskWait::Timer { .. }) => "timer",
        Some(TaskWait::Readable { .. }) => "readable",
        Some(TaskWait::Writable { .. }) => "writable",
        None => "none",
    }
}
