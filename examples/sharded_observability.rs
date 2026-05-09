use sitas::{
    ShardedExecutor, TaskSnapshot, TaskStatus, TaskWait, current_executor_shard, executor::sleep,
};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ShardedExecutor::start(2)?;

    for task_idx in 0..4 {
        let shard_id = sitas::ShardId(task_idx % runtime.shard_count());
        runtime.spawn_named_on(shard_id, format!("worker-{task_idx}"), async move {
            for _ in 0..3 {
                let _ = current_executor_shard().expect("task is running on a shard");
                sleep(Duration::from_millis(30)).await;
            }
        })?;
    }

    for sample_idx in 0..5 {
        std::thread::sleep(Duration::from_millis(20));
        let snapshot = runtime.snapshot();

        println!("sample {sample_idx}: running={}", snapshot.running);
        for shard in &snapshot.shards {
            let Some(executor) = shard.executor.as_ref() else {
                println!("  shard {} stopped", shard.shard_id.0);
                continue;
            };

            println!(
                "  shard {}: ready={} tasks={} timers={} read={} write={}",
                shard.shard_id.0,
                executor.ready_queue_len,
                executor.task_count,
                executor.timer_count,
                executor.read_interest_count,
                executor.write_interest_count
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
