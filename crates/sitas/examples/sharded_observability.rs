//! Samples runtime snapshots while named shard tasks are sleeping and waking.
//!
//! This is the lightweight alternative to a tracing UI for now: owned snapshots
//! expose queues, waits, polls, and counters without keeping shards alive.
mod support;
use sitas::{
    ExecutorSnapshot, ShardedExecutor, TaskSnapshot, TaskStatus, TaskWait, current_executor_shard,
    executor::sleep,
};
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("sharded_observability");
    let runtime = ShardedExecutor::start(2)?;
    let observer = runtime.observer();

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
        let snapshot = observer.snapshot();
        let now = Instant::now();

        println!("sample {sample_idx}: running={}", snapshot.running);
        for shard in &snapshot.shards {
            let Some(executor) = shard.executor.as_ref() else {
                println!("  shard {} stopped", shard.shard_id.0);
                continue;
            };

            println!(
                "  shard {}: ready={} tasks={} timers={} read={} write={} task_polls={} group_polls={} done={} budget_hits={}",
                shard.shard_id.0,
                executor.ready_queue_len,
                executor.task_count,
                executor.timer_count,
                executor.read_interest_count,
                executor.write_interest_count,
                executor.total_task_polls,
                executor.total_scheduling_group_polls(),
                executor.total_completed_tasks,
                executor.ready_poll_budget_exhaustions
            );
            print_scheduling_groups(executor);
            print_io_uring(executor);

            for task in &executor.tasks {
                println!(
                    "    task {} {} status={} age_ms={} state_ms={} polls={} wait={}",
                    task.id.0,
                    task.name.as_deref().unwrap_or("<unnamed>"),
                    status_name(task.status),
                    task.age_at(now).as_millis(),
                    task.state_duration_at(now).as_millis(),
                    task.poll_count,
                    wait_name(task)
                );
            }
        }
    }

    runtime.stop()?;
    Ok(())
}

fn print_scheduling_groups(executor: &ExecutorSnapshot) {
    let total_poll_time = executor.total_scheduling_group_poll_time();
    for group in &executor.scheduling_groups {
        if group.total_polls == 0 && group.ready_queue_len == 0 {
            continue;
        }

        println!(
            "      group {} shares={} ready={} polls={} avg_us={} share={:.1}%",
            group.name,
            group.shares,
            group.ready_queue_len,
            group.total_polls,
            group
                .average_poll_time()
                .map_or(0, |duration| duration.as_micros()),
            group.poll_time_share_of(total_poll_time) * 100.0
        );
    }
}

fn print_io_uring(executor: &ExecutorSnapshot) {
    #[cfg(target_os = "linux")]
    if let Some(io_uring) = &executor.io_uring {
        println!(
            "      uring: submit={} tracked={} buffered={} wakers={} dispatched={} reads={} writes={}",
            io_uring.ring.pending_submissions,
            io_uring.ring.tracked_operations,
            io_uring.completed_operations,
            io_uring.registered_wakers,
            io_uring.total_dispatched_operations,
            io_uring.total_dispatched_operation_kinds.reads,
            io_uring.total_dispatched_operation_kinds.writes
        );
    }

    #[cfg(not(target_os = "linux"))]
    let _ = executor;
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
