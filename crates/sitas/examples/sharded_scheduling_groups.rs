//! Demonstrates scheduling groups across executor shards.
//!
//! A sharded scheduling group is one executor-local group per shard. This
//! example first spawns grouped work directly from the runtime owner, then has a
//! task on shard 0 submit grouped work to every shard through `ShardedSubmitter`.

mod support;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sitas::executor::{block_on, yield_now};
use sitas::{ExecutorSnapshot, ShardId, ShardedExecutor, current_executor_shard, join_all_shards};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("sharded_scheduling_groups");
    let runtime = ShardedExecutor::start(3)?;
    let foreground = runtime.create_scheduling_group_on_all("foreground", 100)?;
    let background = runtime.create_scheduling_group_on_all("background", 25)?;

    let foreground_handles = runtime.spawn_with_handle_named_in_group_on_all(
        &foreground,
        |shard_id| format!("foreground-local-{}", shard_id.0),
        |shard_id| async move { local_work(shard_id, WorkKind::Foreground).await },
    )?;

    let submitter = runtime.submitter();
    let task_submitter = submitter.clone();
    let task_background = background.clone();
    let submitter_handle =
        runtime.spawn_with_handle_named_on(ShardId(0), "submit-background-fanout", async move {
            let handles = task_submitter
                .submit_with_handle_named_in_group_to_all(
                    &task_background,
                    |shard_id| format!("background-submitted-{}", shard_id.0),
                    |shard_id| async move { local_work(shard_id, WorkKind::Background).await },
                )
                .expect("background fan-out submit succeeds");

            join_all_shards(handles)
                .await
                .expect("background fan-out joins")
        })?;

    let foreground_results = block_on(join_all_shards(foreground_handles))?;
    let background_results = block_on(submitter_handle)?;

    println!("foreground results:");
    print_results(&foreground_results);
    println!();
    println!("background results submitted from shard 0:");
    print_results(&background_results);
    println!();
    print_group_snapshots(&runtime.snapshot());

    drop(submitter);
    runtime.stop()?;
    Ok(())
}

async fn local_work(expected_shard: ShardId, kind: WorkKind) -> WorkResult {
    let started = Instant::now();
    let deadline = started + kind.duration();
    let mut chunks = 0u64;

    while Instant::now() < deadline {
        compute_chunk(kind.chunk_duration());
        chunks += 1;
        yield_now().await;
    }

    WorkResult {
        shard_id: current_executor_shard().unwrap_or(expected_shard),
        label: kind.label(),
        chunks,
        elapsed: started.elapsed(),
    }
}

fn compute_chunk(duration: Duration) {
    let started = Instant::now();
    let mut x = 1.0001f64;
    while started.elapsed() < duration {
        x = (x.cos().abs() + 1.0).ln();
    }
    black_box(x);
}

fn print_results(results: &[(ShardId, WorkResult)]) {
    println!(
        "{:>5} {:>10} {:>8} {:>12}",
        "shard", "group", "chunks", "elapsed(ms)"
    );
    for (joined_shard, result) in results {
        println!(
            "{:>5} {:>10} {:>8} {:>12}",
            joined_shard.0,
            result.label,
            result.chunks,
            result.elapsed.as_millis()
        );
        assert_eq!(*joined_shard, result.shard_id);
    }
}

fn print_group_snapshots(snapshot: &sitas::ShardedExecutorSnapshot) {
    println!("scheduling group snapshots:");
    for shard in &snapshot.shards {
        let Some(executor) = shard.executor.as_ref() else {
            println!("  shard {} stopped", shard.shard_id.0);
            continue;
        };

        print_executor_groups(shard.shard_id, executor);
    }
}

fn print_executor_groups(shard_id: ShardId, executor: &ExecutorSnapshot) {
    let total_poll_time = executor.total_scheduling_group_poll_time();
    println!(
        "  shard {}: tasks={} task_polls={} group_polls={}",
        shard_id.0,
        executor.task_count,
        executor.total_task_polls,
        executor.total_scheduling_group_polls()
    );
    for group in &executor.scheduling_groups {
        let average_us = group
            .average_poll_time()
            .map_or(0, |duration| duration.as_micros());
        println!(
            "    {:>10}: shares={:>3} polls={:>5} avg_us={:>5} charged_ms={:>4} share={:>5.1}% vruntime={}",
            group.name,
            group.shares,
            group.total_polls,
            average_us,
            group.total_poll_time.as_millis(),
            group.poll_time_share_of(total_poll_time) * 100.0,
            group.virtual_runtime
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum WorkKind {
    Foreground,
    Background,
}

impl WorkKind {
    fn label(self) -> &'static str {
        match self {
            WorkKind::Foreground => "foreground",
            WorkKind::Background => "background",
        }
    }

    fn duration(self) -> Duration {
        match self {
            WorkKind::Foreground => Duration::from_millis(90),
            WorkKind::Background => Duration::from_millis(90),
        }
    }

    fn chunk_duration(self) -> Duration {
        match self {
            WorkKind::Foreground => Duration::from_micros(500),
            WorkKind::Background => Duration::from_micros(500),
        }
    }
}

#[derive(Debug)]
struct WorkResult {
    shard_id: ShardId,
    label: &'static str,
    chunks: u64,
    elapsed: Duration,
}
