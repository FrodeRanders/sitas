//! Demonstrates Sitas scheduling groups with CPU-bound cooperative tasks.
//!
//! The first run places all work in the default group. That is the historical
//! FIFO-style baseline. The second run assigns the same work to weighted
//! scheduling groups, so the executor charges actual poll time to each group
//! and prefers the group with the lowest weighted virtual runtime.

mod support;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sitas::executor::{JoinError, SchedulingGroup, Spawner, executor_and_spawner, yield_now};
use sitas::{ExecutorSnapshot, SchedulingGroupSnapshot};

const DEFAULT_SECONDS: u64 = 3;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("scheduling_group_demo");
    let duration = std::env::args()
        .nth(1)
        .map(|arg| arg.parse::<u64>())
        .transpose()?
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SECONDS));

    println!("running default-group baseline for {}s", duration.as_secs());
    let baseline = run_demo(false, duration)?;
    print_rows(&baseline);

    println!();
    println!(
        "running weighted scheduling groups for {}s",
        duration.as_secs()
    );
    let weighted = run_demo(true, duration)?;
    print_rows(&weighted);

    Ok(())
}

fn run_demo(
    weighted_groups: bool,
    duration: Duration,
) -> Result<Vec<GroupResult>, Box<dyn std::error::Error>> {
    let (executor, spawner) = executor_and_spawner();
    let deadline = Instant::now() + duration;

    let sg100 = group(&spawner, weighted_groups, "sg100", 100)?;
    let sg20 = group(&spawner, weighted_groups, "sg20", 20)?;
    let sg50 = group(&spawner, weighted_groups, "sg50", 50)?;

    let mut handles = Vec::new();
    spawn_group_tasks(
        &spawner,
        &sg100,
        "sg100",
        5,
        deadline,
        TaskKind::Heavy,
        &mut handles,
    )?;
    spawn_group_tasks(
        &spawner,
        &sg20,
        "sg20",
        3,
        deadline,
        TaskKind::Light,
        &mut handles,
    )?;
    spawn_group_tasks(
        &spawner,
        &sg50,
        "sg50",
        2,
        deadline,
        TaskKind::Medium,
        &mut handles,
    )?;

    let results = executor.run_until(async move {
        let mut outputs = Vec::new();
        for handle in handles {
            outputs.push(handle.await?);
        }
        Ok::<_, JoinError>(outputs)
    })?;
    let snapshot = executor.snapshot();
    drop(spawner);
    executor.run();

    Ok(summarize(weighted_groups, results, &snapshot))
}

fn group(
    spawner: &Spawner,
    weighted_groups: bool,
    name: &str,
    shares: u32,
) -> Result<SchedulingGroup, Box<dyn std::error::Error>> {
    if weighted_groups {
        Ok(spawner.create_scheduling_group(name, shares)?)
    } else {
        Ok(SchedulingGroup::default())
    }
}

fn spawn_group_tasks(
    spawner: &Spawner,
    group: &SchedulingGroup,
    label: &'static str,
    concurrency: usize,
    deadline: Instant,
    kind: TaskKind,
    handles: &mut Vec<sitas::executor::JoinHandle<TaskResult>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for task_idx in 0..concurrency {
        let name = format!("{label}-{task_idx}");
        let handle = spawner.spawn_with_handle_named_in_group(group, name, async move {
            compute_until(deadline, kind).await
        })?;
        handles.push(handle);
    }

    Ok(())
}

async fn compute_until(deadline: Instant, kind: TaskKind) -> TaskResult {
    let mut executed = 0u64;
    let mut runtime = Duration::ZERO;

    while Instant::now() < deadline {
        runtime += compute_chunk(kind);
        executed += 1;
        yield_now().await;
    }

    TaskResult {
        label: kind.label(),
        executed,
        runtime,
    }
}

fn compute_chunk(kind: TaskKind) -> Duration {
    let started = Instant::now();
    let mut x = kind.seed();
    while started.elapsed() < kind.chunk_duration() {
        x = match kind {
            TaskKind::Heavy => x.exp() / 3.0,
            TaskKind::Medium => x.cos(),
            TaskKind::Light => (x + 1.0).ln(),
        };
        if !x.is_finite() {
            x = kind.seed();
        }
    }
    black_box(x);
    started.elapsed()
}

fn summarize(
    weighted_groups: bool,
    task_results: Vec<TaskResult>,
    snapshot: &ExecutorSnapshot,
) -> Vec<GroupResult> {
    let specs = [
        ("sg100", 100, 1_000u64),
        ("sg20", 20, 100u64),
        ("sg50", 50, 400u64),
    ];

    specs
        .into_iter()
        .map(|(label, shares, task_time_us)| {
            let mut executed = 0u64;
            let mut runtime = Duration::ZERO;
            for result in task_results.iter().filter(|result| result.label == label) {
                executed += result.executed;
                runtime += result.runtime;
            }

            let group_snapshot = if weighted_groups {
                snapshot
                    .scheduling_groups
                    .iter()
                    .find(|group| group.name == label)
            } else {
                snapshot
                    .scheduling_groups
                    .iter()
                    .find(|group| group.name == "default")
            };

            GroupResult {
                label,
                shares,
                task_time_us,
                executed,
                runtime,
                group_snapshot: group_snapshot.cloned(),
            }
        })
        .collect()
}

fn print_rows(results: &[GroupResult]) {
    let total_charged = results
        .iter()
        .filter_map(|result| {
            result
                .group_snapshot
                .as_ref()
                .map(|group| group.total_poll_time)
        })
        .sum();

    println!(
        "{:8} {:>8} {:>15} {:>10} {:>12} {:>8} {:>12} {:>10} {:>12}",
        "group",
        "shares",
        "task_time(us)",
        "executed",
        "runtime(ms)",
        "polls",
        "charged(ms)",
        "share(%)",
        "vruntime"
    );

    for result in results {
        let polls = result
            .group_snapshot
            .as_ref()
            .map_or(0, |group| group.total_polls);
        let charged_ms = result
            .group_snapshot
            .as_ref()
            .map_or(0, |group| group.total_poll_time.as_millis());
        let poll_share = result
            .group_snapshot
            .as_ref()
            .map_or(0.0, |group| group.poll_time_share_of(total_charged) * 100.0);
        let vruntime = result
            .group_snapshot
            .as_ref()
            .map_or(0, |group| group.virtual_runtime);

        println!(
            "{:8} {:>8} {:>15} {:>10} {:>12} {:>8} {:>12} {:>9.1} {:>12}",
            result.label,
            result.shares,
            result.task_time_us,
            result.executed,
            result.runtime.as_millis(),
            polls,
            charged_ms,
            poll_share,
            vruntime
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum TaskKind {
    Heavy,
    Medium,
    Light,
}

impl TaskKind {
    fn label(self) -> &'static str {
        match self {
            TaskKind::Heavy => "sg100",
            TaskKind::Medium => "sg50",
            TaskKind::Light => "sg20",
        }
    }

    fn chunk_duration(self) -> Duration {
        match self {
            TaskKind::Heavy => Duration::from_millis(1),
            TaskKind::Medium => Duration::from_micros(400),
            TaskKind::Light => Duration::from_micros(100),
        }
    }

    fn seed(self) -> f64 {
        match self {
            TaskKind::Heavy => 1.0,
            TaskKind::Medium => 0.1,
            TaskKind::Light => 0.1,
        }
    }
}

#[derive(Debug)]
struct TaskResult {
    label: &'static str,
    executed: u64,
    runtime: Duration,
}

#[derive(Debug)]
struct GroupResult {
    label: &'static str,
    shares: u32,
    task_time_us: u64,
    executed: u64,
    runtime: Duration,
    group_snapshot: Option<SchedulingGroupSnapshot>,
}
