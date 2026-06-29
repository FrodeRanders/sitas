//! Compares optional NUMA memory placement policies on shard-local buffers.
//!
//! On Linux NUMA hosts this can make locality visible: `LocalToCpu` asks the
//! kernel to allocate future shard-thread pages on the NUMA node local to the
//! pinned CPU, while `Bind(NumaNodeId(0))` deliberately makes every shard use
//! node 0. On non-Linux platforms, including macOS, placement is reported as
//! unsupported and the example still runs as a four-shard memory benchmark.

mod support;

use sitas::{
    CpuPlacement, MemoryPlacement, MemoryPlacementStatus, NumaNodeId, ShardId, ShardedExecutor,
    ShardedExecutorConfig, available_cpu_ids, current_executor_cpu_placement,
    current_executor_memory_placement, current_executor_shard, executor::block_on,
};
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_SHARDS: usize = 4;
const DEFAULT_MIB_PER_SHARD: usize = 128;
const DEFAULT_PASSES: usize = 8;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("sharded_memory_placement");
    let config = DemoConfig::from_args(env::args())?;

    println!("available CPUs: {:?}", available_cpu_ids());
    println!(
        "shards: {}, MiB/shard: {}, passes: {}",
        config.shards, config.mib_per_shard, config.passes
    );
    println!(
        "note: macOS does not expose Linux NUMA placement here; compare placement status before interpreting timings"
    );

    let cases = [
        Case {
            name: "default",
            placement: MemoryPlacement::Default,
        },
        Case {
            name: "local-to-cpu",
            placement: MemoryPlacement::LocalToCpu,
        },
        Case {
            name: "bind-node-0",
            placement: MemoryPlacement::Bind(NumaNodeId(0)),
        },
        Case {
            name: "interleave-node-0",
            placement: MemoryPlacement::Interleave(vec![NumaNodeId(0)]),
        },
    ];

    let mut summaries = Vec::with_capacity(cases.len());
    for case in cases {
        summaries.push(run_case(case, &config)?);
    }

    println!();
    println!("summary:");
    for summary in summaries {
        println!(
            "  {:>16}: wall={:>8.2?} slowest-shard={:>8.2?} checksum=0x{:016x}",
            summary.name, summary.wall, summary.slowest_shard, summary.checksum
        );
    }

    Ok(())
}

fn run_case(case: Case, config: &DemoConfig) -> Result<CaseSummary, Box<dyn std::error::Error>> {
    println!();
    println!("case: {}", case.name);

    let runtime = ShardedExecutor::start_with_config(
        ShardedExecutorConfig::new(config.shards)
            .with_cpu_placement(CpuPlacement::Sequential)
            .with_memory_placement(case.placement.clone()),
    )?;

    for shard in &runtime.snapshot().shards {
        println!(
            "  shard {} {} | {}",
            shard.shard_id.0, shard.cpu_placement, shard.memory_placement
        );
    }

    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(runtime.shard_count());
    for shard_idx in 0..runtime.shard_count() {
        let mib_per_shard = config.mib_per_shard;
        let passes = config.passes;
        handles.push(runtime.spawn_with_handle_named_on(
            ShardId(shard_idx),
            format!("memory-placement-scan-{shard_idx}"),
            async move { run_shard_memory_work(mib_per_shard, passes) },
        )?);
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(block_on(handle)?);
    }
    let wall = wall_start.elapsed();

    results.sort_by_key(|result| result.shard_id.0);
    let slowest_shard = results
        .iter()
        .map(|result| result.elapsed)
        .max()
        .unwrap_or_default();
    let checksum = results.iter().fold(0u64, |acc, result| {
        acc ^ result.checksum.rotate_left(result.shard_id.0 as u32)
    });

    for result in &results {
        println!(
            "  shard {} elapsed={:>8.2?} throughput={:>8.1} MiB/s {} | {} checksum=0x{:016x}",
            result.shard_id.0,
            result.elapsed,
            result.throughput_mib_s,
            result.cpu_placement,
            result.memory_placement,
            result.checksum
        );
    }

    runtime.stop()?;

    Ok(CaseSummary {
        name: case.name,
        wall,
        slowest_shard,
        checksum,
    })
}

fn run_shard_memory_work(mib_per_shard: usize, passes: usize) -> ShardResult {
    let shard_id = current_executor_shard().expect("task runs on a shard");
    let cpu_placement = current_executor_cpu_placement().expect("task runs on a sharded executor");
    let memory_placement =
        current_executor_memory_placement().expect("task runs on a sharded executor");

    let word_count = mib_per_shard * 1024 * 1024 / std::mem::size_of::<u64>();
    let mut buffer = Vec::with_capacity(word_count);

    // First-touch happens here, inside the shard thread and after any requested
    // memory policy has been applied.
    for idx in 0..word_count {
        buffer.push(seed_word(shard_id.0, idx));
    }

    let start = Instant::now();
    for pass in 0..passes {
        mutate_buffer(&mut buffer, pass as u64);
    }
    let elapsed = start.elapsed();

    let checksum = buffer
        .iter()
        .step_by(257)
        .fold(0u64, |acc, word| acc.rotate_left(7) ^ *word);
    let touched_mib = mib_per_shard.saturating_mul(passes);
    let throughput_mib_s = if elapsed.is_zero() {
        0.0
    } else {
        touched_mib as f64 / elapsed.as_secs_f64()
    };

    ShardResult {
        shard_id,
        cpu_placement: cpu_placement.to_string(),
        memory_placement: memory_placement_label(&memory_placement),
        elapsed,
        throughput_mib_s,
        checksum,
    }
}

fn mutate_buffer(buffer: &mut [u64], pass: u64) {
    let mut carry = 0x9e37_79b9_7f4a_7c15u64 ^ pass;
    for word in buffer {
        carry = carry.rotate_left(9).wrapping_add(*word);
        *word = word.rotate_left(13) ^ carry;
    }
}

fn seed_word(shard_idx: usize, idx: usize) -> u64 {
    let shard = (shard_idx as u64).wrapping_mul(0xd1b5_4a32_d192_ed03);
    let item = (idx as u64).wrapping_mul(0x94d0_49bb_1331_11eb);
    shard ^ item ^ 0x517a_5eed_cafe_f00d
}

fn memory_placement_label(status: &MemoryPlacementStatus) -> String {
    match status {
        MemoryPlacementStatus::Default => String::from("default memory placement"),
        MemoryPlacementStatus::Applied { policy } => {
            format!("memory placement applied: {policy:?}")
        }
        MemoryPlacementStatus::Unsupported { requested, reason } => {
            format!("memory placement {requested:?} unsupported: {reason}")
        }
        MemoryPlacementStatus::Failed { requested, error } => {
            format!("memory placement {requested:?} failed: {error}")
        }
    }
}

#[derive(Debug, Clone)]
struct DemoConfig {
    shards: usize,
    mib_per_shard: usize,
    passes: usize,
}

impl DemoConfig {
    fn from_args(
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self {
            shards: DEFAULT_SHARDS,
            mib_per_shard: DEFAULT_MIB_PER_SHARD,
            passes: DEFAULT_PASSES,
        };

        let mut args = args.into_iter();
        let _program = args.next();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--shards" => config.shards = parse_next(&mut args, "--shards")?,
                "--mib-per-shard" => {
                    config.mib_per_shard = parse_next(&mut args, "--mib-per-shard")?
                }
                "--passes" => config.passes = parse_next(&mut args, "--passes")?,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}").into()),
            }
        }

        if config.shards == 0 {
            return Err("shard count must be greater than zero".into());
        }
        if config.mib_per_shard == 0 {
            return Err("MiB per shard must be greater than zero".into());
        }
        if config.passes == 0 {
            return Err("passes must be greater than zero".into());
        }

        Ok(config)
    }
}

fn parse_next(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let value = args
        .next()
        .ok_or_else(|| format!("{name} requires a value"))?;
    Ok(value.parse()?)
}

fn print_usage() {
    println!(
        "usage: cargo run --release --example sharded_memory_placement -- \
         [--shards N] [--mib-per-shard N] [--passes N]"
    );
}

#[derive(Debug, Clone)]
struct Case {
    name: &'static str,
    placement: MemoryPlacement,
}

#[derive(Debug, Clone)]
struct CaseSummary {
    name: &'static str,
    wall: Duration,
    slowest_shard: Duration,
    checksum: u64,
}

#[derive(Debug, Clone)]
struct ShardResult {
    shard_id: ShardId,
    cpu_placement: String,
    memory_placement: String,
    elapsed: Duration,
    throughput_mib_s: f64,
    checksum: u64,
}
