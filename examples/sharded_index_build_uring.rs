//! Builds a sorted index by partitioning file work across executor shards with `io_uring`.
//!
//! This is intentionally more application-like than the tiny examples. It
//! demonstrates how sitas keeps work shard-affine, returns owned metadata, and
//! uses explicit merge submissions instead of sharing mutable index state.
//! Compared with `sharded_index_build`, the partition phase uses Linux
//! `io_uring` offset reads and writes inside each shard task.
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

#[cfg(target_os = "linux")]
use sitas::os::{
    IoUringDispatcher, IoUringReadFuture, IoUringWriteFuture, available_io_uring,
    block_on_io_uring, report_io_uring_unavailable,
};
use sitas::{
    CpuPlacement, ShardId, ShardedExecutor, ShardedExecutorConfig, available_cpu_ids,
    current_executor_cpu_placement, current_executor_shard, executor::block_on,
};
use std::cmp::Ordering;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::rc::Rc;
use std::sync::{Arc, Mutex};

// The demo builds a sorted secondary index for a fixed-record data file.
//
// It has three visible stages:
// 1. setup: create an unsorted data file and start one executor shard per
//    configured shard;
// 2. shard work: each shard scans one contiguous data-file partition, sorts
//    that partition locally, and writes a materialized run file;
// 3. merge work: sorted run files are paired up and merged by tasks submitted
//    back onto shards until one final sorted index run remains.
const DEFAULT_RECORD_COUNT: usize = 10_000;
const DEFAULT_SEED: u64 = 0x517a_5eed;
const PAYLOAD_SIZE: usize = 24;
const RECORD_SIZE: usize = 8 + PAYLOAD_SIZE;
const INDEX_ENTRY_SIZE: usize = 16;
const PARTITION_READ_CHUNK_RECORDS: usize = 256;

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("sharded_index_build_uring requires Linux io_uring support");
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DemoConfig::from_args(env::args())?;
    if available_io_uring(8)?.is_none() {
        report_io_uring_unavailable();
        return Ok(());
    }

    // Setup: create deterministic input files in the temp directory. The index
    // file starts empty; it is populated after all shard-local runs have been
    // merged and verified.
    let paths = DemoPaths::new();
    fs::create_dir_all(&paths.run_dir)?;
    create_data_file(&paths.data, config.record_count, config.seed)?;
    File::create(&paths.index)?;

    // Start the shard-per-thread runtime. Sequential CPU placement pins shard
    // N to the Nth available CPU on Linux, while macOS reports the request as
    // unsupported in snapshots and keeps running unpinned.
    let runtime = ShardedExecutor::start_with_config(
        ShardedExecutorConfig::new(config.shard_count).with_cpu_placement(CpuPlacement::Sequential),
    )?;
    let shard_count = runtime.shard_count();
    // This mutex protects demo progress reporting, not service state. The
    // actual index-building work remains partitioned by shard and communicates
    // through owned `ShardRun` values.
    let progress = Arc::new(Mutex::new(vec![ShardProgress::default(); shard_count]));
    let mut handles = Vec::with_capacity(shard_count);

    println!("data file: {}", paths.data.display());
    println!("index file: {}", paths.index.display());
    println!("run files: {}", paths.run_dir.display());
    println!("available CPUs: {:?}", available_cpu_ids());
    println!(
        "records: {}, shards: {shard_count}, seed: 0x{:016x}, record bytes: {}, index-entry bytes: {INDEX_ENTRY_SIZE}",
        config.record_count,
        config.seed,
        config.record_count * RECORD_SIZE
    );

    // Partition phase: submit one named task per shard. Each task runs on its
    // assigned shard thread and only returns metadata for its materialized run;
    // the sorted entries themselves stay on disk.
    for shard_idx in 0..shard_count {
        let partition = Partition::for_shard(shard_idx, shard_count, config.record_count);
        let data_path = paths.data.clone();
        let run_path = paths.partition_run_path(shard_idx);
        let progress = Arc::clone(&progress);
        // This variant keeps the same shard ownership shape but replaces the
        // partition scan and run-file writes with offset-based io_uring I/O.
        // The dispatcher is still local to the task; later runtime work can
        // move that dispatcher into the shard executor loop.
        let handle = runtime.spawn_with_handle_named_on(
            ShardId(shard_idx),
            format!("uring-index-partition-{shard_idx}"),
            async move { build_sorted_run_uring(data_path, run_path, partition, progress) },
        )?;
        handles.push(handle);
    }

    print_progress("started", &runtime, &progress);

    let mut runs = Vec::with_capacity(shard_count);
    for handle in handles {
        let run = block_on(handle)??;
        runs.push(run);
        print_progress("joined partition", &runtime, &progress);
    }

    for run in &runs {
        println!(
            "shard {} scanned {} records, sorted {} entries, read {} bytes, wrote {} bytes, observed {}, run {}",
            run.shard_id.0,
            run.records_scanned,
            run.entry_count,
            run.bytes_read,
            run.bytes_written,
            run.cpu_placement,
            run.path.display()
        );
    }

    // Merge phase: keep submitting merge tasks to shards until the run list has
    // collapsed to a single globally sorted run file.
    runs.sort_by_key(|run| run.shard_id.0);
    let final_run = merge_runs_on_shards(&runtime, &progress, &paths.run_dir, runs)?;

    // Finalization: copy the final run into the advertised index path and
    // verify both global sort order and that every offset points back to a
    // record with the same key.
    fs::copy(&final_run.path, &paths.index)?;
    verify_index_file(&paths.data, &paths.index, config.record_count)?;

    println!(
        "wrote {} sorted index entries ({} bytes) from {}",
        final_run.entry_count,
        final_run.entry_count * INDEX_ENTRY_SIZE,
        final_run.path.display()
    );

    runtime.stop()?;
    if config.cleanup {
        cleanup_files(&paths)?;
        println!("removed generated files");
    }

    Ok(())
}

// Runs inside a shard executor task. This is the shard-local partition builder.
// It uses offset-based io_uring reads and writes so the file cursor is never
// shared or mutated by the kernel operations.
#[cfg(target_os = "linux")]
fn build_sorted_run_uring(
    data_path: PathBuf,
    run_path: PathBuf,
    partition: Partition,
    progress: Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<ShardRun> {
    let shard_id = current_executor_shard().expect("index task runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("index task runs on a sharded executor");

    set_progress(&progress, shard_id, Phase::Scanning, 0, 0, 0);

    let ring = available_io_uring(64)?
        .ok_or_else(|| io::Error::other("io_uring unavailable after startup check"))?;
    let dispatcher = IoUringDispatcher::new(ring).into_shared();
    let data = File::open(data_path)?;

    let mut entries = read_partition_entries_uring(
        Rc::clone(&dispatcher),
        data.as_raw_fd(),
        partition,
        shard_id,
        &progress,
    )?;

    let bytes_read = entries.len() * RECORD_SIZE;
    let bytes_written = entries.len() * INDEX_ENTRY_SIZE;

    set_progress(
        &progress,
        shard_id,
        Phase::Sorting,
        entries.len(),
        bytes_read,
        0,
    );
    entries.sort_unstable();
    write_index_file_uring(Rc::clone(&dispatcher), &run_path, &entries)?;
    set_progress(
        &progress,
        shard_id,
        Phase::Sorted,
        entries.len(),
        bytes_read,
        bytes_written,
    );

    Ok(ShardRun {
        shard_id,
        cpu_placement,
        records_scanned: entries.len(),
        entry_count: entries.len(),
        bytes_read,
        bytes_written,
        path: run_path,
    })
}

#[cfg(target_os = "linux")]
fn read_partition_entries_uring(
    dispatcher: sitas::os::SharedIoUringDispatcher,
    fd: RawFd,
    partition: Partition,
    shard_id: ShardId,
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<Vec<IndexEntry>> {
    let mut entries = Vec::with_capacity(partition.record_count);
    let mut next_record = partition.start_record;

    while next_record < partition.end_record {
        let chunk_records = PARTITION_READ_CHUNK_RECORDS.min(partition.end_record - next_record);
        let buffer = uring_read_exact_at(
            Rc::clone(&dispatcher),
            fd,
            record_offset(next_record),
            chunk_records * RECORD_SIZE,
        )?;

        for (chunk_idx, record_bytes) in buffer.chunks_exact(RECORD_SIZE).enumerate() {
            let record_idx = next_record + chunk_idx;
            let record = record_from_bytes(record_bytes)?;
            entries.push(IndexEntry {
                key: record.key,
                offset: record_offset(record_idx),
            });
        }
        next_record += chunk_records;

        if entries.len() % 512 == 0 || next_record == partition.end_record {
            set_progress(
                progress,
                shard_id,
                Phase::Scanning,
                entries.len(),
                entries.len() * RECORD_SIZE,
                0,
            );
        }
    }

    Ok(entries)
}

#[cfg(target_os = "linux")]
fn write_index_file_uring(
    dispatcher: sitas::os::SharedIoUringDispatcher,
    path: &Path,
    entries: &[IndexEntry],
) -> io::Result<()> {
    let file = File::create(path)?;
    let mut buffer = Vec::with_capacity(entries.len() * INDEX_ENTRY_SIZE);
    for entry in entries {
        encode_index_entry(*entry, &mut buffer);
    }
    uring_write_all_at(dispatcher, file.as_raw_fd(), 0, buffer)?;

    Ok(())
}

#[cfg(target_os = "linux")]
fn uring_read_exact_at(
    dispatcher: sitas::os::SharedIoUringDispatcher,
    fd: RawFd,
    offset: u64,
    len: usize,
) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(len);

    while output.len() < len {
        let read_offset = offset + output.len() as u64;
        let remaining = len - output.len();
        let future =
            IoUringReadFuture::queue(Rc::clone(&dispatcher), fd, vec![0; remaining], read_offset)?;
        let completion = block_on_io_uring(Rc::clone(&dispatcher), future)?;
        let read = uring_result_to_usize(completion.completion.result, "read")?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "io_uring read returned EOF before the requested record was complete",
            ));
        }
        output.extend_from_slice(&completion.buffer[..read]);
    }

    Ok(output)
}

#[cfg(target_os = "linux")]
fn uring_write_all_at(
    dispatcher: sitas::os::SharedIoUringDispatcher,
    fd: RawFd,
    offset: u64,
    mut buffer: Vec<u8>,
) -> io::Result<()> {
    let mut write_offset = offset;

    while !buffer.is_empty() {
        let requested = buffer.len();
        let future = IoUringWriteFuture::queue(Rc::clone(&dispatcher), fd, buffer, write_offset)?;
        let completion = block_on_io_uring(Rc::clone(&dispatcher), future)?;
        let bytes = uring_result_to_usize(completion.completion.result, "write")?;
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "io_uring write accepted zero bytes",
            ));
        }
        write_offset += bytes as u64;
        if bytes >= requested {
            buffer = Vec::new();
        } else {
            buffer = completion.buffer[bytes..].to_vec();
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn uring_result_to_usize(result: i32, operation: &str) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }

    usize::try_from(result).map_err(|_| {
        io::Error::other(format!(
            "io_uring {operation} completion result did not fit usize"
        ))
    })
}

fn create_data_file(path: &Path, record_count: usize, seed: u64) -> io::Result<()> {
    // Deterministic input makes the example reproducible across macOS, Linux,
    // different shard counts, and repeated benchmark-style runs.
    let mut file = File::create(path)?;
    let mut rng = Lcg::new(seed);

    for _ in 0..record_count {
        let mut payload = [0u8; PAYLOAD_SIZE];
        rng.fill(&mut payload);
        write_record(
            &mut file,
            Record {
                key: rng.next_u64(),
                payload,
            },
        )?;
    }

    Ok(())
}

fn cleanup_files(paths: &DemoPaths) -> io::Result<()> {
    match fs::remove_file(&paths.data) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match fs::remove_file(&paths.index) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match fs::remove_dir_all(&paths.run_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn write_index_entry(file: &mut File, entry: IndexEntry) -> io::Result<()> {
    file.write_all(&entry.key.to_le_bytes())?;
    file.write_all(&entry.offset.to_le_bytes())?;
    Ok(())
}

fn encode_index_entry(entry: IndexEntry, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&entry.key.to_le_bytes());
    buffer.extend_from_slice(&entry.offset.to_le_bytes());
}

// Verification is intentionally outside the sharded runtime: it checks the
// externally visible result, not the implementation path that produced it.
fn verify_index_file(
    data_path: &Path,
    index_path: &Path,
    expected_entries: usize,
) -> io::Result<()> {
    let mut data = File::open(data_path)?;
    let mut index = File::open(index_path)?;
    let mut previous = None;
    let mut count = 0usize;

    while let Some(entry) = read_index_entry(&mut index)? {
        if let Some(previous) = previous
            && previous > entry
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index entries are not globally sorted",
            ));
        }

        data.seek(SeekFrom::Start(entry.offset))?;
        let record = read_record(&mut data)?;
        if record.key != entry.key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index entry does not point to a record with the same key",
            ));
        }

        previous = Some(entry);
        count += 1;
    }

    if count != expected_entries {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("index contains {count} entries, expected {expected_entries}"),
        ));
    }

    Ok(())
}

fn merge_runs_on_shards(
    runtime: &ShardedExecutor,
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
    run_dir: &Path,
    mut runs: Vec<ShardRun>,
) -> Result<ShardRun, Box<dyn std::error::Error>> {
    let mut round = 0usize;

    // Each round halves the number of run files when possible. An odd run is
    // carried forward unchanged into the next round.
    while runs.len() > 1 {
        let mut handles = Vec::with_capacity(runs.len() / 2);
        let mut carried = Vec::with_capacity(runs.len() % 2);
        let mut run_iter = runs.into_iter();

        while let Some(left) = run_iter.next() {
            let Some(right) = run_iter.next() else {
                carried.push(left);
                break;
            };

            let target_shard = left.shard_id;
            let output_path = run_dir.join(format!(
                "merge-r{round}-s{}-s{}.run",
                left.shard_id.0, right.shard_id.0
            ));

            // Merge work is explicit cross-shard work: the new task is placed
            // on the shard that owned the left input run. That policy is simple
            // rather than load-balanced; its purpose is to keep placement
            // decisions visible while the runtime mechanics are still evolving.
            let progress = Arc::clone(progress);
            let handle = runtime.spawn_with_handle_named_on(
                target_shard,
                format!(
                    "index-merge-round-{round}-{}-{}",
                    left.shard_id.0, right.shard_id.0
                ),
                async move { merge_pair(left, right, output_path, progress) },
            )?;
            handles.push(handle);
        }

        let mut merged = carried;
        for handle in handles {
            merged.push(block_on(handle)??);
            print_progress("joined merge", runtime, progress);
        }

        merged.sort_by_key(|run| run.shard_id.0);
        println!("merge round {round} produced {} runs", merged.len());
        runs = merged;
        round += 1;
    }

    Ok(runs.pop().expect("at least one sorted run"))
}

// Runs inside a shard executor task. It streams two already sorted run files
// into a new sorted run file without loading both inputs into memory.
fn merge_pair(
    left: ShardRun,
    right: ShardRun,
    output_path: PathBuf,
    progress: Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<ShardRun> {
    let shard_id = current_executor_shard().expect("merge task runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("merge task runs on a sharded executor");
    let total_entries = left.entry_count + right.entry_count;
    let bytes_read = total_entries * INDEX_ENTRY_SIZE;
    let bytes_written = total_entries * INDEX_ENTRY_SIZE;

    set_progress(
        &progress,
        shard_id,
        Phase::Merging,
        total_entries,
        bytes_read,
        0,
    );

    let entry_count = merge_run_files(&left.path, &right.path, &output_path)?;

    set_progress(
        &progress,
        shard_id,
        Phase::Merged,
        entry_count,
        bytes_read,
        bytes_written,
    );

    Ok(ShardRun {
        shard_id,
        cpu_placement,
        records_scanned: left.records_scanned + right.records_scanned,
        entry_count,
        bytes_read,
        bytes_written,
        path: output_path,
    })
}

fn merge_run_files(left_path: &Path, right_path: &Path, output_path: &Path) -> io::Result<usize> {
    let mut left = RunReader::open(left_path)?;
    let mut right = RunReader::open(right_path)?;
    let mut output = File::create(output_path)?;
    let mut count = 0usize;

    loop {
        match (left.peek()?, right.peek()?) {
            (Some(left_entry), Some(right_entry)) if left_entry <= right_entry => {
                write_index_entry(&mut output, left_entry)?;
                left.consume();
                count += 1;
            }
            (Some(_), Some(right_entry)) => {
                write_index_entry(&mut output, right_entry)?;
                right.consume();
                count += 1;
            }
            (Some(left_entry), None) => {
                write_index_entry(&mut output, left_entry)?;
                left.consume();
                count += 1;
                count += left.drain_into(&mut output)?;
                break;
            }
            (None, Some(right_entry)) => {
                write_index_entry(&mut output, right_entry)?;
                right.consume();
                count += 1;
                count += right.drain_into(&mut output)?;
                break;
            }
            (None, None) => break,
        }
    }

    Ok(count)
}

// Progress is a demo-level observability layer. It combines explicit per-shard
// counters from this example with the runtime's own task snapshots.
fn print_progress(
    label: &str,
    runtime: &ShardedExecutor,
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
) {
    let snapshot = runtime.snapshot();
    let progress = progress.lock().expect("progress mutex poisoned");

    println!("{label}:");
    for shard in &snapshot.shards {
        let progress = progress[shard.shard_id.0];
        let task_count = shard
            .executor
            .as_ref()
            .map_or(0, |executor| executor.task_count);
        println!(
            "  shard {} {} phase={} records={} read={}B wrote={}B tasks={}",
            shard.shard_id.0,
            shard.cpu_placement,
            progress.phase.name(),
            progress.records,
            progress.bytes_read,
            progress.bytes_written,
            task_count
        );
    }
}

fn set_progress(
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
    shard_id: ShardId,
    phase: Phase,
    records: usize,
    bytes_read: usize,
    bytes_written: usize,
) {
    let mut progress = progress.lock().expect("progress mutex poisoned");
    progress[shard_id.0] = ShardProgress {
        phase,
        records,
        bytes_read,
        bytes_written,
    };
}

fn read_record(file: &mut File) -> io::Result<Record> {
    let mut buffer = [0u8; RECORD_SIZE];
    file.read_exact(&mut buffer)?;
    record_from_bytes(&buffer)
}

fn record_from_bytes(buffer: &[u8]) -> io::Result<Record> {
    if buffer.len() != RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "record buffer has {} bytes, expected {RECORD_SIZE}",
                buffer.len()
            ),
        ));
    }
    let mut key = [0u8; 8];
    key.copy_from_slice(&buffer[..8]);

    let mut payload = [0u8; PAYLOAD_SIZE];
    payload.copy_from_slice(&buffer[8..]);

    Ok(Record {
        key: u64::from_le_bytes(key),
        payload,
    })
}

fn write_record(file: &mut File, record: Record) -> io::Result<()> {
    file.write_all(&record.key.to_le_bytes())?;
    file.write_all(&record.payload)?;
    Ok(())
}

fn read_index_entry(file: &mut File) -> io::Result<Option<IndexEntry>> {
    let mut buffer = [0u8; INDEX_ENTRY_SIZE];
    let mut read = 0usize;

    while read < buffer.len() {
        match file.read(&mut buffer[read..])? {
            0 if read == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial index entry",
                ));
            }
            bytes => read += bytes,
        }
    }

    let mut key = [0u8; 8];
    key.copy_from_slice(&buffer[..8]);
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&buffer[8..]);

    Ok(Some(IndexEntry {
        key: u64::from_le_bytes(key),
        offset: u64::from_le_bytes(offset),
    }))
}

struct RunReader {
    file: File,
    // A one-entry lookahead is enough for a classic two-way merge.
    peeked: Option<Option<IndexEntry>>,
}

impl RunReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            peeked: None,
        })
    }

    fn peek(&mut self) -> io::Result<Option<IndexEntry>> {
        if self.peeked.is_none() {
            self.peeked = Some(read_index_entry(&mut self.file)?);
        }

        Ok(self.peeked.expect("peeked entry initialized"))
    }

    fn consume(&mut self) {
        self.peeked = None;
    }

    fn drain_into(&mut self, output: &mut File) -> io::Result<usize> {
        let mut count = 0usize;

        while let Some(entry) = self.peek()? {
            write_index_entry(output, entry)?;
            self.consume();
            count += 1;
        }

        Ok(count)
    }
}

fn record_offset(record_idx: usize) -> u64 {
    (record_idx * RECORD_SIZE) as u64
}

#[derive(Debug, Clone, Copy)]
struct DemoConfig {
    // Number of fixed-size records to generate in the input data file.
    record_count: usize,
    // Number of shard executor threads to start for this run.
    shard_count: usize,
    // Deterministic seed used by the tiny local pseudo-random generator.
    seed: u64,
    // Remove temp data, index, and run files after successful verification.
    cleanup: bool,
}

impl DemoConfig {
    fn from_args(
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self {
            record_count: DEFAULT_RECORD_COUNT,
            shard_count: available_cpu_ids().len(),
            seed: DEFAULT_SEED,
            cleanup: false,
        };

        let mut args = args.into_iter();
        let program = args
            .next()
            .unwrap_or_else(|| String::from("sharded_index_build"));

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--records" => {
                    let value = next_arg(&mut args, "--records")?;
                    config.record_count = parse_non_zero_usize("--records", &value)?;
                }
                "--shards" => {
                    let value = next_arg(&mut args, "--shards")?;
                    config.shard_count = parse_non_zero_usize("--shards", &value)?;
                }
                "--seed" => {
                    let value = next_arg(&mut args, "--seed")?;
                    config.seed = parse_u64("--seed", &value)?;
                }
                "--cleanup" => config.cleanup = true,
                "--help" | "-h" => {
                    print_usage(&program);
                    std::process::exit(0);
                }
                unknown => {
                    return Err(invalid_input(format!(
                        "unknown argument {unknown:?}; run with --help for usage"
                    ))
                    .into());
                }
            }
        }

        Ok(config)
    }
}

fn next_arg(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| invalid_input(format!("{option} requires a value")).into())
}

fn parse_non_zero_usize(option: &str, value: &str) -> io::Result<usize> {
    let parsed = parse_usize(value)
        .map_err(|error| invalid_input(format!("{option} expects a positive integer: {error}")))?;
    if parsed == 0 {
        return Err(invalid_input(format!("{option} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_usize(value: &str) -> Result<usize, std::num::ParseIntError> {
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        usize::from_str_radix(hex, 16)
    } else {
        value.parse()
    }
}

fn parse_u64(option: &str, value: &str) -> io::Result<u64> {
    let parsed = if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16)
    } else {
        value.parse()
    };

    parsed.map_err(|error| invalid_input(format!("{option} expects an integer seed: {error}")))
}

fn print_usage(program: &str) {
    println!(
        "usage: {program} [--records N] [--shards N] [--seed N|0xHEX] [--cleanup]\n\
         defaults: --records {DEFAULT_RECORD_COUNT} --shards <available-cpus> --seed 0x{DEFAULT_SEED:016x}"
    );
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[derive(Debug)]
struct DemoPaths {
    data: PathBuf,
    index: PathBuf,
    run_dir: PathBuf,
}

impl DemoPaths {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("sitas-index-demo-{}", std::process::id()));
        Self {
            data: base.with_extension("data"),
            index: base.with_extension("index"),
            run_dir: base.with_extension("runs"),
        }
    }

    fn partition_run_path(&self, shard_idx: usize) -> PathBuf {
        self.run_dir.join(format!("partition-s{shard_idx}.run"))
    }
}

#[derive(Debug, Clone, Copy)]
struct Partition {
    // Half-open record range [start_record, end_record) assigned to one shard.
    start_record: usize,
    end_record: usize,
    record_count: usize,
}

impl Partition {
    fn for_shard(shard_idx: usize, shard_count: usize, record_count: usize) -> Self {
        let start_record = shard_idx * record_count / shard_count;
        let end_record = (shard_idx + 1) * record_count / shard_count;
        Self {
            start_record,
            end_record,
            record_count: end_record - start_record,
        }
    }
}

#[derive(Debug)]
struct ShardRun {
    // The shard that produced this materialized sorted run.
    shard_id: ShardId,
    cpu_placement: sitas::CpuPlacementStatus,
    // Original data records represented by this run. During merge rounds this
    // is the sum of the input runs' scanned records.
    records_scanned: usize,
    entry_count: usize,
    // Bytes read and written by the task that produced this run, not cumulative
    // lifetime I/O across all earlier rounds.
    bytes_read: usize,
    bytes_written: usize,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    // Sorting by (key, offset) gives deterministic order even when keys repeat.
    key: u64,
    offset: u64,
}

impl Ord for IndexEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

impl PartialOrd for IndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy)]
struct Record {
    key: u64,
    payload: [u8; PAYLOAD_SIZE],
}

#[derive(Debug, Clone, Copy)]
struct ShardProgress {
    phase: Phase,
    records: usize,
    bytes_read: usize,
    bytes_written: usize,
}

impl Default for ShardProgress {
    fn default() -> Self {
        Self {
            phase: Phase::Waiting,
            records: 0,
            bytes_read: 0,
            bytes_written: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Waiting,
    Scanning,
    Sorting,
    Sorted,
    Merging,
    Merged,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Phase::Waiting => "waiting",
            Phase::Scanning => "scanning",
            Phase::Sorting => "sorting",
            Phase::Sorted => "sorted",
            Phase::Merging => "merging",
            Phase::Merged => "merged",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let value = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&value[..chunk.len()]);
        }
    }
}
