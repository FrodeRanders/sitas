//! Builds a sorted index by partitioning file work across executor shards with `io_uring`.
//!
//! This is intentionally more application-like than the tiny examples. It
//! demonstrates how sitas keeps work shard-affine, returns owned metadata, and
//! uses explicit merge submissions instead of sharing mutable index state.
//! Compared with `sharded_index_build`, partition scans, run writes, and merge
//! run reads/writes use Linux `io_uring` offsets inside each shard task.
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

#[cfg(target_os = "linux")]
use sitas::executor::{read_exact_at_uring, write_all_at_uring};
#[cfg(target_os = "linux")]
use sitas::os::{available_io_uring, report_io_uring_unavailable};
use sitas::{
    CpuPlacement, ExecutorSnapshot, ShardId, ShardedExecutor, ShardedExecutorConfig,
    available_cpu_ids, current_executor_cpu_placement, current_executor_shard, executor::block_on,
};
use std::cmp::Ordering;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
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
        // The futures are driven by the shard executor's thread-local
        // dispatcher, so the task can await completions without a nested
        // io_uring-specific block_on loop.
        let handle = runtime.spawn_with_handle_named_on(
            ShardId(shard_idx),
            format!("uring-index-partition-{shard_idx}"),
            async move { build_sorted_run_uring(data_path, run_path, partition, progress).await },
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
    // collapsed to a single globally sorted run file. Merge tasks also use
    // offset-based io_uring I/O for reading run entries and writing merged
    // batches.
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
async fn build_sorted_run_uring(
    data_path: PathBuf,
    run_path: PathBuf,
    partition: Partition,
    progress: Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<ShardRun> {
    let shard_id = current_executor_shard().expect("index task runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("index task runs on a sharded executor");

    set_progress(&progress, shard_id, Phase::Scanning, 0, 0, 0);

    let data = File::open(data_path)?;

    let mut entries =
        read_partition_entries_uring(data.as_raw_fd(), partition, shard_id, &progress).await?;

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
    write_index_file_uring(&run_path, &entries).await?;
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
async fn read_partition_entries_uring(
    fd: RawFd,
    partition: Partition,
    shard_id: ShardId,
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<Vec<IndexEntry>> {
    let mut entries = Vec::with_capacity(partition.record_count);
    let mut next_record = partition.start_record;

    while next_record < partition.end_record {
        let chunk_records = PARTITION_READ_CHUNK_RECORDS.min(partition.end_record - next_record);
        let buffer =
            read_exact_at_uring(fd, record_offset(next_record), chunk_records * RECORD_SIZE)
                .await?;

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
async fn write_index_file_uring(path: &Path, entries: &[IndexEntry]) -> io::Result<()> {
    let file = File::create(path)?;
    let mut buffer = Vec::with_capacity(entries.len() * INDEX_ENTRY_SIZE);
    for entry in entries {
        encode_index_entry(*entry, &mut buffer);
    }
    write_all_at_uring(file.as_raw_fd(), 0, buffer).await?;

    Ok(())
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

fn encode_index_entry(entry: IndexEntry, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&entry.key.to_le_bytes());
    buffer.extend_from_slice(&entry.offset.to_le_bytes());
}

fn decode_index_entry(buffer: &[u8]) -> io::Result<IndexEntry> {
    if buffer.len() != INDEX_ENTRY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "index entry buffer has {} bytes, expected {INDEX_ENTRY_SIZE}",
                buffer.len()
            ),
        ));
    }

    let mut key = [0u8; 8];
    key.copy_from_slice(&buffer[..8]);
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&buffer[8..]);

    Ok(IndexEntry {
        key: u64::from_le_bytes(key),
        offset: u64::from_le_bytes(offset),
    })
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

#[cfg(target_os = "linux")]
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
                async move { merge_pair_uring(left, right, output_path, progress).await },
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
// into a new sorted run file through offset-based io_uring reads and writes.
#[cfg(target_os = "linux")]
async fn merge_pair_uring(
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

    let entry_count = merge_run_files_uring(&left, &right, &output_path).await?;

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

#[cfg(target_os = "linux")]
async fn merge_run_files_uring(
    left_run: &ShardRun,
    right_run: &ShardRun,
    output_path: &Path,
) -> io::Result<usize> {
    let mut left = UringRunReader::open(&left_run.path, left_run.entry_count)?;
    let mut right = UringRunReader::open(&right_run.path, right_run.entry_count)?;
    let output = File::create(output_path)?;
    let output_fd = output.as_raw_fd();
    let mut output_offset = 0u64;
    let mut output_buffer = Vec::with_capacity(PARTITION_READ_CHUNK_RECORDS * INDEX_ENTRY_SIZE);
    let mut count = 0usize;

    loop {
        match (left.peek().await?, right.peek().await?) {
            (Some(left_entry), Some(right_entry)) if left_entry <= right_entry => {
                append_merged_entry_uring(
                    output_fd,
                    &mut output_offset,
                    &mut output_buffer,
                    left_entry,
                )
                .await?;
                left.consume();
                count += 1;
            }
            (Some(_), Some(right_entry)) => {
                append_merged_entry_uring(
                    output_fd,
                    &mut output_offset,
                    &mut output_buffer,
                    right_entry,
                )
                .await?;
                right.consume();
                count += 1;
            }
            (Some(left_entry), None) => {
                append_merged_entry_uring(
                    output_fd,
                    &mut output_offset,
                    &mut output_buffer,
                    left_entry,
                )
                .await?;
                left.consume();
                count += 1;
                count += left
                    .drain_into(output_fd, &mut output_offset, &mut output_buffer)
                    .await?;
                break;
            }
            (None, Some(right_entry)) => {
                append_merged_entry_uring(
                    output_fd,
                    &mut output_offset,
                    &mut output_buffer,
                    right_entry,
                )
                .await?;
                right.consume();
                count += 1;
                count += right
                    .drain_into(output_fd, &mut output_offset, &mut output_buffer)
                    .await?;
                break;
            }
            (None, None) => break,
        }
    }

    flush_merged_entries_uring(output_fd, &mut output_offset, &mut output_buffer).await?;
    Ok(count)
}

#[cfg(target_os = "linux")]
async fn append_merged_entry_uring(
    output_fd: RawFd,
    output_offset: &mut u64,
    output_buffer: &mut Vec<u8>,
    entry: IndexEntry,
) -> io::Result<()> {
    encode_index_entry(entry, output_buffer);
    if output_buffer.len() >= PARTITION_READ_CHUNK_RECORDS * INDEX_ENTRY_SIZE {
        flush_merged_entries_uring(output_fd, output_offset, output_buffer).await?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn flush_merged_entries_uring(
    output_fd: RawFd,
    output_offset: &mut u64,
    output_buffer: &mut Vec<u8>,
) -> io::Result<()> {
    if output_buffer.is_empty() {
        return Ok(());
    }

    let written = output_buffer.len();
    write_all_at_uring(output_fd, *output_offset, std::mem::take(output_buffer)).await?;
    *output_offset += written as u64;
    Ok(())
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
        let io_uring = shard
            .executor
            .as_ref()
            .map_or_else(String::new, io_uring_summary);
        println!(
            "  shard {} {} phase={} records={} read={}B wrote={}B tasks={}{}",
            shard.shard_id.0,
            shard.cpu_placement,
            progress.phase.name(),
            progress.records,
            progress.bytes_read,
            progress.bytes_written,
            task_count,
            io_uring
        );
    }
}

fn io_uring_summary(executor: &ExecutorSnapshot) -> String {
    #[cfg(target_os = "linux")]
    if let Some(io_uring) = &executor.io_uring {
        return format!(
            " uring(submit={} tracked={} buffered={} wakers={} dispatched={} reads={} writes={})",
            io_uring.ring.pending_submissions,
            io_uring.ring.tracked_operations,
            io_uring.completed_operations,
            io_uring.registered_wakers,
            io_uring.total_dispatched_operations,
            io_uring.total_dispatched_operation_kinds.reads,
            io_uring.total_dispatched_operation_kinds.writes
        );
    }

    let _ = executor;
    String::new()
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

    decode_index_entry(&buffer).map(Some)
}

#[cfg(target_os = "linux")]
struct UringRunReader {
    file: File,
    entry_count: usize,
    next_index: usize,
    // A one-entry lookahead is enough for a classic two-way merge.
    peeked: Option<Option<IndexEntry>>,
}

#[cfg(target_os = "linux")]
impl UringRunReader {
    fn open(path: &Path, entry_count: usize) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            entry_count,
            next_index: 0,
            peeked: None,
        })
    }

    async fn peek(&mut self) -> io::Result<Option<IndexEntry>> {
        if self.peeked.is_none() {
            self.peeked = Some(self.read_next_entry().await?);
        }

        Ok(self.peeked.expect("peeked entry initialized"))
    }

    fn consume(&mut self) {
        self.peeked = None;
    }

    async fn drain_into(
        &mut self,
        output_fd: RawFd,
        output_offset: &mut u64,
        output_buffer: &mut Vec<u8>,
    ) -> io::Result<usize> {
        let mut count = 0usize;

        while let Some(entry) = self.peek().await? {
            append_merged_entry_uring(output_fd, output_offset, output_buffer, entry).await?;
            self.consume();
            count += 1;
        }

        Ok(count)
    }

    async fn read_next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        if self.next_index >= self.entry_count {
            return Ok(None);
        }

        let offset = (self.next_index * INDEX_ENTRY_SIZE) as u64;
        let buffer = read_exact_at_uring(self.file.as_raw_fd(), offset, INDEX_ENTRY_SIZE).await?;
        self.next_index += 1;
        decode_index_entry(&buffer).map(Some)
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
        let base = env::temp_dir().join(format!("sitas-index-demo-{}", std::process::id()));
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
