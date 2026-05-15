use sitas::{
    ShardId, ShardedExecutor, available_cpu_ids, current_executor_cpu_placement,
    current_executor_shard, executor::block_on,
};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const RECORD_COUNT: usize = 10_000;
const PAYLOAD_SIZE: usize = 24;
const RECORD_SIZE: usize = 8 + PAYLOAD_SIZE;
const INDEX_ENTRY_SIZE: usize = 16;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths = DemoPaths::new();
    create_data_file(&paths.data, RECORD_COUNT)?;
    File::create(&paths.index)?;

    let runtime = ShardedExecutor::start_pinned_on_available_cpus()?;
    let shard_count = runtime.shard_count();
    let progress = Arc::new(Mutex::new(vec![ShardProgress::default(); shard_count]));
    let mut handles = Vec::with_capacity(shard_count);

    println!("data file: {}", paths.data.display());
    println!("index file: {}", paths.index.display());
    println!("available CPUs: {:?}", available_cpu_ids());
    println!("records: {RECORD_COUNT}, shards: {shard_count}");

    for shard_idx in 0..shard_count {
        let partition = Partition::for_shard(shard_idx, shard_count, RECORD_COUNT);
        let data_path = paths.data.clone();
        let progress = Arc::clone(&progress);
        let handle = runtime.spawn_with_handle_named_on(
            ShardId(shard_idx),
            format!("index-partition-{shard_idx}"),
            async move { build_sorted_run(data_path, partition, progress) },
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
            "shard {} scanned {} records, sorted {} entries, observed {}",
            run.shard_id.0,
            run.records_scanned,
            run.entries.len(),
            run.cpu_placement
        );
    }

    runs.sort_by_key(|run| run.shard_id.0);
    let final_run = merge_runs_on_shards(&runtime, &progress, runs)?;
    write_index_file(&paths.index, &final_run.entries)?;
    verify_index_file(&paths.data, &paths.index, RECORD_COUNT)?;

    println!(
        "wrote {} sorted index entries ({} bytes)",
        final_run.entries.len(),
        final_run.entries.len() * INDEX_ENTRY_SIZE
    );

    runtime.stop()?;
    Ok(())
}

fn build_sorted_run(
    data_path: PathBuf,
    partition: Partition,
    progress: Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<ShardRun> {
    let shard_id = current_executor_shard().expect("index task runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("index task runs on a sharded executor");

    set_progress(&progress, shard_id, Phase::Scanning, 0);

    let mut data = File::open(data_path)?;
    data.seek(SeekFrom::Start(record_offset(partition.start_record)))?;

    let mut entries = Vec::with_capacity(partition.record_count);
    for record_idx in partition.start_record..partition.end_record {
        let record = read_record(&mut data)?;
        entries.push(IndexEntry {
            key: record.key,
            offset: record_offset(record_idx),
        });

        if entries.len() % 512 == 0 || record_idx + 1 == partition.end_record {
            set_progress(&progress, shard_id, Phase::Scanning, entries.len());
        }
    }

    set_progress(&progress, shard_id, Phase::Sorting, entries.len());
    entries.sort_unstable();
    set_progress(&progress, shard_id, Phase::Sorted, entries.len());

    Ok(ShardRun {
        shard_id,
        cpu_placement,
        records_scanned: entries.len(),
        entries,
    })
}

fn create_data_file(path: &Path, record_count: usize) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut rng = Lcg::new(0x517a_5eed);

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

fn write_index_file(path: &Path, entries: &[IndexEntry]) -> io::Result<()> {
    let mut file = File::create(path)?;
    for entry in entries {
        file.write_all(&entry.key.to_le_bytes())?;
        file.write_all(&entry.offset.to_le_bytes())?;
    }
    Ok(())
}

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
    mut runs: Vec<ShardRun>,
) -> Result<ShardRun, Box<dyn std::error::Error>> {
    let mut round = 0usize;

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
            let progress = Arc::clone(progress);
            let handle = runtime.spawn_with_handle_named_on(
                target_shard,
                format!(
                    "index-merge-round-{round}-{}-{}",
                    left.shard_id.0, right.shard_id.0
                ),
                async move { merge_pair(left, right, progress) },
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

fn merge_pair(
    left: ShardRun,
    right: ShardRun,
    progress: Arc<Mutex<Vec<ShardProgress>>>,
) -> io::Result<ShardRun> {
    let shard_id = current_executor_shard().expect("merge task runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("merge task runs on a sharded executor");
    let total_entries = left.entries.len() + right.entries.len();

    set_progress(&progress, shard_id, Phase::Merging, total_entries);

    let entries = merge_entries(left.entries, right.entries);

    set_progress(&progress, shard_id, Phase::Merged, entries.len());

    Ok(ShardRun {
        shard_id,
        cpu_placement,
        records_scanned: left.records_scanned + right.records_scanned,
        entries,
    })
}

fn merge_entries(left: Vec<IndexEntry>, right: Vec<IndexEntry>) -> Vec<IndexEntry> {
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let mut merged = Vec::with_capacity(left.len() + right.len());

    loop {
        match (left.peek(), right.peek()) {
            (Some(left_entry), Some(right_entry)) if left_entry <= right_entry => {
                merged.push(left.next().expect("left entry was peeked"));
            }
            (Some(_), Some(_)) => {
                merged.push(right.next().expect("right entry was peeked"));
            }
            (Some(_), None) => {
                merged.extend(left);
                break;
            }
            (None, Some(_)) => {
                merged.extend(right);
                break;
            }
            (None, None) => break,
        }
    }

    merged
}

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
            "  shard {} {} phase={} records={} tasks={}",
            shard.shard_id.0,
            shard.cpu_placement,
            progress.phase.name(),
            progress.records,
            task_count
        );
    }
}

fn set_progress(
    progress: &Arc<Mutex<Vec<ShardProgress>>>,
    shard_id: ShardId,
    phase: Phase,
    records: usize,
) {
    let mut progress = progress.lock().expect("progress mutex poisoned");
    progress[shard_id.0] = ShardProgress { phase, records };
}

fn read_record(file: &mut File) -> io::Result<Record> {
    let mut buffer = [0u8; RECORD_SIZE];
    file.read_exact(&mut buffer)?;

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

fn record_offset(record_idx: usize) -> u64 {
    (record_idx * RECORD_SIZE) as u64
}

#[derive(Debug)]
struct DemoPaths {
    data: PathBuf,
    index: PathBuf,
}

impl DemoPaths {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("sitas-index-demo-{}", std::process::id()));
        Self {
            data: base.with_extension("data"),
            index: base.with_extension("index"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Partition {
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
    shard_id: ShardId,
    cpu_placement: sitas::CpuPlacementStatus,
    records_scanned: usize,
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
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
}

impl Default for ShardProgress {
    fn default() -> Self {
        Self {
            phase: Phase::Waiting,
            records: 0,
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
