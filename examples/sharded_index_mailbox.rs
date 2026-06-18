//! Builds a sorted index using typed mailbox transfer between executor shards.
//!
//! This keeps the existing file-backed run demo separate. Here, scan tasks
//! send owned index-entry batches to destination shard assemblers through
//! logical work-unit mailboxes, so intermediate cross-shard exchange does not
//! use files and the logical work assignment does not have to be uniform.
//!
//! There are three roles in this demo:
//!
//! - scanner tasks read contiguous input-file partitions on executor shards;
//! - assembler work units receive mailbox batches and build sorted runs;
//! - the coordinator in `main` starts work, waits for task handles, merges the
//!   assembler run files, verifies the final index, and shuts the runtime down.
//!
//! The mailbox path is deliberately scoped to scanner -> assembler transfer.
//! Assemblers do not send mailbox messages back to coordination. They return
//! small owned `PartitionRun` metadata through their task handles, and the
//! coordinator reads their sorted run files during the final k-way merge.

use sitas::{
    CpuPlacement, RouteByKey, ShardId, ShardLocal, ShardMailboxConfig, ShardedExecutor,
    ShardedExecutorConfig, WorkUnitMailboxSet, WorkUnitRouter, WorkUnitSpec, available_cpu_ids,
    current_executor_cpu_placement, current_executor_shard, executor::block_on,
};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DEFAULT_RECORD_COUNT: usize = 10_000;
const DEFAULT_SEED: u64 = 0x517a_5eed;
const PAYLOAD_SIZE: usize = 24;
const RECORD_SIZE: usize = 8 + PAYLOAD_SIZE;
const INDEX_ENTRY_SIZE: usize = 16;
const READ_CHUNK_RECORDS: usize = 256;
const SEND_BATCH_ENTRIES: usize = 128;
const DEFAULT_MAILBOX_CAPACITY: usize = 64;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DemoConfig::from_args(env::args())?;
    let overall_start = Instant::now();

    // The input and final index remain ordinary files because they are the
    // externally visible data set. The experiment here is only about replacing
    // intermediate cross-shard exchange with typed owned messages.
    let paths = DemoPaths::new();

    fs::create_dir_all(&paths.partition_dir)?;
    create_data_file(&paths.data, config.record_count, config.seed)?;
    File::create(&paths.index)?;

    // Use the same shard-per-thread executor shape as the file-backed index
    // demo so the interesting difference is the transfer mechanism, not the
    // runtime topology.
    let runtime = ShardedExecutor::start_with_config(
        ShardedExecutorConfig::new(config.shard_count).with_cpu_placement(CpuPlacement::Sequential),
    )?;
    let shard_count = runtime.shard_count();

    // By default the demo uses one more logical assembler than physical
    // shards. That makes non-uniform assignment visible: work-unit names are
    // stable logical destinations, while their shard placement is a separate
    // table that can put several receivers on one shard.
    let assembler_count = config.assembler_count.unwrap_or(shard_count + 1);
    let work_units = assembler_work_units(assembler_count, shard_count);

    // The router answers "which logical assembler owns this key?" It does not
    // answer "which shard should I send to?" directly. WorkUnitMailboxSet uses
    // the work-unit placement table to resolve the logical name to a receiver.
    let router = WorkUnitRouter::new(work_units.iter().map(|spec| spec.name))?;
    let progress: ShardLocal<BTreeMap<String, ShardProgress>> =
        ShardLocal::new(runtime.submitter(), |_| BTreeMap::new());
    let mailboxes = Arc::new(WorkUnitMailboxSet::new(
        &runtime.submitter(),
        work_units.clone(),
        ShardMailboxConfig::new(config.mailbox_capacity),
    )?);

    println!("data file: {}", paths.data.display());
    println!("index file: {}", paths.index.display());
    println!("partition files: {}", paths.partition_dir.display());
    println!("available CPUs: {:?}", available_cpu_ids());
    println!(
        "records: {}, shards: {shard_count}, assemblers: {assembler_count}, seed: 0x{:016x}, mailbox capacity: {}",
        config.record_count, config.seed, config.mailbox_capacity
    );
    println!("logical assembler placement:");
    for spec in &work_units {
        println!("  {} -> shard {}", spec.name.label(), spec.shard_id.0);
    }

    // Start receivers before producers. A sender may be cloned freely, but
    // each work-unit mailbox has exactly one receiver and it must be taken on
    // the shard named by the placement table.
    let mut assembler_handles = Vec::with_capacity(work_units.len());
    for spec in &work_units {
        let mailboxes = Arc::clone(&mailboxes);
        let progress = progress.clone();
        let work_unit = spec.name;
        let output_path = paths.partition_path(work_unit.0);
        assembler_handles.push(runtime.spawn_with_handle_named_on(
            spec.shard_id,
            format!("mailbox-index-{}", work_unit.label()),
            async move {
                assemble_partition(mailboxes, work_unit, progress, shard_count, output_path).await
            },
        )?);
    }

    // Scanner tasks keep the original partitioning model: each shard reads a
    // contiguous slice of the data file. The difference is that each record is
    // routed by key to a logical assembler instead of being kept in the
    // scanning shard's local run.
    let scan_start = Instant::now();
    let mut scanner_handles = Vec::with_capacity(shard_count);
    for shard_idx in 0..shard_count {
        let mailboxes = Arc::clone(&mailboxes);
        let progress = progress.clone();
        let data_path = paths.data.clone();
        let partition = Partition::for_shard(shard_idx, shard_count, config.record_count);
        let router = router.clone();
        scanner_handles.push(runtime.spawn_with_handle_named_on(
            ShardId(shard_idx),
            format!("mailbox-index-scan-{shard_idx}"),
            async move { scan_and_send(data_path, partition, router, mailboxes, progress).await },
        )?);
    }

    for handle in scanner_handles {
        block_on(handle)??;
    }
    let scan_elapsed = scan_start.elapsed();
    print_progress("scan and transfer complete", &runtime, &progress);

    let assemble_start = Instant::now();
    let mut partitions = Vec::with_capacity(shard_count);
    for handle in assembler_handles {
        partitions.push(block_on(handle)??);
    }
    partitions.sort_by_key(|partition| partition.work_unit.0);
    let assemble_elapsed = assemble_start.elapsed();
    print_progress("assembly complete", &runtime, &progress);

    // Assemblers write locally sorted partition files. A final k-way merge is
    // still needed because the example wants one externally visible sorted
    // index file, but the records reached the assemblers through mailboxes
    // instead of through intermediate cross-shard files.
    k_way_merge_partition_files(&partitions, &paths.index)?;
    verify_index_file(&paths.data, &paths.index, config.record_count)?;

    for partition in &partitions {
        println!(
            "{} on shard {} received {} entries, wrote {} bytes, observed {}, partition {}",
            partition.work_unit.label(),
            partition.shard_id.0,
            partition.entry_count,
            partition.bytes_written,
            partition.cpu_placement,
            partition.path.display()
        );
    }
    println!(
        "timing: scan+transfer={scan_elapsed:.1?} assemble={assemble_elapsed:.1?} total={:.1?}",
        overall_start.elapsed()
    );
    println!("verification: mailbox-built index is valid and globally sorted");

    drop(mailboxes);
    drop(progress);
    runtime.stop()?;
    if config.cleanup {
        cleanup_files(&paths)?;
        println!("removed generated files");
    }

    Ok(())
}

async fn scan_and_send(
    data_path: PathBuf,
    partition: Partition,
    router: WorkUnitRouter<IndexWorkUnit>,
    mailboxes: Arc<WorkUnitMailboxSet<IndexWorkUnit, IndexMessage>>,
    progress: ShardLocal<BTreeMap<String, ShardProgress>>,
) -> io::Result<()> {
    let shard_id = current_executor_shard().expect("scanner runs on a shard");
    let task_label = format!("scan-{}", shard_id.0);

    let senders = senders_for_assemblers(&mailboxes, router.work_unit_count())?;
    let mut batches = vec![Vec::with_capacity(SEND_BATCH_ENTRIES); router.work_unit_count()];
    let mut data = File::open(data_path)?;
    let mut next_record = partition.start_record;
    let mut scanned = 0usize;
    let mut sent = 0usize;

    set_progress(
        &progress,
        &task_label,
        Phase::Scanning,
        ProgressNumbers {
            records_scanned: 0,
            entries_sent: 0,
            entries_received: 0,
            bytes_read: 0,
            bytes_written: 0,
        },
    );

    while next_record < partition.end_record {
        let chunk_records = READ_CHUNK_RECORDS.min(partition.end_record - next_record);
        data.seek(SeekFrom::Start(record_offset(next_record)))?;
        let mut buffer = vec![0u8; chunk_records * RECORD_SIZE];
        data.read_exact(&mut buffer)?;

        for (chunk_idx, record_bytes) in buffer.chunks_exact(RECORD_SIZE).enumerate() {
            let record_idx = next_record + chunk_idx;
            let record = record_from_bytes(record_bytes)?;
            let destination = router.route(&record.key).0;
            batches[destination].push(IndexEntry {
                key: record.key,
                offset: record_offset(record_idx),
            });
            scanned += 1;

            if batches[destination].len() == SEND_BATCH_ENTRIES {
                let batch = std::mem::take(&mut batches[destination]);
                senders[destination]
                    .send(IndexMessage::Entries {
                        from: shard_id,
                        batch,
                    })
                    .await
                    .map_err(io_other)?;
                sent += SEND_BATCH_ENTRIES;
                set_progress(
                    &progress,
                    &task_label,
                    Phase::Sending,
                    ProgressNumbers {
                        records_scanned: scanned,
                        entries_sent: sent,
                        entries_received: 0,
                        bytes_read: scanned * RECORD_SIZE,
                        bytes_written: 0,
                    },
                );
            }
        }

        next_record += chunk_records;
    }

    for (destination, batch) in batches.into_iter().enumerate() {
        if !batch.is_empty() {
            sent += batch.len();
            senders[destination]
                .send(IndexMessage::Entries {
                    from: shard_id,
                    batch,
                })
                .await
                .map_err(io_other)?;
        }
    }

    for sender in &senders {
        sender
            .send(IndexMessage::ProducerDone { from: shard_id })
            .await
            .map_err(io_other)?;
    }

    set_progress(
        &progress,
        &task_label,
        Phase::Done,
        ProgressNumbers {
            records_scanned: scanned,
            entries_sent: sent,
            entries_received: 0,
            bytes_read: scanned * RECORD_SIZE,
            bytes_written: 0,
        },
    );

    Ok(())
}

async fn assemble_partition(
    mailboxes: Arc<WorkUnitMailboxSet<IndexWorkUnit, IndexMessage>>,
    work_unit: IndexWorkUnit,
    progress: ShardLocal<BTreeMap<String, ShardProgress>>,
    producer_count: usize,
    output_path: PathBuf,
) -> io::Result<PartitionRun> {
    let shard_id = current_executor_shard().expect("assembler runs on a shard");
    let cpu_placement =
        current_executor_cpu_placement().expect("assembler runs on a sharded executor");
    let task_label = work_unit.label();

    let mut receiver = mailboxes
        .receiver_for_current_shard(&work_unit)
        .map_err(io_other)?;
    let mut entries = Vec::new();
    let mut producers_done = 0usize;

    set_progress(
        &progress,
        &task_label,
        Phase::Receiving,
        ProgressNumbers::default(),
    );

    while producers_done < producer_count {
        match receiver.recv().await.map_err(io_other)? {
            IndexMessage::Entries { from, batch } => {
                debug_assert!(from.0 < producer_count);
                entries.extend(batch);
                set_progress(
                    &progress,
                    &task_label,
                    Phase::Receiving,
                    ProgressNumbers {
                        entries_received: entries.len(),
                        ..ProgressNumbers::default()
                    },
                );
            }
            IndexMessage::ProducerDone { from } => {
                debug_assert!(from.0 < producer_count);
                producers_done += 1;
            }
        }
    }

    set_progress(
        &progress,
        &task_label,
        Phase::Sorting,
        ProgressNumbers {
            entries_received: entries.len(),
            ..ProgressNumbers::default()
        },
    );
    entries.sort_unstable();
    write_index_file(&output_path, &entries)?;
    let bytes_written = entries.len() * INDEX_ENTRY_SIZE;
    set_progress(
        &progress,
        &task_label,
        Phase::Done,
        ProgressNumbers {
            entries_received: entries.len(),
            bytes_written,
            ..ProgressNumbers::default()
        },
    );

    Ok(PartitionRun {
        work_unit,
        shard_id,
        cpu_placement,
        entry_count: entries.len(),
        bytes_written,
        path: output_path,
    })
}

fn senders_for_assemblers(
    mailboxes: &WorkUnitMailboxSet<IndexWorkUnit, IndexMessage>,
    assembler_count: usize,
) -> io::Result<Vec<sitas::ShardSender<IndexMessage>>> {
    (0..assembler_count)
        .map(|idx| mailboxes.sender_to(&IndexWorkUnit(idx)).map_err(io_other))
        .collect()
}

fn assembler_work_units(
    assembler_count: usize,
    shard_count: usize,
) -> Vec<WorkUnitSpec<IndexWorkUnit>> {
    (0..assembler_count)
        .map(|idx| {
            // Intentionally non-uniform: the last logical assembler is placed
            // on shard 0. With the default assembler_count = shard_count + 1,
            // this gives shard 0 two receivers and makes the naming/placement
            // split visible in output.
            let assigned = if idx == assembler_count - 1 {
                ShardId(0)
            } else {
                ShardId(idx % shard_count)
            };
            WorkUnitSpec::new(IndexWorkUnit(idx), assigned)
        })
        .collect()
}

fn create_data_file(path: &Path, record_count: usize, seed: u64) -> io::Result<()> {
    // Deterministic input makes the demo reproducible across shard counts and
    // across runs when comparing the file-backed and mailbox variants.
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
    remove_file_if_exists(&paths.data)?;
    remove_file_if_exists(&paths.index)?;
    match fs::remove_dir_all(&paths.partition_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn k_way_merge_partition_files(partitions: &[PartitionRun], output_path: &Path) -> io::Result<()> {
    // Each assembler run is internally sorted. A heap over the current head of
    // each run is enough to produce one globally sorted output stream.
    let mut readers = partitions
        .iter()
        .map(|partition| RunReader::open(&partition.path))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();

    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = reader.next_entry()? {
            heap.push(Reverse((entry, idx)));
        }
    }

    let mut output = File::create(output_path)?;
    while let Some(Reverse((entry, idx))) = heap.pop() {
        write_index_entry(&mut output, entry)?;
        if let Some(next) = readers[idx].next_entry()? {
            heap.push(Reverse((next, idx)));
        }
    }

    Ok(())
}

fn write_index_file(path: &Path, entries: &[IndexEntry]) -> io::Result<()> {
    let mut file = File::create(path)?;
    for entry in entries {
        write_index_entry(&mut file, *entry)?;
    }
    Ok(())
}

fn write_index_entry(file: &mut File, entry: IndexEntry) -> io::Result<()> {
    file.write_all(&entry.key.to_le_bytes())?;
    file.write_all(&entry.offset.to_le_bytes())
}

fn verify_index_file(
    data_path: &Path,
    index_path: &Path,
    expected_entries: usize,
) -> io::Result<()> {
    // Verification checks the product, not the implementation path: sorted
    // order across the final index and each stored offset pointing to a record
    // with the matching key.
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

fn print_progress(
    label: &str,
    runtime: &ShardedExecutor,
    progress: &ShardLocal<BTreeMap<String, ShardProgress>>,
) {
    let snapshot = runtime.snapshot();
    let progress_values: Vec<(ShardId, BTreeMap<String, ShardProgress>)> =
        block_on(progress.map_all(|_, p| p.clone())).unwrap_or_default();

    println!("{label}:");
    for shard in &snapshot.shards {
        let task_count = shard
            .executor
            .as_ref()
            .map_or(0, |executor| executor.task_count);
        let tasks = progress_values
            .iter()
            .find(|(id, _)| *id == shard.shard_id)
            .map(|(_, map)| map);
        if let Some(map) = tasks {
            for (task_label, p) in map {
                println!(
                    "  shard {} {} {task_label} phase={} scanned={} sent={} received={} read={}B wrote={}B tasks={}",
                    shard.shard_id.0,
                    shard.cpu_placement,
                    p.phase.name(),
                    p.numbers.records_scanned,
                    p.numbers.entries_sent,
                    p.numbers.entries_received,
                    p.numbers.bytes_read,
                    p.numbers.bytes_written,
                    task_count
                );
            }
        } else {
            println!(
                "  shard {} {} phase=idle tasks={task_count}",
                shard.shard_id.0, shard.cpu_placement,
            );
        }
    }
}

fn set_progress(
    progress: &ShardLocal<BTreeMap<String, ShardProgress>>,
    task_label: &str,
    phase: Phase,
    numbers: ProgressNumbers,
) {
    let _ = progress.with_current_result(|map| {
        let p = map.entry(task_label.to_string()).or_default();
        p.phase = phase;
        p.numbers = numbers;
    });
}

fn read_record(file: &mut File) -> io::Result<Record> {
    let mut buffer = [0u8; RECORD_SIZE];
    file.read_exact(&mut buffer)?;
    record_from_bytes(&buffer)
}

fn record_from_bytes(buffer: &[u8]) -> io::Result<Record> {
    debug_assert_eq!(buffer.len(), RECORD_SIZE);
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
    file.write_all(&record.payload)
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

fn io_other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

struct RunReader {
    file: File,
}

impl RunReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        read_index_entry(&mut self.file)
    }
}

#[derive(Debug, Clone, Copy)]
struct DemoConfig {
    record_count: usize,
    shard_count: usize,
    seed: u64,
    mailbox_capacity: usize,
    assembler_count: Option<usize>,
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
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            assembler_count: None,
            cleanup: true,
        };

        let mut args = args.into_iter().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--records" => config.record_count = parse_next(&mut args, "--records")?,
                "--shards" => config.shard_count = parse_next(&mut args, "--shards")?,
                "--seed" => config.seed = parse_seed(parse_next_string(&mut args, "--seed")?)?,
                "--mailbox-capacity" => {
                    config.mailbox_capacity = parse_next(&mut args, "--mailbox-capacity")?;
                }
                "--assemblers" => {
                    config.assembler_count = Some(parse_next(&mut args, "--assemblers")?)
                }
                "--no-cleanup" => config.cleanup = false,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other}").into()),
            }
        }

        if config.shard_count == 0 {
            return Err("shard count must be greater than zero".into());
        }
        if config.record_count == 0 {
            return Err("record count must be greater than zero".into());
        }
        if config.mailbox_capacity == 0 {
            return Err("mailbox capacity must be greater than zero".into());
        }
        if matches!(config.assembler_count, Some(0)) {
            return Err("assembler count must be greater than zero".into());
        }

        Ok(config)
    }
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T::Err: std::error::Error + 'static,
{
    Ok(parse_next_string(args, name)?.parse()?)
}

fn parse_next_string(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn parse_seed(value: String) -> Result<u64, Box<dyn std::error::Error>> {
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        Ok(u64::from_str_radix(hex, 16)?)
    } else {
        Ok(value.parse()?)
    }
}

fn print_usage() {
    println!(
        "usage: cargo run --example sharded_index_mailbox -- [--records N] [--shards N] [--assemblers N] [--seed N|0xN] [--mailbox-capacity N] [--no-cleanup]"
    );
}

struct DemoPaths {
    data: PathBuf,
    index: PathBuf,
    partition_dir: PathBuf,
}

impl DemoPaths {
    fn new() -> Self {
        let base = env::temp_dir().join(format!("sitas-mailbox-index-{}", std::process::id()));
        Self {
            data: base.with_extension("data"),
            index: base.with_extension("index"),
            partition_dir: base.with_extension("parts"),
        }
    }

    fn partition_path(&self, shard_idx: usize) -> PathBuf {
        self.partition_dir
            .join(format!("partition-{shard_idx}.run"))
    }
}

#[derive(Debug, Clone, Copy)]
struct Partition {
    start_record: usize,
    end_record: usize,
}

impl Partition {
    fn for_shard(shard_idx: usize, shard_count: usize, record_count: usize) -> Self {
        let base = record_count / shard_count;
        let remainder = record_count % shard_count;
        let start = shard_idx * base + shard_idx.min(remainder);
        let len = base + usize::from(shard_idx < remainder);
        Self {
            start_record: start,
            end_record: start + len,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Record {
    key: u64,
    payload: [u8; PAYLOAD_SIZE],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexEntry {
    key: u64,
    offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IndexWorkUnit(usize);

impl IndexWorkUnit {
    fn label(self) -> String {
        format!("assembler-{}", self.0)
    }
}

#[derive(Debug)]
enum IndexMessage {
    Entries {
        from: ShardId,
        batch: Vec<IndexEntry>,
    },
    ProducerDone {
        from: ShardId,
    },
}

#[derive(Debug)]
struct PartitionRun {
    work_unit: IndexWorkUnit,
    shard_id: ShardId,
    cpu_placement: sitas::CpuPlacementStatus,
    entry_count: usize,
    bytes_written: usize,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, Default)]
struct ShardProgress {
    phase: Phase,
    numbers: ProgressNumbers,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProgressNumbers {
    records_scanned: usize,
    entries_sent: usize,
    entries_received: usize,
    bytes_read: usize,
    bytes_written: usize,
}

#[derive(Debug, Clone, Copy, Default)]
enum Phase {
    #[default]
    Idle,
    Scanning,
    Sending,
    Receiving,
    Sorting,
    Done,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Scanning => "scanning",
            Self::Sending => "sending",
            Self::Receiving => "receiving",
            Self::Sorting => "sorting",
            Self::Done => "done",
        }
    }
}

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
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let value = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&value[..chunk.len()]);
        }
    }
}
