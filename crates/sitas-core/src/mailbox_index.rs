//! Sharded in-memory index build over typed inter-shard mailboxes.
//!
//! This is the `no_std` counterpart of the host `sharded_index_mailbox`
//! example. It exercises the same mailbox idea without `std`: scanner shards
//! generate a deterministic record set, route each key to a *logical* assembler
//! work unit through a typed [`ShardSender`] channel, and assemblers receive
//! the owned entry batches, sort them into runs, and hand the completed runs
//! back to the coordinator over a result channel. The coordinator merges the
//! runs and verifies the final index, then writes the verified record count to
//! a caller-provided result page.
//!
//! There are no files: the record set is generated on the fly from a pure
//! function of the record index, so the coordinator can re-derive each key when
//! verifying, and cross-shard exchange happens entirely through owned messages.
//!
//! The no_std channel only exposes a non-blocking [`ShardSender::try_send`], so
//! a producer whose mailbox is full backs off by parking on the runtime's
//! [`ShardParker`] and retries instead of spinning. The coordinator likewise
//! parks (never spins) while waiting for assembler results.

use alloc::boxed::Box;
use alloc::collections::BinaryHeap;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::Reverse;
use core::ptr;
use core::time::Duration;

use crate::placement::{ShardPlacement, shard_for_hash};
use crate::reactor_backend::ReactorBackend;
use crate::shard::ShardId;
use crate::shard_executor::ShardExecutor;
use crate::shard_runtime::{ShardParker, ShardReceiver, ShardRuntime, ShardSender};

/// Number of records in the in-memory data set.
pub const RECORD_COUNT: usize = 512;
/// Number of physical shards (scanner + assembler threads).
pub const SHARD_COUNT: usize = 2;
/// Number of logical assembler work units: one more than shards, with the last
/// one deliberately placed on shard 0 so logical-vs-physical placement is
/// visible.
pub const ASSEMBLER_COUNT: usize = 3;
/// Deterministic input seed (the example's default).
pub const SEED: u64 = 0x517a_5eed;

/// Success sentinel: the verified record count written to the result page.
pub const SUCCESS_SENTINEL: u32 = RECORD_COUNT as u32;
/// Failure sentinel written to the result page on any error.
pub const FAILURE_SENTINEL: u32 = 0xBAD_C0DE;

/// Entries batched into one mailbox message.
const SEND_BATCH_ENTRIES: usize = 32;
/// Capacity of each assembler mailbox.
const CHANNEL_CAPACITY: usize = 32;
/// Capacity of the coordinator result mailbox.
const RESULT_CHANNEL_CAPACITY: usize = 8;
/// Park interval used while a requester waits for work to finish.
const PARK_TIMEOUT: Duration = Duration::from_millis(5);
/// Park interval a producer sleeps before retrying a full mailbox.
const SEND_BACKOFF: Duration = Duration::from_millis(1);

/// Runs the CharlotteOS mailbox index demo and writes its result to
/// `result_page` (the verified record count, or [`FAILURE_SENTINEL`]).
///
/// # Safety
///
/// `result_page` must be valid for a volatile `u32` write for the duration of
/// this call.
pub unsafe fn mailbox_index_test<R: ShardRuntime + ?Sized>(runtime: &R, result_page: *mut u32) {
    let sentinel = match build_index_with_mailboxes(runtime) {
        Ok(verified) if verified == RECORD_COUNT => verified as u32,
        _ => FAILURE_SENTINEL,
    };
    unsafe { ptr::write_volatile(result_page, sentinel) };
}

/// Runs the full scanner -> mailbox -> assembler -> coordinator pipeline and
/// returns the number of verified index entries.
fn build_index_with_mailboxes<R: ShardRuntime + ?Sized>(runtime: &R) -> Result<usize, ()> {
    let parker = runtime.parker();

    // One typed mailbox per logical assembler. Senders are cloned into the
    // scanner threads; receivers move into the assembler shards.
    let mut assembler_senders = Vec::with_capacity(ASSEMBLER_COUNT);
    let mut assembler_receivers = Vec::with_capacity(ASSEMBLER_COUNT);
    for _ in 0..ASSEMBLER_COUNT {
        let (sender, receiver) =
            runtime.channel(CHANNEL_CAPACITY).map_err(|_| ())?;
        assembler_senders.push(sender);
        assembler_receivers.push(receiver);
    }

    // Result mailbox: assemblers deliver their owned sorted runs here.
    let (result_sender, mut result_receiver) = runtime
        .channel(RESULT_CHANNEL_CAPACITY)
        .map_err(|_| ())?;

    // Start receivers before producers.
    for (work_unit, receiver) in assembler_receivers.into_iter().enumerate() {
        let placement = work_unit_shard(work_unit);
        let parker = Arc::clone(&parker);
        let result_sender = result_sender.clone();
        let reactor = runtime.shard_reactor(placement);
        runtime.spawn_shard(
            placement,
            ShardPlacement::Sequential,
            Box::new(move || {
                run_assembler_shard(receiver, parker, result_sender, work_unit, reactor)
            }),
        );
    }

    // One producer thread per shard; each routes its partition by key to the
    // logical assemblers.
    for shard_idx in 0..SHARD_COUNT {
        let senders = assembler_senders.clone();
        let parker = Arc::clone(&parker);
        runtime.spawn_shard(
            ShardId(shard_idx),
            ShardPlacement::Sequential,
            Box::new(move || run_scanner_shard(senders, parker, shard_idx)),
        );
    }

    // Wait for every assembler run, parking (never spinning) between polls.
    let mut runs = Vec::with_capacity(ASSEMBLER_COUNT);
    while runs.len() < ASSEMBLER_COUNT {
        match result_receiver.try_recv() {
            Some(run) => runs.push(run),
            None => parker.park(Some(PARK_TIMEOUT)),
        }
    }

    verify_runs(&runs)
}

/// A logical assembler's physical shard: the last work unit is placed on shard
/// 0, everything else round-robins, mirroring the host example's non-uniform
/// placement.
fn work_unit_shard(work_unit: usize) -> ShardId {
    if work_unit == ASSEMBLER_COUNT - 1 {
        ShardId(0)
    } else {
        ShardId(work_unit % SHARD_COUNT)
    }
}

/// A scanner shard: generate its partition of records, batch entries by
/// destination assembler, and send them as owned messages.
fn run_scanner_shard(
    senders: Vec<ShardSender<IndexMessage>>,
    parker: Arc<dyn ShardParker>,
    shard_idx: usize,
) {
    let (start, end) = partition_for(shard_idx, SHARD_COUNT, RECORD_COUNT);
    let shard_id = ShardId(shard_idx);
    let mut batches = vec![Vec::with_capacity(SEND_BATCH_ENTRIES); ASSEMBLER_COUNT];

    for record_idx in start..end {
        let key = record_key(record_idx);
        let assembler = shard_for_hash(&key, ASSEMBLER_COUNT).0;
        batches[assembler].push(IndexEntry {
            key,
            offset: record_idx as u64,
        });
        if batches[assembler].len() == SEND_BATCH_ENTRIES {
            send_message(
                &senders[assembler],
                parker.as_ref(),
                IndexMessage::Entries {
                    from: shard_id,
                    batch: core::mem::take(&mut batches[assembler]),
                },
            );
        }
    }

    // Flush the trailing partial batches.
    for (assembler, batch) in batches.into_iter().enumerate() {
        if !batch.is_empty() {
            send_message(
                &senders[assembler],
                parker.as_ref(),
                IndexMessage::Entries {
                    from: shard_id,
                    batch,
                },
            );
        }
    }

    // Signal completion to every assembler.
    for sender in &senders {
        send_message(sender, parker.as_ref(), IndexMessage::ProducerDone { from: shard_id });
    }
}

/// An assembler shard: its message loop is a task on its own `ShardExecutor`.
/// Receiving parks the shard in the reactor wait; a scanner's send wakes
/// exactly this task and releases the reactor (the same channel-wake chain the
/// sharded KV uses).
fn run_assembler_shard<R>(
    receiver: ShardReceiver<IndexMessage>,
    parker: Arc<dyn ShardParker>,
    result_sender: ShardSender<RunResult>,
    work_unit: usize,
    reactor: R,
) where
    R: ReactorBackend + Send + 'static,
    R::Waker: 'static,
{
    let mut executor = ShardExecutor::new(reactor).with_idle_wait(Some(PARK_TIMEOUT));
    executor.spawn(assembler_task(receiver, parker, result_sender, work_unit));
    executor.run();
}

async fn assembler_task(
    mut receiver: ShardReceiver<IndexMessage>,
    parker: Arc<dyn ShardParker>,
    result_sender: ShardSender<RunResult>,
    work_unit: usize,
) {
    let mut entries = Vec::new();
    let mut producers_done = 0usize;

    while producers_done < SHARD_COUNT {
        match receiver.recv().await {
            Some(IndexMessage::Entries { from, batch }) => {
                debug_assert!(from.0 < SHARD_COUNT, "scanner shard out of range");
                entries.extend(batch);
            }
            Some(IndexMessage::ProducerDone { from }) => {
                debug_assert!(from.0 < SHARD_COUNT, "scanner shard out of range");
                producers_done += 1;
            }
            None => return,
        }
    }

    entries.sort_unstable();
    send_run(&result_sender, parker.as_ref(), RunResult { work_unit, entries });
}

/// Send one message through a mailbox, backing off with a short park when the
/// mailbox is full. `try_send` returns the message on failure, so it is simply
/// re-sent after the backoff.
fn send_message(
    sender: &ShardSender<IndexMessage>,
    parker: &dyn ShardParker,
    mut message: IndexMessage,
) {
    loop {
        match sender.try_send(message) {
            Ok(()) => return,
            Err(returned) => {
                parker.park(Some(SEND_BACKOFF));
                message = returned;
            }
        }
    }
}

/// Deliver the owned sorted run to the coordinator, backing off on a full
/// result mailbox.
fn send_run(
    sender: &ShardSender<RunResult>,
    parker: &dyn ShardParker,
    mut run: RunResult,
) {
    loop {
        match sender.try_send(run) {
            Ok(()) => return,
            Err(returned) => {
                parker.park(Some(SEND_BACKOFF));
                run = returned;
            }
        }
    }
}

/// Check every run is internally sorted, then k-way merge the runs and verify
/// the final index: globally sorted, complete, and every entry pointing at a
/// record with the matching key.
fn verify_runs(runs: &[RunResult]) -> Result<usize, ()> {
    for run in runs {
        if run.entries.windows(2).any(|window| window[0] > window[1]) {
            return Err(());
        }
    }

    let mut merged = Vec::with_capacity(RECORD_COUNT);
    let mut heap = BinaryHeap::new();
    let mut cursor = vec![0usize; runs.len()];

    for (run_idx, run) in runs.iter().enumerate() {
        if let Some(&first) = run.entries.first() {
            heap.push(Reverse((first, run_idx)));
        }
    }

    while let Some(Reverse((entry, run_idx))) = heap.pop() {
        merged.push(entry);
        cursor[run_idx] += 1;
        if let Some(&next) = runs[run_idx].entries.get(cursor[run_idx]) {
            heap.push(Reverse((next, run_idx)));
        }
    }

    if merged.len() != RECORD_COUNT {
        return Err(());
    }
    if merged.windows(2).any(|window| window[0] > window[1]) {
        return Err(());
    }

    // Every logical assembler must have delivered exactly one run.
    let mut work_units: Vec<usize> = runs.iter().map(|run| run.work_unit).collect();
    work_units.sort_unstable();
    if work_units.iter().copied().ne(0..ASSEMBLER_COUNT) {
        return Err(());
    }

    // Every entry must address the record it claims to, and every record
    // exactly once. Offsets live in `[0, RECORD_COUNT)`, so a bijection holds
    // iff their XOR is the XOR of the whole range (completeness + no dupes).
    let mut offset_xor = 0u64;
    let mut expected_xor = 0u64;
    for entry in &merged {
        if entry.key != record_key(entry.offset as usize) {
            return Err(());
        }
        offset_xor ^= entry.offset;
    }
    for record_idx in 0..RECORD_COUNT {
        expected_xor ^= record_idx as u64;
    }
    if offset_xor != expected_xor {
        return Err(());
    }

    Ok(merged.len())
}

/// Deterministic key for `record_idx`, reproducible from any shard so the
/// coordinator can verify without storing the data set.
fn record_key(record_idx: usize) -> u64 {
    splitmix64(SEED ^ (record_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// SplitMix64 finalizer: a cheap injective-ish avalanche of a single u64.
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Contiguous record partition for one scanner shard.
fn partition_for(shard_idx: usize, shard_count: usize, record_count: usize) -> (usize, usize) {
    let base = record_count / shard_count;
    let remainder = record_count % shard_count;
    let start = shard_idx * base + shard_idx.min(remainder);
    let len = base + usize::from(shard_idx < remainder);
    (start, start + len)
}

/// One index entry: the record key and the logical record offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexEntry {
    key: u64,
    offset: u64,
}

/// A typed mailbox message from a scanner shard to an assembler work unit.
enum IndexMessage {
    Entries {
        from: ShardId,
        batch: Vec<IndexEntry>,
    },
    ProducerDone {
        from: ShardId,
    },
}

/// A completed, sorted assembler run delivered to the coordinator.
struct RunResult {
    work_unit: usize,
    entries: Vec<IndexEntry>,
}
