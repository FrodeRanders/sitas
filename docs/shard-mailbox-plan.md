# Shard Mailbox and Index Transfer Demo Plan

## Purpose

This plan introduces an explicit message-transfer layer between executor shards.
The goal is to let work units transfer owned data directly between shards without
using files as the cross-shard exchange medium, while preserving the project's
shared-nothing application-state model.

The existing `examples/sharded_index_build.rs` and
`examples/sharded_index_build_uring.rs` demos should remain unchanged. A new
example will demonstrate an alternative index build that keeps shard ownership
through message transfer.

## Design Position

The mailbox is a transport for owned messages, not shared mutable state.

Invariants:

- only the owning shard mutates its local state;
- values crossing shard boundaries are owned and `Send + 'static`;
- cross-shard communication is explicit in the type system;
- receivers run on the owning shard;
- no references to shard-local state cross `.await`;
- observability exposes owned snapshots;
- unsafe code is avoided in the first implementation.

The first implementation should be conservative: a bounded per-shard inbound
mailbox with multiple producers and one shard-local consumer. Internally, use a
small safe ring buffer guarded by a mutex plus executor wakeups. Do not start
with a lock-free MPMC queue. The API and tests should define the semantics
first; a later optimization can replace the internal queue with a more atomic
ring without changing callers.

## Chosen Implementation Route

### Model

Use one inbound mailbox per shard:

```text
producer shard/task(s)  ->  target shard inbound ring  ->  target shard consumer task
```

Each inbound mailbox has:

- a bounded ring of owned messages;
- cloneable senders that can be used from any shard thread;
- one receiver consumed by a task running on the target shard;
- async receive wakeups integrated with the existing executor waker model;
- non-blocking send first, then optional async send/backpressure.

This keeps the ownership story simple: many tasks may transfer messages to a
target shard, but only the target shard drains and mutates the state associated
with those messages.

### Initial Module

Add a new module:

```text
src/shard_mailbox.rs
```

Export from `src/lib.rs` after the public API settles.

Initial public types:

```rust
pub struct ShardMailbox<M>;
pub struct ShardSender<M>;
pub struct ShardReceiver<M>;
pub struct ShardMailboxSet<M>;
pub struct WorkUnitSpec<N>;
pub struct WorkUnitMailboxSet<N, M>;

pub struct ShardMailboxConfig {
    pub capacity_per_shard: usize,
}

pub enum ShardMailError<M> {
    Full(M),
    Closed(M),
    InvalidShard { shard_id: ShardId },
}

pub enum ShardRecvError {
    Closed,
}
```

The exact names can be adjusted during implementation, but the API should make
ownership transfer and failure modes visible.

### Logical Work-Unit Names

The per-shard mailbox is the primitive. A named work-unit directory should sit
on top of it for protocols that need non-uniform work assignment:

```rust
let mailboxes = WorkUnitMailboxSet::<IndexWorkUnit, IndexMessage>::new(
    &runtime.submitter(),
    [
        WorkUnitSpec::new(IndexWorkUnit::Assembler(0), ShardId(0)),
        WorkUnitSpec::new(IndexWorkUnit::Assembler(1), ShardId(0)),
        WorkUnitSpec::new(IndexWorkUnit::Assembler(2), ShardId(2)),
    ],
    ShardMailboxConfig::new(capacity),
)?;
```

This gives each logical work unit its own mailbox while preserving explicit
placement. Multiple work units may be assigned to the same shard, and some
shards may have no work unit for a given protocol. Senders route by logical
name:

```rust
mailboxes.sender_to(&IndexWorkUnit::Assembler(owner))?;
```

Receiver tasks take the mailbox for their logical name and should verify that
they are running on the assigned shard:

```rust
mailboxes.receiver_for_current_shard(&IndexWorkUnit::Assembler(owner))?;
```

This keeps raw `ShardId` as the physical placement identity and work-unit names
as the logical addressing identity.

### Uniform vs Non-Uniform Routing

The routing discriminator should be explicit:

```text
uniform:
    key -> ShardId -> ShardMailboxSet<M>

non-uniform:
    key -> WorkUnitName -> WorkUnitMailboxSet<N, M> -> assigned ShardId
```

Use `UniformShardRouter` when logical ownership is the physical shard:

```rust
let router = UniformShardRouter::new(runtime.shard_count())?;
let shard = router.route(&key);
mailboxes.sender_to(shard)?.try_send(message)?;
```

Use `WorkUnitRouter<N>` when the key selects a logical work unit and placement
is separate:

```rust
let router = WorkUnitRouter::new([Assembler(0), Assembler(1), Assembler(2)])?;
let owner = router.route(&key);
mailboxes.sender_to(&owner)?.try_send(message)?;
```

The key discriminator remains available in both models. The difference is the
target type: physical shard for uniform routing, logical work-unit name for
non-uniform routing.

### Construction

The mailbox set should be created against a `ShardedSubmitter` or shard count:

```rust
let mailboxes = ShardMailboxSet::<IndexMessage>::new(
    &runtime.submitter(),
    ShardMailboxConfig::new(capacity),
)?;
```

It should expose:

```rust
mailboxes.sender_to(shard_id) -> Result<ShardSender<M>, ShardMailError<M>>
mailboxes.receiver_for_current_shard() -> Result<ShardReceiver<M>, ...>
mailboxes.receiver_for(shard_id) -> Result<ShardReceiver<M>, ...>
```

Only one receiver may be taken per shard. Taking a second receiver should fail.
This preserves the single-consumer assumption.

### Send API

Start with non-blocking send:

```rust
impl<M: Send + 'static> ShardSender<M> {
    pub fn target_shard(&self) -> ShardId;
    pub fn try_send(&self, message: M) -> Result<(), ShardMailError<M>>;
    pub fn close(&self);
}
```

Add async send after receive semantics and wakeups are stable:

```rust
pub async fn send(&self, message: M) -> Result<(), ShardMailError<M>>;
```

The async send future should complete when capacity becomes available, and
dropped send futures must not leak wakers or permits.

### Receive API

The receiver is consumed on the target shard:

```rust
impl<M: Send + 'static> ShardReceiver<M> {
    pub fn shard_id(&self) -> ShardId;
    pub fn try_recv(&mut self) -> Result<M, ShardRecvError>;
    pub async fn recv(&mut self) -> Result<M, ShardRecvError>;
    pub fn close(&mut self);
}
```

`recv().await` must register the current task's waker and be woken when:

- a message is enqueued;
- all senders are dropped and the ring is empty;
- the receiver is closed.

### Ring Buffer Semantics

The first internal ring should be safe and simple:

```text
Mutex<Inner<M>>

Inner<M> {
    ring: Vec<Option<M>>,
    head: usize,
    tail: usize,
    len: usize,
    capacity: usize,
    sender_count: usize,
    receiver_taken: bool,
    receiver_closed: bool,
    recv_waker: Option<Waker>,
    send_wakers: Vec<Waker>, // only when async send is added
}
```

This is still a ring buffer, but guarded by a mutex so the first version avoids
subtle memory-ordering bugs. The safe API is the important artifact. Once tests
define behavior, the internal queue can be optimized.

Use `Vec<Option<M>>` or `VecDeque<M>` only behind this abstraction. Do not expose
the storage structure.

### Backpressure

Phase 1:

- bounded `try_send`;
- callers handle `Full(message)` explicitly.

Phase 2:

- `send(message).await`;
- sender waker registration;
- dropped send future cleanup;
- receiver wake of one or more send waiters when capacity opens.

For the index demo, start with bounded `try_send` and local batching. Producers
can retry by yielding when a destination is full. That keeps the first demo
simple while still exercising backpressure.

### Shutdown

Define shutdown before implementation:

- dropping the last sender closes the stream after all queued messages drain;
- closing the receiver causes future sends to return `Closed(message)`;
- dropping the receiver wakes blocked senders once async send exists;
- dropping the mailbox set closes all receivers and senders;
- messages remaining in a dropped mailbox are dropped exactly once.

### Observability

Add owned snapshots:

```rust
pub struct ShardMailboxSnapshot {
    pub shard_id: ShardId,
    pub capacity: usize,
    pub len: usize,
    pub sender_count: usize,
    pub receiver_taken: bool,
    pub receiver_closed: bool,
    pub sent: u64,
    pub received: u64,
    pub full_rejections: u64,
    pub closed_rejections: u64,
}
```

The snapshot should not expose message references or storage internals.

## Test Plan

Implement narrow unit tests first:

- construction rejects zero capacity;
- `try_send` followed by `try_recv` transfers one owned message;
- messages preserve FIFO order per target mailbox;
- full mailbox returns `ShardMailError::Full(message)` with the original value;
- receiver close rejects future sends with the original value;
- dropping all senders lets the receiver observe closure after draining;
- taking two receivers for the same shard fails;
- queued messages are dropped exactly once;
- snapshots report capacity, length, sender count, close state, and counters.

Then executor integration tests:

- `recv().await` wakes when another shard sends;
- a task awaiting `recv()` stays on its original target shard when resumed;
- many producers can send to one shard;
- all shards can send to all shards without shared application state;
- runtime shutdown wakes pending receivers cleanly.

Then stress tests:

- many small messages across all shards;
- bounded-capacity retry loop with `yield_now`;
- randomized sender drops and receiver closure.

## New Demo: `sharded_index_mailbox`

Add a new example:

```text
examples/sharded_index_mailbox.rs
```

Do not modify:

```text
examples/sharded_index_build.rs
examples/sharded_index_build_uring.rs
```

### Demo Goal

Demonstrate index construction where shards transfer owned index entries to
their owning partition shard instead of writing sorted intermediate run files
for cross-shard exchange.

The final index may still be written to a file because it is the external
result. The important difference is that intermediate cross-shard data moves by
typed mailbox messages, not by run files.

### Data Ownership Policy

Route entries by key:

```rust
fn owner_for_key(key: u64, shard_count: usize) -> ShardId {
    ShardId((key as usize) % shard_count)
}
```

Any shard may scan records, but the shard selected by `owner_for_key` owns and
mutates the in-memory partition for that key range/hash bucket.

This means all shards stay active in the assembly phase:

- scan shards produce entries;
- destination shards receive entries;
- each destination shard sorts and finalizes its own partition.

### Message Types

Use a typed enum local to the demo:

```rust
enum IndexMessage {
    Entries {
        from: ShardId,
        batch: Vec<IndexEntry>,
    },
    ProducerDone {
        from: ShardId,
    },
}
```

Messages are owned. No borrowed buffers cross shards.

### Demo Flow

1. Create the deterministic data file exactly like the existing index demo.
2. Start `ShardedExecutor` with sequential CPU placement.
3. Create `ShardLocal<ShardProgress>` for per-shard progress.
4. Create `WorkUnitMailboxSet<IndexWorkUnit, IndexMessage>` with bounded
   capacity and explicit logical work-unit placement.
5. Spawn one receiver/assembler task per assembler work unit:
   - take that work unit's receiver on its assigned shard;
   - collect incoming `Entries` batches into a shard-local `Vec<IndexEntry>`;
   - count `ProducerDone` messages from all producer shards;
   - when all producers are done and the mailbox is drained, sort local entries;
   - write one final partition file owned by that work unit;
   - return `ShardRun` metadata.
6. Spawn one scanner task per shard:
   - scan its contiguous data partition;
   - create `IndexEntry { key, offset }`;
   - group entries by logical `owner_for_key`;
   - send batches to destination work-unit mailboxes by logical name;
   - send one `ProducerDone` to every assembler work unit.
7. Join scanner tasks.
8. Join assembler tasks.
9. Merge only the final per-shard partition files into the advertised index, or
   concatenate if the ownership policy is changed to range partitioning.
10. Verify the final index using the existing verification logic.

### Sorting and Final Output

Hash ownership keeps distribution simple but does not produce globally ordered
partitions by concatenation. The first mailbox demo should therefore:

- sort each destination shard's local entries;
- write one sorted partition file per shard;
- do a final k-way merge into the output index file.

This keeps the demo correct while still avoiding file-based exchange during
assembly.

A later variant can use range partitioning by sampled key boundaries. That would
allow partition-file concatenation, but it requires either a sampling pass or
precomputed ranges and is not needed for the first message-transfer demo.

### Progress Output

Extend the existing progress fields only in the demo-local type:

```rust
enum Phase {
    Scanning,
    Sending,
    Receiving,
    Sorting,
    Writing,
    Done,
}
```

Track:

- records scanned;
- entries sent;
- entries received;
- mailbox full retries;
- bytes read;
- bytes written.

### Expected Demonstration Value

The current index demo shows:

- shard-local partition scan;
- file-materialized run exchange;
- decreasing parallelism as merge rounds collapse.

The new mailbox demo should show:

- shard-local scan remains;
- cross-shard transfer is typed messages;
- assembly keeps all destination shards active;
- only final partition files and final output are file-backed;
- shared mutable index state is still absent.

## Implementation Phases

### Phase 1: Mailbox Semantics

- Add `src/shard_mailbox.rs`.
- Implement bounded safe ring with `try_send` and `try_recv`.
- Implement close/drop behavior.
- Add owned snapshots.
- Add unit tests for construction, FIFO, full, close, drop, and snapshot cases.
- Add named work-unit routing with explicit `WorkUnitSpec<N>` placement for
  non-uniform logical assignment.
- Add tests for duplicate names, invalid shard placement, wrong-shard receiver
  acquisition, and multiple work units assigned to one shard.
- Export public types from `src/lib.rs`.
- Update `docs/architecture.md` with the new module responsibilities.

### Phase 2: Executor Receive

- Add async `recv()` future.
- Register and wake receiver task when producers enqueue messages.
- Test wakeup from another shard.
- Test receiver closure and sender drop wakeups.

### Phase 3: Async Backpressure

- Add async `send()` future if demo retry loops become noisy.
- Track sender waiters and wake when capacity opens.
- Test dropped send futures do not leak wakers or messages.

### Phase 4: Index Mailbox Demo

- Add `examples/sharded_index_mailbox.rs`.
- Reuse deterministic file generation and verification logic from the existing
  demo, copying helper code if needed to keep examples independent.
- Implement scanner tasks that batch by destination shard.
- Implement assembler tasks that receive and sort owned entries.
- Write per-shard partition files and final merged index.
- Print runtime snapshots and progress snapshots.

### Phase 5: Broader Validation

Run:

```bash
cargo fmt --check
cargo test shard_mailbox
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```

For Linux:

```bash
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh cargo test
```

Also run the demos manually:

```bash
cargo run --example sharded_index_build
cargo run --example sharded_index_mailbox
```

## Non-Goals For The First Version

- lock-free MPMC queues;
- borrowed cross-shard buffers;
- global load balancing;
- work stealing;
- distributed messaging;
- replacing `ShardedSubmitter`;
- removing the existing file-backed index demos;
- production-grade zero-copy buffer ownership.

## Later Optimizations

Once the safe mailbox API is stable:

- replace the mutex-protected ring with an atomic bounded MPSC ring;
- add per-producer ordering metrics;
- add batch send/recv APIs;
- add owned buffer pools for large transfers;
- add a reusable placement registry if multiple protocols need shared logical
  naming and discovery;
- add range-partitioned index demo variant that can concatenate final shard
  outputs without a k-way merge;
- integrate mailbox counters into executor or runtime snapshots if they prove
  broadly useful.
