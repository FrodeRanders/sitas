# NUMA, Rust Ownership, and Sitas Memory Placement

This note captures the design implications of NUMA placement for Sitas. It was
prompted by Edera's article
["NUMA - Part 1: Cores, memory, and the distance between them"](https://edera.dev/stories/numa-part-1-cores-memory-and-the-distance-between-them),
published June 23, 2026.

The article's central point is directly relevant to a shard-per-core runtime:
CPU placement and memory placement are separate axes. Pinning a task to a CPU is
not enough if the pages it mostly touches live on a remote NUMA node. In the
worst case, CPU pinning can make bad locality stable by keeping a task fixed on
one node while its hot memory remains on another.

## NUMA Model

NUMA means non-uniform memory access. A machine exposes multiple memory nodes.
Each node has CPUs that are local to it and memory that can be reached cheaply by
those CPUs. CPUs can still read memory from other nodes, but that access crosses
an interconnect and usually costs more latency, lower bandwidth, and more tail
variance under load.

The practical model for Sitas is:

- a shard can be pinned to a CPU;
- the CPU may belong to a NUMA node;
- memory touched by that shard should preferably live on the same node;
- cross-node access is valid, but should be treated as a performance cost;
- cross-node traffic can become an interconnect bottleneck under load.

NUMA is not only a multi-socket concern. Modern servers can expose multiple NUMA
nodes inside one socket through chiplet or clustering configurations. Code
should therefore reason in terms of observed CPU-to-node topology, not sockets.

## First Touch

Linux commonly follows first-touch behavior for ordinary demand-paged memory:
the page is physically allocated when some CPU first writes or faults it in, and
the page is placed according to that CPU's memory policy and locality.

This matters because allocation and initialization are separate events:

```rust
let mut data = Vec::with_capacity(n);
// No page locality has necessarily been established for all future elements.

for item in source {
    data.push(item);
}
// The thread doing these writes is likely the first-touch thread.
```

If one startup thread allocates and initializes a large buffer, then hands it to
workers on other NUMA nodes, the workers may own the buffer in Rust terms while
still paying remote memory access in hardware terms.

## Rust Ownership Is Not Physical Locality

Rust ownership answers who may access or mutate a value according to the type
system. It does not promise where the backing pages live.

Moving these values usually moves a small handle:

- `Box<T>` moves a pointer;
- `Vec<T>` moves pointer, length, and capacity;
- `String` moves pointer, length, and capacity;
- `Arc<T>` moves or clones a shared pointer;
- an owned buffer submitted through a mailbox may move only the descriptor.

The backing allocation generally remains where its pages already are. Therefore:

- Rust ownership transfer is a semantic transfer, not necessarily a NUMA
  migration;
- `Send + 'static` is a thread-safety/lifetime boundary, not a locality
  guarantee;
- a destination shard may receive ownership of pages first touched on another
  shard;
- page migration, range binding, allocator arenas, and first-touch
  initialization are separate mechanisms.

This fits the Sitas invariants: values crossing shard boundaries must be owned,
but ownership alone should not be interpreted as locality.

## Current Sitas Shape

Sitas already has the correct initial abstraction boundary.

`ShardedExecutorConfig` accepts CPU placement and memory placement as runtime
startup policies. CPU placement is applied first. Memory placement is then
applied to the shard thread before the executor begins running tasks.

Current memory placement policies are:

- `Default`: do not change memory policy;
- `LocalToCpu`: bind future allocations to the NUMA node observed for the
  shard's pinned CPU;
- `Bind(NumaNodeId)`: bind future allocations to one node;
- `Preferred(NumaNodeId)`: prefer one node while allowing kernel fallback;
- `Interleave(Vec<NumaNodeId>)`: spread future allocations across nodes.

The important qualifier is "future allocations". This is thread default memory
policy, not complete allocator control. It does not automatically re-home pages
that were already touched, and it does not yet expose `mbind`, page migration,
or shard-local allocator arenas.

The current model is intentionally conservative:

- placement is observable through snapshots;
- unsupported platforms report unsupported status;
- placement can be advisory or required at startup;
- memory placement remains a runtime concern rather than an application service
  concern;
- services can keep the normal shard-local ownership model.

## Effect on Shard-Local State

Shard-local state is the best fit for NUMA-aware behavior. If a shard is pinned
to CPU `C`, and memory placement is local to `C`'s NUMA node before shard-local
state is initialized, ordinary Rust allocations made by that shard can land on
the right node.

The recommended pattern is:

1. Start shard threads.
2. Apply CPU placement.
3. Apply memory placement.
4. Initialize shard-local state on the owning shard.
5. Mutate that state only from the owning shard.

This aligns both the Sitas ownership model and the hardware locality model. The
same shard that owns and mutates the state is also the shard that first touches
and later reuses it.

The risky pattern is:

1. Allocate and initialize a large data structure outside the owning shard.
2. Move the handle into a shard.
3. Treat Rust ownership as if it moved physical memory locality.

This can be correct semantically while still being poor placement physically.

## Effect on Message Passing

The mailbox impact is real but secondary. A mailbox is a transport boundary for
owned messages. It should not become responsible for NUMA policy.

For small typed commands, NUMA effects are usually dominated by existing
cross-core costs:

- cache-line movement for the message payload;
- synchronization on queue metadata;
- producer/consumer cache-line bouncing;
- wakeup and scheduling costs;
- allocator behavior for message storage.

For large owned payloads, NUMA becomes more visible. Sending a `Vec<T>` from
shard A to shard B transfers ownership of the vector handle, but the backing
pages may remain local to shard A's node. If shard B will scan or mutate that
buffer heavily, B may now be doing remote memory access.

Guidance:

- keep hot mailbox messages compact;
- prefer sending commands that cause the destination shard to allocate or fill
  long-lived destination-owned state locally;
- batch small messages when latency allows, but be aware that a large batch
  first touched on the producer may be remote to the consumer;
- treat one heavily targeted inbound mailbox as both a coherence and possible
  NUMA traffic point;
- do not hide page migration or allocation policy inside `ShardSender`.

The mailbox API should continue to express ownership transfer. NUMA placement
belongs in shard startup, shard-local initialization, and future explicit memory
placement helpers.

## Interleaving

Interleaving can make performance more predictable by spreading pages across
nodes, but it often trades peak locality for a stable average. It is useful when
a workload is NUMA-oblivious, spans many nodes anyway, or is bandwidth-bound and
can use multiple memory controllers.

For Sitas, interleaving should remain an explicit policy, not a default. The
shared-nothing model is usually better served by local allocation for
shard-local state. Interleaving can be useful for global read-mostly data or
workloads that intentionally span all shards, but those cases should be chosen
deliberately.

## Future Encapsulation

The current `MemoryPlacement` policy gives Sitas a structural hook without
forcing premature allocator design. Future work should extend the same boundary
rather than leak NUMA concerns into application service APIs.

Useful future abstractions:

- `ShardPlacement`: an owned snapshot tying `ShardId` to requested CPU, applied
  CPU, observed NUMA node, requested memory policy, and applied memory policy.
- Shard-local initialization helpers that run constructors on the owning shard
  after memory policy is applied.
- Destination-owned buffer builders, where a target shard allocates and fills
  long-lived buffers locally rather than receiving producer-touched buffers.
- Optional range-level APIs over Linux `mbind` for specific owned buffers.
- Optional page migration APIs for rare cases where a buffer must be moved
  physically after ownership transfer.
- Optional allocator integration or arenas if repeated shard-local allocation
  patterns justify it.
- Metrics or diagnostics that report placement status and, where possible,
  observed NUMA topology.

Those mechanisms should remain safe APIs over isolated OS-specific code. They
should not weaken the invariants that only the owning shard mutates application
state and that values crossing shard boundaries are owned.

## Design Position

Sitas should treat NUMA as a placement layer below the shared-nothing service
model:

- service state ownership remains the semantic invariant;
- CPU and memory placement are runtime policies;
- initialization locality matters because of first touch;
- mailbox transfer remains explicit owned-message transfer;
- large payload movement should be designed consciously;
- future page/range/allocator mechanisms should be encapsulated behind small
  runtime APIs.

The near-term goal is not full NUMA automation. The useful goal is to keep
enough structure that Sitas can later add stronger memory-locality controls
without rewriting the service model or making application code depend directly
on Linux NUMA primitives.
