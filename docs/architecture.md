# Architecture

## 1. Purpose

`sitas` is an experimental Rust runtime and service model inspired by Seastar's shard-per-core architecture. Not a Seastar clone. The project explores how the same broad ideas work in Rust:

- shard-local ownership;
- explicit cross-shard communication;
- typed service boundaries;
- owned values crossing execution boundaries;
- minimal shared mutable state;
- small safe APIs over isolated unsafe/OS-specific mechanisms;
- dependency-light runtime experimentation.

The project started with a standard-library-only sharded key-value store. That baseline remains the semantic reference. Newer branches extend it with a custom executor, Unix readiness primitives, TCP helpers, sharded async execution, shard-local values, observability snapshots, CPU placement, and Linux `io_uring` support.

## 2. Architectural invariants

These invariants define the project.

1. **Shard ownership:** each piece of application service state is owned by a shard.
2. **Local mutation:** only the owning shard may mutate its service state.
3. **No mutex-normalized service state:** application services should not be modeled as `Arc<Mutex<Service>>` or equivalent shared mutable state.
4. **Explicit transfer:** cross-shard interaction happens through typed messages, typed reply handles, or typed submitter calls.
5. **Owned boundaries:** values crossing shard boundaries are owned. They do not borrow from shard-local state.
6. **No escaping local references:** references to `ShardLocal<T>` values or other shard-owned internals must not escape synchronous access closures and must not cross `.await`.
7. **Shard affinity:** async work remains on its assigned shard unless explicitly submitted elsewhere.
8. **Owned observability:** snapshots are owned values. Observability must not expose live borrowed runtime internals.
9. **Unsafe isolation:** unsafe code is allowed only where needed for runtime internals, FFI, wakers, low-level queues, or kernel I/O. Safe APIs should carry the public model.
10. **Synchronization boundary:** runtime internals may use synchronization for task bookkeeping, wakers, reply state, cancellation, and shutdown. Application service state remains shared-nothing.

## 3. Layered model

The current architecture is best understood as layers:

```text
Application service APIs
    |
Typed command/reply and submitter APIs
    |
Placement and shard routing
    |
Shard-local service state / ShardLocal<T>
    |
Shard executor or std shard thread
    |
Mailbox, task queue, timers, readiness, wakeups
    |
OS primitives: pipe/poll/epoll/io_uring/affinity
```

The std-only baseline exercises the upper part of this model without a custom async runtime. The non-std runtime path gradually fills in executor and OS layers.

## 4. Module responsibilities

### `runtime`

`runtime` is the reusable standard-library kernel. It does not know about key-value commands or concrete service state.

Responsibilities:

- bounded shard mailboxes;
- shard set startup;
- one-shot reply handles;
- non-blocking mailbox send error mapping;
- shard thread join handling;
- shutdown support;
- small reusable building blocks for concrete sharded services.

### `kv`

`kv` is the reference service proving the shared-nothing model.

`ShardedKv` owns one `KvShardHandle` per shard and one thread join handle per shard in the std-only path. Each shard thread owns exactly one `KvService`. `KvService` owns the actual `HashMap<String, String>` and is private to the module.

The service exposes blocking methods, non-blocking enqueue variants, submit/wait-later handles, aggregate operations, snapshots, key listing, and shard-local atomic operations such as `compare_and_put` and `get_or_put`.

### `counter`

`ShardedCounter` is a small second service. It proves that the runtime primitives are not key-value-specific without forcing premature generic service abstractions.

### `sharded`

`sharded` provides a generic sharded service trait and runtime. The `ShardService` trait captures the reusable pattern: per-shard state, typed command enum, initial state factory, `process` function, and a `stop_command` constructor that builds the command which causes the shard loop to exit. `Sharded<S>` provides lifecycle management (start, shutdown, snapshot) and raw command submission. Shutdown is automatic: the generic infrastructure calls `stop_command` once per shard and joins the threads. Concrete services like `ShardedCounter` and `ShardedKv` remain available with their richer type-specific APIs; the generic layer does not replace them but provides a common infrastructure.

### `stream_reply`

`stream_reply` provides streaming reply channels. A `StreamProducer<T>` bridges between a shard producing multiple values and a consumer receiving them incrementally. Unlike one-shot `Reply<T>`, stream replies deliver a sequence of owned values followed by a terminal completion signal signalled by dropping the last `StreamSender` clone.

Two async consumption patterns are available. `StreamBatch<'a, T>` is a one-shot future borrowing a `StreamReply` mutably — call `reply.next_batch()` repeatedly in a `while let` loop. `StreamFuture<T>` owns the reply (via `reply.into_stream()`) and provides the same `next_batch()` loop plus convenience methods `collect()` and `fold()`.

Senders use an atomic refcount so intermediate clone drops do not prematurely signal end-of-stream. The stream only closes when the last sender is dropped.

### `async_service`

`async_service` bridges std-layer sharded services and the async executor. `AsyncShardedKv` wraps a `ShardedKv` reference and provides async methods that call `submit_*` followed by `reply.wait_async().await`. `AsyncShardedCounter` does the same for `ShardedCounter`. `OwnedAsyncShardedKv` manages lifecycle for owned kv stores used from async contexts.

### `backpressure`

`backpressure` provides a spawn backpressure mechanism for the async executor, living under `executor`. A `BackpressureGuard` acts as a counting semaphore with an async `acquire()` future and a non-blocking `try_acquire()`. `Spawner::with_backpressure(capacity)` integrates backpressure directly into the spawn path: every `spawn` call acquires a permit, and the permit is held in the task's future for its lifetime. `AcquirePermit` uses a generation counter to clean up wakers when the future is dropped before capacity becomes available, preventing a slow memory leak in the waiters list.

### `sharded_tcp`

`sharded_tcp` integrates TCP helpers with the shard-per-thread model. `ShardedTcpServer` validates its configuration, creates non-blocking listeners before returning from startup, and spawns stoppable readiness-driven accept-loop tasks. On Linux with `SO_REUSEPORT`, the kernel distributes connections across shards. Linux listeners can also opt into explicit `SO_INCOMING_CPU` placement, either sequentially over `available_cpu_ids()` or with a caller-provided CPU list. Socket option constants and `setsockopt` calls are isolated behind a private socket-options helper so the accept-loop code stays focused on lifecycle and routing. Without `SO_REUSEPORT`, a single accept shard distributes connections via the `ShardedSubmitter`. Connection handlers receive the TCP stream and a submitter clone for spawning work on their shard. The server returns a stop handle, configured per-shard connection limits are enforced by shard-local handler permits, and handle snapshots expose owned counters for connection-limit drops, accept errors, and handler submit failures. Accept-loop diagnostics are emitted as typed `ShardedTcpEvent` values through an optional event sink; the built-in snapshot counters are updated from the same events. Accepted connections expose a Linux kTLS ULP helper, but full kTLS handoff remains a handler/TLS-stack responsibility because the server does not own handshake state or traffic keys.

### `shard_mailbox`

`shard_mailbox` provides typed owned-message transfer between executor shards. A `ShardMailboxSet<M>` owns one bounded inbound mailbox per shard. Cloneable `ShardSender<M>` handles may enqueue owned `M: Send + 'static` values for a target shard, while a single `ShardReceiver<M>` drains that target shard's queue. `ShardSender` provides non-blocking `try_send` (returning `Full(M)` or `Closed(M)` on failure with the original owned message) and awaitable `send` (parking until capacity is available or the receiver closes). For non-uniform work assignment, `WorkUnitMailboxSet<N, M>` maps logical work-unit names to independently assigned shard-local receivers; multiple work units may run on one shard and some shards may have no work unit for a given protocol. `UniformShardRouter` keeps the original key-to-`ShardId` route explicit, while `WorkUnitRouter<N>` routes keys to logical names before placement resolves those names to shards. The mailbox is a transport boundary, not shared application state: producers transfer ownership and only receiver tasks running on the owning shard should mutate the state associated with received messages. Mailbox snapshots are owned values that expose queue length, capacity, close state, sender count, send waiter count, and send/receive/rejection counters without exposing message references or queue internals.

#### Cache behavior of cross-shard movement

Shard-local ownership improves cache behavior by keeping each service's mutable working set hot on the core that owns it. Cross-shard messaging does not eliminate hardware movement; it makes that movement explicit. When shard A constructs an owned message and sends it to shard B, the cache lines containing the message payload are likely hot on A. When B receives and reads the message, those cache lines must be fetched into B's cache hierarchy, possibly by cache-coherence transfer from A or by refetch from memory.

Mailbox queues also contain shared runtime metadata. Producer and consumer positions, readiness flags, waiter state, counters, and close state may be touched by different cores. If those fields share cache lines or are updated at high frequency, the cache line can bounce between cores even though application state itself remains shard-local. Multiple producers targeting one inbound queue can make the producer-side metadata especially hot.

The architectural rule is therefore not "messages are free". It is: application state does not suffer uncontrolled cache-line contention because only the owning shard mutates it. Mailbox transfer still pays for payload cache-line migration, synchronization on queue metadata, possible allocator effects, and memory bandwidth. Large owned payloads should usually be batched, partitioned by route, or represented by compact commands plus owned buffers when that reduces repeated cross-core traffic.

Practical guidance:

- keep high-frequency mutation in shard-local state;
- prefer compact typed commands for hot paths;
- batch small messages when latency requirements allow it;
- avoid polling observability snapshots at a rate that turns counters into cache traffic;
- keep frequently written per-shard or per-queue metadata separated enough to avoid false sharing;
- treat one heavily targeted inbound mailbox as a possible coherence bottleneck.

#### Performance comparison: mailbox transfer vs file-backed exchange

The `sharded_index_mailbox` and `sharded_index_build` examples build the same sorted secondary index over a fixed-record data file, differing only in how intermediate data crosses shard boundaries. The build example writes per-shard sorted run files and performs multi-round pairwise merge I/O. The mailbox example routes entries by key through owned mailbox messages to logical assembler work units, which sort and write pre-partitioned files before a single k-way merge.

Measured on macOS (Apple Silicon, 8 CPUs), release mode, deterministic input, default seed:

| Records | Shards | Build total | Mailbox total | Build merge | Mailbox scan+xfer |
|---------|--------|-------------|---------------|-------------|-------------------|
| 1M      | 1      | 6.1s        | 8.9s          | ~0          | 20ms              |
| 1M      | 2      | 8.6s        | 8.9s          | 2.9s        | 13ms              |
| 1M      | 4      | 10.2s       | 8.2s          | 5.3s        | 9ms               |
| 1M      | 8      | 13.3s       | 9.3s          | 6.9s        | 12ms              |
| 5M      | 2      | 47.2s       | 46.3s         | 15.0s       | 63ms              |
| 5M      | 4      | 57.4s       | 45.4s         | 27.4s       | 39ms              |
| 5M      | 8      | 69.9s       | 51.0s         | 34.9s       | 43ms              |

The build example anti-scales with shard count: more shards produce more run files, requiring more merge rounds of full-dataset file I/O. At 5M records, going from 2 to 8 shards increases build total by 48%. The mailbox example stays roughly flat because in-memory transfer replaces intermediate file exchange and the final k-way merge reads each partition file once.

Both totals are dominated by unbuffered I/O overhead: data file creation, `write_index_entry` (two 8-byte `write_all` calls per entry), and verification. The mailbox's reported work phases (scan+transfer in tens of milliseconds, assembler join in microseconds) reflect concurrent shard execution where scanning, mailbox transfer, assembler sort, and partition writes overlap on executor threads.

The build example wins at 1 shard because it has no merge phase and copies one file. At 4+ shards the mailbox approach pulls ahead because it avoids O(N log S) merge I/O. Neither example uses buffered writers, so both pay per-entry syscall costs that dwarf the architectural difference at small scale.

### `udp`

`udp` provides a non-blocking UDP socket using direct Unix FFI (following the same pattern as `os`). It supports `bind`, `recv_from`, and `send_to` for datagram communication. Located under `executor` alongside `tcp`, it is designed for readiness-based integration with the custom executor.

### `metrics`

`metrics` provides a thread-safe metrics collector using atomic counters. `RuntimeMetrics` accumulates counters for task lifecycle, I/O operations, network connections, shard commands/replies, executor polls/wakeups, and `io_uring` submissions. `MetricsSnapshot` is an owned point-in-time snapshot suitable for observability without live borrows.

### `placement`

`placement` defines routing from keys to shards. The default strategy is hash-based. Placement is explicit and replaceable, but not production-grade consistent hashing yet.

### `os`

`os` contains Unix OS primitives and FFI boundaries.

Current responsibilities:

- direct Unix FFI for pipe, read, write, fcntl, close, socket operations, readiness backends, and platform error access;
- non-blocking pipe wakeups for cross-thread reactor notification;
- Linux `epoll` readiness waiting;
- macOS/iOS `kqueue` readiness waiting;
- fallback non-Linux Unix `poll` readiness waiting;
- cloneable `OsWaker`;
- blocking `OsReactor::wait` that can be woken by the pipe;
- Linux `io_uring` operations, owned-buffer futures, and dispatcher support.

This layer is smaller than a full production reactor. It establishes the FFI boundary and wake/readiness mechanisms before deeper `io_uring` integration.

Because the project uses Rust edition 2024, C FFI declarations must use `unsafe extern "C" { ... }`.

### `executor`

`executor` is a minimal single-threaded async kernel.

Responsibilities:

- owning pinned futures in tasks;
- maintaining a ready queue;
- maintaining per-scheduling-group ready queues with weighted virtual runtime
  selection when tasks opt into explicit scheduling groups;
- coalescing repeated wakes to one ready-queue entry per task;
- budgeting ready-queue polling so self-waking tasks cannot indefinitely starve timers or readiness;
- implementing custom wakers that re-enqueue tasks;
- catching task panics at executor boundaries;
- preserving panic payloads through join handles and `block_on` where observable;
- driving a root future with `block_on` without requiring the root future to be `Send` or `'static`;
- `Executor::run_until` for polling a root future while still driving spawned tasks;
- typed join handles and `JoinError`;
- aborting pending tasks through join handles;
- cooperative stop tokens;
- `TaskScope` for grouped child tasks and bounded cooperative shutdown;
- `Notify` for cloneable one-shot async wake events;
- `yield_now` for cooperative wakeup tests;
- `race` for composing two futures and dropping the losing future;
- timer registration, timeout futures, and cancellation cleanup;
- readiness futures for read/write interests;
- Unix reactor sleep when no tasks are ready;
- an internal event-driver result so the run loops consume reactor wakeups,
  descriptor readiness, and Linux completion wakeups through one
  executor-facing path;
- Linux executor-owned `io_uring` read-at, read-exact-at, and write-at helpers
  driven from the executor loop through the same idle reactor wait used for
  timers and readiness;
- driver-event counters split readiness wakeups from Linux completion wakeups
  in owned executor snapshots, and further split readiness events by whether
  they carried readable or writable fd progress. Linux completion wake counters
  count driver wake cycles, while dispatched-completion counters count
  individual operation completions;
- Linux completion-dispatch counters report non-empty dispatch batches,
  dispatched completion count, completion budget, and completion budget
  exhaustion events;
- cumulative executor counters for spawned tasks, completed tasks, task polls,
  and ready-poll budget exhaustion events.

The executor is small and dependency-free---a semantic experiment before a production runtime.

### Scheduling groups

Scheduling groups are the first Seastar-like resource-class mechanism. A
[`Spawner`](../src/executor/spawner.rs) can create an executor-local group with
a name and relative share count, then spawn tasks into that group. Ordinary
`spawn` calls use the default group with 100 shares. A group handle created by
one executor is rejected by other executors; the default group handle is
portable because it names the built-in default queue.

Each group owns its own ready queue. When the executor chooses the next ready
task, it selects a non-empty group with the lowest weighted virtual runtime,
pops one task from that group's FIFO queue, and charges the group for
wall-clock poll time scaled by its shares. Simple: weighted scheduling without
preemption, priorities, load balancing, or a production resource controller.

Scheduling group snapshots are owned values and include group id, name, shares,
ready queue length, total charged poll count, total charged poll time, and
virtual runtime. Snapshot helper methods derive average charged poll time and a
group's share of executor poll time without exposing live scheduler internals.
Task snapshots include the scheduling group id and the owned scheduling group
name when the snapshot builder can resolve it. The `scheduling_group_demo`
example first runs all work in the default group as a baseline, then repeats the
workload with weighted groups.

`TaskScope` can spawn children into a scheduling group. The scope does not own
scheduling policy; it keeps structured child lifetime management while
delegating group ownership checks to the underlying `Spawner`.

On `ShardedExecutor`, a sharded scheduling group is one executor-local group
per shard. Creating a group on all shards does not introduce a global scheduler
or shared state; it gives matching per-shard groups the same name and shares.
Grouped fan-out spawns and grouped submitter calls place work explicitly per
shard. Sharded group handles are tied to the runtime that created them; using
one with another runtime is rejected.

### TCP helpers

The executor currently supports readiness-driven TCP helpers:

- `accept_async` for non-blocking listener acceptance;
- `connect_async` for raw non-blocking IPv4/IPv6 socket connection;
- `read_exact_async`;
- `write_all_async`;
- `copy_async`;
- timeout variants for accept, connect, read, write, and copy;
- accepted-stream handoff to spawned tasks;
- fixed-count server helpers;
- idle-timeout accept loops;
- stop-token-controlled accept loops;
- scoped stoppable server helpers that propagate shutdown to handlers;
- bounded handler shutdown with abort of uncooperative handlers.

These helpers validate that one executor thread can interleave client and server TCP work using readiness events.

### `sharded_executor`

`sharded_executor` connects the single-threaded executor to the shard-per-core direction.

Current responsibilities:

- `ShardedExecutor::start` starts one executor/reactor on each shard thread;
- `start_on_available_parallelism` starts one shard per available parallelism unit;
- `start_on_available_cpus` starts one shard per CPU in `available_cpu_ids`;
- `start_pinned_on_available_cpus` and `start_required_pinned_on_available_cpus` combine one-shard-per-available-CPU sizing with sequential CPU placement;
- `start_with_config` accepts shard count, thread-name prefix, CPU placement policy, and optional required CPU placement;
- shard threads are named predictably, for example `sitas-shard-N`;
- `spawn_on` places a future on an explicit `ShardId`;
- `spawn_named_on` gives observable task names;
- `spawn_with_handle_on` returns awaitable join handles;
- grouped single-shard spawn helpers place explicitly addressed work into
  matching executor-local scheduling groups;
- `spawn_on_all`, `spawn_named_on_all`, `map_all`, and `map_reduce_all` provide direct runtime-level fan-out and shard-tagged collection helpers;
- `create_scheduling_group_on_all` and grouped fan-out spawn helpers create
  matching executor-local scheduling groups across shards without adding
  implicit load balancing; grouped helpers are available in both named and
  unnamed forms;
- `current_executor_shard` exposes the current shard identity from code running on a shard;
- `current_executor_cpu_placement` exposes that shard thread's observed CPU placement status from code running on a shard;
- `available_cpu_ids` reports the CPU ids used by sequential placement;
- `snapshot` returns owned per-shard executor snapshots;
- `observer` creates a weak monitoring handle;
- `submitter` creates cloneable cross-shard submission capability;
- runtime shutdown drops owned spawners and joins executor threads.

CPU placement is an explicit runtime request. Linux applies hard affinity with `sched_setaffinity` and observes container cpuset restrictions through `sched_getaffinity`. Explicit CPU lists are preflighted against `available_cpu_ids` before shard threads start. Non-Linux platforms report unsupported. By default, placement failures are recorded in shard snapshots and startup succeeds. `require_cpu_placement` turns that into fail-fast behavior.

Not yet load balancing or scheduling classes. It establishes the shared-nothing async shape: work is owned by a shard thread and moves only through explicit submission.

The `sharded_index_build` example demonstrates this shape on a fixed-record file: each shard scans and sorts one data-file partition into a materialized local index run file, then merge rounds submitted back onto shards stream those run files into new materialized runs before the final sorted offset index is written. Its command-line options can vary record count, shard count, seed, and cleanup behavior, while progress output reports per-shard phase, records, task count, and file bytes read/written.

### `ShardedSubmitter`

`ShardedSubmitter` is the first explicit cross-shard async mechanism.

A task on one shard can submit work to another shard and await the returned join handle. The remote future is polled by the target shard executor. The awaiting task resumes on its original shard after the remote work completes.

Submitters own spawner clones. They are lifetime capabilities: they must be dropped before the runtime can fully drain.

Supported higher-level forms:

- submit to one shard and await a handle;
- named submit to one shard;
- submit into a sharded scheduling group on one shard, with named and unnamed
  forms;
- submit to all shards;
- named submit to all shards;
- submit into a sharded scheduling group on all shards, with named and unnamed
  forms;
- `join_all_shards` returning shard-tagged outputs;
- `join_all_shards_timeout` for bounded joins that abort still-owned shard work on timeout or join failure;
- `map_all` and `map_named_all`;
- `map_reduce_all`.

### `shard_local`

`ShardLocal<T>` adds one owned value per executor shard.

Access is routed through the sharded submitter. The closure runs synchronously on the owning shard and receives `&mut T`. The implementation uses an internal `UnsafeCell` plus runtime shard-owner checks rather than a mutex. References to the local value cannot escape the closure or cross `.await`.

Capabilities:

- map over all shard-local values;
- reduce over all shard-local values;
- clone handles without cloning the underlying state;
- direct `with_current` access for code already running on the owning shard;
- spawn one worker per shard;
- spawn named workers visible in snapshots;
- spawn stoppable workers using shared cooperative stop tokens;
- bounded shutdown of stoppable workers with abort of uncooperative children;
- collect or reduce shard-tagged worker outputs.

`ShardLocal<T>` is the strongest current expression of the Rust-native Seastar idea: local mutable state, no service mutex, explicit routing, and no escaping references.

## 5. Service request flow

### Blocking key-value calls

For blocking `put`, `get`, and `delete`:

1. `ShardedKv` routes the key through the placement strategy.
2. The selected `KvShardHandle` creates a one-shot reply handle.
3. The handle sends a typed `KvCommand` to the shard mailbox.
4. The caller blocks waiting for the reply.
5. The shard thread reads or mutates its local `KvService`.
6. The shard replies with an owned value.

No references into shard-local state cross the mailbox boundary.

### Submitted calls

Submitted commands use the same routing and enqueue path, but return a reply handle immediately. The caller can later use:

- `wait`;
- `try_wait`;
- `wait_timeout`;
- `wait_async` on the custom executor.

### Async shard executor calls

In the sharded executor path, work is submitted to a target shard as a future. That future is polled only by the target shard executor. Awaiters resume on their original shard after the join result is available.

## 6. Backpressure and reply handles

Shard command mailboxes are bounded `std::sync::mpsc::sync_channel` queues. The default capacity is exposed as `DEFAULT_MAILBOX_CAPACITY`, and callers can configure capacity through startup configuration.

Blocking methods wait for mailbox capacity. `try_*` methods attempt to enqueue without waiting and return `ShardError::MailboxFull` if saturated.

Once a command is accepted, both blocking and `try_*` calls wait for the shard reply unless the caller uses a submit form.

The custom reply primitive is std-only and waker-aware. It lets blocking code wait synchronously and lets executor tasks await replies through `wait_async` without using Tokio or `std::sync::mpsc` as the async waiting mechanism.

## 7. Snapshots and observability

Observability is snapshot-based rather than tracing-based for now.

Service snapshots:

- `shard_snapshots` returns owned `ShardSnapshot` values containing `ShardId` and current key counts;
- `keys_on_shard` and `all_keys` return owned, sorted key vectors;
- `get_many` and `delete_many` preserve input order and return owned key/value results;
- `ShardedCounter` returns owned per-shard snapshots and aggregate totals.

Executor snapshots expose:

- ready queue depth;
- task count;
- timer count;
- I/O interest counts;
- Linux executor-owned `io_uring` lifecycle status, distinguishing not-started,
  unavailable, installed, and shutdown states;
- ready-task and Linux completion-dispatch budgets;
- Linux completion-dispatch batch, completion count, and budget exhaustion
  counters;
- Linux `io_uring` dispatcher snapshots when installed, including pending
  submissions, tracked operations, buffered completions, registered wakers,
  abandoned buffers, cumulative operation-kind counters, and final executor
  shutdown drain outcome when teardown has recorded one;
- shard thread names;
- CPU placement status;
- named task states.

`TaskSnapshot` exposes:

- task id;
- optional task name;
- lifecycle state;
- last known wait interest;
- poll count;
- accumulated poll time;
- key timestamps;
- helper methods for deriving age, time since last scheduling/poll activity,
  and current coarse-state duration from a caller-supplied `Instant`.

Snapshots support simple progress views without a logging framework, Tokio console, or third-party observability dependency.

## 8. Shutdown model

### Std sharded services

`ShardedKv::stop` consumes the store handle, sends `Stop` to each shard, and joins all shard threads.

`shutdown(&mut self)` performs the same shutdown while retaining the handle, which lets callers inspect stopped runtime snapshots.

`Drop` performs best-effort shutdown if the caller forgets explicit shutdown. Explicit shutdown is preferred because it can return errors.

### Executor tasks

Executor shutdown tracks spawned tasks, clears timer/readiness registrations, and drops pending task futures. Dropping futures is the normal cancellation path, so timer and readiness cleanup must be implemented through `Drop` where necessary. The scheduler keeps every spawned task in a weak-reference list for observability; it opportunistically compacts dead entries from that list so a long-running executor that churns short-lived tasks keeps the list bounded by the live task count rather than growing without bound until teardown.

`TaskScope` provides cooperative shutdown for grouped children. If bounded shutdown times out, still-owned children may be aborted.

### Sharded executor

Stopping the sharded executor requires dropping owned spawners/submitters and joining shard executor threads. Submitters are explicit lifetime capabilities and can keep the runtime alive if retained.

`shutdown`/`stop` drain cooperatively and will wait indefinitely for a task that never completes. `shutdown_timeout(duration)` and its consuming variant `stop_timeout(duration)` bound this: they drop the owned spawners, wait up to `duration` for shards to drain, then forcibly stop any shard still running by signaling its executor run loop to exit (via a scheduler stop flag plus a reactor wake) and dropping its remaining task futures. The returned `ShardedShutdownOutcome` reports which shards, if any, were forced, so an uncooperative task cannot hang teardown.

## 9. I/O readiness model

The readiness path is small:

- file descriptors are set non-blocking where required;
- operations attempt normal read/write/accept/connect work first;
- `WouldBlock` registers interest and yields to the executor;
- the Unix reactor sleeps using `epoll`, `kqueue`, or fallback `poll` plus a
  wake pipe;
- the `epoll` and `kqueue` backends keep a persistent kernel registration set
  and reconcile it against the executor's current fd interests before each
  wait, so unchanged interests do not churn through add/delete syscalls;
- readiness wakes the interested task;
- the operation retries.

This is readiness-based, not completion-based. It is separate from the `io_uring` completion path.

Linux uses `epoll`. macOS and iOS use `kqueue`. Other non-Linux Unix targets
currently use `poll`.

The TCP server's `SO_INCOMING_CPU` and kTLS ULP hooks are Linux socket options
on the readiness-based TCP path. They do not make TCP accept, send, or receive
completion-based `io_uring` operations.

## 10. Linux `io_uring` model

The Linux `io_uring` backend is a supported part of the runtime. It has a
narrow executor integration for owned-buffer `read_at`, `read_exact_at`, and
`write_at` style file I/O. This is enough for shard tasks to await
completion-based file operations without using the separate `block_on_io_uring`
bridge. It is not yet the single unified I/O engine for all sharded executor
work; readiness, timers, and `io_uring` remain distinct wait sources.

Each Linux executor run loop installs a thread-local `IoUringDispatcher` when
the host allows `io_uring_setup`. Executor-backed `io_uring` futures store only
operation ids and owned buffers, not `Rc` dispatcher handles, so they can remain
`Send` before being moved onto a shard thread. When polled on the shard, they
queue operations against the thread-local dispatcher and register their task
waker. The executor dispatches locally available completions after ready-task
polling with a fixed per-tick completion budget and gives pending `io_uring`
work the executor wait slot when the shard has no ready tasks. Executor teardown
makes a bounded attempt to drain abandoned operations before discarding the
thread-local dispatcher when no live task wakers remain registered. This avoids
hiding completion work behind an unrelated timer or readiness wait, but it is
still a staged integration rather than one combined production wait primitive.
If `run_until` returns while spawned tasks still have registered `io_uring`
wakers, the dispatcher remains installed for that executor on the current
thread so a later `run` or `run_until` can continue driving those operations.
A different executor may not take over that thread-local dispatcher while it
still has live state.

This integration keeps some limits visible:

- it is Linux-only and reports normal unsupported behavior when `io_uring` is
  unavailable;
- executor-owned `io_uring` setup fails fast when strict availability is
  requested with `SITAS_REQUIRE_IO_URING=1`;
- the dispatcher is per executor thread, not shared across shards;
- completion dispatch and shutdown draining use fixed internal limits rather
  than caller-tunable policy;
- timers, readiness waits, and `io_uring` waits now share the Linux executor
  idle wait shape, but are not yet exposed as one portable production event
  source with deadline-aware completion policy;
- final verification in the index example still uses std file I/O.

It has two layers of completion state:

- `IoUring` owns the raw ring and local tracked-operation table;
- `IoUringDispatcher` owns async-facing state: wakers, buffered completions, abandoned operations, deferred owned buffers, and cumulative counters.

### Normal tracked completion path

```
queue_*_operation()
  |
  | records IoUringOperationId -> IoUringOperationKind
  | increments tracked operations
  v
pending SQE
  |
  | submit_pending() or wait_completions()
  v
kernel owns operation
  |
  | CQE appears
  v
local completion queue
  |
  | try_operation_completion()
  | removes operation from tracked table
  v
IoUringDispatcher::dispatch_available()
  |
  | buffers completion and wakes registered task if any
  v
future polls again
  |
  | take_completion()
  v
completion consumed by future
```

### Abandoned operation path

When a future is dropped before its operation completes:

```
future Drop
  |
  | clear_waker(operation)
  v
abandon_operation(operation)
  |
  | record operation as abandoned
  | queue async cancel when possible
  | keep owned buffers deferred
  v
kernel completes original and/or cancel operation
  |
  v
dispatch_available()
  |
  | discard abandoned completion
  | drop deferred buffer only after completion is observed
```

The dispatcher keeps abandoned owned read/write buffers alive until the matching kernel completion is dispatched. This is the safety boundary for owned-buffer futures: dropping the future must not free memory that the kernel may still read from or write to.

Dropping the dispatcher itself preserves the same boundary. A single `IoUringDispatcher::shutdown_drain` routine implements the teardown policy and is shared by explicit shutdown, the executor integration, and `Drop`. It first makes a bounded attempt to drive remaining operations to completion so their buffers are freed normally. If the bounded attempt cannot confirm completion, the deferred buffers backing operations the kernel may still touch are leaked rather than freed, because the ring descriptor and mmaps are torn down immediately afterward. The rule is drain-or-leak, never free while an operation is in flight: leaking an allocation is recoverable, a kernel write to freed memory is not. Confirmed-idle teardown frees everything normally and leaks nothing. When live task wakers are still registered the drain is skipped entirely, because those buffers belong to live futures that release them on drop. The outcome, including a `leaked_buffers` count, is recorded and exposed through the dispatcher snapshot, so a leak is observable rather than silent; `Drop` additionally logs a warning in debug builds when it has to leak.

Shared SQ/CQ ring indices are accessed as atomics: the submitter releases the SQ tail and the consumer acquires the CQ tail, pairing with the kernel's matching acquire/release so SQE payloads and posted CQEs are observed in order on weakly-ordered targets, not only on x86.

### Snapshot semantics

Raw ring snapshot fields represent live ring state:

- pending submissions;
- pending completions;
- tracked operations;
- operation kinds.

Dispatcher snapshot fields represent async-facing live and historical state:

- registered wakers;
- completed operations buffered for futures;
- abandoned operations;
- deferred buffers;
- optional executor shutdown drain outcome, including status, wait budget,
  completions dispatched during the drain, and the number of owned buffers
  leaked (non-zero only when the drain timed out with operations still in
  flight);
- total dispatched operations;
- total buffered operations;
- total woken operations;
- total discarded operations;
- operation-kind grouped counters.

`is_idle()` for the raw ring means no pending submissions, buffered completions, or tracked operations. `is_idle()` for the dispatcher additionally requires no registered wakers, buffered completions, abandoned operations, or deferred buffers. Cumulative totals do not affect idleness.

## 11. Current non-goals

The current architecture does not yet aim to provide:

- persistence;
- distributed clustering;
- procedural macro service generation;
- a single unified `io_uring`/timer/readiness event source for the
  sharded executor (the per-shard `io_uring` integration is supported, but one
  combined production wait primitive is still future work);
- generic load balancing;
- production-grade Seastar-like scheduling/resource classes;
- a stable public runtime API;
- replacing the custom runtime path with Tokio, Glommio, Monoio, or another external runtime.

CPU placement exists as an experimental Linux-supported runtime request. Portable production CPU placement and richer scheduling policy remain future work.

Removed from non-goals (now implemented):
- per-shard Linux `io_uring` integration (owned-buffer read/write futures, dispatcher lifecycle tracking, atomic SQ/CQ ordering, and drain-or-leak teardown safety; a single unified event source remains future work);
- broader BSD `kqueue` support beyond macOS/iOS (NetBSD/FreeBSD/OpenBSD `kqueue` now supported);
- generic `Sharded<T>` abstraction (evaluated and provided as opt-in generic infrastructure);
- streaming/chunked responses (stream reply channels available);
- async spawn backpressure (backpressure guard available);
- network-facing sharded TCP service (sharded TCP server available);
- UDP support (UDP socket module available);
- runtime metrics collection (metrics module with atomic counters available).

## 12. Roadmap

### Near term

1. Keep the std-only sharded service baseline correct and well tested.
2. Keep the custom executor small and observable.
3. Strengthen cancellation/drop tests for timers, readiness, reply handles, and task scopes.
4. Continue separating stable architecture from experimental OS/runtime details.
5. Keep snapshot fields documented when they change.

### Medium term

1. Improve `ShardLocal<T>` ergonomics while preserving non-escaping reference rules.
2. Harden `ShardedSubmitter` lifecycle and shutdown semantics.
3. Expand runtime examples that demonstrate shard-local services running on `ShardedExecutor`.
4. Clarify the boundary between blocking std services and executor-backed services.
5. Add better platform-gated validation for Linux-only features.

### Recently completed

- Generic `Sharded<T>` trait and `Sharded<S>` runtime infrastructure (opt-in, not replacing concrete services).
- Streaming reply channels (`StreamReply<T>`, `StreamSender<T>`, `StreamProducer<T>`).
- Async-std bridge (`AsyncShardedKv`, `OwnedAsyncShardedKv`, `AsyncShardedCounter`).
- Spawn backpressure mechanism (`BackpressureGuard`, `Permit`, `BackpressureTask`).
- Network-facing sharded TCP server (`ShardedTcpServer` with `SO_REUSEPORT` on Linux).
- UDP socket module with direct Unix FFI (`UdpSocket`).
- Runtime metrics collection (`RuntimeMetrics`, `MetricsSnapshot`).
- Extended `io_uring` opcode coverage (accept, connect, send, recv stubs).
- BSD `kqueue` support for NetBSD/FreeBSD/OpenBSD.

### Longer term

1. Explore procedural macros for generating command enums, client stubs, routing, and reply plumbing from service traits.
2. Unify executor `io_uring` waits with timers and readiness into a single
   deadline-aware wait path instead of the current priority-based integration.
3. Extend scheduling/resource classes beyond the current minimal weighted
   cooperative groups only after the executor and service semantics are stable.
4. Explore network-facing sharded services with explicit key routing.

## 13. Design stance

The project should remain boring at the semantic core and experimental at the runtime edge.

The preferred direction is:

```
correct shared-nothing ownership
    -> typed service boundaries
    -> observable lifecycle
    -> explicit cross-shard submission
    -> shard-local async services
    -> low-level I/O backends
    -> scheduling and performance policy
```

Avoid clever abstractions that obscure ownership, shutdown, cancellation, or shard affinity. The purpose is not merely to build a runtime; it is to discover a Rust-native formulation of the Seastar idea.
