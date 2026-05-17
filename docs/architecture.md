# Architecture

## 1. Purpose

`sitas` is an experimental Rust runtime and service model inspired by Seastar's shard-per-core architecture. It is not a direct Seastar clone. The project explores how the same broad ideas can be expressed in a Rust-native way:

- shard-local ownership;
- explicit cross-shard communication;
- typed service boundaries;
- owned values crossing execution boundaries;
- minimal shared mutable state;
- small safe APIs over isolated unsafe/OS-specific mechanisms;
- dependency-light runtime experimentation.

The project started with a standard-library-only sharded key-value store. That baseline remains the semantic reference point. Newer branches extend it with a custom executor, Unix readiness primitives, TCP helpers, sharded async execution, shard-local values, observability snapshots, CPU placement, and experimental Linux `io_uring` support.

## 2. Architectural invariants

These invariants define the project more than any particular implementation detail.

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

`runtime` is the reusable standard-library kernel. It deliberately does not know about key-value commands or concrete service state.

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

`ShardedCounter` is the deliberately small second service. It proves that the runtime primitives are not key-value-specific without forcing premature generic service abstractions.

### `placement`

`placement` defines routing from keys to shards. The default strategy is hash-based. The goal is to make placement explicit and replaceable, not to provide production-grade consistent hashing yet.

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
- experimental Linux `io_uring` operations and dispatcher support.

This layer is intentionally smaller than a full production reactor. It establishes the FFI boundary and wake/readiness mechanisms before deeper production `io_uring` integration.

Because the project uses Rust edition 2024, C FFI declarations must use `unsafe extern "C" { ... }`.

### `executor`

`executor` is a minimal single-threaded async kernel.

Responsibilities:

- owning pinned futures in tasks;
- maintaining a ready queue;
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
- an internal event-driver result so the run loops consume readiness wakeups
  and Linux completion wakeups through one executor-facing path;
- Linux executor-owned `io_uring` read-at, read-exact-at, and write-at helpers
  driven from the executor loop when a shard has no ready tasks;
- cumulative executor counters for spawned tasks, completed tasks, task polls,
  and ready-poll budget exhaustion events.

The executor is intentionally small and dependency-free. It is a semantic experiment before a production runtime.

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
- `spawn_on_all`, `spawn_named_on_all`, `map_all`, and `map_reduce_all` provide direct runtime-level fan-out and shard-tagged collection helpers;
- `current_executor_shard` exposes the current shard identity from code running on a shard;
- `current_executor_cpu_placement` exposes that shard thread's observed CPU placement status from code running on a shard;
- `available_cpu_ids` reports the CPU ids used by sequential placement;
- `snapshot` returns owned per-shard executor snapshots;
- `observer` creates a weak monitoring handle;
- `submitter` creates cloneable cross-shard submission capability;
- runtime shutdown drops owned spawners and joins executor threads.

CPU placement is an explicit runtime request. Linux applies hard affinity with `sched_setaffinity` where supported and observes container cpuset restrictions through `sched_getaffinity`. Explicit CPU lists are preflighted against `available_cpu_ids` before shard threads are started. Non-Linux platforms report unsupported placement honestly rather than pretending to pin. By default, placement failures are recorded in shard snapshots and startup still succeeds. `require_cpu_placement` turns that into fail-fast startup behavior for deployments that depend on hard affinity.

This layer is not yet load balancing or scheduling classes. It establishes the shared-nothing async shape: work is owned by a shard thread and moves only through explicit submission.

The `sharded_index_build` example demonstrates this shape on a fixed-record file: each shard scans and sorts one data-file partition into a materialized local index run file, then merge rounds submitted back onto shards stream those run files into new materialized runs before the final sorted offset index is written. Its command-line options can vary record count, shard count, seed, and cleanup behavior, while progress output reports per-shard phase, records, task count, and file bytes read/written.

### `ShardedSubmitter`

`ShardedSubmitter` is the first explicit cross-shard async mechanism.

A task on one shard can submit work to another shard and await the returned join handle. The remote future is polled by the target shard executor. The awaiting task resumes on its original shard after the remote work completes.

Submitters own spawner clones. They are therefore lifetime capabilities: they must be dropped before the runtime can fully drain.

Supported higher-level forms:

- submit to one shard and await a handle;
- named submit to one shard;
- submit to all shards;
- named submit to all shards;
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
- Linux `io_uring` dispatcher snapshots when installed, including pending
  submissions, tracked operations, buffered completions, registered wakers,
  abandoned buffers, and cumulative operation-kind counters;
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
- key timestamps.

Snapshots are designed to support simple progress views without adding a logging framework, Tokio console, or third-party observability dependency.

## 8. Shutdown model

### Std sharded services

`ShardedKv::stop` consumes the store handle, sends `Stop` to each shard, and joins all shard threads.

`shutdown(&mut self)` performs the same shutdown while retaining the handle, which lets callers inspect stopped runtime snapshots.

`Drop` performs best-effort shutdown if the caller forgets explicit shutdown. Explicit shutdown is preferred because it can return errors.

### Executor tasks

Executor shutdown tracks spawned tasks, clears timer/readiness registrations, and drops pending task futures. Dropping futures is the normal cancellation path, so timer and readiness cleanup must be implemented through `Drop` where necessary.

`TaskScope` provides cooperative shutdown for grouped children. If bounded shutdown times out, still-owned children may be aborted.

### Sharded executor

Stopping the sharded executor requires dropping owned spawners/submitters and joining shard executor threads. Submitters are explicit lifetime capabilities and can keep the runtime alive if retained.

## 9. I/O readiness model

The readiness path is intentionally small:

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

This is readiness-based, not completion-based. It is separate from the experimental `io_uring` path.

Linux uses `epoll`. macOS and iOS use `kqueue`. Other non-Linux Unix targets
currently use `poll`.

## 10. Linux `io_uring` model

The Linux `io_uring` backend is experimental. It now has a narrow executor
integration for owned-buffer `read_at`, `read_exact_at`, and `write_at` style
file I/O. This is enough for shard tasks to await completion-based file
operations without using the separate `block_on_io_uring` bridge. It is still
not the production I/O engine for all sharded executor work.

Each Linux executor run loop installs a thread-local `IoUringDispatcher` when
the host allows `io_uring_setup`. Executor-backed `io_uring` futures store only
operation ids and owned buffers, not `Rc` dispatcher handles, so they can remain
`Send` before being moved onto a shard thread. When polled on the shard, they
queue operations against the thread-local dispatcher and register their task
waker. The executor dispatches locally available completions after ready-task
polling and gives pending `io_uring` work the executor wait slot when the shard
has no ready tasks. This avoids hiding completion work behind an unrelated
timer or readiness wait, but it is still a staged integration rather than one
combined production wait primitive.

This integration deliberately keeps some limits visible:

- it is Linux-only and reports normal unsupported behavior when `io_uring` is
  unavailable;
- the dispatcher is per executor thread, not shared across shards;
- timers, readiness waits, and `io_uring` waits are not yet unified into one
  production event source with deadline-aware completion waiting;
- final verification in the index example still uses std file I/O.

It has two layers of completion state:

- `IoUring` owns the raw ring and local tracked-operation table;
- `IoUringDispatcher` owns async-facing state: wakers, buffered completions, abandoned operations, deferred owned buffers, and cumulative counters.

### Normal tracked completion path

```text
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

```text
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
- production-grade unified `io_uring`/timer/readiness integration for the
  sharded executor;
- broader BSD `kqueue` support beyond macOS/iOS;
- generic load balancing;
- Seastar-like scheduling/resource classes;
- a stable public runtime API;
- replacing the custom runtime path with Tokio, Glommio, Monoio, or another external runtime.

CPU placement exists as an experimental Linux-supported runtime request. Portable production CPU placement and richer scheduling policy remain future work.

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

### Longer term

1. Evaluate whether a generic `Sharded<T>` abstraction is useful or whether typed service generation is better.
2. Explore procedural macros for generating command enums, client stubs, routing, and reply plumbing from service traits.
3. Unify executor `io_uring` waits with timers and readiness into a single
   deadline-aware wait path instead of the current priority-based integration.
4. Generalize the `kqueue` backend beyond macOS/iOS where the platform ABI is
   known.
5. Add scheduling/resource classes only after the executor and service semantics are stable.
6. Explore network-facing sharded services with explicit key routing.

## 13. Design stance

The project should remain boring at the semantic core and experimental at the runtime edge.

The preferred direction is:

```text
correct shared-nothing ownership
    -> typed service boundaries
    -> observable lifecycle
    -> explicit cross-shard submission
    -> shard-local async services
    -> low-level I/O backends
    -> scheduling and performance policy
```

Avoid clever abstractions that obscure ownership, shutdown, cancellation, or shard affinity. The purpose is not merely to build a runtime; it is to discover a Rust-native formulation of the Seastar idea.
