# Architecture

The std-only baseline validates the ownership model and starts a minimal
executor experiment. The `non-std-runtime` branch then begins adding small Unix
runtime backend pieces before moving into async I/O, CPU affinity, and
networking.

## Components

`runtime` provides the small reusable standard-library kernel:

- bounded shard mailboxes
- shard set startup
- one-shot reply handles
- non-blocking mailbox send error mapping
- shard thread join handling

It deliberately does not know about key-value commands or service state.

`os` provides the first non-std runtime backend primitive:

- direct Unix FFI, currently `pipe`, `read`, `write`, `fcntl`, `close`, Linux
  `epoll`, and non-Linux Unix `poll`
- a non-blocking pipe for cross-thread reactor wakeups
- read/write-readiness waiting for caller-provided file descriptors
- a cloneable `OsWaker`
- a blocking `OsReactor::wait` that can be woken by the pipe
- an experimental Linux `io_uring` backend for tracked no-op, timeout, cancel,
  read, and write operations
- an `io_uring` dispatcher that bridges tracked kernel completions to async
  task wakers and exposes snapshot-based observability

This is intentionally smaller than a full reactor. It establishes the FFI
boundary, a portable macOS/Linux wake mechanism, and the first
platform-specific readiness backend before introducing persistent file
descriptor registration, kernel timers, or deeper backends such as `kqueue`.

`executor` provides a minimal single-threaded async kernel:

- tasks own pinned futures
- a ready queue stores runnable tasks
- ready-queue polling is budgeted so one self-waking task cannot indefinitely
  starve timers and reactor readiness
- repeated wakes coalesce to one ready-queue entry per task
- custom wakers re-enqueue tasks
- task panics are caught at the executor boundary so unrelated tasks can keep
  running
- `block_on` and join handles preserve task panic payloads when observed
- `block_on` drives one root future to completion without requiring that root
  future to be `Send` or `'static`
- `Executor::run_until` polls a root future directly while still driving
  spawned executor tasks
- join handles let tasks await typed outputs from spawned tasks and observe
  task panics as `JoinError`
- pending tasks can be aborted through their join handles, which drops their
  futures and lets timer/readiness cleanup run through normal future `Drop`
- `TaskScope` groups child tasks under one stop token, supports cooperative
  shutdown by requesting stop and joining children, and aborts still-owned
  children when the scope is dropped or when bounded shutdown times out
- `Notify` provides a cloneable one-shot async event for waking one or more
  tasks when something happens without treating that event as cooperative
  shutdown
- executor shutdown tracks spawned tasks, clears timer/readiness registrations,
  and drops pending task futures
- `yield_now` proves cooperative wakeups without third-party runtimes
- `race` composes two futures and completes with the first result, dropping the
  losing future through ordinary cancellation cleanup
- stop tokens provide a small cooperative shutdown primitive for async
  operations
- on Unix, the executor sleeps on `OsReactor` when no tasks are ready
- timer futures register task wakers in the scheduler and drive reactor timeouts
- sleeping futures unregister their timers when dropped, which keeps cancelled
  composed futures from leaving stale timer entries behind
- `timeout` composes a future with a timer and returns `TimeoutError` if the
  deadline completes first
- read/write-readiness futures register file descriptors and resume when the
  reactor reports them ready
- `accept_async` retries `TcpListener::accept`, awaits listener readability
  when non-blocking listeners report `WouldBlock`, and returns non-blocking
  accepted streams for the async read/write helpers
- `connect_async` starts TCP connections with raw non-blocking IPv4 or IPv6
  sockets, awaits writability, and checks the stream for connection errors
- client and server TCP futures can run on the same executor thread, using
  readiness events to interleave connect, accept, read, and write operations
- `read_exact_async` retries normal `Read` operations and awaits readability
  when non-blocking descriptors report `WouldBlock`
- `write_all_async` retries normal `Write` operations and awaits writability
  when non-blocking descriptors report `WouldBlock`
- `copy_async` composes the read and write helpers to copy until EOF while
  using both readiness paths
- timeout variants for accept, connect, read, write, and copy compose the same
  I/O futures with executor timers and report `io::ErrorKind::TimedOut`
- accepted TCP streams can be handed to spawned tasks, allowing one executor
  thread to interleave multiple connection handlers
- `serve_tcp_n` owns a listener, accepts a bounded number of connections,
  spawns one handler per stream, and awaits the handler join results
- `serve_tcp_n_timeout` adds a bounded handler-join deadline after the fixed
  accept count has been reached
- `serve_tcp_until_idle` runs the same handler-spawning accept loop until the
  listener stays idle past a caller-provided timeout
- `serve_tcp_until_idle_timeout` adds a bounded handler-join deadline after
  the idle accept loop stops
- `serve_tcp_until_stopped` races listener accepts against a stop token for
  explicit accept-loop shutdown
- `serve_tcp_until_stopped_timeout` adds a bounded handler-join deadline for
  the non-scoped stoppable server path, aborting uncooperative handlers if the
  deadline elapses after the accept loop stops
- `serve_tcp_until_stopped_scoped` uses a task scope for accepted connection
  handlers, passing each handler a stop token so shutdown propagates beyond the
  listener accept loop; the first handler error wakes and stops the accept loop,
  triggers scoped handler shutdown, and is returned to the caller
- `serve_tcp_until_stopped_scoped_timeout` adds a bounded handler shutdown
  deadline and aborts uncooperative handlers if the deadline elapses

`sharded_executor` connects the single-threaded executor to the shard-per-core
direction:

- `ShardedExecutor::start` starts one executor/reactor on each shard thread
- `ShardedExecutor::start_with_config` accepts a `ShardedExecutorConfig`,
  currently covering shard count and thread-name prefix
- shard executor threads are named `sitas-shard-N`, matching their `ShardId`
  and giving OS/debugging tools a stable per-shard label
- `spawn_on` places a future on an explicit `ShardId`
- `spawn_named_on` places a future with a human-readable name for snapshots
- `spawn_with_handle_on` places a future and returns an awaitable join handle
- `current_executor_shard` lets code running on a shard thread observe its
  current shard identity
- `snapshot` returns owned per-shard executor snapshots with ready queue depth,
  task count, timer count, I/O interest counts, shard thread names, and named
  task states
- `observer` returns a weak monitoring handle that can snapshot the runtime
  without keeping shard threads alive
- `submitter` returns a cloneable cross-shard submission handle so shard-local
  tasks can submit work to another shard and await the result
- stopping the runtime drops the owned spawners and joins the executor threads

This is not CPU pinning yet, and it does not implement load balancing or
scheduling classes. It establishes the first shared-nothing async shape: work is
owned by a shard thread and remains on that shard unless callers explicitly
submit different work to a different shard.

`ShardedSubmitter` is the first explicit cross-shard async mechanism. A task on
one shard can call `submit_with_handle_to` for another `ShardId`, then await the
returned join handle. The remote future is polled by the target shard executor,
and the awaiting task resumes on its original shard when the remote work
completes. Submitters own spawner clones, so they are also explicit lifetime
capabilities: they must be dropped before the runtime can fully drain.

The submitter also supports broadcast-style submission with
`submit_with_handle_to_all` and `submit_with_handle_named_to_all`. These submit
one future per shard and return `ShardedJoinHandle` values that preserve the
target `ShardId`. `join_all_shards` awaits those handles in order and returns
shard-tagged outputs, giving the runtime a small dependency-free equivalent of
"run this on every shard and collect the replies."

For callers that do not need direct handle ownership, `map_all`,
`map_named_all`, and `map_reduce_all` provide the next layer up. They still run
the mapped futures on the target shard executors, but they hide the
submit-and-join plumbing and return either shard-tagged outputs or one reduced
value.

`shard_local` adds one owned value per executor shard. Access is routed through
the sharded submitter and the closure runs synchronously on the owning shard,
receiving `&mut T`. The implementation uses an internal `UnsafeCell` with a
runtime shard check rather than a mutex; references to the local value cannot
escape the closure or cross an `.await`. `ShardLocal::map_all` and
`ShardLocal::map_reduce_all` provide the same collect-or-reduce convenience for
stateful shard-local operations. Cloning a `ShardLocal<T>` creates another
handle to the same shard-owned values, so a service handle can be moved into
long-running shard tasks without cloning the state itself. Code that is already
running on a shard executor can call `ShardLocal::with_current` to access its
own local value directly, avoiding a submit-and-reschedule round trip while
keeping the runtime owner check. `ShardLocal::spawn_workers` and
`ShardLocal::spawn_named_workers` start one async worker per shard and pass each
worker a cloned handle to the same shard-owned state, which gives long-running
shard services a small shared-nothing startup pattern. They return
`ShardLocalWorkers`, a small join set that can either collect shard-tagged
worker outputs or reduce them into one value. `ShardLocal::spawn_stoppable_workers`
and `ShardLocal::spawn_named_stoppable_workers` add one shared cooperative stop
token across those per-shard workers and return `StoppableShardLocalWorkers`,
which can request stop before joining or reducing outputs. Timeout variants
bound cooperative shutdown and abort still-owned workers if the deadline
elapses. Named shard-local workers use the existing executor task names, so they
appear in shard snapshots with status, wait reason, and poll counters like other
long-running tasks.

Executor observability is deliberately snapshot-based instead of tracing-based
for now. `TaskSnapshot` exposes each observable task's id, optional name,
lifecycle state, last known wait interest, poll count, accumulated poll time,
and key timestamps. This is enough to build simple long-running task progress
views without adding a logging framework, Tokio console, or third-party
dependencies.

## io_uring Completion Lifecycle

The Linux `io_uring` backend has two layers of completion state. `IoUring`
owns the raw ring and local tracked-operation table. `IoUringDispatcher` owns
the async-facing state: task wakers, buffered tracked completions, abandoned
operations, deferred owned buffers, and cumulative counters.

The normal tracked completion path is:

```text
queue_*_operation()
  |
  | records IoUringOperationId -> IoUringOperationKind
  | increments IoUringSnapshot.ring.tracked_operations
  v
pending SQE
  |
  | submit_pending() or wait_completions()
  | decrements IoUringSnapshot.ring.pending_submissions
  v
kernel owns operation
  |
  | CQE appears
  v
IoUring local completion queue
  |
  | drain_completions() / wait_completions()
  | reflected by IoUringSnapshot.pending_completions
  v
IoUring::try_operation_completion()
  |
  | removes the operation from IoUring's tracked table
  | returns IoUringOperationCompletion { operation, kind, result, flags }
  v
IoUringDispatcher::dispatch_available()
  |
  | no abandon record exists
  | increments total_dispatched_* and total_buffered_*
  v
dispatcher.completions
  |
  | optional registered waker is removed and woken
  | reflected by completed_operations and completed_operation_kinds
  v
future polls again
  |
  | take_completion()
  v
completion consumed by the future
```

When a future is dropped before its operation completes, the dispatcher changes
the path:

```text
future Drop
  |
  | clear_waker(operation)
  v
abandon_operation(operation)
  |
  | records operation -> kind in abandoned_operations
  | queues async cancel when possible
  | records cancel operation as abandoned too
  v
kernel completes original and/or cancel operation
  |
  v
IoUring::try_operation_completion()
  |
  v
IoUringDispatcher::dispatch_available()
  |
  | operation exists in abandoned_operations
  | removes abandoned record
  | removes deferred buffer if this was owned read/write
  | increments total_dispatched_* and total_discarded_*
  v
completion discarded
```

The dispatcher keeps abandoned owned read/write buffers alive in
`deferred_buffers` until the matching kernel completion is dispatched. This is
the safety boundary for owned-buffer futures: dropping the future does not drop
the allocation while the kernel may still read from or write to it.
`examples/os_uring_lifecycle.rs` prints the normal and abandoned paths as live
snapshots when run on a Linux host with `io_uring` enabled.

The snapshot fields map to these states:

- `IoUringSnapshot.pending_submissions`: SQEs queued in userspace but not yet
  accepted by the kernel.
- `IoUringSnapshot.pending_completions`: raw CQEs already drained into the
  local `IoUring` queue.
- `IoUringSnapshot.tracked_operations`: operations that have been submitted or
  queued and whose tracked completion has not yet been consumed from `IoUring`.
- `IoUringSnapshot.operation_kinds`: tracked operations grouped by kind.
  `IoUringOperationKindCounts::total()` and `is_empty()` summarize those
  grouped counts without repeating the individual fields.
- `IoUringDispatcherSnapshot.registered_wakers`: tasks waiting for tracked
  completions.
- `IoUringDispatcherSnapshot.completed_operations`: tracked completions buffered
  for futures to consume.
- `IoUringDispatcherSnapshot.completed_operation_kinds`: buffered completions
  grouped by operation kind.
- `IoUringDispatcherSnapshot.abandoned_operations`: original and cancellation
  operations whose completions will be discarded.
- `IoUringDispatcherSnapshot.abandoned_operation_kinds`: abandoned operations
  grouped by kind.
- `IoUringDispatcherSnapshot.deferred_buffers`: owned read/write buffers being
  kept alive only because the kernel operation has not completed yet.
- `IoUringDispatcherSnapshot.total_dispatched_operations`: all tracked
  completions dispatched since the dispatcher was created.
- `IoUringDispatcherSnapshot.total_buffered_operations`: dispatched completions
  made available to futures.
- `IoUringDispatcherSnapshot.total_woken_operations`: dispatched completions
  that woke a registered task.
- `IoUringDispatcherSnapshot.total_discarded_operations`: abandoned completions
  discarded instead of being exposed to a future.
- `IoUringDispatcherSnapshot.total_*_operation_kinds`: the same cumulative
  counters grouped by no-op, timeout, cancel, read, and write operation kind.

Both `IoUringSnapshot` and `IoUringDispatcherSnapshot` expose `is_idle()`.
For the raw ring, idle means no pending submissions, no locally buffered
completions, and no tracked operations. For the dispatcher, idle additionally
requires no registered wakers, buffered completions, abandoned operations, or
deferred owned buffers. Cumulative totals do not affect idleness; they describe
history, not live work.

Shard reply handles can be converted into awaitable futures through
`wait_async`. Replies use a small custom std-only one-shot primitive rather than
`std::sync::mpsc`, so a waiting future can store its task waker directly in the
reply state. When the shard sends the response, the reply wakes the task on the
custom executor.

The executor and reply futures may use synchronization for task bookkeeping, but
application service state remains shard-local.

`ShardedKv` owns:

- one `KvShardHandle` per shard
- one `JoinHandle` per shard thread

Each `KvShardHandle` owns a sender for that shard's mailbox.

Each shard thread owns:

- one receiving mailbox
- one `KvService`

`KvService` owns the actual `HashMap<String, String>`. It is private to the
`kv` module and is never shared with callers.

## Request Flow

For blocking `put`, `get`, and `delete`:

1. `ShardedKv` routes the key through the default hash placement strategy.
2. The matching `KvShardHandle` creates a one-shot reply channel.
3. The handle sends a typed `KvCommand` to the shard mailbox.
4. The caller blocks waiting for the reply.
5. The shard thread mutates or reads its local `KvService`.
6. The shard replies with an owned value.

No references into shard-local state cross the mailbox boundary.

For submitted commands, steps 1-3 are the same. The returned reply handle can
then be consumed by `wait`, `wait_timeout`, or `wait_async`.

## Placement

`placement` defines a small `Placement<K>` trait and a default `HashPlacement`
implementation backed by `shard_for_hash`. `ShardedKv::start` and
`ShardedKv::start_with_config` use the default hash strategy, while
`ShardedKv::start_with_placement` lets callers provide a placement strategy for
key-routed stores.

## Backpressure

Shard command mailboxes are bounded `std::sync::mpsc::sync_channel` queues. The
default capacity is exposed as `DEFAULT_MAILBOX_CAPACITY`, and callers can start
the store with `ShardedKv::start_with_config`. Running services expose their
configured shard count, per-shard mailbox capacity, and stopped state through
owned runtime snapshots.

The default `put`, `get`, `delete`, and length methods block if a shard mailbox
is full, waiting until the owning shard drains capacity. The `try_*` variants
attempt to enqueue without waiting for mailbox capacity and return
`ShardError::MailboxFull` if the queue is saturated.

Once a command is accepted into a mailbox, both the blocking and `try_*` methods
wait for the shard's reply. Submitted reply handles can be awaited through the
custom executor, but mailbox enqueue itself is still synchronous.

## Reply Handles

The `submit_*` methods enqueue commands and return `KvReply<T>` handles. This
lets a caller issue multiple commands first and then call `wait` on each reply
later. Callers can also consume a reply with `wait_async` to await it on the
custom executor.

The `try_submit_*` methods combine non-blocking mailbox enqueue with delayed
waiting. If the mailbox has capacity, the caller receives a reply handle. If the
mailbox is full, the call returns `ShardError::MailboxFull`.

Reply handles also support `try_wait` for a single non-blocking poll and
`wait_timeout` for bounded blocking waits. A timeout is reported as
`ShardError::ReplyTimeout`.

Aggregate reply handles, such as total length, all keys, multi-key reads,
multi-key deletes, counter totals, and per-shard snapshots, also expose
`wait_async`.

## Snapshots

`shard_snapshots` returns owned `ShardSnapshot` values containing a `ShardId` and
the current key count for that shard. Snapshot collection is implemented by
sending length commands to shards; it does not expose references to shard-local
state.

`keys_on_shard` and `all_keys` return owned, sorted key vectors. They clone keys
inside the owning shard and send those owned values back to the caller.

`get_many` sends one owned get command per requested key and returns owned
key/value pairs in the same order the keys were submitted. Missing keys are
reported as `None`.

`delete_many` follows the same ordered shape, returning each key with its
previous value. Missing keys are reported as `None`.

## Shard-Local Atomic Operations

`compare_and_put` performs the comparison and mutation inside the shard that owns
the key. This avoids exposing a racy get-then-put sequence to callers and keeps
multi-step service logic local to the state owner.

`get_or_put` follows the same pattern: the owning shard either returns the
existing value or inserts and returns the provided value as one local operation.

## Second Service

`ShardedCounter` is a deliberately small second service built on the same
runtime primitives. It has its own command enum and shard-local state, proving
the runtime layer is reusable without making key-value behavior generic. Its
aggregate total operation also uses a reply handle, so callers can enqueue all
shard reads before waiting for the final sum. Like the key-value store, it can
return owned per-shard snapshots without exposing references to shard-local
state.

## Shutdown

`ShardedKv::stop` consumes the store handle, sends `Stop` to each shard, and
joins all shard threads. `shutdown(&mut self)` performs the same shutdown while
retaining the handle, which lets callers inspect a stopped runtime snapshot.
`Drop` also performs best-effort shutdown if a caller forgets to shut the
service down explicitly, but explicit shutdown is preferred because it can
return errors.

## Current Non-Goals

This milestone does not implement:

- persistence
- CPU pinning
- procedural macros
- production-grade `io_uring` integration with the sharded executor
- `kqueue` support on macOS
- load balancing or scheduling classes

Those are later runtime concerns. The current goal is still to grow the
shared-nothing runtime shape in small, dependency-free steps.
