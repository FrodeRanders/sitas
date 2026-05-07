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

- direct Unix FFI, currently `pipe`, `poll`, `read`, `write`, `fcntl`, and
  `close`
- a non-blocking pipe for cross-thread reactor wakeups
- read/write-readiness polling for caller-provided file descriptors
- a cloneable `OsWaker`
- a blocking `OsReactor::wait` that can be woken by the pipe

This is intentionally smaller than a full reactor. It establishes the FFI
boundary and a portable macOS/Linux wake mechanism before introducing file
descriptor registration, timers, network sockets, or platform-specific backends
such as `epoll`, `kqueue`, or `io_uring`.

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
  children when the scope is dropped
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
- `serve_tcp_until_idle` runs the same handler-spawning accept loop until the
  listener stays idle past a caller-provided timeout
- `serve_tcp_until_stopped` races listener accepts against a stop token for
  explicit accept-loop shutdown
- `serve_tcp_until_stopped_scoped` uses a task scope for accepted connection
  handlers, passing each handler a stop token so shutdown propagates beyond the
  listener accept loop

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

- non-blocking I/O
- file descriptor registration
- actor framework behavior
- networking
- persistence
- CPU pinning
- `io_uring`
- procedural macros

Those are later runtime concerns. The current goal is the shard-local ownership
and message-passing kernel.
