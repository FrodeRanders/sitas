# Architecture

This first milestone is intentionally small. It validates the ownership model
before introducing async I/O, custom scheduling, CPU affinity, networking, or
OS-specific backends.

## Components

`runtime` provides the small reusable standard-library kernel:

- bounded shard mailboxes
- shard set startup
- one-shot reply handles
- non-blocking mailbox send error mapping
- shard thread join handling

It deliberately does not know about key-value commands or service state.

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

For `put`, `get`, and `delete`:

1. `ShardedKv` hashes the key into a `ShardId`.
2. The matching `KvShardHandle` creates a one-shot reply channel.
3. The handle sends a typed `KvCommand` to the shard mailbox.
4. The caller blocks waiting for the reply.
5. The shard thread mutates or reads its local `KvService`.
6. The shard replies with an owned value.

No references into shard-local state cross the mailbox boundary.

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
wait for the shard's reply. This is deliberately simple backpressure: no async
runtime, no polling API, and no scheduler integration yet.

## Reply Handles

The `submit_*` methods enqueue commands and return `KvReply<T>` handles. This
lets a caller issue multiple commands first and then call `wait` on each reply
later. These handles are blocking one-shot receivers, not futures.

The `try_submit_*` methods combine non-blocking mailbox enqueue with delayed
waiting. If the mailbox has capacity, the caller receives a reply handle. If the
mailbox is full, the call returns `ShardError::MailboxFull`.

Reply handles also support `try_wait` for a single non-blocking poll and
`wait_timeout` for bounded blocking waits. A timeout is reported as
`ShardError::ReplyTimeout`.

## Snapshots

`shard_snapshots` returns owned `ShardSnapshot` values containing a `ShardId` and
the current key count for that shard. Snapshot collection is implemented by
sending length commands to shards; it does not expose references to shard-local
state.

`keys_on_shard` and `all_keys` return owned, sorted key vectors. They clone keys
inside the owning shard and send those owned values back to the caller.

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

- async/await
- non-blocking I/O
- a reactor
- a custom executor
- actor framework behavior
- networking
- persistence
- CPU pinning
- `io_uring`
- procedural macros

Those are later runtime concerns. The current goal is the shard-local ownership
and message-passing kernel.
