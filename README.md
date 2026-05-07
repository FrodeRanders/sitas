# Sitas

`sitas` is a small Rust experiment in shard-local service ownership, typed
message passing, executor internals, and Unix runtime backends.

The project is inspired by Seastar's shard-per-core, shared-nothing model, but
this first milestone is intentionally much smaller. It does not attempt to clone
Seastar.

## First Milestone

The current version implements a sharded key-value store using only the Rust
standard library:

- a small reusable std-only runtime layer
- a minimal executor experiment with custom wakers, join handles, awaitable
  shard replies, cancellable spawned tasks, timers, timeouts, OS-backed
  sleeping, racing futures, and read/write-readiness futures on this branch
- direct root-future driving without requiring the root future to be `Send` or
  `'static`
- budgeted ready-queue polling so timers and I/O can progress under repeated
  task wakeups
- task scopes that group child tasks under one cooperative stop signal and
  abort still-owned children when dropped or when bounded shutdown times out
- executor shutdown cleanup for pending task futures and readiness/timer
  registrations
- small async connect, accept, read, write, and copy helpers layered on
  non-blocking Unix descriptors
- a bounded async TCP server helper that spawns and joins one handler per
  accepted connection
- an idle-timeout async TCP server helper for cancellable accept loops
- stop tokens and a stoppable async TCP server helper for explicit accept-loop
  shutdown
- a scoped async TCP server helper that propagates shutdown into accepted
  connection handlers and stops accepting when a handler fails
- timeout variants for the async Unix I/O helpers
- an early Unix runtime backend experiment using direct OS FFI for reactor wakes
  and descriptor readiness
- one OS thread per shard
- one mailbox per shard
- bounded shard mailboxes
- owned runtime snapshots for shard count, mailbox capacity, and stopped state
- typed internal commands
- blocking request/reply with `std::sync::mpsc`
- local service state owned by the shard thread
- key-based routing through default hash placement or caller-provided placement
- clean shutdown through `ShardedKv::stop`
- non-consuming shutdown for post-shutdown runtime inspection
- best-effort shutdown on drop if `stop` is not called
- basic backpressure by blocking callers when a shard mailbox is full
- `try_*` operations that report a full mailbox instead of waiting for capacity
- `submit_*` operations that enqueue a command and return a reply handle for
  blocking or async waiting later
- std-only one-shot replies that can store wakers for the custom executor
- multi-key reads and deletes that preserve caller-provided key order
- owned per-shard snapshots for observing distribution without sharing state
- owned key snapshots for debugging and inspection
- shard-local compare-and-put for atomic conditional updates
- shard-local get-or-put for atomic read-or-insert behavior
- a second sharded counter service with delayed total reads and per-shard
  snapshots, proving the runtime layer is reusable

The core invariant is:

```text
Only the owning shard may mutate its service state.
All cross-shard interaction happens through typed messages.
```

No mutex protects the key-value service state because that state is never
shared. Values returned across shard boundaries are owned values.

See [docs/architecture.md](docs/architecture.md) for the current request flow
and shutdown model.

## Deliberately Missing

This milestone does not include:

- Tokio, Glommio, Monoio, or other async runtimes
- actor frameworks
- production-ready async I/O
- production-ready networking
- persistence
- CPU pinning
- scheduling classes
- procedural macro service generation
- broad `unsafe` usage outside the small Unix FFI backend

Later milestones may add async I/O, CPU affinity, backpressure, and fuller
OS-specific runtime backends.

## Platform Notes

The std-only baseline should work on both macOS and Linux because it only uses
portable Rust standard-library concurrency primitives. The `non-std-runtime`
branch keeps macOS and Linux as active targets for direct Unix runtime work.

Linux is expected to become the primary performance and production target for
later low-level runtime work, especially for CPU affinity and `io_uring`.

## Example

```rust
use sitas::{ShardId, ShardedKv};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = ShardedKv::start(4)?;

    kv.put("alpha", "one")?;
    kv.put("beta", "two")?;

    println!("{:?}", kv.get("alpha")?);

    for shard_idx in 0..kv.shard_count() {
        let len = kv.len_on_shard(ShardId(shard_idx))?;
        println!("shard {shard_idx}: {len} keys");
    }

    kv.stop()?;
    Ok(())
}
```

Run the included example:

```sh
cargo run --example basic_kv
```

Run the concurrent caller example:

```sh
cargo run --example concurrent_kv
```

Run the submit-and-wait-later example:

```sh
cargo run --example submit_kv
```

Run the custom-executor async reply example:

```sh
cargo run --example async_kv
```

Run the executor async accept helper example:

```sh
cargo run --example async_accept
```

Run the executor async connect helper example:

```sh
cargo run --example async_connect
```

Run the executor TCP echo example:

```sh
cargo run --example async_tcp_echo
```

Run the same-executor TCP echo pair example:

```sh
cargo run --example async_tcp_pair
```

Run the bounded async TCP server helper example:

```sh
cargo run --example async_tcp_server
```

Run the idle-timeout async TCP server helper example:

```sh
cargo run --example async_tcp_idle_server
```

Run the stoppable async TCP server helper example:

```sh
cargo run --example async_tcp_stoppable_server
```

Run the scoped async TCP server helper example:

```sh
cargo run --example async_tcp_scoped_server
```

Run the async TCP timeout example:

```sh
cargo run --example async_tcp_timeout
```

Run the executor multi-client TCP echo example:

```sh
cargo run --example async_tcp_multi_echo
```

Run the executor async copy helper example:

```sh
cargo run --example async_copy
```

Run the executor read-readiness future example:

```sh
cargo run --example async_readable
```

Run the executor async write helper example:

```sh
cargo run --example async_write
```

Run the executor timer example:

```sh
cargo run --example executor_sleep
```

Run the executor task abort example:

```sh
cargo run --example executor_abort
```

Run the executor timeout example:

```sh
cargo run --example executor_timeout
```

Run the executor race example:

```sh
cargo run --example executor_race
```

Run the executor task scope example:

```sh
cargo run --example executor_task_scope
```

Run the custom placement example:

```sh
cargo run --example custom_placement
```

Run the counter example:

```sh
cargo run --example basic_counter
```

Run the OS reactor wake example:

```sh
cargo run --example os_reactor
```

Run the OS read-readiness example:

```sh
cargo run --example os_readable
```

## Development

Run the standard checks:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
cargo run --example basic_kv
cargo run --example concurrent_kv
cargo run --example submit_kv
cargo run --example async_kv
cargo run --example async_accept
cargo run --example async_connect
cargo run --example async_tcp_echo
cargo run --example async_tcp_pair
cargo run --example async_tcp_server
cargo run --example async_tcp_idle_server
cargo run --example async_tcp_stoppable_server
cargo run --example async_tcp_scoped_server
cargo run --example async_tcp_timeout
cargo run --example async_tcp_multi_echo
cargo run --example async_copy
cargo run --example async_readable
cargo run --example async_write
cargo run --example executor_sleep
cargo run --example executor_abort
cargo run --example executor_timeout
cargo run --example executor_race
cargo run --example executor_task_scope
cargo run --example custom_placement
cargo run --example basic_counter
cargo run --example os_reactor
cargo run --example os_readable
```

Run the Linux Docker check from macOS:

```sh
tools/linux-docker.sh
```

Pass a custom command after the script name to run a narrower Linux check:

```sh
tools/linux-docker.sh cargo test os::tests
```
