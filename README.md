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
  shard replies, timers, OS-backed sleeping, and read/write-readiness futures on
  this branch
- small async read and write helpers layered on non-blocking Unix descriptors
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
- networking
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
cargo run --example async_readable
cargo run --example async_write
cargo run --example executor_sleep
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
