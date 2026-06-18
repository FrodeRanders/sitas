# Sitas

`Sitas` is a pure-Rust (edition 2024), zero-dependency exploration of Seastar-like shard-per-core architecture 
built on Rust ownership semantics. It explores what shared-nothing looks like when designed around type
boundaries, explicit message passing, and isolated unsafe.

## Overview

`Sitas` currently has...

A stable baseline:
- Shard-per-thread runtime with bounded mailboxes, typed Command/Reply<T> APIs, and clean startup/shutdown
- ShardedKv and ShardedCounter as concrete reference services
- Blocking, try_* (non-blocking send), and submit_* (reply handle) variants for all operations
- Owned ShardSnapshot / RuntimeSnapshot observability

A custom executor:
- Single-threaded async executor (block_on, run, run_until) without requiring Send + 'static
- Spawner, JoinHandle<T>, task lifecycle tracking, timer wheel, sleep, timeout, race
- Cooperative primitives: StopSource/StopToken, Notify, yield_now, TaskScope
- Scheduling groups with weighted virtual runtime

Unix I/O:
- Readiness futures (readable, writable, read_exact_async, write_all_async, copy_async)
- Non-blocking TCP connect/accept/copy helpers
- TCP server helpers: fixed-count, idle-timeout, stoppable, scoped variants
- Backends: epoll (Linux), kqueue (macOS/iOS), poll (fallback)

A sharded executor:
- ShardedExecutor with one executor + OS thread per shard
- ShardedSubmitter for cross-shard async submission
- CPU affinity on Linux (sched_setaffinity), container cpuset-aware
- ShardLocal<T> — per-shard owned values with no mutex, closure-based access, stoppable workers
  
Experimental io_uring (Linux):
- Full ring, dispatcher lifecycle, tracked operations, abandoned buffer safety, completion counting
- Integrated for file I/O futures (read_at_uring, write_all_at_uring)
- Not yet unified with readiness/timers into a production I/O engine

## Inspiration

`Sitas` is inspired by Seastar's shard-per-core, shared-nothing model, but
is smaller and does not attempt to clone Seastar.

## First Milestone

The current version implements a sharded key-value store on top of the runtime
using only the Rust standard library.

`Sitas` has:

- a small reusable std-only runtime layer
- a custom executor with wakers, join handles, awaitable shard replies,
  cancellable tasks, timers, timeouts, OS-backed sleeping, racing futures,
  and read/write-readiness futures
- root-future driving without requiring `Send` or `'static`
- budgeted ready-queue polling so timers and I/O can progress under repeated
  task wakeups
- task scopes that group child tasks under one cooperative stop signal and
  abort still-owned children when dropped or when bounded shutdown times out
- a one-shot async notification primitive for waking one or more executor tasks
  without modeling the event as shutdown
- executor shutdown cleanup for pending task futures and readiness/timer
  registrations
- small async connect, accept, read, write, and copy helpers layered on
  non-blocking Unix descriptors
- a bounded async TCP server helper that spawns and joins one handler per
  accepted connection
- a shard-per-thread async runtime with one executor/reactor per shard thread
  and explicit `ShardId` placement
- dependency-free executor snapshots for named tasks, task states, poll counts,
  queue depth, timers, and I/O interests across shards
- weak observer handles for monitoring shard executors without keeping them
  alive
- cloneable sharded submitters that let work on one shard submit and await work
  on another shard
- broadcast-style shard submission for running one async task per shard and
  collecting shard-tagged outputs
- map/reduce helpers for running one async computation per shard and reducing
  the shard-tagged outputs
- shard-local state cells that run synchronous mutations on the owning shard
  executor without protecting the state with a mutex
- shard-local map/reduce helpers for collecting or reducing outputs from
  shard-owned state
- cloneable shard-local service handles that can be moved into async shard
  tasks while sharing the same per-shard state
- direct current-shard access for shard-local state when a task is already
  running on the owning shard executor
- shard-local worker helpers that start one async task per shard with a cloned
  handle to the same shard-owned state
- shard-local worker join sets that collect or reduce per-shard worker outputs
- stoppable shard-local workers that share a cooperative stop token across
  shards
- bounded stoppable worker shutdown that aborts still-running shard workers
  when the deadline elapses
- named shard-local workers that appear in dependency-free executor snapshots
  for long-running task observability
- an idle-timeout async TCP server helper for cancellable accept loops
- bounded handler-join variants for fixed-count, idle-timeout, and stoppable
  TCP server helpers
- stop tokens and a stoppable async TCP server helper for explicit accept-loop
  shutdown
- a scoped async TCP server helper that propagates shutdown into accepted
  connection handlers, stops accepting when a handler fails, and can bound
  handler shutdown time
- timeout variants for the async Unix I/O helpers
- an early Unix runtime backend using direct OS FFI for reactor wakes and
  descriptor readiness, including Linux `epoll` and a portable Unix `poll`
  fallback
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

```
Only the owning shard may mutate its service state.
All cross-shard interaction happens through typed messages.
```

No mutex protects the key-value service state because it is never shared.
Values returned across shard boundaries are owned values.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current request flow
and shutdown model.

## Deliberately Missing

This milestone does not include:

- Tokio, Glommio, Monoio, or other async runtimes
- actor frameworks
- production-ready async I/O
- production-ready networking
- persistence
- portable production CPU pinning
- scheduling classes
- procedural macro service generation
- broad `unsafe` usage outside the small Unix FFI backend

Later milestones may add async I/O, CPU affinity, backpressure, and fuller
OS-specific runtime backends.

## Platform Notes

The std-only baseline works on both macOS and Linux because it uses only
portable Rust standard-library concurrency primitives. The `non-std-runtime`
branch keeps macOS and Linux as active targets for direct Unix runtime work.

Linux is the primary performance and production target for later low-level
runtime work, especially CPU affinity and `io_uring`.

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

Examples are grouped by concept level. Start at the top and work down.

### Sharded services (std-only baseline)

```sh
cargo run --example basic_kv                # basic key-value store
cargo run --example concurrent_kv           # concurrent callers
cargo run --example submit_kv               # submit-and-wait-later
cargo run --example async_kv                # custom-executor async replies
cargo run --example custom_placement        # caller-provided key placement
cargo run --example basic_counter           # second service proving runtime reuse
```

### Executor basics (timers, cancellation, composition)

```sh
cargo run --example executor_sleep          # timer futures
cargo run --example executor_abort          # task abort
cargo run --example executor_timeout        # timeout cancellation
cargo run --example executor_race           # racing two futures
cargo run --example executor_task_scope     # structured child tasks
```

### I/O readiness (non-blocking descriptor helpers)

```sh
cargo run --example async_readable          # read-readiness future
cargo run --example async_write             # async write helper
cargo run --example async_copy              # async copy helper
```

### TCP (accept, connect, server patterns)

```sh
cargo run --example async_accept            # accept helper
cargo run --example async_connect           # connect helper
cargo run --example async_tcp_echo          # TCP echo
cargo run --example async_tcp_pair          # same-executor TCP pair
cargo run --example async_tcp_server        # bounded server
cargo run --example async_tcp_server_timeout        # bounded shutdown
cargo run --example async_tcp_idle_server           # idle-timeout server
cargo run --example async_tcp_idle_server_timeout    # bounded idle shutdown
cargo run --example async_tcp_stoppable_server      # stop-token server
cargo run --example async_tcp_scoped_server         # scoped server with handler propagation
cargo run --example async_tcp_timeout       # I/O timeout helpers
cargo run --example async_tcp_multi_echo    # multi-client echo
```

### Sharded runtime (shard-per-thread executor)

```sh
cargo run --example sharded_executor        # one executor per shard
cargo run --example sharded_observability   # task snapshots across shards
cargo run --example sharded_submit          # cross-shard async submission
cargo run --example sharded_broadcast       # broadcast to all shards
cargo run --example sharded_map_reduce      # map/reduce over shards
cargo run --example sharded_index_build     # fixed-record index build
```

### Shard-local state (per-shard owned values)

```sh
cargo run --example shard_local             # basic shard-local access
cargo run --example shard_local_handle      # cloneable handles
cargo run --example shard_local_current     # direct current-shard access
cargo run --example shard_local_workers     # per-shard workers
cargo run --example shard_local_stoppable_workers           # cooperative stop
cargo run --example shard_local_stoppable_workers_timeout    # bounded shutdown
cargo run --example shard_local_worker_observability        # worker snapshots
```

### OS primitives (low-level reactor)

```sh
cargo run --example os_reactor              # reactor wake
cargo run --example os_readable             # read-readiness
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
cargo run --example async_tcp_server_timeout
cargo run --example async_tcp_idle_server
cargo run --example async_tcp_idle_server_timeout
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

By default this proves the Linux build, formatting, clippy lints, tests, docs,
and examples, but `io_uring` may be skipped if the container runtime blocks
`io_uring_setup`. To require `io_uring` coverage, run with Docker seccomp
disabled:

```sh
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh
```

That sets `SITAS_REQUIRE_IO_URING=1` inside the container, so the `io_uring`
tests and examples fail instead of silently skipping when the kernel or
container configuration does not allow `io_uring`. If seccomp is not enough for
your Docker environment, the script also supports:

```sh
SITAS_DOCKER_PRIVILEGED=1 SITAS_REQUIRE_IO_URING=1 tools/linux-docker.sh
```

Pass a custom command after the script name to run a narrower Linux check:

```sh
tools/linux-docker.sh cargo test os::tests
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh cargo test os::uring::tests -- --nocapture
```

Custom `cargo fmt` and `cargo clippy` commands automatically install the
matching rustup component inside the transient container before running.
