# sitas-core — no_std repair plan

## Current state

`sitas-core` at commit `57d1071` does not compile on **any** target (host or
`aarch64-unknown-none`).  The commit history claims it compiled cleanly at
`8c73913` ("Add ShardRuntime trait + ringbuf + fix no_std imports for
executor"), but the refactor commits since then (`f712f19`, `d5549f0`,
`d67234a`, `57d1071`) deleted or renamed critical modules without updating
callers.  The `catten-user` binary in charlotte-os depends on it and cannot
rebuild until sitas-core is fixed.

## Inventory of blockers

### 1. Crate root (`lib.rs`)

- Missing `#![no_std]`
- Missing `extern crate alloc;`
- Missing `pub mod executor;`, `pub mod kv;`, `pub mod counter;`, `pub mod os;`
  These modules existed in the pre-split monolith (`commit 2e30ff5~1`) but
  were never carried over to the workspace crates.

### 2. Phantom modules (files that existed in history but are now missing)

| Module       | Old location (commit 2e30ff5~1)        | Used by                            | What it defined                                             |
|--------------|----------------------------------------|------------------------------------|-------------------------------------------------------------|
| `executor`   | `src/executor.rs` + `src/executor/*`   | `sharded_executor`, `spawner`, `shard_local`, `runtime`, `task_set`, `task_state`, `shard_mailbox`, `stream_reply` | `Executor`, `executor_and_spawner`, `block_on`, `Spawner`, `JoinHandle`, `SchedulingGroup`, `StopSource`, `StopToken`, `DEFAULT_SCHEDULING_GROUP_ID`, etc. |
| `kv`         | `src/kv.rs` + `src/kv/reply.rs`       | `basic_kv`, `async_service`        | `ShardedKv`, `ShardedKvConfig`                             |
| `counter`    | `src/counter.rs`                       | `async_service`                    | `ShardedCounter`, `CounterShardSnapshot`                    |
| `os`         | `src/os.rs` + `src/os/*.rs`           | `reactor_backend`, `types`, `scheduler`, `snapshot`, `root`, `task_set`, `join`, `driver`, `tests` | `OsReactor`, `OsWaker`, `OsEvent`, `RawFd`                 |

The old module bodies are heavily `std`-coupled (`std::mpsc`, `std::thread`,
`std::collections::HashMap`, `std::os::unix::io::RawFd`, `std::net`).
They cannot be dropped in as-is; they must be reimplemented over the new
`no_std` primitives (`ShardRuntime`, `ringbuf::RingBuffer`, `instant::Instant`,
`crate::io::ErrorKind`).

### 3. Syntax errors in `src/sharded_executor.rs`

- Line 4: `//!` inner doc comment appears after `use` items (E0753)
- Lines 381–1122: `impl ShardedExecutor<R>` block is never closed with `}`
- Line 448: orphaned statement `config.validate()?;` outside any function
- Line 1051: `where` clause before `->` return type
- Lines 448–543: Body of `start_with_config` with no function declaration

### 4. `no_std` incompatibilities in the modules `catten-user` actually needs

`catten-user` depends on `sitas-charlotte`, which depends on
`sitas-core::{reactor_backend, shard_runtime, shard}`.  Plus `catten-user`
directly calls `sitas_core::basic_kv`, which needs `crate::kv::*` and
`crate::shard_runtime::ShardRuntime`.

Minimal dependency chain:

```
catten-user
├── sitas-charlotte
│   ├── reactor_backend  (traits: ReactorWaker, SchedulerWake, ReactorEvent, ReactorBackend)
│   ├── shard_runtime    (ShardRuntime trait, ShardJoinHandle, RingBuffer channels)
│   └── shard            (ShardId)
└── sitas-core
    ├── basic_kv         (basic_kv_test → ShardedKv)
    └── kv (phantom)     (ShardedKv, ShardedKvConfig — must be recreated)
```

The concrete `std` usage blocking `no_std`:

- **`reactor_backend.rs` line 49**: `use std::io;` — used for `io::ErrorKind`/`io::Result`.
  Fix: replace with `use crate::io::{self, ErrorKind};` (sitas-core already has a
  polyfill in `src/io.rs`).

- **`reactor_backend.rs` lines 146–283**: `#[cfg(unix)]` concrete `OsReactor`/`MockReactor`
  implementations.  These are std-only but are **not needed by catten-user**
  (sitas-charlotte provides its own `CharlotteReactor`).  Gate them behind a
  `#[cfg(feature = "std")]` or move them into `sitas-unix`.

- **`kv.rs` (does not exist)**:  The old `src/kv.rs` (`commit 2e30ff5~1`)
  defines `ShardedKv` using `std::thread::spawn`, `std::sync::mpsc`, and
  `std::collections::HashMap`.  It must be reimplemented over `ShardRuntime`
  (replacing thread/MPSC with `RingBuffer` channels and `spawn_shard`)
  and `alloc::collections::BTreeMap` or `hashbrown` (replacing HashMap).

### 5. Modules with `std` that catten-user does NOT need

These can be feature-gated behind `std` and left broken until later:

- `sharded_executor`, `async_service`, `runtime`, `scheduler`, `shard_local`,
  `shard_mailbox`, `spawner`, `sync`, `task`, `task_set`, `task_state`,
  `scheduling_group`, `scope`, `join`, `root`, `driver`, `snapshot`,
  `tcp`, `udp`, `sharded`, `counters`, `io_interest`, `backpressure`,
  `current`, `types`, `timer`, `future`, `stream_reply`

## Fix plan (ordered by dependency)

### Phase 1 — Get `catten-user`'s transitive deps compiling `no_std`

1. **Restore crate root declarations** (5 minutes)
   - Add `#![no_std]` and `extern crate alloc;` to `lib.rs`
   - Add `pub mod kv;` (will need kv.rs next)
   - Add a `std` feature to `Cargo.toml`
   - Gate all unneeded modules behind `#[cfg(feature = "std")]` (see list above)

2. **Fix `reactor_backend.rs`** (10 minutes)
   - Replace `use std::io;` → `use crate::io::{self, ErrorKind};`
   - Gate lines 140–end (`impl ReactorWaker for OsWaker` etc.) behind `#[cfg(feature = "std")]`

3. **Create `kv.rs` — the main work** (1–2 hours)
   - Recover `ShardedKv`, `ShardedKvConfig` signatures from `2e30ff5~1:src/kv.rs`
   - Replace `std::thread::spawn` / `std::sync::mpsc` with `ShardRuntime::spawn_shard`
     and `shard_runtime::channel`
   - Replace `std::collections::HashMap` with `alloc::collections::BTreeMap` or
     `hashbrown::HashMap`
   - Implement the 3 methods `basic_kv_test` actually calls:
       - `ShardedKv::start_with_runtime(config, runtime) -> Result<Self>`
       - `shard.put(key, value) -> Result<()>`
       - `shard.get(key) -> Option<value>`
       - `shard.total_len() -> Result<usize>`
   - The old `kv.rs` handles this via a `Cmd` enum sent over channels; the new
     implementation should follow the same pattern but over `ShardRuntime`'s
     `channel()` instead of `mpsc`.

4. **Verify** (10 minutes)
   - `cargo build -p sitas-core --target aarch64-unknown-none` should work
   - `cargo build -p sitas-charlotte` transitively
   - `cargo +nightly build --manifest-path crates/catten-user/Cargo.toml --target … -Z build-std=core,alloc`

### Phase 2 — Full sitas restoration (future session)

5. Fix `sharded_executor.rs` syntax errors
6. Recreate `executor` module facade + `block_on`, `executor_and_spawner` etc.
   from the old `src/executor.rs`, ported to `no_std`
7. Recreate `counter.rs`, `os.rs` (or move the os reactor to `sitas-unix`)
8. Restore `shard_mailbox` error/config types (`ShardSendError`, `ShardRecvError`, etc.)
9. Feature-gate `tcp`/`udp` (inherently std-only; move to `sitas-unix`)

## Quick-start: recover missing modules from history

The old monolith's files are accessible via git:

```sh
# Executor facade + all submodules
git show 2e30ff5~1:src/executor.rs > crates/sitas-core/src/executor.rs
for f in $(git ls-tree -r --name-only 2e30ff5~1 src/executor/); do
  git show 2e30ff5~1:$f > crates/sitas-core/src/$(basename $f)
done

# Kv module
git show 2e30ff5~1:src/kv.rs > crates/sitas-core/src/kv.rs
git show 2e30ff5~1:src/kv/reply.rs > crates/sitas-core/src/kv_reply.rs
```

These files will need substantial editing (replace std::* with no_std equivalents),
but they provide the original API surface and implementation logic as a baseline.
