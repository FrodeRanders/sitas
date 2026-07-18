# sitas — status

## Current state (restoration complete)

The workspace split and the full runtime restoration are both done. All lanes
build and are validated by `tools/linux-docker.sh`:

- **`sitas-core` default (`no_std` + `alloc`)** — the CharlotteOS lane:
  `ShardRuntime`, `ReactorBackend`, `ShardExecutor`, ring-buffer channels,
  `kv`/`basic_kv`. Guarded by
  `cargo check -p sitas-core -p sitas-charlotte --target aarch64-unknown-none`.
- **`sitas-core --features std`** — the restored host runtime (the former
  monolith): `executor` + `os` (readiness I/O, TCP/UDP, Linux `io_uring`),
  `runtime`, `kv_service`, `counter`, `sharded`, `sharded_executor`,
  `shard_local`, `shard_mailbox`, `sharded_tcp`, `stream_reply`,
  `async_service`, `metrics`, `running_stats`, `charlotte_abi`. 330+ unit
  tests.
- **`crates/sitas`** — the `sitas` facade package: re-exports core with `std`
  and owns all examples and integration tests (`use sitas::...` unchanged).
- **`crates/sitas-unix`** — `UnixRuntime: ShardRuntime` (std threads,
  `Mutex`/`Condvar` parker, `OsReactor` per shard), so the CharlotteOS code
  path (`kv` over `ShardExecutor`) is tested on Unix hosts.
- **charlotte-os integration** — `catten-user` builds against this tree with
  `cargo +nightly build --target aarch64-unknown-none.json -Z build-std=core,alloc`.

## How the restoration was done

- `sitas-core` uses `#![cfg_attr(not(feature = "std"), no_std)]`; the `std`
  feature was previously unusable because the crate was unconditionally
  `no_std` (every `use std::...` failed, ~220 errors).
- The old monolith modules were transplanted from the pre-split tree
  (`2e30ff5~1` lineage) into `crates/sitas-core` behind the `std` feature.
  The old rich kv service was renamed `kv_service`; the `no_std` `kv` keeps
  its name and its CharlotteOS callers.
- The monolith's `ReactorBackend` impls were adapted to the `no_std` error
  type (`crate::io::ErrorKind`); `From<std::io::Error>` conversions live in
  `sitas_core::io`.
- Incoherent half-migration stubs (e.g. `ShardedKv::start_with_placement_runtime`
  assigning a `Sharded` into a `ShardSet` field) were removed; the generic
  runtime lane is the `no_std` `kv`, not the std services.
- `ShardJoinHandle::from_raw` and the `Raw` variant are available in all
  feature combinations so `sitas-charlotte` survives feature unification with
  std-enabled builds in one workspace.
- The root `src/`, `examples/`, `tests/` monolith reference tree was removed;
  examples/tests moved verbatim to `crates/sitas`.

## Remaining known gaps

- `RawJoinHandle::join` on CharlotteOS reports an error (kernel thread
  lifecycle is cooperative; joining is not implemented).
- `ShardRuntime` placement is advisory in `UnixRuntime` (no pinning); CPU
  pinning remains an explicit experiment in `sitas_core::sharded_executor`.
- The std `sharded_executor` is not generic over `ShardRuntime`; running the
  full shard-per-thread runtime on CharlotteOS is future work layered on
  `ShardExecutor`.
- `charlotte_abi` (the in-memory ABI reference model) and `sitas-charlotte`
  (the real backend) are validated separately; no combined conformance suite
  yet.

## Validation

```sh
tools/linux-docker.sh                        # full lane: fmt, clippy, tests, bare-metal guard, doc, examples
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh sh -c \
  'cargo test -p sitas-core --features std uring && cargo run -p sitas --example os_uring'
cargo check -p sitas-core -p sitas-charlotte --target aarch64-unknown-none
```
