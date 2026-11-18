# sitas — status

## Current state (ownership and lifecycle hardening)

The workspace split and runtime restoration are complete. The runtime remains
experimental. `tools/linux-docker.sh` covers the host and portable build lanes:

- **`sitas-core` default (`no_std` + `alloc`)** — the Knytos lane:
  `ShardRuntime`, `ReactorBackend`, `ShardExecutor`, ring-buffer channels,
  `kv`/`basic_kv`. Guarded by
  `cargo check -p sitas-core -p sitas-knytos --target aarch64-unknown-none`.
- **`sitas-core --features std`** — the restored host runtime (the former
  monolith): `executor` + `os` (readiness I/O, TCP/UDP, Linux `io_uring`),
  `runtime`, `kv_service`, `counter`, `sharded`, `sharded_executor`,
  `shard_local`, `shard_mailbox`, `sharded_tcp`, `stream_reply`,
  `async_service`, `metrics`, `running_stats`, `charlotte_abi`. 330+ unit
  tests.
- **`crates/sitas`** — the `sitas` facade package: re-exports core with `std`
  and owns all examples and integration tests (`use sitas::...` unchanged).
- **`crates/sitas-unix`** — `UnixRuntime: ShardRuntime` (std threads,
  `Mutex`/`Condvar` parker, `OsReactor` per shard), so the Knytos code
  path (`kv` over `ShardExecutor`) is tested on Unix hosts.
- **knytos integration** — `knytos-user` builds against this tree with
  `cargo +nightly build --target aarch64-unknown-none.json -Z build-std=core,alloc`.

## Hardening changes

- Shard-local access checks runtime identity as well as shard number before
  touching non-atomic state. Cross-runtime current-shard access is rejected.
- The portable queue has safe synchronized storage and normal payload cleanup.
  Channels handle last-sender and receiver destruction, serialize close against
  accepted sends, and remove cancelled receive wakers.
- `ShardRuntime` join handles implement `ShardThread<T>`. Portable KV and the
  mailbox-index fixture retain and join workers. KV startup failure and worker
  failure have host regressions through `UnixRuntime`.
- Both executor loops poll readiness between ready-task batches. Portable
  `try_run` returns reactor failures instead of spinning.
- Timed host shutdown returns an owning outcome for unfinished threads. Its
  `wait()` confirms later termination; dropping it detaches those threads. A
  stop request never claims to interrupt a running poll.
- Executor `io_uring` buffers belong to their originating dispatcher until
  completion. Synchronized result handles support movement and cancellation
  across threads. Final origin destruction fails pending results safely, and
  completion draining uses a monotonic deadline rather than an unbounded CQE wait.
- Knytos CQ consumption has atomic publication/reuse ordering and serialized
  readers. Joins retain spawn generations and release abandoned subscriptions.
  Raw CQ mapping construction is an explicitly unsafe boundary.

See `docs/architecture.md` for the guarantees and limits. These changes do not
turn host test success or cross-compilation into guest boot qualification.

## How the restoration was done

- `sitas-core` uses `#![cfg_attr(not(feature = "std"), no_std)]`; the `std`
  feature was previously unusable because the crate was unconditionally
  `no_std` (every `use std::...` failed, ~220 errors).
- The old monolith modules were transplanted from the pre-split tree
  (`2e30ff5~1` lineage) into `crates/sitas-core` behind the `std` feature.
  The old rich kv service was renamed `kv_service`; the `no_std` `kv` keeps
  its name and its Knytos callers.
- The monolith's `ReactorBackend` impls were adapted to the `no_std` error
  type (`crate::io::ErrorKind`); `From<std::io::Error>` conversions live in
  `sitas_core::io`.
- Incoherent half-migration stubs (e.g. `ShardedKv::start_with_placement_runtime`
  assigning a `Sharded` into a `ShardSet` field) were removed; the generic
  runtime lane is the `no_std` `kv`, not the std services.
- `ShardJoinHandle::from_raw` and the `Raw` variant are available in all
  feature combinations so `sitas-knytos` survives feature unification with
  std-enabled builds in one workspace.
- The root `src/`, `examples/`, `tests/` monolith reference tree was removed;
  examples/tests moved verbatim to `crates/sitas`.

## Remaining known gaps

- Knytos uses real kernel-backed joins; the placeholder `RawJoinHandle`
  remains available only for incomplete foreign backends. Thread creation is
  still infallible in `ShardRuntime`; Knytos rejects failure by aborting the
  domain rather than returning a partially started worker set.
- The portable public queue uses a spin lock for bookkeeping. Its retained ring
  slot convention provides N - 1 usable entries for capacity N; it is not a
  lock-free data-plane queue.
- A timed host shutdown may return running threads. Only joining its outcome
  confirms their termination; a non-returning poll cannot be safely preempted.
- Workspace executable profiles abort on panic; panic-catching tests run with
  unwinding and do not establish per-task isolation in those executables.
- `ShardRuntime` placement is advisory in `UnixRuntime` (no pinning); CPU
  pinning remains an explicit experiment in `sitas_core::sharded_executor`.
- The std `sharded_executor` is not generic over `ShardRuntime`; running the
  full shard-per-thread runtime on Knytos is future work layered on
  `ShardExecutor`.
- `charlotte_abi` (the in-memory ABI reference model) and `sitas-knytos`
  (the real backend) are validated separately; no combined conformance suite
  yet.

## Validation

Hardening pass checked on 2026-10-29:

- macOS workspace library/integration tests: **423 passed**; all-target build/test
  coverage and strict workspace Clippy also passed.
- Isolated `sitas-core` without default features: **10 passed**.
- Full `SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh`: passed, including **474**
  workspace tests (**392** core tests, including **49** `io_uring` tests), strict
  Clippy, the AArch64 default-lane guard, docs and every scripted example.
- AArch64 and x86-64 default-lane checks passed locally. The sibling Knytos
  `catten-user` source also passed a locked Cargo check for `aarch64-unknown-none`
  with build output isolated under `/private/tmp`.
- Formatting, documentation generation and whitespace checks passed. No fresh
  Knytos guest boot was performed; source compatibility is not boot evidence.


```sh
tools/linux-docker.sh                        # full lane: fmt, clippy, tests, bare-metal guard, doc, examples
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh sh -c \
  'cargo test -p sitas-core --features std uring && cargo run -p sitas --example os_uring'
cargo check -p sitas-core -p sitas-knytos --target aarch64-unknown-none
```
