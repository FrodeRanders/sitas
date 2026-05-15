# AGENTS.md

## Project purpose

This repository explores a Rust-native, Seastar-inspired shard-per-core runtime and service model. The project is not a line-by-line Seastar clone. It asks what a shared-nothing runtime should look like when designed around Rust ownership, type boundaries, explicit message passing, and isolated unsafe code.

The original baseline is a dependency-free, standard-library sharded service kernel. The active exploration adds a custom executor, Unix OS primitives, readiness-based I/O, TCP helpers, sharded executors, shard-local state, observability snapshots, CPU placement, and experimental Linux `io_uring` support.

Before changing runtime, executor, sharding, I/O, lifecycle, or service-state code, read `docs/architecture.md`.

## Architectural invariants

Preserve these unless the change is explicitly about revising the architecture and updates `docs/architecture.md` at the same time.

1. Only the owning shard may mutate its application service state.
2. Application service state must not be hidden behind `Arc<Mutex<Service>>` as the normal programming model.
3. Cross-shard interaction must be explicit: typed messages, typed submitter calls, or typed runtime handles.
4. Values crossing shard boundaries must be owned values. Use explicit `Send + 'static` boundaries where the runtime requires cross-thread transfer.
5. References to shard-local state must not escape the synchronous access closure and must not cross an `.await`.
6. Work remains on its assigned shard unless explicitly submitted to another shard.
7. Observability should return owned snapshots, not borrowed references into runtime or shard-local internals.
8. Runtime internals may use synchronization for task bookkeeping, wakers, reply state, and lifecycle management. That does not weaken the shared-nothing application-state model.
9. Unsafe code, where unavoidable, must be isolated behind small safe APIs and documented with the invariant it relies on.

## Current implementation tracks

### Stable std-only baseline

The baseline validates the ownership model with:

- one OS thread per shard;
- bounded standard-library mailboxes;
- typed command/reply APIs;
- shard-local service state;
- clean startup and shutdown;
- blocking calls, `try_*` variants, and submit/wait-later reply handles;
- custom std-only one-shot replies that can also be awaited by the custom executor;
- `ShardedKv` and `ShardedCounter` as concrete services;
- runtime and service snapshots using owned values.

### Non-std runtime exploration

The exploration branch contains:

- direct Unix FFI in `os`;
- a small custom single-threaded executor;
- timers, timeouts, cooperative stop tokens, task scopes, and join handles;
- readiness futures for non-blocking file descriptors;
- TCP accept/connect/read/write/copy helpers;
- server helpers for fixed-count, idle-timeout, and stoppable accept loops;
- `ShardedExecutor` with one executor/reactor per shard thread;
- explicit cross-shard async submission via `ShardedSubmitter`;
- `ShardLocal<T>` for one owned value per executor shard;
- snapshot-based observability for tasks, shards, and runtime state;
- Linux CPU affinity experiments;
- experimental Linux `io_uring` primitives and dispatcher lifecycle tracking.

## Coding rules

- Use stable Rust and edition 2024 unless the project deliberately documents otherwise.
- Keep the standard-library baseline clean and understandable.
- Do not introduce Tokio, Glommio, Monoio, async-std, actor frameworks, or other runtime dependencies casually.
- Do not replace the project’s custom runtime path with a third-party runtime without an explicit architectural decision.
- Do not add dependencies for simple functionality that can reasonably be implemented with the standard library in this experiment.
- Prefer typed command enums and typed service APIs over unstructured actor mailboxes.
- Prefer small, explicit abstractions over generic machinery that obscures ownership or lifetimes.
- Avoid premature generalization of `Sharded<T>` if the concrete service pattern is clearer.
- Keep blocking std-only APIs clearly distinct from awaitable custom-executor APIs.
- Do not call something async unless it is actually integrated with `Future`/`Waker` semantics.
- When adding runtime features, include tests that cover cancellation/drop behavior, shutdown, and error paths.

## Unsafe and FFI rules

- New unsafe code requires a local safety comment explaining the exact invariant.
- Keep FFI declarations in `os` or a similarly isolated module.
- With Rust edition 2024, all foreign function blocks must be written as `unsafe extern "C" { ... }`.
- FFI call sites must remain inside small wrappers that convert raw OS results into Rust types and errors.
- Do not expose raw file descriptor ownership ambiguously. Make ownership, borrowing, and close/drop responsibilities explicit.
- Owned buffers submitted to kernel I/O must remain alive until the kernel completion is observed or safely discarded through the dispatcher lifecycle.

## Testing and validation

Run the narrowest relevant tests first, then broaden.

Preferred commands:

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```

When touching platform-specific code, also run the relevant platform validation if available:

```bash
cargo test --all-targets
SITAS_DOCKER_IO_URING=1 tools/linux-docker.sh
```

For Linux-only features such as CPU affinity or `io_uring`, keep tests gated so macOS and other Unix platforms report unsupported behavior honestly rather than failing spuriously.

## Documentation responsibilities

Update `docs/architecture.md` when a change affects:

- shard ownership rules;
- executor semantics;
- task lifecycle, cancellation, panic, or shutdown behavior;
- reply-handle behavior;
- cross-shard submission;
- shard-local state access;
- readiness, TCP, or `io_uring` lifecycle;
- CPU placement semantics;
- observability snapshot fields;
- non-goals or roadmap direction.

Keep `AGENTS.md` operational and concise. Put detailed design explanations in `docs/architecture.md`.

## Current non-goals

Do not implement these unless explicitly requested by the current task:

- persistence;
- procedural macro service generation;
- production-grade `io_uring` integration with the sharded executor;
- portable `kqueue` support;
- general load balancing;
- scheduling/resource classes;
- distributed clustering;
- replacing the custom runtime with a third-party runtime.

CPU placement exists as an explicit experimental runtime request on Linux. Do not treat it as a finished portable scheduling policy.

## Design direction

The project should grow in this order:

1. Keep the shared-nothing service model correct.
2. Keep runtime and executor semantics observable and testable.
3. Add async/runtime features in small layers.
4. Preserve safe public APIs.
5. Isolate unsafe and OS-specific code.
6. Add performance-oriented mechanisms only after the semantics are clear.

The desired result is a Rust-native shared-nothing runtime architecture: typed boundaries, shard-local mutation, explicit cross-shard transfer, owned snapshots, and a minimal dependency footprint.
