You are helping me create a new Rust project called `shardstar`.

The goal is to experiment with a Rust-native architecture inspired by Seastar, but not to clone Seastar directly.

Seastar’s valuable ideas are:

- one shard per core / worker thread
- shard-local state
- no shared mutable service state
- no mutexes around application service state
- explicit cross-shard communication
- message passing instead of shared-memory coordination
- request/reply semantics
- eventually: async I/O, reactor loops, scheduling, backpressure, CPU affinity

However, this Rust project should start much smaller.

The first version must not depend on Tokio, Glommio, Monoio, async runtimes, actor frameworks, or other third-party concurrency libraries.

The first version should implement the architectural kernel only:

- one OS thread per shard
- one mailbox per shard
- typed messages
- blocking request/reply using standard-library channels
- local service state owned by the shard thread
- key-based routing to shards
- clean shutdown
- tests
- a simple example program

The purpose of the first milestone is to prove the ownership and message-passing model, not to build a production async runtime.

Use only the Rust standard library in the first milestone.

No `unsafe`.

No Tokio.

No async/await.

No `Arc<Mutex<Service>>`.

No global mutable state.

No custom executor yet.

No networking yet.

No persistence yet.

No `io_uring` yet.

No CPU pinning yet.

No procedural macros yet.

The core invariant is:

    Only the owning shard may mutate its service state.
    All cross-shard interaction happens through typed messages.

The first concrete service should be a sharded key-value store.

Project name:

    shardstar

Suggested layout:

    shardstar/
      Cargo.toml
      src/
        lib.rs
        error.rs
        shard.rs
        placement.rs
        kv.rs
      examples/
        basic_kv.rs
      tests/
        kv_tests.rs

Use Rust edition 2021 or newer.

Do not use external crates in the first version.

===============================================================================
ARCHITECTURE
===============================================================================

The initial architecture is:

    ShardedKv
        owns N KvShardHandle values
        owns N JoinHandle values
        routes keys to shards using a placement function

    KvShardHandle
        owns a Sender<KvCommand>
        knows its ShardId

    Shard thread
        owns a Receiver<KvCommand>
        owns one KvService
        loops over incoming commands
        mutates KvService locally
        replies using one-shot std::sync::mpsc channels
        exits on Stop

    KvService
        owns HashMap<String, String>
        is never shared between threads
        is never protected by a mutex
        is mutated only by the shard thread that owns it

===============================================================================
PUBLIC TYPES
===============================================================================

Define:

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ShardId(pub usize);

Expose `ShardId` from the crate root.

Define an error type without external crates:

    #[derive(Debug)]
    pub enum ShardError {
        InvalidShardCount,
        InvalidShardId(usize),
        SendFailed,
        ReplyFailed,
        ShardStopped,
        ThreadJoinFailed,
    }

Implement:

    std::fmt::Display
    std::error::Error

for `ShardError`.

===============================================================================
PLACEMENT
===============================================================================

Create `src/placement.rs`.

Implement a simple hash-based placement function using the standard library:

    pub fn shard_for_hash<K: std::hash::Hash>(key: &K, shard_count: usize) -> ShardId;

Use:

    std::collections::hash_map::DefaultHasher

This is not meant to be a production-consistent hashing strategy. It is only the first routing mechanism.

Also consider adding a small placement trait, but do not over-engineer it.

A simple trait is acceptable if useful:

    pub trait Placement<K> {
        fn shard_for(&self, key: &K, shard_count: usize) -> ShardId;
    }

But if that complicates the first version, skip the trait and keep the function.

===============================================================================
KV SERVICE
===============================================================================

Create `src/kv.rs`.

Implement:

    struct KvService {
        map: HashMap<String, String>,
    }

Methods:

    impl KvService {
        fn new() -> Self;

        fn get(&mut self, key: String) -> Option<String>;

        fn put(&mut self, key: String, value: String);

        fn delete(&mut self, key: String) -> Option<String>;

        fn len(&self) -> usize;
    }

It is acceptable for `get` to take `String` and clone the returned value.

Do not expose references into shard-local state.

Do not return `&str` or `&String` from shard-local state.

All returned values crossing a shard boundary should be owned.

===============================================================================
COMMAND ENUM
===============================================================================

Define an internal command enum:

    enum KvCommand {
        Get {
            key: String,
            reply: std::sync::mpsc::Sender<Option<String>>,
        },
        Put {
            key: String,
            value: String,
            reply: std::sync::mpsc::Sender<()>,
        },
        Delete {
            key: String,
            reply: std::sync::mpsc::Sender<Option<String>>,
        },
        Len {
            reply: std::sync::mpsc::Sender<usize>,
        },
        Stop {
            reply: std::sync::mpsc::Sender<()>,
        },
    }

The command enum is private to the kv module unless there is a good reason to expose it.

===============================================================================
SHARD HANDLE
===============================================================================

Define:

    struct KvShardHandle {
        id: ShardId,
        sender: std::sync::mpsc::Sender<KvCommand>,
    }

Implement helper methods on `KvShardHandle` if useful:

    fn send_get(&self, key: String) -> Result<Option<String>, ShardError>;
    fn send_put(&self, key: String, value: String) -> Result<(), ShardError>;
    fn send_delete(&self, key: String) -> Result<Option<String>, ShardError>;
    fn send_len(&self) -> Result<usize, ShardError>;
    fn send_stop(&self) -> Result<(), ShardError>;

Each method should:

1. create a reply channel
2. send a command
3. wait for the reply
4. map send/receive failures to ShardError

Use blocking `recv()` for the first version.

This is intentional.

Do not call this async.

Do not pretend this is a Future.

===============================================================================
PUBLIC SHARDED KV API
===============================================================================

Define:

    pub struct ShardedKv {
        shards: Vec<KvShardHandle>,
        joins: Vec<std::thread::JoinHandle<()>>,
    }

Implement:

    impl ShardedKv {
        pub fn start(shard_count: usize) -> Result<Self, ShardError>;

        pub fn shard_count(&self) -> usize;

        pub fn shard_for_key(&self, key: &str) -> ShardId;

        pub fn put(
            &self,
            key: impl Into<String>,
            value: impl Into<String>,
        ) -> Result<(), ShardError>;

        pub fn get(
            &self,
            key: impl Into<String>,
        ) -> Result<Option<String>, ShardError>;

        pub fn delete(
            &self,
            key: impl Into<String>,
        ) -> Result<Option<String>, ShardError>;

        pub fn len_on_shard(
            &self,
            shard_id: ShardId,
        ) -> Result<usize, ShardError>;

        pub fn total_len(&self) -> Result<usize, ShardError>;

        pub fn stop(self) -> Result<(), ShardError>;
    }

Important design choices:

- `stop(self)` consumes the handle.
- The caller cannot use `ShardedKv` after stop.
- `stop` should send a Stop command to every shard.
- `stop` should then join every shard thread.
- If a shard thread panics or cannot be joined, return `ShardError::ThreadJoinFailed`.
- If a channel is disconnected, return a meaningful error.
- Starting with zero shards must return `ShardError::InvalidShardCount`.

Routing:

    key -> shard_for_hash(&key, shard_count) -> target shard

`put`, `get`, and `delete` should route to the shard owning the key.

`len_on_shard` should address a specific shard directly.

`total_len` should query all shards and sum the results.

===============================================================================
SHARD THREAD LOOP
===============================================================================

Each shard thread should roughly do:

    let mut service = KvService::new();

    loop {
        match receiver.recv() {
            Ok(KvCommand::Get { key, reply }) => {
                let value = service.get(key);
                let _ = reply.send(value);
            }
            Ok(KvCommand::Put { key, value, reply }) => {
                service.put(key, value);
                let _ = reply.send(());
            }
            Ok(KvCommand::Delete { key, reply }) => {
                let value = service.delete(key);
                let _ = reply.send(value);
            }
            Ok(KvCommand::Len { reply }) => {
                let _ = reply.send(service.len());
            }
            Ok(KvCommand::Stop { reply }) => {
                let _ = reply.send(());
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

Do not share `KvService` with the outside.

Do not use `Arc<Mutex<KvService>>`.

===============================================================================
CRATE ROOT
===============================================================================

In `src/lib.rs`, expose:

    pub mod error;
    pub mod shard;
    pub mod placement;
    pub mod kv;

    pub use error::ShardError;
    pub use shard::ShardId;

Optionally expose:

    pub use kv::ShardedKv;

===============================================================================
TESTS
===============================================================================

Add tests covering:

1. starting with zero shards fails

2. starting with one shard succeeds

3. starting with four shards succeeds

4. put/get one key

5. get missing key returns None

6. overwrite existing key

7. delete existing key

8. delete missing key returns None

9. many keys can be inserted and retrieved

10. keys distribute across shards

    This test should not depend on exact distribution.
    It can insert many keys and check that more than one shard has entries.

11. len_on_shard works

12. total_len works

13. stop joins all shard threads cleanly

14. repeated operations before stop work

Do not test exact hash shard placement values unless necessary, because DefaultHasher is not a stable external contract.

===============================================================================
EXAMPLE PROGRAM
===============================================================================

Create:

    examples/basic_kv.rs

Example:

    use shardstar::{ShardId, ShardedKv};

    fn main() -> Result<(), Box<dyn std::error::Error>> {
        let kv = ShardedKv::start(4)?;

        kv.put("alpha", "one")?;
        kv.put("beta", "two")?;
        kv.put("gamma", "three")?;

        for key in ["alpha", "beta", "gamma", "delta"] {
            let shard = kv.shard_for_key(key);
            let value = kv.get(key)?;
            println!("{key:?} is on shard {:?}, value = {:?}", shard, value);
        }

        for shard_idx in 0..kv.shard_count() {
            let shard = ShardId(shard_idx);
            let len = kv.len_on_shard(shard)?;
            println!("shard {shard_idx}: {len} keys");
        }

        println!("total keys: {}", kv.total_len()?);

        kv.stop()?;

        Ok(())
    }

===============================================================================
DOCUMENTATION
===============================================================================

Add crate-level documentation explaining the design.

The documentation should say:

- This project is inspired by Seastar’s shard-per-core/shared-nothing model.
- This first milestone is not an async runtime.
- This first milestone deliberately uses only the Rust standard library.
- The goal is to validate shard-local ownership and typed message passing.
- Application state is owned by a shard thread.
- Other threads interact with that state only by sending messages.
- No mutex protects the service state because the service state is not shared.
- Cross-shard values are owned values.
- Later milestones may add async I/O, custom executors, CPU pinning, backpressure, and runtime backends.

Also document what is deliberately missing:

- no async/await
- no non-blocking I/O
- no custom executor
- no network server
- no persistence
- no CPU pinning
- no scheduling classes
- no procedural macro service generation

===============================================================================
STYLE
===============================================================================

Use straightforward Rust.

Keep abstractions small.

Prefer explicit command enums over generic cleverness.

Avoid premature generalization.

Do not implement an actor framework.

Do not implement a custom async runtime.

Do not introduce third-party dependencies unless absolutely necessary.

Run:

    cargo fmt
    cargo test
    cargo clippy

if available.

===============================================================================
FUTURE ROADMAP
===============================================================================

Do not implement these now, but leave the code structured so they can be added later.

Milestone 2: Bounded mailboxes and visible backpressure

- Replace unbounded channels with sync_channel or another bounded mechanism.
- Add send failure / full queue handling.
- Possibly add non-blocking try-submit APIs.

Milestone 3: Generic sharded services

- Extract reusable infrastructure from ShardedKv.
- Define a generic Sharded<T> if it can be done cleanly.
- Avoid fighting the borrow checker too early.
- Keep the typed KV service as the reference example.

Milestone 4: Submitted call handles

- Add APIs that submit a request and return a handle.
- Example:
      let handle = kv.submit_get("alpha")?;
      let result = handle.wait()?;
- This gives future-like request/reply without async/await.
- Do not call it Future unless it implements std::future::Future.

Milestone 5: !Send shard-local service state

- Investigate constructing service state inside the shard thread.
- Demonstrate a service using Rc<RefCell<T>> internally.
- Ensure such state cannot cross shard boundaries.
- Preserve Send + 'static requirements for messages crossing shard boundaries.

Milestone 6: Runtime abstraction

- Separate shard transport and execution from service API.
- Keep the std blocking backend as the simplest backend.
- Later backends may use Tokio, Glommio, Monoio, or a custom executor.
- Do not introduce this abstraction before the basic design is stable.

Milestone 7: Async/reactor experiment

- Explore a local executor per shard.
- Explore std::future::Future, Waker, task queues, and cooperative polling.
- Add timers only after the executor model is clear.
- Do not add network I/O until the executor is reliable.

Milestone 8: CPU pinning

- Add optional CPU affinity.
- This may use an external crate later.
- Keep it feature-gated.

Milestone 9: Procedural macro service generation

- Add a separate crate:
      shardstar-macros
- Generate command enums and client stubs from a service trait.
- Example future target:

      #[sharded_service]
      trait AccountService {
          #[route(account_id)]
          fn get_balance(&mut self, account_id: AccountId) -> Result<Money>;

          #[route(account_id)]
          fn post_transaction(
              &mut self,
              account_id: AccountId,
              tx: Transaction,
          ) -> Result<()>;
      }

Milestone 10: I/O backend

- Add networking.
- Route requests to shards by key.
- Consider one listener per shard or a frontend acceptor that routes requests.
- Later investigate io_uring, epoll, kqueue, Glommio, or Monoio.

Milestone 11: Scheduling and resource classes

- Add concepts similar to Seastar scheduling groups.
- Separate foreground requests from background work.
- Add metrics for queue length and latency.

===============================================================================
IMPORTANT PHILOSOPHY
===============================================================================

This project is not trying to reimplement Seastar in Rust line by line.

It is trying to ask:

    What would a Seastar-inspired shared-nothing runtime look like
    if designed around Rust’s ownership model from the start?

Therefore:

- prefer typed messages to untyped actor mailboxes
- prefer owned values across shard boundaries
- prefer local mutation inside a shard
- prefer compile-time boundaries where possible
- prefer simple explicit code over clever generic abstractions
- defer async/runtime complexity until the ownership model is solid

The first deliverable should be boring, small, and correct.

Build the smallest useful kernel first.



