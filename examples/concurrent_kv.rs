//! Exercises the blocking key-value API from several OS threads.
//!
//! The service state is still shard-owned; concurrency here means many callers
//! enqueue typed commands, not that the map itself is shared behind a mutex.
use std::thread;

use sitas::ShardedKv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = ShardedKv::start(4)?;
    let caller_count = 8;
    let keys_per_caller = 100;

    thread::scope(|scope| {
        for caller_idx in 0..caller_count {
            let kv = &kv;

            scope.spawn(move || {
                for key_idx in 0..keys_per_caller {
                    let key = format!("caller-{caller_idx}-key-{key_idx}");
                    let value = format!("value-{caller_idx}-{key_idx}");

                    kv.put(&key, &value).expect("put should succeed");
                    let stored = kv.get(&key).expect("get should succeed");

                    assert_eq!(stored, Some(value));
                }
            });
        }
    });

    println!("total keys: {}", kv.total_len()?);

    kv.stop()?;

    Ok(())
}
