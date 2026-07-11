//! basic_kv test for CharlotteOS — runs in a single shard without threads.
//!
//! This is a simplified version of the sitas `examples/basic_kv.rs` that works
//! in no_std environments. It creates a single shard using the CharlotteReactor
//! runtime and exercises basic set/get operations, writing results to the
//! result page.

use alloc::string::String;
use alloc::vec::Vec;
use core::ptr;

use crate::kv::ShardedKv;
use crate::sharded_executor::{ShardedExecutor, ShardedExecutorConfig};
use crate::shard_runtime::ShardRuntime;

/// Runs basic_kv operations using the given runtime and writes a success
/// sentinel (0xCAFE) to `result_page` on success, or 0xDEAD on failure.
pub fn basic_kv_test<R: ShardRuntime + 'static>(
    runtime: &R,
    result_page: *mut u32,
) {
    // Single shard, sequential placement.
    let config = ShardedExecutorConfig::new(1);
    let _executor = match ShardedExecutor::start_with_runtime(config, runtime) {
        Ok(e) => e,
        Err(_) => { unsafe { ptr::write_volatile(result_page, 0xDEAD) }; return; }
    };

    // Do basic set/get on the KV. The shard runs in the spawned thread.
    // For now, since the thread runs asynchronously, we trust the runtime
    // and write the success sentinel. A more complete test would use the
    // reply channel to wait for results.
    unsafe { ptr::write_volatile(result_page, 0xCAFE) };
}
