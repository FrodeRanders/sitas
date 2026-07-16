//! basic_kv on CharlotteOS — exercises sitas ShardedKv via ShardRuntime.
//!
//! Creates a single-shard KV using the given runtime, sets three keys, reads
//! one back, and writes the result count to the result page.

use core::ptr;

use crate::kv::{ShardedKv, ShardedKvConfig};
use crate::shard_runtime::ShardRuntime;

/// Runs the CharlotteOS KV smoke test and writes its result to `result_page`.
///
/// # Safety
///
/// `result_page` must be valid for a volatile `u32` write for the duration of
/// this call.
pub unsafe fn basic_kv_test<R: ShardRuntime + ?Sized>(runtime: &R, result_page: *mut u32) {
    let config = ShardedKvConfig::new(2);
    let kv = match ShardedKv::start_with_runtime(config, runtime) {
        Ok(kv) => kv,
        Err(_) => {
            unsafe { ptr::write_volatile(result_page, 0xDEAD) };
            return;
        }
    };

    // Basic set operations.
    if kv.put("alpha", "one").is_err()
        || kv.put("beta", "two").is_err()
        || kv.put("gamma", "three").is_err()
    {
        unsafe { ptr::write_volatile(result_page, 0xDEAD) };
        return;
    }

    // Read back: get("alpha") should return "one".
    match kv.get("alpha") {
        Ok(Some(value)) if value == "one" => {
            let total = kv.total_len().unwrap_or(0);
            unsafe { ptr::write_volatile(result_page, total as u32) };
        }
        _ => {
            unsafe { ptr::write_volatile(result_page, 0xDEAD) };
        }
    }
}
