//! Uses submit/wait-later key-value operations.
//!
//! Submit handles decouple enqueueing from waiting, which is the std-only
//! precursor to awaiting replies on the custom executor.
mod support;
use sitas::ShardedKv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("submit_kv");
    let kv = ShardedKv::start(4)?;

    let alpha = kv.submit_put("alpha", "one")?;
    let beta = kv.submit_put("beta", "two")?;

    alpha.wait()?;
    beta.wait()?;

    let alpha = kv.submit_get("alpha")?;
    let beta = kv.submit_get("beta")?;
    let total = kv.submit_total_len()?;

    println!("alpha = {:?}", alpha.wait()?);
    println!("beta = {:?}", beta.wait()?);
    println!("total keys = {}", total.wait()?);

    kv.stop()?;

    Ok(())
}
