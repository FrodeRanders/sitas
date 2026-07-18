//! Minimal key-value service usage.
//!
//! The example uses blocking calls on purpose: this is the baseline service
//! API before async reply handles or the custom executor enter the picture.
mod support;
use sitas::{ShardId, ShardedKv};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("basic_kv");
    let kv = ShardedKv::start(4)?;

    kv.put("alpha", "one")?;
    kv.put("beta", "two")?;
    kv.put("gamma", "three")?;
    let updated = kv.compare_and_put("alpha", Some("one".to_string()), "updated")?;
    println!("alpha compare-and-put updated: {updated}");
    println!(
        "epsilon get-or-put: {:?}",
        kv.get_or_put("epsilon", "five")?
    );

    for key in ["alpha", "beta", "gamma", "delta", "epsilon"] {
        let shard = kv.shard_for_key(key);
        let value = kv.get(key)?;
        println!("{key:?} is on shard {:?}, value = {:?}", shard, value);
    }

    for shard_idx in 0..kv.shard_count() {
        let shard = ShardId(shard_idx);
        let len = kv.len_on_shard(shard)?;
        println!("shard {shard_idx}: {len} keys");
    }

    println!("snapshots: {:?}", kv.shard_snapshots()?);
    println!(
        "selected values: {:?}",
        kv.get_many(["gamma", "alpha", "missing"])?
    );
    println!("all keys: {:?}", kv.all_keys()?);
    println!("deleted values: {:?}", kv.delete_many(["delta", "beta"])?);
    println!("all keys after delete: {:?}", kv.all_keys()?);
    println!("total keys: {}", kv.total_len()?);

    kv.stop()?;

    Ok(())
}
