use sitas::{ShardId, ShardedCounter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counter = ShardedCounter::start(4)?;

    counter.add_on_shard(ShardId(0), 5)?;
    counter.add_on_shard(ShardId(1), 7)?;
    counter.add_on_shard(ShardId(2), -2)?;

    for shard_idx in 0..counter.shard_count() {
        let shard = ShardId(shard_idx);
        println!("shard {shard_idx}: {}", counter.get_on_shard(shard)?);
    }

    println!("total: {}", counter.total()?);

    let delayed_total = counter.submit_total()?;
    println!("delayed total: {}", delayed_total.wait()?);
    println!("snapshots: {:?}", counter.shard_snapshots()?);

    counter.stop()?;

    Ok(())
}
