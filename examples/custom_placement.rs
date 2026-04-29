use shardstar::placement::Placement;
use shardstar::{ShardId, ShardedKv, ShardedKvConfig};

#[derive(Debug, Clone, Copy)]
struct FirstBytePlacement;

impl Placement<str> for FirstBytePlacement {
    fn shard_for(&self, key: &str, shard_count: usize) -> ShardId {
        debug_assert!(shard_count > 0);

        let first_byte = key.as_bytes().first().copied().unwrap_or_default();
        ShardId((first_byte as usize) % shard_count)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = ShardedKv::start_with_placement(
        ShardedKvConfig::new(4).with_mailbox_capacity(16),
        FirstBytePlacement,
    )?;

    for (key, value) in [
        ("alpha", "one"),
        ("beta", "two"),
        ("gamma", "three"),
        ("delta", "four"),
    ] {
        kv.put(key, value)?;
        println!("{key:?} routed to {:?}", kv.shard_for_key(key));
    }

    println!("snapshots: {:?}", kv.shard_snapshots()?);
    println!("total keys: {}", kv.total_len()?);

    kv.stop()?;

    Ok(())
}
