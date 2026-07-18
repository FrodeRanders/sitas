use sitas::placement::{HashPlacement, Placement, shard_for_hash};
use sitas::{ShardId, ShardedKv};

#[test]
fn shard_for_hash_returns_shard_inside_range() {
    for shard_count in 1..16 {
        for idx in 0..100 {
            let shard = shard_for_hash(&format!("key-{idx}"), shard_count);

            assert!(shard.0 < shard_count);
        }
    }
}

#[test]
fn shard_for_hash_is_repeatable_for_same_key_and_shard_count() {
    let first = shard_for_hash(&"alpha", 8);
    let second = shard_for_hash(&"alpha", 8);

    assert_eq!(first, second);
}

#[test]
fn hash_placement_matches_shard_for_hash() {
    let placement = HashPlacement;

    for shard_count in 1..16 {
        for idx in 0..100 {
            let key = format!("key-{idx}");

            assert_eq!(
                placement.shard_for(&key, shard_count),
                shard_for_hash(&key, shard_count)
            );
        }
    }
}

#[test]
fn sharded_kv_uses_public_placement_function() {
    let kv = ShardedKv::start(4).unwrap();
    let key = "alpha";

    assert_eq!(
        kv.shard_for_key(key),
        shard_for_hash(&key, kv.shard_count())
    );

    kv.stop().unwrap();
}

#[test]
fn shard_id_debug_is_useful() {
    assert_eq!(format!("{:?}", ShardId(3)), "ShardId(3)");
}
