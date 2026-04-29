use shardstar::placement::Placement;
use shardstar::{RuntimeSnapshot, ShardError, ShardId, ShardSnapshot, ShardedKv, ShardedKvConfig};
use std::error::Error;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
struct FirstShardPlacement;

impl Placement<str> for FirstShardPlacement {
    fn shard_for(&self, _key: &str, _shard_count: usize) -> ShardId {
        ShardId(0)
    }
}

#[test]
fn starting_with_zero_shards_fails() {
    let result = ShardedKv::start(0);

    assert_eq!(result.unwrap_err(), ShardError::InvalidShardCount);
}

#[test]
fn starting_with_one_shard_succeeds() {
    let kv = ShardedKv::start(1).unwrap();

    assert_eq!(kv.shard_count(), 1);
    assert_eq!(kv.mailbox_capacity(), shardstar::DEFAULT_MAILBOX_CAPACITY);

    kv.stop().unwrap();
}

#[test]
fn starting_with_four_shards_succeeds() {
    let kv = ShardedKv::start(4).unwrap();

    assert_eq!(kv.shard_count(), 4);

    kv.stop().unwrap();
}

#[test]
fn starting_with_config_succeeds() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    assert_eq!(kv.shard_count(), 2);
    assert_eq!(kv.mailbox_capacity(), 4);
    assert_eq!(
        kv.runtime_snapshot(),
        RuntimeSnapshot {
            shard_count: 2,
            mailbox_capacity: 4,
            stopped: false,
        }
    );

    kv.stop().unwrap();
}

#[test]
fn starting_with_custom_placement_routes_keys_through_strategy() {
    let kv = ShardedKv::start_with_placement(
        ShardedKvConfig::new(4).with_mailbox_capacity(4),
        FirstShardPlacement,
    )
    .unwrap();

    assert_eq!(kv.shard_for_key("alpha"), ShardId(0));
    assert_eq!(kv.shard_for_key("beta"), ShardId(0));

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(kv.len_on_shard(ShardId(0)).unwrap(), 2);
    assert_eq!(kv.total_len().unwrap(), 2);

    for shard_idx in 1..kv.shard_count() {
        assert_eq!(kv.len_on_shard(ShardId(shard_idx)).unwrap(), 0);
    }

    kv.stop().unwrap();
}

#[test]
fn starting_with_zero_mailbox_capacity_fails() {
    let result = ShardedKv::start_with_config(ShardedKvConfig::new(1).with_mailbox_capacity(0));

    assert_eq!(result.unwrap_err(), ShardError::InvalidMailboxCapacity);
}

#[test]
fn put_and_get_one_key() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();

    assert_eq!(kv.get("alpha").unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn try_put_and_try_get_work_when_mailbox_has_capacity() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(1)).unwrap();

    kv.try_put("alpha", "one").unwrap();

    assert_eq!(kv.try_get("alpha").unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn get_many_returns_owned_values_in_input_order() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();
    kv.put("gamma", "three").unwrap();

    assert_eq!(
        kv.get_many(["gamma", "missing", "alpha", "beta"]).unwrap(),
        vec![
            ("gamma".to_string(), Some("three".to_string())),
            ("missing".to_string(), None),
            ("alpha".to_string(), Some("one".to_string())),
            ("beta".to_string(), Some("two".to_string())),
        ]
    );

    kv.stop().unwrap();
}

#[test]
fn submit_get_many_can_wait_later() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let reply = kv.submit_get_many(["beta", "alpha", "missing"]).unwrap();

    assert_eq!(
        reply.wait().unwrap(),
        vec![
            ("beta".to_string(), Some("two".to_string())),
            ("alpha".to_string(), Some("one".to_string())),
            ("missing".to_string(), None),
        ]
    );

    kv.stop().unwrap();
}

#[test]
fn try_get_many_and_try_submit_get_many_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(3).with_mailbox_capacity(4)).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(
        kv.try_get_many(["alpha", "missing"]).unwrap(),
        vec![
            ("alpha".to_string(), Some("one".to_string())),
            ("missing".to_string(), None),
        ]
    );

    let reply = kv.try_submit_get_many(["beta", "alpha"]).unwrap();
    assert_eq!(
        reply.wait().unwrap(),
        vec![
            ("beta".to_string(), Some("two".to_string())),
            ("alpha".to_string(), Some("one".to_string())),
        ]
    );

    kv.stop().unwrap();
}

#[test]
fn submit_put_and_submit_get_can_wait_later() {
    let kv = ShardedKv::start(2).unwrap();

    let put = kv.submit_put("alpha", "one").unwrap();
    let get = kv.submit_get("alpha").unwrap();

    put.wait().unwrap();
    assert_eq!(get.wait().unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn try_submit_put_and_try_submit_get_can_wait_later() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(8)).unwrap();

    let put = kv.try_submit_put("alpha", "one").unwrap();
    let get = kv.try_submit_get("alpha").unwrap();

    put.wait().unwrap();
    assert_eq!(get.wait().unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn reply_try_wait_and_wait_timeout_work() {
    let kv = ShardedKv::start(2).unwrap();

    let put = kv.submit_put("alpha", "one").unwrap();

    match put.try_wait().unwrap() {
        Some(()) => {}
        None => put.wait_timeout(Duration::from_secs(1)).unwrap(),
    }

    let get = kv.submit_get("alpha").unwrap();
    assert_eq!(
        get.wait_timeout(Duration::from_secs(1)).unwrap(),
        Some("one".to_string())
    );

    kv.stop().unwrap();
}

#[test]
fn get_missing_key_returns_none() {
    let kv = ShardedKv::start(2).unwrap();

    assert_eq!(kv.get("missing").unwrap(), None);

    kv.stop().unwrap();
}

#[test]
fn overwrite_existing_key() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("alpha", "updated").unwrap();

    assert_eq!(kv.get("alpha").unwrap(), Some("updated".to_string()));
    assert_eq!(kv.total_len().unwrap(), 1);

    kv.stop().unwrap();
}

#[test]
fn compare_and_put_inserts_when_expected_absent_matches() {
    let kv = ShardedKv::start(2).unwrap();

    assert!(kv.compare_and_put("alpha", None, "one").unwrap());
    assert_eq!(kv.get("alpha").unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn compare_and_put_replaces_only_when_expected_value_matches() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();

    assert!(!kv
        .compare_and_put("alpha", Some("wrong".to_string()), "two")
        .unwrap());
    assert_eq!(kv.get("alpha").unwrap(), Some("one".to_string()));
    assert!(kv
        .compare_and_put("alpha", Some("one".to_string()), "two")
        .unwrap());
    assert_eq!(kv.get("alpha").unwrap(), Some("two".to_string()));

    kv.stop().unwrap();
}

#[test]
fn get_or_put_inserts_absent_key_and_returns_existing_afterward() {
    let kv = ShardedKv::start(2).unwrap();

    assert_eq!(kv.get_or_put("alpha", "one").unwrap(), "one".to_string());
    assert_eq!(kv.get_or_put("alpha", "two").unwrap(), "one".to_string());
    assert_eq!(kv.get("alpha").unwrap(), Some("one".to_string()));

    kv.stop().unwrap();
}

#[test]
fn try_and_submit_compare_and_put_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    assert!(kv.try_compare_and_put("alpha", None, "one").unwrap());

    let reply = kv
        .submit_compare_and_put("alpha", Some("one".to_string()), "two")
        .unwrap();
    assert!(reply.wait_timeout(Duration::from_secs(1)).unwrap());
    assert_eq!(kv.get("alpha").unwrap(), Some("two".to_string()));

    let reply = kv
        .try_submit_compare_and_put("alpha", Some("two".to_string()), "three")
        .unwrap();
    assert!(reply.wait().unwrap());
    assert_eq!(kv.get("alpha").unwrap(), Some("three".to_string()));

    kv.stop().unwrap();
}

#[test]
fn try_and_submit_get_or_put_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    assert_eq!(
        kv.try_get_or_put("alpha", "one").unwrap(),
        "one".to_string()
    );

    let reply = kv.submit_get_or_put("alpha", "two").unwrap();
    assert_eq!(
        reply.wait_timeout(Duration::from_secs(1)).unwrap(),
        "one".to_string()
    );

    let reply = kv.try_submit_get_or_put("beta", "three").unwrap();
    assert_eq!(reply.wait().unwrap(), "three".to_string());
    assert_eq!(kv.get("beta").unwrap(), Some("three".to_string()));

    kv.stop().unwrap();
}

#[test]
fn delete_existing_key() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();

    assert_eq!(kv.delete("alpha").unwrap(), Some("one".to_string()));
    assert_eq!(kv.get("alpha").unwrap(), None);

    kv.stop().unwrap();
}

#[test]
fn try_delete_works_when_mailbox_has_capacity() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(1)).unwrap();

    kv.put("alpha", "one").unwrap();

    assert_eq!(kv.try_delete("alpha").unwrap(), Some("one".to_string()));
    assert_eq!(kv.get("alpha").unwrap(), None);

    kv.stop().unwrap();
}

#[test]
fn submit_delete_can_wait_later() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();
    let delete = kv.submit_delete("alpha").unwrap();

    assert_eq!(delete.wait().unwrap(), Some("one".to_string()));
    assert_eq!(kv.get("alpha").unwrap(), None);

    kv.stop().unwrap();
}

#[test]
fn delete_many_returns_previous_values_in_input_order() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();
    kv.put("gamma", "three").unwrap();

    assert_eq!(
        kv.delete_many(["gamma", "missing", "alpha", "beta"])
            .unwrap(),
        vec![
            ("gamma".to_string(), Some("three".to_string())),
            ("missing".to_string(), None),
            ("alpha".to_string(), Some("one".to_string())),
            ("beta".to_string(), Some("two".to_string())),
        ]
    );
    assert_eq!(kv.total_len().unwrap(), 0);

    kv.stop().unwrap();
}

#[test]
fn submit_delete_many_can_wait_later() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let reply = kv.submit_delete_many(["beta", "alpha", "missing"]).unwrap();

    assert_eq!(
        reply.wait().unwrap(),
        vec![
            ("beta".to_string(), Some("two".to_string())),
            ("alpha".to_string(), Some("one".to_string())),
            ("missing".to_string(), None),
        ]
    );
    assert_eq!(kv.total_len().unwrap(), 0);

    kv.stop().unwrap();
}

#[test]
fn try_delete_many_and_try_submit_delete_many_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(3).with_mailbox_capacity(4)).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();
    kv.put("gamma", "three").unwrap();

    assert_eq!(
        kv.try_delete_many(["alpha", "missing"]).unwrap(),
        vec![
            ("alpha".to_string(), Some("one".to_string())),
            ("missing".to_string(), None),
        ]
    );

    let reply = kv.try_submit_delete_many(["gamma", "beta"]).unwrap();
    assert_eq!(
        reply.wait().unwrap(),
        vec![
            ("gamma".to_string(), Some("three".to_string())),
            ("beta".to_string(), Some("two".to_string())),
        ]
    );
    assert_eq!(kv.total_len().unwrap(), 0);

    kv.stop().unwrap();
}

#[test]
fn delete_missing_key_returns_none() {
    let kv = ShardedKv::start(2).unwrap();

    assert_eq!(kv.delete("missing").unwrap(), None);

    kv.stop().unwrap();
}

#[test]
fn many_keys_can_be_inserted_and_retrieved() {
    let kv = ShardedKv::start(4).unwrap();

    for idx in 0..1_000 {
        kv.put(format!("key-{idx}"), format!("value-{idx}"))
            .unwrap();
    }

    for idx in 0..1_000 {
        assert_eq!(
            kv.get(format!("key-{idx}")).unwrap(),
            Some(format!("value-{idx}"))
        );
    }

    assert_eq!(kv.total_len().unwrap(), 1_000);

    kv.stop().unwrap();
}

#[test]
fn keys_distribute_across_shards() {
    let kv = ShardedKv::start(4).unwrap();

    for idx in 0..200 {
        kv.put(format!("key-{idx}"), format!("value-{idx}"))
            .unwrap();
    }

    let occupied_shards = (0..kv.shard_count())
        .filter(|shard_idx| kv.len_on_shard(ShardId(*shard_idx)).unwrap() > 0)
        .count();

    assert!(occupied_shards > 1);

    kv.stop().unwrap();
}

#[test]
fn len_on_shard_works() {
    let kv = ShardedKv::start(1).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(kv.len_on_shard(ShardId(0)).unwrap(), 2);

    kv.stop().unwrap();
}

#[test]
fn try_len_methods_work_when_mailboxes_have_capacity() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(1)).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let total_from_shards = (0..kv.shard_count())
        .map(|idx| kv.try_len_on_shard(ShardId(idx)).unwrap())
        .sum::<usize>();

    assert_eq!(total_from_shards, 2);
    assert_eq!(kv.try_total_len().unwrap(), 2);

    kv.stop().unwrap();
}

#[test]
fn submit_len_methods_can_wait_later() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let shard_replies = (0..kv.shard_count())
        .map(|idx| kv.submit_len_on_shard(ShardId(idx)).unwrap())
        .collect::<Vec<_>>();
    let total = kv.submit_total_len().unwrap();

    let total_from_shards = shard_replies
        .into_iter()
        .map(|reply| reply.wait().unwrap())
        .sum::<usize>();

    assert_eq!(total_from_shards, 2);
    assert_eq!(total.wait().unwrap(), 2);

    kv.stop().unwrap();
}

#[test]
fn total_len_reply_can_wait_with_timeout() {
    let kv = ShardedKv::start(2).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let total = kv.submit_total_len().unwrap();

    assert_eq!(total.wait_timeout(Duration::from_secs(1)).unwrap(), 2);

    kv.stop().unwrap();
}

#[test]
fn shard_snapshots_report_lengths_in_shard_order() {
    let kv = ShardedKv::start(4).unwrap();

    for idx in 0..100 {
        kv.put(format!("key-{idx}"), format!("value-{idx}"))
            .unwrap();
    }

    let snapshots = kv.shard_snapshots().unwrap();

    assert_eq!(snapshots.len(), kv.shard_count());
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.shard_id)
            .collect::<Vec<_>>(),
        vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)]
    );
    assert_eq!(
        snapshots.iter().map(|snapshot| snapshot.len).sum::<usize>(),
        100
    );

    kv.stop().unwrap();
}

#[test]
fn try_and_submit_shard_snapshots_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    let snapshots = kv.try_shard_snapshots().unwrap();
    let submitted = kv.submit_shard_snapshots().unwrap();

    assert_eq!(
        snapshots.iter().map(|snapshot| snapshot.len).sum::<usize>(),
        2
    );
    assert_eq!(
        submitted
            .wait_timeout(Duration::from_secs(1))
            .unwrap()
            .iter()
            .map(|snapshot| snapshot.len)
            .sum::<usize>(),
        2
    );

    kv.stop().unwrap();
}

#[test]
fn keys_on_shard_returns_sorted_owned_keys() {
    let kv = ShardedKv::start(1).unwrap();

    kv.put("gamma", "three").unwrap();
    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(
        kv.keys_on_shard(ShardId(0)).unwrap(),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );

    kv.stop().unwrap();
}

#[test]
fn all_keys_returns_sorted_owned_keys() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("gamma", "three").unwrap();
    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(
        kv.all_keys().unwrap(),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );

    kv.stop().unwrap();
}

#[test]
fn try_and_submit_all_keys_work() {
    let kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    kv.put("gamma", "three").unwrap();
    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();

    assert_eq!(
        kv.try_all_keys().unwrap(),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );
    assert_eq!(
        kv.submit_all_keys()
            .unwrap()
            .wait_timeout(Duration::from_secs(1))
            .unwrap(),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );

    kv.stop().unwrap();
}

#[test]
fn keys_on_invalid_shard_fails() {
    let kv = ShardedKv::start(1).unwrap();

    assert_eq!(
        kv.keys_on_shard(ShardId(99)).unwrap_err(),
        ShardError::InvalidShardId(99)
    );
    assert_eq!(
        kv.try_submit_keys_on_shard(ShardId(99)).unwrap_err(),
        ShardError::InvalidShardId(99)
    );

    kv.stop().unwrap();
}

#[test]
fn shard_snapshot_is_an_owned_value() {
    let snapshot = ShardSnapshot {
        shard_id: ShardId(7),
        len: 11,
    };

    assert_eq!(snapshot.shard_id, ShardId(7));
    assert_eq!(snapshot.len, 11);
    assert_eq!(
        format!("{snapshot:?}"),
        "ShardSnapshot { shard_id: ShardId(7), len: 11 }"
    );
}

#[test]
fn len_on_invalid_shard_fails() {
    let kv = ShardedKv::start(1).unwrap();

    assert_eq!(
        kv.len_on_shard(ShardId(99)).unwrap_err(),
        ShardError::InvalidShardId(99)
    );

    kv.stop().unwrap();
}

#[test]
fn shard_errors_have_display_messages_and_no_sources() {
    let cases = [
        (
            ShardError::InvalidShardCount,
            "shard count must be greater than zero",
        ),
        (ShardError::InvalidShardId(7), "invalid shard id: 7"),
        (
            ShardError::InvalidMailboxCapacity,
            "mailbox capacity must be greater than zero",
        ),
        (ShardError::MailboxFull, "shard mailbox is full"),
        (ShardError::SendFailed, "failed to send command to shard"),
        (
            ShardError::ReplyFailed,
            "failed to receive reply from shard",
        ),
        (
            ShardError::ReplyTimeout,
            "timed out waiting for shard reply",
        ),
        (ShardError::ShardStopped, "shard has stopped"),
        (ShardError::ThreadJoinFailed, "failed to join shard thread"),
    ];

    for (error, message) in cases {
        assert_eq!(error.to_string(), message);
        assert!(error.source().is_none());
    }
}

#[test]
fn total_len_works() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.put("beta", "two").unwrap();
    kv.put("gamma", "three").unwrap();

    assert_eq!(kv.total_len().unwrap(), 3);

    kv.stop().unwrap();
}

#[test]
fn stop_joins_all_shard_threads_cleanly() {
    let kv = ShardedKv::start(4).unwrap();

    kv.stop().unwrap();
}

#[test]
fn shutdown_marks_runtime_snapshot_stopped_and_is_idempotent() {
    let mut kv =
        ShardedKv::start_with_config(ShardedKvConfig::new(2).with_mailbox_capacity(4)).unwrap();

    kv.put("alpha", "one").unwrap();
    kv.shutdown().unwrap();
    kv.shutdown().unwrap();

    assert_eq!(
        kv.runtime_snapshot(),
        RuntimeSnapshot {
            shard_count: 2,
            mailbox_capacity: 4,
            stopped: true,
        }
    );
    assert_eq!(kv.get("alpha"), Err(ShardError::ShardStopped));
    assert_eq!(kv.try_total_len(), Err(ShardError::ShardStopped));
}

#[test]
fn dropping_store_without_stop_shuts_down_shards() {
    let kv = ShardedKv::start(4).unwrap();

    kv.put("alpha", "one").unwrap();
}

#[test]
fn repeated_operations_before_stop_work() {
    let kv = ShardedKv::start(4).unwrap();

    for idx in 0..100 {
        let key = format!("key-{idx}");
        kv.put(&key, "initial").unwrap();
        kv.put(&key, "updated").unwrap();
        assert_eq!(kv.get(&key).unwrap(), Some("updated".to_string()));
        assert_eq!(kv.delete(&key).unwrap(), Some("updated".to_string()));
        assert_eq!(kv.get(&key).unwrap(), None);
    }

    assert_eq!(kv.total_len().unwrap(), 0);

    kv.stop().unwrap();
}

#[test]
fn concurrent_callers_can_use_the_store() {
    let kv = ShardedKv::start(4).unwrap();
    let thread_count = 8;
    let keys_per_thread = 100;

    thread::scope(|scope| {
        for thread_idx in 0..thread_count {
            let kv = &kv;

            scope.spawn(move || {
                for key_idx in 0..keys_per_thread {
                    let key = format!("thread-{thread_idx}-key-{key_idx}");
                    let value = format!("value-{thread_idx}-{key_idx}");

                    kv.put(&key, &value).unwrap();
                    assert_eq!(kv.get(&key).unwrap(), Some(value));
                }
            });
        }
    });

    assert_eq!(kv.total_len().unwrap(), thread_count * keys_per_thread);

    kv.stop().unwrap();
}

#[test]
fn compare_and_put_allows_only_one_absent_key_winner() {
    let kv = ShardedKv::start(4).unwrap();
    let contender_count = 16;

    let wins = thread::scope(|scope| {
        let handles = (0..contender_count)
            .map(|idx| {
                let kv = &kv;
                scope.spawn(move || {
                    kv.compare_and_put("leader", None, format!("candidate-{idx}"))
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count()
    });

    assert_eq!(wins, 1);
    assert!(kv.get("leader").unwrap().is_some());

    kv.stop().unwrap();
}

#[test]
fn get_or_put_concurrent_callers_get_one_winning_value() {
    let kv = ShardedKv::start(4).unwrap();
    let contender_count = 16;

    let values = thread::scope(|scope| {
        let handles = (0..contender_count)
            .map(|idx| {
                let kv = &kv;
                scope.spawn(move || kv.get_or_put("leader", format!("candidate-{idx}")).unwrap())
            })
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    assert_eq!(values.len(), contender_count);
    assert!(values.iter().all(|value| value == &values[0]));
    assert_eq!(kv.get("leader").unwrap(), Some(values[0].clone()));

    kv.stop().unwrap();
}
