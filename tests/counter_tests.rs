use sitas::{
    CounterShardSnapshot, RuntimeSnapshot, ShardError, ShardId, ShardedCounter,
    ShardedCounterConfig, DEFAULT_MAILBOX_CAPACITY,
};

#[test]
fn starting_counter_with_zero_shards_fails() {
    assert_eq!(
        ShardedCounter::start(0).unwrap_err(),
        ShardError::InvalidShardCount
    );
}

#[test]
fn starting_counter_with_zero_mailbox_capacity_fails() {
    assert_eq!(
        ShardedCounter::start_with_config(ShardedCounterConfig::new(1).with_mailbox_capacity(0))
            .unwrap_err(),
        ShardError::InvalidMailboxCapacity
    );
}

#[test]
fn add_and_get_on_shard_work() {
    let counter = ShardedCounter::start(2).unwrap();

    assert_eq!(counter.shard_count(), 2);
    assert_eq!(counter.mailbox_capacity(), DEFAULT_MAILBOX_CAPACITY);
    assert_eq!(counter.get_on_shard(ShardId(0)).unwrap(), 0);
    assert_eq!(counter.add_on_shard(ShardId(0), 5).unwrap(), 5);
    assert_eq!(counter.add_on_shard(ShardId(0), -2).unwrap(), 3);
    assert_eq!(counter.get_on_shard(ShardId(0)).unwrap(), 3);

    counter.stop().unwrap();
}

#[test]
fn counter_total_sums_all_shards() {
    let counter = ShardedCounter::start(4).unwrap();

    counter.add_on_shard(ShardId(0), 5).unwrap();
    counter.add_on_shard(ShardId(1), 7).unwrap();
    counter.add_on_shard(ShardId(2), -2).unwrap();

    assert_eq!(counter.total().unwrap(), 10);

    counter.stop().unwrap();
}

#[test]
fn counter_try_total_and_submit_total_work() {
    let counter =
        ShardedCounter::start_with_config(ShardedCounterConfig::new(3).with_mailbox_capacity(4))
            .unwrap();

    counter.add_on_shard(ShardId(0), 5).unwrap();
    counter.add_on_shard(ShardId(1), 7).unwrap();
    counter.add_on_shard(ShardId(2), -2).unwrap();

    assert_eq!(counter.try_total().unwrap(), 10);

    let total = counter.submit_total().unwrap();
    assert_eq!(total.wait().unwrap(), 10);

    let total = counter.try_submit_total().unwrap();
    assert_eq!(total.wait().unwrap(), 10);

    counter.stop().unwrap();
}

#[test]
fn counter_shard_snapshots_report_values_in_shard_order() {
    let counter = ShardedCounter::start(4).unwrap();

    counter.add_on_shard(ShardId(0), 5).unwrap();
    counter.add_on_shard(ShardId(1), 7).unwrap();
    counter.add_on_shard(ShardId(2), -2).unwrap();

    assert_eq!(
        counter.shard_snapshots().unwrap(),
        vec![
            CounterShardSnapshot {
                shard_id: ShardId(0),
                value: 5
            },
            CounterShardSnapshot {
                shard_id: ShardId(1),
                value: 7
            },
            CounterShardSnapshot {
                shard_id: ShardId(2),
                value: -2
            },
            CounterShardSnapshot {
                shard_id: ShardId(3),
                value: 0
            },
        ]
    );

    counter.stop().unwrap();
}

#[test]
fn counter_try_and_submit_shard_snapshots_work() {
    let counter =
        ShardedCounter::start_with_config(ShardedCounterConfig::new(2).with_mailbox_capacity(4))
            .unwrap();

    counter.add_on_shard(ShardId(0), 3).unwrap();
    counter.add_on_shard(ShardId(1), 9).unwrap();

    assert_eq!(
        counter.try_shard_snapshots().unwrap(),
        vec![
            CounterShardSnapshot {
                shard_id: ShardId(0),
                value: 3
            },
            CounterShardSnapshot {
                shard_id: ShardId(1),
                value: 9
            },
        ]
    );

    let submitted = counter.submit_shard_snapshots().unwrap();
    assert_eq!(
        submitted.wait().unwrap(),
        vec![
            CounterShardSnapshot {
                shard_id: ShardId(0),
                value: 3
            },
            CounterShardSnapshot {
                shard_id: ShardId(1),
                value: 9
            },
        ]
    );

    let submitted = counter.try_submit_shard_snapshots().unwrap();
    assert_eq!(
        submitted.wait().unwrap(),
        vec![
            CounterShardSnapshot {
                shard_id: ShardId(0),
                value: 3
            },
            CounterShardSnapshot {
                shard_id: ShardId(1),
                value: 9
            },
        ]
    );

    counter.stop().unwrap();
}

#[test]
fn counter_submit_and_try_submit_work() {
    let counter =
        ShardedCounter::start_with_config(ShardedCounterConfig::new(2).with_mailbox_capacity(4))
            .unwrap();

    assert_eq!(counter.mailbox_capacity(), 4);
    assert_eq!(
        counter.runtime_snapshot(),
        RuntimeSnapshot {
            shard_count: 2,
            mailbox_capacity: 4,
            stopped: false,
        }
    );

    let add = counter.submit_add_on_shard(ShardId(0), 4).unwrap();
    assert_eq!(add.wait().unwrap(), 4);

    let add = counter.try_submit_add_on_shard(ShardId(0), 6).unwrap();
    assert_eq!(add.wait().unwrap(), 10);

    let get = counter.submit_get_on_shard(ShardId(0)).unwrap();
    assert_eq!(get.wait().unwrap(), 10);
    assert_eq!(counter.try_get_on_shard(ShardId(0)).unwrap(), 10);

    counter.stop().unwrap();
}

#[test]
fn counter_invalid_shard_fails() {
    let counter = ShardedCounter::start(1).unwrap();

    assert_eq!(
        counter.add_on_shard(ShardId(99), 1).unwrap_err(),
        ShardError::InvalidShardId(99)
    );
    assert_eq!(
        counter.try_submit_get_on_shard(ShardId(99)).unwrap_err(),
        ShardError::InvalidShardId(99)
    );

    counter.stop().unwrap();
}

#[test]
fn dropping_counter_without_stop_shuts_down_shards() {
    let counter = ShardedCounter::start(2).unwrap();

    counter.add_on_shard(ShardId(0), 1).unwrap();
}

#[test]
fn counter_shutdown_marks_runtime_snapshot_stopped_and_is_idempotent() {
    let mut counter =
        ShardedCounter::start_with_config(ShardedCounterConfig::new(2).with_mailbox_capacity(4))
            .unwrap();

    counter.add_on_shard(ShardId(0), 1).unwrap();
    counter.shutdown().unwrap();
    counter.shutdown().unwrap();

    assert_eq!(
        counter.runtime_snapshot(),
        RuntimeSnapshot {
            shard_count: 2,
            mailbox_capacity: 4,
            stopped: true,
        }
    );
    assert_eq!(
        counter.get_on_shard(ShardId(0)),
        Err(ShardError::ShardStopped)
    );
    assert_eq!(counter.try_total(), Err(ShardError::ShardStopped));
}
