use super::{
    Notify, RaceOutput, TaskScope, TaskScopeError, TimeoutError, block_on, executor_and_spawner,
    race, sleep, stop_pair, timeout, yield_now,
};
#[cfg(unix)]
use super::{
    accept_async, accept_timeout_async, connect_async, connect_timeout_async, copy_async,
    copy_timeout_async, read_exact_async, read_exact_timeout_async, readable, serve_tcp_n,
    serve_tcp_n_timeout, serve_tcp_until_idle, serve_tcp_until_idle_timeout,
    serve_tcp_until_stopped, serve_tcp_until_stopped_scoped,
    serve_tcp_until_stopped_scoped_timeout, serve_tcp_until_stopped_timeout, writable,
    write_all_async, write_all_timeout_async,
};
#[cfg(target_os = "linux")]
use super::{read_at_uring, read_exact_at_uring, write_all_at_uring};
#[cfg(target_os = "linux")]
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
#[cfg(unix)]
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn block_on_returns_future_output() {
    assert_eq!(block_on(async { 42 }), 42);
}

#[test]
fn block_on_accepts_stack_borrowing_future() {
    let value = 42;
    let borrowed = block_on(async { &value });

    assert_eq!(*borrowed, 42);
}

#[test]
fn block_on_preserves_root_future_panic() {
    let panic = std::panic::catch_unwind(|| {
        block_on(async {
            panic!("root panic");
        });
    })
    .unwrap_err();

    assert_eq!(panic.downcast_ref::<&str>(), Some(&"root panic"));
}

#[test]
fn stop_token_completes_after_source_stops() {
    let (source, token) = stop_pair();
    assert!(!source.is_stopped());
    assert!(source.stop());
    assert!(!source.stop());
    assert!(token.is_stopped());

    block_on(token);
}

#[test]
fn cloned_stop_tokens_wake_multiple_waiters() {
    let (executor, spawner) = executor_and_spawner();
    let (source, token) = stop_pair();

    let first = spawner.spawn_with_handle(token.clone()).unwrap();
    let second = spawner.spawn_with_handle(token).unwrap();

    executor.run_until(async {
        yield_now().await;
        assert!(source.stop());

        first.await.unwrap();
        second.await.unwrap();
    });

    drop(spawner);
}

#[test]
fn notify_waiters_completes_after_notification() {
    let notify = Notify::new();
    assert!(!notify.is_notified());
    assert!(notify.notify_waiters());
    assert!(!notify.notify_waiters());
    assert!(notify.is_notified());

    block_on(notify.notified());
}

#[test]
fn cloned_notify_wakes_multiple_waiters() {
    let (executor, spawner) = executor_and_spawner();
    let notify = Notify::new();

    let first = spawner.spawn_with_handle(notify.notified()).unwrap();
    let second = spawner.spawn_with_handle(notify.notified()).unwrap();
    let notify_for_task = notify.clone();

    executor.run_until(async {
        yield_now().await;
        assert!(notify_for_task.notify_waiters());

        first.await.unwrap();
        second.await.unwrap();
    });

    drop(spawner);
}

#[test]
fn notify_completes_future_created_after_notification() {
    let (executor, spawner) = executor_and_spawner();
    let notify = Notify::new();
    assert!(notify.notify_waiters());

    executor.run_until(async {
        timeout(Duration::from_millis(1), notify.notified())
            .await
            .unwrap();
    });

    drop(spawner);
}

#[test]
fn task_scope_waits_for_children() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let values = Arc::new(Mutex::new(Vec::new()));

    for value in [1, 2] {
        let values = Arc::clone(&values);
        scope
            .spawn(async move {
                yield_now().await;
                values.lock().unwrap().push(value);
            })
            .unwrap();
    }

    executor.run_until(async move {
        scope.wait().await.unwrap();
    });

    drop(spawner);

    let mut values = values.lock().unwrap().clone();
    values.sort();
    assert_eq!(values, [1, 2]);
}

#[test]
fn task_scope_shutdown_wakes_stop_token_children() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let stopped = Arc::new(Mutex::new(false));
    let stopped_for_task = Arc::clone(&stopped);

    scope
        .spawn_with_stop(move |stop| async move {
            stop.await;
            *stopped_for_task.lock().unwrap() = true;
        })
        .unwrap();

    executor.run_until(async move {
        yield_now().await;
        scope.shutdown().await.unwrap();
    });

    drop(spawner);

    assert!(*stopped.lock().unwrap());
}

#[test]
fn task_scope_shutdown_timeout_waits_for_cooperative_children() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let stopped = Arc::new(Mutex::new(false));
    let stopped_for_task = Arc::clone(&stopped);

    scope
        .spawn_with_stop(move |stop| async move {
            stop.await;
            *stopped_for_task.lock().unwrap() = true;
        })
        .unwrap();

    let result =
        executor.run_until(async move { scope.shutdown_timeout(Duration::from_secs(1)).await });

    drop(spawner);

    assert!(result.is_ok());
    assert!(*stopped.lock().unwrap());
}

#[test]
fn task_scope_shutdown_timeout_aborts_uncooperative_children() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let completed = Arc::new(Mutex::new(false));
    let completed_for_task = Arc::clone(&completed);

    scope
        .spawn(async move {
            sleep(Duration::from_secs(1)).await;
            *completed_for_task.lock().unwrap() = true;
        })
        .unwrap();

    let result =
        executor.run_until(async move { scope.shutdown_timeout(Duration::from_millis(5)).await });

    executor.run_until(async {
        yield_now().await;
    });

    drop(spawner);

    assert!(matches!(result, Err(TaskScopeError::TimedOut)));
    assert!(!*completed.lock().unwrap());
}

#[test]
fn dropping_task_scope_aborts_children() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let completed = Arc::new(Mutex::new(false));
    let completed_for_task = Arc::clone(&completed);

    scope
        .spawn(async move {
            sleep(Duration::from_secs(1)).await;
            *completed_for_task.lock().unwrap() = true;
        })
        .unwrap();

    drop(scope);

    executor.run_until(async {
        yield_now().await;
    });

    drop(spawner);

    assert!(!*completed.lock().unwrap());
}

#[test]
fn run_until_drives_spawned_tasks() {
    let (executor, spawner) = executor_and_spawner();

    let output = executor.run_until(async {
        let worker = spawner
            .spawn_with_handle(async {
                yield_now().await;
                7
            })
            .unwrap();

        worker.await.unwrap()
    });

    drop(spawner);

    assert_eq!(output, 7);
}

#[test]
fn run_until_timer_completes_while_task_keeps_waking() {
    let (executor, spawner) = executor_and_spawner();

    spawner.spawn(AlwaysWake).unwrap();
    drop(spawner);

    let started = Instant::now();
    executor.run_until(async {
        sleep(Duration::from_millis(5)).await;
    });

    assert!(started.elapsed() < Duration::from_secs(1));

    let snapshot = executor.snapshot();
    assert_eq!(snapshot.ready_poll_budget, super::READY_POLL_BUDGET);
    assert!(snapshot.total_task_polls >= super::READY_POLL_BUDGET as u64);
    assert!(snapshot.ready_poll_budget_exhaustions > 0);
}

#[test]
fn yield_now_yields_once_before_completion() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_task = Arc::clone(&events);

    block_on(async move {
        events_for_task.lock().unwrap().push("before");
        yield_now().await;
        events_for_task.lock().unwrap().push("after");
    });

    assert_eq!(&*events.lock().unwrap(), &["before", "after"]);
}

#[test]
fn sleep_delays_future_completion() {
    let started = Instant::now();

    block_on(async {
        sleep(Duration::from_millis(10)).await;
    });

    assert!(started.elapsed() >= Duration::from_millis(10));
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_read_and_write_are_driven_by_executor_loop() -> io::Result<()> {
    if crate::os::available_io_uring(8)?.is_none() {
        return Ok(());
    }

    let path = std::env::temp_dir().join(format!(
        "sitas-executor-uring-{}-{:?}.dat",
        std::process::id(),
        thread::current().id()
    ));
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;

    let (bytes, snapshot) = block_on(async {
        write_all_at_uring(file.as_raw_fd(), 0, b"abcdef".to_vec()).await?;
        let bytes = read_exact_at_uring(file.as_raw_fd(), 2, 3).await?;
        Ok::<_, io::Error>((bytes, super::uring::snapshot()))
    })?;

    assert_eq!(bytes, b"cde");
    let snapshot = snapshot.expect("io_uring dispatcher is installed");
    assert_eq!(snapshot.total_dispatched_operation_kinds.reads, 1);
    assert_eq!(snapshot.total_dispatched_operation_kinds.writes, 1);
    assert_eq!(snapshot.total_dispatched_operations, 2);
    assert!(snapshot.is_idle());
    drop(file);
    fs::remove_file(path)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_read_exact_reports_unexpected_eof() -> io::Result<()> {
    if crate::os::available_io_uring(8)?.is_none() {
        return Ok(());
    }

    let path = std::env::temp_dir().join(format!(
        "sitas-executor-uring-eof-{}-{:?}.dat",
        std::process::id(),
        thread::current().id()
    ));
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;

    let error = block_on(async {
        write_all_at_uring(file.as_raw_fd(), 0, b"abc".to_vec()).await?;
        read_exact_at_uring(file.as_raw_fd(), 0, 4).await
    })
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    drop(file);
    fs::remove_file(path)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_completion_is_not_delayed_by_unexpired_timer() -> io::Result<()> {
    if crate::os::available_io_uring(8)?.is_none() {
        return Ok(());
    }

    let path = std::env::temp_dir().join(format!(
        "sitas-executor-uring-timer-{}-{:?}.dat",
        std::process::id(),
        thread::current().id()
    ));
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let (executor, _spawner) = executor_and_spawner();

    let started = Instant::now();
    let bytes = executor
        .run_until(timeout(Duration::from_millis(250), async {
            write_all_at_uring(file.as_raw_fd(), 0, b"abcdef".to_vec()).await?;
            read_at_uring(file.as_raw_fd(), 2, vec![0; 3]).await
        }))
        .expect("io_uring read/write should complete before timeout")?;

    assert_eq!(bytes, b"cde");
    assert!(started.elapsed() < Duration::from_millis(150));
    let snapshot = executor.snapshot();
    assert!(snapshot.total_driver_events > 0);
    assert!(snapshot.total_completion_events > 0);
    drop(file);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn timeout_returns_future_output_before_deadline() {
    let output = block_on(async { timeout(Duration::from_secs(1), async { 7 }).await });

    assert_eq!(output, Ok(7));
}

#[test]
fn timeout_expires_before_slow_future() {
    let started = Instant::now();

    let output = block_on(async {
        timeout(Duration::from_millis(5), async {
            sleep(Duration::from_secs(60)).await;
            7
        })
        .await
    });

    assert_eq!(output, Err(TimeoutError));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn timeout_drops_inner_sleep_timer() {
    let (executor, spawner) = executor_and_spawner();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let result = timeout(Duration::from_millis(5), async {
                sleep(Duration::from_secs(60)).await;
                7
            })
            .await;
            *output_for_task.lock().unwrap() = Some(result);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some(Err(TimeoutError)));
    assert_eq!(executor.scheduler.snapshot().timer_count, 0);
}

#[test]
fn race_returns_first_future_output() {
    let output = block_on(async {
        race(
            async {
                yield_now().await;
                "first"
            },
            async {
                sleep(Duration::from_secs(60)).await;
                "second"
            },
        )
        .await
    });

    assert_eq!(output, RaceOutput::First("first"));
}

#[test]
fn race_returns_second_future_output() {
    let output = block_on(async {
        race(
            async {
                sleep(Duration::from_secs(60)).await;
                "first"
            },
            async {
                yield_now().await;
                "second"
            },
        )
        .await
    });

    assert_eq!(output, RaceOutput::Second("second"));
}

#[test]
fn race_drops_losing_sleep_timer() {
    let (executor, spawner) = executor_and_spawner();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let result = race(
                async {
                    sleep(Duration::from_millis(5)).await;
                    "fast"
                },
                async {
                    sleep(Duration::from_secs(60)).await;
                    "slow"
                },
            )
            .await;
            *output_for_task.lock().unwrap() = Some(result);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some(RaceOutput::First("fast")));
    assert_eq!(executor.scheduler.snapshot().timer_count, 0);
}

#[test]
fn timers_wake_in_deadline_order() {
    let (executor, spawner) = executor_and_spawner();
    let events = Arc::new(Mutex::new(Vec::new()));

    let slow_events = Arc::clone(&events);
    spawner
        .spawn(async move {
            sleep(Duration::from_millis(20)).await;
            slow_events.lock().unwrap().push("slow");
        })
        .unwrap();

    let fast_events = Arc::clone(&events);
    spawner
        .spawn(async move {
            sleep(Duration::from_millis(5)).await;
            fast_events.lock().unwrap().push("fast");
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(&*events.lock().unwrap(), &["fast", "slow"]);
}

#[test]
fn executor_runs_multiple_spawned_tasks() {
    let (executor, spawner) = executor_and_spawner();
    let values = Arc::new(Mutex::new(Vec::new()));

    for value in 0..3 {
        let values_for_task = Arc::clone(&values);
        spawner
            .spawn(async move {
                yield_now().await;
                values_for_task.lock().unwrap().push(value);
            })
            .unwrap();
    }

    drop(spawner);
    executor.run();

    let mut values = values.lock().unwrap().clone();
    values.sort();
    assert_eq!(values, vec![0, 1, 2]);
}

#[test]
fn repeated_wakes_share_one_ready_queue_entry() {
    let (executor, spawner) = executor_and_spawner();
    spawner.spawn(WakeTwiceThenPending).unwrap();

    let task = executor.scheduler.next_task().unwrap();
    task.poll();

    assert_eq!(executor.scheduler.snapshot().ready_queue_len, 1);
}

#[test]
fn executor_snapshot_reports_cumulative_scheduler_counters() {
    let (executor, spawner) = executor_and_spawner();

    for _ in 0..3 {
        spawner
            .spawn(async {
                yield_now().await;
            })
            .unwrap();
    }

    drop(spawner);
    executor.run();

    let snapshot = executor.snapshot();
    assert_eq!(snapshot.ready_poll_budget, super::READY_POLL_BUDGET);
    assert_eq!(snapshot.total_spawned_tasks, 3);
    assert_eq!(snapshot.total_completed_tasks, 3);
    assert_eq!(snapshot.total_task_polls, 6);
    assert_eq!(snapshot.ready_poll_budget_exhaustions, 0);
    assert_eq!(snapshot.total_driver_events, 0);
    #[cfg(unix)]
    {
        assert_eq!(snapshot.total_readiness_events, 0);
        assert_eq!(snapshot.total_readable_events, 0);
        assert_eq!(snapshot.total_writable_events, 0);
    }
    #[cfg(target_os = "linux")]
    assert_eq!(snapshot.total_completion_events, 0);
}

#[test]
fn panicking_task_does_not_stop_executor() {
    let (executor, spawner) = executor_and_spawner();
    let completed = Arc::new(Mutex::new(false));
    let completed_for_task = Arc::clone(&completed);

    spawner
        .spawn(async {
            panic!("task panic");
        })
        .unwrap();
    spawner
        .spawn(async move {
            *completed_for_task.lock().unwrap() = true;
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert!(*completed.lock().unwrap());
}

#[test]
fn spawn_with_handle_returns_task_output() {
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    let worker = spawner
        .spawn_with_handle(async {
            yield_now().await;
            7
        })
        .unwrap();

    spawner
        .spawn(async move {
            let output = worker.await.unwrap();
            *result_for_task.lock().unwrap() = Some(output);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*result.lock().unwrap(), Some(7));
}

#[test]
fn tasks_can_await_multiple_join_handles() {
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    let first = spawner
        .spawn_with_handle(async {
            yield_now().await;
            2
        })
        .unwrap();
    let second = spawner
        .spawn_with_handle(async {
            yield_now().await;
            yield_now().await;
            5
        })
        .unwrap();

    spawner
        .spawn(async move {
            *result_for_task.lock().unwrap() = Some(first.await.unwrap() + second.await.unwrap());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*result.lock().unwrap(), Some(7));
}

#[test]
fn panicking_join_handle_wakes_waiter() {
    let (executor, spawner) = executor_and_spawner();
    let observed = Arc::new(Mutex::new(false));
    let observed_for_task = Arc::clone(&observed);

    let worker = spawner
        .spawn_with_handle(async {
            panic!("join panic");
        })
        .unwrap();

    spawner
        .spawn(CatchJoinPanic {
            handle: worker,
            observed: observed_for_task,
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert!(*observed.lock().unwrap());
}

#[test]
fn aborted_join_handle_returns_cancelled() {
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    let worker = spawner
        .spawn_with_handle(async {
            sleep(Duration::from_secs(60)).await;
            7
        })
        .unwrap();

    spawner
        .spawn(async move {
            yield_now().await;
            let aborted = worker.abort();
            let cancelled = worker.await.unwrap_err().is_cancelled();
            *result_for_task.lock().unwrap() = Some((aborted, cancelled));
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*result.lock().unwrap(), Some((true, true)));
    assert_eq!(executor.scheduler.snapshot().timer_count, 0);
}

#[test]
fn aborting_completed_join_handle_returns_false() {
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    let worker = spawner
        .spawn_with_handle(async {
            yield_now().await;
            7
        })
        .unwrap();

    spawner
        .spawn(async move {
            yield_now().await;
            yield_now().await;
            let aborted = worker.abort();
            let output = worker.await.unwrap();
            *result_for_task.lock().unwrap() = Some((aborted, output));
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*result.lock().unwrap(), Some((false, 7)));
}

#[test]
fn spawned_tasks_can_sleep_before_joining() {
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    let worker = spawner
        .spawn_with_handle(async {
            sleep(Duration::from_millis(5)).await;
            11
        })
        .unwrap();

    spawner
        .spawn(async move {
            *result_for_task.lock().unwrap() = Some(worker.await.unwrap());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*result.lock().unwrap(), Some(11));
}

#[cfg(unix)]
#[test]
fn readable_future_completes_when_fd_becomes_readable() {
    let (executor, spawner) = executor_and_spawner();
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let reader_fd = reader.as_raw_fd();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            readable(reader_fd).await;

            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte).unwrap();
            *output_for_task.lock().unwrap() = Some(byte[0]);
        })
        .unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        writer.write_all(b"x").unwrap();
    });

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some(b'x'));
    let snapshot = executor.snapshot();
    assert!(snapshot.total_driver_events > 0);
    assert!(snapshot.total_readiness_events > 0);
    assert!(snapshot.total_readable_events > 0);
    assert_eq!(snapshot.total_writable_events, 0);
}

#[cfg(unix)]
#[test]
fn multiple_tasks_can_wait_for_different_readable_fds() {
    let (executor, spawner) = executor_and_spawner();
    let (mut first_reader, mut first_writer) = UnixStream::pair().unwrap();
    let (mut second_reader, mut second_writer) = UnixStream::pair().unwrap();
    first_reader.set_nonblocking(true).unwrap();
    second_reader.set_nonblocking(true).unwrap();
    let first_fd = first_reader.as_raw_fd();
    let second_fd = second_reader.as_raw_fd();
    let output = Arc::new(Mutex::new(Vec::new()));

    let first_output = Arc::clone(&output);
    spawner
        .spawn(async move {
            readable(first_fd).await;
            let mut byte = [0u8; 1];
            first_reader.read_exact(&mut byte).unwrap();
            first_output.lock().unwrap().push(byte[0]);
        })
        .unwrap();

    let second_output = Arc::clone(&output);
    spawner
        .spawn(async move {
            readable(second_fd).await;
            let mut byte = [0u8; 1];
            second_reader.read_exact(&mut byte).unwrap();
            second_output.lock().unwrap().push(byte[0]);
        })
        .unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        second_writer.write_all(b"b").unwrap();
        thread::sleep(Duration::from_millis(5));
        first_writer.write_all(b"a").unwrap();
    });

    drop(spawner);
    executor.run();

    assert_eq!(&*output.lock().unwrap(), b"ba");
}

#[cfg(unix)]
#[test]
fn multiple_tasks_can_wait_for_same_readable_fd() {
    let (executor, spawner) = executor_and_spawner();
    let (reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let reader_fd = reader.as_raw_fd();
    let output = Arc::new(Mutex::new(Vec::new()));

    for value in [1, 2] {
        let output_for_task = Arc::clone(&output);
        spawner
            .spawn(async move {
                readable(reader_fd).await;
                output_for_task.lock().unwrap().push(value);
            })
            .unwrap();
    }

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        writer.write_all(b"x").unwrap();
    });

    drop(spawner);
    executor.run();

    let mut observed = output.lock().unwrap().clone();
    observed.sort();
    assert_eq!(observed, vec![1, 2]);
}

#[cfg(unix)]
#[test]
fn read_and_write_waiters_on_same_fd_are_woken_together() {
    let (executor, spawner) = executor_and_spawner();
    let (reader, mut peer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let fd = reader.as_raw_fd();
    let output = Arc::new(Mutex::new(Vec::new()));

    let readable_output = Arc::clone(&output);
    spawner
        .spawn(async move {
            readable(fd).await;
            readable_output.lock().unwrap().push("readable");
        })
        .unwrap();

    let writable_output = Arc::clone(&output);
    spawner
        .spawn(async move {
            writable(fd).await;
            writable_output.lock().unwrap().push("writable");
        })
        .unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        peer.write_all(b"x").unwrap();
    });

    drop(spawner);
    executor.run();

    let mut observed = output.lock().unwrap().clone();
    observed.sort();
    assert_eq!(observed, vec!["readable", "writable"]);
}

#[cfg(unix)]
#[test]
fn writable_future_completes_when_fd_is_writable() {
    let (executor, spawner) = executor_and_spawner();
    let (_reader, writer) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    let writer_fd = writer.as_raw_fd();
    let output = Arc::new(Mutex::new(false));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            writable(writer_fd).await;
            *output_for_task.lock().unwrap() = true;
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert!(*output.lock().unwrap());
    let snapshot = executor.snapshot();
    assert!(snapshot.total_driver_events > 0);
    assert!(snapshot.total_readiness_events > 0);
    assert_eq!(snapshot.total_readable_events, 0);
    assert!(snapshot.total_writable_events > 0);
}

#[cfg(unix)]
#[test]
fn read_exact_async_waits_until_buffer_is_filled() {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        writer.write_all(b"he").unwrap();
        thread::sleep(Duration::from_millis(5));
        writer.write_all(b"llo").unwrap();
    });

    let mut buffer = [0u8; 5];
    let buffer = block_on(async move {
        read_exact_async(&mut reader, &mut buffer).await.unwrap();
        buffer
    });

    assert_eq!(&buffer, b"hello");
}

#[cfg(unix)]
#[test]
fn read_exact_async_returns_unexpected_eof() {
    let (mut reader, writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    drop(writer);

    let mut buffer = [0u8; 1];
    let error = block_on(async move {
        read_exact_async(&mut reader, &mut buffer)
            .await
            .unwrap_err()
    });

    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[cfg(unix)]
#[test]
fn read_exact_timeout_async_returns_timed_out() {
    let (executor, spawner) = executor_and_spawner();
    let (mut reader, _writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let mut buffer = [0u8; 1];
            let error =
                read_exact_timeout_async(&mut reader, &mut buffer, Duration::from_millis(5))
                    .await
                    .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some(std::io::ErrorKind::TimedOut));
    assert_eq!(executor.scheduler.snapshot().read_interest_count, 0);
}

#[cfg(unix)]
#[test]
fn write_all_async_writes_entire_buffer() {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();

    block_on(async move {
        write_all_async(&mut writer, b"hello").await.unwrap();
    });

    let mut buffer = [0u8; 5];
    reader.read_exact(&mut buffer).unwrap();

    assert_eq!(&buffer, b"hello");
}

#[cfg(unix)]
#[test]
fn write_all_timeout_async_writes_entire_buffer() {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();

    block_on(async move {
        write_all_timeout_async(&mut writer, b"hello", Duration::from_secs(1))
            .await
            .unwrap();
    });

    let mut buffer = [0u8; 5];
    reader.read_exact(&mut buffer).unwrap();

    assert_eq!(&buffer, b"hello");
}

#[cfg(unix)]
#[test]
fn copy_async_copies_until_reader_eof() {
    let (mut source_reader, mut source_writer) = UnixStream::pair().unwrap();
    let (mut sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
    source_reader.set_nonblocking(true).unwrap();
    sink_writer.set_nonblocking(true).unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        source_writer.write_all(b"hello").unwrap();
        thread::sleep(Duration::from_millis(5));
        source_writer.write_all(b" world").unwrap();
    });

    let copied = block_on(async move {
        let mut buffer = [0u8; 4];
        copy_async(&mut source_reader, &mut sink_writer, &mut buffer)
            .await
            .unwrap()
    });

    let mut output = Vec::new();
    sink_reader.read_to_end(&mut output).unwrap();

    assert_eq!(copied, 11);
    assert_eq!(&output, b"hello world");
}

#[cfg(unix)]
#[test]
fn copy_async_rejects_empty_buffer() {
    let (mut source_reader, _source_writer) = UnixStream::pair().unwrap();
    let (_sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
    source_reader.set_nonblocking(true).unwrap();
    sink_writer.set_nonblocking(true).unwrap();

    let error = block_on(async move {
        copy_async(&mut source_reader, &mut sink_writer, &mut [])
            .await
            .unwrap_err()
    });

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[cfg(unix)]
#[test]
fn copy_timeout_async_returns_timed_out() {
    let (mut source_reader, _source_writer) = UnixStream::pair().unwrap();
    let (_sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
    source_reader.set_nonblocking(true).unwrap();
    sink_writer.set_nonblocking(true).unwrap();

    let error = block_on(async move {
        let mut buffer = [0u8; 8];
        copy_timeout_async(
            &mut source_reader,
            &mut sink_writer,
            &mut buffer,
            Duration::from_millis(5),
        )
        .await
        .unwrap_err()
    });

    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

#[cfg(unix)]
#[test]
fn accept_async_waits_for_tcp_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        let mut stream = TcpStream::connect(address).unwrap();
        thread::sleep(Duration::from_millis(10));
        stream.write_all(b"x").unwrap();
    });

    let mut stream = block_on(async move {
        let (mut stream, peer) = accept_async(&listener).await.unwrap();
        assert_eq!(peer.ip(), address.ip());
        let mut empty = [0u8; 1];
        assert_eq!(
            stream.read(&mut empty).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        stream
    });

    let byte = block_on(async move {
        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await.unwrap();
        byte
    });

    assert_eq!(byte, [b'x']);
}

#[cfg(unix)]
#[test]
fn accept_timeout_async_returns_timed_out() {
    let (executor, spawner) = executor_and_spawner();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = accept_timeout_async(&listener, Duration::from_millis(5))
                .await
                .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some(std::io::ErrorKind::TimedOut));
    assert_eq!(executor.scheduler.snapshot().read_interest_count, 0);
}

#[cfg(unix)]
#[test]
fn accepted_tcp_stream_works_with_async_read_and_write() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(b"z").unwrap();

        let mut echo = [0u8; 1];
        stream.read_exact(&mut echo).unwrap();
        echo[0]
    });

    block_on(async move {
        let (mut stream, peer) = accept_async(&listener).await.unwrap();
        assert_eq!(peer.ip(), address.ip());

        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await.unwrap();
        write_all_async(&mut stream, &byte).await.unwrap();
    });

    assert_eq!(client.join().unwrap(), b'z');
}

#[cfg(unix)]
#[test]
fn connect_async_establishes_nonblocking_tcp_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();

        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        stream.write_all(&byte).unwrap();
    });

    let echoed = block_on(async move {
        let mut stream = connect_async(address).await.unwrap();
        write_all_async(&mut stream, b"q").await.unwrap();

        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await.unwrap();
        byte[0]
    });

    server.join().unwrap();
    assert_eq!(echoed, b'q');
}

#[cfg(unix)]
#[test]
fn connect_timeout_async_establishes_nonblocking_tcp_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();

        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        stream.write_all(&byte).unwrap();
    });

    let echoed = block_on(async move {
        let mut stream = connect_timeout_async(address, Duration::from_secs(1))
            .await
            .unwrap();
        write_all_async(&mut stream, b"t").await.unwrap();

        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await.unwrap();
        byte[0]
    });

    server.join().unwrap();
    assert_eq!(echoed, b't');
}

#[cfg(unix)]
#[test]
fn connect_async_supports_ipv6_loopback() {
    let listener = match TcpListener::bind("[::1]:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => return,
        Err(error) => panic!("failed to bind IPv6 loopback listener: {error}"),
    };
    let address = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();

        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        stream.write_all(&byte).unwrap();
    });

    let echoed = block_on(async move {
        let mut stream = connect_async(address).await.unwrap();
        write_all_async(&mut stream, b"v").await.unwrap();

        let mut byte = [0u8; 1];
        read_exact_async(&mut stream, &mut byte).await.unwrap();
        byte[0]
    });

    server.join().unwrap();
    assert_eq!(echoed, b'v');
}

#[cfg(unix)]
#[test]
fn connect_and_accept_can_run_on_same_executor() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();

    let (executor, spawner) = executor_and_spawner();
    let output = Arc::new(Mutex::new(None));

    let server = spawner
        .spawn_with_handle(async move {
            let (mut stream, peer) = accept_async(&listener).await.unwrap();
            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            write_all_async(&mut stream, &byte).await.unwrap();
            peer
        })
        .unwrap();

    let client = spawner
        .spawn_with_handle(async move {
            let mut stream = connect_async(address).await.unwrap();
            write_all_async(&mut stream, b"x").await.unwrap();

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            byte[0]
        })
        .unwrap();

    let output_for_task = Arc::clone(&output);
    spawner
        .spawn(async move {
            let peer = server.await.unwrap();
            let echoed = client.await.unwrap();
            *output_for_task.lock().unwrap() = Some((peer, echoed));
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let (peer, echoed) = output.lock().unwrap().take().unwrap();
    assert_eq!(peer.ip(), address.ip());
    assert_eq!(echoed, b'x');
}

#[cfg(unix)]
#[test]
fn executor_can_spawn_tcp_handlers_for_multiple_accepted_streams() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();

    let clients = (0..3u8)
        .map(|value| {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                let mut stream = TcpStream::connect(address).unwrap();
                stream.write_all(&[b'a' + value]).unwrap();

                let mut echo = [0u8; 1];
                stream.read_exact(&mut echo).unwrap();
                echo[0]
            })
        })
        .collect::<Vec<_>>();

    let (executor, spawner) = executor_and_spawner();
    let accept_spawner = spawner.clone();

    spawner
        .spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = accept_async(&listener).await.unwrap();
                accept_spawner
                    .spawn(async move {
                        let mut byte = [0u8; 1];
                        read_exact_async(&mut stream, &mut byte).await.unwrap();
                        write_all_async(&mut stream, &byte).await.unwrap();
                    })
                    .unwrap();
            }
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let mut echoed = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    echoed.sort();

    assert_eq!(&echoed, b"abc");
}

#[cfg(unix)]
#[test]
fn serve_tcp_n_spawns_handlers_for_accepted_streams() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let clients = (0..3u8)
        .map(|value| {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                let mut stream = TcpStream::connect(address).unwrap();
                stream.write_all(&[b'a' + value]).unwrap();

                let mut echo = [0u8; 1];
                stream.read_exact(&mut echo).unwrap();
                echo[0]
            })
        })
        .collect::<Vec<_>>();

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();

    spawner
        .spawn(async move {
            serve_tcp_n(
                listener,
                server_spawner,
                3,
                |mut stream, _peer| async move {
                    let mut byte = [0u8; 1];
                    read_exact_async(&mut stream, &mut byte).await?;
                    write_all_async(&mut stream, &byte).await
                },
            )
            .await
            .unwrap();
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let mut echoed = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    echoed.sort();

    assert_eq!(&echoed, b"abc");
}

#[cfg(unix)]
#[test]
fn serve_tcp_n_returns_handler_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        TcpStream::connect(address).unwrap();
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = serve_tcp_n(listener, server_spawner, 1, |_stream, _peer| async {
                Err(io::Error::other("handler failed"))
            })
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();
    client.join().unwrap();

    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
}

#[cfg(unix)]
#[test]
fn serve_tcp_n_timeout_aborts_uncooperative_handler() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap_err().kind()
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = serve_tcp_n_timeout(
                listener,
                server_spawner,
                1,
                Duration::from_millis(5),
                |_stream, _peer| async move {
                    sleep(Duration::from_secs(1)).await;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(client.join().unwrap(), io::ErrorKind::UnexpectedEof);
    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::TimedOut));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_idle_stops_after_idle_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let clients = (0..3u8)
        .map(|value| {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                let mut stream = TcpStream::connect(address).unwrap();
                stream.write_all(&[b'a' + value]).unwrap();

                let mut echo = [0u8; 1];
                stream.read_exact(&mut echo).unwrap();
                echo[0]
            })
        })
        .collect::<Vec<_>>();

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let accepted = serve_tcp_until_idle(
                listener,
                server_spawner,
                Duration::from_millis(25),
                |mut stream, _peer| async move {
                    let mut byte = [0u8; 1];
                    read_exact_async(&mut stream, &mut byte).await?;
                    write_all_async(&mut stream, &byte).await
                },
            )
            .await
            .unwrap();
            *output_for_task.lock().unwrap() = Some(accepted);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let mut echoed = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    echoed.sort();

    assert_eq!(*output.lock().unwrap(), Some(3));
    assert_eq!(&echoed, b"abc");
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_idle_returns_handler_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        TcpStream::connect(address).unwrap();
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = serve_tcp_until_idle(
                listener,
                server_spawner,
                Duration::from_millis(5),
                |_stream, _peer| async { Err(io::Error::other("handler failed")) },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();
    client.join().unwrap();

    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_idle_timeout_aborts_uncooperative_handler() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap_err().kind()
    });

    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = serve_tcp_until_idle_timeout(
                listener,
                server_spawner,
                Duration::from_millis(5),
                Duration::from_millis(5),
                |_stream, _peer| async move {
                    sleep(Duration::from_secs(1)).await;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(client.join().unwrap(), io::ErrorKind::UnexpectedEof);
    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::TimedOut));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_stops_after_stop_signal() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let clients = (0..3u8)
        .map(|value| {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                let mut stream = TcpStream::connect(address).unwrap();
                stream.write_all(&[b'a' + value]).unwrap();

                let mut echo = [0u8; 1];
                stream.read_exact(&mut echo).unwrap();
                echo[0]
            })
        })
        .collect::<Vec<_>>();

    let (stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(30)).await;
            stop_source.stop();
        })
        .unwrap();

    spawner
        .spawn(async move {
            let accepted = serve_tcp_until_stopped(
                listener,
                server_spawner,
                stop_token,
                |mut stream, _peer| async move {
                    let mut byte = [0u8; 1];
                    read_exact_async(&mut stream, &mut byte).await?;
                    write_all_async(&mut stream, &byte).await
                },
            )
            .await
            .unwrap();
            *output_for_task.lock().unwrap() = Some(accepted);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    let mut echoed = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    echoed.sort();

    assert_eq!(*output.lock().unwrap(), Some(3));
    assert_eq!(&echoed, b"abc");
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_returns_handler_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        TcpStream::connect(address).unwrap();
    });

    let (stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(5)).await;
            stop_source.stop();
        })
        .unwrap();

    spawner
        .spawn(async move {
            let error = serve_tcp_until_stopped(
                listener,
                server_spawner,
                stop_token,
                |_stream, _peer| async { Err(io::Error::other("handler failed")) },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();
    client.join().unwrap();

    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_timeout_aborts_uncooperative_handler() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap_err().kind()
    });

    let (stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(5)).await;
            stop_source.stop();
        })
        .unwrap();

    spawner
        .spawn(async move {
            let error = serve_tcp_until_stopped_timeout(
                listener,
                server_spawner,
                stop_token,
                Duration::from_millis(5),
                |_stream, _peer| async move {
                    sleep(Duration::from_secs(1)).await;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(client.join().unwrap(), io::ErrorKind::UnexpectedEof);
    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::TimedOut));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_scoped_stops_handlers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        byte[0]
    });

    let (stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(10)).await;
            stop_source.stop();
        })
        .unwrap();

    spawner
        .spawn(async move {
            let accepted = serve_tcp_until_stopped_scoped(
                listener,
                server_spawner,
                stop_token,
                |mut stream, _peer, handler_stop| async move {
                    handler_stop.await;
                    write_all_async(&mut stream, b"x").await
                },
            )
            .await
            .unwrap();
            *output_for_task.lock().unwrap() = Some(accepted);
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(client.join().unwrap(), b'x');
    assert_eq!(*output.lock().unwrap(), Some(1));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_scoped_returns_handler_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        TcpStream::connect(address).unwrap();
    });

    let (_stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            let error = serve_tcp_until_stopped_scoped(
                listener,
                server_spawner,
                stop_token,
                |_stream, _peer, _handler_stop| async { Err(io::Error::other("handler failed")) },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();
    client.join().unwrap();

    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
}

#[cfg(unix)]
#[test]
fn serve_tcp_until_stopped_scoped_timeout_aborts_uncooperative_handler() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap_err().kind()
    });

    let (stop_source, stop_token) = stop_pair();
    let (executor, spawner) = executor_and_spawner();
    let server_spawner = spawner.clone();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    spawner
        .spawn(async move {
            sleep(Duration::from_millis(5)).await;
            stop_source.stop();
        })
        .unwrap();

    spawner
        .spawn(async move {
            let error = serve_tcp_until_stopped_scoped_timeout(
                listener,
                server_spawner,
                stop_token,
                Duration::from_millis(5),
                |_stream, _peer, _handler_stop| async move {
                    sleep(Duration::from_secs(1)).await;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            *output_for_task.lock().unwrap() = Some(error.kind());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(client.join().unwrap(), io::ErrorKind::UnexpectedEof);
    assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::TimedOut));
}

#[test]
fn block_on_can_await_spawned_task_output() {
    let (executor, spawner) = executor_and_spawner();
    let output = Arc::new(Mutex::new(None));
    let output_for_task = Arc::clone(&output);

    let spawner_for_root = spawner.clone();
    spawner
        .spawn(async move {
            let worker = spawner_for_root
                .spawn_with_handle(async {
                    yield_now().await;
                    "done"
                })
                .unwrap();

            *output_for_task.lock().unwrap() = Some(worker.await.unwrap());
        })
        .unwrap();

    drop(spawner);
    executor.run();

    assert_eq!(*output.lock().unwrap(), Some("done"));
}

#[test]
fn spawner_reports_closed_executor() {
    let (executor, spawner) = executor_and_spawner();
    drop(executor);

    assert!(spawner.spawn(async {}).is_err());
}

#[test]
fn dropping_executor_cancels_pending_sleep_task() {
    let (executor, spawner) = executor_and_spawner();
    let worker = spawner
        .spawn_with_handle(async {
            sleep(Duration::from_secs(60)).await;
            7
        })
        .unwrap();

    executor.poll_ready_tasks();
    assert!(executor.scheduler.snapshot().timer_count > 0);

    drop(executor);

    assert!(!worker.abort());
}

#[cfg(unix)]
#[test]
fn dropping_executor_cancels_pending_readable_task() {
    let (executor, spawner) = executor_and_spawner();
    let (reader, _writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let fd = reader.as_raw_fd();

    let worker = spawner
        .spawn_with_handle(async move {
            readable(fd).await;
            7
        })
        .unwrap();

    executor.poll_ready_tasks();
    assert!(executor.scheduler.snapshot().read_interest_count > 0);

    drop(executor);

    assert!(!worker.abort());
}

struct WakeTwiceThenPending;

impl std::future::Future for WakeTwiceThenPending {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        context.waker().wake_by_ref();
        context.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

struct AlwaysWake;

impl std::future::Future for AlwaysWake {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        context.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

struct CatchJoinPanic<T> {
    handle: super::JoinHandle<T>,
    observed: Arc<Mutex<bool>>,
}

impl<T> std::future::Future for CatchJoinPanic<T> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::pin::Pin::new(&mut self.handle).poll(context)
        }));

        match result {
            Ok(std::task::Poll::Ready(Err(error))) if error.is_panic() => {
                *self.observed.lock().unwrap() = true;
                std::task::Poll::Ready(())
            }
            Ok(std::task::Poll::Ready(_)) => std::task::Poll::Ready(()),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(_) => {
                *self.observed.lock().unwrap() = true;
                std::task::Poll::Ready(())
            }
        }
    }
}
