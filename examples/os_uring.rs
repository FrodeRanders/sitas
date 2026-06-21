//! Queues and completes raw `io_uring` operations.
//!
//! This deliberately stays below the executor: it shows the kernel submission
//! and completion lifecycle before those operations are wrapped as futures.
mod support;
#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{IoUringOperationKind, available_io_uring, report_io_uring_unavailable};
    use std::time::Duration;

    support::announce("os_uring");
    let Some(mut ring) = available_io_uring(8)? else {
        report_io_uring_unavailable();
        return Ok(());
    };

    let timeout = ring.queue_timeout_operation(Duration::from_secs(1))?;
    let cancel = ring.queue_cancel_operation(timeout)?;

    let queued = ring.snapshot();
    println!(
        "queued: submissions={} tracked={} timeout={} cancel={} timeouts={} cancellations={}",
        queued.pending_submissions,
        queued.tracked_operations,
        timeout,
        cancel,
        queued.operation_kinds.timeouts,
        queued.operation_kinds.cancellations
    );

    let cancel_completion = ring.wait_operation_completion(cancel)?;
    let timeout_completion = ring.wait_operation_completion(timeout)?;

    println!(
        "cancel completed: kind={:?} result={}",
        cancel_completion.kind, cancel_completion.result
    );
    println!(
        "timeout completed: kind={:?} result={}",
        timeout_completion.kind, timeout_completion.result
    );

    let final_snapshot = ring.snapshot();
    println!(
        "after completions: submissions={} completions={} tracked={}",
        final_snapshot.pending_submissions,
        final_snapshot.pending_completions,
        final_snapshot.tracked_operations
    );

    assert_eq!(
        cancel_completion.kind,
        IoUringOperationKind::Cancel { target: timeout }
    );
    assert_eq!(timeout_completion.kind, IoUringOperationKind::Timeout);
    assert_eq!(final_snapshot.tracked_operations, 0);

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
