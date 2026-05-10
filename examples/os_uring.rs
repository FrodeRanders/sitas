#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::IoUringOperationKind;
    use std::time::Duration;

    let Some(mut ring) = available_ring()? else {
        println!("io_uring unavailable on this Linux host");
        return Ok(());
    };

    let timeout = ring.queue_timeout_operation(Duration::from_secs(1))?;
    let cancel = ring.queue_cancel_operation(timeout)?;

    let queued = ring.snapshot();
    println!(
        "queued: submissions={} tracked={} timeouts={} cancellations={}",
        queued.pending_submissions,
        queued.tracked_operations,
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

#[cfg(target_os = "linux")]
fn available_ring() -> std::io::Result<Option<sitas::os::IoUring>> {
    match sitas::os::IoUring::new(8) {
        Ok(ring) => Ok(Some(ring)),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(1) | Some(22) | Some(38) | Some(95)
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
