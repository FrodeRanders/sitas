#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{IoUringDispatcher, IoUringOperationFuture};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Context;
    use std::time::Duration;

    let Some(ring) = available_ring()? else {
        report_unavailable();
        return Ok(());
    };

    let dispatcher = IoUringDispatcher::new(ring).into_shared();
    let mut future =
        IoUringOperationFuture::queue_timeout(Rc::clone(&dispatcher), Duration::from_secs(30))?;
    let operation = future.operation();
    let waker: std::task::Waker = Arc::new(NoopWake).into();
    let mut context = Context::from_waker(&waker);

    assert!(Pin::new(&mut future).poll(&mut context).is_pending());
    drop(future);

    let abandoned = dispatcher.borrow().snapshot();
    println!(
        "dropped timeout {}: pending={} abandoned={} timeouts={} cancellations={}",
        operation,
        abandoned.ring.pending_submissions,
        abandoned.abandoned_operations,
        abandoned.abandoned_operation_kinds.timeouts,
        abandoned.abandoned_operation_kinds.cancellations
    );
    assert_eq!(abandoned.ring.pending_submissions, 2);
    assert_eq!(abandoned.abandoned_operations, 2);
    assert_eq!(abandoned.abandoned_operation_kinds.timeouts, 1);
    assert_eq!(abandoned.abandoned_operation_kinds.cancellations, 1);

    drain_dispatcher_until_idle(&dispatcher)?;

    let drained = dispatcher.borrow().snapshot();
    println!(
        "after drain: pending={} abandoned={} deferred_buffers={} completed={}",
        drained.ring.pending_submissions,
        drained.abandoned_operations,
        drained.deferred_buffers,
        drained.completed_operations
    );
    assert_eq!(drained.ring.tracked_operations, 0);
    assert_eq!(drained.abandoned_operations, 0);
    assert_eq!(drained.deferred_buffers, 0);
    assert_eq!(drained.completed_operations, 0);

    Ok(())
}

#[cfg(target_os = "linux")]
struct NoopWake;

#[cfg(target_os = "linux")]
impl std::task::Wake for NoopWake {
    fn wake(self: std::sync::Arc<Self>) {}
}

#[cfg(target_os = "linux")]
fn drain_dispatcher_until_idle(
    dispatcher: &sitas::os::SharedIoUringDispatcher,
) -> std::io::Result<()> {
    for _ in 0..8 {
        let snapshot = dispatcher.borrow().snapshot();
        if snapshot.ring.tracked_operations == 0
            && snapshot.abandoned_operations == 0
            && snapshot.deferred_buffers == 0
        {
            return Ok(());
        }
        dispatcher.borrow_mut().wait_and_dispatch(1)?;
    }

    panic!(
        "dispatcher did not become idle after draining: {:?}",
        dispatcher.borrow().snapshot()
    );
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
            if require_io_uring() {
                return Err(error);
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn report_unavailable() {
    println!("io_uring unavailable on this Linux host");
    println!("set SITAS_REQUIRE_IO_URING=1 to fail instead of skipping");
}

#[cfg(target_os = "linux")]
fn require_io_uring() -> bool {
    matches!(
        std::env::var("SITAS_REQUIRE_IO_URING").as_deref(),
        Ok("1" | "true" | "yes" | "on")
    )
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
