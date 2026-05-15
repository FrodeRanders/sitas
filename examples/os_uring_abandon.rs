#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{
        IoUringDispatcher, IoUringOperationFuture, available_io_uring, report_io_uring_unavailable,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Context;
    use std::time::Duration;

    let Some(ring) = available_io_uring(8)? else {
        report_io_uring_unavailable();
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

    dispatcher.borrow_mut().drain_until_idle(8)?;

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

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
