//! Walks through normal and abandoned `io_uring` future lifecycles.
//!
//! The snapshots make the internal state transitions visible: pending kernel
//! work, dispatched completions, consumed futures, and abandoned operations.
mod support;
#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{
        IoUringDispatcher, IoUringOperationFuture, IoUringOperationKind, available_io_uring,
        report_io_uring_unavailable,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;
    use std::time::Duration;

    support::announce("os_uring_lifecycle");
    let Some(ring) = available_io_uring(8)? else {
        report_io_uring_unavailable();
        return Ok(());
    };

    let dispatcher = IoUringDispatcher::new(ring).into_shared();
    let mut context = Context::from_waker(std::task::Waker::noop());

    let mut normal = IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?;
    let normal_operation = normal.operation();
    assert!(Pin::new(&mut normal).poll(&mut context).is_pending());
    print_snapshot("normal pending", &dispatcher.borrow().snapshot());

    assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(1)?, 1);
    let ready = dispatcher.borrow().snapshot();
    print_snapshot("normal dispatched", &ready);
    assert_eq!(ready.completed_operations, 1);
    assert_eq!(ready.total_woken_operations, 1);
    assert_eq!(ready.total_buffered_operation_kinds.nops, 1);

    let completion = match Pin::new(&mut normal).poll(&mut context) {
        std::task::Poll::Ready(completion) => completion,
        std::task::Poll::Pending => panic!("normal completion should be ready after dispatch"),
    };
    assert_eq!(completion.operation, normal_operation);
    assert_eq!(completion.kind, IoUringOperationKind::Nop);
    print_snapshot("normal consumed", &dispatcher.borrow().snapshot());

    let mut abandoned =
        IoUringOperationFuture::queue_timeout(Rc::clone(&dispatcher), Duration::from_secs(30))?;
    assert!(Pin::new(&mut abandoned).poll(&mut context).is_pending());
    drop(abandoned);
    let dropped = dispatcher.borrow().snapshot();
    print_snapshot("abandoned pending", &dropped);
    assert_eq!(dropped.abandoned_operations, 2);
    assert_eq!(dropped.abandoned_operation_kinds.timeouts, 1);
    assert_eq!(dropped.abandoned_operation_kinds.cancellations, 1);

    dispatcher.borrow_mut().drain_until_idle(8)?;
    let drained = dispatcher.borrow().snapshot();
    print_snapshot("abandoned drained", &drained);
    assert_eq!(drained.abandoned_operations, 0);
    assert_eq!(drained.total_discarded_operation_kinds.timeouts, 1);
    assert_eq!(drained.total_discarded_operation_kinds.cancellations, 1);

    Ok(())
}

#[cfg(target_os = "linux")]
fn print_snapshot(label: &str, snapshot: &sitas::os::IoUringDispatcherSnapshot) {
    println!(
        "{label}: idle={} pending={} tracked={} tracked_kinds={} wakers={} completed={} completed_kinds={} abandoned={} abandoned_kinds={} deferred={} total_dispatched={} total_buffered={} total_woken={} total_discarded={}",
        snapshot.is_idle(),
        snapshot.ring.pending_submissions,
        snapshot.ring.tracked_operations,
        snapshot.ring.operation_kinds.total(),
        snapshot.registered_wakers,
        snapshot.completed_operations,
        snapshot.completed_operation_kinds.total(),
        snapshot.abandoned_operations,
        snapshot.abandoned_operation_kinds.total(),
        snapshot.deferred_buffers,
        snapshot.total_dispatched_operations,
        snapshot.total_buffered_operations,
        snapshot.total_woken_operations,
        snapshot.total_discarded_operations
    );
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
