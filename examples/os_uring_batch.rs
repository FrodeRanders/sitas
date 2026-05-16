//! Dispatches several `io_uring` completions as a batch.
//!
//! Batching matters for a shard-per-core runtime because a shard should be able
//! to harvest multiple kernel completions before returning to application work.
#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{
        IoUringDispatcher, IoUringOperationFuture, IoUringOperationKind, available_io_uring,
        block_on_io_uring_all, report_io_uring_unavailable,
    };
    use std::rc::Rc;

    let Some(ring) = available_io_uring(8)? else {
        report_io_uring_unavailable();
        return Ok(());
    };

    let dispatcher = IoUringDispatcher::new(ring).into_shared();
    let futures = [
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
    ];
    let operations: Vec<_> = futures.iter().map(|future| future.operation()).collect();

    assert_eq!(dispatcher.borrow_mut().wait_and_dispatch(3)?, 3);
    let ready = dispatcher.borrow().snapshot();
    println!(
        "buffered completions: total={} nops={} dispatched={} dispatched_nops={} buffered={} buffered_nops={} woken={}",
        ready.completed_operations,
        ready.completed_operation_kinds.nops,
        ready.total_dispatched_operations,
        ready.total_dispatched_operation_kinds.nops,
        ready.total_buffered_operations,
        ready.total_buffered_operation_kinds.nops,
        ready.total_woken_operations
    );
    assert_eq!(ready.completed_operations, 3);
    assert_eq!(ready.completed_operation_kinds.nops, 3);
    assert_eq!(ready.total_dispatched_operations, 3);
    assert_eq!(ready.total_dispatched_operation_kinds.nops, 3);
    assert_eq!(ready.total_buffered_operations, 3);
    assert_eq!(ready.total_buffered_operation_kinds.nops, 3);
    assert_eq!(ready.total_woken_operations, 0);

    let completions = block_on_io_uring_all(Rc::clone(&dispatcher), futures)?;

    for completion in &completions {
        println!(
            "completed operation {} (raw user_data {}) with result {}",
            completion.operation,
            completion.operation.raw(),
            completion.result
        );
        assert_eq!(completion.kind, IoUringOperationKind::Nop);
        assert_eq!(completion.result, 0);
    }

    assert_eq!(completions.len(), operations.len());
    for (completion, operation) in completions.iter().zip(operations.iter()) {
        assert_eq!(completion.operation, *operation);
    }
    assert_eq!(dispatcher.borrow().snapshot().completed_operations, 0);

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
