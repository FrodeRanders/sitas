#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{
        IoUringDispatcher, IoUringOperationFuture, IoUringOperationKind, block_on_io_uring_all,
    };
    use std::rc::Rc;

    let Some(ring) = available_ring()? else {
        report_unavailable();
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
        "buffered completions: total={} nops={}",
        ready.completed_operations, ready.completed_operation_kinds.nops
    );
    assert_eq!(ready.completed_operations, 3);
    assert_eq!(ready.completed_operation_kinds.nops, 3);

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
