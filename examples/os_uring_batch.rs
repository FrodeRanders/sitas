#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use sitas::os::{
        IoUringDispatcher, IoUringOperationFuture, IoUringOperationKind, block_on_io_uring_all,
    };
    use std::rc::Rc;

    let Some(ring) = available_ring()? else {
        println!("io_uring unavailable on this Linux host");
        return Ok(());
    };

    let dispatcher = IoUringDispatcher::new(ring).into_shared();
    let futures = [
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
        IoUringOperationFuture::queue_nop(Rc::clone(&dispatcher))?,
    ];
    let operations: Vec<_> = futures.iter().map(|future| future.operation()).collect();
    let completions = block_on_io_uring_all(Rc::clone(&dispatcher), futures)?;

    for completion in &completions {
        println!(
            "completed operation {} with result {}",
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
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("io_uring is Linux-only");
}
