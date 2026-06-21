//! Wraps `accept_async` in a timeout.
//!
//! The listener has no client, so the expected result is the timeout branch;
//! this makes timer integration visible without any networking noise.
mod support;
use std::net::TcpListener;
use std::time::Duration;

use sitas::executor::{accept_timeout_async, block_on};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::announce("async_tcp_timeout");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;

    let error = block_on(async move {
        accept_timeout_async(&listener, Duration::from_millis(10))
            .await
            .unwrap_err()
    });

    println!("accept timed out: {}", error.kind());

    Ok(())
}
