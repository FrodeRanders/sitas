use std::time::Duration;

use sitas::executor::{block_on, sleep, timeout};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fast = block_on(async {
        timeout(Duration::from_millis(50), async {
            sleep(Duration::from_millis(5)).await;
            "fast future completed"
        })
        .await
    })?;

    let slow = block_on(async {
        timeout(Duration::from_millis(5), async {
            sleep(Duration::from_millis(50)).await;
            "slow future completed"
        })
        .await
    });

    println!("{fast}");
    println!("slow future timed out: {}", slow.is_err());

    Ok(())
}
