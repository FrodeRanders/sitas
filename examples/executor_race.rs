//! Races two futures and keeps the first result.
//!
//! The slower branch is dropped when the faster branch completes, which is the
//! cancellation behavior higher-level timeout helpers build on.
use std::time::Duration;

use sitas::executor::{RaceOutput, block_on, race, sleep};

fn main() {
    let winner = block_on(async {
        race(
            async {
                sleep(Duration::from_millis(5)).await;
                "fast"
            },
            async {
                sleep(Duration::from_millis(50)).await;
                "slow"
            },
        )
        .await
    });

    match winner {
        RaceOutput::First(value) => println!("first future won: {value}"),
        RaceOutput::Second(value) => println!("second future won: {value}"),
    }
}
