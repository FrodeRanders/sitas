use std::time::Duration;

use sitas::executor::{block_on, race, sleep, RaceOutput};

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
