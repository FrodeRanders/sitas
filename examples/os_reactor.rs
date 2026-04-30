use sitas::os::OsReactor;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let reactor = OsReactor::new()?;
    let waker = reactor.waker();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        waker.wake().unwrap();
    });

    let event = reactor.wait(Some(Duration::from_secs(1)))?;
    println!("reactor woke: {}", event.woke);

    Ok(())
}
