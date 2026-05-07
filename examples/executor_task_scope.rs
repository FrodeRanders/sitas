use sitas::executor::{TaskScope, executor_and_spawner, sleep};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn main() {
    let (executor, spawner) = executor_and_spawner();
    let mut scope = TaskScope::new(spawner.clone());
    let events = Arc::new(Mutex::new(Vec::new()));

    for name in ["first", "second"] {
        let events = Arc::clone(&events);
        scope
            .spawn_with_stop(move |stop| async move {
                stop.await;
                events.lock().unwrap().push(name);
            })
            .unwrap();
    }

    let events = Arc::clone(&events);
    executor.run_until(async move {
        sleep(Duration::from_millis(10)).await;
        scope.shutdown().await.unwrap();

        let mut events = events.lock().unwrap().clone();
        events.sort();
        println!("scope stopped children: {}", events.join(", "));
    });

    drop(spawner);
}
