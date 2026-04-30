//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

#[cfg(unix)]
use crate::os::{OsReactor, OsWaker};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Error returned when a task cannot be submitted to an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpawnError;

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "executor is not accepting tasks")
    }
}

impl Error for SpawnError {}

/// Handle used to submit futures to an [`Executor`].
#[derive(Debug)]
pub struct Spawner {
    scheduler: Arc<Scheduler>,
}

impl Clone for Spawner {
    fn clone(&self) -> Self {
        self.scheduler.add_spawner();

        Self {
            scheduler: Arc::clone(&self.scheduler),
        }
    }
}

impl Drop for Spawner {
    fn drop(&mut self) {
        self.scheduler.remove_spawner();
    }
}

impl Spawner {
    /// Spawns a future onto the executor's ready queue.
    pub fn spawn<F>(&self, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            scheduler: Arc::clone(&self.scheduler),
        });

        self.scheduler.schedule(task)
    }

    /// Spawns a future and returns a handle that can await its output.
    pub fn spawn_with_handle<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let shared = Arc::new(Mutex::new(JoinState {
            output: None,
            waker: None,
        }));
        let shared_for_task = Arc::clone(&shared);

        self.spawn(async move {
            let output = future.await;
            let waker = {
                let mut state = shared_for_task
                    .lock()
                    .expect("join handle state mutex poisoned");
                state.output = Some(output);
                state.waker.take()
            };

            if let Some(waker) = waker {
                waker.wake();
            }
        })?;

        Ok(JoinHandle { shared })
    }
}

/// Future returned by [`Spawner::spawn_with_handle`].
#[must_use = "join handles do nothing unless polled or awaited"]
pub struct JoinHandle<T> {
    shared: Arc<Mutex<JoinState<T>>>,
}

struct JoinState<T> {
    output: Option<T>,
    waker: Option<Waker>,
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .shared
            .lock()
            .expect("join handle state mutex poisoned");

        match state.output.take() {
            Some(output) => Poll::Ready(output),
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Single-threaded executor that polls tasks from a ready queue.
#[derive(Debug)]
pub struct Executor {
    scheduler: Arc<Scheduler>,
    #[cfg(unix)]
    reactor: OsReactor,
}

impl Executor {
    /// Runs tasks until all spawners and runnable tasks are gone.
    pub fn run(&self) {
        loop {
            while let Some(task) = self.scheduler.next_task() {
                task.poll();
            }

            if self.scheduler.is_drained() {
                break;
            }

            #[cfg(unix)]
            let _ = self
                .reactor
                .wait(None)
                .expect("OS reactor wait failed while running executor");
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.scheduler.close();
    }
}

struct Task {
    future: Mutex<Option<BoxFuture>>,
    scheduler: Arc<Scheduler>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").finish_non_exhaustive()
    }
}

impl Task {
    fn poll(self: Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut context = Context::from_waker(&waker);
        let mut future_slot = self.future.lock().expect("task future mutex poisoned");

        if let Some(mut future) = future_slot.take() {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(()) => {
                    self.scheduler.finish_task();
                }
                Poll::Pending => {
                    *future_slot = Some(future);
                }
            }
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let scheduler = Arc::clone(&self.scheduler);
        let _ = scheduler.schedule_existing(self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.scheduler.schedule_existing(self.clone());
    }
}

#[derive(Debug)]
struct Scheduler {
    state: Mutex<SchedulerState>,
    #[cfg(unix)]
    waker: OsWaker,
}

#[derive(Debug)]
struct SchedulerState {
    queue: VecDeque<Arc<Task>>,
    accepting: bool,
    spawner_count: usize,
    task_count: usize,
}

impl Scheduler {
    fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                queue: VecDeque::new(),
                accepting: true,
                spawner_count: 1,
                task_count: 0,
            }),
            #[cfg(unix)]
            waker,
        }
    }

    fn add_spawner(&self) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.spawner_count += 1;
    }

    fn remove_spawner(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.spawner_count = state.spawner_count.saturating_sub(1);
            state.spawner_count == 0
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn schedule(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                return Err(SpawnError);
            }
            state.task_count += 1;
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    fn schedule_existing(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                return Err(SpawnError);
            }
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    fn next_task(&self) -> Option<Arc<Task>> {
        self.state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .pop_front()
    }

    fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
    }

    fn close(&self) {
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.accepting = false;
        }

        self.wake_reactor();
    }

    fn finish_task(&self) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.task_count = state.task_count.saturating_sub(1);
            state.queue.is_empty() && state.spawner_count == 0 && state.task_count == 0
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn wake_reactor(&self) {
        #[cfg(unix)]
        let _ = self.waker.wake();
    }
}

/// Creates a paired executor and spawner.
pub fn executor_and_spawner() -> (Executor, Spawner) {
    #[cfg(unix)]
    {
        let reactor = OsReactor::new().expect("failed to create OS reactor for executor");
        let scheduler = Arc::new(Scheduler::new(reactor.waker()));

        (
            Executor {
                scheduler: Arc::clone(&scheduler),
                reactor,
            },
            Spawner { scheduler },
        )
    }
}

/// Runs one future to completion on a fresh single-threaded executor.
///
/// This is intentionally small and requires `Send + 'static` futures because it
/// is implemented by spawning the root future into the executor.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (executor, spawner) = executor_and_spawner();
    let result = Arc::new(Mutex::new(None));
    let result_for_task = Arc::clone(&result);

    spawner
        .spawn(async move {
            let output = future.await;
            *result_for_task
                .lock()
                .expect("block_on result mutex poisoned") = Some(output);
        })
        .expect("fresh executor should accept root future");
    drop(spawner);

    executor.run();

    let output = result
        .lock()
        .expect("block_on result mutex poisoned")
        .take()
        .expect("root future completed without producing a result");
    output
}

/// Returns a future that yields once before completing.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by [`yield_now`].
#[derive(Debug, Clone, Copy)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{block_on, executor_and_spawner, yield_now};
    use std::sync::{Arc, Mutex};

    #[test]
    fn block_on_returns_future_output() {
        assert_eq!(block_on(async { 42 }), 42);
    }

    #[test]
    fn yield_now_yields_once_before_completion() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_for_task = Arc::clone(&events);

        block_on(async move {
            events_for_task.lock().unwrap().push("before");
            yield_now().await;
            events_for_task.lock().unwrap().push("after");
        });

        assert_eq!(&*events.lock().unwrap(), &["before", "after"]);
    }

    #[test]
    fn executor_runs_multiple_spawned_tasks() {
        let (executor, spawner) = executor_and_spawner();
        let values = Arc::new(Mutex::new(Vec::new()));

        for value in 0..3 {
            let values_for_task = Arc::clone(&values);
            spawner
                .spawn(async move {
                    yield_now().await;
                    values_for_task.lock().unwrap().push(value);
                })
                .unwrap();
        }

        drop(spawner);
        executor.run();

        let mut values = values.lock().unwrap().clone();
        values.sort();
        assert_eq!(values, vec![0, 1, 2]);
    }

    #[test]
    fn spawn_with_handle_returns_task_output() {
        let (executor, spawner) = executor_and_spawner();
        let result = Arc::new(Mutex::new(None));
        let result_for_task = Arc::clone(&result);

        let worker = spawner
            .spawn_with_handle(async {
                yield_now().await;
                7
            })
            .unwrap();

        spawner
            .spawn(async move {
                let output = worker.await;
                *result_for_task.lock().unwrap() = Some(output);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some(7));
    }

    #[test]
    fn tasks_can_await_multiple_join_handles() {
        let (executor, spawner) = executor_and_spawner();
        let result = Arc::new(Mutex::new(None));
        let result_for_task = Arc::clone(&result);

        let first = spawner
            .spawn_with_handle(async {
                yield_now().await;
                2
            })
            .unwrap();
        let second = spawner
            .spawn_with_handle(async {
                yield_now().await;
                yield_now().await;
                5
            })
            .unwrap();

        spawner
            .spawn(async move {
                *result_for_task.lock().unwrap() = Some(first.await + second.await);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some(7));
    }

    #[test]
    fn block_on_can_await_spawned_task_output() {
        let (executor, spawner) = executor_and_spawner();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        let spawner_for_root = spawner.clone();
        spawner
            .spawn(async move {
                let worker = spawner_for_root
                    .spawn_with_handle(async {
                        yield_now().await;
                        "done"
                    })
                    .unwrap();

                *output_for_task.lock().unwrap() = Some(worker.await);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some("done"));
    }

    #[test]
    fn spawner_reports_closed_executor() {
        let (executor, spawner) = executor_and_spawner();
        drop(executor);

        assert!(spawner.spawn(async {}).is_err());
    }
}
