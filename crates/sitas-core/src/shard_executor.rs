//! Minimal per-shard futures executor driven by a [`ReactorBackend`].
//!
//! This is the sitas-side half of the co-designed unified shard wait
//! (CharlotteOS architecture doc §7): one executor runs one shard's tasks,
//! and when nothing is runnable the shard blocks in a single reactor wait.
//! The loop is:
//!
//! 1. poll ready tasks, up to a **budget** per iteration;
//! 2. if ready tasks remain after the budget, loop again (the budget is a
//!    fairness yield point, not a scheduling boundary);
//! 3. otherwise block in [`ReactorBackend::wait`] until an event or a wake
//!    arrives;
//! 4. wake the tasks registered for the returned readable handles (drained
//!    completion-queue events), and re-run.
//!
//! Task wakeup is Rust-native: each task has a [`core::task::Waker`] that
//! marks the task ready **and** wakes the reactor, so a wake from another
//! shard (for example a message arriving on a [`ShardReceiver`]) releases a
//! blocked reactor wait and re-polls exactly the affected task. Wakes
//! coalesce: a task woken several times before it is polled is queued once.
//!
//! [`ShardReceiver`]: crate::shard_runtime::ShardReceiver

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::task::Wake;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use spin::Mutex;

use crate::reactor_backend::{ReactorBackend, ReactorEvent, ReactorWaker};

/// How many ready tasks are polled per loop iteration before the executor
/// re-checks for external events. A small budget keeps a busy shard from
/// starving event intake; the default is generous for smoke workloads.
pub const DEFAULT_POLL_BUDGET: usize = 64;

struct ReadyQueue {
    queue: VecDeque<usize>,
    /// Wake coalescing: whether each task is already queued.
    queued: Vec<bool>,
}

struct ExecutorShared<W> {
    ready: Mutex<ReadyQueue>,
    reactor_waker: W,
}

impl<W: ReactorWaker> ExecutorShared<W> {
    fn mark_ready(&self, task: usize) {
        let mut ready = self.ready.lock();
        if let Some(queued) = ready.queued.get_mut(task) {
            if !*queued {
                *queued = true;
                ready.queue.push_back(task);
            }
        }
        drop(ready);
        // Release the reactor if the executor is blocked in `wait`. Waking an
        // executor that is running is a benign no-op (wakes coalesce).
        let _ = self.reactor_waker.wake();
    }

    fn take_ready(&self) -> Option<usize> {
        let mut ready = self.ready.lock();
        let task = ready.queue.pop_front()?;
        ready.queued[task] = false;
        Some(task)
    }

    fn has_ready(&self) -> bool {
        !self.ready.lock().queue.is_empty()
    }
}

/// The per-task waker: marks the task ready and wakes the reactor.
struct TaskWaker<W> {
    task: usize,
    shared: Arc<ExecutorShared<W>>,
}

impl<W: ReactorWaker + 'static> Wake for TaskWaker<W> {
    fn wake(self: Arc<Self>) {
        self.shared.mark_ready(self.task);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.mark_ready(self.task);
    }
}

/// Tasks waiting for an I/O handle (a drained completion-queue cookie on
/// CharlotteOS, a readable descriptor on Unix) register here; the run loop
/// wakes them when [`ReactorBackend::wait`] reports the handle ready.
pub struct IoInterests<H> {
    interests: Mutex<Vec<(H, Waker)>>,
}

impl<H: Copy + Eq> IoInterests<H> {
    fn new() -> Self {
        Self {
            interests: Mutex::new(Vec::new()),
        }
    }

    /// Register `waker` to be woken when `handle` becomes readable. A handle
    /// may have at most one registered waker; re-registering replaces it.
    pub fn register_readable(&self, handle: H, waker: Waker) {
        let mut interests = self.interests.lock();
        if let Some(entry) = interests.iter_mut().find(|(h, _)| *h == handle) {
            entry.1 = waker;
        } else {
            interests.push((handle, waker));
        }
    }

    fn read_handles(&self) -> Vec<H> {
        self.interests.lock().iter().map(|(h, _)| *h).collect()
    }

    fn wake_readable(&self, handle: H) {
        let mut interests = self.interests.lock();
        if let Some(index) = interests.iter().position(|(h, _)| *h == handle) {
            let (_, waker) = interests.swap_remove(index);
            waker.wake();
        }
    }
}

/// A single-shard futures executor: owns its tasks, polls them within a
/// budget, and parks in the reactor when nothing is runnable (§7).
pub struct ShardExecutor<R: ReactorBackend>
where
    R::Waker: 'static,
{
    reactor: R,
    tasks: Vec<Option<Pin<Box<dyn Future<Output = ()> + 'static>>>>,
    wakers: Vec<Waker>,
    shared: Arc<ExecutorShared<R::Waker>>,
    io: Arc<IoInterests<R::Handle>>,
    live: usize,
    poll_budget: usize,
    /// Upper bound on one blocking wait. `None` waits indefinitely for an
    /// event or wake; a bound makes the loop self-healing on backends whose
    /// wakes are shared/coalesced across shards (one process-wide CQ), where
    /// a wake meant for this executor can be consumed by a peer.
    idle_wait: Option<Duration>,
}

impl<R: ReactorBackend> ShardExecutor<R>
where
    R::Waker: 'static,
{
    pub fn new(reactor: R) -> Self {
        let reactor_waker = reactor.waker();
        Self {
            reactor,
            tasks: Vec::new(),
            wakers: Vec::new(),
            shared: Arc::new(ExecutorShared {
                ready: Mutex::new(ReadyQueue {
                    queue: VecDeque::new(),
                    queued: Vec::new(),
                }),
                reactor_waker,
            }),
            io: Arc::new(IoInterests::new()),
            live: 0,
            poll_budget: DEFAULT_POLL_BUDGET,
            idle_wait: None,
        }
    }

    /// Bound each blocking wait (see the field documentation).
    pub fn with_idle_wait(mut self, idle_wait: Option<Duration>) -> Self {
        self.idle_wait = idle_wait;
        self
    }

    /// Override the per-iteration poll budget.
    pub fn with_poll_budget(mut self, poll_budget: usize) -> Self {
        self.poll_budget = poll_budget.max(1);
        self
    }

    /// The shared I/O interest table, for futures that wait on a reactor
    /// handle (a completion cookie / readable descriptor).
    pub fn io(&self) -> Arc<IoInterests<R::Handle>> {
        Arc::clone(&self.io)
    }

    /// Add a task; it is immediately ready and will be polled by [`run`].
    ///
    /// [`run`]: ShardExecutor::run
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        let task = self.tasks.len();
        self.tasks.push(Some(Box::pin(future)));
        self.shared.ready.lock().queued.push(false);
        self.wakers.push(Waker::from(Arc::new(TaskWaker {
            task,
            shared: Arc::clone(&self.shared),
        })));
        self.live += 1;
        self.shared.mark_ready(task);
    }

    fn poll_task(&mut self, task: usize) {
        let Some(slot) = self.tasks.get_mut(task) else {
            return;
        };
        let Some(mut future) = slot.take() else {
            return;
        };
        let waker = self.wakers[task].clone();
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(()) => {
                self.live -= 1;
            }
            Poll::Pending => {
                self.tasks[task] = Some(future);
            }
        }
    }

    /// Run until every spawned task has completed (§7 loop: budgeted polling,
    /// then one blocking reactor wait, then wake tasks for drained events).
    pub fn run(&mut self) {
        while self.live > 0 {
            // 1) Budgeted polling of ready tasks.
            let mut polled = 0;
            while polled < self.poll_budget {
                let Some(task) = self.shared.take_ready() else {
                    break;
                };
                self.poll_task(task);
                polled += 1;
            }
            if self.live == 0 {
                break;
            }
            // 2) Budget exhausted with work remaining: yield point, no block.
            if self.shared.has_ready() {
                continue;
            }
            // 3) Nothing runnable: block until an event or a wake arrives.
            let read = self.io.read_handles();
            let Ok(event) = self.reactor.wait(&read, &[], self.idle_wait) else {
                continue;
            };
            // 4) Drained events wake the tasks that registered interest; an
            //    explicit wake has already queued its task via `mark_ready`.
            for handle in event.readable() {
                self.io.wake_readable(*handle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use crate::io;

    /// A reactor whose `wait` returns immediately, reporting a wake when one
    /// was posted and any handles marked ready by the test.
    struct MockReactor {
        woken: Arc<AtomicBool>,
        readable: Arc<Mutex<Vec<u64>>>,
    }

    #[derive(Clone)]
    struct MockWaker {
        woken: Arc<AtomicBool>,
    }

    impl ReactorWaker for MockWaker {
        fn wake(&self) -> io::Result<()> {
            self.woken.store(true, Ordering::Release);
            Ok(())
        }
    }

    struct MockEvent {
        woke: bool,
        readable: Vec<u64>,
    }

    impl ReactorEvent for MockEvent {
        type Handle = u64;
        fn woke(&self) -> bool {
            self.woke
        }
        fn readable(&self) -> &[u64] {
            &self.readable
        }
        fn writable(&self) -> &[u64] {
            &[]
        }
    }

    impl ReactorBackend for MockReactor {
        type Waker = MockWaker;
        type Handle = u64;
        type Event = MockEvent;

        fn waker(&self) -> MockWaker {
            MockWaker {
                woken: Arc::clone(&self.woken),
            }
        }

        fn wait(
            &self,
            read: &[u64],
            _write: &[u64],
            _timeout: Option<Duration>,
        ) -> io::Result<MockEvent> {
            let woke = self.woken.swap(false, Ordering::AcqRel);
            let mut ready = self.readable.lock();
            let readable: Vec<u64> =
                ready.iter().copied().filter(|h| read.contains(h)).collect();
            ready.retain(|h| !readable.contains(h));
            Ok(MockEvent { woke, readable })
        }
    }

    fn mock_executor() -> (ShardExecutor<MockReactor>, Arc<Mutex<Vec<u64>>>) {
        let readable = Arc::new(Mutex::new(Vec::new()));
        let reactor = MockReactor {
            woken: Arc::new(AtomicBool::new(false)),
            readable: Arc::clone(&readable),
        };
        (ShardExecutor::new(reactor), readable)
    }

    /// A future that yields (self-wakes) `n` times before completing.
    struct YieldTimes {
        remaining: usize,
        counter: Arc<AtomicUsize>,
    }

    impl Future for YieldTimes {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.remaining == 0 {
                self.counter.fetch_add(1, Ordering::AcqRel);
                Poll::Ready(())
            } else {
                self.remaining -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn budgeted_polling_completes_all_tasks() {
        let (executor, _) = mock_executor();
        let mut executor = executor.with_poll_budget(2);
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            executor.spawn(YieldTimes {
                remaining: 3,
                counter: Arc::clone(&done),
            });
        }
        executor.run();
        assert_eq!(done.load(Ordering::Acquire), 5);
    }

    /// One task signals another through a waker slot: the waiting task parks
    /// on `Pending` after registering its waker, and the signalling task's
    /// wake re-queues it — the futures-wakeup path.
    #[test]
    fn cross_task_wake_requeues_the_waiter() {
        struct Gate {
            open: AtomicBool,
            waker: Mutex<Option<Waker>>,
        }

        struct WaitGate(Arc<Gate>, Arc<AtomicUsize>);
        impl Future for WaitGate {
            type Output = ();
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0.open.load(Ordering::Acquire) {
                    self.1.fetch_add(1, Ordering::AcqRel);
                    return Poll::Ready(());
                }
                *self.0.waker.lock() = Some(cx.waker().clone());
                if self.0.open.load(Ordering::Acquire) {
                    self.1.fetch_add(1, Ordering::AcqRel);
                    return Poll::Ready(());
                }
                Poll::Pending
            }
        }

        let (mut executor, _) = mock_executor();
        let gate = Arc::new(Gate {
            open: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let done = Arc::new(AtomicUsize::new(0));
        executor.spawn(WaitGate(Arc::clone(&gate), Arc::clone(&done)));
        let opener_gate = Arc::clone(&gate);
        executor.spawn(YieldTimes {
            remaining: 2,
            counter: Arc::new(AtomicUsize::new(0)),
        });
        executor.spawn(async move {
            opener_gate.open.store(true, Ordering::Release);
            if let Some(waker) = opener_gate.waker.lock().take() {
                waker.wake();
            }
        });
        executor.run();
        assert_eq!(done.load(Ordering::Acquire), 1);
    }

    /// A task registered for an I/O handle is woken when the reactor reports
    /// the handle readable — the drained-event wakeup path.
    #[test]
    fn drained_event_wakes_registered_task() {
        struct WaitReadable {
            handle: u64,
            io: Arc<IoInterests<u64>>,
            ready: Arc<AtomicBool>,
            done: Arc<AtomicUsize>,
            registered: bool,
        }
        impl Future for WaitReadable {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.registered || self.ready.load(Ordering::Acquire) {
                    self.done.fetch_add(1, Ordering::AcqRel);
                    return Poll::Ready(());
                }
                let handle = self.handle;
                self.io.register_readable(handle, cx.waker().clone());
                self.registered = true;
                Poll::Pending
            }
        }

        let (mut executor, readable) = mock_executor();
        let done = Arc::new(AtomicUsize::new(0));
        executor.spawn(WaitReadable {
            handle: 7,
            io: executor.io(),
            ready: Arc::new(AtomicBool::new(false)),
            done: Arc::clone(&done),
            registered: false,
        });
        // The event "arrives" while the executor runs: mark handle 7 readable
        // so the first wait drains it.
        readable.lock().push(7);
        executor.run();
        assert_eq!(done.load(Ordering::Acquire), 1);
    }
}
