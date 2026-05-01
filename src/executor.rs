//! A minimal async executor experiment.
//!
//! This module is intentionally small. It exists to expose the core mechanics
//! behind async task execution: tasks own pinned futures, wakers re-enqueue
//! ready tasks, and an executor repeatedly polls tasks from a ready queue. On
//! Unix, the `non-std-runtime` branch parks the executor on an OS reactor wake
//! source when no tasks are ready.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
#[cfg(unix)]
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

#[cfg(unix)]
use crate::os::{tcp_connect_start, OsReactor, OsWaker};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type PanicPayload = Box<dyn std::any::Any + Send + 'static>;
type PanicHandler = Box<dyn FnOnce(PanicPayload) + Send + 'static>;
type JoinResult<T> = Result<T, JoinError>;

const READY_POLL_BUDGET: usize = 64;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
}

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
        self.spawn_with_panic_handler(future, None).map(|_| ())
    }

    /// Spawns a future and returns a handle that can await its output.
    pub fn spawn_with_handle<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let shared = Arc::new(Mutex::new(JoinState {
            result: None,
            waker: None,
        }));
        let shared_for_task = Arc::clone(&shared);
        let shared_for_panic = Arc::clone(&shared);

        let task = self.spawn_with_panic_handler(
            async move {
                let output = future.await;
                complete_join(&shared_for_task, Ok(output));
            },
            Some(Box::new(move |payload| {
                complete_join(&shared_for_panic, Err(JoinError::Panic(payload)));
            })),
        )?;

        Ok(JoinHandle { shared, task })
    }

    fn spawn_with_panic_handler<F>(
        &self,
        future: F,
        panic_handler: Option<PanicHandler>,
    ) -> Result<Arc<Task>, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task::new(
            Box::pin(future),
            Arc::clone(&self.scheduler),
            panic_handler,
        ));

        self.scheduler.schedule(Arc::clone(&task))?;
        Ok(task)
    }
}

/// Future returned by [`Spawner::spawn_with_handle`].
#[must_use = "join handles do nothing unless polled or awaited"]
pub struct JoinHandle<T> {
    shared: Arc<Mutex<JoinState<T>>>,
    task: Arc<Task>,
}

struct JoinState<T> {
    result: Option<JoinResult<T>>,
    waker: Option<Waker>,
}

/// Error returned by a [`JoinHandle`] when a spawned task did not produce a
/// value.
pub enum JoinError {
    /// The task was aborted before it completed.
    Cancelled,
    /// The task panicked while it was being polled.
    Panic(PanicPayload),
}

impl JoinError {
    /// Returns true if the task was aborted before completion.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, JoinError::Cancelled)
    }

    /// Returns true if the task panicked while it was being polled.
    pub fn is_panic(&self) -> bool {
        matches!(self, JoinError::Panic(_))
    }

    /// Consumes the error and returns the panic payload if the task panicked.
    pub fn into_panic(self) -> Option<PanicPayload> {
        match self {
            JoinError::Cancelled => None,
            JoinError::Panic(payload) => Some(payload),
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => f.write_str("Cancelled"),
            JoinError::Panic(_) => f.write_str("Panic(..)"),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => write!(f, "task was cancelled"),
            JoinError::Panic(_) => write!(f, "task panicked"),
        }
    }
}

impl Error for JoinError {}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> JoinHandle<T> {
    /// Aborts the task if it has not completed yet.
    ///
    /// Awaiting this handle after a successful abort returns
    /// [`JoinError::Cancelled`].
    pub fn abort(&self) -> bool {
        if !self.task.cancel() {
            return false;
        }

        complete_join(&self.shared, Err(JoinError::Cancelled));
        true
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = JoinResult<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .shared
            .lock()
            .expect("join handle state mutex poisoned");

        match state.result.take() {
            Some(result) => Poll::Ready(result),
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

fn complete_join<T>(shared: &Arc<Mutex<JoinState<T>>>, result: JoinResult<T>) {
    let waker = {
        let mut state = shared.lock().expect("join handle state mutex poisoned");
        if state.result.is_some() {
            None
        } else {
            state.result = Some(result);
            state.waker.take()
        }
    };

    if let Some(waker) = waker {
        waker.wake();
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
            self.poll_ready_tasks();

            self.scheduler.wake_expired_timers();

            if self.scheduler.is_drained() {
                break;
            }

            if self.scheduler.has_ready_tasks() {
                continue;
            }

            #[cfg(unix)]
            {
                let event = self
                    .reactor
                    .wait_io(
                        &self.scheduler.read_interest_fds(),
                        &self.scheduler.write_interest_fds(),
                        self.scheduler.time_until_next_timer(),
                    )
                    .expect("OS reactor wait failed while running executor");
                self.scheduler.wake_readable_fds(&event.readable);
                self.scheduler.wake_writable_fds(&event.writable);
            }

            self.scheduler.wake_expired_timers();
        }
    }

    /// Runs `future` to completion while also driving spawned executor tasks.
    pub fn run_until<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let root = Arc::new(RootWaker::new(Arc::clone(&self.scheduler)));
        let waker = Waker::from(Arc::clone(&root));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            if root.take_ready() {
                CURRENT_SCHEDULER.with(|current| {
                    *current.borrow_mut() = Some(Arc::clone(&self.scheduler));
                });

                let poll_result =
                    panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

                CURRENT_SCHEDULER.with(|current| {
                    *current.borrow_mut() = None;
                });

                match poll_result {
                    Ok(Poll::Ready(output)) => return output,
                    Ok(Poll::Pending) => {}
                    Err(payload) => panic::resume_unwind(payload),
                }
            }

            self.poll_ready_tasks();

            self.scheduler.wake_expired_timers();

            if root.is_ready() || self.scheduler.has_ready_tasks() {
                continue;
            }

            #[cfg(unix)]
            {
                let event = self
                    .reactor
                    .wait_io(
                        &self.scheduler.read_interest_fds(),
                        &self.scheduler.write_interest_fds(),
                        self.scheduler.time_until_next_timer(),
                    )
                    .expect("OS reactor wait failed while running root future");
                self.scheduler.wake_readable_fds(&event.readable);
                self.scheduler.wake_writable_fds(&event.writable);
            }

            self.scheduler.wake_expired_timers();
        }
    }

    fn poll_ready_tasks(&self) {
        for _ in 0..READY_POLL_BUDGET {
            let Some(task) = self.scheduler.next_task() else {
                break;
            };
            task.poll();
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.scheduler.close();
    }
}

struct RootWaker {
    ready: Mutex<bool>,
    scheduler: Arc<Scheduler>,
}

impl RootWaker {
    fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            ready: Mutex::new(true),
            scheduler,
        }
    }

    fn take_ready(&self) -> bool {
        let mut ready = self.ready.lock().expect("root waker mutex poisoned");
        let was_ready = *ready;
        *ready = false;
        was_ready
    }

    fn is_ready(&self) -> bool {
        *self.ready.lock().expect("root waker mutex poisoned")
    }

    fn mark_ready(&self) {
        *self.ready.lock().expect("root waker mutex poisoned") = true;
        self.scheduler.wake_reactor();
    }
}

impl fmt::Debug for RootWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootWaker").finish_non_exhaustive()
    }
}

impl Wake for RootWaker {
    fn wake(self: Arc<Self>) {
        self.mark_ready();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark_ready();
    }
}

struct Task {
    state: Mutex<TaskState>,
    scheduler: Arc<Scheduler>,
}

struct TaskState {
    future: Option<BoxFuture>,
    panic_handler: Option<PanicHandler>,
    queued: bool,
    polling: bool,
    cancel_requested: bool,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").finish_non_exhaustive()
    }
}

impl Task {
    fn new(
        future: BoxFuture,
        scheduler: Arc<Scheduler>,
        panic_handler: Option<PanicHandler>,
    ) -> Self {
        Self {
            state: Mutex::new(TaskState {
                future: Some(future),
                panic_handler,
                queued: false,
                polling: false,
                cancel_requested: false,
            }),
            scheduler,
        }
    }

    fn poll(self: Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut context = Context::from_waker(&waker);
        let mut future = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            state.queued = false;

            let Some(future) = state.future.take() else {
                return;
            };
            state.polling = true;
            future
        };

        let scheduler = Arc::clone(&self.scheduler);
        CURRENT_SCHEDULER.with(|current| {
            *current.borrow_mut() = Some(scheduler);
        });

        let poll_result =
            panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

        CURRENT_SCHEDULER.with(|current| {
            *current.borrow_mut() = None;
        });

        match poll_result {
            Ok(Poll::Ready(())) => {
                self.state
                    .lock()
                    .expect("task state mutex poisoned")
                    .polling = false;
                self.scheduler.finish_task();
            }
            Ok(Poll::Pending) => {
                let cancelled = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    if state.cancel_requested {
                        true
                    } else {
                        state.future = Some(future);
                        false
                    }
                };

                if cancelled {
                    self.scheduler.finish_task();
                    self.scheduler.wake_reactor();
                }
            }
            Err(payload) => {
                let panic_handler = {
                    let mut state = self.state.lock().expect("task state mutex poisoned");
                    state.polling = false;
                    state.future = None;
                    state.panic_handler.take()
                };

                if let Some(panic_handler) = panic_handler {
                    panic_handler(payload);
                }
                self.scheduler.finish_task();
            }
        }
    }

    fn mark_queued(&self) -> bool {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        if state.queued || (state.future.is_none() && !state.polling) {
            return false;
        }

        state.queued = true;
        true
    }

    fn cancel(&self) -> bool {
        let should_finish = {
            let mut state = self.state.lock().expect("task state mutex poisoned");
            if state.future.is_none() && !state.polling {
                return false;
            }

            state.cancel_requested = true;

            if state.polling {
                false
            } else {
                state.future = None;
                state.queued = false;
                true
            }
        };

        if should_finish {
            self.scheduler.finish_task();
        }
        self.scheduler.wake_reactor();
        true
    }

    fn drop_future(&self) {
        let mut state = self.state.lock().expect("task state mutex poisoned");
        state.cancel_requested = true;
        if !state.polling {
            state.future = None;
            state.queued = false;
        }
    }

    fn clear_queued(&self) {
        self.state.lock().expect("task state mutex poisoned").queued = false;
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
    tasks: Vec<Weak<Task>>,
    timers: Vec<TimerEntry>,
    #[cfg(unix)]
    read_interests: InterestSet,
    #[cfg(unix)]
    write_interests: InterestSet,
    accepting: bool,
    spawner_count: usize,
    task_count: usize,
    next_timer_id: usize,
}

#[derive(Debug)]
struct TimerEntry {
    id: usize,
    deadline: Instant,
    waker: Waker,
}

#[cfg(unix)]
#[derive(Debug)]
struct InterestSet {
    interests: Vec<IoInterest>,
    ready: Vec<usize>,
    next_id: usize,
}

#[cfg(unix)]
#[derive(Debug)]
struct IoInterest {
    id: usize,
    fd: RawFd,
    waker: Waker,
}

#[cfg(unix)]
impl InterestSet {
    fn new() -> Self {
        Self {
            interests: Vec::new(),
            ready: Vec::new(),
            next_id: 0,
        }
    }

    fn allocate_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    fn register(&mut self, id: usize, fd: RawFd, waker: Waker) {
        match self.interests.iter_mut().find(|interest| interest.id == id) {
            Some(interest) => {
                interest.fd = fd;
                interest.waker = waker;
            }
            None => self.interests.push(IoInterest { id, fd, waker }),
        }
    }

    fn remove(&mut self, id: usize) {
        self.interests.retain(|interest| interest.id != id);
        self.ready.retain(|ready_id| *ready_id != id);
    }

    fn clear(&mut self) {
        self.interests.clear();
        self.ready.clear();
    }

    fn fds(&self) -> Vec<RawFd> {
        self.interests.iter().map(|interest| interest.fd).collect()
    }

    fn wake_ready(&mut self, ready_fds: &[RawFd]) -> Vec<Waker> {
        let mut wakers = Vec::new();
        let mut ready_ids = Vec::new();
        let mut pending = Vec::with_capacity(self.interests.len());

        for interest in self.interests.drain(..) {
            if ready_fds.contains(&interest.fd) {
                ready_ids.push(interest.id);
                wakers.push(interest.waker);
            } else {
                pending.push(interest);
            }
        }

        self.interests = pending;
        self.ready.extend(ready_ids);
        wakers
    }

    fn take_ready(&mut self, id: usize) -> bool {
        let Some(position) = self.ready.iter().position(|ready_id| *ready_id == id) else {
            return false;
        };

        self.ready.swap_remove(position);
        true
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.interests.is_empty() && self.ready.is_empty()
    }
}

impl Scheduler {
    fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                queue: VecDeque::new(),
                tasks: Vec::new(),
                timers: Vec::new(),
                #[cfg(unix)]
                read_interests: InterestSet::new(),
                #[cfg(unix)]
                write_interests: InterestSet::new(),
                accepting: true,
                spawner_count: 1,
                task_count: 0,
                next_timer_id: 0,
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
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                task.clear_queued();
                return Err(SpawnError);
            }
            state.task_count += 1;
            state.tasks.push(Arc::downgrade(&task));
            state.queue.push_back(task);
        }

        self.wake_reactor();
        Ok(())
    }

    fn schedule_existing(&self, task: Arc<Task>) -> Result<(), SpawnError> {
        if !task.mark_queued() {
            return Ok(());
        }

        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            if !state.accepting {
                task.clear_queued();
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

    fn has_ready_tasks(&self) -> bool {
        !self
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .queue
            .is_empty()
    }

    fn close(&self) {
        let tasks = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.accepting = false;
            state.task_count = 0;
            state.queue.clear();
            state.timers.clear();
            #[cfg(unix)]
            {
                state.read_interests.clear();
                state.write_interests.clear();
            }

            state
                .tasks
                .drain(..)
                .filter_map(|task| task.upgrade())
                .collect::<Vec<_>>()
        };

        for task in tasks {
            task.drop_future();
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

    fn allocate_timer_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        let id = state.next_timer_id;
        state.next_timer_id = state.next_timer_id.wrapping_add(1);
        id
    }

    fn register_timer(&self, id: usize, deadline: Instant, waker: Waker) {
        let should_wake = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let previous_next = next_timer_deadline(&state.timers);

            match state.timers.iter_mut().find(|timer| timer.id == id) {
                Some(timer) => {
                    timer.deadline = deadline;
                    timer.waker = waker;
                }
                None => state.timers.push(TimerEntry {
                    id,
                    deadline,
                    waker,
                }),
            }

            previous_next.map_or(true, |previous| deadline < previous)
        };

        if should_wake {
            self.wake_reactor();
        }
    }

    fn remove_timer(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.timers.retain(|timer| timer.id != id);
    }

    fn wake_expired_timers(&self) {
        let expired = {
            let now = Instant::now();
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            let mut expired = Vec::new();
            let mut pending = Vec::with_capacity(state.timers.len());

            for timer in state.timers.drain(..) {
                if timer.deadline <= now {
                    expired.push(timer.waker);
                } else {
                    pending.push(timer);
                }
            }

            state.timers = pending;
            expired
        };

        for waker in expired {
            waker.wake();
        }
    }

    fn time_until_next_timer(&self) -> Option<Duration> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        let deadline = next_timer_deadline(&state.timers)?;
        Some(deadline.saturating_duration_since(Instant::now()))
    }

    #[cfg(unix)]
    fn allocate_read_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.allocate_id()
    }

    #[cfg(unix)]
    fn register_read_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    fn remove_read_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.remove(id);
    }

    #[cfg(unix)]
    fn read_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.fds()
    }

    #[cfg(unix)]
    fn wake_readable_fds(&self, readable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.read_interests.wake_ready(readable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    fn take_ready_read_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.read_interests.take_ready(id)
    }

    #[cfg(unix)]
    fn allocate_write_interest_id(&self) -> usize {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.allocate_id()
    }

    #[cfg(unix)]
    fn register_write_interest(&self, id: usize, fd: RawFd, waker: Waker) {
        {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.register(id, fd, waker);
        }

        self.wake_reactor();
    }

    #[cfg(unix)]
    fn remove_write_interest(&self, id: usize) {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.remove(id);
    }

    #[cfg(unix)]
    fn write_interest_fds(&self) -> Vec<RawFd> {
        let state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.fds()
    }

    #[cfg(unix)]
    fn wake_writable_fds(&self, writable: &[RawFd]) {
        let wakers = {
            let mut state = self.state.lock().expect("scheduler state mutex poisoned");
            state.write_interests.wake_ready(writable)
        };

        for waker in wakers {
            waker.wake();
        }
    }

    #[cfg(unix)]
    fn take_ready_write_interest(&self, id: usize) -> bool {
        let mut state = self.state.lock().expect("scheduler state mutex poisoned");
        state.write_interests.take_ready(id)
    }

    fn wake_reactor(&self) {
        #[cfg(unix)]
        let _ = self.waker.wake();
    }
}

fn next_timer_deadline(timers: &[TimerEntry]) -> Option<Instant> {
    timers.iter().map(|timer| timer.deadline).min()
}

fn current_scheduler() -> Arc<Scheduler> {
    CURRENT_SCHEDULER
        .with(|current| current.borrow().as_ref().cloned())
        .expect("executor futures must be polled by sitas::executor::Executor")
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
/// The root future is polled directly by the executor, so it may borrow from
/// the caller's stack.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let (executor, spawner) = executor_and_spawner();
    drop(spawner);

    executor.run_until(future)
}

/// Returns a future that completes after `duration`.
///
/// This future is driven by the executor's internal timer list. It must be
/// polled by this module's [`Executor`].
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
        timer_id: None,
        scheduler: None,
    }
}

/// Returns a future that resolves to an error if `future` does not complete
/// before `duration` elapses.
pub fn timeout<F>(duration: Duration, future: F) -> Timeout<F>
where
    F: Future,
{
    Timeout {
        future: Box::pin(future),
        sleep: sleep(duration),
    }
}

/// Returns a future that completes with whichever input future completes first.
pub fn race<A, B>(first: A, second: B) -> Race<A, B>
where
    A: Future,
    B: Future,
{
    Race {
        first: Some(Box::pin(first)),
        second: Some(Box::pin(second)),
    }
}

/// Returns a future that completes when `fd` is readable.
#[cfg(unix)]
pub fn readable(fd: RawFd) -> Readable {
    Readable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Returns a future that completes when `fd` is writable.
#[cfg(unix)]
pub fn writable(fd: RawFd) -> Writable {
    Writable {
        fd,
        interest_id: None,
        scheduler: None,
    }
}

/// Reads exactly enough bytes to fill `buffer`, awaiting read readiness when
/// the reader would otherwise block.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn read_exact_async<R>(reader: &mut R, buffer: &mut [u8]) -> io::Result<()>
where
    R: Read + AsRawFd,
{
    let mut filled = 0usize;

    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "reader reached EOF before buffer was filled",
                ));
            }
            Ok(bytes_read) => {
                filled += bytes_read;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(reader.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Reads exactly enough bytes to fill `buffer`, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn read_exact_timeout_async<R>(
    reader: &mut R,
    buffer: &mut [u8],
    duration: Duration,
) -> io::Result<()>
where
    R: Read + AsRawFd,
{
    timeout_io(duration, read_exact_async(reader, buffer)).await
}

/// Writes the entire buffer, awaiting write readiness when the writer would
/// otherwise block.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn write_all_async<W>(writer: &mut W, buffer: &[u8]) -> io::Result<()>
where
    W: Write + AsRawFd,
{
    let mut written = 0usize;

    while written < buffer.len() {
        match writer.write(&buffer[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "writer accepted zero bytes before buffer was written",
                ));
            }
            Ok(bytes_written) => {
                written += bytes_written;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                writable(writer.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Writes the entire buffer, failing with `io::ErrorKind::TimedOut` if
/// `duration` elapses first.
///
/// The caller is responsible for putting the underlying descriptor in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn write_all_timeout_async<W>(
    writer: &mut W,
    buffer: &[u8],
    duration: Duration,
) -> io::Result<()>
where
    W: Write + AsRawFd,
{
    timeout_io(duration, write_all_async(writer, buffer)).await
}

/// Copies bytes from `reader` to `writer` until `reader` reaches EOF, awaiting
/// descriptor readiness whenever either side would otherwise block.
///
/// The caller is responsible for putting both underlying descriptors in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn copy_async<R, W>(reader: &mut R, writer: &mut W, buffer: &mut [u8]) -> io::Result<u64>
where
    R: Read + AsRawFd,
    W: Write + AsRawFd,
{
    if buffer.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy buffer must not be empty",
        ));
    }

    let mut copied = 0u64;

    loop {
        match reader.read(buffer) {
            Ok(0) => return Ok(copied),
            Ok(bytes_read) => {
                write_all_async(writer, &buffer[..bytes_read]).await?;
                copied += bytes_read as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(reader.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Copies bytes from `reader` to `writer`, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The caller is responsible for putting both underlying descriptors in
/// non-blocking mode before using this helper.
#[cfg(unix)]
pub async fn copy_timeout_async<R, W>(
    reader: &mut R,
    writer: &mut W,
    buffer: &mut [u8],
    duration: Duration,
) -> io::Result<u64>
where
    R: Read + AsRawFd,
    W: Write + AsRawFd,
{
    timeout_io(duration, copy_async(reader, writer, buffer)).await
}

/// Accepts one TCP connection, awaiting listener readiness when accepting
/// would otherwise block.
///
/// The caller is responsible for putting the listener in non-blocking mode
/// before using this helper. The returned stream is placed in non-blocking mode
/// before it is returned.
#[cfg(unix)]
pub async fn accept_async(listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                stream.set_nonblocking(true)?;
                return Ok((stream, peer));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(listener.as_raw_fd()).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Accepts one TCP connection, failing with `io::ErrorKind::TimedOut` if
/// `duration` elapses first.
///
/// The caller is responsible for putting the listener in non-blocking mode
/// before using this helper. The returned stream is placed in non-blocking mode
/// before it is returned.
#[cfg(unix)]
pub async fn accept_timeout_async(
    listener: &TcpListener,
    duration: Duration,
) -> io::Result<(TcpStream, SocketAddr)> {
    timeout_io(duration, accept_async(listener)).await
}

/// Accepts `connection_count` TCP connections and spawns one handler task for
/// each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Handler
/// futures run concurrently on `spawner`; this future waits for all spawned
/// handlers before returning.
#[cfg(unix)]
pub async fn serve_tcp_n<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    connection_count: usize,
    mut handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::with_capacity(connection_count);

    for _ in 0..connection_count {
        let (stream, peer) = accept_async(&listener).await?;
        handlers.push(
            spawner
                .spawn_with_handle(handler(stream, peer))
                .map_err(spawn_error_to_io)?,
        );
    }

    join_tcp_handlers(handlers).await?;

    Ok(())
}

/// Accepts TCP connections until `idle_timeout` elapses without a new
/// connection, spawning one handler task for each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Handler
/// futures run concurrently on `spawner`; this future waits for all spawned
/// handlers before returning the number of accepted connections.
#[cfg(unix)]
pub async fn serve_tcp_until_idle<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    idle_timeout: Duration,
    mut handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::new();

    loop {
        match accept_timeout_async(&listener, idle_timeout).await {
            Ok((stream, peer)) => {
                handlers.push(
                    spawner
                        .spawn_with_handle(handler(stream, peer))
                        .map_err(spawn_error_to_io)?,
                );
            }
            Err(error) if error.kind() == io::ErrorKind::TimedOut => break,
            Err(error) => return Err(error),
        }
    }

    let accepted = handlers.len();
    join_tcp_handlers(handlers).await?;

    Ok(accepted)
}

#[cfg(unix)]
async fn join_tcp_handlers(handlers: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    for handler in handlers {
        handler.await.map_err(join_error_to_io)??;
    }
    Ok(())
}

/// Connects to a TCP address without blocking the executor.
///
/// The returned stream is non-blocking.
#[cfg(unix)]
pub async fn connect_async(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = tcp_connect_start(address)?;
    writable(stream.as_raw_fd()).await;

    match stream.take_error()? {
        Some(error) => Err(error),
        None => Ok(stream),
    }
}

/// Connects to a TCP address without blocking the executor, failing with
/// `io::ErrorKind::TimedOut` if `duration` elapses first.
///
/// The returned stream is non-blocking.
#[cfg(unix)]
pub async fn connect_timeout_async(
    address: SocketAddr,
    duration: Duration,
) -> io::Result<TcpStream> {
    timeout_io(duration, connect_async(address)).await
}

#[cfg(unix)]
async fn timeout_io<F, T>(duration: Duration, future: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout(duration, future).await {
        Ok(result) => result,
        Err(TimeoutError) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "async operation timed out",
        )),
    }
}

#[cfg(unix)]
fn spawn_error_to_io(error: SpawnError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error)
}

#[cfg(unix)]
fn join_error_to_io(error: JoinError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
}

/// Future returned by [`readable`].
#[cfg(unix)]
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Readable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

#[cfg(unix)]
impl Future for Readable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_read_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_read_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_read_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

#[cfg(unix)]
impl Drop for Readable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_read_interest(interest_id);
        }
    }
}

/// Future returned by [`writable`].
#[cfg(unix)]
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Writable {
    fd: RawFd,
    interest_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

#[cfg(unix)]
impl Future for Writable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let interest_id = match self.interest_id {
            Some(interest_id) => interest_id,
            None => {
                let interest_id = scheduler.allocate_write_interest_id();
                self.interest_id = Some(interest_id);
                interest_id
            }
        };

        if scheduler.take_ready_write_interest(interest_id) {
            return Poll::Ready(());
        }

        scheduler.register_write_interest(interest_id, self.fd, context.waker().clone());
        Poll::Pending
    }
}

#[cfg(unix)]
impl Drop for Writable {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(interest_id)) = (&self.scheduler, self.interest_id) {
            scheduler.remove_write_interest(interest_id);
        }
    }
}

/// Future returned by [`sleep`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Sleep {
    deadline: Instant,
    timer_id: Option<usize>,
    scheduler: Option<Arc<Scheduler>>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            if let Some(timer_id) = self.timer_id.take() {
                current_scheduler().remove_timer(timer_id);
            }
            return Poll::Ready(());
        }

        let scheduler = current_scheduler();
        self.scheduler = Some(Arc::clone(&scheduler));
        let timer_id = match self.timer_id {
            Some(timer_id) => timer_id,
            None => {
                let timer_id = scheduler.allocate_timer_id();
                self.timer_id = Some(timer_id);
                timer_id
            }
        };

        scheduler.register_timer(timer_id, self.deadline, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let (Some(scheduler), Some(timer_id)) = (&self.scheduler, self.timer_id) {
            scheduler.remove_timer(timer_id);
        }
    }
}

/// Error returned by [`timeout`] when the deadline wins the race.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "future timed out")
    }
}

impl Error for TimeoutError {}

/// Future returned by [`timeout`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    sleep: Sleep,
}

impl<F> fmt::Debug for Timeout<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout").finish_non_exhaustive()
    }
}

impl<F> Unpin for Timeout<F> {}

impl<F> Future for Timeout<F>
where
    F: Future,
{
    type Output = Result<F::Output, TimeoutError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Poll::Ready(output) = this.future.as_mut().poll(context) {
            return Poll::Ready(Ok(output));
        }

        if Pin::new(&mut this.sleep).poll(context).is_ready() {
            return Poll::Ready(Err(TimeoutError));
        }

        Poll::Pending
    }
}

/// Output returned by [`race`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaceOutput<A, B> {
    /// The first future completed before the second future.
    First(A),
    /// The second future completed before the first future.
    Second(B),
}

/// Future returned by [`race`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct Race<A, B>
where
    A: Future,
    B: Future,
{
    first: Option<Pin<Box<A>>>,
    second: Option<Pin<Box<B>>>,
}

impl<A, B> fmt::Debug for Race<A, B>
where
    A: Future,
    B: Future,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Race").finish_non_exhaustive()
    }
}

impl<A, B> Unpin for Race<A, B>
where
    A: Future,
    B: Future,
{
}

impl<A, B> Future for Race<A, B>
where
    A: Future,
    B: Future,
{
    type Output = RaceOutput<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(first) = this.first.as_mut() {
            if let Poll::Ready(output) = first.as_mut().poll(context) {
                this.second.take();
                this.first.take();
                return Poll::Ready(RaceOutput::First(output));
            }
        }

        if let Some(second) = this.second.as_mut() {
            if let Poll::Ready(output) = second.as_mut().poll(context) {
                this.first.take();
                this.second.take();
                return Poll::Ready(RaceOutput::Second(output));
            }
        }

        Poll::Pending
    }
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
    #[cfg(unix)]
    use super::{
        accept_async, accept_timeout_async, connect_async, connect_timeout_async, copy_async,
        copy_timeout_async, read_exact_async, read_exact_timeout_async, readable, serve_tcp_n,
        serve_tcp_until_idle, writable, write_all_async, write_all_timeout_async,
    };
    use super::{
        block_on, executor_and_spawner, race, sleep, timeout, yield_now, RaceOutput, TimeoutError,
    };
    #[cfg(unix)]
    use std::io::{self, Read, Write};
    #[cfg(unix)]
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::unix::io::AsRawFd;
    #[cfg(unix)]
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};
    #[cfg(unix)]
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn block_on_returns_future_output() {
        assert_eq!(block_on(async { 42 }), 42);
    }

    #[test]
    fn block_on_accepts_stack_borrowing_future() {
        let value = 42;
        let borrowed = block_on(async { &value });

        assert_eq!(*borrowed, 42);
    }

    #[test]
    fn block_on_preserves_root_future_panic() {
        let panic = std::panic::catch_unwind(|| {
            block_on(async {
                panic!("root panic");
            });
        })
        .unwrap_err();

        assert_eq!(panic.downcast_ref::<&str>(), Some(&"root panic"));
    }

    #[test]
    fn run_until_drives_spawned_tasks() {
        let (executor, spawner) = executor_and_spawner();

        let output = executor.run_until(async {
            let worker = spawner
                .spawn_with_handle(async {
                    yield_now().await;
                    7
                })
                .unwrap();

            worker.await.unwrap()
        });

        drop(spawner);

        assert_eq!(output, 7);
    }

    #[test]
    fn run_until_timer_completes_while_task_keeps_waking() {
        let (executor, spawner) = executor_and_spawner();

        spawner.spawn(AlwaysWake).unwrap();
        drop(spawner);

        let started = Instant::now();
        executor.run_until(async {
            sleep(Duration::from_millis(5)).await;
        });

        assert!(started.elapsed() < Duration::from_secs(1));
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
    fn sleep_delays_future_completion() {
        let started = Instant::now();

        block_on(async {
            sleep(Duration::from_millis(10)).await;
        });

        assert!(started.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn timeout_returns_future_output_before_deadline() {
        let output = block_on(async { timeout(Duration::from_secs(1), async { 7 }).await });

        assert_eq!(output, Ok(7));
    }

    #[test]
    fn timeout_expires_before_slow_future() {
        let started = Instant::now();

        let output = block_on(async {
            timeout(Duration::from_millis(5), async {
                sleep(Duration::from_secs(60)).await;
                7
            })
            .await
        });

        assert_eq!(output, Err(TimeoutError));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn timeout_drops_inner_sleep_timer() {
        let (executor, spawner) = executor_and_spawner();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let result = timeout(Duration::from_millis(5), async {
                    sleep(Duration::from_secs(60)).await;
                    7
                })
                .await;
                *output_for_task.lock().unwrap() = Some(result);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some(Err(TimeoutError)));
        assert!(executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .timers
            .is_empty());
    }

    #[test]
    fn race_returns_first_future_output() {
        let output = block_on(async {
            race(
                async {
                    yield_now().await;
                    "first"
                },
                async {
                    sleep(Duration::from_secs(60)).await;
                    "second"
                },
            )
            .await
        });

        assert_eq!(output, RaceOutput::First("first"));
    }

    #[test]
    fn race_returns_second_future_output() {
        let output = block_on(async {
            race(
                async {
                    sleep(Duration::from_secs(60)).await;
                    "first"
                },
                async {
                    yield_now().await;
                    "second"
                },
            )
            .await
        });

        assert_eq!(output, RaceOutput::Second("second"));
    }

    #[test]
    fn race_drops_losing_sleep_timer() {
        let (executor, spawner) = executor_and_spawner();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let result = race(
                    async {
                        sleep(Duration::from_millis(5)).await;
                        "fast"
                    },
                    async {
                        sleep(Duration::from_secs(60)).await;
                        "slow"
                    },
                )
                .await;
                *output_for_task.lock().unwrap() = Some(result);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some(RaceOutput::First("fast")));
        assert!(executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .timers
            .is_empty());
    }

    #[test]
    fn timers_wake_in_deadline_order() {
        let (executor, spawner) = executor_and_spawner();
        let events = Arc::new(Mutex::new(Vec::new()));

        let slow_events = Arc::clone(&events);
        spawner
            .spawn(async move {
                sleep(Duration::from_millis(20)).await;
                slow_events.lock().unwrap().push("slow");
            })
            .unwrap();

        let fast_events = Arc::clone(&events);
        spawner
            .spawn(async move {
                sleep(Duration::from_millis(5)).await;
                fast_events.lock().unwrap().push("fast");
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(&*events.lock().unwrap(), &["fast", "slow"]);
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
    fn repeated_wakes_share_one_ready_queue_entry() {
        let (executor, spawner) = executor_and_spawner();
        spawner.spawn(WakeTwiceThenPending).unwrap();

        let task = executor.scheduler.next_task().unwrap();
        task.poll();

        assert_eq!(
            executor
                .scheduler
                .state
                .lock()
                .expect("scheduler state mutex poisoned")
                .queue
                .len(),
            1
        );
    }

    #[test]
    fn panicking_task_does_not_stop_executor() {
        let (executor, spawner) = executor_and_spawner();
        let completed = Arc::new(Mutex::new(false));
        let completed_for_task = Arc::clone(&completed);

        spawner
            .spawn(async {
                panic!("task panic");
            })
            .unwrap();
        spawner
            .spawn(async move {
                *completed_for_task.lock().unwrap() = true;
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert!(*completed.lock().unwrap());
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
                let output = worker.await.unwrap();
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
                *result_for_task.lock().unwrap() =
                    Some(first.await.unwrap() + second.await.unwrap());
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some(7));
    }

    #[test]
    fn panicking_join_handle_wakes_waiter() {
        let (executor, spawner) = executor_and_spawner();
        let observed = Arc::new(Mutex::new(false));
        let observed_for_task = Arc::clone(&observed);

        let worker = spawner
            .spawn_with_handle(async {
                panic!("join panic");
            })
            .unwrap();

        spawner
            .spawn(CatchJoinPanic {
                handle: worker,
                observed: observed_for_task,
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert!(*observed.lock().unwrap());
    }

    #[test]
    fn aborted_join_handle_returns_cancelled() {
        let (executor, spawner) = executor_and_spawner();
        let result = Arc::new(Mutex::new(None));
        let result_for_task = Arc::clone(&result);

        let worker = spawner
            .spawn_with_handle(async {
                sleep(Duration::from_secs(60)).await;
                7
            })
            .unwrap();

        spawner
            .spawn(async move {
                yield_now().await;
                let aborted = worker.abort();
                let cancelled = worker.await.unwrap_err().is_cancelled();
                *result_for_task.lock().unwrap() = Some((aborted, cancelled));
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some((true, true)));
        assert!(executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .timers
            .is_empty());
    }

    #[test]
    fn aborting_completed_join_handle_returns_false() {
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
                yield_now().await;
                yield_now().await;
                let aborted = worker.abort();
                let output = worker.await.unwrap();
                *result_for_task.lock().unwrap() = Some((aborted, output));
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some((false, 7)));
    }

    #[test]
    fn spawned_tasks_can_sleep_before_joining() {
        let (executor, spawner) = executor_and_spawner();
        let result = Arc::new(Mutex::new(None));
        let result_for_task = Arc::clone(&result);

        let worker = spawner
            .spawn_with_handle(async {
                sleep(Duration::from_millis(5)).await;
                11
            })
            .unwrap();

        spawner
            .spawn(async move {
                *result_for_task.lock().unwrap() = Some(worker.await.unwrap());
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*result.lock().unwrap(), Some(11));
    }

    #[cfg(unix)]
    #[test]
    fn readable_future_completes_when_fd_becomes_readable() {
        let (executor, spawner) = executor_and_spawner();
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let reader_fd = reader.as_raw_fd();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                readable(reader_fd).await;

                let mut byte = [0u8; 1];
                reader.read_exact(&mut byte).unwrap();
                *output_for_task.lock().unwrap() = Some(byte[0]);
            })
            .unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            writer.write_all(b"x").unwrap();
        });

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some(b'x'));
    }

    #[cfg(unix)]
    #[test]
    fn multiple_tasks_can_wait_for_different_readable_fds() {
        let (executor, spawner) = executor_and_spawner();
        let (mut first_reader, mut first_writer) = UnixStream::pair().unwrap();
        let (mut second_reader, mut second_writer) = UnixStream::pair().unwrap();
        first_reader.set_nonblocking(true).unwrap();
        second_reader.set_nonblocking(true).unwrap();
        let first_fd = first_reader.as_raw_fd();
        let second_fd = second_reader.as_raw_fd();
        let output = Arc::new(Mutex::new(Vec::new()));

        let first_output = Arc::clone(&output);
        spawner
            .spawn(async move {
                readable(first_fd).await;
                let mut byte = [0u8; 1];
                first_reader.read_exact(&mut byte).unwrap();
                first_output.lock().unwrap().push(byte[0]);
            })
            .unwrap();

        let second_output = Arc::clone(&output);
        spawner
            .spawn(async move {
                readable(second_fd).await;
                let mut byte = [0u8; 1];
                second_reader.read_exact(&mut byte).unwrap();
                second_output.lock().unwrap().push(byte[0]);
            })
            .unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            second_writer.write_all(b"b").unwrap();
            thread::sleep(Duration::from_millis(5));
            first_writer.write_all(b"a").unwrap();
        });

        drop(spawner);
        executor.run();

        assert_eq!(&*output.lock().unwrap(), b"ba");
    }

    #[cfg(unix)]
    #[test]
    fn writable_future_completes_when_fd_is_writable() {
        let (executor, spawner) = executor_and_spawner();
        let (_reader, writer) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        let writer_fd = writer.as_raw_fd();
        let output = Arc::new(Mutex::new(false));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                writable(writer_fd).await;
                *output_for_task.lock().unwrap() = true;
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert!(*output.lock().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn read_exact_async_waits_until_buffer_is_filled() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            writer.write_all(b"he").unwrap();
            thread::sleep(Duration::from_millis(5));
            writer.write_all(b"llo").unwrap();
        });

        let mut buffer = [0u8; 5];
        let buffer = block_on(async move {
            read_exact_async(&mut reader, &mut buffer).await.unwrap();
            buffer
        });

        assert_eq!(&buffer, b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn read_exact_async_returns_unexpected_eof() {
        let (mut reader, writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        drop(writer);

        let mut buffer = [0u8; 1];
        let error = block_on(async move {
            read_exact_async(&mut reader, &mut buffer)
                .await
                .unwrap_err()
        });

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[cfg(unix)]
    #[test]
    fn read_exact_timeout_async_returns_timed_out() {
        let (executor, spawner) = executor_and_spawner();
        let (mut reader, _writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let mut buffer = [0u8; 1];
                let error =
                    read_exact_timeout_async(&mut reader, &mut buffer, Duration::from_millis(5))
                        .await
                        .unwrap_err();
                *output_for_task.lock().unwrap() = Some(error.kind());
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some(std::io::ErrorKind::TimedOut));
        assert!(executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .read_interests
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn write_all_async_writes_entire_buffer() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();

        block_on(async move {
            write_all_async(&mut writer, b"hello").await.unwrap();
        });

        let mut buffer = [0u8; 5];
        reader.read_exact(&mut buffer).unwrap();

        assert_eq!(&buffer, b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn write_all_timeout_async_writes_entire_buffer() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();

        block_on(async move {
            write_all_timeout_async(&mut writer, b"hello", Duration::from_secs(1))
                .await
                .unwrap();
        });

        let mut buffer = [0u8; 5];
        reader.read_exact(&mut buffer).unwrap();

        assert_eq!(&buffer, b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn copy_async_copies_until_reader_eof() {
        let (mut source_reader, mut source_writer) = UnixStream::pair().unwrap();
        let (mut sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
        source_reader.set_nonblocking(true).unwrap();
        sink_writer.set_nonblocking(true).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            source_writer.write_all(b"hello").unwrap();
            thread::sleep(Duration::from_millis(5));
            source_writer.write_all(b" world").unwrap();
        });

        let copied = block_on(async move {
            let mut buffer = [0u8; 4];
            copy_async(&mut source_reader, &mut sink_writer, &mut buffer)
                .await
                .unwrap()
        });

        let mut output = Vec::new();
        sink_reader.read_to_end(&mut output).unwrap();

        assert_eq!(copied, 11);
        assert_eq!(&output, b"hello world");
    }

    #[cfg(unix)]
    #[test]
    fn copy_async_rejects_empty_buffer() {
        let (mut source_reader, _source_writer) = UnixStream::pair().unwrap();
        let (_sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
        source_reader.set_nonblocking(true).unwrap();
        sink_writer.set_nonblocking(true).unwrap();

        let error = block_on(async move {
            copy_async(&mut source_reader, &mut sink_writer, &mut [])
                .await
                .unwrap_err()
        });

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn copy_timeout_async_returns_timed_out() {
        let (mut source_reader, _source_writer) = UnixStream::pair().unwrap();
        let (_sink_reader, mut sink_writer) = UnixStream::pair().unwrap();
        source_reader.set_nonblocking(true).unwrap();
        sink_writer.set_nonblocking(true).unwrap();

        let error = block_on(async move {
            let mut buffer = [0u8; 8];
            copy_timeout_async(
                &mut source_reader,
                &mut sink_writer,
                &mut buffer,
                Duration::from_millis(5),
            )
            .await
            .unwrap_err()
        });

        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[cfg(unix)]
    #[test]
    fn accept_async_waits_for_tcp_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut stream = TcpStream::connect(address).unwrap();
            thread::sleep(Duration::from_millis(10));
            stream.write_all(b"x").unwrap();
        });

        let mut stream = block_on(async move {
            let (mut stream, peer) = accept_async(&listener).await.unwrap();
            assert_eq!(peer.ip(), address.ip());
            let mut empty = [0u8; 1];
            assert_eq!(
                stream.read(&mut empty).unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            stream
        });

        let byte = block_on(async move {
            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            byte
        });

        assert_eq!(byte, [b'x']);
    }

    #[cfg(unix)]
    #[test]
    fn accept_timeout_async_returns_timed_out() {
        let (executor, spawner) = executor_and_spawner();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let error = accept_timeout_async(&listener, Duration::from_millis(5))
                    .await
                    .unwrap_err();
                *output_for_task.lock().unwrap() = Some(error.kind());
            })
            .unwrap();

        drop(spawner);
        executor.run();

        assert_eq!(*output.lock().unwrap(), Some(std::io::ErrorKind::TimedOut));
        assert!(executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .read_interests
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn accepted_tcp_stream_works_with_async_read_and_write() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();

        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(b"z").unwrap();

            let mut echo = [0u8; 1];
            stream.read_exact(&mut echo).unwrap();
            echo[0]
        });

        block_on(async move {
            let (mut stream, peer) = accept_async(&listener).await.unwrap();
            assert_eq!(peer.ip(), address.ip());

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            write_all_async(&mut stream, &byte).await.unwrap();
        });

        assert_eq!(client.join().unwrap(), b'z');
    }

    #[cfg(unix)]
    #[test]
    fn connect_async_establishes_nonblocking_tcp_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            stream.write_all(&byte).unwrap();
        });

        let echoed = block_on(async move {
            let mut stream = connect_async(address).await.unwrap();
            write_all_async(&mut stream, b"q").await.unwrap();

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            byte[0]
        });

        server.join().unwrap();
        assert_eq!(echoed, b'q');
    }

    #[cfg(unix)]
    #[test]
    fn connect_timeout_async_establishes_nonblocking_tcp_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            stream.write_all(&byte).unwrap();
        });

        let echoed = block_on(async move {
            let mut stream = connect_timeout_async(address, Duration::from_secs(1))
                .await
                .unwrap();
            write_all_async(&mut stream, b"t").await.unwrap();

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            byte[0]
        });

        server.join().unwrap();
        assert_eq!(echoed, b't');
    }

    #[cfg(unix)]
    #[test]
    fn connect_async_supports_ipv6_loopback() {
        let listener = match TcpListener::bind("[::1]:0") {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => return,
            Err(error) => panic!("failed to bind IPv6 loopback listener: {error}"),
        };
        let address = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            stream.write_all(&byte).unwrap();
        });

        let echoed = block_on(async move {
            let mut stream = connect_async(address).await.unwrap();
            write_all_async(&mut stream, b"v").await.unwrap();

            let mut byte = [0u8; 1];
            read_exact_async(&mut stream, &mut byte).await.unwrap();
            byte[0]
        });

        server.join().unwrap();
        assert_eq!(echoed, b'v');
    }

    #[cfg(unix)]
    #[test]
    fn connect_and_accept_can_run_on_same_executor() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();

        let (executor, spawner) = executor_and_spawner();
        let output = Arc::new(Mutex::new(None));

        let server = spawner
            .spawn_with_handle(async move {
                let (mut stream, peer) = accept_async(&listener).await.unwrap();
                let mut byte = [0u8; 1];
                read_exact_async(&mut stream, &mut byte).await.unwrap();
                write_all_async(&mut stream, &byte).await.unwrap();
                peer
            })
            .unwrap();

        let client = spawner
            .spawn_with_handle(async move {
                let mut stream = connect_async(address).await.unwrap();
                write_all_async(&mut stream, b"x").await.unwrap();

                let mut byte = [0u8; 1];
                read_exact_async(&mut stream, &mut byte).await.unwrap();
                byte[0]
            })
            .unwrap();

        let output_for_task = Arc::clone(&output);
        spawner
            .spawn(async move {
                let peer = server.await.unwrap();
                let echoed = client.await.unwrap();
                *output_for_task.lock().unwrap() = Some((peer, echoed));
            })
            .unwrap();

        drop(spawner);
        executor.run();

        let (peer, echoed) = output.lock().unwrap().take().unwrap();
        assert_eq!(peer.ip(), address.ip());
        assert_eq!(echoed, b'x');
    }

    #[cfg(unix)]
    #[test]
    fn executor_can_spawn_tcp_handlers_for_multiple_accepted_streams() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();

        let clients = (0..3u8)
            .map(|value| {
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                    let mut stream = TcpStream::connect(address).unwrap();
                    stream.write_all(&[b'a' + value]).unwrap();

                    let mut echo = [0u8; 1];
                    stream.read_exact(&mut echo).unwrap();
                    echo[0]
                })
            })
            .collect::<Vec<_>>();

        let (executor, spawner) = executor_and_spawner();
        let accept_spawner = spawner.clone();

        spawner
            .spawn(async move {
                for _ in 0..3 {
                    let (mut stream, _) = accept_async(&listener).await.unwrap();
                    accept_spawner
                        .spawn(async move {
                            let mut byte = [0u8; 1];
                            read_exact_async(&mut stream, &mut byte).await.unwrap();
                            write_all_async(&mut stream, &byte).await.unwrap();
                        })
                        .unwrap();
                }
            })
            .unwrap();

        drop(spawner);
        executor.run();

        let mut echoed = clients
            .into_iter()
            .map(|client| client.join().unwrap())
            .collect::<Vec<_>>();
        echoed.sort();

        assert_eq!(&echoed, b"abc");
    }

    #[cfg(unix)]
    #[test]
    fn serve_tcp_n_spawns_handlers_for_accepted_streams() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let clients = (0..3u8)
            .map(|value| {
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                    let mut stream = TcpStream::connect(address).unwrap();
                    stream.write_all(&[b'a' + value]).unwrap();

                    let mut echo = [0u8; 1];
                    stream.read_exact(&mut echo).unwrap();
                    echo[0]
                })
            })
            .collect::<Vec<_>>();

        let (executor, spawner) = executor_and_spawner();
        let server_spawner = spawner.clone();

        spawner
            .spawn(async move {
                serve_tcp_n(
                    listener,
                    server_spawner,
                    3,
                    |mut stream, _peer| async move {
                        let mut byte = [0u8; 1];
                        read_exact_async(&mut stream, &mut byte).await?;
                        write_all_async(&mut stream, &byte).await
                    },
                )
                .await
                .unwrap();
            })
            .unwrap();

        drop(spawner);
        executor.run();

        let mut echoed = clients
            .into_iter()
            .map(|client| client.join().unwrap())
            .collect::<Vec<_>>();
        echoed.sort();

        assert_eq!(&echoed, b"abc");
    }

    #[cfg(unix)]
    #[test]
    fn serve_tcp_n_returns_handler_error() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let client = thread::spawn(move || {
            TcpStream::connect(address).unwrap();
        });

        let (executor, spawner) = executor_and_spawner();
        let server_spawner = spawner.clone();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let error = serve_tcp_n(listener, server_spawner, 1, |_stream, _peer| async {
                    Err(io::Error::new(io::ErrorKind::Other, "handler failed"))
                })
                .await
                .unwrap_err();
                *output_for_task.lock().unwrap() = Some(error.kind());
            })
            .unwrap();

        drop(spawner);
        executor.run();
        client.join().unwrap();

        assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
    }

    #[cfg(unix)]
    #[test]
    fn serve_tcp_until_idle_stops_after_idle_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let clients = (0..3u8)
            .map(|value| {
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(5 + u64::from(value) * 5));
                    let mut stream = TcpStream::connect(address).unwrap();
                    stream.write_all(&[b'a' + value]).unwrap();

                    let mut echo = [0u8; 1];
                    stream.read_exact(&mut echo).unwrap();
                    echo[0]
                })
            })
            .collect::<Vec<_>>();

        let (executor, spawner) = executor_and_spawner();
        let server_spawner = spawner.clone();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let accepted = serve_tcp_until_idle(
                    listener,
                    server_spawner,
                    Duration::from_millis(25),
                    |mut stream, _peer| async move {
                        let mut byte = [0u8; 1];
                        read_exact_async(&mut stream, &mut byte).await?;
                        write_all_async(&mut stream, &byte).await
                    },
                )
                .await
                .unwrap();
                *output_for_task.lock().unwrap() = Some(accepted);
            })
            .unwrap();

        drop(spawner);
        executor.run();

        let mut echoed = clients
            .into_iter()
            .map(|client| client.join().unwrap())
            .collect::<Vec<_>>();
        echoed.sort();

        assert_eq!(*output.lock().unwrap(), Some(3));
        assert_eq!(&echoed, b"abc");
    }

    #[cfg(unix)]
    #[test]
    fn serve_tcp_until_idle_returns_handler_error() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let client = thread::spawn(move || {
            TcpStream::connect(address).unwrap();
        });

        let (executor, spawner) = executor_and_spawner();
        let server_spawner = spawner.clone();
        let output = Arc::new(Mutex::new(None));
        let output_for_task = Arc::clone(&output);

        spawner
            .spawn(async move {
                let error = serve_tcp_until_idle(
                    listener,
                    server_spawner,
                    Duration::from_millis(5),
                    |_stream, _peer| async {
                        Err(io::Error::new(io::ErrorKind::Other, "handler failed"))
                    },
                )
                .await
                .unwrap_err();
                *output_for_task.lock().unwrap() = Some(error.kind());
            })
            .unwrap();

        drop(spawner);
        executor.run();
        client.join().unwrap();

        assert_eq!(*output.lock().unwrap(), Some(io::ErrorKind::Other));
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

                *output_for_task.lock().unwrap() = Some(worker.await.unwrap());
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

    #[test]
    fn dropping_executor_cancels_pending_sleep_task() {
        let (executor, spawner) = executor_and_spawner();
        let worker = spawner
            .spawn_with_handle(async {
                sleep(Duration::from_secs(60)).await;
                7
            })
            .unwrap();

        executor.poll_ready_tasks();
        assert!(!executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .timers
            .is_empty());

        drop(executor);

        assert!(!worker.abort());
    }

    #[cfg(unix)]
    #[test]
    fn dropping_executor_cancels_pending_readable_task() {
        let (executor, spawner) = executor_and_spawner();
        let (reader, _writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let fd = reader.as_raw_fd();

        let worker = spawner
            .spawn_with_handle(async move {
                readable(fd).await;
                7
            })
            .unwrap();

        executor.poll_ready_tasks();
        assert!(!executor
            .scheduler
            .state
            .lock()
            .expect("scheduler state mutex poisoned")
            .read_interests
            .is_empty());

        drop(executor);

        assert!(!worker.abort());
    }

    struct WakeTwiceThenPending;

    impl std::future::Future for WakeTwiceThenPending {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            context.waker().wake_by_ref();
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }

    struct AlwaysWake;

    impl std::future::Future for AlwaysWake {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }

    struct CatchJoinPanic<T> {
        handle: super::JoinHandle<T>,
        observed: Arc<Mutex<bool>>,
    }

    impl<T> std::future::Future for CatchJoinPanic<T> {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                std::pin::Pin::new(&mut self.handle).poll(context)
            }));

            match result {
                Ok(std::task::Poll::Ready(Err(error))) if error.is_panic() => {
                    *self.observed.lock().unwrap() = true;
                    std::task::Poll::Ready(())
                }
                Ok(std::task::Poll::Ready(_)) => std::task::Poll::Ready(()),
                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                Err(_) => {
                    *self.observed.lock().unwrap() = true;
                    std::task::Poll::Ready(())
                }
            }
        }
    }
}
