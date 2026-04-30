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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

#[cfg(unix)]
use crate::os::{OsReactor, OsWaker};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

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

            self.scheduler.wake_expired_timers();

            if self.scheduler.is_drained() {
                break;
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
            let scheduler = Arc::clone(&self.scheduler);
            CURRENT_SCHEDULER.with(|current| {
                *current.borrow_mut() = Some(scheduler);
            });

            let poll_result = future.as_mut().poll(&mut context);

            CURRENT_SCHEDULER.with(|current| {
                *current.borrow_mut() = None;
            });

            match poll_result {
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
}

impl Scheduler {
    fn new(#[cfg(unix)] waker: OsWaker) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                queue: VecDeque::new(),
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
        .expect("executor::sleep must be polled by sitas::executor::Executor")
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

/// Returns a future that completes after `duration`.
///
/// This future is driven by the executor's internal timer list. It must be
/// polled by this module's [`Executor`].
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
        timer_id: None,
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
    use super::{accept_async, copy_async, read_exact_async, readable, writable, write_all_async};
    use super::{block_on, executor_and_spawner, sleep, yield_now};
    #[cfg(unix)]
    use std::io::{Read, Write};
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
                *result_for_task.lock().unwrap() = Some(worker.await);
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
