use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::scheduler::{Scheduler, current_scheduler};

/// Returns a future that completes after `duration`.
///
/// This future is driven by the executor's internal timer list. It must be
/// polled by this module's [`super::Executor`].
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

/// Returns a future that yields once before completing.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
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

        if let Some(first) = this.first.as_mut()
            && let Poll::Ready(output) = first.as_mut().poll(context)
        {
            this.second.take();
            this.first.take();
            return Poll::Ready(RaceOutput::First(output));
        }

        if let Some(second) = this.second.as_mut()
            && let Poll::Ready(output) = second.as_mut().poll(context)
        {
            this.first.take();
            this.second.take();
            return Poll::Ready(RaceOutput::Second(output));
        }

        Poll::Pending
    }
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
