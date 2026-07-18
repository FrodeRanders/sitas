//! Cooperative synchronization primitives for the executor.
//!
//! [`Notify`] provides a one-shot async wake event. [`StopSource`] and
//! [`StopToken`] implement cloneable cooperative stop signals. Both are
//! waker-aware and integrate with the executor's idle-wait loop.

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Creates a source/token pair used to stop async operations cooperatively.
pub fn stop_pair() -> (StopSource, StopToken) {
    let shared = Arc::new(Mutex::new(StopState {
        stopped: false,
        wakers: Vec::new(),
    }));

    (
        StopSource {
            shared: Arc::clone(&shared),
        },
        StopToken { shared },
    )
}

/// Handle used to request cooperative stop.
#[derive(Clone)]
pub struct StopSource {
    shared: Arc<Mutex<StopState>>,
}

/// Future that completes when its matching [`StopSource`] is stopped.
#[derive(Clone)]
#[must_use = "stop tokens do nothing unless polled or awaited"]
pub struct StopToken {
    shared: Arc<Mutex<StopState>>,
}

#[derive(Debug)]
struct StopState {
    stopped: bool,
    wakers: Vec<Waker>,
}

impl StopSource {
    /// Requests stop and wakes tasks waiting on the matching token.
    pub fn stop(&self) -> bool {
        let wakers = {
            let mut state = self.shared.lock().expect("stop token mutex poisoned");
            if state.stopped {
                return false;
            }

            state.stopped = true;
            mem::take(&mut state.wakers)
        };

        for waker in wakers {
            waker.wake();
        }

        true
    }

    /// Returns true if stop has already been requested.
    pub fn is_stopped(&self) -> bool {
        self.shared
            .lock()
            .expect("stop token mutex poisoned")
            .stopped
    }
}

impl StopToken {
    /// Returns true if stop has already been requested.
    pub fn is_stopped(&self) -> bool {
        self.shared
            .lock()
            .expect("stop token mutex poisoned")
            .stopped
    }
}

impl fmt::Debug for StopSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StopSource")
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

impl fmt::Debug for StopToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StopToken")
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

impl Future for StopToken {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.lock().expect("stop token mutex poisoned");
        if state.stopped {
            Poll::Ready(())
        } else {
            if !state
                .wakers
                .iter()
                .any(|waker| waker.will_wake(context.waker()))
            {
                state.wakers.push(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// Cloneable one-shot async notification primitive.
///
/// `Notify` starts unnotified. Calling [`Notify::notify_waiters`] marks it as
/// notified and wakes all current waiters. Once notified, future
/// [`Notify::notified`] futures complete immediately.
#[derive(Clone)]
pub struct Notify {
    shared: Arc<Mutex<NotifyState>>,
}

#[derive(Debug)]
struct NotifyState {
    notified: bool,
    wakers: Vec<Waker>,
}

/// Future returned by [`Notify::notified`].
#[derive(Clone)]
#[must_use = "notification futures do nothing unless polled or awaited"]
pub struct Notified {
    shared: Arc<Mutex<NotifyState>>,
}

impl Notify {
    /// Creates an unnotified event.
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(NotifyState {
                notified: false,
                wakers: Vec::new(),
            })),
        }
    }

    /// Returns a future that completes once this event is notified.
    pub fn notified(&self) -> Notified {
        Notified {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Marks this event as notified and wakes current waiters.
    ///
    /// Returns `true` if this call changed the event from unnotified to
    /// notified, or `false` if it had already been notified.
    pub fn notify_waiters(&self) -> bool {
        let wakers = {
            let mut state = self.shared.lock().expect("notify mutex poisoned");
            if state.notified {
                return false;
            }

            state.notified = true;
            mem::take(&mut state.wakers)
        };

        for waker in wakers {
            waker.wake();
        }

        true
    }

    /// Returns true if this event has already been notified.
    pub fn is_notified(&self) -> bool {
        self.shared.lock().expect("notify mutex poisoned").notified
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notify")
            .field("notified", &self.is_notified())
            .finish()
    }
}

impl fmt::Debug for Notified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let notified = self.shared.lock().expect("notify mutex poisoned").notified;
        f.debug_struct("Notified")
            .field("notified", &notified)
            .finish()
    }
}

impl Future for Notified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.lock().expect("notify mutex poisoned");
        if state.notified {
            Poll::Ready(())
        } else {
            if !state
                .wakers
                .iter()
                .any(|waker| waker.will_wake(context.waker()))
            {
                state.wakers.push(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    #[derive(Debug)]
    struct CountWake {
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn count_waker() -> (Waker, Arc<AtomicUsize>) {
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Arc::new(CountWake {
            wakes: Arc::clone(&wakes),
        })
        .into();

        (waker, wakes)
    }

    #[test]
    fn stop_source_is_idempotent_and_wakes_waiters_once() {
        let (source, mut token) = stop_pair();
        let (waker, wakes) = count_waker();
        let mut context = Context::from_waker(&waker);

        assert!(Pin::new(&mut token).poll(&mut context).is_pending());
        assert!(source.stop());
        assert!(!source.stop());

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(source.is_stopped());
        assert!(token.is_stopped());
        assert!(Pin::new(&mut token).poll(&mut context).is_ready());
    }

    #[test]
    fn stop_token_coalesces_repeated_polls_with_same_waker() {
        let (source, mut token) = stop_pair();
        let (waker, _wakes) = count_waker();
        let mut context = Context::from_waker(&waker);

        assert!(Pin::new(&mut token).poll(&mut context).is_pending());
        assert!(Pin::new(&mut token).poll(&mut context).is_pending());
        assert_eq!(
            token
                .shared
                .lock()
                .expect("stop token mutex poisoned")
                .wakers
                .len(),
            1
        );

        assert!(source.stop());
    }

    #[test]
    fn notify_is_idempotent_and_wakes_waiters_once() {
        let notify = Notify::new();
        let mut notified = notify.notified();
        let (waker, wakes) = count_waker();
        let mut context = Context::from_waker(&waker);

        assert!(Pin::new(&mut notified).poll(&mut context).is_pending());
        assert!(notify.notify_waiters());
        assert!(!notify.notify_waiters());

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(notify.is_notified());
        assert!(Pin::new(&mut notified).poll(&mut context).is_ready());
    }

    #[test]
    fn notify_future_coalesces_repeated_polls_with_same_waker() {
        let notify = Notify::new();
        let mut notified = notify.notified();
        let (waker, _wakes) = count_waker();
        let mut context = Context::from_waker(&waker);

        assert!(Pin::new(&mut notified).poll(&mut context).is_pending());
        assert!(Pin::new(&mut notified).poll(&mut context).is_pending());
        assert_eq!(
            notified
                .shared
                .lock()
                .expect("notify mutex poisoned")
                .wakers
                .len(),
            1
        );

        assert!(notify.notify_waiters());
    }
}
