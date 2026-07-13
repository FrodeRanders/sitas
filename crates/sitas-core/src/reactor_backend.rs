//! OS backend contract for the executor reactor (Option A spike).
//!
//! This module extracts, as explicit traits, the small contract the
//! single-threaded executor actually requires from the operating system in
//! order to sleep until something interesting happens and to be woken from
//! another thread or core. Today that contract is satisfied implicitly by the
//! Unix `OsReactor` / `OsWaker`
//! types under `#[cfg(unix)]`; naming it makes the seam a documented boundary
//! that an alternative backend (for example a co-designed asynchronous OS whose
//! kernel exposes completion capabilities) can implement behind the same
//! interface.
//!
//! This is a *spike*: it defines and validates the boundary without yet
//! rewriting `Executor` and
//! `Scheduler` to be generic over it. The goal is to answer
//! the decision-gate questions:
//!
//! 1. Is the OS contract the executor needs genuinely small and clean?
//! 2. Does a non-Unix (mock or foreign-OS) backend become possible behind it?
//!
//! The answer to (1), by inspection of `executor::driver`, is that the executor
//! needs exactly three things from the OS:
//!
//! - a way to **construct** a reactor and obtain a cloneable **waker** for it;
//! - a way to **block until** an interest becomes ready, the reactor is woken,
//!   or a timeout elapses;
//! - a way to **wake** a blocked reactor from elsewhere.
//!
//! Everything else the executor does (timers, ready queue, `Reply<T>`,
//! `Notify`, `ShardLocal<T>`) is built in safe Rust on top of those.
//!
//! ## Handle abstraction
//!
//! On Unix an I/O interest is a `RawFd`. A foreign backend may identify
//! interests differently — for example a co-designed async OS would hand out a
//! *completion capability* rather than a file descriptor. The contract is
//! therefore generic over an associated [`ReactorBackend::Handle`] type, so the
//! executor's notion of "the thing I registered interest in" does not have to be
//! a Unix descriptor.
//!
//! ## Relationship to the current code
//!
//! The current executor holds a concrete `OsReactor` and the scheduler holds a
//! concrete `OsWaker`. The blanket implementations in this module show those
//! existing types already satisfy the proposed contract with no behavior
//! change; a follow-up step (not part of this spike) would thread the trait
//! through `Executor`/`Scheduler` so the concrete type becomes a parameter.

use core::time::Duration;

use crate::io;

/// A cloneable handle that can wake a blocked [`ReactorBackend`] from another
/// thread or core.
///
/// On Unix this is the write end of the reactor's wake pipe. On a co-designed
/// async OS it would be a capability whose signalling delivers a cross-core
/// wake (for example an inter-processor interrupt).
pub trait ReactorWaker: Clone + Send + Sync {
    /// Wakes the associated reactor.
    ///
    /// Waking an already-woken reactor must be safe and is expected to
    /// coalesce: a reactor that has one pending wake and receives several more
    /// before it next waits should observe a single wake.
    fn wake(&self) -> io::Result<()>;
}

/// Object-safe wake capability stored by the scheduler.
///
/// The scheduler only ever needs to *wake* its reactor; it never needs the
/// concrete waker type. Erasing the waker to this object-safe trait keeps the
/// `Scheduler` non-generic while still routing wakes through
/// whatever [`ReactorWaker`] the active [`ReactorBackend`] provides. Any
/// `ReactorWaker` that is also `Debug` is automatically a `SchedulerWake`.
pub trait SchedulerWake: core::fmt::Debug + Send + Sync {
    /// Wakes the reactor; errors are the backend's concern and are ignored by
    /// the scheduler wake path.
    fn wake(&self);
}

impl<W: ReactorWaker + core::fmt::Debug> SchedulerWake for W {
    fn wake(&self) {
        let _ = ReactorWaker::wake(self);
    }
}

/// The result of a single [`ReactorBackend::wait`] call.
///
/// It reports whether the reactor itself was woken and which registered
/// interests became ready. It is deliberately an owned value: observability and
/// event results in `sitas` never expose borrowed runtime internals.
pub trait ReactorEvent {
    /// The type identifying a ready interest (see [`ReactorBackend::Handle`]).
    type Handle;

    /// Whether the reactor observed and drained a cross-thread wake.
    fn woke(&self) -> bool;

    /// The interests that became readable during this wait.
    fn readable(&self) -> &[Self::Handle];

    /// The interests that became writable during this wait.
    fn writable(&self) -> &[Self::Handle];
}

/// The OS-facing contract a single-shard executor reactor must satisfy.
///
/// One instance backs one executor (one shard). The Unix implementation wraps
/// `epoll`/`kqueue`/`poll` plus a wake pipe; a foreign implementation would
/// wrap whatever "block until an event" and "wake" primitives its OS provides.
pub trait ReactorBackend {
    /// The cloneable waker type used to wake this reactor from elsewhere.
    type Waker: ReactorWaker;

    /// The type identifying an I/O interest. On Unix this is a `RawFd`; a
    /// foreign backend may use, for example, a completion-capability id.
    type Handle: Copy + Eq;

    /// The owned event type returned by [`wait`](ReactorBackend::wait).
    type Event: ReactorEvent<Handle = Self::Handle>;

    /// Returns a cloneable waker for this reactor.
    fn waker(&self) -> Self::Waker;

    /// Blocks until one of `read` becomes readable, one of `write` becomes
    /// writable, the reactor is woken via a [`ReactorWaker`], or `timeout`
    /// elapses.
    ///
    /// `timeout` of `None` means wait indefinitely; a zero duration means poll
    /// without blocking. The returned [`ReactorEvent`] reports what happened.
    fn wait(
        &self,
        read: &[Self::Handle],
        write: &[Self::Handle],
        timeout: Option<Duration>,
    ) -> io::Result<Self::Event>;
}

// --- Validation: the existing Unix types satisfy the contract ---------------
//
// These blanket implementations exist to *prove* the boundary is well-formed:
// the current Unix reactor already fits behind it with no behavior change.

#[cfg(all(feature = "std", unix))]
mod unix_impls {
    use super::{ReactorBackend, ReactorEvent, ReactorWaker};
    use crate::os::{OsEvent, OsReactor, OsWaker};
    use core::time::Duration;
    use std::io;
    use std::os::unix::io::RawFd;

    impl ReactorWaker for OsWaker {
        fn wake(&self) -> io::Result<()> {
            OsWaker::wake(self)
        }
    }

    impl ReactorEvent for OsEvent {
        type Handle = RawFd;

        fn woke(&self) -> bool {
            self.woke
        }

        fn readable(&self) -> &[RawFd] {
            &self.readable
        }

        fn writable(&self) -> &[RawFd] {
            &self.writable
        }
    }

    impl ReactorBackend for OsReactor {
        type Event = OsEvent;
        type Handle = RawFd;
        type Waker = OsWaker;

        fn waker(&self) -> OsWaker {
            OsReactor::waker(self)
        }

        fn wait(
            &self,
            read: &[RawFd],
            write: &[RawFd],
            timeout: Option<Duration>,
        ) -> io::Result<OsEvent> {
            OsReactor::wait_io(self, read, write, timeout)
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    //! A minimal in-memory backend proving the contract is implementable
    //! without any OS descriptors — the stand-in for a future foreign backend
    //! (for example a CharlotteOS completion-capability reactor).

    use super::{ReactorBackend, ReactorEvent, ReactorWaker};
    use core::sync::atomic::{AtomicBool, Ordering};
    use core::time::Duration;
    use std::io;
    use std::sync::{Arc, Condvar, Mutex};

    /// Interests are identified by an opaque integer id, standing in for a
    /// completion-capability handle rather than a Unix `RawFd`.
    type CapId = u64;

    #[derive(Default)]
    struct MockState {
        woken: AtomicBool,
    }

    #[derive(Clone)]
    struct MockWaker {
        state: Arc<MockState>,
        condvar: Arc<(Mutex<()>, Condvar)>,
    }

    impl ReactorWaker for MockWaker {
        fn wake(&self) -> io::Result<()> {
            self.state.woken.store(true, Ordering::Release);
            let (lock, cvar) = &*self.condvar;
            let _guard = lock.lock().unwrap();
            cvar.notify_all();
            Ok(())
        }
    }

    struct MockEvent {
        woke: bool,
        readable: Vec<CapId>,
        writable: Vec<CapId>,
    }

    impl ReactorEvent for MockEvent {
        type Handle = CapId;

        fn woke(&self) -> bool {
            self.woke
        }

        fn readable(&self) -> &[CapId] {
            &self.readable
        }

        fn writable(&self) -> &[CapId] {
            &self.writable
        }
    }

    struct MockReactor {
        state: Arc<MockState>,
        condvar: Arc<(Mutex<()>, Condvar)>,
    }

    impl MockReactor {
        fn new() -> Self {
            Self {
                state: Arc::new(MockState::default()),
                condvar: Arc::new((Mutex::new(()), Condvar::new())),
            }
        }
    }

    impl ReactorBackend for MockReactor {
        type Event = MockEvent;
        type Handle = CapId;
        type Waker = MockWaker;

        fn waker(&self) -> MockWaker {
            MockWaker {
                state: Arc::clone(&self.state),
                condvar: Arc::clone(&self.condvar),
            }
        }

        fn wait(
            &self,
            _read: &[CapId],
            _write: &[CapId],
            timeout: Option<Duration>,
        ) -> io::Result<MockEvent> {
            let (lock, cvar) = &*self.condvar;
            let mut guard = lock.lock().unwrap();
            if !self.state.woken.load(Ordering::Acquire) {
                match timeout {
                    Some(duration) => {
                        let _ = cvar.wait_timeout(guard, duration).unwrap();
                    }
                    None => {
                        guard = cvar.wait(guard).unwrap();
                        drop(guard);
                    }
                }
            }
            let woke = self.state.woken.swap(false, Ordering::AcqRel);
            Ok(MockEvent {
                woke,
                readable: Vec::new(),
                writable: Vec::new(),
            })
        }
    }

    /// Drives a backend through the wait/wake cycle purely through the trait,
    /// proving the executor could rely on `ReactorBackend` without knowing the
    /// concrete OS type.
    fn exercise_backend<B: ReactorBackend>(reactor: &B) {
        // A timeout with no wake reports nothing happened.
        let event = reactor
            .wait(&[], &[], Some(Duration::from_millis(1)))
            .unwrap();
        assert!(!event.woke());
        assert!(event.readable().is_empty());
        assert!(event.writable().is_empty());

        // A wake issued before the wait is observed by the wait.
        reactor.waker().wake().unwrap();
        let event = reactor
            .wait(&[], &[], Some(Duration::from_secs(1)))
            .unwrap();
        assert!(event.woke());
    }

    #[test]
    fn mock_backend_satisfies_contract() {
        let reactor = MockReactor::new();
        exercise_backend(&reactor);
    }

    #[cfg(unix)]
    #[test]
    fn unix_reactor_satisfies_contract() {
        let reactor = crate::os::OsReactor::new().unwrap();
        exercise_backend(&reactor);
    }
}
