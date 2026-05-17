#[cfg(unix)]
use crate::os::{OsEvent, OsReactor};

use super::scheduler::Scheduler;
#[cfg(target_os = "linux")]
use super::uring;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DriverEvent {
    #[cfg(unix)]
    Readiness(ReadinessEvent),
    #[cfg(target_os = "linux")]
    Completion,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadinessEvent {
    readable: Vec<std::os::unix::io::RawFd>,
    writable: Vec<std::os::unix::io::RawFd>,
}

#[cfg(unix)]
impl From<OsEvent> for DriverEvent {
    fn from(event: OsEvent) -> Self {
        Self::Readiness(ReadinessEvent {
            readable: event.readable,
            writable: event.writable,
        })
    }
}

#[cfg(target_os = "linux")]
pub(super) fn dispatch_available(scheduler: &Scheduler) {
    uring::dispatch_available();
    refresh_io_uring_snapshot(scheduler);
}

#[cfg(not(target_os = "linux"))]
pub(super) fn dispatch_available(_scheduler: &Scheduler) {}

#[cfg(unix)]
pub(super) fn wait_for_event(
    scheduler: &Scheduler,
    reactor: &OsReactor,
    context: &str,
) -> Option<DriverEvent> {
    #[cfg(target_os = "linux")]
    if wait_for_io_uring_event(scheduler) {
        return Some(DriverEvent::Completion);
    }

    Some(wait_for_reactor_event(scheduler, reactor, context))
}

#[cfg(not(unix))]
pub(super) fn wait_for_event(scheduler: &Scheduler, context: &str) -> Option<DriverEvent> {
    let _ = (scheduler, context);
    None
}

pub(super) fn apply_event(scheduler: &Scheduler, event: Option<DriverEvent>) {
    match event {
        #[cfg(unix)]
        Some(DriverEvent::Readiness(event)) => {
            scheduler.record_readiness_driver_event(
                !event.readable.is_empty(),
                !event.writable.is_empty(),
            );
            scheduler.wake_readable_fds(&event.readable);
            scheduler.wake_writable_fds(&event.writable);
        }
        #[cfg(target_os = "linux")]
        Some(DriverEvent::Completion) => {
            scheduler.record_completion_driver_event();
        }
        None => {}
    }
}

#[cfg(target_os = "linux")]
fn wait_for_io_uring_event(scheduler: &Scheduler) -> bool {
    if uring::should_wait() {
        uring::wait_and_dispatch().expect("io_uring wait failed while running executor");
        refresh_io_uring_snapshot(scheduler);
        return true;
    }

    false
}

#[cfg(unix)]
fn wait_for_reactor_event(
    scheduler: &Scheduler,
    reactor: &OsReactor,
    context: &str,
) -> DriverEvent {
    let read_fds = scheduler.read_interest_fds();
    let write_fds = scheduler.write_interest_fds();
    let timeout = scheduler.time_until_next_timer();
    reactor
        .wait_io(&read_fds, &write_fds, timeout)
        .unwrap_or_else(|error| panic!("OS reactor wait failed while {context}: {error}"))
        .into()
}

#[cfg(target_os = "linux")]
fn refresh_io_uring_snapshot(scheduler: &Scheduler) {
    scheduler.record_io_uring_snapshot(uring::snapshot());
}
