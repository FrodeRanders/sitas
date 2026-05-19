#[cfg(unix)]
#[cfg(target_os = "linux")]
use std::os::unix::io::RawFd;

#[cfg(unix)]
use crate::os::{OsEvent, OsReactor};

use super::scheduler::Scheduler;
#[cfg(target_os = "linux")]
use super::uring;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DriverEvent {
    #[cfg(unix)]
    readiness: Option<ReadinessEvent>,
    #[cfg(target_os = "linux")]
    completion: bool,
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
        Self {
            readiness: Some(ReadinessEvent {
                readable: event.readable,
                writable: event.writable,
            }),
            #[cfg(target_os = "linux")]
            completion: false,
        }
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
        Some(event) => {
            let readiness = event.readiness.as_ref();
            #[cfg(not(target_os = "linux"))]
            scheduler.record_readiness_driver_event(
                readiness.is_some_and(|event| !event.readable.is_empty()),
                readiness.is_some_and(|event| !event.writable.is_empty()),
            );

            #[cfg(target_os = "linux")]
            scheduler.record_driver_event(
                readiness.is_some(),
                readiness.is_some_and(|event| !event.readable.is_empty()),
                readiness.is_some_and(|event| !event.writable.is_empty()),
                event.completion,
            );

            if let Some(event) = event.readiness {
                scheduler.wake_readable_fds(&event.readable);
                scheduler.wake_writable_fds(&event.writable);
            }
        }
        None => {}
    }
}

#[cfg(unix)]
fn wait_for_reactor_event(
    scheduler: &Scheduler,
    reactor: &OsReactor,
    context: &str,
) -> DriverEvent {
    #[cfg(target_os = "linux")]
    prepare_io_uring_for_reactor_wait(scheduler, context);

    let read_fds = scheduler.read_interest_fds();
    let write_fds = scheduler.write_interest_fds();
    #[cfg(target_os = "linux")]
    let mut read_fds = read_fds;
    #[cfg(target_os = "linux")]
    let completion_fd = if uring::should_wait() {
        uring::completion_fd()
    } else {
        None
    };
    #[cfg(target_os = "linux")]
    if let Some(fd) = completion_fd
        && !read_fds.contains(&fd)
    {
        read_fds.push(fd);
    }

    let timeout = scheduler.time_until_next_timer();
    let event: DriverEvent = reactor
        .wait_io(&read_fds, &write_fds, timeout)
        .unwrap_or_else(|error| panic!("OS reactor wait failed while {context}: {error}"))
        .into();

    #[cfg(target_os = "linux")]
    let mut event = event;
    #[cfg(target_os = "linux")]
    if let Some(fd) = completion_fd {
        dispatch_io_uring_if_ready(scheduler, &mut event, fd);
    }

    event
}

#[cfg(target_os = "linux")]
fn prepare_io_uring_for_reactor_wait(scheduler: &Scheduler, context: &str) {
    uring::submit_pending()
        .unwrap_or_else(|error| panic!("io_uring submit failed while {context}: {error}"));
    refresh_io_uring_snapshot(scheduler);
}

#[cfg(target_os = "linux")]
fn dispatch_io_uring_if_ready(scheduler: &Scheduler, event: &mut DriverEvent, fd: RawFd) {
    let Some(readiness) = event.readiness.as_mut() else {
        return;
    };
    if let Some(index) = readiness
        .readable
        .iter()
        .position(|ready_fd| *ready_fd == fd)
    {
        readiness.readable.remove(index);
        uring::dispatch_available();
        refresh_io_uring_snapshot(scheduler);
        event.completion = true;
    }
}

#[cfg(target_os = "linux")]
fn refresh_io_uring_snapshot(scheduler: &Scheduler) {
    scheduler.record_io_uring_snapshot(uring::snapshot());
}
