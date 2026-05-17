#[cfg(unix)]
use crate::os::OsEvent;

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
    pub(super) readable: Vec<std::os::unix::io::RawFd>,
    pub(super) writable: Vec<std::os::unix::io::RawFd>,
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
