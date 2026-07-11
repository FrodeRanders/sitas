//! Linux `epoll(7)` readiness backend.
//!
//! Maintains a persistent kernel registration set and reconciles it
//! against the executor's current fd interests before each wait, so
//! unchanged interests do not churn through add/delete syscalls.

use std::collections::HashMap;
use std::io;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::sync::Mutex;
use std::time::Duration;

use super::{EINTR, OsEvent, OwnedFd, last_os_error, push_unique_fd, timeout_to_wait_ms};

const EPOLLIN: u32 = 0x0001;
const EPOLLOUT: u32 = 0x0004;
const EPOLLERR: u32 = 0x0008;
const EPOLLHUP: u32 = 0x0010;
const EPOLL_CTL_ADD: c_int = 1;
const EPOLL_CTL_DEL: c_int = 2;
const EPOLL_CTL_MOD: c_int = 3;

#[repr(C)]
#[derive(Clone, Copy)]
struct EpollEvent {
    events: u32,
    data: u64,
}

unsafe extern "C" {
    fn epoll_create1(flags: c_int) -> c_int;
    fn epoll_ctl(epoll_fd: c_int, op: c_int, fd: c_int, event: *mut EpollEvent) -> c_int;
    fn epoll_wait(
        epoll_fd: c_int,
        events: *mut EpollEvent,
        maxevents: c_int,
        timeout: c_int,
    ) -> c_int;
}

#[derive(Debug)]
pub(super) struct EpollBackend {
    epoll_fd: OwnedFd,
    registry: Mutex<EpollRegistry>,
}

impl EpollBackend {
    pub(super) fn new(wake_fd: RawFd) -> io::Result<Self> {
        let epoll_fd = create_epoll()?;
        let registry = Mutex::new(EpollRegistry::new());

        register_epoll_fd(epoll_fd.raw(), wake_fd, EPOLLIN, 0)?;
        registry
            .lock()
            .expect("epoll registry mutex poisoned")
            .insert_wake(wake_fd);

        Ok(Self { epoll_fd, registry })
    }

    pub(super) fn wait_io<F>(
        &self,
        read_fds: &[RawFd],
        write_fds: &[RawFd],
        timeout: Option<Duration>,
        mut drain_wakes: F,
    ) -> io::Result<OsEvent>
    where
        F: FnMut() -> io::Result<bool>,
    {
        let timeout_ms = timeout_to_wait_ms(timeout);
        let interests = EpollInterests::new(read_fds, write_fds);
        reconcile_epoll_interests(self.epoll_fd.raw(), &self.registry, &interests)?;

        loop {
            let max_events = interests.len() + 1;
            let mut events = vec![EpollEvent { events: 0, data: 0 }; max_events];

            // SAFETY: `events` points to initialized storage for `max_events`
            // event values, and the reactor-owned epoll descriptor remains
            // open for the call.
            let result = unsafe {
                epoll_wait(
                    self.epoll_fd.raw(),
                    events.as_mut_ptr(),
                    max_events as c_int,
                    timeout_ms,
                )
            };
            if result > 0 {
                let mut woke = false;
                let mut readable = Vec::new();
                let mut writable = Vec::new();

                for event in events.iter().take(result as usize) {
                    if event.data == 0 {
                        woke = event.events & EPOLLIN != 0 && drain_wakes()?;
                        continue;
                    }
                    let interest = self
                        .interest(event.data)
                        .expect("epoll returned an unknown interest index");
                    if interest.read && event.events & (EPOLLIN | EPOLLERR | EPOLLHUP) != 0 {
                        push_unique_fd(&mut readable, interest.fd);
                    }
                    if interest.write && event.events & (EPOLLOUT | EPOLLERR | EPOLLHUP) != 0 {
                        push_unique_fd(&mut writable, interest.fd);
                    }
                }

                return Ok(OsEvent::ready(woke, readable, writable));
            }
            if result == 0 {
                return Ok(OsEvent::empty());
            }

            let error = last_os_error();
            if error.raw_os_error() == Some(EINTR) {
                continue;
            }
            return Err(error);
        }
    }

    fn interest(&self, token: u64) -> Option<EpollInterest> {
        self.registry
            .lock()
            .expect("epoll registry mutex poisoned")
            .get(token)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EpollInterest {
    fd: RawFd,
    read: bool,
    write: bool,
}

impl EpollInterest {
    fn events(&self) -> u32 {
        let mut events = 0;
        if self.read {
            events |= EPOLLIN;
        }
        if self.write {
            events |= EPOLLOUT;
        }
        events
    }
}

#[derive(Debug)]
struct EpollInterests {
    interests: Vec<EpollInterest>,
}

impl EpollInterests {
    fn new(read_fds: &[RawFd], write_fds: &[RawFd]) -> Self {
        let mut interests = Vec::with_capacity(read_fds.len() + write_fds.len());

        for fd in read_fds {
            add_epoll_interest(&mut interests, *fd, true, false);
        }
        for fd in write_fds {
            add_epoll_interest(&mut interests, *fd, false, true);
        }

        Self { interests }
    }

    fn iter(&self) -> impl Iterator<Item = &EpollInterest> {
        self.interests.iter()
    }

    fn len(&self) -> usize {
        self.interests.len()
    }

    fn contains_fd(&self, fd: RawFd) -> bool {
        self.interests.iter().any(|interest| interest.fd == fd)
    }
}

fn add_epoll_interest(interests: &mut Vec<EpollInterest>, fd: RawFd, read: bool, write: bool) {
    if let Some(interest) = interests.iter_mut().find(|interest| interest.fd == fd) {
        interest.read |= read;
        interest.write |= write;
    } else {
        interests.push(EpollInterest { fd, read, write });
    }
}

#[derive(Debug)]
struct EpollRegistry {
    next_token: u64,
    by_token: HashMap<u64, EpollInterest>,
    by_fd: HashMap<RawFd, u64>,
}

impl EpollRegistry {
    fn new() -> Self {
        Self {
            next_token: 1,
            by_token: HashMap::new(),
            by_fd: HashMap::new(),
        }
    }

    fn insert_wake(&mut self, fd: RawFd) {
        self.by_token.insert(
            0,
            EpollInterest {
                fd,
                read: true,
                write: false,
            },
        );
    }

    fn allocate_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        token
    }

    fn insert(&mut self, token: u64, interest: EpollInterest) {
        self.by_fd.insert(interest.fd, token);
        self.by_token.insert(token, interest);
    }

    fn update(&mut self, token: u64, interest: EpollInterest) {
        self.by_token.insert(token, interest);
    }

    fn remove_fd(&mut self, fd: RawFd) -> Option<EpollInterest> {
        let token = self.by_fd.remove(&fd)?;
        self.by_token.remove(&token)
    }

    fn token_for_fd(&self, fd: RawFd) -> Option<u64> {
        self.by_fd.get(&fd).copied()
    }

    fn get(&self, token: u64) -> Option<EpollInterest> {
        self.by_token.get(&token).copied()
    }

    fn interest_fds(&self) -> Vec<RawFd> {
        self.by_fd.keys().copied().collect()
    }
}

fn reconcile_epoll_interests(
    epoll_fd: RawFd,
    registry: &Mutex<EpollRegistry>,
    interests: &EpollInterests,
) -> io::Result<()> {
    let mut registry = registry.lock().expect("epoll registry mutex poisoned");
    let stale_fds: Vec<_> = registry
        .interest_fds()
        .into_iter()
        .filter(|fd| !interests.contains_fd(*fd))
        .collect();

    for fd in stale_fds {
        if registry.remove_fd(fd).is_some() {
            let _ = delete_epoll_fd(epoll_fd, fd);
        }
    }

    for interest in interests.iter().copied() {
        if let Some(token) = registry.token_for_fd(interest.fd) {
            if registry.get(token) != Some(interest) {
                modify_epoll_fd(epoll_fd, interest.fd, interest.events(), token)?;
                registry.update(token, interest);
            }
        } else {
            let token = registry.allocate_token();
            register_epoll_fd(epoll_fd, interest.fd, interest.events(), token)?;
            registry.insert(token, interest);
        }
    }

    Ok(())
}

fn create_epoll() -> io::Result<OwnedFd> {
    // SAFETY: `epoll_create1` is called with no flags and does not borrow
    // memory from Rust.
    let fd = unsafe { epoll_create1(0) };
    if fd < 0 {
        Err(last_os_error())
    } else {
        Ok(OwnedFd::new(fd))
    }
}

fn register_epoll_fd(epoll_fd: RawFd, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
    control_epoll_fd(epoll_fd, EPOLL_CTL_ADD, fd, events, data)
}

fn modify_epoll_fd(epoll_fd: RawFd, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
    control_epoll_fd(epoll_fd, EPOLL_CTL_MOD, fd, events, data)
}

fn delete_epoll_fd(epoll_fd: RawFd, fd: RawFd) -> io::Result<()> {
    // SAFETY: `epoll_fd` is owned by the reactor and `fd` was previously
    // registered by this backend. The event pointer is unused for
    // `EPOLL_CTL_DEL` on Linux.
    let result = unsafe { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

fn control_epoll_fd(
    epoll_fd: RawFd,
    op: c_int,
    fd: RawFd,
    events: u32,
    data: u64,
) -> io::Result<()> {
    let mut event = EpollEvent { events, data };

    // SAFETY: `epoll_fd` is an open epoll descriptor, `fd` is borrowed for
    // readiness observation, and `event` points to an initialized event value
    // for the duration of the call.
    let result = unsafe { epoll_ctl(epoll_fd, op, fd, &mut event) };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoll_interest_events_read_only() {
        let interest = EpollInterest {
            fd: 1,
            read: true,
            write: false,
        };
        assert_eq!(interest.events(), EPOLLIN);
    }

    #[test]
    fn epoll_interest_events_write_only() {
        let interest = EpollInterest {
            fd: 1,
            read: false,
            write: true,
        };
        assert_eq!(interest.events(), EPOLLOUT);
    }

    #[test]
    fn epoll_interest_events_read_write() {
        let interest = EpollInterest {
            fd: 1,
            read: true,
            write: true,
        };
        assert_eq!(interest.events(), EPOLLIN | EPOLLOUT);
    }

    #[test]
    fn epoll_interests_new_merges_duplicate_fds() {
        let interests = EpollInterests::new(&[1, 2, 1], &[2, 3]);
        assert_eq!(interests.len(), 3);
        assert!(interests.contains_fd(1));
        assert!(interests.contains_fd(2));
        assert!(interests.contains_fd(3));
        assert!(!interests.contains_fd(4));

        let fd1 = interests
            .iter()
            .find(|i| i.fd == 1)
            .expect("fd 1 should be present");
        assert!(fd1.read, "fd1 appeared in read set (twice)");
        assert!(!fd1.write, "fd1 was not in write set");

        let fd2 = interests
            .iter()
            .find(|i| i.fd == 2)
            .expect("fd 2 should be present");
        assert!(fd2.read, "fd2 appeared in read set");
        assert!(fd2.write, "fd2 appeared in write set");
    }

    #[test]
    fn epoll_interests_empty() {
        let interests = EpollInterests::new(&[], &[]);
        assert_eq!(interests.len(), 0);
        assert!(!interests.contains_fd(0));
    }

    #[test]
    fn epoll_registry_allocate_token_increments() {
        let mut reg = EpollRegistry::new();
        assert_eq!(reg.allocate_token(), 1);
        assert_eq!(reg.allocate_token(), 2);
        assert_eq!(reg.allocate_token(), 3);
    }

    #[test]
    fn epoll_registry_insert_and_get() {
        let mut reg = EpollRegistry::new();
        let token = reg.allocate_token();
        let interest = EpollInterest {
            fd: 42,
            read: true,
            write: false,
        };
        reg.insert(token, interest);
        assert_eq!(reg.get(token), Some(interest));
        assert_eq!(reg.token_for_fd(42), Some(token));
    }

    #[test]
    fn epoll_registry_update() {
        let mut reg = EpollRegistry::new();
        let token = reg.allocate_token();
        let initial = EpollInterest {
            fd: 10,
            read: true,
            write: false,
        };
        reg.insert(token, initial);
        let updated = EpollInterest {
            fd: 10,
            read: true,
            write: true,
        };
        reg.update(token, updated);
        assert_eq!(reg.get(token), Some(updated));
        assert_eq!(reg.token_for_fd(10), Some(token));
    }

    #[test]
    fn epoll_registry_remove() {
        let mut reg = EpollRegistry::new();
        let token = reg.allocate_token();
        let interest = EpollInterest {
            fd: 99,
            read: true,
            write: false,
        };
        reg.insert(token, interest);
        let removed = reg.remove_fd(99);
        assert_eq!(removed, Some(interest));
        assert_eq!(reg.get(token), None);
        assert_eq!(reg.token_for_fd(99), None);
    }

    #[test]
    fn epoll_registry_remove_nonexistent() {
        let mut reg = EpollRegistry::new();
        assert_eq!(reg.remove_fd(999), None);
    }

    #[test]
    fn epoll_registry_get_nonexistent_token() {
        let reg = EpollRegistry::new();
        assert_eq!(reg.get(0), None);
        assert_eq!(reg.get(999), None);
    }

    #[test]
    fn epoll_registry_token_for_fd_nonexistent() {
        let reg = EpollRegistry::new();
        assert_eq!(reg.token_for_fd(42), None);
    }

    #[test]
    fn epoll_registry_interest_fds() {
        let mut reg = EpollRegistry::new();
        let t1 = reg.allocate_token();
        let t2 = reg.allocate_token();
        reg.insert(
            t1,
            EpollInterest {
                fd: 10,
                read: true,
                write: false,
            },
        );
        reg.insert(
            t2,
            EpollInterest {
                fd: 20,
                read: false,
                write: true,
            },
        );
        let mut fds = reg.interest_fds();
        fds.sort();
        assert_eq!(fds, vec![10, 20]);
    }

    #[test]
    fn epoll_registry_insert_wake() {
        let mut reg = EpollRegistry::new();
        reg.insert_wake(5);
        let wake = reg.get(0).expect("wake fd should be at token 0");
        assert_eq!(wake.fd, 5);
        assert!(wake.read);
        assert!(!wake.write);
    }

    #[test]
    fn add_epoll_interest_new_fd() {
        let mut interests = Vec::new();
        add_epoll_interest(&mut interests, 1, true, false);
        assert_eq!(interests.len(), 1);
        assert_eq!(interests[0].fd, 1);
        assert!(interests[0].read);
        assert!(!interests[0].write);
    }

    #[test]
    fn add_epoll_interest_merge() {
        let mut interests = vec![EpollInterest {
            fd: 1,
            read: true,
            write: false,
        }];
        add_epoll_interest(&mut interests, 1, false, true);
        assert_eq!(interests.len(), 1);
        assert!(interests[0].read);
        assert!(interests[0].write);
    }
}
