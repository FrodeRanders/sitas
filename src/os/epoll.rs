use std::collections::HashMap;
use std::io;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::sync::Mutex;
use std::time::Duration;

use super::{EINTR, OsEvent, OwnedFd, last_os_error, timeout_to_wait_ms};

const EPOLLIN: u32 = 0x0001;
const EPOLLOUT: u32 = 0x0004;
const EPOLLERR: u32 = 0x0008;
const EPOLLHUP: u32 = 0x0010;
const EPOLL_CTL_ADD: c_int = 1;
const EPOLL_CTL_DEL: c_int = 2;

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
        let registration =
            register_epoll_interests(self.epoll_fd.raw(), &self.registry, &interests)?;

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
                    let interest = registration
                        .interest(event.data)
                        .expect("epoll returned an unknown interest index");
                    if interest.read && event.events & (EPOLLIN | EPOLLERR | EPOLLHUP) != 0 {
                        push_unique_fd(&mut readable, interest.fd);
                    }
                    if interest.write && event.events & (EPOLLOUT | EPOLLERR | EPOLLHUP) != 0 {
                        push_unique_fd(&mut writable, interest.fd);
                    }
                }

                return Ok(OsEvent {
                    woke,
                    readable,
                    writable,
                });
            }
            if result == 0 {
                return Ok(OsEvent {
                    woke: false,
                    readable: Vec::new(),
                    writable: Vec::new(),
                });
            }

            let error = last_os_error();
            if error.raw_os_error() == Some(EINTR) {
                continue;
            }
            return Err(error);
        }
    }
}

#[derive(Debug, Clone, Copy)]
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
    interests: HashMap<u64, EpollInterest>,
}

impl EpollRegistry {
    fn new() -> Self {
        Self {
            next_token: 1,
            interests: HashMap::new(),
        }
    }

    fn insert_wake(&mut self, fd: RawFd) {
        self.interests.insert(
            0,
            EpollInterest {
                fd,
                read: true,
                write: false,
            },
        );
    }

    fn insert_temporary(&mut self, interest: EpollInterest) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        self.interests.insert(token, interest);
        token
    }

    fn remove(&mut self, token: u64) -> Option<EpollInterest> {
        self.interests.remove(&token)
    }

    fn get(&self, token: u64) -> Option<EpollInterest> {
        self.interests.get(&token).copied()
    }
}

struct EpollRegistration<'a> {
    epoll_fd: RawFd,
    registry: &'a Mutex<EpollRegistry>,
    tokens: Vec<u64>,
}

impl Drop for EpollRegistration<'_> {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().expect("epoll registry mutex poisoned");

        for token in self.tokens.drain(..) {
            let Some(interest) = registry.remove(token) else {
                continue;
            };
            // SAFETY: `epoll_fd` is owned by the reactor and `fd` was
            // previously registered by this guard. The event pointer is unused
            // for `EPOLL_CTL_DEL` on Linux.
            let _ = unsafe {
                epoll_ctl(
                    self.epoll_fd,
                    EPOLL_CTL_DEL,
                    interest.fd,
                    std::ptr::null_mut::<EpollEvent>(),
                )
            };
        }
    }
}

impl EpollRegistration<'_> {
    fn interest(&self, token: u64) -> Option<EpollInterest> {
        self.registry
            .lock()
            .expect("epoll registry mutex poisoned")
            .get(token)
    }
}

fn register_epoll_interests<'a>(
    epoll_fd: RawFd,
    registry: &'a Mutex<EpollRegistry>,
    interests: &EpollInterests,
) -> io::Result<EpollRegistration<'a>> {
    let mut registration = EpollRegistration {
        epoll_fd,
        registry,
        tokens: Vec::with_capacity(interests.len()),
    };

    for interest in interests.iter().copied() {
        let token = {
            let mut registry = registry.lock().expect("epoll registry mutex poisoned");
            registry.insert_temporary(interest)
        };

        if let Err(error) = register_epoll_fd(epoll_fd, interest.fd, interest.events(), token) {
            registry
                .lock()
                .expect("epoll registry mutex poisoned")
                .remove(token);
            return Err(error);
        }

        registration.tokens.push(token);
    }

    Ok(registration)
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
    let mut event = EpollEvent { events, data };

    // SAFETY: `epoll_fd` is an open epoll descriptor, `fd` is borrowed for
    // readiness observation, and `event` points to an initialized event value
    // for the duration of the call.
    let result = unsafe { epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &mut event) };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

fn push_unique_fd(fds: &mut Vec<RawFd>, fd: RawFd) {
    if !fds.contains(&fd) {
        fds.push(fd);
    }
}
