use std::collections::HashMap;
use std::io;
use std::os::raw::{c_int, c_short, c_uint, c_void};
use std::os::unix::io::RawFd;
use std::ptr;
use std::sync::Mutex;
use std::time::Duration;

use super::{EINTR, OsEvent, OwnedFd, last_os_error};

const EVFILT_READ: c_short = -1;
const EVFILT_WRITE: c_short = -2;
const EV_ADD: u16 = 0x0001;
const EV_DELETE: u16 = 0x0002;
const EV_ENABLE: u16 = 0x0004;
const EV_ERROR: u16 = 0x4000;
const EV_EOF: u16 = 0x8000;

#[repr(C)]
#[derive(Clone, Copy)]
struct Kevent {
    ident: usize,
    filter: c_short,
    flags: u16,
    fflags: c_uint,
    data: isize,
    udata: *mut c_void,
}

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

unsafe extern "C" {
    fn kqueue() -> c_int;
    fn kevent(
        kq: c_int,
        changelist: *const Kevent,
        nchanges: c_int,
        eventlist: *mut Kevent,
        nevents: c_int,
        timeout: *const Timespec,
    ) -> c_int;
}

#[derive(Debug)]
pub(super) struct KqueueBackend {
    kqueue_fd: OwnedFd,
    registry: Mutex<KqueueRegistry>,
}

impl KqueueBackend {
    pub(super) fn new(wake_fd: RawFd) -> io::Result<Self> {
        let kqueue_fd = create_kqueue()?;
        let registry = Mutex::new(KqueueRegistry::new());

        register_kqueue_fd(kqueue_fd.raw(), wake_fd, EVFILT_READ, 0)?;
        registry
            .lock()
            .expect("kqueue registry mutex poisoned")
            .insert_wake(wake_fd);

        Ok(Self {
            kqueue_fd,
            registry,
        })
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
        let timeout = timeout.map(duration_to_timespec);
        let timeout_ptr = timeout.as_ref().map_or(ptr::null::<Timespec>(), |timeout| {
            timeout as *const Timespec
        });
        let interests = KqueueInterests::new(read_fds, write_fds);
        let registration =
            register_kqueue_interests(self.kqueue_fd.raw(), &self.registry, &interests)?;

        loop {
            let max_events = interests.len() + 1;
            let mut events = vec![empty_kevent(); max_events];

            // SAFETY: `events` points to initialized storage for `max_events`
            // event values, and the reactor-owned kqueue descriptor remains
            // open for the call.
            let result = unsafe {
                kevent(
                    self.kqueue_fd.raw(),
                    ptr::null::<Kevent>(),
                    0,
                    events.as_mut_ptr(),
                    max_events as c_int,
                    timeout_ptr,
                )
            };
            if result > 0 {
                let mut woke = false;
                let mut readable = Vec::new();
                let mut writable = Vec::new();

                for event in events.iter().take(result as usize) {
                    let token = event.udata as usize as u64;
                    if token == 0 {
                        woke = drain_wakes()?;
                        continue;
                    }
                    let interest = registration
                        .interest(token)
                        .expect("kqueue returned an unknown interest token");
                    if interest.read && event.filter == EVFILT_READ {
                        readable.push(interest.fd);
                    }
                    if interest.write && event.filter == EVFILT_WRITE {
                        writable.push(interest.fd);
                    }
                    if event.flags & (EV_ERROR | EV_EOF) != 0 {
                        if interest.read {
                            readable.push(interest.fd);
                        }
                        if interest.write {
                            writable.push(interest.fd);
                        }
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
struct KqueueInterest {
    fd: RawFd,
    read: bool,
    write: bool,
}

#[derive(Debug)]
struct KqueueInterests {
    interests: Vec<KqueueInterest>,
}

impl KqueueInterests {
    fn new(read_fds: &[RawFd], write_fds: &[RawFd]) -> Self {
        let mut interests = Vec::with_capacity(read_fds.len() + write_fds.len());

        for fd in read_fds {
            add_kqueue_interest(&mut interests, *fd, true, false);
        }
        for fd in write_fds {
            add_kqueue_interest(&mut interests, *fd, false, true);
        }

        Self { interests }
    }

    fn iter(&self) -> impl Iterator<Item = &KqueueInterest> {
        self.interests.iter()
    }

    fn len(&self) -> usize {
        self.interests.len()
    }
}

fn add_kqueue_interest(interests: &mut Vec<KqueueInterest>, fd: RawFd, read: bool, write: bool) {
    if let Some(interest) = interests.iter_mut().find(|interest| interest.fd == fd) {
        interest.read |= read;
        interest.write |= write;
    } else {
        interests.push(KqueueInterest { fd, read, write });
    }
}

#[derive(Debug)]
struct KqueueRegistry {
    next_token: u64,
    interests: HashMap<u64, KqueueInterest>,
}

impl KqueueRegistry {
    fn new() -> Self {
        Self {
            next_token: 1,
            interests: HashMap::new(),
        }
    }

    fn insert_wake(&mut self, fd: RawFd) {
        self.interests.insert(
            0,
            KqueueInterest {
                fd,
                read: true,
                write: false,
            },
        );
    }

    fn insert_temporary(&mut self, interest: KqueueInterest) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        self.interests.insert(token, interest);
        token
    }

    fn remove(&mut self, token: u64) -> Option<KqueueInterest> {
        self.interests.remove(&token)
    }

    fn get(&self, token: u64) -> Option<KqueueInterest> {
        self.interests.get(&token).copied()
    }
}

struct KqueueRegistration<'a> {
    kqueue_fd: RawFd,
    registry: &'a Mutex<KqueueRegistry>,
    tokens: Vec<u64>,
}

impl Drop for KqueueRegistration<'_> {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .expect("kqueue registry mutex poisoned");

        for token in self.tokens.drain(..) {
            let Some(interest) = registry.remove(token) else {
                continue;
            };
            if interest.read {
                let _ = delete_kqueue_fd(self.kqueue_fd, interest.fd, EVFILT_READ);
            }
            if interest.write {
                let _ = delete_kqueue_fd(self.kqueue_fd, interest.fd, EVFILT_WRITE);
            }
        }
    }
}

impl KqueueRegistration<'_> {
    fn interest(&self, token: u64) -> Option<KqueueInterest> {
        self.registry
            .lock()
            .expect("kqueue registry mutex poisoned")
            .get(token)
    }
}

fn register_kqueue_interests<'a>(
    kqueue_fd: RawFd,
    registry: &'a Mutex<KqueueRegistry>,
    interests: &KqueueInterests,
) -> io::Result<KqueueRegistration<'a>> {
    let mut registration = KqueueRegistration {
        kqueue_fd,
        registry,
        tokens: Vec::with_capacity(interests.len()),
    };

    for interest in interests.iter().copied() {
        let token = {
            let mut registry = registry.lock().expect("kqueue registry mutex poisoned");
            registry.insert_temporary(interest)
        };

        if let Err(error) = register_temporary_kqueue_interest(kqueue_fd, interest, token) {
            registry
                .lock()
                .expect("kqueue registry mutex poisoned")
                .remove(token);
            return Err(error);
        }

        registration.tokens.push(token);
    }

    Ok(registration)
}

fn register_temporary_kqueue_interest(
    kqueue_fd: RawFd,
    interest: KqueueInterest,
    token: u64,
) -> io::Result<()> {
    if interest.read {
        register_kqueue_fd(kqueue_fd, interest.fd, EVFILT_READ, token)?;
    }
    if interest.write {
        register_kqueue_fd(kqueue_fd, interest.fd, EVFILT_WRITE, token)?;
    }
    Ok(())
}

fn create_kqueue() -> io::Result<OwnedFd> {
    // SAFETY: `kqueue` takes no borrowed memory and returns a new descriptor on
    // success.
    let fd = unsafe { kqueue() };
    if fd < 0 {
        Err(last_os_error())
    } else {
        Ok(OwnedFd::new(fd))
    }
}

fn register_kqueue_fd(kqueue_fd: RawFd, fd: RawFd, filter: c_short, token: u64) -> io::Result<()> {
    let event = Kevent {
        ident: fd as usize,
        filter,
        flags: EV_ADD | EV_ENABLE,
        fflags: 0,
        data: 0,
        udata: token as usize as *mut c_void,
    };

    submit_kqueue_change(kqueue_fd, &event)
}

fn delete_kqueue_fd(kqueue_fd: RawFd, fd: RawFd, filter: c_short) -> io::Result<()> {
    let event = Kevent {
        ident: fd as usize,
        filter,
        flags: EV_DELETE,
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
    };

    submit_kqueue_change(kqueue_fd, &event)
}

fn submit_kqueue_change(kqueue_fd: RawFd, event: &Kevent) -> io::Result<()> {
    // SAFETY: `kqueue_fd` is an open kqueue descriptor and `event` points to
    // one initialized change event for the duration of the call.
    let result = unsafe {
        kevent(
            kqueue_fd,
            event as *const Kevent,
            1,
            ptr::null_mut::<Kevent>(),
            0,
            ptr::null::<Timespec>(),
        )
    };
    if result < 0 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

fn empty_kevent() -> Kevent {
    Kevent {
        ident: 0,
        filter: 0,
        flags: 0,
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
    }
}

fn duration_to_timespec(duration: Duration) -> Timespec {
    Timespec {
        tv_sec: duration.as_secs().min(i64::MAX as u64) as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    }
}
