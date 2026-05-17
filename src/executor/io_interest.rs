use std::os::unix::io::RawFd;
use std::task::Waker;

#[derive(Debug)]
pub(super) struct InterestSet {
    interests: Vec<IoInterest>,
    ready: Vec<usize>,
    next_id: usize,
}

#[derive(Debug)]
struct IoInterest {
    id: usize,
    fd: RawFd,
    waker: Waker,
}

impl InterestSet {
    pub(super) fn new() -> Self {
        Self {
            interests: Vec::new(),
            ready: Vec::new(),
            next_id: 0,
        }
    }

    pub(super) fn allocate_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    pub(super) fn register(&mut self, id: usize, fd: RawFd, waker: Waker) {
        match self.interests.iter_mut().find(|interest| interest.id == id) {
            Some(interest) => {
                interest.fd = fd;
                interest.waker = waker;
            }
            None => self.interests.push(IoInterest { id, fd, waker }),
        }
    }

    pub(super) fn remove(&mut self, id: usize) {
        self.interests.retain(|interest| interest.id != id);
        self.ready.retain(|ready_id| *ready_id != id);
    }

    pub(super) fn clear(&mut self) {
        self.interests.clear();
        self.ready.clear();
    }

    pub(super) fn fds(&self) -> Vec<RawFd> {
        let mut fds = Vec::new();

        for interest in &self.interests {
            if !fds.contains(&interest.fd) {
                fds.push(interest.fd);
            }
        }

        fds
    }

    pub(super) fn len(&self) -> usize {
        self.interests.len()
    }

    pub(super) fn wake_ready(&mut self, ready_fds: &[RawFd]) -> Vec<Waker> {
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

    pub(super) fn take_ready(&mut self, id: usize) -> bool {
        let Some(position) = self.ready.iter().position(|ready_id| *ready_id == id) else {
            return false;
        };

        self.ready.swap_remove(position);
        true
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.interests.is_empty() && self.ready.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interest_set_reports_unique_fds_but_wakes_all_waiters() {
        let mut set = InterestSet::new();
        let waker = Waker::noop().clone();

        set.register(0, 10, waker.clone());
        set.register(1, 10, waker.clone());
        set.register(2, 11, waker);

        assert_eq!(set.fds(), vec![10, 11]);

        let wakers = set.wake_ready(&[10]);
        assert_eq!(wakers.len(), 2);
        assert!(set.take_ready(0));
        assert!(set.take_ready(1));
        assert!(!set.take_ready(2));
        assert_eq!(set.fds(), vec![11]);
    }
}
