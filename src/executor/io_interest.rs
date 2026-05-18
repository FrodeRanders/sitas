use std::os::unix::io::RawFd;
use std::task::Waker;

#[derive(Debug)]
pub(super) struct ReadinessInterests {
    reads: InterestSet,
    writes: InterestSet,
}

impl ReadinessInterests {
    pub(super) fn new() -> Self {
        Self {
            reads: InterestSet::new(),
            writes: InterestSet::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.reads.clear();
        self.writes.clear();
    }

    pub(super) fn read_len(&self) -> usize {
        self.reads.len()
    }

    pub(super) fn write_len(&self) -> usize {
        self.writes.len()
    }

    pub(super) fn allocate_read_id(&mut self) -> usize {
        self.reads.allocate_id()
    }

    pub(super) fn register_read(&mut self, id: usize, fd: RawFd, waker: Waker) {
        self.reads.register(id, fd, waker);
    }

    pub(super) fn remove_read(&mut self, id: usize) {
        self.reads.remove(id);
    }

    pub(super) fn read_fds(&self) -> Vec<RawFd> {
        self.reads.fds()
    }

    pub(super) fn wake_readable(&mut self, readable: &[RawFd]) -> Vec<Waker> {
        self.reads.wake_ready(readable)
    }

    pub(super) fn take_ready_read(&mut self, id: usize) -> bool {
        self.reads.take_ready(id)
    }

    pub(super) fn allocate_write_id(&mut self) -> usize {
        self.writes.allocate_id()
    }

    pub(super) fn register_write(&mut self, id: usize, fd: RawFd, waker: Waker) {
        self.writes.register(id, fd, waker);
    }

    pub(super) fn remove_write(&mut self, id: usize) {
        self.writes.remove(id);
    }

    pub(super) fn write_fds(&self) -> Vec<RawFd> {
        self.writes.fds()
    }

    pub(super) fn wake_writable(&mut self, writable: &[RawFd]) -> Vec<Waker> {
        self.writes.wake_ready(writable)
    }

    pub(super) fn take_ready_write(&mut self, id: usize) -> bool {
        self.writes.take_ready(id)
    }
}

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interest_ids_are_allocated_monotonically() {
        let mut set = InterestSet::new();

        assert_eq!(set.allocate_id(), 0);
        assert_eq!(set.allocate_id(), 1);
        assert_eq!(set.allocate_id(), 2);
    }

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

    #[test]
    fn registering_existing_interest_replaces_its_fd() {
        let mut set = InterestSet::new();
        let waker = Waker::noop().clone();

        set.register(0, 10, waker.clone());
        set.register(0, 11, waker);

        assert_eq!(set.len(), 1);
        assert_eq!(set.fds(), vec![11]);
        assert_eq!(set.wake_ready(&[10]).len(), 0);
        assert!(!set.take_ready(0));
        assert_eq!(set.wake_ready(&[11]).len(), 1);
        assert!(set.take_ready(0));
    }

    #[test]
    fn remove_drops_pending_and_ready_interest() {
        let mut set = InterestSet::new();
        let waker = Waker::noop().clone();

        set.register(0, 10, waker.clone());
        set.register(1, 11, waker);
        set.remove(0);

        assert_eq!(set.len(), 1);
        assert_eq!(set.fds(), vec![11]);

        assert_eq!(set.wake_ready(&[11]).len(), 1);
        set.remove(1);
        assert!(!set.take_ready(1));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn clear_drops_pending_and_ready_interests() {
        let mut set = InterestSet::new();
        let waker = Waker::noop().clone();

        set.register(0, 10, waker.clone());
        set.register(1, 11, waker);
        assert_eq!(set.wake_ready(&[10]).len(), 1);
        set.clear();

        assert_eq!(set.len(), 0);
        assert_eq!(set.fds(), Vec::<RawFd>::new());
        assert!(!set.take_ready(0));
        assert!(!set.take_ready(1));
    }

    #[test]
    fn readiness_interests_keep_read_and_write_sets_separate() {
        let mut interests = ReadinessInterests::new();
        let waker = Waker::noop().clone();
        let read_id = interests.allocate_read_id();
        let write_id = interests.allocate_write_id();

        interests.register_read(read_id, 10, waker.clone());
        interests.register_write(write_id, 10, waker);

        assert_eq!(interests.read_len(), 1);
        assert_eq!(interests.write_len(), 1);
        assert_eq!(interests.read_fds(), vec![10]);
        assert_eq!(interests.write_fds(), vec![10]);

        assert_eq!(interests.wake_readable(&[10]).len(), 1);
        assert!(interests.take_ready_read(read_id));
        assert!(!interests.take_ready_write(write_id));
        assert_eq!(interests.write_len(), 1);

        assert_eq!(interests.wake_writable(&[10]).len(), 1);
        assert!(interests.take_ready_write(write_id));
    }

    #[test]
    fn readiness_interests_clear_both_directions() {
        let mut interests = ReadinessInterests::new();
        let waker = Waker::noop().clone();

        interests.register_read(0, 10, waker.clone());
        interests.register_write(0, 11, waker);
        assert_eq!(interests.wake_readable(&[10]).len(), 1);
        assert_eq!(interests.wake_writable(&[11]).len(), 1);

        interests.clear();

        assert_eq!(interests.read_len(), 0);
        assert_eq!(interests.write_len(), 0);
        assert!(!interests.take_ready_read(0));
        assert!(!interests.take_ready_write(0));
    }
}
