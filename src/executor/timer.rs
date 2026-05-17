use std::task::Waker;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(super) struct TimerSet {
    timers: Vec<TimerEntry>,
    next_id: usize,
}

#[derive(Debug)]
struct TimerEntry {
    id: usize,
    deadline: Instant,
    waker: Waker,
}

impl TimerSet {
    pub(super) fn new() -> Self {
        Self {
            timers: Vec::new(),
            next_id: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.timers.clear();
    }

    pub(super) fn allocate_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    pub(super) fn len(&self) -> usize {
        self.timers.len()
    }

    pub(super) fn register(&mut self, id: usize, deadline: Instant, waker: Waker) -> bool {
        let previous_next = self.next_deadline();

        match self.timers.iter_mut().find(|timer| timer.id == id) {
            Some(timer) => {
                timer.deadline = deadline;
                timer.waker = waker;
            }
            None => self.timers.push(TimerEntry {
                id,
                deadline,
                waker,
            }),
        }

        previous_next.is_none_or(|previous| deadline < previous)
    }

    pub(super) fn remove(&mut self, id: usize) {
        self.timers.retain(|timer| timer.id != id);
    }

    pub(super) fn expired(&mut self, now: Instant) -> Vec<Waker> {
        let mut expired = Vec::new();
        let mut pending = Vec::with_capacity(self.timers.len());

        for timer in self.timers.drain(..) {
            if timer.deadline <= now {
                expired.push(timer.waker);
            } else {
                pending.push(timer);
            }
        }

        self.timers = pending;
        expired
    }

    pub(super) fn time_until_next(&self, now: Instant) -> Option<Duration> {
        let deadline = self.next_deadline()?;
        Some(deadline.saturating_duration_since(now))
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.timers.iter().map(|timer| timer.deadline).min()
    }
}
