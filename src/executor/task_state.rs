use std::time::{Duration, Instant};

use super::{BoxFuture, PanicHandler, TaskId, TaskSnapshot, TaskStatus, TaskWait};

pub(super) struct TaskState {
    future: Option<BoxFuture>,
    panic_handler: Option<PanicHandler>,
    queued: bool,
    polling: bool,
    cancel_requested: bool,
    completed: bool,
    poll_count: u64,
    total_poll_time: Duration,
    waiting_for: Option<TaskWait>,
    last_scheduled_at: Option<Instant>,
    last_poll_started_at: Option<Instant>,
    last_poll_finished_at: Option<Instant>,
}

impl TaskState {
    pub(super) fn new(future: BoxFuture, panic_handler: Option<PanicHandler>) -> Self {
        Self {
            future: Some(future),
            panic_handler,
            queued: false,
            polling: false,
            cancel_requested: false,
            completed: false,
            poll_count: 0,
            total_poll_time: Duration::ZERO,
            waiting_for: None,
            last_scheduled_at: None,
            last_poll_started_at: None,
            last_poll_finished_at: None,
        }
    }

    pub(super) fn take_future_for_poll(&mut self, poll_started_at: Instant) -> Option<BoxFuture> {
        self.queued = false;

        let future = self.future.take()?;
        self.polling = true;
        self.waiting_for = None;
        self.last_poll_started_at = Some(poll_started_at);
        self.poll_count += 1;
        Some(future)
    }

    pub(super) fn finish_ready(&mut self, poll_duration: Duration, poll_finished_at: Instant) {
        self.polling = false;
        self.completed = true;
        self.waiting_for = None;
        self.total_poll_time += poll_duration;
        self.last_poll_finished_at = Some(poll_finished_at);
    }

    pub(super) fn finish_pending(
        &mut self,
        future: BoxFuture,
        poll_duration: Duration,
        poll_finished_at: Instant,
    ) -> bool {
        self.polling = false;
        self.total_poll_time += poll_duration;
        self.last_poll_finished_at = Some(poll_finished_at);
        if self.waiting_for.is_none() {
            self.waiting_for = Some(TaskWait::Unknown);
        }
        if self.cancel_requested {
            self.completed = true;
            self.waiting_for = None;
            true
        } else {
            self.future = Some(future);
            false
        }
    }

    pub(super) fn finish_panicked(
        &mut self,
        poll_duration: Duration,
        poll_finished_at: Instant,
    ) -> Option<PanicHandler> {
        self.polling = false;
        self.future = None;
        self.completed = true;
        self.waiting_for = None;
        self.total_poll_time += poll_duration;
        self.last_poll_finished_at = Some(poll_finished_at);
        self.panic_handler.take()
    }

    pub(super) fn mark_queued(&mut self, now: Instant) -> bool {
        if self.queued || (self.future.is_none() && !self.polling) {
            return false;
        }

        self.queued = true;
        self.waiting_for = None;
        self.last_scheduled_at = Some(now);
        true
    }

    pub(super) fn cancel(&mut self) -> Option<bool> {
        if self.future.is_none() && !self.polling {
            return None;
        }

        self.cancel_requested = true;

        if self.polling {
            Some(false)
        } else {
            self.future = None;
            self.queued = false;
            self.completed = true;
            self.waiting_for = None;
            Some(true)
        }
    }

    pub(super) fn drop_future(&mut self) {
        self.cancel_requested = true;
        if !self.polling {
            self.future = None;
            self.queued = false;
            self.completed = true;
            self.waiting_for = None;
        }
    }

    pub(super) fn clear_queued(&mut self) {
        self.queued = false;
    }

    pub(super) fn set_waiting_for(&mut self, waiting_for: TaskWait) {
        if self.polling && !self.completed {
            self.waiting_for = Some(waiting_for);
        }
    }

    pub(super) fn snapshot(
        &self,
        id: TaskId,
        name: Option<String>,
        created_at: Instant,
    ) -> TaskSnapshot {
        let status = if self.completed && self.cancel_requested {
            TaskStatus::Cancelled
        } else if self.completed {
            TaskStatus::Completed
        } else if self.polling {
            TaskStatus::Polling
        } else if self.queued {
            TaskStatus::Queued
        } else {
            TaskStatus::Waiting
        };

        TaskSnapshot {
            id,
            name,
            status,
            waiting_for: self.waiting_for,
            poll_count: self.poll_count,
            total_poll_time: self.total_poll_time,
            created_at,
            last_scheduled_at: self.last_scheduled_at,
            last_poll_started_at: self.last_poll_started_at,
            last_poll_finished_at: self.last_poll_finished_at,
        }
    }
}
