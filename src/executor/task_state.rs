//! Task lifecycle state machine.
//!
//! [`TaskState`] tracks the current phase of a spawned task: waiting,
//! queued, polling, or completed (including cancelled and panicked).
//! Cancellation is cooperative: a polling task finishes its current poll
//! before the cancellation takes effect. The state machine also records
//! cumulative poll count, accumulated poll time, and key timestamps for
//! observability snapshots.

use std::time::{Duration, Instant};

use super::{
    BoxFuture, PanicHandler, SchedulingGroupId, TaskId, TaskSnapshot, TaskStatus, TaskWait,
};

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
        scheduling_group_id: SchedulingGroupId,
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
            scheduling_group_id,
            scheduling_group_name: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::DEFAULT_SCHEDULING_GROUP_ID;

    fn boxed_future() -> BoxFuture {
        Box::pin(async {})
    }

    fn task_snapshot(state: &TaskState) -> TaskSnapshot {
        state.snapshot(
            TaskId(7),
            Some("task".to_string()),
            DEFAULT_SCHEDULING_GROUP_ID,
            Instant::now(),
        )
    }

    #[test]
    fn new_task_state_starts_waiting_with_future() {
        let state = TaskState::new(boxed_future(), None);
        let snapshot = task_snapshot(&state);

        assert_eq!(snapshot.id, TaskId(7));
        assert_eq!(snapshot.name.as_deref(), Some("task"));
        assert_eq!(snapshot.scheduling_group_id, DEFAULT_SCHEDULING_GROUP_ID);
        assert_eq!(snapshot.scheduling_group_name, None);
        assert_eq!(snapshot.status, TaskStatus::Waiting);
        assert_eq!(snapshot.waiting_for, None);
        assert_eq!(snapshot.poll_count, 0);
    }

    #[test]
    fn queued_state_records_schedule_time_once_until_polled() {
        let mut state = TaskState::new(boxed_future(), None);
        let scheduled_at = Instant::now();

        assert!(state.mark_queued(scheduled_at));
        assert!(!state.mark_queued(scheduled_at));

        let snapshot = task_snapshot(&state);
        assert_eq!(snapshot.status, TaskStatus::Queued);
        assert_eq!(snapshot.last_scheduled_at, Some(scheduled_at));
    }

    #[test]
    fn polling_state_tracks_wait_interest_and_pending_snapshot() {
        let mut state = TaskState::new(boxed_future(), None);
        let poll_started_at = Instant::now();
        let future = state
            .take_future_for_poll(poll_started_at)
            .expect("future should be available");
        let deadline = poll_started_at + Duration::from_millis(10);

        state.set_waiting_for(TaskWait::Timer { deadline });
        let snapshot = task_snapshot(&state);
        assert_eq!(snapshot.status, TaskStatus::Polling);
        assert_eq!(snapshot.waiting_for, Some(TaskWait::Timer { deadline }));
        assert_eq!(snapshot.poll_count, 1);
        assert_eq!(snapshot.last_poll_started_at, Some(poll_started_at));

        let poll_finished_at = poll_started_at + Duration::from_millis(1);
        assert!(!state.finish_pending(
            future,
            poll_finished_at.duration_since(poll_started_at),
            poll_finished_at
        ));

        let snapshot = task_snapshot(&state);
        assert_eq!(snapshot.status, TaskStatus::Waiting);
        assert_eq!(snapshot.waiting_for, Some(TaskWait::Timer { deadline }));
        assert_eq!(snapshot.last_poll_finished_at, Some(poll_finished_at));
    }

    #[test]
    fn pending_without_wait_interest_records_unknown_wait() {
        let mut state = TaskState::new(boxed_future(), None);
        let poll_started_at = Instant::now();
        let future = state
            .take_future_for_poll(poll_started_at)
            .expect("future should be available");
        let poll_finished_at = poll_started_at + Duration::from_millis(1);

        assert!(!state.finish_pending(
            future,
            poll_finished_at.duration_since(poll_started_at),
            poll_finished_at
        ));

        assert_eq!(task_snapshot(&state).waiting_for, Some(TaskWait::Unknown));
    }

    #[test]
    fn cancellation_while_polling_finishes_after_pending_result() {
        let mut state = TaskState::new(boxed_future(), None);
        let poll_started_at = Instant::now();
        let future = state
            .take_future_for_poll(poll_started_at)
            .expect("future should be available");

        assert_eq!(state.cancel(), Some(false));

        let poll_finished_at = poll_started_at + Duration::from_millis(1);
        assert!(state.finish_pending(
            future,
            poll_finished_at.duration_since(poll_started_at),
            poll_finished_at
        ));

        let snapshot = task_snapshot(&state);
        assert_eq!(snapshot.status, TaskStatus::Cancelled);
        assert_eq!(snapshot.waiting_for, None);
    }

    #[test]
    fn cancellation_before_polling_drops_future_immediately() {
        let mut state = TaskState::new(boxed_future(), None);

        assert_eq!(state.cancel(), Some(true));
        assert_eq!(state.cancel(), None);
        assert!(state.take_future_for_poll(Instant::now()).is_none());
        assert_eq!(task_snapshot(&state).status, TaskStatus::Cancelled);
    }

    #[test]
    fn ready_and_panicked_states_complete_task() {
        let mut ready = TaskState::new(boxed_future(), None);
        let started = Instant::now();
        let _future = ready
            .take_future_for_poll(started)
            .expect("future should be available");
        let finished = started + Duration::from_millis(1);
        ready.finish_ready(finished.duration_since(started), finished);
        assert_eq!(task_snapshot(&ready).status, TaskStatus::Completed);

        let mut panicked = TaskState::new(boxed_future(), Some(Box::new(|_| {})));
        let _future = panicked
            .take_future_for_poll(started)
            .expect("future should be available");
        assert!(
            panicked
                .finish_panicked(finished.duration_since(started), finished)
                .is_some()
        );
        assert_eq!(task_snapshot(&panicked).status, TaskStatus::Completed);
        assert!(
            panicked
                .finish_panicked(finished.duration_since(started), finished)
                .is_none()
        );
    }
}
