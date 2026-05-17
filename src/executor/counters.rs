#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SchedulerCounters {
    pub(super) total_spawned_tasks: u64,
    pub(super) total_completed_tasks: u64,
    pub(super) total_task_polls: u64,
    pub(super) ready_poll_budget_exhaustions: u64,
    pub(super) total_driver_events: u64,
    #[cfg(unix)]
    pub(super) total_readiness_events: u64,
    #[cfg(unix)]
    pub(super) total_readable_events: u64,
    #[cfg(unix)]
    pub(super) total_writable_events: u64,
    #[cfg(target_os = "linux")]
    pub(super) total_completion_events: u64,
}

impl SchedulerCounters {
    pub(super) fn record_spawned_task(&mut self) {
        self.total_spawned_tasks += 1;
    }

    pub(super) fn record_completed_task(&mut self) {
        self.total_completed_tasks += 1;
    }

    pub(super) fn record_ready_poll_batch(&mut self, polled: usize, exhausted_budget: bool) {
        self.total_task_polls += polled as u64;
        if exhausted_budget {
            self.ready_poll_budget_exhaustions += 1;
        }
    }

    #[cfg(unix)]
    pub(super) fn record_readiness_driver_event(&mut self, readable: bool, writable: bool) {
        self.total_driver_events += 1;
        self.total_readiness_events += 1;
        if readable {
            self.total_readable_events += 1;
        }
        if writable {
            self.total_writable_events += 1;
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn record_completion_driver_event(&mut self) {
        self.total_driver_events += 1;
        self.total_completion_events += 1;
    }
}
