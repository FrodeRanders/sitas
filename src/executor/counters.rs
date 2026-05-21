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
    #[cfg(target_os = "linux")]
    pub(super) total_completion_dispatch_batches: u64,
    #[cfg(target_os = "linux")]
    pub(super) total_dispatched_completions: u64,
    #[cfg(target_os = "linux")]
    pub(super) completion_dispatch_budget_exhaustions: u64,
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

    #[cfg(target_os = "linux")]
    pub(super) fn record_completion_dispatch_batch(
        &mut self,
        dispatched: usize,
        exhausted_budget: bool,
    ) {
        self.total_completion_dispatch_batches += 1;
        self.total_dispatched_completions += dispatched as u64;
        if exhausted_budget {
            self.completion_dispatch_budget_exhaustions += 1;
        }
    }

    #[cfg(all(unix, not(target_os = "linux")))]
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
    pub(super) fn record_driver_event(
        &mut self,
        readiness: bool,
        readable: bool,
        writable: bool,
        completion: bool,
    ) {
        self.total_driver_events += 1;
        if readiness {
            self.total_readiness_events += 1;
            if readable {
                self.total_readable_events += 1;
            }
            if writable {
                self.total_writable_events += 1;
            }
        }
        if completion {
            self.total_completion_events += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_lifecycle_counters_accumulate() {
        let mut counters = SchedulerCounters::default();

        counters.record_spawned_task();
        counters.record_spawned_task();
        counters.record_completed_task();

        assert_eq!(counters.total_spawned_tasks, 2);
        assert_eq!(counters.total_completed_tasks, 1);
    }

    #[test]
    fn ready_poll_batch_counts_polls_and_budget_exhaustions() {
        let mut counters = SchedulerCounters::default();

        counters.record_ready_poll_batch(3, false);
        counters.record_ready_poll_batch(5, true);

        assert_eq!(counters.total_task_polls, 8);
        assert_eq!(counters.ready_poll_budget_exhaustions, 1);
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn readiness_events_count_driver_and_direction_progress() {
        let mut counters = SchedulerCounters::default();

        counters.record_readiness_driver_event(true, false);
        counters.record_readiness_driver_event(false, true);
        counters.record_readiness_driver_event(true, true);
        counters.record_readiness_driver_event(false, false);

        assert_eq!(counters.total_driver_events, 4);
        assert_eq!(counters.total_readiness_events, 4);
        assert_eq!(counters.total_readable_events, 2);
        assert_eq!(counters.total_writable_events, 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn combined_driver_event_counts_one_wait_with_split_progress() {
        let mut counters = SchedulerCounters::default();

        counters.record_driver_event(true, true, false, true);

        assert_eq!(counters.total_driver_events, 1);
        assert_eq!(counters.total_readiness_events, 1);
        assert_eq!(counters.total_readable_events, 1);
        assert_eq!(counters.total_writable_events, 0);
        assert_eq!(counters.total_completion_events, 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn completion_dispatch_batches_count_completions_and_budget_exhaustions() {
        let mut counters = SchedulerCounters::default();

        counters.record_completion_dispatch_batch(3, false);
        counters.record_completion_dispatch_batch(5, true);

        assert_eq!(counters.total_completion_dispatch_batches, 2);
        assert_eq!(counters.total_dispatched_completions, 8);
        assert_eq!(counters.completion_dispatch_budget_exhaustions, 1);
    }
}
