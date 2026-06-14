//! Observability metrics for the sharded runtime.
//!
//! Provides a basic metrics collector that accumulates counters for runtime
//! events and exposes them through owned snapshots. This is intentionally
//! minimal: a simple counter/gauge/histogram interface without external
//! dependency on OpenTelemetry or Prometheus crates.
//!
//! The [`RuntimeMetrics`] collector can be installed into any layer of the
//! runtime and provides thread-safe accumulation via atomics.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// A thread-safe metrics collector for the sharded runtime.
#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    // Task lifecycle counters
    tasks_spawned: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_cancelled: AtomicU64,
    tasks_panicked: AtomicU64,

    // I/O counters
    reads_total: AtomicU64,
    writes_total: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    io_errors: AtomicU64,

    // Network counters
    connections_accepted: AtomicU64,
    connections_closed: AtomicU64,
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,

    // Shard counters
    commands_sent: AtomicU64,
    commands_received: AtomicU64,
    replies_sent: AtomicU64,
    replies_received: AtomicU64,

    // Executor counters
    polls_total: AtomicU64,
    wakeups_total: AtomicU64,
    idle_cycles: AtomicU64,

    // io_uring counters
    uring_ops_submitted: AtomicU64,
    uring_ops_completed: AtomicU64,
    uring_ops_cancelled: AtomicU64,

    // Memory (best-effort via usize atomics)
    active_tasks: AtomicUsize,
    peak_active_tasks: AtomicUsize,
}

impl RuntimeMetrics {
    /// Creates a new metrics collector with all counters at zero.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                tasks_spawned: AtomicU64::new(0),
                tasks_completed: AtomicU64::new(0),
                tasks_cancelled: AtomicU64::new(0),
                tasks_panicked: AtomicU64::new(0),
                reads_total: AtomicU64::new(0),
                writes_total: AtomicU64::new(0),
                read_bytes: AtomicU64::new(0),
                write_bytes: AtomicU64::new(0),
                io_errors: AtomicU64::new(0),
                connections_accepted: AtomicU64::new(0),
                connections_closed: AtomicU64::new(0),
                bytes_received: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
                commands_sent: AtomicU64::new(0),
                commands_received: AtomicU64::new(0),
                replies_sent: AtomicU64::new(0),
                replies_received: AtomicU64::new(0),
                polls_total: AtomicU64::new(0),
                wakeups_total: AtomicU64::new(0),
                idle_cycles: AtomicU64::new(0),
                uring_ops_submitted: AtomicU64::new(0),
                uring_ops_completed: AtomicU64::new(0),
                uring_ops_cancelled: AtomicU64::new(0),
                active_tasks: AtomicUsize::new(0),
                peak_active_tasks: AtomicUsize::new(0),
            }),
        }
    }

    // Task lifecycle

    /// Records a spawned task.
    pub fn task_spawned(&self) {
        self.inner.tasks_spawned.fetch_add(1, Ordering::Relaxed);
        let active = self.inner.active_tasks.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner
            .peak_active_tasks
            .fetch_max(active, Ordering::Relaxed);
    }

    /// Records a completed task.
    pub fn task_completed(&self) {
        self.inner.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.inner.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a cancelled task.
    pub fn task_cancelled(&self) {
        self.inner.tasks_cancelled.fetch_add(1, Ordering::Relaxed);
        self.inner.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a panicked task.
    pub fn task_panicked(&self) {
        self.inner.tasks_panicked.fetch_add(1, Ordering::Relaxed);
        self.inner.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    // I/O

    /// Records a read operation with its byte count.
    pub fn record_read(&self, bytes: u64) {
        self.inner.reads_total.fetch_add(1, Ordering::Relaxed);
        self.inner.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a write operation with its byte count.
    pub fn record_write(&self, bytes: u64) {
        self.inner.writes_total.fetch_add(1, Ordering::Relaxed);
        self.inner.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records an I/O error.
    pub fn record_io_error(&self) {
        self.inner.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    // Network

    /// Records an accepted connection.
    pub fn connection_accepted(&self) {
        self.inner
            .connections_accepted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a closed connection.
    pub fn connection_closed(&self) {
        self.inner
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records received bytes.
    pub fn record_bytes_received(&self, bytes: u64) {
        self.inner
            .bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records sent bytes.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.inner.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    // Shard

    /// Records a sent command.
    pub fn command_sent(&self) {
        self.inner.commands_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a received command.
    pub fn command_received(&self) {
        self.inner.commands_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a sent reply.
    pub fn reply_sent(&self) {
        self.inner.replies_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a received reply.
    pub fn reply_received(&self) {
        self.inner.replies_received.fetch_add(1, Ordering::Relaxed);
    }

    // Executor

    /// Records a task poll.
    pub fn poll(&self) {
        self.inner.polls_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a task wakeup.
    pub fn wakeup(&self) {
        self.inner.wakeups_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an idle cycle (no tasks ready).
    pub fn idle_cycle(&self) {
        self.inner.idle_cycles.fetch_add(1, Ordering::Relaxed);
    }

    // io_uring

    /// Records a submitted io_uring operation.
    pub fn uring_op_submitted(&self) {
        self.inner
            .uring_ops_submitted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a completed io_uring operation.
    pub fn uring_op_completed(&self) {
        self.inner
            .uring_ops_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cancelled io_uring operation.
    pub fn uring_op_cancelled(&self) {
        self.inner
            .uring_ops_cancelled
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns an owned snapshot of the current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let inner = &self.inner;
        MetricsSnapshot {
            tasks_spawned: inner.tasks_spawned.load(Ordering::Acquire),
            tasks_completed: inner.tasks_completed.load(Ordering::Acquire),
            tasks_cancelled: inner.tasks_cancelled.load(Ordering::Acquire),
            tasks_panicked: inner.tasks_panicked.load(Ordering::Acquire),
            reads_total: inner.reads_total.load(Ordering::Acquire),
            writes_total: inner.writes_total.load(Ordering::Acquire),
            read_bytes: inner.read_bytes.load(Ordering::Acquire),
            write_bytes: inner.write_bytes.load(Ordering::Acquire),
            io_errors: inner.io_errors.load(Ordering::Acquire),
            connections_accepted: inner.connections_accepted.load(Ordering::Acquire),
            connections_closed: inner.connections_closed.load(Ordering::Acquire),
            bytes_received: inner.bytes_received.load(Ordering::Acquire),
            bytes_sent: inner.bytes_sent.load(Ordering::Acquire),
            commands_sent: inner.commands_sent.load(Ordering::Acquire),
            commands_received: inner.commands_received.load(Ordering::Acquire),
            replies_sent: inner.replies_sent.load(Ordering::Acquire),
            replies_received: inner.replies_received.load(Ordering::Acquire),
            polls_total: inner.polls_total.load(Ordering::Acquire),
            wakeups_total: inner.wakeups_total.load(Ordering::Acquire),
            idle_cycles: inner.idle_cycles.load(Ordering::Acquire),
            uring_ops_submitted: inner.uring_ops_submitted.load(Ordering::Acquire),
            uring_ops_completed: inner.uring_ops_completed.load(Ordering::Acquire),
            uring_ops_cancelled: inner.uring_ops_cancelled.load(Ordering::Acquire),
            active_tasks: inner.active_tasks.load(Ordering::Acquire),
            peak_active_tasks: inner.peak_active_tasks.load(Ordering::Acquire),
        }
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        let inner = &self.inner;
        inner.tasks_spawned.store(0, Ordering::Release);
        inner.tasks_completed.store(0, Ordering::Release);
        inner.tasks_cancelled.store(0, Ordering::Release);
        inner.tasks_panicked.store(0, Ordering::Release);
        inner.reads_total.store(0, Ordering::Release);
        inner.writes_total.store(0, Ordering::Release);
        inner.read_bytes.store(0, Ordering::Release);
        inner.write_bytes.store(0, Ordering::Release);
        inner.io_errors.store(0, Ordering::Release);
        inner.connections_accepted.store(0, Ordering::Release);
        inner.connections_closed.store(0, Ordering::Release);
        inner.bytes_received.store(0, Ordering::Release);
        inner.bytes_sent.store(0, Ordering::Release);
        inner.commands_sent.store(0, Ordering::Release);
        inner.commands_received.store(0, Ordering::Release);
        inner.replies_sent.store(0, Ordering::Release);
        inner.replies_received.store(0, Ordering::Release);
        inner.polls_total.store(0, Ordering::Release);
        inner.wakeups_total.store(0, Ordering::Release);
        inner.idle_cycles.store(0, Ordering::Release);
        inner.uring_ops_submitted.store(0, Ordering::Release);
        inner.uring_ops_completed.store(0, Ordering::Release);
        inner.uring_ops_cancelled.store(0, Ordering::Release);
        inner.active_tasks.store(0, Ordering::Release);
        inner.peak_active_tasks.store(0, Ordering::Release);
    }
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// An owned point-in-time snapshot of runtime metrics.
#[must_use]
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    /// Total tasks spawned.
    pub tasks_spawned: u64,
    /// Total tasks that completed successfully.
    pub tasks_completed: u64,
    /// Total tasks that were cancelled.
    pub tasks_cancelled: u64,
    /// Total tasks that panicked.
    pub tasks_panicked: u64,

    /// Total read operations.
    pub reads_total: u64,
    /// Total write operations.
    pub writes_total: u64,
    /// Total bytes read.
    pub read_bytes: u64,
    /// Total bytes written.
    pub write_bytes: u64,
    /// Total I/O errors.
    pub io_errors: u64,

    /// Total connections accepted.
    pub connections_accepted: u64,
    /// Total connections closed.
    pub connections_closed: u64,
    /// Total bytes received (network).
    pub bytes_received: u64,
    /// Total bytes sent (network).
    pub bytes_sent: u64,

    /// Total commands sent to shards.
    pub commands_sent: u64,
    /// Total commands received by shards.
    pub commands_received: u64,
    /// Total replies sent by shards.
    pub replies_sent: u64,
    /// Total replies received by clients.
    pub replies_received: u64,

    /// Total task polls.
    pub polls_total: u64,
    /// Total task wakeups.
    pub wakeups_total: u64,
    /// Total idle cycles (no ready tasks).
    pub idle_cycles: u64,

    /// Total io_uring ops submitted.
    pub uring_ops_submitted: u64,
    /// Total io_uring ops completed.
    pub uring_ops_completed: u64,
    /// Total io_uring ops cancelled.
    pub uring_ops_cancelled: u64,

    /// Current active tasks.
    pub active_tasks: usize,
    /// Peak active tasks observed.
    pub peak_active_tasks: usize,
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tasks: {}/{}/{} (spawned/completed/active), ",
            self.tasks_spawned, self.tasks_completed, self.active_tasks
        )?;
        write!(
            f,
            "io: {}r/{}w ({}B/{}B), ",
            self.reads_total, self.writes_total, self.read_bytes, self.write_bytes
        )?;
        write!(
            f,
            "net: {} conns ({}B rx/{}B tx), ",
            self.connections_accepted, self.bytes_received, self.bytes_sent
        )?;
        write!(
            f,
            "cmds: {}/{}, replies: {}/{}, ",
            self.commands_sent, self.commands_received, self.replies_sent, self.replies_received
        )?;
        write!(
            f,
            "executor: {} polls, {} wakeups, {} idle, ",
            self.polls_total, self.wakeups_total, self.idle_cycles
        )?;
        write!(
            f,
            "uring: {}/{}/{} (submitted/completed/cancelled)",
            self.uring_ops_submitted, self.uring_ops_completed, self.uring_ops_cancelled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_task_counters_work() {
        let m = RuntimeMetrics::new();

        m.task_spawned();
        m.task_spawned();
        m.task_completed();
        m.task_cancelled();

        let s = m.snapshot();
        assert_eq!(s.tasks_spawned, 2);
        assert_eq!(s.tasks_completed, 1);
        assert_eq!(s.tasks_cancelled, 1);
        assert_eq!(s.active_tasks, 0);
        assert_eq!(s.peak_active_tasks, 2);
    }

    #[test]
    fn metrics_io_counters_work() {
        let m = RuntimeMetrics::new();

        m.record_read(1024);
        m.record_read(512);
        m.record_write(256);
        m.record_io_error();

        let s = m.snapshot();
        assert_eq!(s.reads_total, 2);
        assert_eq!(s.read_bytes, 1536);
        assert_eq!(s.writes_total, 1);
        assert_eq!(s.write_bytes, 256);
        assert_eq!(s.io_errors, 1);
    }

    #[test]
    fn metrics_network_counters_work() {
        let m = RuntimeMetrics::new();

        m.connection_accepted();
        m.connection_accepted();
        m.connection_closed();
        m.record_bytes_received(4096);
        m.record_bytes_sent(2048);

        let s = m.snapshot();
        assert_eq!(s.connections_accepted, 2);
        assert_eq!(s.connections_closed, 1);
        assert_eq!(s.bytes_received, 4096);
        assert_eq!(s.bytes_sent, 2048);
    }

    #[test]
    fn metrics_reset_zeros_all() {
        let m = RuntimeMetrics::new();

        m.task_spawned();
        m.record_read(100);
        m.poll();

        m.reset();

        let s = m.snapshot();
        assert_eq!(s.tasks_spawned, 0);
        assert_eq!(s.reads_total, 0);
        assert_eq!(s.polls_total, 0);
    }

    #[test]
    fn metrics_clone_shares_counters() {
        let m1 = RuntimeMetrics::new();
        let m2 = m1.clone();

        m1.task_spawned();
        m2.record_read(42);

        let s = m1.snapshot();
        assert_eq!(s.tasks_spawned, 1);
        assert_eq!(s.reads_total, 1);
        assert_eq!(s.read_bytes, 42);
    }
}
