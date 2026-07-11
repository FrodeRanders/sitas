//! Polyfill for `std::time::Instant` — not available in `core`.
//! Uses a monotonically increasing counter (atomically). On a real backend
//! (e.g. the CharlotteOS kernel), `Instant` would be provided by the kernel.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(u64);

impl Instant {
    pub fn now() -> Self {
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    pub fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
        if self.0 >= earlier.0 {
            Some(Duration::from_nanos(self.0 - earlier.0))
        } else {
            None
        }
    }

    pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    pub fn checked_add(&self, d: Duration) -> Option<Self> {
        let ns = d.as_nanos() as u64;
        self.0.checked_add(ns).map(Instant)
    }

    pub fn elapsed(&self) -> Duration {
        Self::now().saturating_duration_since(*self)
    }
}
