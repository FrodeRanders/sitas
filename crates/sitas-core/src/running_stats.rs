//! Running statistics for streaming samples.
//!
//! Computes count, mean, variance, standard deviation, coefficient of
//! variation, minimum, maximum, and total from a stream of `f64` samples
//! without storing the individual values.
//!
//! Uses Welford's algorithm for numerically stable one-pass mean and
//! variance computation.

use core::fmt;
use core::time::{Duration, Instant};

/// Online statistics accumulator for `f64` samples.
///
/// # Example
///
/// ```
/// use sitas::RunningStatistics;
///
/// let mut stats = RunningStatistics::new();
/// stats.add_sample(2.0);
/// stats.add_sample(4.0);
/// stats.add_sample(4.0);
/// stats.add_sample(4.0);
/// stats.add_sample(5.0);
/// stats.add_sample(5.0);
/// stats.add_sample(7.0);
/// stats.add_sample(9.0);
///
/// assert_eq!(stats.count(), 8);
/// assert!((stats.mean().unwrap() - 5.0).abs() < 1e-10);
/// assert!((stats.std_dev().unwrap() - 2.138).abs() < 0.01);
/// assert!((stats.cv().unwrap() - 42.76).abs() < 0.1);
/// assert_eq!(stats.min().unwrap(), 2.0);
/// assert_eq!(stats.max().unwrap(), 9.0);
/// ```
#[derive(Debug, Clone)]
pub struct RunningStatistics {
    count: u64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
    total: u64,
}

impl Default for RunningStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl RunningStatistics {
    /// Creates an accumulator with no samples.
    pub const fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            min: 0.0,
            max: 0.0,
            total: 0,
        }
    }

    /// Discards all accumulated samples.
    pub fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.min = 0.0;
        self.max = 0.0;
        self.total = 0;
    }

    /// Feeds one sample into the accumulator.
    ///
    /// Welford's algorithm is used so mean and variance are stable across
    /// a large number of samples.
    pub fn add_sample(&mut self, x: f64) {
        if self.count == 0 {
            self.min = x;
            self.max = x;
        } else {
            self.min = self.min.min(x);
            self.max = self.max.max(x);
        }

        self.total += x.round() as u64;

        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    /// Records the duration between `start` and `end` as a sample in
    /// seconds.
    ///
    /// Convenience for:
    /// `stats.add_sample(duration.as_secs_f64())`.
    pub fn add_sample_duration(&mut self, duration: Duration) {
        self.add_sample(duration.as_secs_f64());
    }

    /// Records the elapsed time between `start` and `end` as a sample in
    /// seconds.
    ///
    /// Convenience for:
    /// `stats.add_sample_duration(end.duration_since(start))`.
    pub fn add_sample_interval(&mut self, start: Instant, end: Instant) {
        self.add_sample_duration(end.duration_since(start));
    }

    /// Number of samples observed so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Smallest observed sample.
    ///
    /// Returns `None` when no samples have been added.
    pub fn min(&self) -> Option<f64> {
        (self.count > 0).then_some(self.min)
    }

    /// Smallest observed sample as a [`Duration`].
    ///
    /// Returns `None` when no samples have been added.
    pub fn min_duration(&self) -> Option<Duration> {
        self.min().map(Duration::from_secs_f64)
    }

    /// Largest observed sample.
    ///
    /// Returns `None` when no samples have been added.
    pub fn max(&self) -> Option<f64> {
        (self.count > 0).then_some(self.max)
    }

    /// Largest observed sample as a [`Duration`].
    ///
    /// Returns `None` when no samples have been added.
    pub fn max_duration(&self) -> Option<Duration> {
        self.max().map(Duration::from_secs_f64)
    }

    /// Sample mean.
    ///
    /// Returns `None` when no samples have been added.
    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then_some(self.mean)
    }

    /// Sample mean as a [`Duration`].
    ///
    /// Returns `None` when no samples have been added.
    pub fn mean_duration(&self) -> Option<Duration> {
        self.mean().map(Duration::from_secs_f64)
    }

    /// Rounded integral total of all samples.
    ///
    /// Useful when the samples represent counts or quantities that
    /// naturally accumulate.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Sample variance (n − 1 denominator).
    ///
    /// Returns `None` when fewer than two samples have been added.
    pub fn variance(&self) -> Option<f64> {
        (self.count > 1).then(|| self.m2 / (self.count - 1) as f64)
    }

    /// Sample standard deviation.
    ///
    /// Returns `None` when fewer than two samples have been added.
    pub fn std_dev(&self) -> Option<f64> {
        self.variance().map(|v| v.sqrt())
    }

    /// Standard deviation as a [`Duration`].
    ///
    /// Returns `None` when fewer than two samples have been added.
    pub fn std_dev_duration(&self) -> Option<Duration> {
        self.std_dev().map(Duration::from_secs_f64)
    }

    /// Coefficient of variation (CV) as a percentage.
    ///
    /// Defined when the mean is non-zero and at least two samples have
    /// been added. CV is unit-free, making it useful for comparing
    /// dispersion across metrics with different scales.
    ///
    /// Returns `None` when the mean is zero or fewer than two samples
    /// have been added.
    pub fn cv(&self) -> Option<f64> {
        if self.count > 1 && self.mean != 0.0 {
            Some(100.0 * self.std_dev().expect("std_dev defined") / self.mean)
        } else {
            None
        }
    }

    /// Merges the statistics from `other` into this accumulator.
    ///
    /// Uses Chan et al.'s parallel algorithm for combining partial
    /// Welford accumulators. After merging, this accumulator describes
    /// the combined dataset as if all samples had been fed directly.
    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }

        if self.count == 0 {
            *self = other.clone();
            return;
        }

        let combined_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let count_f64 = |n: u64| n as f64;

        self.m2 += other.m2
            + delta * delta * count_f64(self.count) * count_f64(other.count)
                / count_f64(combined_count);

        self.mean = (count_f64(self.count) * self.mean + count_f64(other.count) * other.mean)
            / count_f64(combined_count);

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count = combined_count;
    }
}

impl fmt::Display for RunningStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (
            self.mean(),
            self.std_dev(),
            self.cv(),
            self.min(),
            self.max(),
        ) {
            (Some(mean), Some(std_dev), Some(cv), Some(min), Some(max)) => {
                write!(
                    f,
                    "n={} mean={mean:.3} σ={std_dev:.3} cv={cv:.1}% min={min:.3} max={max:.3} total={}",
                    self.count, self.total
                )
            }
            (Some(mean), _, _, Some(min), Some(max)) => {
                write!(
                    f,
                    "n={} mean={mean:.3} min={min:.3} max={max:.3} total={}",
                    self.count, self.total
                )
            }
            _ => {
                write!(f, "n=0")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_has_zero_count() {
        let stats = RunningStatistics::new();
        assert_eq!(stats.count(), 0);
        assert_eq!(stats.min(), None);
        assert_eq!(stats.max(), None);
        assert_eq!(stats.mean(), None);
        assert_eq!(stats.variance(), None);
        assert_eq!(stats.std_dev(), None);
        assert_eq!(stats.cv(), None);
        assert_eq!(stats.total(), 0);
    }

    #[test]
    fn single_sample_sets_min_max_mean() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(7.0);

        assert_eq!(stats.count(), 1);
        assert_eq!(stats.min(), Some(7.0));
        assert_eq!(stats.max(), Some(7.0));
        assert_eq!(stats.mean(), Some(7.0));
        assert_eq!(stats.variance(), None);
        assert_eq!(stats.std_dev(), None);
        assert_eq!(stats.cv(), None);
    }

    #[test]
    fn two_samples_variance_defined() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(3.0);
        stats.add_sample(5.0);

        assert_eq!(stats.count(), 2);
        assert_eq!(stats.mean(), Some(4.0));
        assert_eq!(stats.variance(), Some(2.0));
        assert_eq!(stats.std_dev(), Some(2.0_f64.sqrt()));
        assert_eq!(stats.cv(), Some(100.0 * 2.0_f64.sqrt() / 4.0));
    }

    #[test]
    fn cv_undefined_when_mean_is_zero() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(-1.0);
        stats.add_sample(1.0);
        assert_eq!(stats.mean(), Some(0.0));
        assert_eq!(stats.cv(), None);
    }

    #[test]
    fn known_dataset() {
        let mut stats = RunningStatistics::new();
        for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            stats.add_sample(x);
        }

        assert_eq!(stats.count(), 8);
        assert!((stats.mean().unwrap() - 5.0).abs() < 1e-10);
        assert!((stats.std_dev().unwrap() - 2.138).abs() < 0.01);
        assert!((stats.cv().unwrap() - 42.76).abs() < 0.1);
        assert_eq!(stats.min().unwrap(), 2.0);
        assert_eq!(stats.max().unwrap(), 9.0);
    }

    #[test]
    fn large_values_stable() {
        let mut stats = RunningStatistics::new();
        for x in [1e12, 1e12 + 1.0, 1e12 + 2.0] {
            stats.add_sample(x);
        }

        assert!((stats.mean().unwrap() - (1e12 + 1.0)).abs() < 1e-6);
        assert!((stats.variance().unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn reset_clears_accumulator() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(1.0);
        stats.add_sample(2.0);
        stats.add_sample(3.0);
        stats.reset();

        assert_eq!(stats.count(), 0);
        assert_eq!(stats.mean(), None);
        assert_eq!(stats.min(), None);
    }

    #[test]
    fn merge_combines_two_accumulators() {
        let mut a = RunningStatistics::new();
        a.add_sample(1.0);
        a.add_sample(2.0);
        a.add_sample(3.0);

        let mut b = RunningStatistics::new();
        b.add_sample(4.0);
        b.add_sample(5.0);

        a.merge(&b);

        assert_eq!(a.count(), 5);
        assert!((a.mean().unwrap() - 3.0).abs() < 1e-10);
        assert_eq!(a.min().unwrap(), 1.0);
        assert_eq!(a.max().unwrap(), 5.0);
        assert!((a.variance().unwrap() - 2.5).abs() < 1e-10);
    }

    #[test]
    fn merge_with_empty_is_noop() {
        let mut a = RunningStatistics::new();
        a.add_sample(1.0);
        a.add_sample(2.0);

        let b = RunningStatistics::new();
        a.merge(&b);

        assert_eq!(a.count(), 2);
        assert_eq!(a.mean(), Some(1.5));
    }

    #[test]
    fn add_sample_interval_records_duration() {
        let now = Instant::now();
        let later = now + Duration::from_millis(150);
        let mut stats = RunningStatistics::new();
        stats.add_sample_interval(now, later);

        assert_eq!(stats.count(), 1);
        assert!((stats.min().unwrap() - 0.15).abs() < 0.01);
    }

    #[test]
    fn duration_accessors_convert_correctly() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(0.25);
        stats.add_sample(0.75);

        let mean_dur = stats.mean_duration().unwrap();
        assert!((mean_dur.as_secs_f64() - 0.5).abs() < 1e-10);

        let min_dur = stats.min_duration().unwrap();
        assert!((min_dur.as_secs_f64() - 0.25).abs() < 1e-10);

        let max_dur = stats.max_duration().unwrap();
        assert!((max_dur.as_secs_f64() - 0.75).abs() < 1e-10);
    }

    #[test]
    fn display_formatting() {
        let mut stats = RunningStatistics::new();
        stats.add_sample(2.0);
        stats.add_sample(4.0);
        stats.add_sample(4.0);
        stats.add_sample(5.0);

        let display = stats.to_string();
        assert!(display.contains("n=4"));
        assert!(display.contains("mean=3.750"));
        assert!(display.contains("min=2.000"));
        assert!(display.contains("max=5.000"));
    }
}
