use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::types::{DEFAULT_SCHEDULING_GROUP_SHARES, SchedulingGroupId};

static NEXT_EXECUTOR_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExecutorId(usize);

impl ExecutorId {
    pub(super) fn allocate() -> Self {
        Self(NEXT_EXECUTOR_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Handle naming a scheduling group inside one executor.
///
/// Scheduling groups are executor-local. A group handle should be used only
/// with the executor that created it, except for the default group handle,
/// which names the built-in default group in any executor.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulingGroup {
    executor_id: Option<ExecutorId>,
    id: SchedulingGroupId,
    name: String,
    shares: u32,
}

impl SchedulingGroup {
    pub(super) fn new(
        executor_id: ExecutorId,
        id: SchedulingGroupId,
        name: String,
        shares: u32,
    ) -> Self {
        Self {
            executor_id: Some(executor_id),
            id,
            name,
            shares,
        }
    }

    /// Returns this group's executor-local identifier.
    pub fn id(&self) -> SchedulingGroupId {
        self.id
    }

    /// Returns this group's human-readable name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns this group's relative scheduling weight.
    pub fn shares(&self) -> u32 {
        self.shares
    }

    pub(super) fn belongs_to(&self, executor_id: ExecutorId) -> bool {
        self.executor_id.is_none_or(|owner| owner == executor_id)
    }
}

impl Default for SchedulingGroup {
    fn default() -> Self {
        Self {
            executor_id: None,
            id: super::types::DEFAULT_SCHEDULING_GROUP_ID,
            name: String::from("default"),
            shares: DEFAULT_SCHEDULING_GROUP_SHARES,
        }
    }
}

/// Error returned when a scheduling group cannot be created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulingGroupError {
    /// Scheduling group shares must be greater than zero.
    ZeroShares,
}

impl fmt::Display for SchedulingGroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchedulingGroupError::ZeroShares => {
                write!(f, "scheduling group shares must be greater than zero")
            }
        }
    }
}

impl Error for SchedulingGroupError {}
