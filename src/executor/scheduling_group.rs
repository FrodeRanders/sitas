use std::error::Error;
use std::fmt;

use super::types::{DEFAULT_SCHEDULING_GROUP_SHARES, SchedulingGroupId};

/// Handle naming a scheduling group inside one executor.
///
/// Scheduling groups are executor-local. A group handle should be used only
/// with the spawner that created it.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulingGroup {
    id: SchedulingGroupId,
    name: String,
    shares: u32,
}

impl SchedulingGroup {
    pub(super) fn new(id: SchedulingGroupId, name: String, shares: u32) -> Self {
        Self { id, name, shares }
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
}

impl Default for SchedulingGroup {
    fn default() -> Self {
        Self {
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
