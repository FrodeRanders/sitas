use std::fmt;
use std::thread;

/// Logical CPU identifier used for shard thread placement.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CpuId(pub usize);

/// CPU placement policy for sharded executor threads.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CpuPlacement {
    /// Do not request CPU affinity.
    Unpinned,
    /// Place shards sequentially across the CPUs currently available to the
    /// process.
    Sequential,
    /// Place each shard on the CPU at the matching index.
    Explicit(Vec<CpuId>),
}

impl CpuPlacement {
    pub(crate) fn cpu_for_shard(
        &self,
        shard_idx: usize,
        available_cpus: &[CpuId],
    ) -> Option<CpuId> {
        match self {
            CpuPlacement::Unpinned => None,
            CpuPlacement::Sequential => {
                if available_cpus.is_empty() {
                    Some(CpuId(shard_idx))
                } else {
                    Some(available_cpus[shard_idx % available_cpus.len()])
                }
            }
            CpuPlacement::Explicit(cpus) => cpus.get(shard_idx).copied(),
        }
    }

    pub(crate) fn validate(&self, shard_count: usize) -> bool {
        match self {
            CpuPlacement::Explicit(cpus) => cpus.len() >= shard_count,
            CpuPlacement::Unpinned | CpuPlacement::Sequential => true,
        }
    }

    pub(crate) fn validate_against_available_cpus(
        &self,
        shard_count: usize,
        available_cpus: &[CpuId],
    ) -> Result<(), String> {
        match self {
            CpuPlacement::Explicit(cpus) if cpus.len() < shard_count => Err(format!(
                "explicit placement provides {} CPUs for {shard_count} shards",
                cpus.len()
            )),
            CpuPlacement::Explicit(cpus) => {
                for cpu in cpus.iter().take(shard_count) {
                    if !available_cpus.contains(cpu) {
                        return Err(format!(
                            "CPU {} is not in the process available CPU set {:?}",
                            cpu.0, available_cpus
                        ));
                    }
                }

                Ok(())
            }
            CpuPlacement::Unpinned | CpuPlacement::Sequential => Ok(()),
        }
    }
}

/// Result of applying CPU placement to one shard thread.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CpuPlacementStatus {
    /// No CPU placement was requested.
    Unpinned,
    /// The shard thread was pinned to the requested CPU.
    Applied(CpuId),
    /// This platform does not provide Linux-style hard CPU affinity.
    Unsupported {
        /// CPU requested by the placement policy, if any.
        requested: Option<CpuId>,
        /// Human-readable reason reported by the runtime.
        reason: String,
    },
    /// The runtime attempted placement, but the OS rejected it.
    Failed {
        /// CPU requested by the placement policy.
        requested: CpuId,
        /// Human-readable OS error.
        error: String,
    },
}

impl CpuPlacementStatus {
    /// Returns the CPU requested by the placement policy, if any.
    pub fn requested_cpu(&self) -> Option<CpuId> {
        match self {
            CpuPlacementStatus::Unpinned => None,
            CpuPlacementStatus::Applied(cpu) => Some(*cpu),
            CpuPlacementStatus::Unsupported { requested, .. } => *requested,
            CpuPlacementStatus::Failed { requested, .. } => Some(*requested),
        }
    }

    /// Returns whether the OS accepted the requested placement.
    pub fn is_applied(&self) -> bool {
        matches!(self, CpuPlacementStatus::Applied(_))
    }
}

impl fmt::Display for CpuPlacementStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuPlacementStatus::Unpinned => write!(f, "unpinned"),
            CpuPlacementStatus::Applied(cpu) => write!(f, "pinned to CPU {}", cpu.0),
            CpuPlacementStatus::Unsupported { requested, reason } => {
                if let Some(cpu) = requested {
                    write!(f, "CPU {} requested but unsupported: {reason}", cpu.0)
                } else {
                    write!(f, "unsupported: {reason}")
                }
            }
            CpuPlacementStatus::Failed { requested, error } => {
                write!(f, "failed to pin to CPU {}: {error}", requested.0)
            }
        }
    }
}

/// Returns the CPU ids available to this process for shard placement.
///
/// On Linux this uses `sched_getaffinity`, so it honors container cpusets and
/// other process affinity restrictions. On other platforms it falls back to
/// `0..std::thread::available_parallelism()`.
pub fn available_cpu_ids() -> Vec<CpuId> {
    platform::available_cpu_ids().unwrap_or_else(|| {
        let count = thread::available_parallelism().map_or(1, usize::from);
        (0..count).map(CpuId).collect()
    })
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn current_thread_cpu_ids() -> Option<Vec<CpuId>> {
    platform::available_cpu_ids()
}

pub(crate) fn apply_to_current_thread(requested: Option<CpuId>) -> CpuPlacementStatus {
    let Some(cpu) = requested else {
        return CpuPlacementStatus::Unpinned;
    };

    platform::apply_to_current_thread(cpu)
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{CpuId, CpuPlacementStatus};
    use std::io;
    use std::mem;
    use std::os::raw::{c_int, c_ulong, c_void};

    const CPU_SETSIZE: usize = 1024;
    const CPU_BITS: usize = usize::BITS as usize;
    const CPU_SET_WORDS: usize = CPU_SETSIZE / CPU_BITS;

    #[repr(C)]
    struct CpuSet {
        bits: [usize; CPU_SET_WORDS],
    }

    impl CpuSet {
        fn empty() -> Self {
            Self {
                bits: [0; CPU_SET_WORDS],
            }
        }

        fn set(&mut self, cpu: CpuId) -> bool {
            let word_idx = cpu.0 / CPU_BITS;
            let bit_idx = cpu.0 % CPU_BITS;

            if word_idx >= self.bits.len() {
                return false;
            }

            self.bits[word_idx] |= 1usize << bit_idx;
            true
        }

        fn contains(&self, cpu_idx: usize) -> bool {
            let word_idx = cpu_idx / CPU_BITS;
            let bit_idx = cpu_idx % CPU_BITS;

            self.bits
                .get(word_idx)
                .is_some_and(|word| (word & (1usize << bit_idx)) != 0)
        }
    }

    unsafe extern "C" {
        fn sched_getaffinity(pid: c_int, cpusetsize: c_ulong, mask: *mut c_void) -> c_int;
        fn sched_setaffinity(pid: c_int, cpusetsize: c_ulong, mask: *const c_void) -> c_int;
    }

    pub(super) fn available_cpu_ids() -> Option<Vec<CpuId>> {
        let mut set = CpuSet::empty();
        let result = unsafe {
            sched_getaffinity(
                0,
                mem::size_of::<CpuSet>() as c_ulong,
                (&mut set as *mut CpuSet).cast::<c_void>(),
            )
        };

        if result != 0 {
            return None;
        }

        let cpus = (0..CPU_SETSIZE)
            .filter(|cpu_idx| set.contains(*cpu_idx))
            .map(CpuId)
            .collect::<Vec<_>>();

        Some(cpus)
    }

    pub(super) fn apply_to_current_thread(cpu: CpuId) -> CpuPlacementStatus {
        let mut set = CpuSet::empty();
        if !set.set(cpu) {
            return CpuPlacementStatus::Failed {
                requested: cpu,
                error: format!("CPU index exceeds supported CPU_SETSIZE {}", CPU_SETSIZE),
            };
        }

        let result = unsafe {
            sched_setaffinity(
                0,
                mem::size_of::<CpuSet>() as c_ulong,
                (&set as *const CpuSet).cast::<c_void>(),
            )
        };

        if result == 0 {
            CpuPlacementStatus::Applied(cpu)
        } else {
            CpuPlacementStatus::Failed {
                requested: cpu,
                error: io::Error::last_os_error().to_string(),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{CpuId, CpuPlacementStatus};

    pub(super) fn available_cpu_ids() -> Option<Vec<CpuId>> {
        None
    }

    pub(super) fn apply_to_current_thread(cpu: CpuId) -> CpuPlacementStatus {
        CpuPlacementStatus::Unsupported {
            requested: Some(cpu),
            reason: String::from("hard CPU affinity is only implemented on Linux"),
        }
    }
}
