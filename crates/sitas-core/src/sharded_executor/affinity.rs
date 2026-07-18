//! CPU placement for shard executor threads.
//!
//! [`CpuPlacement`] controls hard CPU affinity via `sched_setaffinity` on
//! Linux. Non-Linux platforms report unsupported placement honestly. Callers
//! may opt into fail-fast required placement through
//! [`ShardedExecutorConfig`].

use std::fmt;
use std::thread;

/// Logical CPU identifier used for shard thread placement.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CpuId(pub usize);

/// Linux NUMA node identifier observed for a CPU.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumaNodeId(pub usize);

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

/// Memory placement policy for future shard-thread allocations.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryPlacement {
    /// Do not request a memory placement policy.
    Default,
    /// Bind future allocations to the NUMA node local to the shard's pinned
    /// CPU. This requires CPU placement to have reported a NUMA node.
    LocalToCpu,
    /// Bind future allocations to a specific NUMA node.
    Bind(NumaNodeId),
    /// Prefer a specific NUMA node while allowing kernel fallback.
    Preferred(NumaNodeId),
    /// Interleave future allocations across the provided NUMA nodes.
    Interleave(Vec<NumaNodeId>),
}

impl MemoryPlacement {
    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            MemoryPlacement::Interleave(nodes) if nodes.is_empty() => Err(String::from(
                "interleave placement requires at least one NUMA node",
            )),
            MemoryPlacement::Default
            | MemoryPlacement::LocalToCpu
            | MemoryPlacement::Bind(_)
            | MemoryPlacement::Preferred(_)
            | MemoryPlacement::Interleave(_) => Ok(()),
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
    Applied {
        /// CPU accepted by the OS affinity call.
        cpu: CpuId,
        /// NUMA node observed for `cpu`, when the platform exposes it.
        numa_node: Option<NumaNodeId>,
    },
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
            CpuPlacementStatus::Applied { cpu, .. } => Some(*cpu),
            CpuPlacementStatus::Unsupported { requested, .. } => *requested,
            CpuPlacementStatus::Failed { requested, .. } => Some(*requested),
        }
    }

    /// Returns whether the OS accepted the requested placement.
    pub fn is_applied(&self) -> bool {
        matches!(self, CpuPlacementStatus::Applied { .. })
    }
}

/// Result of applying memory placement to one shard thread.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryPlacementStatus {
    /// No memory placement was requested.
    Default,
    /// The OS accepted the requested memory placement.
    Applied {
        /// Resolved policy applied to the shard thread.
        policy: MemoryPlacement,
    },
    /// This platform does not provide Linux NUMA memory policy calls.
    Unsupported {
        /// Requested memory placement policy.
        requested: MemoryPlacement,
        /// Human-readable reason reported by the runtime.
        reason: String,
    },
    /// The runtime attempted placement, but the OS rejected it or the policy
    /// could not be resolved for this shard.
    Failed {
        /// Requested memory placement policy.
        requested: MemoryPlacement,
        /// Human-readable OS or runtime error.
        error: String,
    },
}

impl MemoryPlacementStatus {
    /// Returns whether the OS accepted the requested placement.
    pub fn is_applied(&self) -> bool {
        matches!(self, MemoryPlacementStatus::Applied { .. })
    }
}

impl fmt::Display for MemoryPlacementStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryPlacementStatus::Default => write!(f, "default memory placement"),
            MemoryPlacementStatus::Applied { policy } => {
                write!(f, "memory placement applied: {policy:?}")
            }
            MemoryPlacementStatus::Unsupported { requested, reason } => {
                write!(f, "memory placement {requested:?} unsupported: {reason}")
            }
            MemoryPlacementStatus::Failed { requested, error } => {
                write!(f, "memory placement {requested:?} failed: {error}")
            }
        }
    }
}

impl fmt::Display for CpuPlacementStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuPlacementStatus::Unpinned => write!(f, "unpinned"),
            CpuPlacementStatus::Applied { cpu, numa_node } => {
                if let Some(node) = numa_node {
                    write!(f, "pinned to CPU {} on NUMA node {}", cpu.0, node.0)
                } else {
                    write!(f, "pinned to CPU {}", cpu.0)
                }
            }
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

/// Returns the NUMA node observed for `cpu`, when the platform exposes it.
pub fn numa_node_for_cpu(cpu: CpuId) -> Option<NumaNodeId> {
    platform::numa_node_for_cpu(cpu)
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

pub(crate) fn apply_memory_to_current_thread(
    placement: &MemoryPlacement,
    cpu_placement: &CpuPlacementStatus,
) -> MemoryPlacementStatus {
    let resolved = match placement {
        MemoryPlacement::Default => return MemoryPlacementStatus::Default,
        MemoryPlacement::LocalToCpu => {
            let Some(node) = cpu_placement.numa_node() else {
                return MemoryPlacementStatus::Failed {
                    requested: placement.clone(),
                    error: String::from("pinned CPU NUMA node is unavailable"),
                };
            };
            MemoryPlacement::Bind(node)
        }
        MemoryPlacement::Bind(node) => MemoryPlacement::Bind(*node),
        MemoryPlacement::Preferred(node) => MemoryPlacement::Preferred(*node),
        MemoryPlacement::Interleave(nodes) => MemoryPlacement::Interleave(nodes.clone()),
    };

    platform::apply_memory_to_current_thread(placement, resolved)
}

impl CpuPlacementStatus {
    fn numa_node(&self) -> Option<NumaNodeId> {
        match self {
            CpuPlacementStatus::Applied { numa_node, .. } => *numa_node,
            CpuPlacementStatus::Unpinned
            | CpuPlacementStatus::Unsupported { .. }
            | CpuPlacementStatus::Failed { .. } => None,
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{CpuId, CpuPlacementStatus, MemoryPlacement, MemoryPlacementStatus, NumaNodeId};
    use std::fs;
    use std::io;
    use std::mem;
    use std::os::raw::{c_int, c_long, c_ulong, c_void};
    use std::path::PathBuf;

    const CPU_SETSIZE: usize = 1024;
    const CPU_BITS: usize = usize::BITS as usize;
    const CPU_SET_WORDS: usize = CPU_SETSIZE / CPU_BITS;
    const NODE_SETSIZE: usize = 1024;
    const NODE_BITS: usize = c_ulong::BITS as usize;
    const NODE_SET_WORDS: usize = NODE_SETSIZE / NODE_BITS;
    const MPOL_DEFAULT: c_int = 0;
    const MPOL_PREFERRED: c_int = 1;
    const MPOL_BIND: c_int = 2;
    const MPOL_INTERLEAVE: c_int = 3;
    #[cfg(target_arch = "x86_64")]
    const SYS_SET_MEMPOLICY: c_long = 238;
    #[cfg(target_arch = "aarch64")]
    const SYS_SET_MEMPOLICY: c_long = 237;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    const SYS_SET_MEMPOLICY: c_long = -1;

    #[repr(C)]
    struct CpuSet {
        bits: [usize; CPU_SET_WORDS],
    }

    #[repr(C)]
    struct NodeSet {
        bits: [c_ulong; NODE_SET_WORDS],
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

    impl NodeSet {
        fn empty() -> Self {
            Self {
                bits: [0; NODE_SET_WORDS],
            }
        }

        fn set(&mut self, node: NumaNodeId) -> bool {
            let word_idx = node.0 / NODE_BITS;
            let bit_idx = node.0 % NODE_BITS;

            if word_idx >= self.bits.len() {
                return false;
            }

            self.bits[word_idx] |= (1 as c_ulong) << bit_idx;
            true
        }
    }

    unsafe extern "C" {
        fn sched_getaffinity(pid: c_int, cpusetsize: c_ulong, mask: *mut c_void) -> c_int;
        fn sched_setaffinity(pid: c_int, cpusetsize: c_ulong, mask: *const c_void) -> c_int;
        fn syscall(number: c_long, ...) -> c_long;
    }

    pub(super) fn available_cpu_ids() -> Option<Vec<CpuId>> {
        let mut set = CpuSet::empty();

        // SAFETY: `sched_getaffinity` reads the calling thread's CPU
        // affinity mask. We pass pid=0 for the current thread,
        // `sizeof(CpuSet)` as the mask size (matching the kernel's
        // expectation for the raw mask buffer), and a pointer to an
        // owned, properly-aligned `CpuSet`. The kernel only writes
        // within the provided size.
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

        // SAFETY: `sched_setaffinity` pins the calling thread to the
        // CPUs in `set`. We pass pid=0 for the current thread,
        // `sizeof(CpuSet)` matching the kernel's expectation, and a
        // pointer to an immutable, initialized `CpuSet` whose lifetime
        // covers the call.
        let result = unsafe {
            sched_setaffinity(
                0,
                mem::size_of::<CpuSet>() as c_ulong,
                (&set as *const CpuSet).cast::<c_void>(),
            )
        };

        if result == 0 {
            CpuPlacementStatus::Applied {
                cpu,
                numa_node: numa_node_for_cpu(cpu),
            }
        } else {
            CpuPlacementStatus::Failed {
                requested: cpu,
                error: io::Error::last_os_error().to_string(),
            }
        }
    }

    pub(super) fn numa_node_for_cpu(cpu: CpuId) -> Option<NumaNodeId> {
        let cpu_dir = PathBuf::from(format!("/sys/devices/system/cpu/cpu{}", cpu.0));
        let entries = fs::read_dir(cpu_dir).ok()?;

        entries
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter_map(|name| name.strip_prefix("node").map(str::to_owned))
            .filter_map(|node| node.parse::<usize>().ok())
            .min()
            .map(NumaNodeId)
    }

    pub(super) fn apply_memory_to_current_thread(
        requested: &MemoryPlacement,
        resolved: MemoryPlacement,
    ) -> MemoryPlacementStatus {
        if SYS_SET_MEMPOLICY < 0 {
            return MemoryPlacementStatus::Unsupported {
                requested: requested.clone(),
                reason: String::from(
                    "set_mempolicy syscall number is unknown on this Linux architecture",
                ),
            };
        }

        let (mode, nodes): (c_int, Vec<NumaNodeId>) = match &resolved {
            MemoryPlacement::Default => (MPOL_DEFAULT, Vec::new()),
            MemoryPlacement::LocalToCpu => unreachable!("LocalToCpu must be resolved first"),
            MemoryPlacement::Bind(node) => (MPOL_BIND, vec![*node]),
            MemoryPlacement::Preferred(node) => (MPOL_PREFERRED, vec![*node]),
            MemoryPlacement::Interleave(nodes) => (MPOL_INTERLEAVE, nodes.clone()),
        };

        let mut set = NodeSet::empty();
        for node in nodes {
            if !set.set(node) {
                return MemoryPlacementStatus::Failed {
                    requested: requested.clone(),
                    error: format!("NUMA node index exceeds supported NODE_SETSIZE {NODE_SETSIZE}"),
                };
            }
        }

        let nodemask = if matches!(resolved, MemoryPlacement::Default) {
            std::ptr::null()
        } else {
            set.bits.as_ptr()
        };

        // SAFETY: `set_mempolicy` updates the calling thread's default memory
        // policy through the raw Linux syscall. `nodemask` either points to
        // the initialized `NodeSet` storage above for the duration of the call
        // or is null for MPOL_DEFAULT. `NODE_SETSIZE` is the number of node
        // bits represented by the mask.
        let result = unsafe { syscall(SYS_SET_MEMPOLICY, mode, nodemask, NODE_SETSIZE as c_ulong) };

        if result == 0 {
            MemoryPlacementStatus::Applied { policy: resolved }
        } else {
            MemoryPlacementStatus::Failed {
                requested: requested.clone(),
                error: io::Error::last_os_error().to_string(),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{CpuId, CpuPlacementStatus, MemoryPlacement, MemoryPlacementStatus, NumaNodeId};

    pub(super) fn available_cpu_ids() -> Option<Vec<CpuId>> {
        None
    }

    pub(super) fn apply_to_current_thread(cpu: CpuId) -> CpuPlacementStatus {
        CpuPlacementStatus::Unsupported {
            requested: Some(cpu),
            reason: String::from("hard CPU affinity is only implemented on Linux"),
        }
    }

    pub(super) fn numa_node_for_cpu(_cpu: CpuId) -> Option<NumaNodeId> {
        None
    }

    pub(super) fn apply_memory_to_current_thread(
        requested: &MemoryPlacement,
        _resolved: MemoryPlacement,
    ) -> MemoryPlacementStatus {
        MemoryPlacementStatus::Unsupported {
            requested: requested.clone(),
            reason: String::from("NUMA memory placement is only implemented on Linux"),
        }
    }
}
