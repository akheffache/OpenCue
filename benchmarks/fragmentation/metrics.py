"""
Metrics - Measures fragmentation and resource utilization from CueBot.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

import opencue

logger = logging.getLogger(__name__)


# =============================================================================
# Frame type configuration (for stranded analysis)
# =============================================================================
FRAME_TYPE_MEMORY_KB = {
    1: int(0.52 * 1024 * 1024),    # 0.52 GB
    2: int(1.38 * 1024 * 1024),    # 1.38 GB
    4: int(6.44 * 1024 * 1024),    # 6.44 GB
    8: int(27.27 * 1024 * 1024),   # 27.27 GB
    16: int(63.85 * 1024 * 1024),  # 63.85 GB
    32: int(109.00 * 1024 * 1024), # 109 GB
    64: int(233.12 * 1024 * 1024), # 233.12 GB
}


@dataclass
class HostMetrics:
    """Metrics for a single host."""
    name: str
    host_type: str  # elk, ram, jaime
    total_cores: float
    idle_cores: float
    booked_cores: float
    total_memory_kb: int
    idle_memory_kb: int
    booked_memory_kb: int
    running_frame_count: int
    is_fragmented: bool = False
    stranded_reason: str = ""  # "core-limited", "memory-limited", or ""


@dataclass
class FragmentationReport:
    """Aggregated fragmentation metrics for the entire farm."""
    # Core metrics
    total_cores: float = 0.0
    booked_cores: float = 0.0
    idle_cores: float = 0.0
    fragmented_cores: float = 0.0

    # Memory metrics (in GB for readability)
    total_memory_gb: float = 0.0
    booked_memory_gb: float = 0.0
    idle_memory_gb: float = 0.0

    # Host metrics
    total_hosts: int = 0
    hosts_with_frames: int = 0
    fully_utilized_hosts: int = 0
    fragmented_hosts: int = 0

    # Per host-type breakdown
    by_host_type: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Frame metrics
    total_frames: int = 0
    running_frames: int = 0
    pending_frames: int = 0

    # Calculated percentages
    @property
    def core_utilization_percent(self) -> float:
        if self.total_cores == 0:
            return 0.0
        return (self.booked_cores / self.total_cores) * 100

    @property
    def memory_utilization_percent(self) -> float:
        if self.total_memory_gb == 0:
            return 0.0
        return (self.booked_memory_gb / self.total_memory_gb) * 100

    @property
    def fragmentation_percent(self) -> float:
        if self.idle_cores == 0:
            return 0.0
        return (self.fragmented_cores / self.idle_cores) * 100

    @property
    def effective_fragmentation_percent(self) -> float:
        if self.total_cores == 0:
            return 0.0
        return (self.fragmented_cores / self.total_cores) * 100


@dataclass
class PendingFrameRequirements:
    """Smallest resource requirements among pending frames."""
    min_cores: int = 1
    min_memory_kb: int = 0
    frame_types_pending: List[int] = field(default_factory=list)


@dataclass
class DispatchTimingStats:
    """Timing statistics for dispatch operations."""
    start_time: float = 0.0
    end_time: float = 0.0
    dispatch_rounds: int = 0
    frames_at_start: int = 0
    frames_at_end: int = 0

    @property
    def total_time_sec(self) -> float:
        return self.end_time - self.start_time

    @property
    def frames_dispatched(self) -> int:
        return self.frames_at_end - self.frames_at_start

    @property
    def frames_per_second(self) -> float:
        if self.total_time_sec == 0:
            return 0.0
        return self.frames_dispatched / self.total_time_sec


@dataclass
class PriorityStats:
    """Statistics for a single priority level."""
    priority: int
    job_name: str
    total_frames: int = 0
    running_frames: int = 0
    pending_frames: int = 0
    succeeded_frames: int = 0
    cores_reserved: float = 0.0

    @property
    def fill_rate(self) -> float:
        if self.total_frames == 0:
            return 0.0
        return (self.running_frames / self.total_frames) * 100


@dataclass
class PriorityBreakdown:
    """Priority analysis across all jobs."""
    by_priority: Dict[int, PriorityStats] = field(default_factory=dict)

    @property
    def priorities_sorted(self) -> List[int]:
        return sorted(self.by_priority.keys(), reverse=True)

    def is_fair(self) -> bool:
        """Check if higher priorities have higher fill rates."""
        sorted_priorities = self.priorities_sorted
        for i in range(len(sorted_priorities) - 1):
            high = self.by_priority[sorted_priorities[i]]
            low = self.by_priority[sorted_priorities[i + 1]]
            if high.fill_rate < low.fill_rate:
                return False
        return True


@dataclass
class StrandedAnalysis:
    """Analysis of why cores are stranded."""
    total_stranded_cores: float = 0.0
    core_limited_cores: float = 0.0  # Not enough cores on host
    memory_limited_cores: float = 0.0  # Not enough memory on host
    core_limited_hosts: int = 0
    memory_limited_hosts: int = 0

    # By host type
    by_host_type: Dict[str, Dict[str, float]] = field(default_factory=dict)

    @property
    def core_limited_percent(self) -> float:
        if self.total_stranded_cores == 0:
            return 0.0
        return (self.core_limited_cores / self.total_stranded_cores) * 100

    @property
    def memory_limited_percent(self) -> float:
        if self.total_stranded_cores == 0:
            return 0.0
        return (self.memory_limited_cores / self.total_stranded_cores) * 100


class MetricsCollector:
    """Collects fragmentation and utilization metrics from CueBot."""

    def __init__(self):
        self.host_metrics: List[HostMetrics] = []
        self.timing_stats: DispatchTimingStats = DispatchTimingStats()
        self.priority_breakdown: PriorityBreakdown = PriorityBreakdown()
        self.stranded_analysis: StrandedAnalysis = StrandedAnalysis()

    def _detect_host_type(self, host) -> str:
        """Detect host type based on core count."""
        cores = host.data.cores
        if cores <= 16:
            return "elk"
        elif cores <= 32:
            return "ram"
        else:
            return "jaime"

    def _check_stranded_reason(
        self,
        idle_cores: float,
        idle_memory_kb: int,
        pending_requirements: PendingFrameRequirements,
    ) -> str:
        """
        Determine why a host with idle cores can't run any pending frame.
        Returns: "core-limited", "memory-limited", or "" if not stranded.
        """
        if not pending_requirements.frame_types_pending:
            return ""

        # Check each pending frame type to see what could fit
        could_fit_if_more_cores = False
        could_fit_if_more_memory = False

        for core_req in pending_requirements.frame_types_pending:
            mem_req = FRAME_TYPE_MEMORY_KB.get(core_req, 0)

            has_enough_cores = idle_cores >= core_req
            has_enough_memory = idle_memory_kb >= mem_req

            if has_enough_cores and has_enough_memory:
                return ""  # Can fit this frame type, not stranded

            if has_enough_memory and not has_enough_cores:
                could_fit_if_more_cores = True

            if has_enough_cores and not has_enough_memory:
                could_fit_if_more_memory = True

        # Determine primary reason
        if could_fit_if_more_cores and not could_fit_if_more_memory:
            return "core-limited"
        elif could_fit_if_more_memory and not could_fit_if_more_cores:
            return "memory-limited"
        elif could_fit_if_more_cores:
            return "core-limited"  # Default to core-limited if both could help
        else:
            return "core-limited"  # No pending frame could fit even with more resources

    def _can_fit_any_pending_frame(
        self,
        idle_cores: float,
        idle_memory_kb: int,
        pending_requirements: PendingFrameRequirements,
    ) -> bool:
        """Check if idle resources can fit any pending frame type."""
        if not pending_requirements.frame_types_pending:
            return True

        for core_req in pending_requirements.frame_types_pending:
            mem_req = FRAME_TYPE_MEMORY_KB.get(core_req, 0)
            if idle_cores >= core_req and idle_memory_kb >= mem_req:
                return True

        return False

    def start_timing(self, job_names: List[str]) -> None:
        """Start timing dispatch operations."""
        self.timing_stats = DispatchTimingStats()
        self.timing_stats.start_time = time.time()

        # Count running frames at start
        running = 0
        for job_name in job_names:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    running += job.data.job_stats.running_frames
            except Exception:
                pass
        self.timing_stats.frames_at_start = running

    def record_dispatch_round(self) -> None:
        """Record a dispatch round."""
        self.timing_stats.dispatch_rounds += 1

    def stop_timing(self, job_names: List[str]) -> DispatchTimingStats:
        """Stop timing and calculate final stats."""
        self.timing_stats.end_time = time.time()

        # Count running frames at end
        running = 0
        for job_name in job_names:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    running += job.data.job_stats.running_frames
            except Exception:
                pass
        self.timing_stats.frames_at_end = running

        return self.timing_stats

    def collect_priority_breakdown(self, job_names: List[str]) -> PriorityBreakdown:
        """Collect per-priority statistics."""
        self.priority_breakdown = PriorityBreakdown()

        for job_name in job_names:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    priority = job.data.priority
                    stats = PriorityStats(
                        priority=priority,
                        job_name=job.name(),
                        total_frames=job.data.job_stats.total_frames,
                        running_frames=job.data.job_stats.running_frames,
                        pending_frames=job.data.job_stats.pending_frames,
                        succeeded_frames=job.data.job_stats.succeeded_frames,
                        cores_reserved=job.data.job_stats.reserved_cores,
                    )
                    self.priority_breakdown.by_priority[priority] = stats
            except Exception as e:
                logger.warning(f"Failed to get priority stats for job {job_name}: {e}")

        return self.priority_breakdown

    def collect_from_jobs(self, job_names: List[str]) -> PendingFrameRequirements:
        """Collect pending frame requirements from specified jobs."""
        requirements = PendingFrameRequirements()
        frame_types_with_pending = set()

        for job_name in job_names:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    layers = job.getLayers()
                    for layer in layers:
                        if layer.pendingFrames() > 0:
                            cores = int(layer.data.min_cores)
                            frame_types_with_pending.add(cores)
            except Exception as e:
                logger.warning(f"Failed to get pending frames for job {job_name}: {e}")

        requirements.frame_types_pending = sorted(frame_types_with_pending)
        if requirements.frame_types_pending:
            requirements.min_cores = min(requirements.frame_types_pending)

        return requirements

    def collect_host_metrics(
        self,
        pending_requirements: Optional[PendingFrameRequirements] = None,
    ) -> List[HostMetrics]:
        """Collect metrics from all hosts."""
        self.host_metrics = []

        if pending_requirements is None:
            pending_requirements = PendingFrameRequirements()

        try:
            hosts = opencue.api.getHosts()
        except Exception as e:
            logger.error(f"Failed to get hosts: {e}")
            return []

        for host in hosts:
            try:
                total_cores = host.data.cores
                idle_cores = host.data.idle_cores
                booked_cores = total_cores - idle_cores

                total_memory_kb = host.data.memory
                idle_memory_kb = host.data.idle_memory
                booked_memory_kb = total_memory_kb - idle_memory_kb

                # Count running frames via procs
                try:
                    procs = host.getProcs()
                    running_frame_count = len(procs)
                except Exception:
                    running_frame_count = 0

                # Check if fragmented and why
                is_fragmented = False
                stranded_reason = ""
                if idle_cores > 0:
                    is_fragmented = not self._can_fit_any_pending_frame(
                        idle_cores, idle_memory_kb, pending_requirements
                    )
                    if is_fragmented:
                        stranded_reason = self._check_stranded_reason(
                            idle_cores, idle_memory_kb, pending_requirements
                        )

                metrics = HostMetrics(
                    name=host.data.name,
                    host_type=self._detect_host_type(host),
                    total_cores=total_cores,
                    idle_cores=idle_cores,
                    booked_cores=booked_cores,
                    total_memory_kb=total_memory_kb,
                    idle_memory_kb=idle_memory_kb,
                    booked_memory_kb=booked_memory_kb,
                    running_frame_count=running_frame_count,
                    is_fragmented=is_fragmented,
                    stranded_reason=stranded_reason,
                )
                self.host_metrics.append(metrics)

            except Exception as e:
                logger.warning(f"Failed to collect metrics for host {host.data.name}: {e}")

        return self.host_metrics

    def collect_stranded_analysis(self) -> StrandedAnalysis:
        """Analyze why cores are stranded."""
        self.stranded_analysis = StrandedAnalysis()

        # Initialize per-type tracking
        for host_type in ["elk", "ram", "jaime"]:
            self.stranded_analysis.by_host_type[host_type] = {
                "core_limited": 0.0,
                "memory_limited": 0.0,
            }

        for hm in self.host_metrics:
            if hm.is_fragmented and hm.idle_cores > 0:
                self.stranded_analysis.total_stranded_cores += hm.idle_cores

                if hm.stranded_reason == "core-limited":
                    self.stranded_analysis.core_limited_cores += hm.idle_cores
                    self.stranded_analysis.core_limited_hosts += 1
                    self.stranded_analysis.by_host_type[hm.host_type]["core_limited"] += hm.idle_cores
                elif hm.stranded_reason == "memory-limited":
                    self.stranded_analysis.memory_limited_cores += hm.idle_cores
                    self.stranded_analysis.memory_limited_hosts += 1
                    self.stranded_analysis.by_host_type[hm.host_type]["memory_limited"] += hm.idle_cores

        return self.stranded_analysis

    def generate_report(
        self,
        job_names: Optional[List[str]] = None,
    ) -> FragmentationReport:
        """Generate a complete fragmentation report."""
        report = FragmentationReport()

        # Collect pending frame requirements if jobs specified
        pending_requirements = None
        if job_names:
            pending_requirements = self.collect_from_jobs(job_names)
            logger.info(f"Pending frame types: {pending_requirements.frame_types_pending}")

        # Collect host metrics
        host_metrics = self.collect_host_metrics(pending_requirements)

        if not host_metrics:
            logger.warning("No host metrics collected")
            return report

        # Initialize per-type tracking
        type_stats = {}
        for host_type in ["elk", "ram", "jaime"]:
            type_stats[host_type] = {
                "count": 0,
                "total_cores": 0.0,
                "booked_cores": 0.0,
                "idle_cores": 0.0,
                "fragmented_cores": 0.0,
                "fragmented_hosts": 0,
            }

        # Aggregate metrics
        for hm in host_metrics:
            report.total_hosts += 1
            report.total_cores += hm.total_cores
            report.booked_cores += hm.booked_cores
            report.idle_cores += hm.idle_cores
            report.total_memory_gb += hm.total_memory_kb / 1024 / 1024
            report.booked_memory_gb += hm.booked_memory_kb / 1024 / 1024
            report.idle_memory_gb += hm.idle_memory_kb / 1024 / 1024

            if hm.running_frame_count > 0:
                report.hosts_with_frames += 1

            if hm.idle_cores == 0:
                report.fully_utilized_hosts += 1

            if hm.is_fragmented:
                report.fragmented_hosts += 1
                report.fragmented_cores += hm.idle_cores

            # Per-type stats
            ts = type_stats[hm.host_type]
            ts["count"] += 1
            ts["total_cores"] += hm.total_cores
            ts["booked_cores"] += hm.booked_cores
            ts["idle_cores"] += hm.idle_cores
            if hm.is_fragmented:
                ts["fragmented_cores"] += hm.idle_cores
                ts["fragmented_hosts"] += 1

        report.by_host_type = type_stats

        # Collect job frame stats if provided
        if job_names:
            for job_name in job_names:
                try:
                    jobs = opencue.api.getJobs(job=[job_name])
                    for job in jobs:
                        report.total_frames += job.data.job_stats.total_frames
                        report.running_frames += job.data.job_stats.running_frames
                        report.pending_frames += job.data.job_stats.pending_frames
                except Exception:
                    pass

            # Collect priority breakdown and stranded analysis
            self.collect_priority_breakdown(job_names)
            self.collect_stranded_analysis()

        return report

    def print_timing_stats(self, stats: DispatchTimingStats) -> None:
        """Print dispatch timing statistics."""
        print("\n--- DISPATCH TIMING ---")
        print(f"Total Time:         {stats.total_time_sec:.1f}s")
        print(f"Dispatch Rounds:    {stats.dispatch_rounds}")
        print(f"Frames Dispatched:  {stats.frames_dispatched}")
        print(f"Frames/Second:      {stats.frames_per_second:.1f}")

    def print_priority_breakdown(self, breakdown: Optional[PriorityBreakdown] = None) -> None:
        """Print priority analysis."""
        if breakdown is None:
            breakdown = self.priority_breakdown

        if not breakdown.by_priority:
            return

        print("\n--- PRIORITY BREAKDOWN ---")
        print(f"{'Priority':<10} {'Total':<10} {'Running':<10} {'Pending':<10} {'Fill Rate':<10} {'Cores':<10}")
        print("-" * 60)

        for priority in breakdown.priorities_sorted:
            stats = breakdown.by_priority[priority]
            print(f"{stats.priority:<10} {stats.total_frames:<10} {stats.running_frames:<10} "
                  f"{stats.pending_frames:<10} {stats.fill_rate:<9.1f}% {stats.cores_reserved:<10.0f}")

        print("-" * 60)
        fairness = "YES" if breakdown.is_fair() else "NO (priority inversion detected)"
        print(f"Scheduling Fair:    {fairness}")

    def print_stranded_analysis(self, analysis: Optional[StrandedAnalysis] = None) -> None:
        """Print stranded resource analysis."""
        if analysis is None:
            analysis = self.stranded_analysis

        if analysis.total_stranded_cores == 0:
            print("\n--- STRANDED ANALYSIS ---")
            print("No stranded cores detected.")
            return

        print("\n--- STRANDED ANALYSIS ---")
        print(f"Total Stranded Cores: {analysis.total_stranded_cores:.0f}")
        print(f"\nStranded Due To:")
        print(f"  Core-limited:   {analysis.core_limited_cores:>8.0f} ({analysis.core_limited_percent:.1f}%) "
              f"- {analysis.core_limited_hosts} hosts")
        print(f"  Memory-limited: {analysis.memory_limited_cores:>8.0f} ({analysis.memory_limited_percent:.1f}%) "
              f"- {analysis.memory_limited_hosts} hosts")

        print(f"\nBy Host Type:")
        for host_type in ["elk", "ram", "jaime"]:
            stats = analysis.by_host_type.get(host_type, {})
            core_lim = stats.get("core_limited", 0)
            mem_lim = stats.get("memory_limited", 0)
            total = core_lim + mem_lim
            if total > 0:
                print(f"  {host_type.upper():<8} Core-limited: {core_lim:>6.0f}  Memory-limited: {mem_lim:>6.0f}")

    def print_report(self, report: FragmentationReport) -> None:
        """Print a formatted fragmentation report."""
        print("\n" + "=" * 70)
        print("FRAGMENTATION REPORT")
        print("=" * 70)

        print("\n--- OVERALL METRICS ---")
        print(f"Total Hosts:        {report.total_hosts}")
        print(f"Hosts with Frames:  {report.hosts_with_frames}")
        print(f"Fully Utilized:     {report.fully_utilized_hosts}")
        print(f"Fragmented Hosts:   {report.fragmented_hosts}")

        print(f"\nTotal Cores:        {report.total_cores:.0f}")
        print(f"Booked Cores:       {report.booked_cores:.0f}")
        print(f"Idle Cores:         {report.idle_cores:.0f}")
        print(f"Fragmented Cores:   {report.fragmented_cores:.0f}")

        print(f"\nCore Utilization:   {report.core_utilization_percent:.1f}%")
        print(f"Fragmentation:      {report.fragmentation_percent:.1f}% of idle cores unusable")
        print(f"Effective Frag:     {report.effective_fragmentation_percent:.1f}% of total cores")

        print(f"\nTotal Memory:       {report.total_memory_gb:.1f} GB")
        print(f"Booked Memory:      {report.booked_memory_gb:.1f} GB")
        print(f"Memory Utilization: {report.memory_utilization_percent:.1f}%")

        print("\n--- FRAME METRICS ---")
        print(f"Total Frames:       {report.total_frames}")
        print(f"Running Frames:     {report.running_frames}")
        print(f"Pending Frames:     {report.pending_frames}")

        print("\n--- BY HOST TYPE ---")
        for host_type, stats in report.by_host_type.items():
            if stats["count"] > 0:
                util = (stats["booked_cores"] / stats["total_cores"] * 100) if stats["total_cores"] > 0 else 0
                frag = (stats["fragmented_cores"] / stats["idle_cores"] * 100) if stats["idle_cores"] > 0 else 0
                print(f"\n{host_type.upper()}:")
                print(f"  Count:            {stats['count']}")
                print(f"  Total Cores:      {stats['total_cores']:.0f}")
                print(f"  Booked Cores:     {stats['booked_cores']:.0f}")
                print(f"  Utilization:      {util:.1f}%")
                print(f"  Fragmented Cores: {stats['fragmented_cores']:.0f}")
                print(f"  Fragmentation:    {frag:.1f}%")

        # Print additional analyses
        self.print_priority_breakdown()
        self.print_stranded_analysis()

        print("\n" + "=" * 70)
