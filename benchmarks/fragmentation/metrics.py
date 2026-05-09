"""
Metrics - Measures fragmentation and resource utilization from CueBot.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

import opencue

logger = logging.getLogger(__name__)


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
    is_fragmented: bool = False  # True if idle cores but can't fit any pending frame


@dataclass
class FragmentationReport:
    """Aggregated fragmentation metrics for the entire farm."""
    # Core metrics
    total_cores: float = 0.0
    booked_cores: float = 0.0
    idle_cores: float = 0.0
    fragmented_cores: float = 0.0  # Idle cores that can't fit any pending frame

    # Memory metrics (in GB for readability)
    total_memory_gb: float = 0.0
    booked_memory_gb: float = 0.0
    idle_memory_gb: float = 0.0

    # Host metrics
    total_hosts: int = 0
    hosts_with_frames: int = 0
    fully_utilized_hosts: int = 0
    fragmented_hosts: int = 0  # Hosts with idle cores but can't fit pending work

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
        """Percentage of idle cores that are fragmented (unusable)."""
        if self.idle_cores == 0:
            return 0.0
        return (self.fragmented_cores / self.idle_cores) * 100

    @property
    def effective_fragmentation_percent(self) -> float:
        """Fragmented cores as percentage of total cores."""
        if self.total_cores == 0:
            return 0.0
        return (self.fragmented_cores / self.total_cores) * 100


@dataclass
class PendingFrameRequirements:
    """Smallest resource requirements among pending frames."""
    min_cores: int = 1
    min_memory_kb: int = 0
    frame_types_pending: List[int] = field(default_factory=list)  # Core counts with pending frames


class MetricsCollector:
    """Collects fragmentation and utilization metrics from CueBot."""

    def __init__(self):
        self.host_metrics: List[HostMetrics] = []

    def _detect_host_type(self, host) -> str:
        """Detect host type based on core count."""
        cores = host.data.cores
        if cores <= 16:
            return "elk"
        elif cores <= 32:
            return "ram"
        else:
            return "jaime"

    def _can_fit_any_pending_frame(
        self,
        idle_cores: float,
        idle_memory_kb: int,
        pending_requirements: PendingFrameRequirements,
    ) -> bool:
        """Check if idle resources can fit any pending frame type."""
        if not pending_requirements.frame_types_pending:
            return True  # No pending frames, not fragmented

        # Check each pending frame type
        for core_req in pending_requirements.frame_types_pending:
            # Simple check: if cores fit, assume memory likely fits
            # More accurate would be to check memory per frame type
            if idle_cores >= core_req:
                return True

        return False

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

                # Check if fragmented
                is_fragmented = False
                if idle_cores > 0:
                    is_fragmented = not self._can_fit_any_pending_frame(
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
                )
                self.host_metrics.append(metrics)

            except Exception as e:
                logger.warning(f"Failed to collect metrics for host {host.data.name}: {e}")

        return self.host_metrics

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

        return report

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

        print("\n" + "=" * 70)
