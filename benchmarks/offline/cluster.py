"""
Cluster data model for the offline simulator.

Mirrors the OpenCue concepts that matter for scheduling:

    Host    - a physical/virtual machine with a fixed total capacity
              and a current set of running procs.
    Frame   - a single unit of dispatchable work belonging to a layer.
              Has a state (WAITING / RUNNING / DONE) and, while running,
              a scheduled completion time.
    Layer   - a group of frames that share resource requirements
              (cores, memory, gpus, tags, os). The unit at which the
              Scheduler makes placement decisions.
    Job     - a collection of layers. Carries priority + arrival time
              for scheduler ordering.
    Cluster - the whole world: hosts plus a queue of pending jobs.

The classes are deliberately plain dataclasses; the scheduler logic
lives in schedulers.py and operates on these structures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Set


# ---- frame ------------------------------------------------------------------

STATE_WAITING = "WAITING"
STATE_RUNNING = "RUNNING"
STATE_DONE = "DONE"


@dataclass
class Frame:
    frame_id: str
    layer_id: str
    job_id: str
    int_number: int
    state: str = STATE_WAITING
    # When state is RUNNING: simulated unix time at which the frame finishes
    # and frees its resources. Set by the simulator at dispatch time.
    finish_time: float = 0.0
    # Host id this frame is running on (or last ran on).
    host_id: Optional[str] = None


# ---- layer ------------------------------------------------------------------

@dataclass
class Layer:
    layer_id: str
    job_id: str
    show_id: str
    cores_min: int          # in cores (not core points; converted at the boundary)
    mem_min_kb: int
    gpus_min: int
    gpu_mem_min_kb: int
    tags: Set[str] = field(default_factory=set)  # layer requires ANY of these
    os: Optional[str] = None
    frames: List[Frame] = field(default_factory=list)
    # Runtime distribution parameters for frames in this layer (median seconds).
    # The simulator samples actual frame runtimes from a lognormal with this median.
    runtime_median_s: float = 600.0
    runtime_sigma: float = 1.0

    def waiting_frame_count(self) -> int:
        return sum(1 for f in self.frames if f.state == STATE_WAITING)

    def next_frame_number(self) -> int:
        """Lowest int_number among WAITING frames; -1 if none."""
        return min(
            (f.int_number for f in self.frames if f.state == STATE_WAITING),
            default=-1,
        )

    def next_waiting_frames(self, limit: int) -> List[Frame]:
        """Frames sorted by int_number, up to limit, that are WAITING."""
        return sorted(
            (f for f in self.frames if f.state == STATE_WAITING),
            key=lambda f: f.int_number,
        )[:limit]


# ---- job --------------------------------------------------------------------

@dataclass
class Job:
    job_id: str
    show_id: str
    priority: int
    ts_started: float           # simulated time when submitted
    layers: List[Layer] = field(default_factory=list)
    paused: bool = False
    state: str = "PENDING"
    # Cumulative cores currently used by this job's running frames. Updated by
    # the simulator on dispatch and frame completion. Mirrors job_resource.int_cores.
    cores_in_use: int = 0
    # int_max_cores cap. Default is large so caps don't bite the simulator
    # unless we explicitly set them per job.
    max_cores: int = 100000


# ---- host -------------------------------------------------------------------

@dataclass
class Host:
    host_id: str
    name: str
    alloc: str
    cores_total: int            # in cores
    mem_total_kb: int
    gpus_total: int
    gpu_mem_total_kb: int
    tags: Set[str] = field(default_factory=set)  # what the host advertises
    os: Optional[str] = None
    # Current idle resources. Mirror the host table's int_*_idle columns.
    cores_idle: int = 0
    mem_idle_kb: int = 0
    gpus_idle: int = 0
    gpu_mem_idle_kb: int = 0
    # Procs currently running. Each is a Frame ref; finish_time tells us when
    # the simulator should free its resources.
    running: List[Frame] = field(default_factory=list)

    def __post_init__(self):
        # If idle resources weren't set explicitly, initialize to total.
        if self.cores_idle == 0 and self.cores_total > 0:
            self.cores_idle = self.cores_total
        if self.mem_idle_kb == 0 and self.mem_total_kb > 0:
            self.mem_idle_kb = self.mem_total_kb
        if self.gpus_idle == 0 and self.gpus_total > 0:
            self.gpus_idle = self.gpus_total
        if self.gpu_mem_idle_kb == 0 and self.gpu_mem_total_kb > 0:
            self.gpu_mem_idle_kb = self.gpu_mem_total_kb

    def running_procs(self) -> int:
        return len(self.running)


# ---- show -------------------------------------------------------------------

@dataclass
class Show:
    show_id: str
    alloc: str
    burst_cores: int
    # Cumulative cores currently in use by this show on this alloc. Mirrors
    # subscription.int_cores.
    cores_in_use: int = 0


# ---- cluster ----------------------------------------------------------------

@dataclass
class Cluster:
    hosts: List[Host] = field(default_factory=list)
    jobs: List[Job] = field(default_factory=list)
    shows: List[Show] = field(default_factory=list)

    def show(self, show_id: str) -> Show:
        for s in self.shows:
            if s.show_id == show_id:
                return s
        raise KeyError(show_id)

    def total_cores(self) -> int:
        return sum(h.cores_total for h in self.hosts)

    def total_idle_cores(self) -> int:
        return sum(h.cores_idle for h in self.hosts)

    def waiting_frame_count(self) -> int:
        return sum(
            sum(1 for f in l.frames if f.state == STATE_WAITING)
            for j in self.jobs
            for l in j.layers
        )

    def running_frame_count(self) -> int:
        return sum(len(h.running) for h in self.hosts)
