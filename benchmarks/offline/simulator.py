"""
Discrete-time simulator. Runs a scheduler against a workload, in-process.

Each tick:

  1. Advance simulated time by tick_seconds.
  2. Complete any frames whose finish_time has passed; free their resources.
  3. Inject newly-arrived jobs.
  4. Call scheduler.tick(cluster, now) -> list of (host, frame) bookings.
  5. Apply each booking: mark frame RUNNING, set finish_time from runtime
     sampler, record metrics.

The two schedulers (Legacy, Smart) run in separate simulator instances
against the same workload and host fleet, with the same RNG seed. The
caller then compares the recorded metrics.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Tuple

from cluster import (
    Cluster,
    Frame,
    Host,
    Job,
    STATE_DONE,
    STATE_RUNNING,
    STATE_WAITING,
)
from workload import sample_frame_runtime


# ---- metrics record -------------------------------------------------------

@dataclass
class TickMetrics:
    t: float
    cores_total: int = 0
    cores_busy: int = 0
    cores_idle: int = 0
    frames_running: int = 0
    frames_waiting: int = 0
    bookings_this_tick: int = 0
    fragmented_cores: int = 0           # idle cores while waiting frames exist
    db_ops_cumulative: int = 0


@dataclass
class WaitRecord:
    layer_id: str
    job_id: str
    priority: int
    cores_min: int
    first_seen_at: float
    first_dispatched_at: float = -1.0   # -1 means never dispatched in the sim


# ---- simulator ------------------------------------------------------------

class Simulator:
    def __init__(self,
                 cluster: Cluster,
                 jobs_in_order_of_arrival: List[Job],
                 scheduler,
                 tick_seconds: float = 1.0,
                 runtime_seed: int = 42):
        self.cluster = cluster
        self.pending_arrivals = list(jobs_in_order_of_arrival)
        self.scheduler = scheduler
        self.tick_seconds = tick_seconds
        self.now = 0.0
        self.rng = random.Random(runtime_seed)

        self.metrics: List[TickMetrics] = []
        self.wait_records: dict = {}   # layer_id -> WaitRecord
        self.total_bookings = 0

    # ---- main loop -------------------------------------------------------

    def run(self, simulation_seconds: float) -> None:
        end_t = simulation_seconds
        while self.now < end_t:
            self._complete_finished_frames()
            self._inject_arrivals()
            self._record_wait_starts()
            bookings = self.scheduler.tick(self.cluster, self.now)
            self._apply_bookings(bookings)
            self._record_metrics(bookings_count=len(bookings))
            self.now += self.tick_seconds

    # ---- step bodies -----------------------------------------------------

    def _complete_finished_frames(self):
        for h in self.cluster.hosts:
            still_running: List[Frame] = []
            for f in h.running:
                if f.finish_time <= self.now:
                    # Free resources.
                    layer = self._layer_of(f)
                    if layer is not None:
                        h.cores_idle      += layer.cores_min
                        h.mem_idle_kb     += layer.mem_min_kb
                        h.gpus_idle       += layer.gpus_min
                        h.gpu_mem_idle_kb += layer.gpu_mem_min_kb
                        job = self._job_of_layer(layer.job_id)
                        if job is not None:
                            job.cores_in_use -= layer.cores_min
                            if job.cores_in_use < 0:
                                job.cores_in_use = 0
                        show = self.cluster.show(layer.show_id)
                        show.cores_in_use -= layer.cores_min
                        if show.cores_in_use < 0:
                            show.cores_in_use = 0
                    f.state = STATE_DONE
                else:
                    still_running.append(f)
            h.running = still_running

    def _inject_arrivals(self):
        while self.pending_arrivals and self.pending_arrivals[0].ts_started <= self.now:
            j = self.pending_arrivals.pop(0)
            self.cluster.jobs.append(j)

    def _record_wait_starts(self):
        """Mark the first time each layer appears as WAITING with no dispatch yet."""
        for j in self.cluster.jobs:
            for layer in j.layers:
                if layer.layer_id in self.wait_records:
                    continue
                if layer.waiting_frame_count() > 0:
                    self.wait_records[layer.layer_id] = WaitRecord(
                        layer_id=layer.layer_id,
                        job_id=j.job_id,
                        priority=j.priority,
                        cores_min=layer.cores_min,
                        first_seen_at=self.now,
                    )

    def _apply_bookings(self, bookings: List[Tuple[Host, Frame]]):
        for (host, frame) in bookings:
            layer = self._layer_of(frame)
            if layer is None:
                continue
            frame.state = STATE_RUNNING
            frame.host_id = host.host_id
            runtime = sample_frame_runtime(self.rng, layer)
            frame.finish_time = self.now + runtime
            host.running.append(frame)
            self.total_bookings += 1
            rec = self.wait_records.get(layer.layer_id)
            if rec is not None and rec.first_dispatched_at < 0:
                rec.first_dispatched_at = self.now

    def _record_metrics(self, bookings_count: int):
        cluster = self.cluster
        cores_total = cluster.total_cores()
        cores_idle = cluster.total_idle_cores()
        cores_busy = cores_total - cores_idle
        frames_running = cluster.running_frame_count()
        frames_waiting = cluster.waiting_frame_count()
        fragmented = cores_idle if frames_waiting > 0 else 0
        db_ops = getattr(self.scheduler, "db_ops", 0)
        self.metrics.append(TickMetrics(
            t=self.now,
            cores_total=cores_total,
            cores_busy=cores_busy,
            cores_idle=cores_idle,
            frames_running=frames_running,
            frames_waiting=frames_waiting,
            bookings_this_tick=bookings_count,
            fragmented_cores=fragmented,
            db_ops_cumulative=db_ops,
        ))

    # ---- helpers ---------------------------------------------------------

    def _layer_of(self, frame: Frame):
        for j in self.cluster.jobs:
            if j.job_id != frame.job_id:
                continue
            for layer in j.layers:
                if layer.layer_id == frame.layer_id:
                    return layer
        return None

    def _job_of_layer(self, job_id: str):
        for j in self.cluster.jobs:
            if j.job_id == job_id:
                return j
        return None
