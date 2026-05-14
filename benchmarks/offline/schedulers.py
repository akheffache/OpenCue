"""
Two scheduler implementations, run side-by-side by the simulator.

  LegacyScheduler  - imitates Cuebot's per-host greedy first-fit. For each
                     idle host, scans pending jobs in priority order and
                     books the first frame that fits. Single-threaded
                     idealization (no inter-thread races, no 300ms sleep);
                     this is the BEST case for the legacy algorithm.

  SmartScheduler   - the algorithm from cuebot/dispatcher/Scheduler.java:
                     group hosts by static spec, query candidate layers
                     once per group, layer-driven best-fit with
                     stranding score, persistent priority-keyed
                     reservations.

Both expose the same interface:

    Scheduler.tick(cluster, now) -> List[(host, frame)]

The simulator applies the returned bookings to the cluster state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from cluster import (
    Cluster,
    Frame,
    Host,
    Job,
    Layer,
    STATE_WAITING,
)


# ============================================================================
# Common helpers
# ============================================================================

def _layer_fits_on_host_idle(layer: Layer, h: Host) -> bool:
    """Does ONE frame of this layer fit on the host's CURRENT idle resources?"""
    return (h.cores_idle    >= layer.cores_min
        and h.mem_idle_kb   >= layer.mem_min_kb
        and h.gpus_idle     >= layer.gpus_min
        and h.gpu_mem_idle_kb >= layer.gpu_mem_min_kb)


def _layer_fits_on_host_total(layer: Layer, h: Host) -> bool:
    """Could a frame of this layer fit on this host IF the host were idle?"""
    return (h.cores_total    >= layer.cores_min
        and h.mem_total_kb   >= layer.mem_min_kb
        and h.gpus_total     >= layer.gpus_min
        and h.gpu_mem_total_kb >= layer.gpu_mem_min_kb)


def _tags_compatible(layer: Layer, h: Host) -> bool:
    """The layer's tag set is a requirement; any-match against the host's tags."""
    if not layer.tags:
        return True
    return bool(layer.tags & h.tags)


def _os_compatible(layer: Layer, h: Host) -> bool:
    return layer.os is None or h.os == layer.os


def _dispatchable_layers(c: Cluster) -> List[Layer]:
    """All layers that have at least one WAITING frame, on PENDING unpaused jobs."""
    out: List[Layer] = []
    for j in c.jobs:
        if j.paused or j.state != "PENDING":
            continue
        for layer in j.layers:
            if layer.waiting_frame_count() > 0:
                out.append(layer)
    return out


def _job_of(layer: Layer, c: Cluster) -> Optional[Job]:
    for j in c.jobs:
        if j.job_id == layer.job_id:
            return j
    return None


# ============================================================================
# Legacy: per-host greedy first-fit by priority
# ============================================================================

class LegacyScheduler:
    """Cuebot's legacy dispatcher behavior, idealized as single-threaded.

    For each host with bookable capacity, walk the queue of dispatchable
    jobs in priority+age order and book the first frame that fits. After
    each booking, the host's idle resources are decremented; the next
    iteration on this host considers the next-priority job.

    Counts every "look at a job for this host" as a DB op so we can show
    the cost difference vs SmartScheduler.
    """

    def __init__(self):
        self.db_ops = 0  # cumulative

    def tick(self, c: Cluster, now: float) -> List[Tuple[Host, Frame]]:
        bookings: List[Tuple[Host, Frame]] = []
        layers = _dispatchable_layers(c)
        # Rank layers by priority (desc), age (asc).
        layer_jobs = [(l, _job_of(l, c)) for l in layers]
        layer_jobs.sort(key=lambda lj: (-lj[1].priority, lj[1].ts_started))

        for h in c.hosts:
            if h.cores_idle <= 0:
                continue
            # Walk the prioritized list, take the first that fits this host.
            for layer, job in layer_jobs:
                self.db_ops += 1  # per-(host, layer) lookup, the heavy join in master
                if not _tags_compatible(layer, h): continue
                if not _os_compatible(layer, h):   continue
                if not _layer_fits_on_host_idle(layer, h): continue
                if job.cores_in_use + layer.cores_min > job.max_cores: continue
                show = c.show(layer.show_id)
                if show.cores_in_use + layer.cores_min > show.burst_cores: continue

                # Pick the next WAITING frame in dispatch order.
                next_frames = layer.next_waiting_frames(limit=1)
                if not next_frames:
                    continue
                frame = next_frames[0]
                bookings.append((h, frame))
                # Decrement in-memory so the next host iteration sees the state.
                h.cores_idle      -= layer.cores_min
                h.mem_idle_kb     -= layer.mem_min_kb
                h.gpus_idle       -= layer.gpus_min
                h.gpu_mem_idle_kb -= layer.gpu_mem_min_kb
                job.cores_in_use      += layer.cores_min
                show.cores_in_use     += layer.cores_min
                # Don't break: a host can take more frames if anything else fits.
                if h.cores_idle <= 0:
                    break

        return bookings


# ============================================================================
# Smart: group + score + reserve (Python port of Scheduler.java)
# ============================================================================

# Stranding-score weights (mirror Scheduler.java).
W_CORES = 1.0
W_MEM = 1.0
W_GPUS = 4.0
W_GPU_MEM = 1.0


@dataclass(frozen=True)
class HostSpecKey:
    alloc: str
    tags_normalized: str
    os: Optional[str]
    has_gpu: bool


def _spec_key(h: Host) -> HostSpecKey:
    tags_norm = " ".join(sorted(h.tags))
    return HostSpecKey(
        alloc=h.alloc,
        tags_normalized=tags_norm,
        os=h.os,
        has_gpu=(h.gpus_total > 0 or h.gpu_mem_total_kb > 0),
    )


def _group_by_spec(hosts: List[Host]) -> Dict[HostSpecKey, List[Host]]:
    groups: Dict[HostSpecKey, List[Host]] = {}
    for h in hosts:
        groups.setdefault(_spec_key(h), []).append(h)
    return groups


def _compute_max_more(h: Host, l: Layer, job: Job, show) -> int:
    """How many ADDITIONAL frames of l fit on h after the first placement,
    respecting physical, job_max_cores, and show_burst caps. Mirrors
    Scheduler.computeMaxMore."""
    rem_cores  = h.cores_idle    - l.cores_min
    rem_mem    = h.mem_idle_kb   - l.mem_min_kb
    rem_gpus   = h.gpus_idle     - l.gpus_min
    rem_gpum   = h.gpu_mem_idle_kb - l.gpu_mem_min_kb

    big = 10**18
    max_more = big
    if l.cores_min > 0:
        max_more = min(max_more, rem_cores // l.cores_min)
        job_rem = job.max_cores - job.cores_in_use - l.cores_min
        if job_rem < 0: job_rem = 0
        max_more = min(max_more, job_rem // l.cores_min)
        show_rem = show.burst_cores - show.cores_in_use - l.cores_min
        if show_rem < 0: show_rem = 0
        max_more = min(max_more, show_rem // l.cores_min)
    if l.mem_min_kb > 0:
        max_more = min(max_more, rem_mem // l.mem_min_kb)
    if l.gpus_min > 0:
        max_more = min(max_more, rem_gpus // l.gpus_min)
    if l.gpu_mem_min_kb > 0:
        max_more = min(max_more, rem_gpum // l.gpu_mem_min_kb)

    if max_more >= big:
        max_more = 0
    return max(0, max_more)


def _placement_score(h: Host, l: Layer, job: Job, show) -> float:
    """Stranding-based score; lower is better. Mirrors Scheduler.placementScore."""
    max_more = _compute_max_more(h, l, job, show)
    rem_cores = h.cores_idle    - l.cores_min
    rem_mem   = h.mem_idle_kb   - l.mem_min_kb
    rem_gpus  = h.gpus_idle     - l.gpus_min
    rem_gpum  = h.gpu_mem_idle_kb - l.gpu_mem_min_kb

    strand_cores = (rem_cores - max_more * l.cores_min) if l.cores_min > 0 else 0
    strand_mem_gb = ((rem_mem - max_more * l.mem_min_kb) / (1024.0 * 1024.0)) if l.mem_min_kb > 0 else 0
    strand_gpus = (rem_gpus - max_more * l.gpus_min) if l.gpus_min > 0 else 0
    strand_gpum_gb = ((rem_gpum - max_more * l.gpu_mem_min_kb) / (1024.0 * 1024.0)) if l.gpu_mem_min_kb > 0 else 0

    return (W_CORES * strand_cores
          + W_MEM   * strand_mem_gb
          + W_GPUS  * strand_gpus
          + W_GPU_MEM * strand_gpum_gb)


@dataclass
class Reservation:
    layer_id: str
    priority: int


class SmartScheduler:
    """Python port of cuebot/dispatcher/Scheduler.java.

    Per tick:
      1. Group hosts by static spec.
      2. For each group, build the candidate layer list.
      3. For each candidate in priority order, dispatch-loop with
         stranding-score best-fit. Override lower-priority reservations.
      4. Reconcile each candidate's reservation count to waiting_frame_count.
      5. Sweep reservations whose layer left the dispatchable set.
    """

    def __init__(self, layer_candidates_per_group_max: int = 2000):
        self.reservations: Dict[str, Reservation] = {}   # host_id -> Reservation
        self.cap = layer_candidates_per_group_max
        self.db_ops = 0  # group-level queries

    # ---- per-tick entry ---------------------------------------------------

    def tick(self, c: Cluster, now: float) -> List[Tuple[Host, Frame]]:
        bookings: List[Tuple[Host, Frame]] = []
        all_layers = _dispatchable_layers(c)
        if not all_layers:
            self.reservations.clear()
            return bookings

        layer_jobs = [(l, _job_of(l, c)) for l in all_layers]
        groups = _group_by_spec(c.hosts)
        seen_layer_ids: Set[str] = set()

        for spec, group_hosts in groups.items():
            # One per-group candidate "query" (counted as one DB op).
            self.db_ops += 1
            candidates = self._candidates_for_group(spec, group_hosts, layer_jobs)
            if not candidates:
                continue

            for layer, job in candidates:
                seen_layer_ids.add(layer.layer_id)
                show = c.show(layer.show_id)
                if job.cores_in_use + layer.cores_min > job.max_cores:    continue
                if show.cores_in_use + layer.cores_min > show.burst_cores: continue

                # Dispatch loop with reservation-aware best-fit.
                while True:
                    best = self._best_fit(layer, group_hosts, job, show)
                    if best is None:
                        break
                    next_frames = layer.next_waiting_frames(limit=1)
                    if not next_frames:
                        break
                    frame = next_frames[0]
                    bookings.append((best, frame))

                    best.cores_idle      -= layer.cores_min
                    best.mem_idle_kb     -= layer.mem_min_kb
                    best.gpus_idle       -= layer.gpus_min
                    best.gpu_mem_idle_kb -= layer.gpu_mem_min_kb
                    job.cores_in_use      += layer.cores_min
                    show.cores_in_use     += layer.cores_min

                    # Override the reservation if the host had a lower-priority one.
                    existing = self.reservations.get(best.host_id)
                    if existing is not None and existing.priority < job.priority:
                        self.reservations[best.host_id] = Reservation(layer.layer_id, job.priority)

                    if job.cores_in_use + layer.cores_min > job.max_cores:    break
                    if show.cores_in_use + layer.cores_min > show.burst_cores: break

                # Reconcile reservations for this layer to its remaining
                # WAITING (unfittable) frame count.
                self._reconcile(layer, group_hosts, job, show)

        # Orphan sweep.
        self.reservations = {
            host_id: r for host_id, r in self.reservations.items()
            if r.layer_id in seen_layer_ids
        }
        return bookings

    # ---- per-group candidates (mirrors SELECT_CANDIDATES_FOR_GROUP) -------

    def _candidates_for_group(self, spec: HostSpecKey, hosts: List[Host],
                               layer_jobs: List[Tuple[Layer, Job]]
                               ) -> List[Tuple[Layer, Job]]:
        # Filter to layers compatible with this group's static spec.
        max_total_cores = max((h.cores_total for h in hosts), default=0)
        out: List[Tuple[Layer, Job]] = []
        # All hosts in a group share tags/os; pick any to test tag compat.
        proto = hosts[0]
        for l, j in layer_jobs:
            if not _tags_compatible(l, proto):           continue
            if not _os_compatible(l, proto):             continue
            if l.cores_min > max_total_cores:            continue
            out.append((l, j))
        # Sort by priority desc, ts_started asc.
        out.sort(key=lambda lj: (-lj[1].priority, lj[1].ts_started))
        return out[:self.cap]

    # ---- best-fit + reservation rule --------------------------------------

    def _reservation_allows(self, h: Host, layer: Layer, priority: int) -> bool:
        r = self.reservations.get(h.host_id)
        return (r is None
                or r.layer_id == layer.layer_id
                or r.priority < priority)

    def _best_fit(self, layer: Layer, hosts: List[Host], job: Job, show) -> Optional[Host]:
        best: Optional[Host] = None
        best_score = float("inf")
        for h in hosts:
            if not self._reservation_allows(h, layer, job.priority): continue
            if not _layer_fits_on_host_idle(layer, h):               continue
            s = _placement_score(h, layer, job, show)
            if s < best_score:
                best_score = s
                best = h
        return best

    # ---- reconcile reservations ------------------------------------------

    def _reconcile(self, layer: Layer, hosts: List[Host], job: Job, show):
        mine = [host_id for host_id, r in self.reservations.items()
                if r.layer_id == layer.layer_id]
        need = layer.waiting_frame_count()
        have = len(mine)
        if have > need:
            for host_id in mine[need:]:
                self.reservations.pop(host_id, None)
        elif have < need:
            want = need - have
            for _ in range(want):
                target = self._pick_target(layer, hosts, job)
                if target is None:
                    break
                self.reservations[target.host_id] = Reservation(layer.layer_id, job.priority)

    def _pick_target(self, layer: Layer, hosts: List[Host], job: Job) -> Optional[Host]:
        best: Optional[Host] = None
        best_procs = 10**9
        for h in hosts:
            if not _layer_fits_on_host_total(layer, h):              continue
            if not self._reservation_allows(h, layer, job.priority): continue
            if h.running_procs() < best_procs:
                best_procs = h.running_procs()
                best = h
        return best
