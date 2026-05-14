"""
Workload generator with production distributions.

Constants are taken from benchmarks/fragmentation/ on
claude/redis-optimization-continued-ihiUy, which were measured against a
real production render farm. The numbers are what give the simulator
its credibility.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from typing import Dict, List

from cluster import (
    Cluster,
    Frame,
    Host,
    Job,
    Layer,
    Show,
    STATE_WAITING,
)


# ---- production farm: 1,553 hosts, 57,248 cores -----------------------------

HOSTS_CONFIG = [
    # (name_prefix, count, cores, memory_gb, tags)
    ("elk",   1004,  16, 125, {"general", "render"}),
    ("ram",    303,  32, 251, {"general", "render", "midrange"}),
    ("jaime",  246, 128, 503, {"general", "render", "highend"}),
]

# ---- frame size distribution (power-of-2 cores) -----------------------------
#
# % shares from the production trace. Memory is the per-frame requirement.
# Tags are layer-level requirements (an "OR" against host tags); a layer
# tagged "midrange|highend" runs only on hosts that advertise either.

FRAME_TYPES = [
    # (cores, percent, mem_gb, layer_tags_set)
    ( 1, 22.89,   0.52, {"general"}),
    ( 2, 25.54,   1.38, {"general"}),
    ( 4, 34.22,   6.44, {"render"}),
    ( 8, 14.55,  27.27, {"render"}),
    (16,  1.88,  63.85, {"render"}),
    (32,  0.28, 109.00, {"midrange", "highend"}),
    (64,  0.03, 233.12, {"highend"}),
]

PRIORITIES = [10, 30, 50, 70, 90]

GB_KB = 1024 * 1024
KB_PER_GB = float(GB_KB)


def build_production_cluster(seed: int = 0, scale: float = 1.0) -> Cluster:
    """Build the cluster with the production host distribution.

    scale: 0.0 < scale <= 1.0. Multiplies each host-type count, rounded.
    Use scale=0.1 to get a 1/10th cluster for fast iteration.
    """
    rng = random.Random(seed)
    hosts: List[Host] = []
    next_id = 1
    for prefix, count, cores, mem_gb, tags in HOSTS_CONFIG:
        scaled_count = max(1, int(round(count * scale)))
        for i in range(scaled_count):
            hosts.append(Host(
                host_id=f"host-{next_id:05d}",
                name=f"{prefix}-{i:04d}",
                alloc="A1",
                cores_total=cores,
                mem_total_kb=int(mem_gb * GB_KB),
                gpus_total=0,
                gpu_mem_total_kb=0,
                tags=set(tags),
                os="rhel7",
            ))
            next_id += 1
    shows = [
        Show(show_id="benchmark", alloc="A1", burst_cores=10_000_000),
    ]
    return Cluster(hosts=hosts, shows=shows)


# ---- workload generation ----------------------------------------------------

@dataclass
class WorkloadConfig:
    """
    Parameters for the workload generator.

      target_load_fraction: target cluster load as a fraction of total cores.
      jobs_per_burst:       how many jobs to submit at each arrival event.
      arrival_interval_s:   simulated seconds between bursts.
      simulation_seconds:   total simulated duration.
      runtime_median_s:     median per-frame runtime (lognormal).
      runtime_sigma:        lognormal sigma; higher = wider tail.
    """
    target_load_fraction: float = 0.7
    jobs_per_burst: int = 5
    arrival_interval_s: float = 60.0
    simulation_seconds: float = 4 * 3600  # 4 hours
    runtime_median_s: float = 600.0       # 10 minutes
    runtime_sigma: float = 0.9            # moderate spread


def _pick_frame_type(rng: random.Random) -> dict:
    r = rng.uniform(0, 100)
    acc = 0.0
    for cores, pct, mem_gb, tags in FRAME_TYPES:
        acc += pct
        if r <= acc:
            return {"cores": cores, "mem_gb": mem_gb, "tags": tags}
    return {"cores": FRAME_TYPES[0][0], "mem_gb": FRAME_TYPES[0][2],
            "tags": FRAME_TYPES[0][3]}


def generate_job(rng: random.Random, ts: float, target_cores: int,
                 runtime_median_s: float, runtime_sigma: float) -> Job:
    """Generate one job whose layers together demand roughly target_cores."""
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    show_id = "benchmark"
    priority = rng.choice(PRIORITIES)

    # Pick one layer type per job (simpler than mixed-layer jobs and matches
    # how most render-farm jobs look: one shape repeated across N frames).
    spec = _pick_frame_type(rng)
    n_frames = max(1, target_cores // max(1, spec["cores"]))
    layer_id = f"{job_id}-layer-0"

    layer = Layer(
        layer_id=layer_id,
        job_id=job_id,
        show_id=show_id,
        cores_min=spec["cores"],
        mem_min_kb=int(spec["mem_gb"] * GB_KB),
        gpus_min=0,
        gpu_mem_min_kb=0,
        tags=set(spec["tags"]),
        os=None,
        runtime_median_s=runtime_median_s,
        runtime_sigma=runtime_sigma,
    )
    for i in range(n_frames):
        layer.frames.append(Frame(
            frame_id=f"{layer_id}-frame-{i:04d}",
            layer_id=layer_id,
            job_id=job_id,
            int_number=i,
            state=STATE_WAITING,
        ))

    return Job(
        job_id=job_id,
        show_id=show_id,
        priority=priority,
        ts_started=ts,
        layers=[layer],
    )


def generate_arrivals(cfg: WorkloadConfig, total_cluster_cores: int,
                      seed: int = 1) -> List[Job]:
    """
    Generate the sequence of jobs that arrive over the simulation. Each
    arrival burst contributes roughly cfg.target_load_fraction *
    total_cluster_cores worth of work, spread across cfg.jobs_per_burst jobs.
    """
    rng = random.Random(seed)
    jobs: List[Job] = []
    target_total_cores_per_burst = int(cfg.target_load_fraction * total_cluster_cores
                                        * cfg.arrival_interval_s / cfg.runtime_median_s)
    cores_per_job = max(1, target_total_cores_per_burst // max(1, cfg.jobs_per_burst))

    t = 0.0
    while t < cfg.simulation_seconds:
        for _ in range(cfg.jobs_per_burst):
            jobs.append(generate_job(
                rng, t, cores_per_job,
                cfg.runtime_median_s, cfg.runtime_sigma,
            ))
        t += cfg.arrival_interval_s
    return jobs


# ---- runtime sampling -------------------------------------------------------

def sample_frame_runtime(rng: random.Random, layer: Layer) -> float:
    """Sample a frame runtime in seconds from a lognormal distribution."""
    import math
    mu = math.log(layer.runtime_median_s)
    return float(rng.lognormvariate(mu, layer.runtime_sigma))
