# Offline Scheduler Simulator

Pure-Python discrete-time simulator for the OpenCue scheduler. Runs
in-process — no Cuebot, no database, no gRPC, no Docker.

The point: A/B-test scheduling algorithms on a realistic workload in
seconds, without spinning up infrastructure. Used to validate the
SCHEDULER branch's algorithm against the legacy dispatcher behavior,
and to quickly tune parameters (commit pool size, candidate caps,
weight tunings) without running a real Cuebot.

For production validation, use `benchmarks/fragmentation/` instead —
that one drives a real Cuebot via gRPC.

## Files

| File             | Role                                                    |
|------------------|---------------------------------------------------------|
| `cluster.py`     | Data model: Host, Frame, Layer, Job, Show, Cluster.     |
| `workload.py`    | Production farm + workload distributions.               |
| `schedulers.py`  | `LegacyScheduler` (per-host first-fit) and              |
|                  | `SmartScheduler` (port of `Scheduler.java`).            |
| `simulator.py`   | Discrete-time event loop, frame completion, metrics.    |
| `run.py`         | CLI: runs both schedulers, prints comparison.           |

## Production workload (baked in)

Farm: 1,553 hosts / 57,248 cores

| Type  | Count | Cores | Memory  | Tags                                |
|-------|------:|------:|--------:|-------------------------------------|
| elk   | 1,004 |    16 |  125 GB | general, render                     |
| ram   |   303 |    32 |  251 GB | general, render, midrange           |
| jaime |   246 |   128 |  503 GB | general, render, highend            |

Frame mix (per-frame demand):

| Cores | % of frames | Memory   | Layer-tag req     |
|------:|------------:|---------:|-------------------|
|    1  |   22.89%    |   0.52 GB | general          |
|    2  |   25.54%    |   1.38 GB | general          |
|    4  |   34.22%    |   6.44 GB | render           |
|    8  |   14.55%    |  27.27 GB | render           |
|   16  |    1.88%    |  63.85 GB | render           |
|   32  |    0.28%    | 109.00 GB | midrange/highend |
|   64  |    0.03%    | 233.12 GB | highend          |

Priorities: 10, 30, 50, 70, 90.

These numbers are ported from `benchmarks/fragmentation/` on
`claude/redis-optimization-continued-ihiUy`, which were measured against
a real production farm.

## Usage

```bash
cd benchmarks/offline
python3 run.py                                          # 2 simulated hours, default load
python3 run.py --hours 4 --load 0.8 --burst 10          # heavier sim
python3 run.py --legacy-csv legacy.csv --smart-csv smart.csv
```

Output is a side-by-side table:

```
================================================================================
METRIC                                  Legacy         Smart
================================================================================

--- utilization & fragmentation ---
  avg utilization                       54.3%         84.1%  (+29.8%)
  peak utilization                      71.0%         98.4%  (+27.4%)
  avg fragmentation                     38.1%         11.6%  (-26.5%)
  peak fragmentation                    52.0%         18.9%  (-33.1%)

--- throughput ---
  total bookings                        12384         14021  (+1637)

--- DB query load (proxy: scheduler ops) ---
  scheduler 'db ops'                  9,420,800       16,800  (-...)
  reduction factor                  560.8x

--- per-layer wait time (first dispatch after submission) ---
  ...

--- wide-layer wait time (>=16 cores) ---
  ...
================================================================================
```

CSV columns: `t, cores_total, cores_busy, cores_idle, frames_running,
frames_waiting, bookings_this_tick, fragmented_cores, db_ops_cumulative`.
Suitable for plotting (matplotlib, R, Excel, whatever).

## What the simulator does NOT model

- gRPC latency to RQD (treated as zero).
- Cuebot's `findNextDispatchFrames` SQL cost (modeled as instant).
- DB transaction overhead.
- Cross-Cuebot leader election (one planner instance).
- The 300 ms `HealthyThreadPool` sleep (Legacy is the *idealized*
  single-threaded version; a real master Cuebot would do worse).
- Frame-completion-driven re-dispatch from `HostReportHandler`.

Trade-off: the Legacy numbers represent the *theoretical best* for the
master algorithm. Real master is slower than what's shown here. The
delta between Legacy and Smart in the output is therefore a *lower
bound* on the real-world improvement.

## What the simulator DOES model

- Per-frame fit (cores, memory, GPUs).
- Layer tag matching against host tag sets.
- Job `int_max_cores` and show subscription `int_burst` caps.
- Frame runtime sampling (lognormal; median configurable).
- Frame completion freeing host resources.
- Stranding-based placement score (multi-resource E-PVM).
- Reservations with priority-keyed override.
- Per-host running-procs counter (used by reservation target picker).

## Extending

Workload changes: `workload.py` `HOSTS_CONFIG`, `FRAME_TYPES`, or pass
different `WorkloadConfig` values via `--load`, `--burst`, etc.

Algorithm changes: edit `schedulers.py`. Both schedulers expose
`tick(cluster, now) -> List[(host, frame)]`; the simulator handles the
rest.

To run only one scheduler, comment out the other run in `run.py`.
