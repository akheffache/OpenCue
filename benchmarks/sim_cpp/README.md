# C++ Offline Scheduler Simulator

Fast in-process simulator for A/B-testing the OpenCue scheduler. ~100× faster
than the Python version in `benchmarks/offline/`. Same workload, same
algorithms, same metrics — useful for iterating on parameter tunings against
full-scale (1,553-host) production farms in seconds rather than tens of minutes.

No Cuebot, no DB, no gRPC, no Docker. Pure C++17 stdlib + pthreads.

For production validation use `benchmarks/fragmentation/` (drives a real Cuebot).
For quick algorithmic experiments in a notebook environment use
`benchmarks/offline/`. For full-scale comparisons, use this.

## Files

```
cluster.hpp     Host / Frame / Layer / Job / Show / Cluster
workload.hpp    Production farm constants + workload generator
schedulers.hpp  LegacyScheduler + SmartScheduler
simulator.hpp   Discrete-time event loop (template on scheduler type)
report.hpp      Aggregation, percentiles, side-by-side comparison
main.cpp        CLI entry point; runs both schedulers in parallel threads
Makefile        one-shot build with g++ / clang++
```

## Build

```
cd benchmarks/sim_cpp
make
```

Produces a single executable `./sim`. Requires C++17 (`g++` 7+ or `clang++` 5+)
and pthreads. No external dependencies.

## Run

```bash
./sim                                       # 2-hour default sim, full scale
./sim --hours 0.5 --scale 0.3               # 30 min, 30% cluster
./sim --silos                               # 3-allocation setup, Legacy is constrained
./sim --silos --smart-no-silos              # Smart treats fleet as unified
./sim --legacy-csv legacy.csv --smart-csv smart.csv   # per-tick CSVs for plotting
```

CLI options:

```
--hours F              simulated duration in hours [2.0]
--load F               target load fraction [0.7]
--burst N              jobs per arrival burst [5]
--arrival-interval F   seconds between bursts [60]
--runtime-median F     median per-frame runtime, seconds [600]
--seed N               RNG seed [0]
--tick-seconds F       simulator tick size [1.0]
--scale F              cluster scale (0.1 = 1/10 hosts) [1.0]
--silos                3-alloc setup; Legacy enforces alloc routing
--smart-no-silos       in silos mode, let Smart treat fleet as one pool
--legacy-csv PATH      per-tick Legacy metrics
--smart-csv PATH       per-tick Smart metrics
```

## Silos

Real-world Cuebot farms are manually partitioned into multiple allocations
(elk → small, ram → mid, jaime → big) so the legacy dispatcher's
per-host first-fit doesn't fragment big hosts with small frames. `--silos`
mirrors that: hosts split across three allocs, layers tagged with their
allowed allocs by frame-size band, and LegacyScheduler enforces the routing.

`--smart-no-silos` is the value-prop: SmartScheduler doesn't *need* the
manual partitioning; you can keep the operational setup (silos in the host
fleet) and tell Smart to treat everything as one pool. The point is to show
that Smart removes the operator burden of maintaining alloc routing.

## Metrics

Output is a side-by-side table on stdout:

```
================================================================================
METRIC                            Legacy       Smart
================================================================================

--- utilization & fragmentation ---
  avg utilization
  peak utilization
  avg fragmentation
  peak fragmentation

--- throughput ---
  total bookings

--- DB query load (proxy: scheduler ops) ---
  scheduler 'db ops'
  reduction factor

--- per-layer wait (excl. never-dispatched: BIASED) ---
  layers dispatched
  layers never dispatched
  biased p50/p95/p99 wait

--- per-layer wait (incl. never-dispatched at sim_end: HONEST) ---
  p50/p95/p99 wait

--- wide-layer (>=16 cores) wait (honest) ---
  wide layers dispatched
  wide never dispatched
  wide p50/p95/p99 wait
```

Two wait-time sections: the **biased** numbers count only layers that
dispatched (legacy may "win" because it only reaches the easy ones); the
**honest** numbers treat never-dispatched layers as still-waiting at
sim end, which is the comparison that doesn't lie about throughput.

## Threading model

Each scheduler runs in its own `std::async(std::launch::async)` thread.
Cluster state is fully owned per-scheduler so there's no contention. The
elapsed wall time reported is `max(legacy_runtime, smart_runtime)`, since
the two runs are concurrent.

The SmartScheduler itself is single-threaded inside its tick — that's the
algorithm. We do NOT model the production commit pool in the simulator
(the simulator's commit is the in-memory state change, which is
instantaneous in C++). For the production timing of the commit pool, see
`benchmarks/fragmentation/`.

## What's modeled vs not

Modeled:
- Per-frame multi-resource fit (cores, mem, gpus, gpu_mem)
- Layer tag matching, OS matching, alloc routing
- Job `int_max_cores` and show `int_burst` caps
- Frame runtime sampling (lognormal)
- Frame completion freeing host resources
- Stranding-based placement score
- Priority-keyed reservations with override on dispatch
- Per-host running-procs counter

Not modeled (intentional, documented):
- gRPC latency to RQD (zero)
- findNextDispatchFrames SQL cost (instant)
- DB transaction overhead
- 300ms HealthyThreadPool sleep
- Frame-completion-driven re-dispatch from HostReportHandler

These make Legacy look better than it is in production, so the
Smart-vs-Legacy delta in the output is a **lower bound** on the real-world
improvement.
