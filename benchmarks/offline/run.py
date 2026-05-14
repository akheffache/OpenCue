#!/usr/bin/env python3
"""
Run the offline simulator with both schedulers (Legacy and Smart) on
identical workloads, then print a side-by-side comparison.

Usage:
    python3 run.py [--hours 4] [--load 0.7] [--burst 5] [--seed 0]
                   [--legacy-csv legacy.csv] [--smart-csv smart.csv]

The CSVs contain per-tick metrics suitable for plotting in a notebook.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from copy import deepcopy
from typing import List, Tuple

# Local imports (run.py is invoked from this directory).
from cluster import Cluster
from schedulers import LegacyScheduler, SmartScheduler
from simulator import Simulator, TickMetrics, WaitRecord
from workload import (
    HOSTS_CONFIG,
    WorkloadConfig,
    build_production_cluster,
    generate_arrivals,
)


# ---- aggregate stats -------------------------------------------------------

def aggregate(metrics: List[TickMetrics]) -> dict:
    if not metrics:
        return {}
    cores_total = max(m.cores_total for m in metrics)
    util = [(m.cores_busy / m.cores_total) if m.cores_total else 0 for m in metrics]
    frag = [(m.fragmented_cores / m.cores_total) if m.cores_total else 0
            for m in metrics]
    bookings = sum(m.bookings_this_tick for m in metrics)
    final_db_ops = metrics[-1].db_ops_cumulative
    return {
        "ticks": len(metrics),
        "cores_total": cores_total,
        "avg_util_pct": 100.0 * statistics.mean(util),
        "peak_util_pct": 100.0 * max(util),
        "avg_frag_pct": 100.0 * statistics.mean(frag),
        "peak_frag_pct": 100.0 * max(frag),
        "total_bookings": bookings,
        "db_ops": final_db_ops,
    }


def wait_quantiles(waits: List[WaitRecord]) -> dict:
    # Wait time per layer: from first_seen_at to first_dispatched_at.
    durations = [r.first_dispatched_at - r.first_seen_at
                 for r in waits if r.first_dispatched_at >= 0]
    if not durations:
        return {"layers_dispatched": 0, "layers_never_dispatched": len(waits)}
    durations.sort()
    return {
        "layers_dispatched": len(durations),
        "layers_never_dispatched": sum(1 for r in waits if r.first_dispatched_at < 0),
        "p50_s": durations[len(durations) // 2],
        "p95_s": durations[int(0.95 * (len(durations) - 1))],
        "p99_s": durations[int(0.99 * (len(durations) - 1))],
    }


def wide_layer_wait_quantiles(waits: List[WaitRecord],
                              wide_cores_threshold: int = 16) -> dict:
    wide = [r for r in waits if r.cores_min >= wide_cores_threshold
            and r.first_dispatched_at >= 0]
    if not wide:
        return {"wide_layers_dispatched": 0}
    durations = sorted(r.first_dispatched_at - r.first_seen_at for r in wide)
    return {
        "wide_layers_dispatched": len(durations),
        "wide_p50_s": durations[len(durations) // 2],
        "wide_p95_s": durations[int(0.95 * (len(durations) - 1))],
        "wide_p99_s": durations[int(0.99 * (len(durations) - 1))],
    }


# ---- csv ------------------------------------------------------------------

def write_metrics_csv(path: str, metrics: List[TickMetrics]) -> None:
    with open(path, "w", newline="") as fp:
        w = csv.writer(fp)
        w.writerow([
            "t", "cores_total", "cores_busy", "cores_idle",
            "frames_running", "frames_waiting",
            "bookings_this_tick", "fragmented_cores", "db_ops_cumulative",
        ])
        for m in metrics:
            w.writerow([m.t, m.cores_total, m.cores_busy, m.cores_idle,
                        m.frames_running, m.frames_waiting,
                        m.bookings_this_tick, m.fragmented_cores,
                        m.db_ops_cumulative])


# ---- comparison report ----------------------------------------------------

def print_comparison(legacy: dict, smart: dict,
                     legacy_waits: dict, smart_waits: dict,
                     legacy_wide: dict, smart_wide: dict) -> None:
    def row(name: str, a, b, unit: str = "", fmt: str = ".1f"):
        if a is None or b is None:
            print(f"  {name:<32} {str(a):>12}  {str(b):>12}")
            return
        af = f"{a:{fmt}}{unit}"
        bf = f"{b:{fmt}}{unit}"
        # Indicator
        delta_str = ""
        try:
            delta = b - a
            if abs(delta) > 1e-6:
                sign = "+" if delta > 0 else ""
                delta_str = f" ({sign}{delta:{fmt}}{unit})"
        except TypeError:
            pass
        print(f"  {name:<32} {af:>12}  {bf:>12}{delta_str}")

    print("=" * 80)
    print(f"{'METRIC':<34} {'Legacy':>12}  {'Smart':>12}")
    print("=" * 80)

    print("\n--- utilization & fragmentation ---")
    row("avg utilization",   legacy["avg_util_pct"],  smart["avg_util_pct"],  "%")
    row("peak utilization",  legacy["peak_util_pct"], smart["peak_util_pct"], "%")
    row("avg fragmentation", legacy["avg_frag_pct"],  smart["avg_frag_pct"],  "%")
    row("peak fragmentation",legacy["peak_frag_pct"], smart["peak_frag_pct"], "%")

    print("\n--- throughput ---")
    row("total bookings", legacy["total_bookings"], smart["total_bookings"], "", "d")

    print("\n--- DB query load (proxy: scheduler ops) ---")
    row("scheduler 'db ops'", legacy["db_ops"], smart["db_ops"], "", "d")
    try:
        ratio = legacy["db_ops"] / max(1, smart["db_ops"])
        print(f"  reduction factor                  {ratio:.1f}x")
    except (KeyError, ZeroDivisionError):
        pass

    print("\n--- per-layer wait time (first dispatch after submission) ---")
    for k in ["layers_dispatched", "layers_never_dispatched",
              "p50_s", "p95_s", "p99_s"]:
        a, b = legacy_waits.get(k), smart_waits.get(k)
        if a is None and b is None:
            continue
        fmt = "d" if k.endswith("dispatched") else ".0f"
        unit = "" if k.endswith("dispatched") else "s"
        row(k.replace("_", " "), a, b, unit, fmt)

    print("\n--- wide-layer wait time (>=16 cores) ---")
    for k in ["wide_layers_dispatched", "wide_p50_s", "wide_p95_s", "wide_p99_s"]:
        a, b = legacy_wide.get(k), smart_wide.get(k)
        if a is None and b is None:
            continue
        fmt = "d" if k.endswith("dispatched") else ".0f"
        unit = "" if k.endswith("dispatched") else "s"
        row(k.replace("_", " "), a, b, unit, fmt)

    print("=" * 80)


# ---- main ----------------------------------------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(description="Offline scheduling simulator.")
    p.add_argument("--hours", type=float, default=2.0,
                   help="Simulated duration in hours (default 2).")
    p.add_argument("--load", type=float, default=0.7,
                   help="Target load fraction (default 0.7).")
    p.add_argument("--burst", type=int, default=5,
                   help="Jobs per arrival burst (default 5).")
    p.add_argument("--arrival-interval", type=float, default=60.0,
                   help="Seconds between arrival bursts (default 60).")
    p.add_argument("--runtime-median", type=float, default=600.0,
                   help="Median per-frame runtime, seconds (default 600).")
    p.add_argument("--seed", type=int, default=0,
                   help="RNG seed (default 0).")
    p.add_argument("--tick-seconds", type=float, default=1.0,
                   help="Simulator tick size (default 1).")
    p.add_argument("--legacy-csv", default=None,
                   help="Where to write per-tick metrics for the Legacy run.")
    p.add_argument("--smart-csv", default=None,
                   help="Where to write per-tick metrics for the Smart run.")
    args = p.parse_args(argv)

    cfg = WorkloadConfig(
        target_load_fraction=args.load,
        jobs_per_burst=args.burst,
        arrival_interval_s=args.arrival_interval,
        simulation_seconds=args.hours * 3600.0,
        runtime_median_s=args.runtime_median,
    )

    # Build two identical cluster + workload pairs.
    cluster_legacy = build_production_cluster(seed=args.seed)
    cluster_smart  = build_production_cluster(seed=args.seed)

    arrivals_legacy = generate_arrivals(cfg, cluster_legacy.total_cores(),
                                        seed=args.seed + 1)
    arrivals_smart  = deepcopy(arrivals_legacy)

    # ---- Legacy run ------------------------------------------------------
    print(f"Running Legacy scheduler on {len(cluster_legacy.hosts):,} hosts "
          f"({cluster_legacy.total_cores():,} cores) for {args.hours}h "
          f"sim time, {len(arrivals_legacy):,} jobs arriving...",
          flush=True)
    sim_legacy = Simulator(cluster_legacy, arrivals_legacy, LegacyScheduler(),
                           tick_seconds=args.tick_seconds, runtime_seed=args.seed + 7)
    sim_legacy.run(cfg.simulation_seconds)

    # ---- Smart run -------------------------------------------------------
    print(f"Running Smart scheduler on the same workload...", flush=True)
    sim_smart = Simulator(cluster_smart, arrivals_smart, SmartScheduler(),
                          tick_seconds=args.tick_seconds, runtime_seed=args.seed + 7)
    sim_smart.run(cfg.simulation_seconds)

    # ---- compare ---------------------------------------------------------
    a_l = aggregate(sim_legacy.metrics)
    a_s = aggregate(sim_smart.metrics)
    w_l = wait_quantiles(list(sim_legacy.wait_records.values()))
    w_s = wait_quantiles(list(sim_smart.wait_records.values()))
    ww_l = wide_layer_wait_quantiles(list(sim_legacy.wait_records.values()))
    ww_s = wide_layer_wait_quantiles(list(sim_smart.wait_records.values()))
    print_comparison(a_l, a_s, w_l, w_s, ww_l, ww_s)

    if args.legacy_csv:
        write_metrics_csv(args.legacy_csv, sim_legacy.metrics)
        print(f"\nLegacy per-tick metrics: {args.legacy_csv}")
    if args.smart_csv:
        write_metrics_csv(args.smart_csv, sim_smart.metrics)
        print(f"Smart  per-tick metrics: {args.smart_csv}")


if __name__ == "__main__":
    main()
