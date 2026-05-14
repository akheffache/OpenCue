// SPDX-License-Identifier: Apache-2.0
//
// CLI entry point. Runs the Legacy and Smart simulators in parallel threads,
// prints a side-by-side report, optionally writes per-tick CSVs.

#include "cluster.hpp"
#include "workload.hpp"
#include "schedulers.hpp"
#include "simulator.hpp"
#include "report.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <vector>

using namespace sim;

struct Args {
    double      hours              = 2.0;
    double      load               = 0.7;
    int         burst              = 5;
    double      arrival_interval_s = 60.0;
    double      runtime_median     = 600.0;
    int         seed               = 0;
    double      tick_seconds       = 1.0;
    double      scale              = 1.0;
    bool        silos              = false;
    bool        smart_ignore_allocs = false;
    bool        use_rust           = false;
    std::string legacy_csv;
    std::string smart_csv;
};

static void usage(const char* argv0) {
    std::printf(
        "Usage: %s [options]\n"
        "  --hours F                simulated duration in hours [2.0]\n"
        "  --load F                 target load fraction [0.7]\n"
        "  --burst N                jobs per arrival burst [5]\n"
        "  --arrival-interval F     seconds between bursts [60]\n"
        "  --runtime-median F       median per-frame runtime, seconds [600]\n"
        "  --seed N                 RNG seed [0]\n"
        "  --tick-seconds F         simulator tick size [1.0]\n"
        "  --scale F                cluster scale (0.1 = 1/10 hosts) [1.0]\n"
        "  --silos                  3-alloc setup: small/mid/big; Legacy enforces\n"
        "  --smart-no-silos         in silos mode, let Smart ignore alloc routing\n"
        "  --rust                   compare Smart against Rust scheduler instead of Legacy\n"
        "  --legacy-csv PATH        write per-tick Legacy metrics to CSV\n"
        "  --smart-csv PATH         write per-tick Smart metrics to CSV\n"
        "  --help                   show this help\n",
        argv0);
}

static bool parse_args(int argc, char** argv, Args& a) {
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        auto next = [&](double& v) {
            if (i + 1 >= argc) return false;
            v = std::strtod(argv[++i], nullptr);
            return true;
        };
        auto next_i = [&](int& v) {
            if (i + 1 >= argc) return false;
            v = std::atoi(argv[++i]);
            return true;
        };
        auto next_s = [&](std::string& v) {
            if (i + 1 >= argc) return false;
            v = argv[++i];
            return true;
        };
        if      (k == "--hours")              { if (!next(a.hours)) return false; }
        else if (k == "--load")               { if (!next(a.load)) return false; }
        else if (k == "--burst")              { if (!next_i(a.burst)) return false; }
        else if (k == "--arrival-interval")   { if (!next(a.arrival_interval_s)) return false; }
        else if (k == "--runtime-median")     { if (!next(a.runtime_median)) return false; }
        else if (k == "--seed")               { if (!next_i(a.seed)) return false; }
        else if (k == "--tick-seconds")       { if (!next(a.tick_seconds)) return false; }
        else if (k == "--scale")              { if (!next(a.scale)) return false; }
        else if (k == "--silos")              { a.silos = true; }
        else if (k == "--smart-no-silos")     { a.smart_ignore_allocs = true; }
        else if (k == "--rust")               { a.use_rust = true; }
        else if (k == "--legacy-csv")         { if (!next_s(a.legacy_csv)) return false; }
        else if (k == "--smart-csv")          { if (!next_s(a.smart_csv)) return false; }
        else if (k == "--help" || k == "-h")  { usage(argv[0]); std::exit(0); }
        else {
            std::fprintf(stderr, "unknown option: %s\n", k.c_str());
            usage(argv[0]);
            return false;
        }
    }
    return true;
}

int main(int argc, char** argv) {
    Args a;
    if (!parse_args(argc, argv, a)) return 1;

    WorkloadConfig cfg;
    cfg.target_load_fraction = a.load;
    cfg.jobs_per_burst       = a.burst;
    cfg.arrival_interval_s   = a.arrival_interval_s;
    cfg.simulation_seconds   = a.hours * 3600.0;
    cfg.runtime_median_s     = a.runtime_median;

    Cluster cluster_l = build_production_cluster(a.seed, a.scale, a.silos);
    Cluster cluster_s = build_production_cluster(a.seed, a.scale, a.silos);
    auto    arrivals = generate_arrivals(cfg, cluster_l.total_cores(),
                                          a.seed + 1, a.silos);

    std::printf("hosts=%zu  total_cores=%lld  jobs=%zu  sim=%.2fh  silos=%s%s\n",
                cluster_l.hosts.size(),
                (long long)cluster_l.total_cores(),
                arrivals.size(),
                a.hours,
                a.silos ? "yes" : "no",
                a.silos && a.smart_ignore_allocs ? " (Smart ignores allocs)" : "");

    auto t_start = std::chrono::steady_clock::now();

    // Baseline thread runs either Legacy or Rust; Smart runs in parallel.
    // Each thread owns its own cluster, so no contention.
    auto arrivals_copy = arrivals;

    std::vector<TickMetrics>                    metrics_b;
    std::unordered_map<std::string, WaitRecord> waits_b;
    const char* baseline_name = a.use_rust ? "Rust" : "Legacy";

    Simulator<SmartScheduler> sim_s(std::move(cluster_s), std::move(arrivals),
                                     SmartScheduler{},
                                     a.tick_seconds, a.seed + 7);
    if (a.silos && a.smart_ignore_allocs) sim_s.scheduler.ignore_allocs = true;

    auto run_baseline = [&]() {
        if (a.use_rust) {
            Simulator<RustScheduler> sim_b(std::move(cluster_l), std::move(arrivals_copy),
                                            RustScheduler{},
                                            a.tick_seconds, a.seed + 7);
            sim_b.run(cfg.simulation_seconds);
            metrics_b = std::move(sim_b.metrics);
            waits_b   = std::move(sim_b.wait_records);
        } else {
            Simulator<LegacyScheduler> sim_b(std::move(cluster_l), std::move(arrivals_copy),
                                              LegacyScheduler{},
                                              a.tick_seconds, a.seed + 7);
            sim_b.run(cfg.simulation_seconds);
            metrics_b = std::move(sim_b.metrics);
            waits_b   = std::move(sim_b.wait_records);
        }
    };

    auto fut_b = std::async(std::launch::async, run_baseline);
    auto fut_s = std::async(std::launch::async, [&] { sim_s.run(cfg.simulation_seconds); });
    fut_b.get();
    fut_s.get();

    auto t_end = std::chrono::steady_clock::now();
    double wall_s = std::chrono::duration<double>(t_end - t_start).count();
    std::printf("wall time: %.2fs (both schedulers in parallel)\n", wall_s);

    auto agg_l = aggregate(metrics_b);
    auto agg_s = aggregate(sim_s.metrics);

    std::vector<WaitRecord> waits_l, waits_s;
    waits_l.reserve(waits_b.size());
    waits_s.reserve(sim_s.wait_records.size());
    for (auto& [k, v] : waits_b)             waits_l.push_back(v);
    for (auto& [k, v] : sim_s.wait_records)  waits_s.push_back(v);

    double sim_end = cfg.simulation_seconds;
    auto w_l  = wait_stats(waits_l, sim_end, 0);
    auto w_s  = wait_stats(waits_s, sim_end, 0);
    auto ww_l = wait_stats(waits_l, sim_end, 16);
    auto ww_s = wait_stats(waits_s, sim_end, 16);
    print_comparison(baseline_name, agg_l, agg_s, w_l, w_s, ww_l, ww_s);

    if (!a.legacy_csv.empty()) {
        write_csv(a.legacy_csv, metrics_b);
        std::printf("%s per-tick: %s\n", baseline_name, a.legacy_csv.c_str());
    }
    if (!a.smart_csv.empty()) {
        write_csv(a.smart_csv, sim_s.metrics);
        std::printf("Smart  per-tick: %s\n", a.smart_csv.c_str());
    }
    return 0;
}
