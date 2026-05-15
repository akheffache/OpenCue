// SPDX-License-Identifier: Apache-2.0
//
// CLI entry point. Runs two schedulers side-by-side in parallel threads,
// prints a comparison report, optionally writes per-tick CSVs. Choose
// which two via --left and --right (any of legacy, rust, smart).

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
    std::string left               = "legacy";  // legacy | rust | smart
    std::string right              = "smart";
    std::string left_csv;
    std::string right_csv;
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
        "  --silos                  3-alloc setup: small/mid/big; Legacy/Rust enforce\n"
        "  --smart-no-silos         in silos mode, let Smart ignore alloc routing\n"
        "  --left NAME              left column scheduler (legacy|rust|smart) [legacy]\n"
        "  --right NAME             right column scheduler (legacy|rust|smart) [smart]\n"
        "  --left-csv PATH          write per-tick left-scheduler metrics\n"
        "  --right-csv PATH         write per-tick right-scheduler metrics\n"
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
        // back-compat: --rust swaps Legacy for Rust as left column
        else if (k == "--rust")               { a.left = "rust"; }
        else if (k == "--left")               { if (!next_s(a.left)) return false; }
        else if (k == "--right")              { if (!next_s(a.right)) return false; }
        else if (k == "--legacy-csv")         { if (!next_s(a.left_csv)) return false; }
        else if (k == "--smart-csv")          { if (!next_s(a.right_csv)) return false; }
        else if (k == "--left-csv")           { if (!next_s(a.left_csv)) return false; }
        else if (k == "--right-csv")          { if (!next_s(a.right_csv)) return false; }
        else if (k == "--help" || k == "-h")  { usage(argv[0]); std::exit(0); }
        else {
            std::fprintf(stderr, "unknown option: %s\n", k.c_str());
            usage(argv[0]);
            return false;
        }
    }
    auto valid = [](const std::string& s) {
        return s == "legacy" || s == "rust" || s == "smart";
    };
    if (!valid(a.left) || !valid(a.right)) {
        std::fprintf(stderr, "--left/--right must be one of: legacy, rust, smart\n");
        return false;
    }
    return true;
}

struct RunResult {
    std::vector<TickMetrics>                    metrics;
    std::unordered_map<std::string, WaitRecord> waits;
};

static void run_named(const std::string& name, const Args& a,
                       Cluster&& cluster, std::vector<Job>&& arrivals,
                       double sim_seconds, RunResult& out) {
    if (name == "legacy") {
        Simulator<LegacyScheduler> sim(std::move(cluster), std::move(arrivals),
                                        LegacyScheduler{}, a.tick_seconds, a.seed + 7);
        sim.run(sim_seconds);
        out.metrics = std::move(sim.metrics);
        out.waits   = std::move(sim.wait_records);
    } else if (name == "rust") {
        Simulator<RustScheduler> sim(std::move(cluster), std::move(arrivals),
                                      RustScheduler{}, a.tick_seconds, a.seed + 7);
        sim.run(sim_seconds);
        out.metrics = std::move(sim.metrics);
        out.waits   = std::move(sim.wait_records);
    } else {  // smart
        Simulator<SmartScheduler> sim(std::move(cluster), std::move(arrivals),
                                       SmartScheduler{}, a.tick_seconds, a.seed + 7);
        if (a.silos && a.smart_ignore_allocs) sim.scheduler.ignore_allocs = true;
        sim.run(sim_seconds);
        out.metrics = std::move(sim.metrics);
        out.waits   = std::move(sim.wait_records);
    }
}

static const char* display_name(const std::string& n) {
    if (n == "legacy") return "Legacy";
    if (n == "rust")   return "Rust";
    return "Smart";
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
    Cluster cluster_r = build_production_cluster(a.seed, a.scale, a.silos);
    auto    arrivals = generate_arrivals(cfg, cluster_l.total_cores(),
                                          a.seed + 1, a.silos);

    std::printf("hosts=%zu  total_cores=%lld  jobs=%zu  sim=%.2fh  silos=%s%s\n",
                cluster_l.hosts.size(),
                (long long)cluster_l.total_cores(),
                arrivals.size(),
                a.hours,
                a.silos ? "yes" : "no",
                a.silos && a.smart_ignore_allocs && (a.left == "smart" || a.right == "smart")
                    ? " (Smart ignores allocs)" : "");
    std::printf("comparing: %s vs %s\n", display_name(a.left), display_name(a.right));

    auto t_start = std::chrono::steady_clock::now();

    auto arrivals_l = arrivals;
    auto arrivals_r = std::move(arrivals);

    RunResult res_l, res_r;
    auto fut_l = std::async(std::launch::async, [&] {
        run_named(a.left, a, std::move(cluster_l), std::move(arrivals_l),
                   cfg.simulation_seconds, res_l);
    });
    auto fut_r = std::async(std::launch::async, [&] {
        run_named(a.right, a, std::move(cluster_r), std::move(arrivals_r),
                   cfg.simulation_seconds, res_r);
    });
    fut_l.get();
    fut_r.get();

    auto t_end = std::chrono::steady_clock::now();
    double wall_s = std::chrono::duration<double>(t_end - t_start).count();
    std::printf("wall time: %.2fs (both schedulers in parallel)\n", wall_s);

    auto agg_l = aggregate(res_l.metrics);
    auto agg_r = aggregate(res_r.metrics);

    std::vector<WaitRecord> waits_l, waits_r;
    waits_l.reserve(res_l.waits.size());
    waits_r.reserve(res_r.waits.size());
    for (auto& [k, v] : res_l.waits) waits_l.push_back(v);
    for (auto& [k, v] : res_r.waits) waits_r.push_back(v);

    double sim_end = cfg.simulation_seconds;
    auto w_l  = wait_stats(waits_l, sim_end, 0);
    auto w_r  = wait_stats(waits_r, sim_end, 0);
    auto ww_l = wait_stats(waits_l, sim_end, 16);
    auto ww_r = wait_stats(waits_r, sim_end, 16);
    print_comparison(display_name(a.left), display_name(a.right),
                      agg_l, agg_r, w_l, w_r, ww_l, ww_r);

    if (!a.left_csv.empty()) {
        write_csv(a.left_csv, res_l.metrics);
        std::printf("%s per-tick: %s\n", display_name(a.left), a.left_csv.c_str());
    }
    if (!a.right_csv.empty()) {
        write_csv(a.right_csv, res_r.metrics);
        std::printf("%s per-tick: %s\n", display_name(a.right), a.right_csv.c_str());
    }
    return 0;
}
