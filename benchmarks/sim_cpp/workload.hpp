// SPDX-License-Identifier: Apache-2.0
//
// Workload generator with production-measured constants. Ported verbatim from
// benchmarks/offline/workload.py (which was in turn ported from the redis
// branch's benchmarks/fragmentation/).
//
// Two modes:
//
//   silos = false (default)
//     Single allocation "A1". Frame layers carry only their tag requirements
//     (general / render / midrange / highend). Both schedulers see the
//     unified fleet.
//
//   silos = true
//     Three allocations: "small_alloc" (elk), "mid_alloc" (ram),
//     "big_alloc" (jaime). Each layer's allowed_allocs is set based on
//     frame size, the way real-world operators manually partition their
//     farm to make the legacy dispatcher behave. LegacyScheduler enforces
//     the routing. SmartScheduler ignores it (its win).

#pragma once

#include "cluster.hpp"

#include <cmath>
#include <random>
#include <string>
#include <vector>

namespace sim {

constexpr int64_t GB_KB = static_cast<int64_t>(1024) * 1024;

// ---- production farm: 1,553 hosts, 57,248 cores ---------------------------

struct HostTypeConfig {
    std::string           prefix;
    int                   count;
    int                   cores;
    double                memory_gb;
    std::set<std::string> tags;
    std::string           silo_alloc;  // used when silos = true
};

inline const std::vector<HostTypeConfig> HOSTS_CONFIG = {
    {"elk",   1004,  16, 125.0, {"general", "render"},                "small_alloc"},
    {"ram",    303,  32, 251.0, {"general", "render", "midrange"},    "mid_alloc"},
    {"jaime",  246, 128, 503.0, {"general", "render", "highend"},     "big_alloc"},
};

// ---- frame size distribution (power-of-2 cores) ---------------------------

struct FrameTypeConfig {
    int                   cores;
    double                percent;        // % of all frames
    double                memory_gb;
    std::set<std::string> layer_tags;     // tag requirement (OR-match host tags)
    // Used only in silos = true. The allocations this frame's layer is
    // allowed to run on. Mirrors how operators tag-and-allocate manually.
    std::set<std::string> silo_allowed_allocs;
};

inline const std::vector<FrameTypeConfig> FRAME_TYPES = {
    { 1, 22.89,   0.52, {"general"},                {"small_alloc"}},
    { 2, 25.54,   1.38, {"general"},                {"small_alloc"}},
    { 4, 34.22,   6.44, {"render"},                 {"small_alloc"}},
    { 8, 14.55,  27.27, {"render"},                 {"mid_alloc"}},
    {16,  1.88,  63.85, {"render"},                 {"mid_alloc"}},
    {32,  0.28, 109.00, {"midrange", "highend"},    {"big_alloc"}},
    {64,  0.03, 233.12, {"highend"},                {"big_alloc"}},
};

inline const std::vector<int> PRIORITIES = {10, 30, 50, 70, 90};

// ---- cluster build --------------------------------------------------------

inline Cluster build_production_cluster(int seed, double scale, bool silos) {
    Cluster c;
    int next_id = 1;
    for (const auto& cfg : HOSTS_CONFIG) {
        int n = std::max(1, static_cast<int>(std::lround(cfg.count * scale)));
        for (int i = 0; i < n; ++i) {
            Host h;
            char id_buf[32];
            std::snprintf(id_buf, sizeof(id_buf), "host-%05d", next_id);
            h.host_id          = id_buf;
            char name_buf[32];
            std::snprintf(name_buf, sizeof(name_buf), "%s-%04d", cfg.prefix.c_str(), i);
            h.name             = name_buf;
            h.alloc            = silos ? cfg.silo_alloc : std::string("A1");
            h.cores_total      = cfg.cores;
            h.mem_total_kb     = static_cast<int64_t>(cfg.memory_gb * GB_KB);
            h.gpus_total       = 0;
            h.gpu_mem_total_kb = 0;
            h.tags             = cfg.tags;
            h.os               = std::string("rhel7");
            h.cores_idle       = h.cores_total;
            h.mem_idle_kb      = h.mem_total_kb;
            h.gpus_idle        = h.gpus_total;
            h.gpu_mem_idle_kb  = h.gpu_mem_total_kb;
            c.hosts.push_back(std::move(h));
            ++next_id;
        }
    }
    // One Show with effectively unlimited burst in each alloc the simulator uses.
    if (silos) {
        c.shows.push_back(Show{"benchmark", "small_alloc", 1'000'000'000, 0});
        c.shows.push_back(Show{"benchmark", "mid_alloc",   1'000'000'000, 0});
        c.shows.push_back(Show{"benchmark", "big_alloc",   1'000'000'000, 0});
    } else {
        c.shows.push_back(Show{"benchmark", "A1", 1'000'000'000, 0});
    }
    return c;
}

// ---- workload generation --------------------------------------------------

struct WorkloadConfig {
    double target_load_fraction  = 0.7;
    int    jobs_per_burst        = 5;
    double arrival_interval_s    = 60.0;
    double simulation_seconds    = 4 * 3600.0;
    double runtime_median_s      = 600.0;
    double runtime_sigma         = 0.9;
};

inline const FrameTypeConfig& pick_frame_type(std::mt19937_64& rng) {
    std::uniform_real_distribution<double> u(0.0, 100.0);
    double r   = u(rng);
    double acc = 0.0;
    for (const auto& ft : FRAME_TYPES) {
        acc += ft.percent;
        if (r <= acc) return ft;
    }
    return FRAME_TYPES.front();
}

inline std::string short_uuid(std::mt19937_64& rng) {
    static const char* hex = "0123456789abcdef";
    std::string s(8, '0');
    std::uniform_int_distribution<int> u(0, 15);
    for (auto& ch : s) ch = hex[u(rng)];
    return s;
}

inline Job generate_job(std::mt19937_64& rng,
                        double ts,
                        int target_cores,
                        double runtime_median_s,
                        double runtime_sigma,
                        bool silos) {
    Job j;
    j.job_id     = std::string("job-") + short_uuid(rng);
    j.show_id    = "benchmark";
    j.priority   = PRIORITIES[std::uniform_int_distribution<int>(
                       0, static_cast<int>(PRIORITIES.size()) - 1)(rng)];
    j.ts_started = ts;

    const FrameTypeConfig& spec = pick_frame_type(rng);
    int n_frames = std::max(1, target_cores / std::max(1, spec.cores));

    Layer L;
    L.layer_id        = j.job_id + "-layer-0";
    L.job_id          = j.job_id;
    L.show_id         = j.show_id;
    L.cores_min       = spec.cores;
    L.mem_min_kb      = static_cast<int64_t>(spec.memory_gb * GB_KB);
    L.gpus_min        = 0;
    L.gpu_mem_min_kb  = 0;
    L.tags            = spec.layer_tags;
    L.allowed_allocs  = silos ? spec.silo_allowed_allocs : std::set<std::string>{};
    L.runtime_median_s = runtime_median_s;
    L.runtime_sigma   = runtime_sigma;

    L.frames.reserve(n_frames);
    for (int i = 0; i < n_frames; ++i) {
        Frame f;
        char id_buf[64];
        std::snprintf(id_buf, sizeof(id_buf), "%s-frame-%04d", L.layer_id.c_str(), i);
        f.frame_id   = id_buf;
        f.layer_id   = L.layer_id;
        f.job_id     = j.job_id;
        f.int_number = i;
        L.frames.push_back(std::move(f));
    }
    j.layers.push_back(std::move(L));
    return j;
}

inline std::vector<Job> generate_arrivals(const WorkloadConfig& cfg,
                                          int64_t total_cluster_cores,
                                          int seed,
                                          bool silos) {
    std::mt19937_64 rng(static_cast<uint64_t>(seed));
    std::vector<Job> out;
    int64_t per_burst = static_cast<int64_t>(cfg.target_load_fraction
                                              * static_cast<double>(total_cluster_cores)
                                              * cfg.arrival_interval_s
                                              / cfg.runtime_median_s);
    int cores_per_job = std::max<int>(1, static_cast<int>(per_burst
                                              / std::max(1, cfg.jobs_per_burst)));
    for (double t = 0.0; t < cfg.simulation_seconds; t += cfg.arrival_interval_s) {
        for (int k = 0; k < cfg.jobs_per_burst; ++k) {
            out.push_back(generate_job(rng, t, cores_per_job,
                                       cfg.runtime_median_s,
                                       cfg.runtime_sigma, silos));
        }
    }
    return out;
}

// ---- runtime sampling -----------------------------------------------------

inline double sample_frame_runtime(std::mt19937_64& rng, const Layer& L) {
    double mu = std::log(L.runtime_median_s);
    std::lognormal_distribution<double> d(mu, L.runtime_sigma);
    return d(rng);
}

}  // namespace sim
