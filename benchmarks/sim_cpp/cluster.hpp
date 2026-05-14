// SPDX-License-Identifier: Apache-2.0
//
// Cluster data model. Same shape as the Python sim's cluster.py.
//
//   Host    - a machine with fixed total capacity and currently running procs
//   Frame   - a single dispatchable unit, state WAITING/RUNNING/DONE
//   Layer   - a group of frames with shared resource requirements
//   Job     - a collection of layers with a priority and an arrival time
//   Show    - subscription state: cumulative cores used on this alloc
//   Cluster - the whole world
//
// All POD-ish structs with vectors of small structs. The hot path
// (scheduler tick) iterates these in cache-friendly order.

#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim {

enum class FrameState : uint8_t { WAITING = 0, RUNNING = 1, DONE = 2 };

struct Frame {
    std::string frame_id;
    std::string layer_id;
    std::string job_id;
    int         int_number    = 0;
    FrameState  state         = FrameState::WAITING;
    // When RUNNING: simulated unix time when the frame started (set by
    // the simulator at dispatch) and when it's scheduled to finish.
    // The two together let EASY backfill predict per-host free times.
    double      start_time    = 0.0;
    double      finish_time   = 0.0;
    std::string host_id;
};

struct Layer {
    std::string                layer_id;
    std::string                job_id;
    std::string                show_id;
    int                        cores_min       = 0;
    int64_t                    mem_min_kb      = 0;
    int                        gpus_min        = 0;
    int64_t                    gpu_mem_min_kb  = 0;
    std::set<std::string>      tags;             // layer requires ANY of these
    std::optional<std::string> os;
    // Allocations the layer is allowed to run in. Empty = any. Used in silo mode
    // by the Legacy scheduler to enforce manual alloc partitioning.
    std::set<std::string>      allowed_allocs;
    std::vector<Frame>         frames;
    double                     runtime_median_s = 600.0;
    double                     runtime_sigma    = 1.0;

    int waiting_frame_count() const {
        int n = 0;
        for (const auto& f : frames) if (f.state == FrameState::WAITING) ++n;
        return n;
    }

    int next_frame_number() const {
        int best = -1;
        for (const auto& f : frames)
            if (f.state == FrameState::WAITING)
                if (best < 0 || f.int_number < best) best = f.int_number;
        return best;
    }

    // First WAITING frame in int_number order. Returns nullptr if none.
    Frame* next_waiting_frame() {
        Frame* best = nullptr;
        for (auto& f : frames)
            if (f.state == FrameState::WAITING)
                if (best == nullptr || f.int_number < best->int_number) best = &f;
        return best;
    }
};

struct Job {
    std::string        job_id;
    std::string        show_id;
    int                priority      = 0;
    double             ts_started    = 0.0;
    std::vector<Layer> layers;
    bool               paused        = false;
    std::string        state         = "PENDING";
    int                cores_in_use  = 0;
    int                max_cores     = 100000;
};

struct Host {
    std::string                host_id;
    std::string                name;
    std::string                alloc;
    int                        cores_total      = 0;
    int64_t                    mem_total_kb     = 0;
    int                        gpus_total       = 0;
    int64_t                    gpu_mem_total_kb = 0;
    std::set<std::string>      tags;
    std::optional<std::string> os;
    int                        cores_idle       = 0;
    int64_t                    mem_idle_kb      = 0;
    int                        gpus_idle        = 0;
    int64_t                    gpu_mem_idle_kb  = 0;
    // Frames currently running on this host. We store pointers into the Cluster's
    // owned Frame storage (the Layer's frames vector). The pointers are valid
    // for the lifetime of the simulation because we never erase Frames from
    // their owning Layer.
    std::vector<Frame*>        running;

    int running_procs() const { return static_cast<int>(running.size()); }
};

struct Show {
    std::string show_id;
    std::string alloc;
    int         burst_cores  = 1'000'000'000;
    int         cores_in_use = 0;
};

struct Cluster {
    std::vector<Host> hosts;
    std::vector<Job>  jobs;
    std::vector<Show> shows;

    Show& show_of(const std::string& show_id) {
        for (auto& s : shows) if (s.show_id == show_id) return s;
        // Defensive fallback: add a permissive one on the fly.
        shows.push_back(Show{show_id, "A1", 1'000'000'000, 0});
        return shows.back();
    }

    int64_t total_cores() const {
        int64_t n = 0;
        for (const auto& h : hosts) n += h.cores_total;
        return n;
    }
    int64_t total_idle_cores() const {
        int64_t n = 0;
        for (const auto& h : hosts) n += h.cores_idle;
        return n;
    }
    int64_t waiting_frame_count() const {
        int64_t n = 0;
        for (const auto& j : jobs)
            for (const auto& l : j.layers)
                for (const auto& f : l.frames)
                    if (f.state == FrameState::WAITING) ++n;
        return n;
    }
    int64_t running_frame_count() const {
        int64_t n = 0;
        for (const auto& h : hosts) n += h.running.size();
        return n;
    }
};

// Lookup helper used by simulator + reporter.
inline Layer* layer_of(Cluster& c, const Frame& f) {
    for (auto& j : c.jobs) {
        if (j.job_id != f.job_id) continue;
        for (auto& l : j.layers) if (l.layer_id == f.layer_id) return &l;
    }
    return nullptr;
}

inline Job* job_of(Cluster& c, const std::string& job_id) {
    for (auto& j : c.jobs) if (j.job_id == job_id) return &j;
    return nullptr;
}

}  // namespace sim
