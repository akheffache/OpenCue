// SPDX-License-Identifier: Apache-2.0
//
// Discrete-time simulator. Same structure as the Python version's
// simulator.py, but ~100x faster.

#pragma once

#include "cluster.hpp"
#include "workload.hpp"

#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim {

struct TickMetrics {
    double  t                  = 0.0;
    int64_t cores_total        = 0;
    int64_t cores_busy         = 0;
    int64_t cores_idle         = 0;
    int64_t frames_running     = 0;
    int64_t frames_waiting     = 0;
    int     bookings_this_tick = 0;
    int64_t fragmented_cores   = 0;
    int64_t db_ops_cumulative  = 0;
};

struct WaitRecord {
    std::string layer_id;
    std::string job_id;
    int         priority;
    int         cores_min;
    double      first_seen_at        = -1.0;
    double      first_dispatched_at  = -1.0;
};

template <class Scheduler>
class Simulator {
 public:
    Simulator(Cluster c,
              std::vector<Job> arrivals,
              Scheduler scheduler_,
              double tick_seconds_ = 1.0,
              int runtime_seed     = 42)
        : cluster(std::move(c)),
          scheduler(std::move(scheduler_)),
          tick_seconds(tick_seconds_),
          pending_arrivals(std::move(arrivals)),
          rng(static_cast<uint64_t>(runtime_seed)) {
        // Reverse so we can pop_back to pull the earliest in O(1).
        // The arrivals list was generated in ascending ts order.
        std::reverse(pending_arrivals.begin(), pending_arrivals.end());
    }

    void run(double simulation_seconds) {
        while (now < simulation_seconds) {
            complete_finished_frames();
            inject_arrivals();
            record_wait_starts();
            auto bookings = scheduler.tick(cluster, now);
            apply_bookings(bookings);
            record_metrics(static_cast<int>(bookings.size()));
            now += tick_seconds;
        }
    }

    // Order matters: members are initialized in declaration order.
    Cluster                                          cluster;
    Scheduler                                        scheduler;
    double                                           tick_seconds;
    std::vector<TickMetrics>                         metrics;
    std::unordered_map<std::string, WaitRecord>      wait_records;
    double                                           now            = 0.0;
    int64_t                                          total_bookings = 0;

 private:
    std::vector<Job> pending_arrivals;   // reversed: back() is earliest
    std::mt19937_64  rng;

    void complete_finished_frames() {
        for (auto& h : cluster.hosts) {
            // Filter h.running in place.
            auto write = h.running.begin();
            for (auto it = h.running.begin(); it != h.running.end(); ++it) {
                Frame* f = *it;
                if (f->finish_time <= now) {
                    Layer* L = layer_of(cluster, *f);
                    if (L) {
                        h.cores_idle      += effective_cores(L->cores_min);
                        h.mem_idle_kb     += L->mem_min_kb;
                        h.gpus_idle       += L->gpus_min;
                        h.gpu_mem_idle_kb += L->gpu_mem_min_kb;
                        Job* job = job_of(cluster, L->job_id);
                        if (job) {
                            job->cores_in_use = std::max(0, job->cores_in_use - L->cores_min);
                        }
                        Show& show = cluster.show_of(L->show_id);
                        show.cores_in_use = std::max(0, show.cores_in_use - L->cores_min);
                    }
                    f->state = FrameState::DONE;
                } else {
                    *write++ = f;
                }
            }
            h.running.erase(write, h.running.end());
        }
    }

    void inject_arrivals() {
        while (!pending_arrivals.empty() && pending_arrivals.back().ts_started <= now) {
            cluster.jobs.push_back(std::move(pending_arrivals.back()));
            pending_arrivals.pop_back();
        }
    }

    void record_wait_starts() {
        for (auto& j : cluster.jobs) {
            for (auto& L : j.layers) {
                if (wait_records.count(L.layer_id)) continue;
                if (L.waiting_frame_count() > 0) {
                    WaitRecord r;
                    r.layer_id      = L.layer_id;
                    r.job_id        = j.job_id;
                    r.priority      = j.priority;
                    r.cores_min     = L.cores_min;
                    r.first_seen_at = now;
                    wait_records.emplace(L.layer_id, std::move(r));
                }
            }
        }
    }

    void apply_bookings(const std::vector<Booking>& bookings) {
        for (const auto& b : bookings) {
            Layer* L = layer_of(cluster, *b.frame);
            if (!L) continue;
            // The frame was already marked RUNNING inside the scheduler so it
            // wouldn't be returned twice. Set host_id + finish_time here and
            // wire it into the host's running list.
            b.frame->host_id     = b.host->host_id;
            b.frame->start_time  = now;
            b.frame->finish_time = now + sample_frame_runtime(rng, *L);
            b.host->running.push_back(b.frame);
            ++total_bookings;
            auto it = wait_records.find(L->layer_id);
            if (it != wait_records.end() && it->second.first_dispatched_at < 0)
                it->second.first_dispatched_at = now;
        }
    }

    void record_metrics(int bookings_count) {
        TickMetrics m;
        m.t                  = now;
        m.cores_total        = cluster.total_cores();
        m.cores_idle         = cluster.total_idle_cores();
        m.cores_busy         = m.cores_total - m.cores_idle;
        m.frames_running     = cluster.running_frame_count();
        m.frames_waiting     = cluster.waiting_frame_count();
        m.bookings_this_tick = bookings_count;
        m.fragmented_cores   = m.frames_waiting > 0 ? m.cores_idle : 0;
        m.db_ops_cumulative  = scheduler.db_ops;
        metrics.push_back(m);
    }
};

}  // namespace sim
