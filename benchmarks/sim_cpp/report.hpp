// SPDX-License-Identifier: Apache-2.0
//
// Aggregates per-tick metrics + per-layer wait records into a side-by-side
// comparison table. Optional CSV output for plotting.

#pragma once

#include "simulator.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <numeric>
#include <string>
#include <vector>

namespace sim {

struct AggregateStats {
    int64_t ticks            = 0;
    int64_t cores_total      = 0;
    double  avg_util_pct     = 0.0;
    double  peak_util_pct    = 0.0;
    double  avg_frag_pct     = 0.0;
    double  peak_frag_pct    = 0.0;
    int64_t total_bookings   = 0;
    int64_t db_ops           = 0;
};

inline AggregateStats aggregate(const std::vector<TickMetrics>& metrics) {
    AggregateStats s;
    if (metrics.empty()) return s;
    s.ticks       = metrics.size();
    s.cores_total = 0;
    double util_sum = 0, frag_sum = 0, util_peak = 0, frag_peak = 0;
    int64_t bookings = 0;
    for (const auto& m : metrics) {
        s.cores_total = std::max<int64_t>(s.cores_total, m.cores_total);
        double u = m.cores_total ? double(m.cores_busy)        / double(m.cores_total) : 0.0;
        double f = m.cores_total ? double(m.fragmented_cores)  / double(m.cores_total) : 0.0;
        util_sum += u; frag_sum += f;
        if (u > util_peak) util_peak = u;
        if (f > frag_peak) frag_peak = f;
        bookings += m.bookings_this_tick;
    }
    s.avg_util_pct   = 100.0 * util_sum / metrics.size();
    s.peak_util_pct  = 100.0 * util_peak;
    s.avg_frag_pct   = 100.0 * frag_sum / metrics.size();
    s.peak_frag_pct  = 100.0 * frag_peak;
    s.total_bookings = bookings;
    s.db_ops         = metrics.back().db_ops_cumulative;
    return s;
}

// Linear-interpolation percentile, correct for any sample size.
inline double percentile(std::vector<double> v, double p) {
    if (v.empty()) return std::nan("");
    std::sort(v.begin(), v.end());
    if (v.size() == 1) return v[0];
    double k = (v.size() - 1) * p / 100.0;
    double f = std::floor(k);
    double c = std::min(f + 1, double(v.size() - 1));
    if (f == c) return v[static_cast<size_t>(f)];
    return v[static_cast<size_t>(f)] * (c - k) + v[static_cast<size_t>(c)] * (k - f);
}

struct WaitStats {
    int64_t dispatched        = 0;
    int64_t never_dispatched  = 0;
    // Survivorship-biased: only over layers that did dispatch.
    double  p50_disp = std::nan("");
    double  p95_disp = std::nan("");
    double  p99_disp = std::nan("");
    // Honest: never-dispatched layers count as (sim_end - first_seen_at).
    double  p50_all  = std::nan("");
    double  p95_all  = std::nan("");
    double  p99_all  = std::nan("");
};

inline WaitStats wait_stats(const std::vector<WaitRecord>& waits, double sim_end_s,
                            int min_cores_filter = 0) {
    WaitStats s;
    std::vector<double> disp;
    std::vector<double> all;
    disp.reserve(waits.size());
    all.reserve(waits.size());
    for (const auto& r : waits) {
        if (r.cores_min < min_cores_filter) continue;
        if (r.first_dispatched_at >= 0) {
            ++s.dispatched;
            double w = r.first_dispatched_at - r.first_seen_at;
            disp.push_back(w);
            all.push_back(w);
        } else {
            ++s.never_dispatched;
            all.push_back(sim_end_s - r.first_seen_at);
        }
    }
    s.p50_disp = percentile(disp, 50.0);
    s.p95_disp = percentile(disp, 95.0);
    s.p99_disp = percentile(disp, 99.0);
    s.p50_all  = percentile(all, 50.0);
    s.p95_all  = percentile(all, 95.0);
    s.p99_all  = percentile(all, 99.0);
    return s;
}

inline void write_csv(const std::string& path, const std::vector<TickMetrics>& metrics) {
    std::ofstream out(path);
    out << "t,cores_total,cores_busy,cores_idle,frames_running,frames_waiting,"
        << "bookings_this_tick,fragmented_cores,db_ops_cumulative\n";
    for (const auto& m : metrics) {
        out << m.t << ','
            << m.cores_total << ','
            << m.cores_busy << ','
            << m.cores_idle << ','
            << m.frames_running << ','
            << m.frames_waiting << ','
            << m.bookings_this_tick << ','
            << m.fragmented_cores << ','
            << m.db_ops_cumulative << '\n';
    }
}

// Compact printer for one row of the side-by-side table.
inline void row_d(const char* name, double a, double b, const char* unit = "") {
    auto print = [&](double v) {
        if (std::isnan(v)) std::printf("%12s", "n/a");
        else                std::printf("%11.1f%s", v, unit[0] ? unit : "");
    };
    std::printf("  %-32s", name);
    print(a);
    std::printf("  ");
    print(b);
    if (!std::isnan(a) && !std::isnan(b)) {
        double d = b - a;
        std::printf("  (%+.1f%s)", d, unit[0] ? unit : "");
    }
    std::printf("\n");
}

inline void row_i(const char* name, int64_t a, int64_t b) {
    std::printf("  %-32s%12lld%12lld",
                name, (long long)a, (long long)b);
    long long d = (long long)b - (long long)a;
    if (d != 0) std::printf("  (%+lld)", d);
    std::printf("\n");
}

inline void print_comparison(const char* baseline_name,
                              const AggregateStats& l, const AggregateStats& s,
                              const WaitStats& wl, const WaitStats& ws,
                              const WaitStats& wide_l, const WaitStats& wide_s) {
    std::printf("\n%.*s\n", 80, "================================================================================");
    std::printf("%-34s%12s%12s\n", "METRIC", baseline_name, "Smart");
    std::printf("%.*s\n", 80, "================================================================================");

    std::printf("\n--- utilization & fragmentation ---\n");
    row_d("avg utilization",    l.avg_util_pct,  s.avg_util_pct,  "%");
    row_d("peak utilization",   l.peak_util_pct, s.peak_util_pct, "%");
    row_d("avg fragmentation",  l.avg_frag_pct,  s.avg_frag_pct,  "%");
    row_d("peak fragmentation", l.peak_frag_pct, s.peak_frag_pct, "%");

    std::printf("\n--- throughput ---\n");
    row_i("total bookings", l.total_bookings, s.total_bookings);

    std::printf("\n--- DB query load (proxy: scheduler ops) ---\n");
    row_i("scheduler 'db ops'", l.db_ops, s.db_ops);
    if (s.db_ops > 0)
        std::printf("  %-32s%11.1fx\n", "reduction factor",
                    double(l.db_ops) / double(s.db_ops));

    std::printf("\n--- per-layer wait (excl. never-dispatched: BIASED, but listed for context) ---\n");
    row_i("layers dispatched",       wl.dispatched,       ws.dispatched);
    row_i("layers never dispatched", wl.never_dispatched, ws.never_dispatched);
    row_d("biased p50 wait",         wl.p50_disp,         ws.p50_disp, "s");
    row_d("biased p95 wait",         wl.p95_disp,         ws.p95_disp, "s");
    row_d("biased p99 wait",         wl.p99_disp,         ws.p99_disp, "s");

    std::printf("\n--- per-layer wait (incl. never-dispatched at sim_end: HONEST) ---\n");
    row_d("p50 wait",          wl.p50_all, ws.p50_all, "s");
    row_d("p95 wait",          wl.p95_all, ws.p95_all, "s");
    row_d("p99 wait",          wl.p99_all, ws.p99_all, "s");

    std::printf("\n--- wide-layer (>=16 cores) wait (honest) ---\n");
    row_i("wide layers dispatched", wide_l.dispatched, wide_s.dispatched);
    row_i("wide never dispatched",  wide_l.never_dispatched, wide_s.never_dispatched);
    row_d("wide p50 wait",          wide_l.p50_all, wide_s.p50_all, "s");
    row_d("wide p95 wait",          wide_l.p95_all, wide_s.p95_all, "s");
    row_d("wide p99 wait",          wide_l.p99_all, wide_s.p99_all, "s");
    std::printf("%.*s\n", 80, "================================================================================");
}

}  // namespace sim
