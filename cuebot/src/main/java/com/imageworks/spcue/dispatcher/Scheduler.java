
/*
 * Copyright Contributors to the OpenCue Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.imageworks.spcue.dispatcher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.service.HostManager;
import com.imageworks.spcue.service.JobManager;

/**
 * Scheduler: single-threaded periodic dispatch.
 *
 * Replaces the multi-threaded BookingQueue path. Each tick:
 *
 *   1. Acquire a Postgres advisory lock so only one Cuebot plans at a time.
 *   2. Read bookable hosts.
 *   3. Group them by static spec (alloc, normalized tags, os, has_gpu).
 *   4. For each group, run one candidate-layer query.
 *   5. Walk hosts in the group sequentially; for each host walk the cached
 *      candidates, book what fits, and update in-memory accounting so the
 *      next host sees the depleted job/show counters.
 *   6. Release the lock.
 *
 * Gated by scheduler.enabled (default false). When true, HostReportHandler
 * suppresses the legacy BookingQueue enqueue via the existing booking-off
 * branch, so the two paths never both run.
 */
public class Scheduler extends JdbcDaoSupport {

    private static final Logger logger = LogManager.getLogger(Scheduler.class);

    /**
     * Postgres advisory lock key. Must be the same constant on every Cuebot
     * that shares a database. Arbitrary 64-bit integer; chosen as ASCII
     * "OpenCue" for visibility in pg_locks.
     */
    private static final long SCHEDULER_LOCK_KEY = 0x4F70656E437565L;

    @Autowired
    private Environment env;

    private Dispatcher dispatcher;
    private HostManager hostManager;
    private JobManager jobManager;

    private final AtomicBoolean tickInFlight = new AtomicBoolean(false);

    // ---- snapshot queries -------------------------------------------------

    /**
     * Bookable hosts: UP, lock state OPEN, with at least the minimum bookable
     * cores. Returns enough columns to compute the spec key and run the
     * per-host fit check without a second lookup.
     */
    private static final String SELECT_BOOKABLE_HOSTS =
        "SELECT "
        + "  h.pk_host, "
        + "  h.str_name, "
        + "  h.pk_alloc, "
        + "  h.int_cores_idle, "
        + "  h.int_mem_idle, "
        + "  h.int_gpus_idle, "
        + "  h.int_gpu_mem_idle, "
        + "  h.str_tags, "
        + "  hs.str_os "
        + "FROM host h, host_stat hs "
        + "WHERE h.pk_host = hs.pk_host "
        + "  AND hs.str_state = 'UP' "
        + "  AND h.str_lock_state = 'OPEN' "
        + "  AND h.int_cores_idle >= ? ";

    /**
     * Candidate layers for a host spec group. One query per group. Filters:
     *   - job PENDING and unpaused
     *   - tag regex match against the group's normalized tag string
     *   - OS match (or any if the job is OS-agnostic)
     *   - job under int_max_cores
     *   - show under subscription burst on this alloc
     *   - at least one WAITING, depend-resolved frame on the layer
     *   - layer.int_cores_min fits the group's max idle cores
     * Ordered by priority + age, capped by LIMIT.
     */
    private static final String SELECT_CANDIDATES_FOR_GROUP =
        "SELECT "
        + "  l.pk_layer, "
        + "  l.pk_job, "
        + "  j.pk_show, "
        + "  l.int_cores_min, "
        + "  l.int_mem_min, "
        + "  l.int_gpus_min, "
        + "  l.int_gpu_mem_min, "
        + "  jr.int_priority, "
        + "  jr.int_cores       AS job_cores_in_use, "
        + "  jr.int_max_cores   AS job_max_cores, "
        + "  sub.int_cores      AS show_cores_in_use, "
        + "  sub.int_burst      AS show_burst "
        + "FROM   layer l "
        + "JOIN   job j           ON j.pk_job  = l.pk_job "
        + "JOIN   job_resource jr ON jr.pk_job = j.pk_job "
        + "JOIN   subscription sub ON sub.pk_show = j.pk_show AND sub.pk_alloc = ? "
        + "WHERE  j.str_state = 'PENDING' "
        + "  AND  j.b_paused  = false "
        + "  AND  (j.str_os IS NULL OR j.str_os = '' OR j.str_os = ?) "
        + "  AND  ? ~* ('(?x)' || l.str_tags || '\\y') "
        + "  AND  jr.int_cores  < jr.int_max_cores "
        + "  AND  sub.int_cores < sub.int_burst "
        + "  AND  l.int_cores_min <= ? "
        + "  AND  EXISTS ( "
        + "         SELECT 1 FROM frame f "
        + "         WHERE  f.pk_layer = l.pk_layer "
        + "           AND  f.str_state = 'WAITING' "
        + "           AND  f.int_depend_count = 0 "
        + "       ) "
        + "ORDER BY jr.int_priority DESC, j.ts_started ASC "
        + "LIMIT  ? ";

    // ---- row mappers ------------------------------------------------------

    private static final RowMapper<BookableHost> HOST_MAPPER = new RowMapper<BookableHost>() {
        public BookableHost mapRow(ResultSet rs, int i) throws SQLException {
            BookableHost h = new BookableHost();
            h.hostId      = rs.getString("pk_host");
            h.hostName    = rs.getString("str_name");
            h.pkAlloc     = rs.getString("pk_alloc");
            h.coresIdle   = rs.getInt("int_cores_idle");
            h.memIdle     = rs.getLong("int_mem_idle");
            h.gpusIdle    = rs.getInt("int_gpus_idle");
            h.gpuMemIdle  = rs.getLong("int_gpu_mem_idle");
            h.tagsRaw     = rs.getString("str_tags");
            h.os          = rs.getString("str_os");
            return h;
        }
    };

    private static final RowMapper<LayerCandidate> CANDIDATE_MAPPER =
            new RowMapper<LayerCandidate>() {
        public LayerCandidate mapRow(ResultSet rs, int i) throws SQLException {
            LayerCandidate c = new LayerCandidate();
            c.layerId         = rs.getString("pk_layer");
            c.jobId           = rs.getString("pk_job");
            c.showId          = rs.getString("pk_show");
            c.layerCoresMin   = rs.getInt("int_cores_min");
            c.layerMemMin     = rs.getLong("int_mem_min");
            c.layerGpusMin    = rs.getInt("int_gpus_min");
            c.layerGpuMemMin  = rs.getLong("int_gpu_mem_min");
            c.priority        = rs.getInt("int_priority");
            c.jobCoresInUse   = rs.getInt("job_cores_in_use");
            c.jobMaxCores     = rs.getInt("job_max_cores");
            c.showCoresInUse  = rs.getInt("show_cores_in_use");
            c.showBurstCores  = rs.getInt("show_burst");
            return c;
        }
    };

    // ---- tick -------------------------------------------------------------

    /** Public entry point. Invoked by the Quartz trigger. */
    public void runTick() {
        if (!isEnabled()) return;
        if (!tickInFlight.compareAndSet(false, true)) {
            logger.debug("Scheduler: previous tick still running, skipping");
            return;
        }
        long t0 = System.currentTimeMillis();
        try {
            if (!acquireLeaderLock()) {
                logger.debug("Scheduler: another Cuebot holds the planning lock");
                return;
            }
            try {
                int dispatched = doTick();
                long ms = System.currentTimeMillis() - t0;
                if (dispatched > 0 || ms > 500) {
                    logger.info("Scheduler tick: dispatched " + dispatched
                            + " procs, " + ms + " ms");
                }
            } finally {
                releaseLeaderLock();
            }
        } catch (RuntimeException e) {
            logger.error("Scheduler tick failed", e);
        } finally {
            tickInFlight.set(false);
        }
    }

    private int doTick() {
        List<BookableHost> hosts = readBookableHosts();
        if (hosts.isEmpty()) return 0;

        Map<HostSpecKey, List<BookableHost>> groups = groupByHostSpec(hosts);

        int dispatched = 0;
        for (Map.Entry<HostSpecKey, List<BookableHost>> g : groups.entrySet()) {
            HostSpecKey spec = g.getKey();
            List<BookableHost> groupHosts = g.getValue();

            int maxIdleInGroup = groupHosts.stream()
                    .mapToInt(h -> h.coresIdle).max().orElse(0);

            List<LayerCandidate> candidates =
                    readLayerCandidatesForGroup(spec, maxIdleInGroup);
            if (candidates.isEmpty()) continue;

            for (BookableHost h : groupHosts) {
                dispatched += dispatchHostFromCandidates(h, candidates);
            }
        }
        return dispatched;
    }

    // ---- leader lock ------------------------------------------------------

    private boolean acquireLeaderLock() {
        Boolean got = getJdbcTemplate().queryForObject(
                "SELECT pg_try_advisory_lock(?)", Boolean.class, SCHEDULER_LOCK_KEY);
        return Boolean.TRUE.equals(got);
    }

    private void releaseLeaderLock() {
        try {
            getJdbcTemplate().queryForObject(
                    "SELECT pg_advisory_unlock(?)", Boolean.class, SCHEDULER_LOCK_KEY);
        } catch (RuntimeException e) {
            // If the connection dropped, the lock is released automatically.
            logger.debug("Scheduler: pg_advisory_unlock failed (probably connection drop): "
                    + e.getMessage());
        }
    }

    // ---- snapshot reads ---------------------------------------------------

    private List<BookableHost> readBookableHosts() {
        return getJdbcTemplate().query(
                SELECT_BOOKABLE_HOSTS,
                HOST_MAPPER,
                Dispatcher.CORE_POINTS_RESERVED_MIN);
    }

    private List<LayerCandidate> readLayerCandidatesForGroup(HostSpecKey spec,
                                                             int maxIdleInGroup) {
        int limit = env.getProperty("scheduler.layer_candidates_per_group_max",
                Integer.class, 2000);
        return getJdbcTemplate().query(
                SELECT_CANDIDATES_FOR_GROUP,
                CANDIDATE_MAPPER,
                spec.pkAlloc,
                spec.os,
                spec.tagsNormalized,
                maxIdleInGroup,
                limit);
    }

    // ---- grouping ---------------------------------------------------------

    static Map<HostSpecKey, List<BookableHost>> groupByHostSpec(List<BookableHost> hosts) {
        Map<HostSpecKey, List<BookableHost>> groups = new LinkedHashMap<>();
        for (BookableHost h : hosts) {
            HostSpecKey k = new HostSpecKey(
                    h.pkAlloc,
                    normalizeTags(h.tagsRaw),
                    h.os,
                    h.gpusIdle > 0 || h.gpuMemIdle > 0);
            groups.computeIfAbsent(k, x -> new ArrayList<>()).add(h);
        }
        return groups;
    }

    /**
     * Normalize a host's whitespace-separated tag string so equivalent
     * sets ("linux desktop" and "desktop linux") group together.
     */
    static String normalizeTags(String raw) {
        if (raw == null || raw.trim().isEmpty()) return "";
        return Arrays.stream(raw.trim().split("\\s+"))
                     .sorted()
                     .collect(Collectors.joining(" "));
    }

    // ---- per-host match + dispatch ----------------------------------------

    /**
     * Walk the candidate list in priority order, dispatch what fits on this
     * host, and update in-memory accounting so subsequent hosts in the same
     * group see the depleted job/show counters. Stops when the host has no
     * more bookable resources.
     */
    private int dispatchHostFromCandidates(BookableHost h, List<LayerCandidate> candidates) {
        int dispatched = 0;
        for (LayerCandidate c : candidates) {
            if (h.coresIdle < Dispatcher.CORE_POINTS_RESERVED_MIN) break;

            if (!fitsOnHost(c, h)) continue;
            if (c.jobCoresInUse  + c.layerCoresMin > c.jobMaxCores)   continue;
            if (c.showCoresInUse + c.layerCoresMin > c.showBurstCores) continue;

            try {
                DispatchHost host = hostManager.getDispatchHost(h.hostId);
                LayerInterface layer = jobManager.getLayer(c.layerId);
                List<VirtualProc> procs = dispatcher.dispatchHost(host, layer);
                if (procs.isEmpty()) continue;

                int bookedCores = 0;
                long bookedMem  = 0;
                int bookedGpus  = 0;
                long bookedGpuMem = 0;
                for (VirtualProc p : procs) {
                    bookedCores  += p.coresReserved;
                    bookedMem    += p.memoryReserved;
                    bookedGpus   += p.gpusReserved;
                    bookedGpuMem += p.gpuMemoryReserved;
                }

                // In-memory accounting carries forward to the next host in this group.
                h.coresIdle   -= bookedCores;
                h.memIdle     -= bookedMem;
                h.gpusIdle    -= bookedGpus;
                h.gpuMemIdle  -= bookedGpuMem;
                c.jobCoresInUse  += bookedCores;
                c.showCoresInUse += bookedCores;

                dispatched += procs.size();

            } catch (RuntimeException e) {
                // Host vanished, layer completed, frame race lost, etc. Move on;
                // next tick reconsiders.
                logger.debug("Scheduler skipped (host=" + h.hostId
                        + ", layer=" + c.layerId + "): " + e.getMessage());
            }
        }
        return dispatched;
    }

    static boolean fitsOnHost(LayerCandidate c, BookableHost h) {
        if (h.coresIdle  < c.layerCoresMin)  return false;
        if (h.memIdle    < c.layerMemMin)    return false;
        if (h.gpusIdle   < c.layerGpusMin)   return false;
        if (h.gpuMemIdle < c.layerGpuMemMin) return false;
        return true;
    }

    // ---- config -----------------------------------------------------------

    private boolean isEnabled() {
        return env.getProperty("scheduler.enabled", Boolean.class, false);
    }

    // ---- POJOs ------------------------------------------------------------

    static final class BookableHost {
        String hostId;
        String hostName;
        String pkAlloc;
        int    coresIdle;
        long   memIdle;
        int    gpusIdle;
        long   gpuMemIdle;
        String tagsRaw;
        String os;
    }

    static final class LayerCandidate {
        String layerId;
        String jobId;
        String showId;
        int    layerCoresMin;
        long   layerMemMin;
        int    layerGpusMin;
        long   layerGpuMemMin;
        int    priority;
        // Mutable in-tick accounting.
        int    jobCoresInUse;
        int    jobMaxCores;
        int    showCoresInUse;
        int    showBurstCores;
    }

    static final class HostSpecKey {
        final String pkAlloc;
        final String tagsNormalized;
        final String os;
        final boolean hasGpu;

        HostSpecKey(String pkAlloc, String tagsNormalized, String os, boolean hasGpu) {
            this.pkAlloc        = pkAlloc;
            this.tagsNormalized = tagsNormalized;
            this.os             = os;
            this.hasGpu         = hasGpu;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HostSpecKey)) return false;
            HostSpecKey k = (HostSpecKey) o;
            return hasGpu == k.hasGpu
                && Objects.equals(pkAlloc,        k.pkAlloc)
                && Objects.equals(tagsNormalized, k.tagsNormalized)
                && Objects.equals(os,             k.os);
        }

        @Override public int hashCode() {
            return Objects.hash(pkAlloc, tagsNormalized, os, hasGpu);
        }

        @Override public String toString() {
            return "HostSpec(alloc=" + pkAlloc
                + ", tags=" + tagsNormalized
                + ", os=" + os
                + ", gpu=" + hasGpu + ")";
        }
    }

    // ---- Spring setters ---------------------------------------------------

    public void setDispatcher(Dispatcher d)   { this.dispatcher  = d; }
    public void setHostManager(HostManager m) { this.hostManager = m; }
    public void setJobManager(JobManager m)   { this.jobManager  = m; }
}
