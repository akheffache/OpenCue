
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.service.HostManager;
import com.imageworks.spcue.service.JobManager;

/**
 * SimpleScheduler — V1.
 *
 * A single-threaded periodic scheduler that replaces the per-host, multi-threaded
 * dispatch loop with a global plan + commit model. Each tick:
 *
 *   1. Snapshots idle hosts and dispatchable layers via two SQL queries.
 *   2. Pairs each layer (ranked by priority and age) with the smallest-surplus
 *      host that fits — classical best-fit bin packing. Fixes the "small frame
 *      lands on big host" fragmentation pathology.
 *   3. Commits each (host, layer) pair by invoking the existing
 *      {@code dispatcher.dispatchHost(host, layer)} entry point, which handles
 *      frame selection, transaction, and RQD launch.
 *
 * Gated by the {@code scheduler.simple.enabled} property. When enabled, the
 * legacy BookingQueue path in HostReportHandler is skipped (one-line gate
 * there), and this scheduler is the sole dispatcher.
 *
 * V1 intentionally omits: delta refresh via ts_updated (re-reads each tick),
 * multi-resource E-PVM scoring, wide-job reservation / EASY backfill, multi-
 * cuebot leader election. These are the v2 algorithmic refinements; the
 * fragmentation win comes from best-fit alone.
 */
public class SimpleScheduler extends JdbcDaoSupport {

    private static final Logger logger = LogManager.getLogger(SimpleScheduler.class);

    @Autowired
    private Environment env;

    private Dispatcher dispatcher;
    private HostManager hostManager;
    private JobManager jobManager;

    // Skip a tick if the previous one is still running (e.g. slow DB).
    private final AtomicBoolean tickInFlight = new AtomicBoolean(false);

    // ---- Snapshot queries --------------------------------------------------

    /**
     * Idle hosts available for booking. Filters mirror the conditions a
     * bookable host must satisfy: UP, lock state OPEN, with at least the
     * minimum bookable cores and memory.
     *
     * Column locations:
     *   host        — pk_host, str_lock_state, str_tags, int_cores_idle,
     *                 int_mem_idle, int_gpus_idle, int_gpu_mem_idle, b_nimby
     *   host_stat   — str_state (UP/DOWN/...), str_os
     */
    private static final String SELECT_IDLE_HOSTS =
        "SELECT "
        + "  h.pk_host, "
        + "  h.str_name, "
        + "  h.int_cores_idle, "
        + "  h.int_mem_idle, "
        + "  h.int_gpus_idle, "
        + "  h.int_gpu_mem_idle, "
        + "  h.str_tags, "
        + "  hs.str_os, "
        + "  h.b_nimby "
        + "FROM host h, host_stat hs "
        + "WHERE h.pk_host = hs.pk_host "
        + "  AND hs.str_state = 'UP' "
        + "  AND h.str_lock_state = 'OPEN' "
        + "  AND h.int_cores_idle >= ? "
        + "  AND h.int_mem_idle  >= ? ";

    /**
     * Dispatchable layers: one row per layer that has at least one WAITING
     * frame with dependencies resolved, on a non-paused PENDING job.
     * Bounded by LIMIT to keep snapshot latency predictable; ranking is the
     * existing priority + age order so the most important work is always
     * considered first.
     */
    private static final String SELECT_DISPATCHABLE_LAYERS =
        "SELECT "
        + "  l.pk_layer, "
        + "  l.pk_job, "
        + "  j.pk_show, "
        + "  l.int_cores_min, "
        + "  l.int_mem_min, "
        + "  l.int_gpus_min, "
        + "  l.int_gpu_mem_min, "
        + "  l.str_tags, "
        + "  l.str_services, "
        + "  jr.int_priority, "
        + "  j.ts_started "
        + "FROM   layer l "
        + "JOIN   job j           ON j.pk_job  = l.pk_job "
        + "JOIN   job_resource jr ON jr.pk_job = j.pk_job "
        + "WHERE  j.str_state = 'PENDING' "
        + "  AND  j.b_paused  = false "
        + "  AND  EXISTS ( "
        + "         SELECT 1 FROM frame f "
        + "         WHERE  f.pk_layer  = l.pk_layer "
        + "           AND  f.str_state = 'WAITING' "
        + "           AND  f.int_depend_count = 0 "
        + "       ) "
        + "ORDER BY jr.int_priority DESC, j.ts_started ASC "
        + "LIMIT  ? ";

    // ---- Row mappers -------------------------------------------------------

    private static final RowMapper<HostRow> HOST_MAPPER = new RowMapper<HostRow>() {
        public HostRow mapRow(ResultSet rs, int i) throws SQLException {
            HostRow h = new HostRow();
            h.id        = rs.getString("pk_host");
            h.name      = rs.getString("str_name");
            h.coresIdle = rs.getInt("int_cores_idle");
            h.memIdle   = rs.getLong("int_mem_idle");
            h.gpusIdle  = rs.getInt("int_gpus_idle");
            h.gpuMemIdle = rs.getLong("int_gpu_mem_idle");
            h.tags      = rs.getString("str_tags");
            h.os        = rs.getString("str_os");
            h.nimby     = rs.getBoolean("b_nimby");
            return h;
        }
    };

    private static final RowMapper<LayerRow> LAYER_MAPPER = new RowMapper<LayerRow>() {
        public LayerRow mapRow(ResultSet rs, int i) throws SQLException {
            LayerRow l = new LayerRow();
            l.layerId   = rs.getString("pk_layer");
            l.jobId     = rs.getString("pk_job");
            l.showId    = rs.getString("pk_show");
            l.coresMin  = rs.getInt("int_cores_min");
            l.memMin    = rs.getLong("int_mem_min");
            l.gpusMin   = rs.getInt("int_gpus_min");
            l.gpuMemMin = rs.getLong("int_gpu_mem_min");
            l.tags      = rs.getString("str_tags");
            l.services  = rs.getString("str_services");
            l.priority  = rs.getInt("int_priority");
            return l;
        }
    };

    // ---- Tick --------------------------------------------------------------

    /**
     * Public entry point called by the Quartz trigger.
     */
    public void runTick() {
        if (!isEnabled()) return;
        if (!tickInFlight.compareAndSet(false, true)) {
            logger.debug("SimpleScheduler: previous tick still running, skipping");
            return;
        }
        long t0 = System.currentTimeMillis();
        try {
            tick();
        } catch (RuntimeException e) {
            logger.error("SimpleScheduler tick failed", e);
        } finally {
            tickInFlight.set(false);
            long ms = System.currentTimeMillis() - t0;
            if (ms > 250) {
                logger.info("SimpleScheduler tick took " + ms + " ms");
            }
        }
    }

    private void tick() {
        List<HostRow>  hosts  = readIdleHosts();
        if (hosts.isEmpty()) return;

        List<LayerRow> layers = readDispatchableLayers();
        if (layers.isEmpty()) return;

        List<Pairing> plan = planBestFit(hosts, layers);
        if (plan.isEmpty()) return;

        commit(plan);
    }

    // ---- Snapshot reads ----------------------------------------------------

    private List<HostRow> readIdleHosts() {
        int  coresMin = Dispatcher.CORE_POINTS_RESERVED_MIN;
        long memMin   = env.getRequiredProperty("dispatcher.memory.mem_reserved_min", Long.class);
        return getJdbcTemplate().query(SELECT_IDLE_HOSTS, HOST_MAPPER, coresMin, memMin);
    }

    private List<LayerRow> readDispatchableLayers() {
        int limit = getIntProperty("scheduler.simple.layer_query_max", 5000);
        return getJdbcTemplate().query(SELECT_DISPATCHABLE_LAYERS, LAYER_MAPPER, limit);
    }

    // ---- Planner: best-fit on cores ---------------------------------------

    static List<Pairing> planBestFit(List<HostRow> hostsIn, List<LayerRow> layers) {
        // Mutable scratch copies — planner subtracts as it assigns.
        List<HostRow> hosts = new ArrayList<>(hostsIn.size());
        for (HostRow h : hostsIn) hosts.add(h.copy());

        // Layers already ordered by priority DESC, age ASC by the SQL.
        List<Pairing> plan = new ArrayList<>();
        for (LayerRow l : layers) {
            HostRow best = null;
            long bestSurplus = Long.MAX_VALUE;
            for (HostRow h : hosts) {
                if (!fits(l, h)) continue;
                long surplus = (long) h.coresIdle - (long) l.coresMin; // best-fit on cores
                if (surplus < bestSurplus) {
                    bestSurplus = surplus;
                    best = h;
                }
            }
            if (best == null) continue;

            plan.add(new Pairing(best.id, l.layerId, l.jobId));

            // Conservative: assume the dispatcher will book up to
            // host_frame_dispatch_max frames from this layer on this host.
            // Remove the host from the planning pool so we don't double-assign.
            // V2 can split hosts across multiple layers.
            hosts.remove(best);
        }
        return plan;
    }

    static boolean fits(LayerRow l, HostRow h) {
        if (h.coresIdle < l.coresMin) return false;
        if (h.memIdle   < l.memMin)   return false;
        if (h.gpusIdle  < l.gpusMin)  return false;
        if (h.gpuMemIdle < l.gpuMemMin) return false;
        if (!tagsCompatible(l.tags, h.tags)) return false;
        // OS / services are checked by the dispatcher's own SQL during
        // dispatchHost(host, layer); a planner-side mismatch just wastes the
        // pairing slot for one tick.
        return true;
    }

    /**
     * Approximate tag match: every whitespace-separated token in the layer's
     * tags must appear as a whitespace-separated token in the host's tags.
     * Sufficient for V1; the existing dispatcher SQL re-checks via regex so
     * any miss is caught at commit time (the commit just books zero frames
     * and we re-plan next tick).
     */
    static boolean tagsCompatible(String layerTags, String hostTags) {
        if (layerTags == null || layerTags.trim().isEmpty()) return true;
        if (hostTags  == null) return false;
        Set<String> hostSet = new HashSet<>(Arrays.asList(hostTags.trim().split("\\s+")));
        for (String t : layerTags.trim().split("\\s+")) {
            if (t.isEmpty()) continue;
            if (!hostSet.contains(t)) return false;
        }
        return true;
    }

    // ---- Commit ------------------------------------------------------------

    private void commit(List<Pairing> plan) {
        int booked = 0;
        for (Pairing p : plan) {
            try {
                DispatchHost host = hostManager.getDispatchHost(p.hostId);
                LayerInterface layer = jobManager.getLayer(p.layerId);
                int n = dispatcher.dispatchHost(host, layer).size();
                booked += n;
            } catch (RuntimeException e) {
                // Stale pairing (host went down, layer completed, etc.) — fine.
                logger.debug("SimpleScheduler commit skipped pairing host=" + p.hostId
                        + " layer=" + p.layerId + ": " + e.getMessage());
            }
        }
        if (booked > 0) {
            logger.info("SimpleScheduler booked " + booked + " frames across "
                    + plan.size() + " (host, layer) pairings");
        }
    }

    // ---- Config helpers ----------------------------------------------------

    private boolean isEnabled() {
        return env.getProperty("scheduler.simple.enabled", Boolean.class, false);
    }

    private int getIntProperty(String key, int fallback) {
        return env.getProperty(key, Integer.class, fallback);
    }

    // ---- POJOs -------------------------------------------------------------

    static final class HostRow {
        String id;
        String name;
        int    coresIdle;
        long   memIdle;
        int    gpusIdle;
        long   gpuMemIdle;
        String tags;
        String os;
        boolean nimby;

        HostRow copy() {
            HostRow c = new HostRow();
            c.id = id; c.name = name;
            c.coresIdle = coresIdle; c.memIdle = memIdle;
            c.gpusIdle = gpusIdle;   c.gpuMemIdle = gpuMemIdle;
            c.tags = tags; c.os = os; c.nimby = nimby;
            return c;
        }
    }

    static final class LayerRow {
        String layerId;
        String jobId;
        String showId;
        int    coresMin;
        long   memMin;
        int    gpusMin;
        long   gpuMemMin;
        String tags;
        String services;
        int    priority;
    }

    static final class Pairing {
        final String hostId;
        final String layerId;
        final String jobId;
        Pairing(String hostId, String layerId, String jobId) {
            this.hostId = hostId; this.layerId = layerId; this.jobId = jobId;
        }
    }

    // ---- Spring setters ----------------------------------------------------

    public void setDispatcher(Dispatcher d)             { this.dispatcher = d; }
    public void setHostManager(HostManager m)           { this.hostManager = m; }
    public void setJobManager(JobManager m)             { this.jobManager = m; }
}
