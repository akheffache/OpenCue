
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
 * Scheduler: single-threaded planner with persistent reservations and a
 * parallel commit pool.
 *
 * Replaces the multi-threaded BookingQueue path. Each tick:
 *
 *   1. Acquire a Postgres advisory lock so only one Cuebot plans at a time.
 *   2. Read bookable hosts.
 *   3. Group them by static spec (alloc, normalized tags, os, has_gpu).
 *   4. For each group:
 *        - one candidate-layer query
 *        - for each candidate in priority order:
 *            * dispatch loop (respects reservations, overrides lower
 *              priority on successful dispatch); submits each
 *              (host, layer) pairing to the commit pool and decrements
 *              in-memory accounting from an estimate
 *            * reconcile: the layer's reservation count should equal its
 *              remaining pending unfittable frame count
 *   5. Wait for the commit pool to drain.
 *   6. Sweep reservations whose layer no longer appears in any candidate set.
 *   7. Release the lock.
 *
 * Planning stays single-threaded so decisions never race on shared state.
 * Commits run on a fixed-size pool consuming a PriorityBlockingQueue ordered
 * by layer priority: I/O parallelism without re-introducing the decision
 * races the old BookingQueue suffered from. Within-layer frame collisions
 * (two workers dispatching different hosts for the same layer can land on
 * overlapping frame snapshots) are caught by the existing
 * frame.int_version optimistic lock; the loser rolls back and the worker
 * iterates to the next frame.
 *
 * Reservation invariant: a host's reservation belongs to the highest-priority
 * layer that has claimed it. A reservation persists across ticks until the
 * owning layer's pending unfittable frames reach zero, the layer leaves the
 * dispatchable set, or a higher-priority layer overrides the claim. This
 * prevents blocked-layer starvation: any layer that doesn't fit anywhere
 * claims hosts so they aren't re-consumed by lower-priority work between
 * the moment the layer becomes blocked and the moment hosts free up
 * enough cores.
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

    // ---- placementScore: E-PVM weights and unit normalization -------------
    //
    // Score units (after normalization):
    //   cores       in whole cores       (host int_cores_idle is in core points;
    //                                      100 core points = 1 core)
    //   memory      in GB                 (host/layer values are in KB)
    //   GPUs        in count              (no normalization)
    //   GPU memory  in GB                 (host/layer values are in KB)
    //
    // Weights tuned so cores and memory contribute equally; GPUs weighted
    // higher to discourage placing non-GPU work on GPU-rich hosts.
    private static final double W_CORES   = 1.0;
    private static final double W_MEM     = 1.0;
    private static final double W_GPUS    = 4.0;
    private static final double W_GPU_MEM = 1.0;

    private static final double CORE_POINTS_PER_CORE = 100.0;
    private static final double KB_PER_GB            = 1024.0 * 1024.0;

    @Autowired
    private Environment env;

    private Dispatcher dispatcher;
    private HostManager hostManager;
    private JobManager jobManager;

    private final AtomicBoolean tickInFlight = new AtomicBoolean(false);

    /**
     * Live host reservations, persistent across ticks. Key: host id. Value:
     * the (layer, priority) pair that has claimed the host. A reservation
     * is created when a blocked layer's reconcile claims a target host, and
     * removed when the layer's pending unfittable frame count reaches zero,
     * the layer leaves the dispatchable set entirely, or a higher-priority
     * layer overrides the claim.
     *
     * Single-writer (the planner thread). Failover via the advisory lock
     * means a new leader starts with an empty map and rebuilds the same set
     * within one or two ticks via reconciliation.
     */
    private final Map<String, Reservation> reservations = new HashMap<>();

    // ---- commit pool ------------------------------------------------------
    //
    // Workers consume CommitTasks from a priority queue and call
    // dispatcher.dispatchHost(host, layer). The planner submits tasks and
    // continues, decrementing in-memory accounting from an estimate. doTick
    // drains the queue before returning so the next tick sees a settled DB.

    private volatile ExecutorService commitPool;
    private final BlockingQueue<CommitTask> commitQueue =
            new PriorityBlockingQueue<>(64,
                    Comparator.<CommitTask>comparingInt(t -> -t.priority));
    private final AtomicInteger inFlightCommits = new AtomicInteger(0);
    private int jobFrameDispatchMax;

    /**
     * Lazy commit-pool init on the first runTick. Avoids touching Spring
     * XML wiring for an init-method, and Cuebot is well past startup by
     * the time scheduler.enabled is flipped on.
     */
    private synchronized void startCommitPoolIfNeeded() {
        if (commitPool != null) return;
        int size = env.getProperty("scheduler.commit_pool_size", Integer.class, 8);
        jobFrameDispatchMax = env.getProperty("dispatcher.job_frame_dispatch_max",
                Integer.class, 8);
        final ExecutorService pool = Executors.newFixedThreadPool(size, r -> {
            Thread t = new Thread(r);
            t.setName("Scheduler-commit-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < size; i++) pool.submit(this::commitWorker);
        commitPool = pool;
        logger.info("Scheduler: commit pool started with " + size + " workers");
    }

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
        + "  h.int_cores, "
        + "  h.int_cores_idle, "
        + "  h.int_mem, "
        + "  h.int_mem_idle, "
        + "  h.int_gpus, "
        + "  h.int_gpus_idle, "
        + "  h.int_gpu_mem, "
        + "  h.int_gpu_mem_idle, "
        + "  h.int_procs, "
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
     *   - layer.int_cores_min fits the group's max host TOTAL cores (not idle
     *     — a blocked layer waiting on a reserved host stays in the candidate
     *     set even when no host has it idle right now)
     * Ordered by priority + age, capped by LIMIT. waiting_frame_count is the
     * number of dispatchable frames on the layer at query time; reconciliation
     * uses it to decide how many hosts the layer should reserve.
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
        + "  sub.int_burst      AS show_burst, "
        + "  ( SELECT COUNT(*) FROM frame f "
        + "    WHERE  f.pk_layer  = l.pk_layer "
        + "      AND  f.str_state = 'WAITING' "
        + "      AND  f.int_depend_count = 0 "
        + "  ) AS waiting_frame_count "
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
            h.hostId       = rs.getString("pk_host");
            h.hostName     = rs.getString("str_name");
            h.pkAlloc      = rs.getString("pk_alloc");
            h.coresTotal   = rs.getInt("int_cores");
            h.coresIdle    = rs.getInt("int_cores_idle");
            h.memTotal     = rs.getLong("int_mem");
            h.memIdle      = rs.getLong("int_mem_idle");
            h.gpusTotal    = rs.getInt("int_gpus");
            h.gpusIdle     = rs.getInt("int_gpus_idle");
            h.gpuMemTotal  = rs.getLong("int_gpu_mem");
            h.gpuMemIdle   = rs.getLong("int_gpu_mem_idle");
            h.runningProcs = rs.getInt("int_procs");
            h.tagsRaw      = rs.getString("str_tags");
            h.os           = rs.getString("str_os");
            return h;
        }
    };

    private static final RowMapper<LayerCandidate> CANDIDATE_MAPPER =
            new RowMapper<LayerCandidate>() {
        public LayerCandidate mapRow(ResultSet rs, int i) throws SQLException {
            LayerCandidate c = new LayerCandidate();
            c.layerId            = rs.getString("pk_layer");
            c.jobId              = rs.getString("pk_job");
            c.showId             = rs.getString("pk_show");
            c.layerCoresMin      = rs.getInt("int_cores_min");
            c.layerMemMin        = rs.getLong("int_mem_min");
            c.layerGpusMin       = rs.getInt("int_gpus_min");
            c.layerGpuMemMin     = rs.getLong("int_gpu_mem_min");
            c.priority           = rs.getInt("int_priority");
            c.jobCoresInUse      = rs.getInt("job_cores_in_use");
            c.jobMaxCores        = rs.getInt("job_max_cores");
            c.showCoresInUse     = rs.getInt("show_cores_in_use");
            c.showBurstCores     = rs.getInt("show_burst");
            c.waitingFrameCount  = rs.getInt("waiting_frame_count");
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
        startCommitPoolIfNeeded();
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

    /**
     * One scheduling tick. The algorithm in order:
     *
     *   1. SNAPSHOT
     *      Read all bookable hosts (UP, OPEN, with at least the minimum
     *      bookable cores) in one SQL query. Each row carries the host's
     *      static spec (alloc, tags, OS), its current idle resources, its
     *      total capacity, and its running proc count.
     *
     *   2. GROUP
     *      Bucket hosts by their static spec key (alloc, normalized tags,
     *      os, has-gpu). Hosts in the same group share the same set of
     *      candidate layers, so one candidate query per group instead of
     *      per host.
     *
     *   3. FOR EACH GROUP:
     *        a. CANDIDATE QUERY
     *           One SQL per group, returning up to
     *           scheduler.layer_candidates_per_group_max layers, ordered
     *           by priority + age. The filter "int_cores_min <= group's
     *           MAX TOTAL cores" includes blocked layers whose reserved
     *           hosts are partially loaded; using max IDLE would let them
     *           drop out of the candidate set and be swept incorrectly.
     *
     *        b. DISPATCH AND RECONCILE (priority order)
     *           Implemented in dispatchGroupWithScoring. For each candidate:
     *             - Drain by best-fit onto fitting hosts. Reservation
     *               rules apply: a host reserved at priority >= c.priority
     *               for another layer is skipped; a host reserved at lower
     *               priority is usable, and on dispatch c takes ownership.
     *             - Reconcile c's reservation count to exactly c's
     *               remaining pending unfittable frame count.
     *           Layer ids encountered are added to seenLayerIds for the
     *           end-of-tick sweep.
     *
     *   4. SWEEP
     *      Any reservation whose layer didn't appear in any candidate set
     *      this tick is dropped. That layer is no longer dispatchable
     *      (job paused, completed, deleted, or its int_cores_min exceeds
     *      every host's total capacity), so its claim is stale.
     *
     * The reservation map persists across ticks. The single invariant is
     * that a host's reservation belongs to the highest-priority layer
     * that has claimed it; every operation above respects this. A new
     * leader after failover starts with an empty map and rebuilds the
     * same set within one or two ticks via the reconcile step.
     *
     * @return total number of procs dispatched this tick
     */
    private int doTick() {
        // 1. SNAPSHOT
        List<BookableHost> hosts = readBookableHosts();
        if (hosts.isEmpty()) {
            // No bookable hosts. Leave existing reservations alone; they
            // belong to layers whose hosts are simply unavailable this tick.
            return 0;
        }

        // 2. GROUP
        Map<HostSpecKey, List<BookableHost>> groups = groupByHostSpec(hosts);
        Set<String> seenLayerIds = new HashSet<>();

        int dispatched = 0;
        for (Map.Entry<HostSpecKey, List<BookableHost>> g : groups.entrySet()) {
            HostSpecKey spec = g.getKey();
            List<BookableHost> groupHosts = g.getValue();

            // 3a. CANDIDATE QUERY (one per group)
            // Filter against max host *total* cores in the group, not max
            // idle. A blocked layer waiting on a partially-loaded reserved
            // host must remain in the candidate set so its reservation
            // survives sweep.
            int maxCoresTotalInGroup = groupHosts.stream()
                    .mapToInt(h -> h.coresTotal).max().orElse(0);

            List<LayerCandidate> candidates =
                    readLayerCandidatesForGroup(spec, maxCoresTotalInGroup);
            if (candidates.isEmpty()) continue;

            // 3b. DISPATCH AND RECONCILE (priority order)
            dispatched += dispatchGroupWithScoring(groupHosts, candidates, seenLayerIds);
        }

        // 4. AWAIT COMMITS: wait for all submitted bookings to finish so the
        // next tick's snapshot reflects them. The pool ran in parallel during
        // step 3b; this is just a barrier, not new latency.
        awaitCommitsDrain();

        // 5. SWEEP orphans
        reservations.entrySet().removeIf(e -> !seenLayerIds.contains(e.getValue().layerId));

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

    // ---- placement: layer-driven, best-fit -------------------------------

    /**
     * Layer-driven placement with persistent reservations. For each
     * candidate in priority order:
     *
     *   1. Dispatch loop: score every fitting host (respecting reservations)
     *      with {@link #placementScore} and pick the one with the lowest
     *      score. Dispatch via {@code dispatcher.dispatchHost(host, layer)}.
     *      If the chosen host carried a lower-priority reservation, override
     *      it to c. Loop until no fitting host, no waiting frames, or the
     *      job/show cap is reached.
     *   2. Reconcile: c's reservation count should equal c.waitingFrameCount
     *      (decremented as we dispatched). Drop excess; claim more if short.
     *
     * Layer ids are recorded in {@code seenLayerIds} so the end-of-tick sweep
     * can drop reservations for layers that left the dispatchable set.
     */
    private int dispatchGroupWithScoring(List<BookableHost> hosts,
                                         List<LayerCandidate> candidates,
                                         Set<String> seenLayerIds) {
        int dispatched = 0;
        for (LayerCandidate c : candidates) {
            seenLayerIds.add(c.layerId);

            if (c.jobCoresInUse  + c.layerCoresMin > c.jobMaxCores)    continue;
            if (c.showCoresInUse + c.layerCoresMin > c.showBurstCores) continue;

            while (true) {
                BookableHost best = null;
                double bestScore = Double.POSITIVE_INFINITY;
                for (BookableHost h : hosts) {
                    if (!reservationAllows(h, c)) continue;
                    if (!fitsOnHost(c, h))        continue;
                    double score = placementScore(h, c);
                    if (score < bestScore) {
                        bestScore = score;
                        best = h;
                    }
                }
                if (best == null) break;     // no host can fit this layer

                // Estimate how many frames this commit will book. The
                // dispatcher books up to job_frame_dispatch_max per call,
                // bounded by the same fit checks placementScore uses.
                long maxMore = computeMaxMore(best, c);
                int  estFrames = (int) Math.min(jobFrameDispatchMax, maxMore + 1);
                if (estFrames <= 0) break;

                int  estCores  = estFrames * c.layerCoresMin;
                long estMem    = (long) estFrames * c.layerMemMin;
                int  estGpus   = estFrames * c.layerGpusMin;
                long estGpuMem = (long) estFrames * c.layerGpuMemMin;

                best.coresIdle      -= estCores;
                best.memIdle        -= estMem;
                best.gpusIdle       -= estGpus;
                best.gpuMemIdle     -= estGpuMem;
                c.jobCoresInUse     += estCores;
                c.showCoresInUse    += estCores;
                c.waitingFrameCount -= estFrames;

                // If the host carried a lower-priority reservation,
                // take ownership. A reservation by c or by anyone equal
                // or higher is preserved (the second case can't happen
                // here because reservationAllows already excluded it).
                Reservation existing = reservations.get(best.hostId);
                if (existing != null && existing.priority < c.priority) {
                    reservations.put(best.hostId,
                            new Reservation(c.layerId, c.priority));
                }

                submitCommit(best.hostId, c.layerId, c.priority);
                dispatched += estFrames;

                if (c.jobCoresInUse  + c.layerCoresMin > c.jobMaxCores)    break;
                if (c.showCoresInUse + c.layerCoresMin > c.showBurstCores) break;
            }

            reconcileReservationsForLayer(c, hosts);
        }
        return dispatched;
    }

    /**
     * A host's reservation lets c through if there is no reservation, the
     * reservation belongs to c, or the existing reservation is strictly
     * lower priority (in which case c may override on successful dispatch).
     */
    private boolean reservationAllows(BookableHost h, LayerCandidate c) {
        Reservation r = reservations.get(h.hostId);
        return r == null
            || r.layerId.equals(c.layerId)
            || r.priority < c.priority;
    }

    /**
     * Ensure c holds exactly c.waitingFrameCount reservations. Drops excess
     * (e.g., we just dispatched some frames so we need fewer reservations)
     * or claims more via {@link #pickReservationTarget}.
     */
    private void reconcileReservationsForLayer(LayerCandidate c, List<BookableHost> hosts) {
        List<String> mine = new ArrayList<>();
        for (Map.Entry<String, Reservation> e : reservations.entrySet()) {
            if (e.getValue().layerId.equals(c.layerId)) mine.add(e.getKey());
        }

        int have = mine.size();
        int need = Math.max(0, c.waitingFrameCount);

        if (have > need) {
            for (String hostId : mine.subList(need, have)) {
                reservations.remove(hostId);
            }
        } else if (have < need) {
            int want = need - have;
            for (int i = 0; i < want; i++) {
                BookableHost t = pickReservationTarget(c, hosts);
                if (t == null) break;       // no more eligible host
                Reservation existing = reservations.get(t.hostId);
                if (existing != null && existing.priority < c.priority) {
                    logger.info("Scheduler: override reservation host=" + t.hostName
                            + " layer=" + existing.layerId + "(p=" + existing.priority
                            + ") -> " + c.layerId + "(p=" + c.priority + ")");
                }
                reservations.put(t.hostId, new Reservation(c.layerId, c.priority));
            }
        }
    }

    /**
     * Pick the host most likely to become available for c soonest, expressed
     * as "host with the fewest running procs": fewer running frames means
     * fewer to wait on before the host frees up enough cores for c. The
     * host must (a) be tag/OS-compatible (granted by group membership),
     * (b) have enough TOTAL capacity for c when fully idle, and (c) not be
     * reserved at equal or higher priority for a different layer.
     */
    private BookableHost pickReservationTarget(LayerCandidate c, List<BookableHost> hosts) {
        BookableHost best = null;
        int bestProcs = Integer.MAX_VALUE;
        for (BookableHost h : hosts) {
            if (h.coresTotal  < c.layerCoresMin)   continue;
            if (h.memTotal    < c.layerMemMin)     continue;
            if (h.gpusTotal   < c.layerGpusMin)    continue;
            if (h.gpuMemTotal < c.layerGpuMemMin)  continue;
            if (!reservationAllows(h, c))          continue;
            if (h.runningProcs < bestProcs) {
                bestProcs = h.runningProcs;
                best = h;
            }
        }
        return best;
    }

    /**
     * Placement score for a (host, layer) pair. Lower is better. Callers MUST
     * call {@link #fitsOnHost} first; this function assumes the layer fits.
     *
     * Multi-resource stranding score (simple E-PVM, after Amir, Awerbuch,
     * Barak, Borgstrom &amp; Keren 2000; Verma et al. 2015 Borg paper). The
     * surplus left on a host after placing one frame is NOT directly the
     * score, because the host will get more frames of the same layer in
     * subsequent dispatches within the same tick. What is actually wasted
     * is the surplus that remains AFTER packing the host with as many
     * frames of this layer as the dispatcher would let in.
     *
     * The packing prediction mirrors the dispatcher's per-frame fit
     * checks in {@code CoreUnitDispatcher.dispatchHost(host, job)},
     * evaluated for a per-tick total:
     *
     *   - physical fit on every dimension
     *     (host.idle_D &gt;= layer.min_D, in the per-frame loop)
     *   - job's int_max_cores cap (isJobBookable check)
     *   - show's subscription int_burst cap (isShowAtOrOverBurst check)
     *
     * Per-call caps host_frame_dispatch_max and job_frame_dispatch_max
     * are intentionally NOT applied: they only bound how many dispatch
     * CALLS the Scheduler's loop will need, not the per-tick total. The
     * post-book MEM_RESERVED_MIN floor breaks the current call but allows
     * the next call to book one more frame, so it does not change the
     * count materially.
     *
     * Algorithm:
     *
     *     remaining_D  = h.idle_D - layer.min_D
     *     maxMore      = min over all caps above of (capacity / layer.min_D)
     *     stranded_D   = remaining_D - maxMore * layer.min_D
     *     score        = sum over D of W_D * stranded_D
     *
     * Stranded_D is the residual capacity on dimension D after the host
     * is packed: capacity that this layer cannot consume because some
     * other dimension or cap exhausted first.
     *
     * Effect: hosts whose resource ratio matches the layer score near
     * zero regardless of absolute size; only hosts with the wrong shape
     * (excess on some dimension) get penalized. Reservations are the
     * mechanism that protects big hosts for big layers; best-fit no
     * longer has to do that by penalizing big-host placements.
     *
     * Examples (defaults W_CORES=1, W_MEM=1, W_GPUS=4, W_GPU_MEM=1)
     * for a 4-core 4GB layer, ignoring job/show caps:
     *
     *     host  4 cores   4GB:   maxMore=0     score = 0
     *     host 64 cores  64GB:   maxMore=15    score = 0    (ratio match)
     *     host  4 cores  64GB:   maxMore=0     score = 60   (mem stranded)
     *     host 64 cores 256GB:   maxMore=15    score = 192  (mem stranded)
     */
    static double placementScore(BookableHost h, LayerCandidate c) {
        long maxMore = computeMaxMore(h, c);
        long remCores  = h.coresIdle  - c.layerCoresMin;
        long remMem    = h.memIdle    - c.layerMemMin;
        long remGpus   = h.gpusIdle   - c.layerGpusMin;
        long remGpuMem = h.gpuMemIdle - c.layerGpuMemMin;

        double strandCores  = c.layerCoresMin  > 0
                ? (remCores  - maxMore * c.layerCoresMin)  / CORE_POINTS_PER_CORE : 0;
        double strandMem    = c.layerMemMin    > 0
                ? (remMem    - maxMore * c.layerMemMin)    / KB_PER_GB           : 0;
        double strandGpus   = c.layerGpusMin   > 0
                ?  remGpus   - maxMore * c.layerGpusMin                           : 0;
        double strandGpuMem = c.layerGpuMemMin > 0
                ? (remGpuMem - maxMore * c.layerGpuMemMin) / KB_PER_GB           : 0;

        return W_CORES   * strandCores
             + W_MEM     * strandMem
             + W_GPUS    * strandGpus
             + W_GPU_MEM * strandGpuMem;
    }

    /**
     * Predict the number of ADDITIONAL frames of c (beyond the first) that
     * could be dispatched to h within this tick. Shared by placementScore
     * (which uses it to compute stranding) and the dispatch loop (which
     * uses it to estimate the frames a single commit will book).
     *
     * Caps applied (mirroring the dispatcher's per-frame fit checks):
     *   - physical fit on each dimension
     *   - job int_max_cores  (matches isJobBookable)
     *   - show int_burst     (matches isShowAtOrOverBurst)
     *
     * Per-call caps host_frame_dispatch_max and job_frame_dispatch_max
     * are NOT applied here because they bound a single dispatch CALL, not
     * the per-tick total. The dispatch loop applies job_frame_dispatch_max
     * when estimating a single commit's worth of frames.
     */
    static long computeMaxMore(BookableHost h, LayerCandidate c) {
        long remCores  = h.coresIdle  - c.layerCoresMin;
        long remMem    = h.memIdle    - c.layerMemMin;
        long remGpus   = h.gpusIdle   - c.layerGpusMin;
        long remGpuMem = h.gpuMemIdle - c.layerGpuMemMin;

        long maxMore = Long.MAX_VALUE;
        if (c.layerCoresMin  > 0) maxMore = Math.min(maxMore, remCores  / c.layerCoresMin);
        if (c.layerMemMin    > 0) maxMore = Math.min(maxMore, remMem    / c.layerMemMin);
        if (c.layerGpusMin   > 0) maxMore = Math.min(maxMore, remGpus   / c.layerGpusMin);
        if (c.layerGpuMemMin > 0) maxMore = Math.min(maxMore, remGpuMem / c.layerGpuMemMin);

        if (c.layerCoresMin > 0) {
            long jobRem = (long) c.jobMaxCores - c.jobCoresInUse - c.layerCoresMin;
            if (jobRem < 0) jobRem = 0;
            maxMore = Math.min(maxMore, jobRem / c.layerCoresMin);
        }
        if (c.layerCoresMin > 0) {
            long showRem = (long) c.showBurstCores - c.showCoresInUse - c.layerCoresMin;
            if (showRem < 0) showRem = 0;
            maxMore = Math.min(maxMore, showRem / c.layerCoresMin);
        }
        if (maxMore == Long.MAX_VALUE) maxMore = 0;
        return maxMore;
    }

    // ---- commit pool: submission, worker, drain ---------------------------

    private void submitCommit(String hostId, String layerId, int priority) {
        inFlightCommits.incrementAndGet();
        commitQueue.add(new CommitTask(hostId, layerId, priority));
    }

    /**
     * Worker loop. Pulls highest-priority CommitTask off the queue and
     * runs the dispatch call. Errors are logged and ignored; the version
     * lock on frame.int_version is the safety net for cross-worker
     * collisions on overlapping frame snapshots.
     */
    private void commitWorker() {
        while (!Thread.currentThread().isInterrupted()) {
            CommitTask t;
            try {
                t = commitQueue.take();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                DispatchHost host = hostManager.getDispatchHost(t.hostId);
                LayerInterface layer = jobManager.getLayer(t.layerId);
                dispatcher.dispatchHost(host, layer);
            } catch (RuntimeException e) {
                logger.debug("Scheduler commit failed (host=" + t.hostId
                        + ", layer=" + t.layerId + "): " + e.getMessage());
            } finally {
                inFlightCommits.decrementAndGet();
            }
        }
    }

    /**
     * Block until all submitted commits have completed. Called at the end
     * of each tick so the next tick's snapshot reflects this tick's
     * bookings. Polls every 2ms; the planner thread has nothing else to
     * do until commits drain.
     */
    private void awaitCommitsDrain() {
        while (inFlightCommits.get() > 0) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
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
        // Total capacity. Used by pickReservationTarget to check whether the
        // host could fit a layer when fully idle, independent of the host's
        // current load.
        int    coresTotal;
        long   memTotal;
        int    gpusTotal;
        long   gpuMemTotal;
        // Current idle resources. Decremented as we dispatch within a tick.
        int    coresIdle;
        long   memIdle;
        int    gpusIdle;
        long   gpuMemIdle;
        // Current running proc count. Used as the "soonest-to-free" heuristic
        // for reservation target selection.
        int    runningProcs;
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
        // Number of pending unfittable frames. Initialized from
        // waiting_frame_count in the candidate query; decremented as the
        // layer dispatches in this tick. Reconcile keeps the layer's
        // reservation count equal to this value.
        int    waitingFrameCount;
    }

    /**
     * A claim on a host by a specific layer at a specific priority. Stored
     * by host id. Persistent across ticks. The priority is what the override
     * comparison uses; storing it on the reservation (rather than looking it
     * up from the current candidate set) means an override decision works
     * even when the owner layer doesn't appear in the current group's
     * candidates.
     */
    static final class Reservation {
        final String layerId;
        final int    priority;
        Reservation(String layerId, int priority) {
            this.layerId  = layerId;
            this.priority = priority;
        }
    }

    /**
     * A pending dispatch. Submitted by the planner, consumed by a commit
     * pool worker. The PriorityBlockingQueue orders by descending priority
     * so under load high-priority layers drain first; same priority is
     * FCFS via insertion order at the queue's internal level (PBQ doesn't
     * guarantee FIFO across equal-priority items, but render-farm priority
     * granularity makes this immaterial).
     */
    static final class CommitTask {
        final String hostId;
        final String layerId;
        final int    priority;
        CommitTask(String hostId, String layerId, int priority) {
            this.hostId   = hostId;
            this.layerId  = layerId;
            this.priority = priority;
        }
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
