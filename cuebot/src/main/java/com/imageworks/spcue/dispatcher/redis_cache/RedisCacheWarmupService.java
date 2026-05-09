
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

package com.imageworks.spcue.dispatcher.redis_cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.imageworks.spcue.JobDetail;
import com.imageworks.spcue.dao.JobDao;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Populates Redis cache from SQL on application startup.
 *
 * This ensures Redis has correct data after a cuebot restart.
 * Once populated, incremental updates are handled by event publishing.
 *
 * The warm-up runs SYNCHRONOUSLY at startup - the application will not
 * accept scheduling requests until warmup is complete. This is simpler
 * and safer than async warmup with ready flags.
 */
@Service
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisCacheWarmupService {

    private static final Logger logger = LogManager.getLogger(RedisCacheWarmupService.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final JobDao jobDao;

    // Redis key prefixes (same as in RedisSchedulingEventListener)
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String JOB_PREFIX = "job:";
    private static final String LIMIT_PREFIX = "limit:";
    private static final String LAYER_LIMITS_PREFIX = "layer:limits:";

    // Distributed lock for warmup (prevents concurrent warmup from multiple cuebots)
    private static final String WARMUP_LOCK_KEY = "cuebot:warmup:lock";
    private static final long WARMUP_LOCK_TIMEOUT_MINUTES = 5;

    // Unique instance ID for lock ownership
    private final String instanceId = UUID.randomUUID().toString();

    @Autowired
    public RedisCacheWarmupService(RedisTemplate<String, String> redisTemplate,
                                    JdbcTemplate jdbcTemplate,
                                    JobDao jobDao) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.jobDao = jobDao;
    }

    /**
     * Warm up Redis cache at application startup.
     * Runs SYNCHRONOUSLY - application won't accept requests until complete.
     *
     * Uses a distributed lock to prevent concurrent warmup from multiple cuebots.
     * If another instance is already warming up, this instance skips warmup
     * (the other instance will populate Redis, and we have SQL fallback).
     */
    @PostConstruct
    public void init() {
        logger.info("Attempting to acquire warmup lock (instance: {})...", instanceId);

        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(WARMUP_LOCK_KEY, instanceId, WARMUP_LOCK_TIMEOUT_MINUTES, TimeUnit.MINUTES);

        if (Boolean.TRUE.equals(acquired)) {
            logger.info("Warmup lock acquired, starting Redis cache warm-up...");
            try {
                warmupCache();
            } finally {
                // Only release if we still own the lock (could have expired)
                String currentOwner = redisTemplate.opsForValue().get(WARMUP_LOCK_KEY);
                if (instanceId.equals(currentOwner)) {
                    redisTemplate.delete(WARMUP_LOCK_KEY);
                    logger.info("Warmup lock released");
                } else {
                    logger.warn("Warmup lock expired or was taken by another instance");
                }
            }
        } else {
            logger.info("Another cuebot instance is warming up Redis, skipping (SQL fallback available)");
        }
    }

    /**
     * Main warm-up method - populates Redis from SQL.
     */
    public void warmupCache() {
        long startTime = System.currentTimeMillis();
        logger.info("Starting Redis cache warm-up from SQL...");

        try {
            // Clear existing scheduling data (in case of stale data)
            clearSchedulingData();

            // Populate limits first (they're referenced by layers)
            int limitCount = warmupLimits();

            // Populate job metadata (needed for building DispatchFrame objects)
            // Note: Job finding queries stay in SQL - only frame dispatch uses Redis
            int jobCount = warmupJobMetadata();

            // Populate layers and their limits
            int layerCount = warmupLayers();

            // Populate waiting frames
            int frameCount = warmupWaitingFrames();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Redis cache warm-up completed in {}ms: {} jobs, {} layers, {} frames, {} limits",
                    duration, jobCount, layerCount, frameCount, limitCount);

        } catch (Exception e) {
            logger.error("Redis cache warm-up failed", e);
            // Don't throw - the system can still work with SQL fallback
        }
    }

    /**
     * Clear existing scheduling data from Redis.
     * Uses SCAN to find and delete keys without blocking Redis for too long.
     */
    private void clearSchedulingData() {
        logger.info("Clearing existing scheduling data from Redis...");

        String[] patterns = {
            "frame:*",
            "layer:*",
            "job:*",
            "frames:waiting:*",
            "layers:waiting:*",
            "limit:*"
        };

        int totalDeleted = 0;
        for (String pattern : patterns) {
            Set<String> keysToDelete = new HashSet<>();

            // Use SCAN via keys() - Spring Data Redis handles cursor internally
            // Note: For very large datasets, consider using scan() with ScanOptions
            var keys = redisTemplate.keys(pattern);
            if (keys != null) {
                keysToDelete.addAll(keys);
            }

            if (!keysToDelete.isEmpty()) {
                redisTemplate.delete(keysToDelete);
                totalDeleted += keysToDelete.size();
                logger.debug("Deleted {} keys matching pattern {}", keysToDelete.size(), pattern);
            }
        }

        logger.info("Cleared {} existing scheduling keys from Redis", totalDeleted);
    }

    /**
     * Warm up limit data.
     */
    private int warmupLimits() {
        String sql = "SELECT lr.pk_limit_record, lr.str_name, lr.int_max_value, " +
                     "COALESCE(SUM(ls.int_running_count), 0) AS int_running " +
                     "FROM limit_record lr " +
                     "LEFT JOIN layer_limit ll ON ll.pk_limit_record = lr.pk_limit_record " +
                     "LEFT JOIN layer_stat ls ON ls.pk_layer = ll.pk_layer " +
                     "GROUP BY lr.pk_limit_record, lr.str_name, lr.int_max_value";

        List<Map<String, Object>> limits = jdbcTemplate.queryForList(sql);
        int count = 0;

        for (Map<String, Object> row : limits) {
            String limitId = (String) row.get("pk_limit_record");
            int maxValue = ((Number) row.get("int_max_value")).intValue();
            int running = ((Number) row.get("int_running")).intValue();

            // Store limit metadata
            String limitKey = LIMIT_PREFIX + limitId;
            redisTemplate.opsForHash().put(limitKey, "maxValue", String.valueOf(maxValue));
            redisTemplate.opsForHash().put(limitKey, "name", (String) row.get("str_name"));

            // Store running count
            String runningKey = LIMIT_PREFIX + limitId + ":running";
            redisTemplate.opsForValue().set(runningKey, String.valueOf(running));

            count++;
        }

        logger.debug("Warmed up {} limits", count);
        return count;
    }

    /**
     * Warm up job metadata for pending jobs.
     *
     * Note: Job FINDING queries stay in SQL (they're already fast - simple index lookups).
     * We only cache job metadata so we can build DispatchFrame objects from Redis frame data.
     */
    private int warmupJobMetadata() {
        String sql = "SELECT j.pk_job, j.pk_show, j.pk_folder, j.pk_facility, " +
                     "j.str_name, j.str_state, j.b_paused, j.str_os, j.str_shot, j.str_user, j.str_log_dir, " +
                     "s.str_name AS show_name, " +
                     "jr.int_priority, jr.int_cores, jr.int_min_cores, jr.int_max_cores, " +
                     "jr.int_gpus, jr.int_max_gpus, " +
                     "fr.int_cores AS folder_cores, fr.int_max_cores AS folder_max_cores, " +
                     "fr.int_gpus AS folder_gpus, fr.int_max_gpus AS folder_max_gpus, " +
                     "EXTRACT(EPOCH FROM j.ts_updated) AS ts_updated " +
                     "FROM job j " +
                     "JOIN show s ON s.pk_show = j.pk_show " +
                     "JOIN job_resource jr ON jr.pk_job = j.pk_job " +
                     "JOIN folder_resource fr ON fr.pk_folder = j.pk_folder " +
                     "WHERE j.str_state = 'PENDING' AND j.b_paused = false";

        List<Map<String, Object>> jobs = jdbcTemplate.queryForList(sql);
        int count = 0;

        for (Map<String, Object> row : jobs) {
            String jobId = (String) row.get("pk_job");
            String showId = (String) row.get("pk_show");
            String facilityId = (String) row.get("pk_facility");

            // Store job metadata (needed for building DispatchFrame objects)
            String jobKey = JOB_PREFIX + jobId;
            Map<String, String> jobData = new HashMap<>();
            jobData.put("showId", showId);
            jobData.put("facilityId", facilityId);
            jobData.put("folderId", (String) row.get("pk_folder"));
            jobData.put("state", (String) row.get("str_state"));
            jobData.put("paused", String.valueOf(row.get("b_paused")));
            jobData.put("os", nullToEmpty(row.get("str_os")));
            jobData.put("priority", String.valueOf(row.get("int_priority")));
            jobData.put("cores", String.valueOf(row.get("int_cores")));
            jobData.put("minCores", String.valueOf(row.get("int_min_cores")));
            jobData.put("maxCores", String.valueOf(row.get("int_max_cores")));
            jobData.put("gpus", String.valueOf(row.get("int_gpus")));
            jobData.put("maxGpus", String.valueOf(row.get("int_max_gpus")));
            jobData.put("tsUpdated", String.valueOf(((Number) row.get("ts_updated")).longValue()));
            jobData.put("folderCores", String.valueOf(row.get("folder_cores")));
            jobData.put("folderMaxCores", String.valueOf(row.get("folder_max_cores")));
            jobData.put("folderGpus", String.valueOf(row.get("folder_gpus")));
            jobData.put("folderMaxGpus", String.valueOf(row.get("folder_max_gpus")));
            jobData.put("showName", (String) row.get("show_name"));
            jobData.put("jobName", (String) row.get("str_name"));
            jobData.put("shot", nullToEmpty(row.get("str_shot")));
            jobData.put("owner", nullToEmpty(row.get("str_user")));
            jobData.put("logDir", nullToEmpty(row.get("str_log_dir")));

            redisTemplate.opsForHash().putAll(jobKey, jobData);
            count++;
        }

        logger.debug("Warmed up {} job metadata entries", count);
        return count;
    }

    /**
     * Warm up metadata for a single job using JobDao.
     * Called when a new job is launched after startup.
     * Uses the DAO instead of raw SQL for type safety and maintainability.
     */
    private void warmupSingleJobMetadata(String jobId) {
        JobDetail job = jobDao.getJobDetail(jobId);

        if (job == null) {
            logger.warn("Job {} not found when warming up metadata", jobId);
            return;
        }

        String jobKey = JOB_PREFIX + jobId;
        Map<String, String> jobData = new HashMap<>();

        // Core identifiers
        jobData.put("showId", nullToEmpty(job.showId));
        jobData.put("facilityId", nullToEmpty(job.facilityId));
        jobData.put("folderId", nullToEmpty(job.groupId));

        // Job state
        jobData.put("state", job.state != null ? job.state.toString() : "");
        jobData.put("paused", String.valueOf(job.isPaused));

        // Display/execution info (needed for DispatchFrame)
        jobData.put("showName", nullToEmpty(job.showName));
        jobData.put("jobName", nullToEmpty(job.name));
        jobData.put("shot", nullToEmpty(job.shot));
        jobData.put("owner", nullToEmpty(job.user));
        jobData.put("logDir", nullToEmpty(job.logDir));
        jobData.put("os", nullToEmpty(job.os));
        jobData.put("lokiURL", nullToEmpty(job.logLokiURL));

        // UID (optional)
        if (job.uid != null && job.uid.isPresent()) {
            jobData.put("uid", String.valueOf(job.uid.get()));
        }

        // Resource constraints
        jobData.put("priority", String.valueOf(job.priority));
        jobData.put("minCores", String.valueOf(job.minCoreUnits));
        jobData.put("maxCores", String.valueOf(job.maxCoreUnits));
        jobData.put("minGpus", String.valueOf(job.minGpuUnits));
        jobData.put("maxGpus", String.valueOf(job.maxGpuUnits));

        redisTemplate.opsForHash().putAll(jobKey, jobData);
        logger.debug("Warmed up job metadata for {}", jobId);
    }

    /**
     * Warm up layers for pending jobs (all jobs).
     */
    private int warmupLayers() {
        return warmupLayers(null);
    }

    /**
     * Warm up layers - shared implementation for both startup and single-job warmup.
     * Uses Redis pipelining to batch layer metadata commands into a single round-trip.
     *
     * @param jobId If null, warms up all pending jobs. If specified, warms up only that job.
     */
    private int warmupLayers(String jobId) {
        String sql = "SELECT l.pk_layer, l.pk_job, l.str_name, l.str_type, l.str_tags, " +
                     "l.str_cmd, l.str_range, l.int_chunk_size, l.str_services, " +
                     "l.int_cores_min, l.int_cores_max, l.int_mem_min, " +
                     "l.int_gpus_min, l.int_gpus_max, l.int_gpu_mem_min, l.b_threadable, " +
                     "ls.int_waiting_count " +
                     "FROM layer l " +
                     "JOIN layer_stat ls ON ls.pk_layer = l.pk_layer " +
                     (jobId == null
                         ? "JOIN job j ON j.pk_job = l.pk_job WHERE j.str_state = 'PENDING' AND j.b_paused = false"
                         : "WHERE l.pk_job = ?");

        List<Map<String, Object>> layers = (jobId == null)
            ? jdbcTemplate.queryForList(sql)
            : jdbcTemplate.queryForList(sql, jobId);

        if (layers.isEmpty()) {
            logger.debug("No layers to warm up{}", jobId != null ? " for job " + jobId : "");
            return 0;
        }

        // Use pipelining to batch all Redis commands into a single round-trip
        redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (Map<String, Object> row : layers) {
                    String layerId = (String) row.get("pk_layer");
                    String layerJobId = (String) row.get("pk_job");
                    int waitingCount = ((Number) row.get("int_waiting_count")).intValue();

                    // Store layer metadata
                    String layerKey = LAYER_PREFIX + layerId;
                    Map<String, String> layerData = new HashMap<>();
                    layerData.put("jobId", layerJobId);
                    layerData.put("name", (String) row.get("str_name"));
                    layerData.put("type", (String) row.get("str_type"));
                    layerData.put("tags", nullToEmpty(row.get("str_tags")));
                    layerData.put("command", nullToEmpty(row.get("str_cmd")));
                    layerData.put("range", nullToEmpty(row.get("str_range")));
                    layerData.put("chunkSize", String.valueOf(row.get("int_chunk_size")));
                    layerData.put("services", nullToEmpty(row.get("str_services")));
                    layerData.put("minCores", String.valueOf(row.get("int_cores_min")));
                    layerData.put("maxCores", String.valueOf(row.get("int_cores_max")));
                    layerData.put("minMemory", String.valueOf(row.get("int_mem_min")));
                    layerData.put("minGpus", String.valueOf(row.get("int_gpus_min")));
                    layerData.put("maxGpus", String.valueOf(row.get("int_gpus_max")));
                    layerData.put("minGpuMemory", String.valueOf(row.get("int_gpu_mem_min")));
                    layerData.put("threadable", String.valueOf(row.get("b_threadable")));

                    operations.opsForHash().putAll(layerKey, layerData);

                    // Track layers with waiting frames
                    if (waitingCount > 0) {
                        operations.opsForSet().add(LAYERS_WAITING_PREFIX + layerJobId, layerId);
                    }
                }
                return null;
            }
        });

        // Store layer limits (separate loop - requires SQL queries per layer)
        for (Map<String, Object> row : layers) {
            String layerId = (String) row.get("pk_layer");
            warmupLayerLimits(layerId);
        }

        logger.debug("Warmed up {} layers{}", layers.size(), jobId != null ? " for job " + jobId : "");
        return layers.size();
    }

    /**
     * Warm up limits for a specific layer.
     */
    private void warmupLayerLimits(String layerId) {
        String sql = "SELECT ll.pk_limit_record, lr.int_max_value " +
                     "FROM layer_limit ll " +
                     "JOIN limit_record lr ON lr.pk_limit_record = ll.pk_limit_record " +
                     "WHERE ll.pk_layer = ?";

        List<Map<String, Object>> limits = jdbcTemplate.queryForList(sql, layerId);

        if (!limits.isEmpty()) {
            String layerLimitsKey = LAYER_LIMITS_PREFIX + layerId;
            for (Map<String, Object> row : limits) {
                String limitId = (String) row.get("pk_limit_record");
                redisTemplate.opsForSet().add(layerLimitsKey, limitId);
            }
        }
    }

    /**
     * Warm up waiting frames (all jobs).
     */
    private int warmupWaitingFrames() {
        return warmupWaitingFrames(null);
    }

    /**
     * Warm up waiting frames - shared implementation for both startup and single-job warmup.
     * Uses Redis pipelining to batch all commands into a single round-trip.
     *
     * @param jobId If null, warms up all pending jobs. If specified, warms up only that job.
     */
    private int warmupWaitingFrames(String jobId) {
        String sql = "SELECT f.pk_frame, f.pk_layer, f.pk_job, f.str_name, " +
                     "f.int_dispatch_order, f.int_layer_order, f.int_retries, f.int_version " +
                     "FROM frame f " +
                     (jobId == null
                         ? "JOIN job j ON j.pk_job = f.pk_job " +
                           "WHERE f.str_state = 'WAITING' AND j.str_state = 'PENDING' AND j.b_paused = false"
                         : "WHERE f.pk_job = ? AND f.str_state = 'WAITING'");

        List<Map<String, Object>> frames = (jobId == null)
            ? jdbcTemplate.queryForList(sql)
            : jdbcTemplate.queryForList(sql, jobId);

        if (frames.isEmpty()) {
            logger.debug("No waiting frames to warm up{}", jobId != null ? " for job " + jobId : "");
            return 0;
        }

        // Use pipelining to batch all Redis commands into a single round-trip
        redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (Map<String, Object> row : frames) {
                    String frameId = (String) row.get("pk_frame");
                    String layerId = (String) row.get("pk_layer");
                    String frameJobId = (String) row.get("pk_job");
                    int dispatchOrder = ((Number) row.get("int_dispatch_order")).intValue();
                    int layerOrder = ((Number) row.get("int_layer_order")).intValue();

                    // Calculate sort score (same as event listener and SQL ORDER BY)
                    double sortScore = dispatchOrder + (layerOrder / 1000000.0);

                    // Add to waiting frames sorted set
                    String waitingKey = FRAMES_WAITING_PREFIX + layerId;
                    operations.opsForZSet().add(waitingKey, frameId, sortScore);

                    // Store frame metadata
                    String frameKey = FRAME_PREFIX + frameId;
                    Map<String, String> frameData = new HashMap<>();
                    frameData.put("layerId", layerId);
                    frameData.put("jobId", frameJobId);
                    frameData.put("state", "WAITING");
                    frameData.put("dispatchOrder", String.valueOf(dispatchOrder));
                    frameData.put("layerOrder", String.valueOf(layerOrder));
                    frameData.put("name", nullToEmpty(row.get("str_name")));
                    frameData.put("retries", String.valueOf(row.get("int_retries")));
                    frameData.put("version", String.valueOf(row.get("int_version")));

                    operations.opsForHash().putAll(frameKey, frameData);
                }
                return null;
            }
        });

        logger.debug("Warmed up {} waiting frames{}", frames.size(), jobId != null ? " for job " + jobId : "");
        return frames.size();
    }

    private String nullToEmpty(Object value) {
        return value != null ? value.toString() : "";
    }

    // ============================================================
    // SINGLE JOB WARMUP (called when new job is launched)
    // ============================================================

    /**
     * Warm up Redis cache for a single job.
     * Called when a new job is launched AFTER the initial startup warmup.
     *
     * This populates layers and frames for the job so Redis queries work
     * immediately without falling back to SQL.
     *
     * @param jobId The job ID to warm up
     */
    public void warmupJob(String jobId) {
        long startTime = System.currentTimeMillis();
        logger.info("Warming up Redis cache for job: {}", jobId);

        try {
            // Populate job metadata hash (needed for building DispatchFrame objects)
            warmupSingleJobMetadata(jobId);
            // Use shared methods with jobId filter
            int layerCount = warmupLayers(jobId);
            int frameCount = warmupWaitingFrames(jobId);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Job {} warmed up in {}ms: {} layers, {} frames",
                    jobId, duration, layerCount, frameCount);

        } catch (Exception e) {
            logger.error("Failed to warm up job {} in Redis", jobId, e);
            // Don't throw - SQL fallback will work
        }
    }
}
