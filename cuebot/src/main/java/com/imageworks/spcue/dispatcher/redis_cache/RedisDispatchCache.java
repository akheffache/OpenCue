
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.FrameDao;
import com.imageworks.spcue.grpc.host.ThreadMode;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Redis-based dispatch cache for fast FRAME lookups.
 *
 * Builds DispatchFrame objects entirely from Redis hashes for ZERO SQL on hot path.
 * Falls back to SQL only if Redis data is missing (cache miss).
 *
 * Job finding queries remain in SQL as they are already fast (simple index lookups).
 *
 * Redis data structures used:
 * - frame:{frameId} - Frame metadata hash
 * - layer:{layerId} - Layer metadata hash
 * - job:{jobId} - Job metadata hash (for building DispatchFrame)
 * - layers:waiting:{jobId} - Set of layer IDs with waiting frames
 * - frames:waiting:{layerId} - Sorted set of waiting frames
 */
@Repository
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatchCache {

    private static final Logger logger = LogManager.getLogger(RedisDispatchCache.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<List> findDispatchFramesScript;
    private final RedisScript<List> findDispatchFramesByLayerScript;
    private final FrameDao frameDao;

    // Redis key prefixes
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String JOB_PREFIX = "job:";
    private static final String LAYER_LIMITS_PREFIX = "layer:limits:";

    public RedisDispatchCache(RedisTemplate<String, String> redisTemplate,
                               RedisScript<List> findDispatchFramesScript,
                               RedisScript<List> findDispatchFramesByLayerScript,
                               FrameDao frameDao) {
        this.redisTemplate = redisTemplate;
        this.findDispatchFramesScript = findDispatchFramesScript;
        this.findDispatchFramesByLayerScript = findDispatchFramesByLayerScript;
        this.frameDao = frameDao;
        logger.info("Redis dispatcher DAO initialized (frame queries only)");
    }

    // ============================================================
    // FRAME DISPATCH METHODS
    // ============================================================

    /**
     * Find next dispatch frames for a job using Redis.
     * Builds DispatchFrame entirely from Redis - zero SQL on hot path.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit) {
        return findNextDispatchFrames(job, host, limit, false);
    }

    /**
     * Find next dispatch frames for a job (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(JobInterface job, DispatchHost host, int limit) {
        return findNextDispatchFrames(job, host, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearch(job, host, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                logger.debug("No eligible frames found in Redis for job {}", job.getJobId());
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, job.getJobId());

            logger.debug("Redis findNextDispatchFrames: found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search failed, returning empty list", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find next dispatch frames for a job using a VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit) {
        return findNextDispatchFrames(job, proc, limit, false);
    }

    /**
     * Find next dispatch frames for a job using a VirtualProc (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(JobInterface job, VirtualProc proc, int limit) {
        return findNextDispatchFrames(job, proc, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByProc(job, proc, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, job.getJobId());

            logger.debug("Redis findNextDispatchFrames (proc): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by proc failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find next dispatch frames for a specific layer using Redis.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit) {
        return findNextDispatchFrames(layer, host, limit, false);
    }

    /**
     * Find next dispatch frames for a specific layer (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(LayerInterface layer, DispatchHost host, int limit) {
        return findNextDispatchFrames(layer, host, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByLayer(layer, host, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                logger.debug("No eligible frames found in Redis for layer {}", layer.getLayerId());
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, layer.getJobId());

            logger.debug("Redis findNextDispatchFrames (layer): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by layer failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find next dispatch frames for a specific layer using a VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit) {
        return findNextDispatchFrames(layer, proc, limit, false);
    }

    /**
     * Find next dispatch frames for a specific layer using a VirtualProc (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(LayerInterface layer, VirtualProc proc, int limit) {
        return findNextDispatchFrames(layer, proc, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByLayerAndProc(layer, proc, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, layer.getJobId());

            logger.debug("Redis findNextDispatchFrames (layer+proc): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by layer and proc failed", e);
            return Collections.emptyList();
        }
    }

    // ============================================================
    // INTERNAL HELPER METHODS
    // ============================================================

    /**
     * Build DispatchFrame objects from Redis hashes.
     */
    private List<DispatchFrame> buildDispatchFramesFromRedis(List<String> frameIds, String jobId) {
        List<DispatchFrame> frames = new ArrayList<>(frameIds.size());

        Map<Object, Object> jobData = redisTemplate.opsForHash().entries(JOB_PREFIX + jobId);

        for (String frameId : frameIds) {
            try {
                DispatchFrame frame = buildDispatchFrameFromRedis(frameId, jobData);
                if (frame != null) {
                    frames.add(frame);
                }
            } catch (Exception e) {
                logger.debug("Failed to build frame {} from Redis, trying SQL fallback: {}",
                        frameId, e.getMessage());
                try {
                    DispatchFrame frame = frameDao.getDispatchFrame(frameId);
                    if (frame != null) {
                        frames.add(frame);
                    }
                } catch (Exception sqlEx) {
                    logger.debug("SQL fallback also failed for frame {}: {}", frameId, sqlEx.getMessage());
                }
            }
        }

        return frames;
    }

    /**
     * Build a single DispatchFrame from Redis hashes.
     */
    private DispatchFrame buildDispatchFrameFromRedis(String frameId, Map<Object, Object> jobData) {
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(FRAME_PREFIX + frameId);
        if (frameData == null || frameData.isEmpty()) {
            throw new RuntimeException("Frame data not found in Redis: " + frameId);
        }

        String layerId = getString(frameData, "layerId");
        if (layerId == null) {
            throw new RuntimeException("Layer ID not found in frame data: " + frameId);
        }

        Map<Object, Object> layerData = redisTemplate.opsForHash().entries(LAYER_PREFIX + layerId);
        if (layerData == null || layerData.isEmpty()) {
            throw new RuntimeException("Layer data not found in Redis: " + layerId);
        }

        DispatchFrame frame = new DispatchFrame();

        // Frame fields
        frame.id = frameId;
        frame.layerId = layerId;
        frame.jobId = getString(frameData, "jobId");
        frame.name = getString(frameData, "name");
        frame.retries = getInt(frameData, "retries", 0);
        frame.state = FrameState.valueOf(getString(frameData, "state", "WAITING"));

        // Layer fields
        frame.layerName = getString(layerData, "name");
        frame.command = getString(layerData, "command");
        frame.range = getString(layerData, "range");
        frame.chunkSize = getInt(layerData, "chunkSize", 1);
        frame.services = getString(layerData, "services");
        frame.minCores = getInt(layerData, "minCores", 100);
        frame.maxCores = getInt(layerData, "maxCores", 0);
        frame.threadable = getBoolean(layerData, "threadable", false);
        frame.minGpus = getInt(layerData, "minGpus", 0);
        frame.maxGpus = getInt(layerData, "maxGpus", 0);
        frame.minGpuMemory = getLong(layerData, "minGpuMemory", 0);
        frame.setMinMemory(getLong(layerData, "minMemory", 0));

        // Job fields (for building complete DispatchFrame)
        frame.show = getString(jobData, "showName");
        frame.shot = getString(jobData, "shot");
        frame.owner = getString(jobData, "owner");
        frame.uid = getOptionalInt(jobData, "uid");
        frame.logDir = getString(jobData, "logDir");
        frame.jobName = getString(jobData, "jobName");
        frame.os = getString(jobData, "os");
        frame.lokiURL = getString(jobData, "lokiURL");

        if (frame.showId == null) {
            frame.showId = getString(jobData, "showId");
        }
        if (frame.facilityId == null) {
            frame.facilityId = getString(jobData, "facilityId");
        }

        return frame;
    }

    // Helper methods for safe type conversion
    private String getString(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : "";
    }

    private String getString(Map<Object, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null && !value.toString().isEmpty() ? value.toString() : defaultValue;
    }

    private int getInt(Map<Object, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long getLong(Map<Object, Object> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(Map<Object, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private Optional<Integer> getOptionalInt(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(value.toString()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    // ============================================================
    // LUA SCRIPT EXECUTION METHODS
    // ============================================================

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearch(JobInterface job, DispatchHost host, int limit, boolean noGpu) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;

        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(host.idleGpus),
                String.valueOf(host.idleGpuMemory),
                host.tags != null ? host.tags : "",
                String.valueOf(threadMode),
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByProc(JobInterface job, VirtualProc proc, int limit, boolean noGpu) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();

        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(proc.coresReserved),
                String.valueOf(proc.memoryReserved),
                String.valueOf(proc.gpusReserved),
                String.valueOf(proc.gpuMemoryReserved),
                proc.tags != null ? proc.tags : "",
                "1", // Proc dispatch doesn't check threadable
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByLayer(LayerInterface layer, DispatchHost host, int limit, boolean noGpu) {
        String layerKey = LAYER_PREFIX + layer.getLayerId();
        String framesWaitingKey = FRAMES_WAITING_PREFIX + layer.getLayerId();
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layer.getLayerId();
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;

        return redisTemplate.execute(
                findDispatchFramesByLayerScript,
                java.util.Arrays.asList(layerKey, framesWaitingKey, layerLimitsKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(host.idleGpus),
                String.valueOf(host.idleGpuMemory),
                host.tags != null ? host.tags : "",
                String.valueOf(threadMode),
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByLayerAndProc(LayerInterface layer, VirtualProc proc, int limit, boolean noGpu) {
        String layerKey = LAYER_PREFIX + layer.getLayerId();
        String framesWaitingKey = FRAMES_WAITING_PREFIX + layer.getLayerId();
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layer.getLayerId();

        return redisTemplate.execute(
                findDispatchFramesByLayerScript,
                java.util.Arrays.asList(layerKey, framesWaitingKey, layerLimitsKey),
                String.valueOf(proc.coresReserved),
                String.valueOf(proc.memoryReserved),
                String.valueOf(proc.gpusReserved),
                String.valueOf(proc.gpuMemoryReserved),
                proc.tags != null ? proc.tags : "",
                "1", // Proc dispatch doesn't check threadable
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    /**
     * Check if Redis has data for a job (layers with waiting frames).
     */
    public boolean hasJobData(String jobId) {
        try {
            String key = LAYERS_WAITING_PREFIX + jobId;
            Long size = redisTemplate.opsForSet().size(key);
            return size != null && size > 0;
        } catch (Exception e) {
            logger.debug("Failed to check Redis for job {}: {}", jobId, e.getMessage());
            return false;
        }
    }
}
