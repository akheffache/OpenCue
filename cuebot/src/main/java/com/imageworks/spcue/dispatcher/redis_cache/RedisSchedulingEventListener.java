
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import com.imageworks.spcue.grpc.job.FrameState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Listens for FRAME state change events and syncs them to Redis.
 *
 * Job finding queries remain in SQL - they're already fast (simple index lookups).
 * Only frame dispatch queries use Redis.
 *
 * Uses @TransactionalEventListener(AFTER_COMMIT) to ensure sync happens
 * only after the SQL transaction commits successfully.
 *
 * Uses @Async to make Redis sync non-blocking - the main thread continues
 * immediately while Redis operations happen in the background.
 *
 * This ensures:
 * 1. SQL is always the source of truth
 * 2. Redis is eventually consistent (after commit)
 * 3. No performance impact on the main dispatch path
 */
@Component
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisSchedulingEventListener {

    private static final Logger logger = LogManager.getLogger(RedisSchedulingEventListener.class);

    private final RedisTemplate<String, String> redisTemplate;

    // Redis key prefixes
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String JOB_PREFIX = "job:";
    private static final String LIMIT_PREFIX = "limit:";
    private static final String LAYER_LIMITS_PREFIX = "layer:limits:";

    public RedisSchedulingEventListener(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        logger.info("Redis scheduling event listener initialized (frame events only)");
    }

    /**
     * Handle frame state changes.
     * - When frame becomes WAITING: add to waiting sorted set
     * - When frame leaves WAITING: remove from waiting sorted set
     * - When frame becomes RUNNING: increment limit counters
     * - When frame leaves RUNNING: decrement limit counters
     */
    @Async("redisAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onFrameStateChanged(FrameStateChangedEvent event) {
        String frameId = event.getFrameId();
        String layerId = event.getLayerId();
        String jobId = event.getJobId();
        FrameState previousState = event.getPreviousState();
        FrameState newState = event.getNewState();

        logger.debug("Frame state changed: {} from {} to {}", frameId, previousState, newState);

        try {
            String waitingSetKey = FRAMES_WAITING_PREFIX + layerId;

            if (newState == FrameState.WAITING) {
                // Frame became WAITING - add to sorted set with dispatch order as score
                redisTemplate.opsForZSet().add(waitingSetKey, frameId, event.getSortScore());

                // Track that this layer has waiting frames
                redisTemplate.opsForSet().add(LAYERS_WAITING_PREFIX + jobId, layerId);

                // Create frame metadata hash (only for WAITING frames)
                createFrameMetadata(event);

                logger.debug("Added frame {} to waiting set {} with score {}",
                        frameId, waitingSetKey, event.getSortScore());

            } else if (previousState == FrameState.WAITING) {
                // Frame left WAITING state - remove from sorted set and delete hash
                redisTemplate.opsForZSet().remove(waitingSetKey, frameId);
                redisTemplate.delete(FRAME_PREFIX + frameId);

                // Check if layer still has waiting frames and clean up layers:waiting set.
                // Note: There's a small race window where another thread could add a frame
                // between size() and remove(). However, this is self-healing because:
                // 1. When a frame becomes WAITING, it always does SADD to layers:waiting (line 98)
                // 2. So even if we incorrectly remove the layer, it gets re-added immediately
                // 3. Worst case: one dispatch cycle sees empty set, falls back to SQL
                Long waitingCount = redisTemplate.opsForZSet().size(waitingSetKey);
                if (waitingCount == null || waitingCount == 0) {
                    redisTemplate.opsForSet().remove(LAYERS_WAITING_PREFIX + jobId, layerId);
                }

                logger.debug("Removed frame {} from waiting set {} and deleted hash", frameId, waitingSetKey);
            }

            // Track limit running counts - CRITICAL for 1-to-1 parity with SQL
            updateLimitCounters(layerId, previousState, newState);

        } catch (Exception e) {
            logger.error("Failed to sync frame state to Redis: {}", frameId, e);
            // Don't throw - SQL is source of truth, Redis is best-effort cache
        }
    }

    /**
     * Update limit running counters when frames start/stop running.
     * This is CRITICAL for 1-to-1 parity with SQL scheduling.
     *
     * SQL calculates: SUM(layer_stat.int_running_count) for all layers sharing a limit
     * Redis tracks: limit:{limitId}:running counter (incremented/decremented atomically)
     */
    private void updateLimitCounters(String layerId, FrameState previousState, FrameState newState) {
        // Get limits for this layer
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layerId;
        Set<String> limitIds = redisTemplate.opsForSet().members(layerLimitsKey);

        if (limitIds == null || limitIds.isEmpty()) {
            return; // No limits for this layer
        }

        boolean wasRunning = (previousState == FrameState.RUNNING);
        boolean isRunning = (newState == FrameState.RUNNING);

        if (!wasRunning && isRunning) {
            // Frame started running - increment all limit counters
            for (String limitId : limitIds) {
                String runningKey = LIMIT_PREFIX + limitId + ":running";
                Long newCount = redisTemplate.opsForValue().increment(runningKey);
                logger.debug("Limit {} running count incremented to {}", limitId, newCount);
            }
        } else if (wasRunning && !isRunning) {
            // Frame stopped running - decrement all limit counters
            for (String limitId : limitIds) {
                String runningKey = LIMIT_PREFIX + limitId + ":running";
                Long newCount = redisTemplate.opsForValue().decrement(runningKey);
                // Ensure we don't go negative (safety check)
                if (newCount != null && newCount < 0) {
                    redisTemplate.opsForValue().set(runningKey, "0");
                    logger.warn("Limit {} running count went negative, reset to 0", limitId);
                } else {
                    logger.debug("Limit {} running count decremented to {}", limitId, newCount);
                }
            }
        }
    }

    /**
     * Handle layer updates - store layer resource requirements and limits.
     */
    @Async("redisAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onLayerUpdated(LayerUpdatedEvent event) {
        String layerId = event.getLayerId();

        logger.debug("Layer updated: {}", layerId);

        try {
            String layerKey = LAYER_PREFIX + layerId;

            Map<String, String> layerData = new HashMap<>();
            layerData.put("jobId", event.getJobId());
            layerData.put("name", event.getLayerName());
            layerData.put("type", event.getLayerType());
            layerData.put("minCores", String.valueOf(event.getMinCores()));
            layerData.put("maxCores", String.valueOf(event.getMaxCores()));
            layerData.put("minMemory", String.valueOf(event.getMinMemory()));
            layerData.put("minGpus", String.valueOf(event.getMinGpus()));
            layerData.put("maxGpus", String.valueOf(event.getMaxGpus()));
            layerData.put("minGpuMemory", String.valueOf(event.getMinGpuMemory()));
            layerData.put("threadable", String.valueOf(event.isThreadable()));
            layerData.put("command", event.getCommand() != null ? event.getCommand() : "");
            layerData.put("range", event.getRange() != null ? event.getRange() : "");
            layerData.put("chunkSize", String.valueOf(event.getChunkSize()));
            layerData.put("services", event.getServices() != null ? event.getServices() : "");

            redisTemplate.opsForHash().putAll(layerKey, layerData);

            // Store layer tags as SET for efficient matching (no string parsing in Lua)
            String layerTagsKey = layerKey + ":tags";
            redisTemplate.delete(layerTagsKey); // Clear existing tags
            Set<String> normalizedTags = RedisCacheLoadService.normalizeTags(event.getTags());
            if (!normalizedTags.isEmpty()) {
                redisTemplate.opsForSet().add(layerTagsKey, normalizedTags.toArray(new String[0]));
            }

            // Store layer limits - CRITICAL for 1-to-1 parity with SQL
            if (event.hasLimits()) {
                updateLayerLimits(layerId, event.getLimits());
            }

            logger.debug("Updated layer metadata in Redis: {}", layerId);

        } catch (Exception e) {
            logger.error("Failed to sync layer to Redis: {}", layerId, e);
        }
    }

    /**
     * Store layer limits in Redis.
     * - layer:limits:{layerId} = set of limitIds that apply to this layer
     * - limit:{limitId} = hash with maxValue
     * - limit:{limitId}:running = counter of currently running frames
     */
    private void updateLayerLimits(String layerId, Map<String, Integer> limits) {
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layerId;

        // Clear existing limits for this layer
        redisTemplate.delete(layerLimitsKey);

        for (Map.Entry<String, Integer> entry : limits.entrySet()) {
            String limitId = entry.getKey();
            int maxValue = entry.getValue();

            // Add limit to layer's limit set
            redisTemplate.opsForSet().add(layerLimitsKey, limitId);

            // Store/update limit metadata
            String limitKey = LIMIT_PREFIX + limitId;
            redisTemplate.opsForHash().put(limitKey, "maxValue", String.valueOf(maxValue));

            // Initialize running counter if not exists
            String runningKey = LIMIT_PREFIX + limitId + ":running";
            if (Boolean.FALSE.equals(redisTemplate.hasKey(runningKey))) {
                redisTemplate.opsForValue().set(runningKey, "0");
            }

            logger.debug("Stored limit {} with maxValue {} for layer {}", limitId, maxValue, layerId);
        }
    }

    /**
     * Create frame metadata hash in Redis (only for WAITING frames).
     * The hash is deleted when frame leaves WAITING state.
     */
    private void createFrameMetadata(FrameStateChangedEvent event) {
        String frameKey = FRAME_PREFIX + event.getFrameId();

        Map<String, String> frameData = new HashMap<>();
        frameData.put("layerId", event.getLayerId());
        frameData.put("jobId", event.getJobId());
        frameData.put("state", "WAITING");
        frameData.put("dispatchOrder", String.valueOf(event.getDispatchOrder()));
        frameData.put("layerOrder", String.valueOf(event.getLayerOrder()));
        // Additional DispatchFrame fields
        frameData.put("name", event.getFrameName() != null ? event.getFrameName() : "");
        frameData.put("retries", String.valueOf(event.getRetries()));
        frameData.put("version", String.valueOf(event.getVersion()));

        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }

    /**
     * Handle job completion - clean up Redis data.
     */
    @Async("redisAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onJobCompleted(RedisSchedulingEventPublisher.JobCompletedEvent event) {
        cleanupJob(event.getJobId());
    }

    /**
     * Remove all Redis data for a job (called when job completes/deletes).
     */
    public void cleanupJob(String jobId) {
        try {
            // Get all layers for this job
            String layersWaitingKey = LAYERS_WAITING_PREFIX + jobId;
            var layerIds = redisTemplate.opsForSet().members(layersWaitingKey);

            if (layerIds != null) {
                for (String layerId : layerIds) {
                    // Get all frame IDs from the waiting set before deleting
                    String waitingSetKey = FRAMES_WAITING_PREFIX + layerId;
                    var frameIds = redisTemplate.opsForZSet().range(waitingSetKey, 0, -1);

                    // Delete individual frame hashes
                    if (frameIds != null) {
                        for (String frameId : frameIds) {
                            redisTemplate.delete(FRAME_PREFIX + frameId);
                        }
                    }

                    // Delete waiting frames set for this layer
                    redisTemplate.delete(waitingSetKey);
                    // Delete layer metadata
                    redisTemplate.delete(LAYER_PREFIX + layerId);
                    // Delete layer limits set
                    redisTemplate.delete(LAYER_LIMITS_PREFIX + layerId);
                    // Delete layer tags set
                    redisTemplate.delete(LAYER_PREFIX + layerId + ":tags");
                }
            }

            // Delete the layers waiting set
            redisTemplate.delete(layersWaitingKey);

            // Delete job metadata
            redisTemplate.delete(JOB_PREFIX + jobId);

            logger.debug("Cleaned up Redis data for job: {}", jobId);

        } catch (Exception e) {
            logger.error("Failed to cleanup Redis data for job: {}", jobId, e);
        }
    }
}
