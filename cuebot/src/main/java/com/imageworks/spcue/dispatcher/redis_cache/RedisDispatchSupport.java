
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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.DispatcherDao;

/**
 * Redis-enhanced dispatch support for FRAME queries only.
 *
 * Provides a Redis-first approach for finding dispatch frames:
 * 1. Try Redis for fast frame lookup
 * 2. Fall back to SQL DAO if Redis fails or returns empty results
 *
 * Job finding queries remain in SQL as they are already fast
 * (simple index lookups returning just job IDs).
 *
 * This service is only active when redis.scheduling.enabled=true.
 */
@Service
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatchSupport {

    private static final Logger logger = LogManager.getLogger(RedisDispatchSupport.class);

    private final RedisDispatchCache redisDispatchCache;
    private final DispatcherDao sqlDispatcherDao;

    @Autowired
    public RedisDispatchSupport(RedisDispatchCache redisDispatchCache,
                                 DispatcherDao sqlDispatcherDao) {
        this.redisDispatchCache = redisDispatchCache;
        this.sqlDispatcherDao = sqlDispatcherDao;
        logger.info("Redis dispatch support initialized - Redis-first frame dispatch enabled");
    }

    /**
     * Find next dispatch frames using Redis with SQL fallback.
     *
     * @param job   The job to find frames for
     * @param host  The host with available resources
     * @param limit Maximum frames to return
     * @return List of dispatchable frames
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit) {
        long startTime = System.currentTimeMillis();

        // Check if Redis has data for this job
        if (redisDispatchCache.hasJobData(job.getJobId())) {
            List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, limit);

            if (!frames.isEmpty()) {
                logger.debug("Redis dispatch: found {} frames for job {} in {}ms",
                        frames.size(), job.getJobId(), System.currentTimeMillis() - startTime);
                return frames;
            }

            // Redis returned empty - could be a cache miss or truly no frames
            // Fall through to SQL to be safe
            logger.debug("Redis returned empty for job {}, falling back to SQL", job.getJobId());
        }

        // Fall back to SQL
        List<DispatchFrame> frames = sqlDispatcherDao.findNextDispatchFrames(job, host, limit);

        logger.debug("SQL dispatch fallback: found {} frames for job {} in {}ms",
                frames.size(), job.getJobId(), System.currentTimeMillis() - startTime);

        return frames;
    }

    /**
     * Find next dispatch frames using VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit) {
        long startTime = System.currentTimeMillis();

        if (redisDispatchCache.hasJobData(job.getJobId())) {
            List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, proc, limit);

            if (!frames.isEmpty()) {
                logger.debug("Redis dispatch (proc): found {} frames in {}ms",
                        frames.size(), System.currentTimeMillis() - startTime);
                return frames;
            }
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(job, proc, limit);
    }

    /**
     * Check if Redis dispatch is available for a job.
     */
    public boolean isRedisAvailableForJob(String jobId) {
        return redisDispatchCache.hasJobData(jobId);
    }

    // ============================================================
    // LAYER DISPATCH METHODS
    // ============================================================

    /**
     * Find next dispatch frames for a specific layer using Redis with SQL fallback.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit) {
        long startTime = System.currentTimeMillis();

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(layer, host, limit);

        if (!frames.isEmpty()) {
            logger.debug("Redis dispatch (layer): found {} frames in {}ms",
                    frames.size(), System.currentTimeMillis() - startTime);
            return frames;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(layer, host, limit);
    }

    /**
     * Find next dispatch frames for a specific layer using VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit) {
        long startTime = System.currentTimeMillis();

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(layer, proc, limit);

        if (!frames.isEmpty()) {
            logger.debug("Redis dispatch (layer+proc): found {} frames in {}ms",
                    frames.size(), System.currentTimeMillis() - startTime);
            return frames;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(layer, proc, limit);
    }
}
