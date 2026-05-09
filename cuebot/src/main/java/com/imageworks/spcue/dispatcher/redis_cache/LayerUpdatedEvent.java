
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Event published when a layer is created or updated.
 * Used to sync layer resource requirements to Redis.
 */
public class LayerUpdatedEvent {

    private final String layerId;
    private final String jobId;
    private final String layerName;
    private final String layerType;
    private final int minCores;
    private final int maxCores;
    private final long minMemory;
    private final int minGpus;
    private final int maxGpus;
    private final long minGpuMemory;
    private final boolean threadable;
    private final String tags;
    private final String command;
    private final String range;
    private final int chunkSize;
    private final String services;
    // Layer limits: map of limitId -> maxValue
    private final Map<String, Integer> limits;

    public LayerUpdatedEvent(String layerId, String jobId, String layerName, String layerType,
                              int minCores, int maxCores, long minMemory,
                              int minGpus, int maxGpus, long minGpuMemory,
                              boolean threadable, String tags, String command,
                              String range, int chunkSize, String services) {
        this(layerId, jobId, layerName, layerType, minCores, maxCores, minMemory,
             minGpus, maxGpus, minGpuMemory, threadable, tags, command, range, chunkSize, services,
             Collections.emptyMap());
    }

    public LayerUpdatedEvent(String layerId, String jobId, String layerName, String layerType,
                              int minCores, int maxCores, long minMemory,
                              int minGpus, int maxGpus, long minGpuMemory,
                              boolean threadable, String tags, String command,
                              String range, int chunkSize, String services,
                              Map<String, Integer> limits) {
        this.layerId = layerId;
        this.jobId = jobId;
        this.layerName = layerName;
        this.layerType = layerType;
        this.minCores = minCores;
        this.maxCores = maxCores;
        this.minMemory = minMemory;
        this.minGpus = minGpus;
        this.maxGpus = maxGpus;
        this.minGpuMemory = minGpuMemory;
        this.threadable = threadable;
        this.tags = tags;
        this.command = command;
        this.range = range;
        this.chunkSize = chunkSize;
        this.services = services;
        this.limits = limits != null ? limits : Collections.emptyMap();
    }

    public String getLayerId() {
        return layerId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getLayerName() {
        return layerName;
    }

    public String getLayerType() {
        return layerType;
    }

    public int getMinCores() {
        return minCores;
    }

    public int getMaxCores() {
        return maxCores;
    }

    public long getMinMemory() {
        return minMemory;
    }

    public int getMinGpus() {
        return minGpus;
    }

    public int getMaxGpus() {
        return maxGpus;
    }

    public long getMinGpuMemory() {
        return minGpuMemory;
    }

    public boolean isThreadable() {
        return threadable;
    }

    public String getTags() {
        return tags;
    }

    public String getCommand() {
        return command;
    }

    public String getRange() {
        return range;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getServices() {
        return services;
    }

    /**
     * Get the limits that apply to this layer.
     * @return Map of limitId -> maxValue
     */
    public Map<String, Integer> getLimits() {
        return limits;
    }

    /**
     * Check if this layer has any limits.
     */
    public boolean hasLimits() {
        return limits != null && !limits.isEmpty();
    }
}
