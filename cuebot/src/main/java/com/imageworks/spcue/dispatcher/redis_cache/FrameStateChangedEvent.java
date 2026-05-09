
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

import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Event published when a frame's state changes.
 * Used to sync frame state to Redis for fast scheduling queries.
 */
public class FrameStateChangedEvent {

    private final String frameId;
    private final String layerId;
    private final String jobId;
    private final FrameState previousState;
    private final FrameState newState;
    private final int dispatchOrder;
    private final int layerOrder;
    // Additional DispatchFrame fields
    private final String frameName;
    private final int retries;
    private final int version;

    public FrameStateChangedEvent(String frameId, String layerId, String jobId,
                                   FrameState previousState, FrameState newState,
                                   int dispatchOrder, int layerOrder,
                                   String frameName, int retries, int version) {
        this.frameId = frameId;
        this.layerId = layerId;
        this.jobId = jobId;
        this.previousState = previousState;
        this.newState = newState;
        this.dispatchOrder = dispatchOrder;
        this.layerOrder = layerOrder;
        this.frameName = frameName;
        this.retries = retries;
        this.version = version;
    }

    public String getFrameId() {
        return frameId;
    }

    public String getLayerId() {
        return layerId;
    }

    public String getJobId() {
        return jobId;
    }

    public FrameState getPreviousState() {
        return previousState;
    }

    public FrameState getNewState() {
        return newState;
    }

    public int getDispatchOrder() {
        return dispatchOrder;
    }

    public int getLayerOrder() {
        return layerOrder;
    }

    public String getFrameName() {
        return frameName;
    }

    public int getRetries() {
        return retries;
    }

    public int getVersion() {
        return version;
    }

    /**
     * Calculate the sort score for Redis sorted set.
     * Just layerOrder - dispatchOrder is now stored on layer, not frame.
     * Frames within a layer are sorted by their frame number (layerOrder).
     */
    public double getSortScore() {
        // Just layerOrder - dispatchOrder is stored on layer instead
        return layerOrder;
    }
}
