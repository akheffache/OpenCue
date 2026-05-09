
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

package com.imageworks.spcue.dao;

import com.imageworks.spcue.FrameInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Interface for publishing scheduling-related events for FRAME queries.
 *
 * This abstraction allows frame state changes to be propagated to
 * external caching systems (like Redis) without coupling the DAO
 * layer to specific implementations.
 *
 * Job finding queries remain in SQL (they're already fast), so
 * we don't need job state change events.
 */
public interface SchedulingEventPublisher {

    /**
     * Publish a frame state change event.
     *
     * @param frame         The frame that changed
     * @param previousState The previous state (may be null for new frames)
     * @param newState      The new state
     */
    void publishFrameStateChanged(FrameInterface frame, FrameState previousState, FrameState newState);

    /**
     * Publish a frame state change with all DispatchFrame info.
     *
     * @param frameId        Frame ID
     * @param layerId        Layer ID
     * @param jobId          Job ID
     * @param previousState  Previous state
     * @param newState       New state
     * @param dispatchOrder  Frame dispatch order
     * @param layerOrder     Layer order
     * @param frameName      Frame name
     * @param retries        Number of retries
     * @param version        Frame version
     */
    void publishFrameStateChanged(String frameId, String layerId, String jobId,
                                   FrameState previousState, FrameState newState,
                                   int dispatchOrder, int layerOrder,
                                   String frameName, int retries, int version);

    /**
     * Publish a layer update event.
     *
     * @param layer The layer that was updated
     */
    void publishLayerUpdated(LayerInterface layer);

    /**
     * Notify that a job has completed and its cache data can be cleaned up.
     *
     * @param jobId      The job ID
     * @param showId     The show ID (can be null)
     * @param facilityId The facility ID (can be null)
     */
    void publishJobCompleted(String jobId, String showId, String facilityId);
}
