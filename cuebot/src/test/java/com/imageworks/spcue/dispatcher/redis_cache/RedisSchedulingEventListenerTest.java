
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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import com.imageworks.spcue.grpc.job.FrameState;
import com.imageworks.spcue.grpc.job.JobState;

@RunWith(MockitoJUnitRunner.class)
public class RedisSchedulingEventListenerTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @Mock
    private SetOperations<String, String> setOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private RedisSchedulingEventListener listener;

    @Before
    public void setUp() {
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        listener = new RedisSchedulingEventListener(redisTemplate);
    }

    // ============================================================
    // FRAME STATE CHANGE TESTS
    // ============================================================

    @Test
    public void testHandleFrameStateChange_WaitingToRunning() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.WAITING, FrameState.RUNNING,
                1, 1, "0001-test", 0, 1
        );

        when(setOperations.members("layer:limits:layer-1")).thenReturn(Collections.emptySet());

        listener.handleFrameStateChanged(event);

        // Frame should be removed from waiting queue
        verify(zSetOperations).remove("frames:waiting:layer-1", "frame-1");

        // Frame state should be updated
        verify(hashOperations).put("frame:frame-1", "state", "RUNNING");
    }

    @Test
    public void testHandleFrameStateChange_RunningToSucceeded() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.RUNNING, FrameState.SUCCEEDED,
                1, 1, "0001-test", 0, 1
        );

        when(setOperations.members("layer:limits:layer-1")).thenReturn(Collections.emptySet());

        listener.handleFrameStateChanged(event);

        // Frame should not be in waiting queue (already removed when it started running)
        verify(zSetOperations, never()).add(anyString(), anyString(), anyDouble());

        // Frame state should be updated
        verify(hashOperations).put("frame:frame-1", "state", "SUCCEEDED");
    }

    @Test
    public void testHandleFrameStateChange_DependToWaiting() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.DEPEND, FrameState.WAITING,
                5, 1, "0001-test", 0, 1
        );

        listener.handleFrameStateChanged(event);

        // Frame should be added to waiting queue with dispatch order as score
        verify(zSetOperations).add("frames:waiting:layer-1", "frame-1", 5.0);

        // Layer should be added to layers:waiting set
        verify(setOperations).add("layers:waiting:job-1", "layer-1");

        // Frame state should be updated
        verify(hashOperations).put("frame:frame-1", "state", "WAITING");
    }

    @Test
    public void testHandleFrameStateChange_RunningToDead() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.RUNNING, FrameState.DEAD,
                1, 1, "0001-test", 3, 1
        );

        when(setOperations.members("layer:limits:layer-1")).thenReturn(Collections.emptySet());

        listener.handleFrameStateChanged(event);

        // Frame state should be DEAD
        verify(hashOperations).put("frame:frame-1", "state", "DEAD");
    }

    @Test
    public void testHandleFrameStateChange_WithLimits_IncrementOnRunning() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.WAITING, FrameState.RUNNING,
                1, 1, "0001-test", 0, 1
        );

        Set<String> limits = new HashSet<>();
        limits.add("maya-license");
        when(setOperations.members("layer:limits:layer-1")).thenReturn(limits);

        listener.handleFrameStateChanged(event);

        // Limit counter should be incremented
        verify(valueOperations).increment("limit:maya-license:running");
    }

    @Test
    public void testHandleFrameStateChange_WithLimits_DecrementOnStop() {
        FrameStateChangedEvent event = new FrameStateChangedEvent(
                "frame-1", "layer-1", "job-1",
                FrameState.RUNNING, FrameState.SUCCEEDED,
                1, 1, "0001-test", 0, 1
        );

        Set<String> limits = new HashSet<>();
        limits.add("maya-license");
        when(setOperations.members("layer:limits:layer-1")).thenReturn(limits);

        listener.handleFrameStateChanged(event);

        // Limit counter should be decremented
        verify(valueOperations).decrement("limit:maya-license:running");
    }

    // ============================================================
    // JOB STATE CHANGE TESTS
    // ============================================================

    @Test
    public void testHandleJobStateChange_Pending() {
        JobStateChangedEvent event = new JobStateChangedEvent(
                "job-1", "show-1", "facility-1", "folder-1",
                JobState.PENDING, false, "linux",
                100, 0, 0, 800, 0, 1, System.currentTimeMillis() / 1000,
                0, -1, 0, -1,
                "testshow", "testshow-shot01-test_job", "shot01",
                "testuser", 1000, "/logs/job-1", null
        );

        listener.handleJobStateChanged(event);

        // Job should be added to pending jobs set
        verify(setOperations).add("jobs:pending:show-1:facility-1", "job-1");

        // Job data should be stored in hash
        verify(hashOperations).putAll(eq("job:job-1"), anyMap());
    }

    @Test
    public void testHandleJobStateChange_Paused() {
        JobStateChangedEvent event = new JobStateChangedEvent(
                "job-1", "show-1", "facility-1", "folder-1",
                JobState.PENDING, true, "linux",  // paused = true
                100, 0, 0, 800, 0, 1, System.currentTimeMillis() / 1000,
                0, -1, 0, -1,
                "testshow", "testshow-shot01-test_job", "shot01",
                "testuser", 1000, "/logs/job-1", null
        );

        listener.handleJobStateChanged(event);

        // Paused job should be removed from pending jobs set
        verify(setOperations).remove("jobs:pending:show-1:facility-1", "job-1");

        // Job data should still be updated
        verify(hashOperations).putAll(eq("job:job-1"), anyMap());
    }

    @Test
    public void testHandleJobStateChange_Finished() {
        JobStateChangedEvent event = new JobStateChangedEvent(
                "job-1", "show-1", "facility-1", "folder-1",
                JobState.FINISHED, false, "linux",
                100, 0, 0, 800, 0, 1, System.currentTimeMillis() / 1000,
                0, -1, 0, -1,
                "testshow", "testshow-shot01-test_job", "shot01",
                "testuser", 1000, "/logs/job-1", null
        );

        listener.handleJobStateChanged(event);

        // Finished job should be removed from pending jobs set
        verify(setOperations).remove("jobs:pending:show-1:facility-1", "job-1");
    }

    // ============================================================
    // JOB COMPLETED TESTS
    // ============================================================

    @Test
    public void testHandleJobCompleted() {
        RedisSchedulingEventPublisher.JobCompletedEvent event =
                new RedisSchedulingEventPublisher.JobCompletedEvent("job-1", "show-1", "facility-1");

        // Mock that the job has some layers
        Set<String> layers = new HashSet<>();
        layers.add("layer-1");
        layers.add("layer-2");
        when(setOperations.members("layers:waiting:job-1")).thenReturn(layers);

        listener.handleJobCompleted(event);

        // Job should be removed from pending jobs
        verify(setOperations).remove("jobs:pending:show-1:facility-1", "job-1");

        // Job hash should be deleted
        verify(redisTemplate).delete("job:job-1");

        // Layers waiting set should be deleted
        verify(redisTemplate).delete("layers:waiting:job-1");
    }

    // ============================================================
    // LAYER UPDATE TESTS
    // ============================================================

    @Test
    public void testHandleLayerUpdated() {
        LayerUpdatedEvent event = new LayerUpdatedEvent(
                "layer-1", "job-1", "show-1",
                "test_layer", "/usr/bin/render #IFRAME#",
                100, 0, true, 0, "shell", "1-100", 1,
                4194304, 0, 0, 0, null
        );

        listener.handleLayerUpdated(event);

        // Layer data should be stored in hash
        verify(hashOperations).putAll(eq("layer:layer-1"), anyMap());

        // Layer should be added to layers:waiting
        verify(setOperations).add("layers:waiting:job-1", "layer-1");
    }

    @Test
    public void testHandleLayerUpdated_WithLimits() {
        java.util.Map<String, Integer> limits = new java.util.HashMap<>();
        limits.put("maya-license", 50);
        limits.put("nuke-license", 25);

        LayerUpdatedEvent event = new LayerUpdatedEvent(
                "layer-1", "job-1", "show-1",
                "test_layer", "/usr/bin/render #IFRAME#",
                100, 0, true, 0, "shell", "1-100", 1,
                4194304, 0, 0, 0, limits
        );

        listener.handleLayerUpdated(event);

        // Layer limits should be stored
        verify(setOperations).add("layer:limits:layer-1", "maya-license");
        verify(setOperations).add("layer:limits:layer-1", "nuke-license");
    }
}
