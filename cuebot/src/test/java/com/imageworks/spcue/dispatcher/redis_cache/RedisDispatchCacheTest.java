
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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.FrameDao;
import com.imageworks.spcue.grpc.host.ThreadMode;

@RunWith(MockitoJUnitRunner.class)
public class RedisDispatchCacheTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private RedisScript<List> findDispatchFramesScript;

    @Mock
    private RedisScript<List> findDispatchFramesByLayerScript;

    @Mock
    private RedisScript<List> findJobsByShowScript;

    @Mock
    private FrameDao frameDao;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @Mock
    private SetOperations<String, String> setOperations;

    private RedisDispatchCache redisDispatchCache;

    @Before
    public void setUp() {
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);

        redisDispatchCache = new RedisDispatchCache(
                redisTemplate,
                findDispatchFramesScript,
                findDispatchFramesByLayerScript,
                findJobsByShowScript,
                frameDao
        );
    }

    @Test
    public void testHasJobData_WithData() {
        when(setOperations.size("layers:waiting:job-123")).thenReturn(3L);

        boolean result = redisDispatchCache.hasJobData("job-123");

        assertTrue(result);
        verify(setOperations).size("layers:waiting:job-123");
    }

    @Test
    public void testHasJobData_NoData() {
        when(setOperations.size("layers:waiting:job-123")).thenReturn(0L);

        boolean result = redisDispatchCache.hasJobData("job-123");

        assertFalse(result);
    }

    @Test
    public void testHasJobData_NullSize() {
        when(setOperations.size("layers:waiting:job-123")).thenReturn(null);

        boolean result = redisDispatchCache.hasJobData("job-123");

        assertFalse(result);
    }

    @Test
    public void testHasJobData_Exception() {
        when(setOperations.size("layers:waiting:job-123")).thenThrow(new RuntimeException("Redis error"));

        boolean result = redisDispatchCache.hasJobData("job-123");

        assertFalse(result);
    }

    @Test
    public void testFindNextDispatchFrames_ReturnsFrames() {
        JobInterface job = createMockJob("job-123", "show-1");
        DispatchHost host = createMockHost();

        // Mock Lua script returning frame IDs
        List<String> frameIds = Arrays.asList("frame-1", "frame-2");
        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(frameIds);

        // Mock frame data in Redis
        Map<Object, Object> frameData1 = createFrameData("frame-1", "layer-1", "job-123");
        Map<Object, Object> frameData2 = createFrameData("frame-2", "layer-1", "job-123");
        Map<Object, Object> layerData = createLayerData("layer-1");
        Map<Object, Object> jobData = createJobData("job-123");

        when(hashOperations.entries("frame:frame-1")).thenReturn(frameData1);
        when(hashOperations.entries("frame:frame-2")).thenReturn(frameData2);
        when(hashOperations.entries("layer:layer-1")).thenReturn(layerData);
        when(hashOperations.entries("job:job-123")).thenReturn(jobData);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, 10);

        assertEquals(2, frames.size());
        assertEquals("frame-1", frames.get(0).id);
        assertEquals("frame-2", frames.get(1).id);
    }

    @Test
    public void testFindNextDispatchFrames_EmptyResult() {
        JobInterface job = createMockJob("job-123", "show-1");
        DispatchHost host = createMockHost();

        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(Collections.emptyList());

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, 10);

        assertTrue(frames.isEmpty());
    }

    @Test
    public void testFindNextDispatchFrames_NullResult() {
        JobInterface job = createMockJob("job-123", "show-1");
        DispatchHost host = createMockHost();

        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(null);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, 10);

        assertTrue(frames.isEmpty());
    }

    @Test
    public void testFindNextDispatchFrames_WithVirtualProc() {
        JobInterface job = createMockJob("job-123", "show-1");
        VirtualProc proc = createMockProc();

        List<String> frameIds = Arrays.asList("frame-1");
        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(frameIds);

        Map<Object, Object> frameData = createFrameData("frame-1", "layer-1", "job-123");
        Map<Object, Object> layerData = createLayerData("layer-1");
        Map<Object, Object> jobData = createJobData("job-123");

        when(hashOperations.entries("frame:frame-1")).thenReturn(frameData);
        when(hashOperations.entries("layer:layer-1")).thenReturn(layerData);
        when(hashOperations.entries("job:job-123")).thenReturn(jobData);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, proc, 10);

        assertEquals(1, frames.size());
        assertEquals("frame-1", frames.get(0).id);
    }

    @Test
    public void testFindNextDispatchFrames_ByLayer() {
        LayerInterface layer = createMockLayer("layer-1", "job-123");
        DispatchHost host = createMockHost();

        List<String> frameIds = Arrays.asList("frame-1");
        when(redisTemplate.execute(eq(findDispatchFramesByLayerScript), anyList(), any()))
                .thenReturn(frameIds);

        Map<Object, Object> frameData = createFrameData("frame-1", "layer-1", "job-123");
        Map<Object, Object> layerData = createLayerData("layer-1");
        Map<Object, Object> jobData = createJobData("job-123");

        when(hashOperations.entries("frame:frame-1")).thenReturn(frameData);
        when(hashOperations.entries("layer:layer-1")).thenReturn(layerData);
        when(hashOperations.entries("job:job-123")).thenReturn(jobData);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(layer, host, 10);

        assertEquals(1, frames.size());
    }

    @Test
    public void testFindNextDispatchFrames_FallbackToSQL() {
        JobInterface job = createMockJob("job-123", "show-1");
        DispatchHost host = createMockHost();

        // Lua script returns frame IDs
        List<String> frameIds = Arrays.asList("frame-missing");
        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(frameIds);

        // Frame data missing in Redis (empty map)
        when(hashOperations.entries("frame:frame-missing")).thenReturn(Collections.emptyMap());
        when(hashOperations.entries("job:job-123")).thenReturn(createJobData("job-123"));

        // SQL fallback should be called
        DispatchFrame sqlFrame = new DispatchFrame();
        sqlFrame.id = "frame-missing";
        when(frameDao.getDispatchFrame("frame-missing")).thenReturn(sqlFrame);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, 10);

        assertEquals(1, frames.size());
        assertEquals("frame-missing", frames.get(0).id);
        verify(frameDao).getDispatchFrame("frame-missing");
    }

    @Test
    public void testBuildDispatchFrame_AllFields() {
        JobInterface job = createMockJob("job-123", "show-1");
        DispatchHost host = createMockHost();

        List<String> frameIds = Arrays.asList("frame-1");
        when(redisTemplate.execute(eq(findDispatchFramesScript), anyList(), any()))
                .thenReturn(frameIds);

        Map<Object, Object> frameData = new HashMap<>();
        frameData.put("layerId", "layer-1");
        frameData.put("jobId", "job-123");
        frameData.put("name", "0001-test");
        frameData.put("retries", "2");
        frameData.put("state", "WAITING");

        Map<Object, Object> layerData = new HashMap<>();
        layerData.put("name", "test_layer");
        layerData.put("command", "/usr/bin/render #IFRAME#");
        layerData.put("range", "1-100");
        layerData.put("chunkSize", "1");
        layerData.put("services", "shell");
        layerData.put("minCores", "100");
        layerData.put("maxCores", "800");
        layerData.put("threadable", "true");
        layerData.put("minGpus", "0");
        layerData.put("maxGpus", "1");
        layerData.put("minGpuMemory", "0");
        layerData.put("minMemory", "4194304");

        Map<Object, Object> jobData = new HashMap<>();
        jobData.put("showName", "testshow");
        jobData.put("shot", "shot01");
        jobData.put("owner", "testuser");
        jobData.put("uid", "1000");
        jobData.put("logDir", "/logs/job-123");
        jobData.put("jobName", "testshow-shot01-test_job");
        jobData.put("os", "linux");
        jobData.put("lokiURL", "http://loki:3100");
        jobData.put("showId", "show-1");
        jobData.put("facilityId", "facility-1");

        when(hashOperations.entries("frame:frame-1")).thenReturn(frameData);
        when(hashOperations.entries("layer:layer-1")).thenReturn(layerData);
        when(hashOperations.entries("job:job-123")).thenReturn(jobData);

        List<DispatchFrame> frames = redisDispatchCache.findNextDispatchFrames(job, host, 10);

        assertEquals(1, frames.size());
        DispatchFrame frame = frames.get(0);

        // Verify all fields are populated correctly
        assertEquals("frame-1", frame.id);
        assertEquals("layer-1", frame.layerId);
        assertEquals("job-123", frame.jobId);
        assertEquals("0001-test", frame.name);
        assertEquals(2, frame.retries);
        assertEquals("test_layer", frame.layerName);
        assertEquals("/usr/bin/render #IFRAME#", frame.command);
        assertEquals("1-100", frame.range);
        assertEquals(1, frame.chunkSize);
        assertEquals(100, frame.minCores);
        assertEquals(800, frame.maxCores);
        assertTrue(frame.threadable);
        assertEquals("testshow", frame.show);
        assertEquals("shot01", frame.shot);
        assertEquals("testuser", frame.owner);
        assertEquals("/logs/job-123", frame.logDir);
    }

    // Helper methods to create mock objects

    private JobInterface createMockJob(String jobId, String showId) {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn(jobId);
        when(job.getShowId()).thenReturn(showId);
        return job;
    }

    private LayerInterface createMockLayer(String layerId, String jobId) {
        LayerInterface layer = mock(LayerInterface.class);
        when(layer.getLayerId()).thenReturn(layerId);
        when(layer.getJobId()).thenReturn(jobId);
        return layer;
    }

    private DispatchHost createMockHost() {
        DispatchHost host = new DispatchHost();
        host.idleCores = 800;
        host.idleMemory = 8589934592L; // 8GB
        host.idleGpus = 1;
        host.idleGpuMemory = 8589934592L;
        host.tags = "general linux";
        host.threadMode = ThreadMode.AUTO_VALUE;
        host.os = "linux";
        return host;
    }

    private VirtualProc createMockProc() {
        VirtualProc proc = new VirtualProc();
        proc.coresReserved = 100;
        proc.memoryReserved = 4294967296L; // 4GB
        proc.gpusReserved = 0;
        proc.gpuMemoryReserved = 0;
        proc.tags = "general linux";
        return proc;
    }

    private Map<Object, Object> createFrameData(String frameId, String layerId, String jobId) {
        Map<Object, Object> data = new HashMap<>();
        data.put("layerId", layerId);
        data.put("jobId", jobId);
        data.put("name", "0001-" + frameId);
        data.put("retries", "0");
        data.put("state", "WAITING");
        return data;
    }

    private Map<Object, Object> createLayerData(String layerId) {
        Map<Object, Object> data = new HashMap<>();
        data.put("name", "test_layer");
        data.put("command", "/usr/bin/render #IFRAME#");
        data.put("range", "1-100");
        data.put("chunkSize", "1");
        data.put("services", "shell");
        data.put("minCores", "100");
        data.put("maxCores", "0");
        data.put("threadable", "false");
        data.put("minGpus", "0");
        data.put("maxGpus", "0");
        data.put("minGpuMemory", "0");
        data.put("minMemory", "4194304");
        return data;
    }

    private Map<Object, Object> createJobData(String jobId) {
        Map<Object, Object> data = new HashMap<>();
        data.put("showName", "testshow");
        data.put("shot", "shot01");
        data.put("owner", "testuser");
        data.put("logDir", "/logs/" + jobId);
        data.put("jobName", "testshow-shot01-test_job");
        data.put("os", "linux");
        data.put("showId", "show-1");
        data.put("facilityId", "facility-1");
        return data;
    }
}
