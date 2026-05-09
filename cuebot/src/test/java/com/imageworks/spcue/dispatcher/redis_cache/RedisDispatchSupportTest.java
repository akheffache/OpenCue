
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.DispatchJob;
import com.imageworks.spcue.GroupInterface;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.ShowInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.DispatcherDao;

@RunWith(MockitoJUnitRunner.class)
public class RedisDispatchSupportTest {

    @Mock
    private RedisDispatchCache redisDispatchCache;

    @Mock
    private DispatcherDao sqlDispatcherDao;

    private RedisDispatchSupport redisDispatchSupport;

    @Before
    public void setUp() {
        redisDispatchSupport = new RedisDispatchSupport(redisDispatchCache, sqlDispatcherDao);
    }

    // ============================================================
    // FRAME DISPATCH TESTS - Job + Host
    // ============================================================

    @Test
    public void testFindNextDispatchFrames_RedisHasData() {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn("job-123");

        DispatchHost host = new DispatchHost();
        host.idleCores = 800;

        DispatchFrame frame1 = new DispatchFrame();
        frame1.id = "frame-1";
        DispatchFrame frame2 = new DispatchFrame();
        frame2.id = "frame-2";

        when(redisDispatchCache.hasJobData("job-123")).thenReturn(true);
        when(redisDispatchCache.findNextDispatchFrames(job, host, 10))
                .thenReturn(Arrays.asList(frame1, frame2));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(job, host, 10);

        assertEquals(2, result.size());
        assertEquals("frame-1", result.get(0).id);
        assertEquals("frame-2", result.get(1).id);
        verify(sqlDispatcherDao, never()).findNextDispatchFrames(any(JobInterface.class), any(DispatchHost.class), anyInt());
    }

    @Test
    public void testFindNextDispatchFrames_RedisMiss_FallbackToSQL() {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn("job-123");

        DispatchHost host = new DispatchHost();

        DispatchFrame sqlFrame = new DispatchFrame();
        sqlFrame.id = "sql-frame-1";

        when(redisDispatchCache.hasJobData("job-123")).thenReturn(true);
        when(redisDispatchCache.findNextDispatchFrames(job, host, 10))
                .thenReturn(Collections.emptyList());
        when(sqlDispatcherDao.findNextDispatchFrames(job, host, 10))
                .thenReturn(Arrays.asList(sqlFrame));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(job, host, 10);

        assertEquals(1, result.size());
        assertEquals("sql-frame-1", result.get(0).id);
        verify(sqlDispatcherDao).findNextDispatchFrames(job, host, 10);
    }

    @Test
    public void testFindNextDispatchFrames_NoRedisData_FallbackToSQL() {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn("job-123");

        DispatchHost host = new DispatchHost();

        DispatchFrame sqlFrame = new DispatchFrame();
        sqlFrame.id = "sql-frame-1";

        when(redisDispatchCache.hasJobData("job-123")).thenReturn(false);
        when(sqlDispatcherDao.findNextDispatchFrames(job, host, 10))
                .thenReturn(Arrays.asList(sqlFrame));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(job, host, 10);

        assertEquals(1, result.size());
        assertEquals("sql-frame-1", result.get(0).id);
        verify(redisDispatchCache, never()).findNextDispatchFrames(any(JobInterface.class), any(DispatchHost.class), anyInt());
    }

    // ============================================================
    // FRAME DISPATCH TESTS - Job + VirtualProc
    // ============================================================

    @Test
    public void testFindNextDispatchFrames_WithProc_RedisHasData() {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn("job-123");

        VirtualProc proc = new VirtualProc();
        proc.coresReserved = 100;

        DispatchFrame frame1 = new DispatchFrame();
        frame1.id = "frame-1";

        when(redisDispatchCache.hasJobData("job-123")).thenReturn(true);
        when(redisDispatchCache.findNextDispatchFrames(job, proc, 10))
                .thenReturn(Arrays.asList(frame1));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(job, proc, 10);

        assertEquals(1, result.size());
        assertEquals("frame-1", result.get(0).id);
        verify(sqlDispatcherDao, never()).findNextDispatchFrames(any(JobInterface.class), any(VirtualProc.class), anyInt());
    }

    @Test
    public void testFindNextDispatchFrames_WithProc_FallbackToSQL() {
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn("job-123");

        VirtualProc proc = new VirtualProc();

        DispatchFrame sqlFrame = new DispatchFrame();
        sqlFrame.id = "sql-frame-1";

        when(redisDispatchCache.hasJobData("job-123")).thenReturn(true);
        when(redisDispatchCache.findNextDispatchFrames(job, proc, 10))
                .thenReturn(Collections.emptyList());
        when(sqlDispatcherDao.findNextDispatchFrames(job, proc, 10))
                .thenReturn(Arrays.asList(sqlFrame));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(job, proc, 10);

        assertEquals(1, result.size());
        assertEquals("sql-frame-1", result.get(0).id);
    }

    // ============================================================
    // FRAME DISPATCH TESTS - Layer
    // ============================================================

    @Test
    public void testFindNextDispatchFrames_ByLayer_RedisHasData() {
        LayerInterface layer = mock(LayerInterface.class);
        DispatchHost host = new DispatchHost();

        DispatchFrame frame1 = new DispatchFrame();
        frame1.id = "frame-1";

        when(redisDispatchCache.findNextDispatchFrames(layer, host, 10))
                .thenReturn(Arrays.asList(frame1));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(layer, host, 10);

        assertEquals(1, result.size());
        assertEquals("frame-1", result.get(0).id);
        verify(sqlDispatcherDao, never()).findNextDispatchFrames(any(LayerInterface.class), any(DispatchHost.class), anyInt());
    }

    @Test
    public void testFindNextDispatchFrames_ByLayer_FallbackToSQL() {
        LayerInterface layer = mock(LayerInterface.class);
        DispatchHost host = new DispatchHost();

        DispatchFrame sqlFrame = new DispatchFrame();
        sqlFrame.id = "sql-frame-1";

        when(redisDispatchCache.findNextDispatchFrames(layer, host, 10))
                .thenReturn(Collections.emptyList());
        when(sqlDispatcherDao.findNextDispatchFrames(layer, host, 10))
                .thenReturn(Arrays.asList(sqlFrame));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(layer, host, 10);

        assertEquals(1, result.size());
        assertEquals("sql-frame-1", result.get(0).id);
    }

    @Test
    public void testFindNextDispatchFrames_ByLayerAndProc_RedisHasData() {
        LayerInterface layer = mock(LayerInterface.class);
        VirtualProc proc = new VirtualProc();

        DispatchFrame frame1 = new DispatchFrame();
        frame1.id = "frame-1";

        when(redisDispatchCache.findNextDispatchFrames(layer, proc, 10))
                .thenReturn(Arrays.asList(frame1));

        List<DispatchFrame> result = redisDispatchSupport.findNextDispatchFrames(layer, proc, 10);

        assertEquals(1, result.size());
        assertEquals("frame-1", result.get(0).id);
    }

    // ============================================================
    // JOB DISPATCH TESTS - Show
    // ============================================================

    @Test
    public void testFindDispatchJobs_ByShow_RedisHasData() {
        ShowInterface show = mock(ShowInterface.class);
        DispatchHost host = new DispatchHost();

        DispatchJob job1 = new DispatchJob();
        job1.id = "job-1";
        DispatchJob job2 = new DispatchJob();
        job2.id = "job-2";

        when(redisDispatchCache.findDispatchJobs(show, host, 10))
                .thenReturn(Arrays.asList(job1, job2));

        Set<String> result = redisDispatchSupport.findDispatchJobs(host, show, 10);

        assertEquals(2, result.size());
        assertTrue(result.contains("job-1"));
        assertTrue(result.contains("job-2"));
        verify(sqlDispatcherDao, never()).findDispatchJobs(any(DispatchHost.class), any(ShowInterface.class), anyInt());
    }

    @Test
    public void testFindDispatchJobs_ByShow_FallbackToSQL() {
        ShowInterface show = mock(ShowInterface.class);
        DispatchHost host = new DispatchHost();

        Set<String> sqlJobs = new HashSet<>(Arrays.asList("sql-job-1", "sql-job-2"));

        when(redisDispatchCache.findDispatchJobs(show, host, 10))
                .thenReturn(Collections.emptyList());
        when(sqlDispatcherDao.findDispatchJobs(host, show, 10))
                .thenReturn(sqlJobs);

        Set<String> result = redisDispatchSupport.findDispatchJobs(host, show, 10);

        assertEquals(2, result.size());
        assertTrue(result.contains("sql-job-1"));
        assertTrue(result.contains("sql-job-2"));
    }

    // ============================================================
    // JOB DISPATCH TESTS - Group
    // ============================================================

    @Test
    public void testFindDispatchJobs_ByGroup_RedisHasData() {
        GroupInterface group = mock(GroupInterface.class);
        DispatchHost host = new DispatchHost();

        DispatchJob job1 = new DispatchJob();
        job1.id = "job-1";

        when(redisDispatchCache.findDispatchJobs(group, host, 50))
                .thenReturn(Arrays.asList(job1));

        Set<String> result = redisDispatchSupport.findDispatchJobs(host, group);

        assertEquals(1, result.size());
        assertTrue(result.contains("job-1"));
        verify(sqlDispatcherDao, never()).findDispatchJobs(any(DispatchHost.class), any(GroupInterface.class));
    }

    @Test
    public void testFindDispatchJobs_ByGroup_FallbackToSQL() {
        GroupInterface group = mock(GroupInterface.class);
        DispatchHost host = new DispatchHost();

        Set<String> sqlJobs = new HashSet<>(Arrays.asList("sql-job-1"));

        when(redisDispatchCache.findDispatchJobs(group, host, 50))
                .thenReturn(Collections.emptyList());
        when(sqlDispatcherDao.findDispatchJobs(host, group))
                .thenReturn(sqlJobs);

        Set<String> result = redisDispatchSupport.findDispatchJobs(host, group);

        assertEquals(1, result.size());
        assertTrue(result.contains("sql-job-1"));
    }

    // ============================================================
    // UTILITY TESTS
    // ============================================================

    @Test
    public void testIsRedisAvailableForJob() {
        when(redisDispatchCache.hasJobData("job-123")).thenReturn(true);
        when(redisDispatchCache.hasJobData("job-456")).thenReturn(false);

        assertTrue(redisDispatchSupport.isRedisAvailableForJob("job-123"));
        assertFalse(redisDispatchSupport.isRedisAvailableForJob("job-456"));
    }
}
