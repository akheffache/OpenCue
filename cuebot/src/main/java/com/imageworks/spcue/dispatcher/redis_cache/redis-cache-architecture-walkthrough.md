# Redis Dispatch Cache Architecture Walkthrough

This document provides a guided tour through the Redis scheduling cache implementation in OpenCue's cuebot.

**Key Design Principle:** SQL is the source of truth. Redis is a read-only cache for WAITING frames only, providing zero-SQL frame dispatch queries.

---

## 1. Frame State Changes

When a frame changes state (e.g., WAITING → RUNNING → SUCCEEDED), the system must keep Redis in sync.

### Flow Diagram

```
FrameDaoJdbc.updateFrameState()
        │
        ▼
publishFrameStateChange()  ──────► SchedulingEventPublisher.publishFrameStateChanged()
        │                                        │
        ▼                                        ▼
   SQL COMMITS                     RedisSchedulingEventPublisher
                                   (publishes Spring event)
                                             │
                                             ▼
                            @TransactionalEventListener(AFTER_COMMIT)
                                             │
                                             ▼
                            RedisSchedulingEventListener.onFrameStateChanged()
                                             │
                                             ▼
                                   Redis Operations:
                                   - Add/remove from sorted set
                                   - Create/delete frame hash
                                   - Update limit counters
```

### Source Files

1. **[FrameDaoJdbc.java](../../dao/postgres/FrameDaoJdbc.java)** (lines 79-83)
   - Entry point: `updateFrameState()`, `updateFrameStarted()`, `updateFrameStopped()`
   - Calls `publishFrameStateChange()` after SQL update

2. **[SchedulingEventPublisher.java](../../dao/SchedulingEventPublisher.java)**
   - Interface for publishing frame state change events
   - Decouples DAO layer from Redis

3. **[RedisSchedulingEventPublisher.java](RedisSchedulingEventPublisher.java)**
   - Creates `FrameStateChangedEvent` and publishes via Spring's `ApplicationEventPublisher`

4. **[FrameStateChangedEvent.java](FrameStateChangedEvent.java)**
   - Event payload containing: frameId, layerId, jobId, previousState, newState, dispatchOrder, layerOrder

5. **[RedisSchedulingEventListener.java](RedisSchedulingEventListener.java)** (lines 79-132)
   - `@TransactionalEventListener(AFTER_COMMIT)` ensures Redis sync only after SQL commits
   - `@Async("redisAsyncExecutor")` makes Redis operations non-blocking
   - Handles:
     - `WAITING` → Add to `frames:waiting:{layerId}` sorted set, create `frame:{frameId}` hash
     - Leaving `WAITING` → Remove from sorted set, delete hash
     - `RUNNING` → Increment limit counters
     - Leaving `RUNNING` → Decrement limit counters

### Key Insight

The `@TransactionalEventListener(AFTER_COMMIT)` annotation is critical. It guarantees that Redis updates only happen after the SQL transaction commits successfully. This ensures SQL remains the source of truth and Redis is eventually consistent.

---

## 2. Job Launch

When a new job is submitted, its layers and frames must be populated in Redis.

### Flow Diagram

```
JobLauncher.launch(JobSpec)
        │
        ▼
JobManagerService.launchJobSpec()
        │
        ├──► createJob() ──► Insert into SQL (job, layers, frames)
        │
        ├──► jobDao.activateJob() ──► Set job state to PENDING
        │
        └──► redisCacheLoadService.loadJob(jobId)
                    │
                    ├──► loadSingleJobMetadata() ──► Uses JobDao.getJobDetail()
                    │                                 Populates job:{jobId} hash
                    │
                    ├──► loadLayers() ──► Populates layer:{layerId} hashes
                    │                      Populates layers:waiting:{jobId} set
                    │                      Populates layer:limits:{layerId} sets
                    │
                    └──► loadWaitingFrames() ──► Populates frame:{frameId} hashes
                                                  Populates frames:waiting:{layerId} sorted sets
```

### Source Files

1. **[JobLauncher.java](../../service/JobLauncher.java)** (lines 76-93)
   - Entry point: `launch(JobSpec)` or `launch(String xml)`
   - Parses job specification and delegates to JobManager

2. **[JobManagerService.java](../../service/JobManagerService.java)** (lines 202-244)
   - `launchJobSpec()` creates the job in SQL
   - Calls `redisCacheLoadService.loadJob()` after job is created

3. **[RedisCacheLoadService.java](RedisCacheLoadService.java)**
   - **Startup load** (lines 80-118): `loadCache()` runs at `@PostConstruct`, bulk-loads all pending jobs
   - **Single job load** (lines 450-468): `loadJob(jobId)` for newly launched jobs
   - Uses `JobDao.getJobDetail()` for type-safe job data retrieval (lines 260-303)

### Redis Data Structures Created

| Key Pattern | Type | Contents |
|-------------|------|----------|
| `job:{jobId}` | Hash | showId, facilityId, priority, minCores, maxCores, etc. |
| `layer:{layerId}` | Hash | jobId, name, type, tags, minCores, minMemory, minGpuMemory, etc. |
| `frame:{frameId}` | Hash | layerId, jobId, state, dispatchOrder, layerOrder, name, retries |
| `layers:waiting:{jobId}` | Set | Layer IDs that have waiting frames |
| `frames:waiting:{layerId}` | Sorted Set | Frame IDs sorted by dispatch order |
| `layer:limits:{layerId}` | Set | Limit IDs applied to this layer |

---

## 3. Scheduling Loop (Frame Dispatch)

When a host reports availability, the dispatcher finds eligible frames and assigns them.

### Flow Diagram

```
Host sends HostReport
        │
        ▼
HostReportHandler.handleHostReport()
        │
        ▼
BookingQueue.execute(DispatchBookHost)
        │
        ▼
CoreUnitDispatcher.dispatchHost()
        │
        ├──► dispatcherDao.findDispatchJobs() ──► SQL (job finding stays in SQL)
        │
        └──► For each job:
                    │
                    ▼
             dispatchSupport.findNextDispatchFrames(job, host, limit)
                    │
                    ▼
             DispatchSupportService.findNextDispatchFrames()
                    │
                    ├──► If Redis enabled:
                    │         redisDispatchSupport.findNextDispatchFrames()
                    │                   │
                    │                   ▼
                    │         RedisDispatchCache.findNextDispatchFrames()
                    │                   │
                    │                   ▼
                    │         Execute Lua script (find_dispatch_frames.lua)
                    │                   │
                    │                   ▼
                    │         buildDispatchFramesFromRedis() ──► ZERO SQL!
                    │
                    └──► Else (fallback):
                              dispatcherDao.findNextDispatchFrames() ──► SQL
```

### Source Files

1. **[HostReportHandler.java](../HostReportHandler.java)**
   - Entry point for host availability reports
   - Queues dispatch commands to BookingQueue

2. **[CoreUnitDispatcher.java](../CoreUnitDispatcher.java)** (lines 46-77, 135+)
   - Main dispatcher implementation
   - `dispatchHost()` iterates through jobs and finds frames

3. **[DispatchSupport.java](../DispatchSupport.java)** (lines 206-237)
   - Interface defining `findNextDispatchFrames()` methods

4. **[DispatchSupportService.java](../DispatchSupportService.java)** (lines 160-207)
   - Routes to Redis or SQL based on configuration
   - `if (redisDispatchSupport != null)` → use Redis, else → use SQL

5. **[RedisDispatchCache.java](RedisDispatchCache.java)**
   - Core Redis dispatch cache logic
   - `findNextDispatchFrames()` executes Lua script and builds DispatchFrame objects
   - `buildDispatchFramesFromRedis()` constructs full DispatchFrame from Redis hashes (zero SQL)

6. **[find_dispatch_frames.lua](../../../resources/lua/find_dispatch_frames.lua)**
   - Atomic Lua script executed in Redis
   - Iterates layers with waiting frames
   - Filters by: tags, cores, memory, GPU memory, limits
   - Returns eligible frame IDs in dispatch order

7. **[find_dispatch_frames_by_layer.lua](../../../resources/lua/find_dispatch_frames_by_layer.lua)**
   - Variant for layer-specific dispatch

### Lua Script Logic (find_dispatch_frames.lua)

```lua
-- For each layer with waiting frames:
1. Check host tags match layer tags
2. Check host cores >= layer minCores
3. Check host memory >= layer minMemory
4. Check GPU memory (only if layer needs GPU)
5. Check limit constraints (running < max)
6. Get frames from sorted set (dispatch order)
7. Return up to N eligible frame IDs
```

### Key Performance Insight

The entire frame-finding query executes in Redis with a single network round-trip (Lua script). The `DispatchFrame` object is built entirely from Redis hashes without touching SQL. Job-finding queries remain in SQL because they're already fast (simple index lookups).

---

## Configuration

Redis scheduling is enabled via application properties:

```properties
redis.scheduling.enabled=true
```

When disabled, the system falls back to pure SQL dispatch (original behavior).

---

## Summary

| Flow | SQL Role | Redis Role |
|------|----------|------------|
| Frame State Change | Source of truth (writes) | Eventually consistent cache (reads) |
| Job Launch | Creates job/layers/frames | Loaded after SQL commit |
| Frame Dispatch | Job finding | Frame finding (zero SQL on hot path) |

The architecture ensures:
1. **SQL is always authoritative** - Redis syncs after SQL commits
2. **Graceful degradation** - Falls back to SQL if Redis unavailable
3. **Horizontal scalability** - Redis shared across multiple cuebot instances
