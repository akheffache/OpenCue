# Redis Dispatch Cache Architecture Walkthrough

This document provides a guided tour through the Redis scheduling cache implementation in OpenCue's cuebot.

**Key Design Principle:** SQL is the source of truth. Redis is a read-only cache for WAITING frames only, providing zero-SQL frame dispatch queries.

**Readiness Gate:** The dispatcher consults `RedisCacheLoadService.isReady()` before querying Redis. While the cache is loading (locally, or on a peer cuebot we are waiting for), all dispatch paths fall back to SQL transparently. This avoids serving empty or partial results from a half-populated cache during multi-cuebot startup.

---

## 1. Frame State Changes

When a frame changes state (e.g., WAITING → RUNNING → SUCCEEDED), the system must keep Redis in sync.

### Flow Diagram

```
FrameDaoJdbc.updateFrameState() / updateFrameStarted() / updateFrameStopped()
        │
        ▼
publishFrameStateChange()  ──────► SchedulingEventPublisher.publishFrameStateChanged()
        │                                        │
        ▼                                        ▼
   SQL COMMITS                     RedisSchedulingEventPublisher
                                   (publishes Spring application event)
                                             │
                                             ▼
                            @TransactionalEventListener(AFTER_COMMIT)
                            @Async("redisAsyncExecutor")
                                             │
                                             ▼
                            RedisSchedulingEventListener.onFrameStateChanged()
                                             │
                                             ▼
                                   Redis Operations:
                                   - WAITING        → ZADD frames:waiting:{layerId},
                                                       SADD layers:waiting:{jobId},
                                                       create frame:{frameId} hash
                                   - leaving WAITING → ZREM frames:waiting:{layerId},
                                                       DEL frame:{frameId},
                                                       atomic-Lua: SREM layers:waiting:{jobId}
                                                       only if frames:waiting:{layerId} is empty
                                   - RUNNING        → INCR limit:{limitId}:running
                                   - leaving RUNNING → DECR limit:{limitId}:running
```

### Source Files

1. **[FrameDaoJdbc.java](../../dao/postgres/FrameDaoJdbc.java)** (line 79)
   - Entry point: `publishFrameStateChange()`, called by `updateFrameState()`, `updateFrameStarted()`, `updateFrameStopped()`, `updateFrameStateForDependencies()`.
   - Calls `schedulingEventPublisher.publishFrameStateChanged(...)` after each SQL update.

2. **[SchedulingEventPublisher.java](../../dao/SchedulingEventPublisher.java)**
   - Interface for publishing frame state change events.
   - Decouples DAO layer from Redis (DAOs only know the interface).

3. **[RedisSchedulingEventPublisher.java](RedisSchedulingEventPublisher.java)**
   - Creates a `FrameStateChangedEvent` and publishes it via Spring's `ApplicationEventPublisher`.

4. **[FrameStateChangedEvent.java](FrameStateChangedEvent.java)**
   - Event payload: `frameId`, `layerId`, `jobId`, `previousState`, `newState`, `sortScore` (= layerOrder), `frameName`, `retries`, `version`, `layerOrder`.

5. **[RedisSchedulingEventListener.java](RedisSchedulingEventListener.java)** (line 132)
   - `@TransactionalEventListener(AFTER_COMMIT)` ensures Redis sync only after SQL commits.
   - `@Async("redisAsyncExecutor")` makes Redis operations non-blocking on the dispatch thread.
   - `CLEANUP_WAITING_LAYER_SCRIPT` (line 84) — inline Lua, atomically SREMs the layer from `layers:waiting:{jobId}` only when its `frames:waiting:{layerId}` ZSET is empty. Eliminates the race where a concurrent WAITING event could re-add the layer between a `ZCARD` and `SREM`.
   - `REPLACE_LAYER_TAGS_SCRIPT` (line 109) — inline Lua, atomically replaces `layer:{layerId}:tags` (DEL + SADD in one EVAL) so a concurrent dispatcher never sees an empty tag set (which would falsely match any host).
   - `updateLimitCounters()` (line 191) — increments/decrements `limit:{limitId}:running` atomically.

### Key Insight

The `@TransactionalEventListener(AFTER_COMMIT)` annotation is critical. It guarantees that Redis updates only happen after the SQL transaction commits successfully. This ensures SQL remains the source of truth and Redis is eventually consistent.

Two correctness hazards are handled with inline Lua scripts (kept alongside `CLEANUP_WAITING_LAYER_SCRIPT` in the same Java file rather than as separate `.lua` resources): waiting-set cleanup and tag-set rewrite. Both close races where a non-atomic delete-then-add sequence could be observed mid-state by the dispatcher.

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
                    ├──► loadLayers(jobId) ──► Pipelined batch:
                    │                            - layer:{layerId} hashes
                    │                            - layers:waiting:{jobId} set
                    │                            - layer:{layerId}:tags sets
                    │                          Then loads layer:limits:{layerId} sets
                    │
                    └──► loadWaitingFrames(jobId) ──► Pipelined batch:
                                                       - frame:{frameId} hashes
                                                       - frames:waiting:{layerId} sorted sets
                                                         (score = layerOrder)
```

### Source Files

1. **[JobLauncher.java](../../service/JobLauncher.java)** (lines 76-93)
   - Entry point: `launch(JobSpec)` or `launch(String xml)`.
   - Parses job specification and delegates to `JobManager`.

2. **[JobManagerService.java](../../service/JobManagerService.java)**
   - `launchJobSpec()` creates the job in SQL.
   - Calls `redisCacheLoadService.loadJob()` after job is created.

3. **[RedisCacheLoadService.java](RedisCacheLoadService.java)**
   - **Startup load** — `init()` (line 128) runs at `@PostConstruct`. Acquires a Redis SET-NX load lock (`cuebot:load:lock`) with a 5-minute TTL. If acquired, `loadCache()` (line 221) bulk-loads all PENDING, non-paused jobs and flips `ready = true`. If not acquired, `waitForPeerLoad()` (line 176) polls the lock every 1s up to the TTL, then flips ready when the peer is done.
   - **Single job load** — `loadJob(jobId)` (line 637) is called when a new job is launched after startup.
   - **Readiness flag** — `isReady()` (line 204), consulted by `RedisDispatchSupport` to gate dispatch on the cache being safely populated.
   - **Pipelining** — `loadLayers` and `loadWaitingFrames` use Redis pipelining (`executePipelined`) to batch all writes into a single round-trip, dramatically reducing startup time.

### Redis Data Structures Created

| Key Pattern | Type | Contents |
|-------------|------|----------|
| `job:{jobId}` | Hash | showId, facilityId, folderId, priority, minCores, maxCores, minGpus, maxGpus, showName, jobName, owner, os, logDir, etc. |
| `layer:{layerId}` | Hash | jobId, name, type, command, range, chunkSize, services, minCores, maxCores, minMemory, minGpus, maxGpus, minGpuMemory, threadable, dispatchOrder |
| `layer:{layerId}:tags` | Set | Pre-normalized (lowercased, trimmed) tag tokens for membership lookup in Lua |
| `layer:limits:{layerId}` | Set | Limit IDs applied to this layer |
| `limit:{limitId}` | Hash | maxValue, name |
| `limit:{limitId}:running` | String (counter) | Current running count across all layers sharing this limit |
| `frame:{frameId}` | Hash | layerId, jobId, state, layerOrder, name, retries, version |
| `layers:waiting:{jobId}` | Set | Layer IDs that currently have waiting frames |
| `frames:waiting:{layerId}` | Sorted Set | Frame IDs scored by `layerOrder` (pre-sorted at insert time) |

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
                    ├──► If redisDispatchSupport != null (bean wired in):
                    │         redisDispatchSupport.findNextDispatchFrames()
                    │                   │
                    │                   ├──► if (!redisCacheLoadService.isReady())
                    │                   │       → fall through to SQL DAO
                    │                   │
                    │                   ├──► if (redisDispatchCache.hasJobData(jobId))
                    │                   │       redisDispatchCache.findNextDispatchFrames()
                    │                   │                   │
                    │                   │                   ▼
                    │                   │         Execute Lua (find_dispatch_frames.lua)
                    │                   │                   │
                    │                   │                   ▼
                    │                   │         buildDispatchFramesFromRedis() ──► ZERO SQL
                    │                   │
                    │                   └──► If empty, fall through to SQL
                    │
                    └──► Else (no Redis at all):
                              dispatcherDao.findNextDispatchFrames() ──► SQL
```

### Source Files

1. **[HostReportHandler.java](../HostReportHandler.java)**
   - Entry point for host availability reports from RQD.
   - Queues dispatch commands to `BookingQueue`.

2. **[CoreUnitDispatcher.java](../CoreUnitDispatcher.java)**
   - Main dispatcher implementation.
   - `dispatchHost()` iterates through candidate jobs and asks for frames per job.

3. **[DispatchSupportService.java](../DispatchSupportService.java)** (line 167+)
   - Routes to Redis or SQL based on whether the `RedisDispatchSupport` bean is wired in.
   - `if (redisDispatchSupport != null)` → use Redis path, else → use SQL DAO directly.

4. **[RedisDispatchSupport.java](RedisDispatchSupport.java)** (line 73+)
   - All four overloads (job/host, job/proc, layer/host, layer/proc) gate on `redisCacheLoadService.isReady()` first. If not ready, they go directly to SQL.
   - When ready, they call into `RedisDispatchCache` and fall back to SQL only if Redis returns empty.

5. **[RedisDispatchCache.java](RedisDispatchCache.java)**
   - `findNextDispatchFrames()` executes the Lua script and builds `DispatchFrame` objects.
   - `buildDispatchFramesFromRedis()` constructs full `DispatchFrame` objects from cached Redis hashes (`job:{jobId}`, `layer:{layerId}`, `frame:{frameId}`) with no SQL on the hot path.

6. **[find_dispatch_frames.lua](../../../../../../resources/lua/find_dispatch_frames.lua)** — job-level dispatch.
7. **[find_dispatch_frames_by_layer.lua](../../../../../../resources/lua/find_dispatch_frames_by_layer.lua)** — layer-targeted dispatch.

### Lua Script Logic (find_dispatch_frames.lua)

```text
1. SMEMBERS layers:waiting:{jobId}   -- get layer IDs with waiting frames
2. For each layer:
     - HGETALL layer:{layerId}        (resource requirements, threadable, dispatchOrder)
     - Check threadable, OS, tag-set membership against host tags
     - Filter on minCores ≤ hostCores, minMemory ≤ hostMemory,
       minGpus ≤ hostGpus, minGpuMemory matches SQL BETWEEN semantics
     - SMEMBERS layer:limits:{layerId}; cache (maxValue - running) per limit
3. Sort eligible layers by dispatchOrder  -- O(L log L), NOT O(F log F)
4. For each eligible layer in order:
     - canFit = min(remainingCores/minCores, remainingMemory/minMemory,
                    remainingGpus/minGpus, remainingGpuMemory/minGpuMemory,
                    limit_capacity)                -- limits CAP canFit, not just gate it
     - ZRANGE frames:waiting:{layerId} 0 (canFit-1)
     - Append frames to result; deduct resources; decrement cached limit capacity
     - Stop if requested limit reached
```

**Note on the two Lua scripts:**
The two scripts diverge intentionally on the GPU-memory predicate. `find_dispatch_frames.lua` mirrors SQL's `BETWEEN (host>0?1:0) AND host` semantics. `find_dispatch_frames_by_layer.lua` mirrors its corresponding SQL query which uses a plain `<= host` predicate. Both are in correct parity with their own SQL counterparts and **must not be "unified"** — comments at the top of each script document this.

### Key Performance Insight

The matching decision in Redis is **O(L log L)** per call — sort the layers by dispatch order, not the frames. Frames are pre-sorted at insert time into per-layer sorted sets (score = `layerOrder`), so the hot path never sorts a frame collection. For a 5,000-frame job with 30 layers:

- SQL pattern: O(F log F) ≈ ~60K comparisons over all waiting frames per call.
- Lua pattern: O(L log L) ≈ ~150 comparisons over layers, then top-K from each layer's ZSET.

Combined with the `canFit` math, the script only returns frames that will actually fit on the host given remaining resources and remaining limit capacity — no wasted booking attempts where SQL would have returned top-N and let the booker fail.

---

## Configuration

Redis scheduling is enabled via application properties:

```properties
redis.scheduling.enabled=true
spring.redis.host=...
spring.redis.port=...
```

Optional knobs:

```properties
# Only set when you are CERTAIN no other cuebots are running.
# Wipes Redis at startup before reloading from SQL.
redis.flush-on-startup=false
```

When `redis.scheduling.enabled=false`, none of the Redis beans are constructed (`@ConditionalOnProperty`), the dispatcher uses the SQL path exclusively, and behavior is identical to pre-Redis cuebot.

---

## Summary

| Flow | SQL Role | Redis Role |
|------|----------|------------|
| Frame State Change | Source of truth (writes) | Eventually consistent cache (reads), kept in sync via AFTER_COMMIT events |
| Job Launch | Creates job/layers/frames | Loaded after SQL commit (pipelined for throughput) |
| Frame Dispatch | Job finding + booking | Frame finding (zero SQL on hot path when ready) |

The architecture ensures:

1. **SQL is always authoritative** — Redis syncs only after SQL commits.
2. **Graceful degradation** — `isReady()` gating, SQL fallback when Redis is unavailable, the cache is loading, or the script returns empty.
3. **Race-free state transitions** — inline Lua scripts for waiting-set cleanup and tag-set rewrite eliminate the windows where a concurrent dispatcher could see an inconsistent intermediate state.
4. **Multi-cuebot safety** — distributed load lock prevents concurrent cache rebuilds; peers wait for the holder to finish before serving from Redis.

---

## TL;DR — Entry points

If you only have a few minutes, read these five call sites in order:

1. **Frame becomes WAITING** → state-change publish at the DAO layer.
   [`FrameDaoJdbc.java#L79`](../../dao/postgres/FrameDaoJdbc.java#L79) — `publishFrameStateChange()` fires for every state transition; the [`AFTER_COMMIT`](RedisSchedulingEventListener.java#L131) listener consumes it.

2. **Sync into Redis** → the listener that maintains the cache.
   [`RedisSchedulingEventListener.java#L132`](RedisSchedulingEventListener.java#L132) — `onFrameStateChanged()` adds/removes from `frames:waiting:{layerId}`, manages `layers:waiting:{jobId}` via atomic Lua cleanup, updates limit running counters.

3. **Dispatcher asks for frames** → Redis-first dispatch with SQL fallback and readiness gate.
   [`RedisDispatchSupport.java#L73`](RedisDispatchSupport.java#L73) — `findNextDispatchFrames(JobInterface, DispatchHost, int)` checks `isReady()`, calls into the cache, falls back to SQL otherwise.

4. **The cache executes the Lua script** → atomic server-side matching.
   [`RedisDispatchCache.java`](RedisDispatchCache.java) (`executeFrameSearch` / `findNextDispatchFrames`) →
   [`find_dispatch_frames.lua`](../../../../../../resources/lua/find_dispatch_frames.lua) — this is where the O(L log L) algorithm lives. Read this script to understand the actual matching semantics: layer eligibility, `canFit` capacity math, and the per-script `limitCapacityCache` that lets later layers see capacity consumed by earlier ones.

5. **Cache population at startup / new job** → load orchestration.
   [`RedisCacheLoadService.java#L128`](RedisCacheLoadService.java#L128) — `init()` does `@PostConstruct` lock + bulk load, or [`waitForPeerLoad()`](RedisCacheLoadService.java#L176) if a peer cuebot is loading. [`loadJob(jobId)`](RedisCacheLoadService.java#L637) is the same path when a new job is launched after startup.
