--[[
  Find next dispatch frames for a job and host.

  This Lua script mirrors the SQL query FIND_DISPATCH_FRAME_BY_JOB_AND_HOST.
  It atomically finds waiting frames that match host resource requirements.

  SMART RESOURCE TRACKING:
  Unlike SQL which returns N frames hoping some will fit, this script tracks
  remaining host resources as frames are selected. It returns ONLY frames that
  will actually fit, accounting for cumulative resource consumption.

  KEYS:
    KEYS[1] = layers:waiting:{jobId} - Set of layer IDs with waiting frames

  ARGV:
    ARGV[1] = hostCores     - Available cores on host
    ARGV[2] = hostMemory    - Available memory on host (bytes)
    ARGV[3] = hostGpus      - Available GPUs on host
    ARGV[4] = hostGpuMemory - Available GPU memory on host (bytes)
    ARGV[5] = hostTags      - Comma-separated list of host tags
    ARGV[6] = threadMode    - 0 = AUTO, 1 = ALL (for threadable check)
    ARGV[7] = limit         - Maximum number of frames to return
    ARGV[8] = noGpu         - 1 = skip GPU checks (NO_GPU mode), 0 = normal

  Returns:
    List of frame IDs that match the criteria, ordered by dispatch order
]]

local layersWaitingKey = KEYS[1]
local hostCores = tonumber(ARGV[1])
local hostMemory = tonumber(ARGV[2])
local hostGpus = tonumber(ARGV[3])
local hostGpuMemory = tonumber(ARGV[4])
local hostTags = ARGV[5]
local threadMode = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local noGpu = tonumber(ARGV[8] or 0)

-- Track remaining resources as we select frames
local remainingCores = hostCores
local remainingMemory = hostMemory
local remainingGpus = hostGpus
local remainingGpuMemory = hostGpuMemory

-- Max frames to fetch per layer (prevents O(n) memory on huge jobs)
local MAX_FRAMES_PER_LAYER = 1000

-- Helper function to check if host tags match layer tags using word boundaries
-- SQL: host.str_tags ~* ('(?x)' || layer.str_tags || '\y')
-- This matches whole words, not substrings
local function tagsMatch(hostTagStr, layerTagPattern)
    if layerTagPattern == nil or layerTagPattern == '' then
        return true
    end

    local hostTagsLower = string.lower(hostTagStr)
    local patternLower = string.lower(layerTagPattern)

    -- Split pattern by | (OR in regex)
    for pattern in string.gmatch(patternLower, "[^|]+") do
        pattern = pattern:gsub("^%s*(.-)%s*$", "%1") -- trim

        -- Word boundary matching: check if pattern appears as a whole word
        -- Check for pattern at start, end, or surrounded by non-word chars
        local wordPattern = "%f[%w]" .. pattern:gsub("([%(%)%.%%%+%-%*%?%[%]%^%$])", "%%%1") .. "%f[%W]"
        if string.find(hostTagsLower, wordPattern) then
            return true
        end

        -- Also check exact match for simple cases
        for tag in string.gmatch(hostTagsLower, "[%w_]+") do
            if tag == pattern then
                return true
            end
        end
    end

    return false
end

-- Helper function to check if layer limits allow dispatch
-- Returns true if all limits have capacity, false if any limit is maxed out
local function checkLayerLimits(layerId)
    local layerLimitsKey = 'layer:limits:' .. layerId
    local limitIds = redis.call('SMEMBERS', layerLimitsKey)

    if #limitIds == 0 then
        return true -- No limits = always allowed
    end

    for _, limitId in ipairs(limitIds) do
        local limitKey = 'limit:' .. limitId
        local runningKey = 'limit:' .. limitId .. ':running'

        local maxValue = tonumber(redis.call('HGET', limitKey, 'maxValue') or 0)
        local running = tonumber(redis.call('GET', runningKey) or 0)

        if maxValue > 0 and running >= maxValue then
            -- Limit is maxed out - cannot dispatch to this layer
            return false
        end
    end

    return true -- All limits have capacity
end

-- Get all layer IDs that have waiting frames for this job
local layerIds = redis.call('SMEMBERS', layersWaitingKey)

if #layerIds == 0 then
    return {}
end

-- Cache layer data and check basic eligibility (tags, threadable, limits)
-- These checks don't depend on remaining resources
local eligibleLayers = {}

for _, layerId in ipairs(layerIds) do
    local layerKey = 'layer:' .. layerId
    local layerData = redis.call('HGETALL', layerKey)

    if #layerData > 0 then
        -- Parse layer data into table
        local layer = {}
        for i = 1, #layerData, 2 do
            layer[layerData[i]] = layerData[i + 1]
        end

        local minCores = tonumber(layer['minCores'] or 0)
        local minMemory = tonumber(layer['minMemory'] or 0)
        local minGpus = tonumber(layer['minGpus'] or 0)
        local minGpuMemory = tonumber(layer['minGpuMemory'] or 0)
        local threadable = layer['threadable'] == 'true'
        local layerTags = layer['tags'] or ''

        -- Check static requirements (don't depend on remaining resources)
        local eligible = true

        -- Check threadable requirement
        -- threadMode: 0 = AUTO (requires threadable), 1 = ALL (any)
        if threadMode == 0 and not threadable then
            eligible = false
        end

        -- Check tags (with word boundary matching)
        if eligible and not tagsMatch(hostTags, layerTags) then
            eligible = false
        end

        -- Check layer limits - CRITICAL for 1-to-1 SQL parity
        if eligible and not checkLayerLimits(layerId) then
            eligible = false
        end

        -- Check if layer could EVER fit on this host (initial resources)
        -- This is a quick filter before we do per-frame checks
        if eligible then
            if minCores > hostCores or minMemory > hostMemory then
                eligible = false
            end
            if noGpu == 0 then
                if minGpus > hostGpus then
                    eligible = false
                end
                -- GPU memory check: only reject if layer NEEDS GPU memory
                -- that exceeds what the host has (or host has none)
                if minGpuMemory > 0 then
                    if hostGpuMemory == 0 or minGpuMemory > hostGpuMemory then
                        eligible = false
                    end
                end
            end
        end

        if eligible then
            table.insert(eligibleLayers, {
                layerId = layerId,
                minCores = minCores,
                minMemory = minMemory,
                minGpus = minGpus,
                minGpuMemory = minGpuMemory
            })
        end
    end
end

if #eligibleLayers == 0 then
    return {}
end

-- Find smallest resource requirements across eligible layers (for early termination)
local smallestMinCores = math.huge
local smallestMinMemory = math.huge
for _, layer in ipairs(eligibleLayers) do
    if layer.minCores < smallestMinCores then
        smallestMinCores = layer.minCores
    end
    if layer.minMemory < smallestMinMemory then
        smallestMinMemory = layer.minMemory
    end
end

-- Collect all candidate frames with their resource requirements
local candidateFrames = {}

for _, layer in ipairs(eligibleLayers) do
    local framesWaitingKey = 'frames:waiting:' .. layer.layerId
    -- Get top N waiting frames for this layer (sorted by dispatch order)
    local frames = redis.call('ZRANGE', framesWaitingKey, 0, MAX_FRAMES_PER_LAYER - 1, 'WITHSCORES')

    for i = 1, #frames, 2 do
        local frameId = frames[i]
        local score = tonumber(frames[i + 1])
        table.insert(candidateFrames, {
            frameId = frameId,
            layerId = layer.layerId,
            score = score,
            minCores = layer.minCores,
            minMemory = layer.minMemory,
            minGpus = layer.minGpus,
            minGpuMemory = layer.minGpuMemory
        })
    end
end

-- Sort all candidate frames by dispatch order (score)
table.sort(candidateFrames, function(a, b)
    return a.score < b.score
end)

-- Select frames that fit in REMAINING resources
local result = {}

for _, frame in ipairs(candidateFrames) do
    -- Check if this frame fits in remaining resources
    local fits = true

    if frame.minCores > remainingCores then
        fits = false
    end
    if frame.minMemory > remainingMemory then
        fits = false
    end
    if noGpu == 0 then
        if frame.minGpus > remainingGpus then
            fits = false
        end
        if frame.minGpuMemory > remainingGpuMemory then
            fits = false
        end
    end

    if fits then
        -- Add frame to result
        table.insert(result, frame.frameId)

        -- Deduct resources from remaining pool
        remainingCores = remainingCores - frame.minCores
        remainingMemory = remainingMemory - frame.minMemory
        if noGpu == 0 then
            remainingGpus = remainingGpus - frame.minGpus
            remainingGpuMemory = remainingGpuMemory - frame.minGpuMemory
        end

        -- Stop if we have enough frames
        if #result >= limit then
            break
        end

        -- Stop if remaining resources are too low for any more frames
        if remainingCores < smallestMinCores or remainingMemory < smallestMinMemory then
            break
        end
    end
end

return result
