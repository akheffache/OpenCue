--[[
  Find next dispatch frames for a job and host.

  This Lua script mirrors the SQL query FIND_DISPATCH_FRAME_BY_JOB_AND_HOST.
  It atomically finds waiting frames that match host resource requirements.

  OPTIMIZED ALGORITHM:
  1. Get eligible layers (filtered by tags, resources, limits)
  2. Sort layers by dispatchOrder (matches SQL ORDER BY)
  3. Process layers in order, take frames from each until host is full
  NO giant frame list, NO expensive sorting of all frames.

  KEYS:
    KEYS[1] = layers:waiting:{jobId} - Set of layer IDs with waiting frames

  ARGV:
    ARGV[1] = hostCores     - Available cores on host
    ARGV[2] = hostMemory    - Available memory on host (bytes)
    ARGV[3] = hostGpus      - Available GPUs on host
    ARGV[4] = hostGpuMemory - Available GPU memory on host (bytes)
    ARGV[5] = threadMode    - 0 = AUTO, 1 = ALL (for threadable check)
    ARGV[6] = limit         - Maximum number of frames to return
    ARGV[7] = noGpu         - 1 = skip GPU checks (NO_GPU mode), 0 = normal
    ARGV[8] = tagCount      - Number of host tags
    ARGV[9+] = hostTags     - Individual host tags (pre-normalized in Java)

  Returns:
    List of frame IDs that match the criteria, ordered by dispatch order
]]

local layersWaitingKey = KEYS[1]
local hostCores = tonumber(ARGV[1])
local hostMemory = tonumber(ARGV[2])
local hostGpus = tonumber(ARGV[3])
local hostGpuMemory = tonumber(ARGV[4])
local threadMode = tonumber(ARGV[5])
local limit = tonumber(ARGV[6])
local noGpu = tonumber(ARGV[7] or 0)
local tagCount = tonumber(ARGV[8] or 0)

-- Build host tag set from ARGV (no string parsing - tags pre-normalized in Java)
local hostTagSet = {}
for i = 1, tagCount do
    hostTagSet[ARGV[8 + i]] = true
end

-- Track remaining resources as we select frames
local remainingCores = hostCores
local remainingMemory = hostMemory
local remainingGpus = hostGpus
local remainingGpuMemory = hostGpuMemory

-- Helper function to check if host tags satisfy layer's required tags.
-- Layer tags are stored as a SET in Redis (layer:{id}:tags).
-- Host tags are passed as pre-normalized values from Java.
-- Returns true if host has at least ONE of the required tags (OR logic).
local function tagsMatch(layerId, hostTagSet)
    local layerTagsKey = 'layer:' .. layerId .. ':tags'
    local requiredTags = redis.call('SMEMBERS', layerTagsKey)

    -- No required tags = layer can run on any host
    if #requiredTags == 0 then
        return true
    end

    -- OR logic: host needs at least ONE of the required tags
    for _, reqTag in ipairs(requiredTags) do
        if hostTagSet[reqTag] then
            return true  -- Found a match!
        end
    end
    return false  -- No matching tag found
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
        local dispatchOrder = tonumber(layer['dispatchOrder'] or 0)

        -- Check static requirements (don't depend on remaining resources)
        local eligible = true

        -- Check threadable requirement
        -- threadMode: 0 = AUTO (requires threadable), 1 = ALL (any)
        if threadMode == 0 and not threadable then
            eligible = false
        end

        -- Check tags (layer tags stored as SET, host tags from ARGV)
        if eligible and not tagsMatch(layerId, hostTagSet) then
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
                dispatchOrder = dispatchOrder,
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

-- Sort layers by dispatchOrder (matches SQL: ORDER BY int_dispatch_order)
table.sort(eligibleLayers, function(a, b)
    return a.dispatchOrder < b.dispatchOrder
end)

-- Process layers in order, take frames from each until host is full
-- NO giant frame list, NO sorting of frames - much more efficient!
local result = {}

for _, layer in ipairs(eligibleLayers) do
    -- Skip this layer if it can no longer fit in remaining resources
    if layer.minCores > remainingCores or layer.minMemory > remainingMemory then
        goto continue
    end
    if noGpu == 0 then
        if layer.minGpus > remainingGpus or layer.minGpuMemory > remainingGpuMemory then
            goto continue
        end
    end

    -- Calculate how many frames from this layer we can fit
    local canFitCores = math.floor(remainingCores / layer.minCores)
    local canFitMemory = math.floor(remainingMemory / layer.minMemory)
    local canFit = math.min(canFitCores, canFitMemory)

    if noGpu == 0 and layer.minGpus > 0 then
        local canFitGpus = math.floor(remainingGpus / layer.minGpus)
        canFit = math.min(canFit, canFitGpus)
    end
    if noGpu == 0 and layer.minGpuMemory > 0 then
        local canFitGpuMem = math.floor(remainingGpuMemory / layer.minGpuMemory)
        canFit = math.min(canFit, canFitGpuMem)
    end

    -- Don't fetch more frames than we need
    local needed = math.min(canFit, limit - #result)
    if needed <= 0 then
        break  -- We have enough frames
    end

    -- Get frames from this layer (already sorted by layerOrder in sorted set)
    local framesWaitingKey = 'frames:waiting:' .. layer.layerId
    local frames = redis.call('ZRANGE', framesWaitingKey, 0, needed - 1)

    -- Add frames to result and deduct resources
    for _, frameId in ipairs(frames) do
        table.insert(result, frameId)
        remainingCores = remainingCores - layer.minCores
        remainingMemory = remainingMemory - layer.minMemory
        if noGpu == 0 then
            remainingGpus = remainingGpus - layer.minGpus
            remainingGpuMemory = remainingGpuMemory - layer.minGpuMemory
        end
    end

    -- Stop if we have enough frames
    if #result >= limit then
        break
    end

    ::continue::
end

return result
