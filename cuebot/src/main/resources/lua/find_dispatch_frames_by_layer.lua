--[[
  Find next dispatch frames for a specific layer and host.

  This Lua script mirrors the SQL query FIND_DISPATCH_FRAME_BY_LAYER_AND_HOST.
  Simpler than the job version - only checks one layer instead of iterating.

  SMART RESOURCE TRACKING:
  Since all frames in a layer have identical resource requirements, we can
  calculate exactly how many frames will fit: floor(remaining / required).
  This returns ONLY frames that will actually fit on the host.

  KEYS:
    KEYS[1] = layer:{layerId} - Layer metadata hash
    KEYS[2] = frames:waiting:{layerId} - Sorted set of waiting frame IDs
    KEYS[3] = layer:limits:{layerId} - Set of limit IDs for this layer

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

local layerKey = KEYS[1]
local framesWaitingKey = KEYS[2]
local layerLimitsKey = KEYS[3]
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

-- Helper to calculate how many frames can fit given remaining resources
local function calculateMaxFrames(remaining, required)
    if required <= 0 then
        return limit  -- No requirement = unlimited (up to limit)
    end
    return math.floor(remaining / required)
end

-- Helper function to check if host tags satisfy layer's required tags.
-- Layer tags are stored as a SET in Redis (layer:{id}:tags).
-- Host tags are passed as pre-normalized values from Java.
-- Returns true if host has at least ONE of the required tags (OR logic).
local function tagsMatch(layerTagsKey, hostTagSet)
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
local function checkLayerLimits()
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

-- Get layer data
local layerData = redis.call('HGETALL', layerKey)

if #layerData == 0 then
    return {}
end

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
local layerTagsKey = layerKey .. ':tags'

-- Check resource requirements
local resourcesMatch = true

-- Check cores
if minCores > hostCores then
    resourcesMatch = false
end

-- Check memory
if minMemory > hostMemory then
    resourcesMatch = false
end

-- GPU checks (skip if noGpu mode)
if noGpu == 0 then
    -- Check GPUs
    if minGpus > hostGpus then
        resourcesMatch = false
    end

    -- GPU memory check: only reject if layer NEEDS GPU memory
    -- that exceeds what the host has (or host has none)
    if minGpuMemory > 0 then
        if hostGpuMemory == 0 or minGpuMemory > hostGpuMemory then
            resourcesMatch = false
        end
    end
end

-- Check threadable requirement
if threadMode == 0 and not threadable then
    resourcesMatch = false
end

-- Check tags (layer tags stored as SET, host tags from ARGV)
if resourcesMatch and not tagsMatch(layerTagsKey, hostTagSet) then
    resourcesMatch = false
end

-- Check layer limits - CRITICAL for 1-to-1 SQL parity
if resourcesMatch and not checkLayerLimits() then
    resourcesMatch = false
end

-- If layer doesn't match, return empty
if not resourcesMatch then
    return {}
end

-- Layer matches - calculate how many frames can actually fit
-- Since all frames in a layer have identical requirements, we calculate:
-- maxFrames = min(hostCores/minCores, hostMemory/minMemory, ...)

local maxByResources = limit  -- Start with requested limit

-- Calculate max frames by cores
if minCores > 0 then
    local maxByCores = calculateMaxFrames(hostCores, minCores)
    if maxByCores < maxByResources then
        maxByResources = maxByCores
    end
end

-- Calculate max frames by memory
if minMemory > 0 then
    local maxByMemory = calculateMaxFrames(hostMemory, minMemory)
    if maxByMemory < maxByResources then
        maxByResources = maxByMemory
    end
end

-- Calculate max frames by GPUs (if not in noGpu mode)
if noGpu == 0 then
    if minGpus > 0 then
        local maxByGpus = calculateMaxFrames(hostGpus, minGpus)
        if maxByGpus < maxByResources then
            maxByResources = maxByGpus
        end
    end

    if minGpuMemory > 0 and hostGpuMemory > 0 then
        local maxByGpuMem = calculateMaxFrames(hostGpuMemory, minGpuMemory)
        if maxByGpuMem < maxByResources then
            maxByResources = maxByGpuMem
        end
    end
end

-- Ensure at least 0
if maxByResources < 0 then
    maxByResources = 0
end

-- Return exactly the frames that will fit (no wasted queries)
if maxByResources == 0 then
    return {}
end

return redis.call('ZRANGE', framesWaitingKey, 0, maxByResources - 1)
