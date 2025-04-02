-- luacheck: std lua51, globals redis cjson time TableHelper, ignore Inventory

require('time')
require('TableHelper')

local Inventory = {}
Inventory.new = function(prefix)
    local self = {}
    local redisKeyCiConfig = prefix .. ':ci'
    local redisKeyDeferredCi = prefix .. ':deferred-ci'
    local redisKeyMissingCi = prefix .. ':missing-ci'
    local redisKeyMissingDs = prefix .. ':missing-ds'
    -- Duplicate, defined also in Queue:
    local redisStreamDeferredCi = prefix .. ':deferred:'

    function self.setCiConfig(ciKey, config)
        if config.filename == nil or config.dsNames == nil or config.dsMap == nil then
            error "CiConfig requires filename, dsNames and dsMap"
        end
        -- config has filename, dsNames and dsMap
        return redis.call('HSET', redisKeyCiConfig, ciKey, cjson.encode(config))
    end

    function self.getCiConfig(ci)
        local ciConfig = redis.call('HGET', redisKeyCiConfig, ci)
        if ciConfig then
            return cjson.decode(ciConfig)
        end
-- error('No CI config for ' .. ci .. ' in ' .. redisKeyCiConfig)
        return nil
    end

    function self.hasCi(ci)
        return redis.call('HEXISTS', redisKeyCiConfig, ci) > 0
    end

    function self.isDeferred(ci)
        return redis.call('HEXISTS', redisKeyDeferredCi, ci) > 0
    end

    function self.deleteCi(ciKey)
        return redis.call('HDEL', redisKeyCiConfig, ciKey) +
            redis.call('HDEL', redisKeyDeferredCi, ciKey) +
            redis.call('HDEL', redisKeyMissingCi, ciKey) +
            redis.call('HDEL', redisKeyMissingDs, ciKey) +
            redis.call('DEL', redisStreamDeferredCi .. ciKey) > 0
    end

    function self.freeDeferred(ciKey)
        redis.call('HDEL', redisKeyDeferredCi, ciKey)
        redis.call('HDEL', redisKeyMissingCi, ciKey)
        redis.call('HDEL', redisKeyMissingDs, ciKey)
    end

    function self.defer(ciKey, ts, dataPoints, reason)
        redis.call('HSET', redisKeyDeferredCi, ciKey, cjson.encode({
            deferredSince = time.now(),
            dataPoints = dataPoints,
            ts = ts,
            reason = reason
        }))
    end

    function self.deferWithReason(ciKey, reason)
        redis.call('HSET', redisKeyDeferredCi, ciKey, cjson.encode({
            deferredSince = time.now(),
            reason = reason
        }))
    end

    function self.deferMeasurementForUnknownCi(ciKey, reason, measurementJson)
        redis.call('HSET', redisKeyDeferredCi, ciKey, cjson.encode({
            deferredSince = time.now(),
            reason = reason
        }))
        redis.call('HSET', redisKeyMissingCi, ciKey, measurementJson)
    end

    function self.deferMeasurementForMissingDs(ciKey, ciConfig, reason, measurementJson)
        redis.call('HSET', redisKeyDeferredCi, ciKey, cjson.encode({
            deferredSince = time.now(),
            reason = reason
        }))

-- TODO: also pass ciConfig?!
        redis.call('HSET', redisKeyMissingDs, ciKey, measurementJson)
    end

    return self
end
