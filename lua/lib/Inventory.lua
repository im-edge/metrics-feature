require('time')
require('TableHelper')

local Inventory = {}
Inventory.new = function(prefix)
    local self = {}
    local redisKeyCiConfig = prefix .. ':ci'
    local redisKeyDeferredCi = prefix .. ':deferred-ci'

    local function mapKeys(cid, tbl)
        local map = cjson.decode(redis.call('HGET', inventory.idxCid, cid))
        if map == nil then
            return nil
        end
        return TableHelper.mapKeys(tbl, map)
    end

    function self.setCiConfig(ci, config)
        if config.filename == nil or config.dsNames == nil or config.dsMap == nil then
            error "CiConfig requires filename, dsNames and dsMap"
        end
        -- config has filename, dsNames and dsMap
        return redis.call('HSET', redisKeyCiConfig, ci, cjson.encode(config))
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

    function self.freeDeferred(ci)
        redis.call('HDEL', redisKeyDeferredCi, ci)
    end

    function self.defer(ci, dataPoints, reason)
        redis.call('HSET', redisKeyDeferredCi, ci, cjson.encode({
            deferredSince = time.now(),
            dataPoints = dataPoints,
            reason = reason
        }))
    end

    return self
end
