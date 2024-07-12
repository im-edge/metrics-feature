-- luacheck: std lua51, globals cjson redis RrdCacheD, ignore Queue
require('Counters')
require('RrdCacheD')

local Queue = {}
function Queue.new(prefix, counters)
    local self = {}

    -- Redis prefixes and keys
    local redisStream = prefix .. ':stream'
    local redisStreamDeferredCi = prefix .. ':deferred:'
    local redisStreamInvalid = prefix .. ':invalid'

    -- Stream trimming
    local maxLenStream = 1000000
    local maxLenSingleDeferred = 10000
    local maxLenInvalid = 10000

    function self.schedule(rrdUpdate)
        redis.call('XADD', redisStream, 'MAXLEN', '~', maxLenStream, '*', 'update', rrdUpdate)
        counters.incrementSucceeded()
    end

    function self.reject(jsonString)
        redis.call('XADD', redisStreamInvalid, 'MAXLEN', '~', maxLenInvalid, '*', 'json', jsonString)
        counters.incrementInvalid()
    end

    function self.defer(ci, jsonString)
        redis.call('XADD', redisStreamDeferredCi .. ci, 'MAXLEN', '~', maxLenSingleDeferred, '*', 'json', jsonString)
        counters.incrementDeferred()
    end

    function self.getDeferredLinesFor(ci)
        return redis.call('XRANGE', redisStreamDeferredCi .. ci, '-', '+')
    end

    function self.forgetDeferredLines(ci)
        redis.call('DEL', redisStreamDeferredCi .. ci)
    end

    function self.freeDeferred(ci, inventory)
        self.forgetDeferredLines(ci)
        inventory.freeDeferred(ci)
    end

    return self
end
