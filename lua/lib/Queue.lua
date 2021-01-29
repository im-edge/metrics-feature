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

    function self.freeDeferred(ci, inventory)
        redis.call('DEL', redisStreamDeferredCi .. ci)
        inventory.freeDeferred(ci)
    end

    function self.rerunDeferred(ci, inventory)
        local jsonString, perfData, update
        for i, r in ipairs(self.getDeferredLinesFor(ci)) do
            -- r -> [{"ok":"1537881339696-0"},["line","guest=0c idle=13794c .. 1537881339"]
            jsonString = r[2][2]
            perfData = cjson.decode(jsonString)
            update = RrdCacheD.prepareUpdate(inventory.getCiConfig(ci), perfData)
            if update == nil then
                self.reject(jsonString)
            else
                self.schedule(update)
            end
        end
        self.freeDeferred(ci, inventory)

        return counters.flush()
    end

    return self
end
