-- luacheck: std lua51, globals redis, ignore Counters
local Counters = {}
Counters.new = function(prefix)
    local redisKey = prefix .. ':counters'
    local succeeded = 0
    local deferred = 0
    local invalid = 0
    local self = {}

    function self.clear()
        succeeded = 0
        deferred = 0
        invalid = 0
    end

    local function redisIncrBy(key, increment)
        if increment ~= 0 then
          redis.call('HINCRBY', redisKey, key, increment)
        end
    end

    function self.getPending()
        return {
            'succeeded',
            succeeded,
            'deferred',
            deferred,
            'invalid',
            invalid
        }
    end

    function self.incrementSucceeded()
        succeeded = succeeded + 1
    end

    function self.incrementDeferred()
        deferred = deferred + 1
    end

    function self.incrementInvalid()
        invalid = invalid + 1
    end

    function self.flush()
        local result = self.getPending()
        redisIncrBy('succeeded', succeeded)
        redisIncrBy('deferred', deferred)
        redisIncrBy('invalid', invalid)
        self.clear()

        return result
    end

    return self
end
