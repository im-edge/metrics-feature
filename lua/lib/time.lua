-- luacheck: std lua51, globals redis, ignore TableHelper
local time = {}
local cachedNow

function time.now()
    return redis.call("TIME")[1]
end

function time.nowOnce()
    if cachedNow == nil then
        cachedNow = time.now()
    end

    return cachedNow
end

-- return time
