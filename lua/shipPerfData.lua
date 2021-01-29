require('Counters')
require('Inventory')
require('Queue')
require('JsonShipper')

local prefix = 'rrd'
local counters = Counters.new(prefix)
local result = JsonShipper.new(
    Inventory.new(prefix),
    Queue.new(prefix, counters)
).pushJson(KEYS[1])
-- add counters to result?
counters.flush()
return result