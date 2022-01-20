require('Counters')
require('Inventory')
require('Queue')
require('JsonShipper')

local prefix = 'rrd'
local counters = Counters.new(prefix)
local result = JsonShipper.new(
    Inventory.new(prefix),
    Queue.new(prefix, counters)
).pushJsonBulk(KEYS)
-- add counters to result?
result = counters.getPending()
counters.flush()
return result
