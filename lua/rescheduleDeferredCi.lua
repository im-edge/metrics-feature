require('Queue')
require('Inventory')

local prefix = 'rrd'
local counters = Counters.new(prefix)
local result = Queue.new(prefix, counters).rerunDeferred(KEYS[1], Inventory.new(prefix))
counters.flush()
return result
