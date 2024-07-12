-- luacheck: std lua51, globals Counters Queue Inventory MeasurementShipper KEYS
require('Counters')
require('Inventory')
require('Queue')
require('MeasurementShipper')

local prefix = 'metrics'
local counters = Counters.new(prefix)
-- local pushResult = MeasurementShipper.new(...) -> combine with counters?
MeasurementShipper.new(
    Inventory.new(prefix),
    Queue.new(prefix, counters)
).pushMeasurements(KEYS)

return counters.flush()
