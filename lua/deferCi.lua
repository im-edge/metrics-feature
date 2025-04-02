-- luacheck: std lua51, globals Inventory KEYS
require('Inventory')
local prefix = 'metrics'
Inventory.new(prefix).deferWithReason(KEYS[1], ARGV[1])
