-- luacheck: std lua51, globals Inventory KEYS
require('Inventory')
local prefix = 'metrics'
return Inventory.new(prefix).deleteCi(KEYS[1])
