require('RrdCacheD')

local JsonShipper = {}
JsonShipper.new = function(inventory, queue)
  local self = {}

  function self.pushJson(jsonString)
    local data = cjson.decode(jsonString)
    local ci = data.ci

    if ci == nil then
      queue.reject(jsonString)
      error('CI name is required')
    end

    if data.dp == nil then
      queue.reject(jsonString)
      error('DataPoint list (dp) is required')
    end

    if next(data.dp) == nil then
      return { 'status', 'ignored', 'reason', 'DataPoint list (dp) is empty: ' .. ci }
    end

    if inventory.isDeferred(ci) then
      queue.defer(ci, jsonString)
      return { 'status', 'deferred', 'reason', 'CI is already deferred: ' .. ci }
    end

    local ciConfig = inventory.getCiConfig(ci)
    if ciConfig == nil then
      inventory.defer(ci, data.dp, 'Unknown CI')
      return { 'status', 'deferred', 'reason', 'Unknown CI: ' .. ci }
    end

    local update, err = RrdCacheD.prepareUpdate(ciConfig, data)
    if err then
      inventory.defer(ci, data.dp, err)
      return { 'status', 'deferred', 'reason', 'Deferred "' .. ci .. '": ' .. err }
    end

    queue.schedule(update)
    return { 'status', 'scheduled' }
  end

  return self
end
