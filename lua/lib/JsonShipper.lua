require('RrdCacheD')

local JsonShipper = {}
JsonShipper.new = function(inventory, queue)
  local self = {}

  local function defer(ci, jsonString, isDeferred)
    if not isDeferred then
      inventory.defer(ci)
    end
    queue.defer(ci, jsonString)

    return { 'status', 'deferred' }
  end

  function self.pushJson(jsonString)
    local data = cjson.decode(jsonString)
    local ci = data.ci

    if ci == nil or data.dp == nil then
      queue.reject(jsonString)
      error('ci and dp are required')
    end

    if inventory.isDeferred(ci) then
      return defer(ci, jsonString, true)
    end

    local ciConfig = inventory.getCiConfig(ci)
    if ciConfig == nil then
      return defer(ci, jsonString, false)
    end

    local update = RrdCacheD.prepareUpdate(ciConfig, data)
    if update == nil then
      return defer(ci, jsonString, false)
    else
      queue.schedule(update)
      return { 'status', 'scheduled' }
    end
  end

  return self
end
