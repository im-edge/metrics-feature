-- luacheck: std lua51, globals cjson RrdCacheD
require('RrdCacheD')

local MeasurementShipper = {}
MeasurementShipper.new = function(inventory, queue)
  local self = {}

  function self.pushMeasurements(rows)
    -- local result = {}
    for _, row in ipairs(rows) do
      self.pushMeasurement(row)
    --  table.insert(result, self.pushData(row))
    end
    -- return result
  end

  function self.pushMeasurement(jsonString)
    local measurement = cjson.decode(jsonString)
    local ci = measurement[1]
    local dp = measurement[3]

    if ci == nil then
      queue.reject(jsonString)
      error('CI name is required' .. jsonString)
    end

    if dp == nil then
      queue.reject(jsonString)
      error('DataPoint list (dp) is required')
    end

    if next(dp) == nil then
      return MeasurementShipper.ignoredStatus('DataPoint list (dp) is empty: ' .. ci)
    end

    if inventory.isDeferred(ci) then
      queue.defer(ci, jsonString)
      return MeasurementShipper.deferredStatus('CI is already deferred: ' .. ci)
    end

    local ciConfig = inventory.getCiConfig(ci)
    if ciConfig == nil then
      local reason = 'Unknown CI'
      inventory.deferMeasurementForUnknownCi(ci, reason, jsonString)
      -- inventory.deferCi(ci, measurement.ts, measurement.dp, 'Unknown CI')
      return MeasurementShipper.deferredStatus(reason .. ': ' .. ci)
    end

    local update, err = RrdCacheD.prepareUpdate(ciConfig, measurement)

    -- Hint: there is only one error condition in RrdCacheD.prepareUpdate()
    if err then
      inventory.deferMeasurementForMissingDs(ci, ciConfig, 'Missing DS', jsonString)
      -- inventory.defer(ci, measurement.ts, measurement.dp, err)
      return MeasurementShipper.deferredStatus('Deferred "' .. ci .. '": ' .. err )
    end

    queue.schedule(update)
    return { 'status', 'scheduled' }
  end

  function self.reRunDeferred(ci, ciConfig)
    local jsonString, measurement, update, err, deferredLines
    local stillDeferred = {}
    deferredLines = queue.getDeferredLinesFor(ci)
    if deferredLines == nil then
      error('Got no deferredLines for ' .. ci)
    end

    for _, r in ipairs(deferredLines) do
      -- r -> [{"ok":"1537881339696-0"},["line","guest=0c idle=13794c .. 1537881339"] ???
      -- r[2][2] = ["[\"b11a53ca-5c78-4597-afec-b03c32531713\",\"if_traffic\",\"29\",[]]",null,{"ifInOctets":["ifInOctets",12838756,"COUNTER",null],"ifOutOctets":["ifOutOctets",1536727,"COUNTER",null],"ifOutQLen":["ifOutQLen",0,"GAUGE",null]}]
      jsonString = r[2][2]
      measurement = cjson.decode(jsonString)

      if next(stillDeferred) then
        table.insert(stillDeferred, jsonString)
      end

      update, err = RrdCacheD.prepareUpdate(ciConfig, measurement)

      -- Hint: there is only one error condition in RrdCacheD.prepareUpdate()
      if err then
        inventory.deferMeasurementForMissingDs(ci, ciConfig, 'Missing DS', jsonString)
        table.insert(stillDeferred, jsonString)
      else
        queue.schedule(update)

        if update == nil then
          queue.reject(jsonString)
        else
          queue.schedule(update)
        end
      end
    end

    if next(stillDeferred) then
      queue.forgetDeferredLines(ci)
      for _, r in ipairs(stillDeferred) do
        queue.defer(ci, r)
      end
      return self
      -- return MeasurementShipper.deferredStatus('Deferred "' .. ci .. '": ' .. err )
    end

    queue.freeDeferred(ci, inventory)
    inventory.setCiConfig(ci, ciConfig)

    return self
  end

  return self
end

MeasurementShipper.deferredStatus = function (status)
  return { 'status', 'deferred', 'reason', status }
end

MeasurementShipper.ignoredStatus = function (status)
  return { 'status', 'deferred', 'reason', status }
end
