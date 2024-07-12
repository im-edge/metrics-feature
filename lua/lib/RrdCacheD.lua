local RrdCacheD = {}

RrdCacheD.escapeFilename = function (filename)
    return string.gsub(filename, ' ' , '\\ ')
end

RrdCacheD.prepareValueString = function(ciConfig, measurement)
    -- important: another error condition would require changes in MeasurementShipper.pushMeasurement()
    local dsNames = ciConfig.dsNames; -- Sort order
    local dsMap = ciConfig.dsMap; -- name mapping
    local value
    local metrics = measurement[3]
    local sorted = {}

    -- for name, _ in pairs(metrics) do
    for name, _ in pairs(metrics) do
        if dsMap[name] == nil then
            -->
            -- will lead to deferral
            return nil, 'Unknown DS name "' .. name .. '"'
        end
    end

    -- metric -> label, value, type, unit
    for _, name in ipairs(dsNames) do
        if metrics[name] == nil then
            table.insert(sorted, 'U')
        else
            value = metrics[name][2]
            if (value == nil) or (value == cjson.null) then
                table.insert(sorted, 'U')
            else
                table.insert(sorted, value)
            end
        end
    end

    return table.concat(sorted, ':'), nil
end

RrdCacheD.prepareUpdate = function(ciConfig, measurement)
    local valueString, err = RrdCacheD.prepareValueString(ciConfig, measurement)
    if err then
        return nil, err
    end
    local ts
    if (measurement[2]) == nil or (measurement[2] == cjson.null) then
        error("RRDCached requires a valid timestamp, cannot set N")
    else
        ts = math.floor(measurement[2])
    end

    return RrdCacheD.escapeFilename(ciConfig.filename) .. ' ' .. ts .. ':' .. valueString, nil
end

RrdCacheD.xxxprepareValueString = function(ciConfig, perfData)
    -- important: another error condition would require changes in MeasurementShipper.pushMeasurement()
    local dsNames = ciConfig.dsNames; -- Sort order
    local dsMap = ciConfig.dsMap; -- name mapping
    local value
    local sorted = {}

    for name, _ in pairs(perfData.dp) do
        if dsMap[name] == nil then
            -- will lead to deferral
            return nil, 'Unknown DS name "' .. name .. '"'
        end
    end

    for _, name in ipairs(dsNames) do
        if perfData.dp[name] == nil then
            table.insert(sorted, 'U')
        else
            value = perfData.dp[name]
            if string.sub(value, -1) == 'c' then
                -- strip last character:
                value = string.sub(value, 1, -2)
            end
            table.insert(sorted, value)
        end
    end

    return table.concat(sorted, ':'), nil
end

RrdCacheD.xxprepareUpdate = function(ciConfig, perfData)
    local valueString, err = RrdCacheD.prepareValueString(ciConfig, perfData)
    if err then
        return nil, err
    end
    local ts
    if perfData.ts == nil then
        ts = 'N'
    else
        ts = math.floor(perfData.ts)
    end

    return RrdCacheD.escapeFilename(ciConfig.filename) .. ' ' .. ts .. ':' .. valueString, nil
end
