local RrdCacheD = {}

RrdCacheD.escapeFilename = function (filename)
    return string.gsub(filename, ' ' , '\\ ')
end

RrdCacheD.prepareValueString = function(ciConfig, perfData)
    local dsNames = ciConfig.dsNames; -- Sort order
    local dsMap = ciConfig.dsMap; -- name mapping
    local value
    local sorted = {}

    for name, _ in pairs(perfData.dp) do
        if dsMap[name] == nil then
            -- will lead to deferral
            return nil, 'Unknown DS name "' + name + '"'
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

RrdCacheD.prepareUpdate = function(ciConfig, perfData)
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
