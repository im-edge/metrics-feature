-- luacheck: std lua51, ignore TableHelper
local TableHelper = {}

-- Whether table has (at least) the same keys as keys
function TableHelper.hasAtLeastSameKeys(table, keys)
    for k, _ in ipairs(keys) do
        if table[k] == nil then
            return false
        end
    end

    return true
end

-- Whether table has (at least) the same keys as keys
function TableHelper.hasAtLeastValuesAsKeys(table, keys)
    for _, v in ipairs(keys) do
        if table[v] == nil then
            return false
        end
    end

    return true
end

function TableHelper.mapKeys(table, keyMap)
    local newKey
    local result = {}
    for k, v in ipairs(table) do
        newKey = keyMap[k]
        if newKey == nil then
            newKey = k
        end
        result[newKey] = v
    end

    return result
end

function TableHelper.mapKey(key, keyMap)
    if keyMap[key] == nil then
        return key
    else
        return keyMap[key]
    end
end
