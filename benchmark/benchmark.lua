#!/usr/bin/env tarantool

package.path = "../?/init.lua;./?/init.lua"
package.cpath = "../?.so;../?.dylib;./?.so;./?.dylib"

local fiber = require('fiber')
local pg = require('pg')

local host, port, user, pass, db = "localhost", "linux", "linux", "linux"

function GenerateBatchData(batchSize, value)
    local arr = {}
    for i = 1, batchSize do
        arr[i] = value
    end
    return arr
end

function SendBatchData(Pool, BatchCount, BatchData)
    local conn = Pool.get()
    for i=1, BatchCount, 1 do
        conn.batch_execute("insert_json_array", BatchData)
    end
    Pool.put(conn)
end

function SendParamData(Pool, DataCount)
    local conn = Pool.get()
    for i=1, DataCount, 1 do
        conn.execute("INSERT INTO _test_table VALUES($1, $2, $3, $4, $5)", 10, -1, "10:00", "test", 0)
    end
    Pool.put(conn)
end


function RunBatchStressTest(FibersNum, BatchSize, BatchCount)
    pool, msg = pg.pool_create({ host = host, port = port, user = user, pass = pass,
        db = db, raise = false, size = FibersNum })

    if pool == nil then error(msg) end

    local BatchData = GenerateBatchData(BatchSize,
        {cur_balance = 10, delta = -1, operation_time = "10:00", description = "test", UID = 0}
    )

    for i=1, FibersNum, 1 do
        fiber.create(SendBatchData, pool, BatchCount, BatchData)
    end
end


function RunParamStressTest(FibersNum, DataCount)
    pool, msg = pg.pool_create({ host = host, port = port, user = user, pass = pass,
        db = db, raise = false, size = FibersNum })

    if pool == nil then error(msg) end

    for i=1, FibersNum, 1 do
        fiber.create(SendParamData, pool, DataCount)
    end
end

local InsertionsNum = 100000
local FibersNumArray = {1, 2, 4, 8, 16}
local BatchSizesArray = {100000, 10000, 1000, 100, 10}

for _, FibersNum in ipairs(FibersNumArray) do
    for _, BatchSize in ipairs(BatchSizesArray) do
        RunBatchStressTest(FibersNum, BatchSize, InsertionsNum / BatchSize)
    end
end







