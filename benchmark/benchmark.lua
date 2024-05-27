
-- package.path = "../?/init.lua;./?/init.lua"
-- package.cpath = "../?.so;../?.dylib;./?.so;./?.dylib"

local fiber = require('fiber')
local pg = require('pg')

local host, port, user, pass, db = "localhost", 5432, "postgres", "linux", "linux"

function GenerateBatchData(batchSize, value)
    local arr = {}
    for i = 1, batchSize do
        arr[i] = value
    end

    return arr
end

function SendBatchData(Pool, BatchSize, BatchCount)
    local conn = Pool:get()
    for i=1, BatchCount, 1 do
        local BatchData = GenerateBatchData(BatchSize,
            {cur_balance = 10, delta = -1, operation_time = "10:00", description = "test", UID = 0}
        )
        conn:batch_execute("insert_jsonb_array", BatchData)
    end
    Pool:put(conn)
end

function SendParamData(Pool, DataCount)
    local conn = Pool:get()
    for i=1, DataCount, 1 do
        conn:execute("INSERT INTO _test_table VALUES($1, $2, $3, $4, $5)", 10, -1, "10:00", "test", 0)
    end
    Pool:put(conn)
end

function RunBatchStressTest(FibersNum, BatchSize, BatchCount, Pool)
    for i=1, FibersNum, 1 do
        fiber.create(SendBatchData, Pool, BatchSize, BatchCount / FibersNum)
    end
end

function RunParamStressTest(FibersNum, DataCount, Pool)
    for i=1, FibersNum, 1 do
        fiber.create(SendParamData, Pool, DataCount / FibersNum)
    end
end

conn, msg = pg.connect({ host = host, port = port, user = user, pass = pass,
    db = db, raise = false})
if conn == nil then error(msg) end

print("Drop")
conn:execute("DROP TABLE IF EXISTS _test_table")
print("Create")
conn:execute("CREATE TABLE _test_table(cur_balance int, delta int, operation_time text, description text, UID int)")

conn:execute("DROP TABLE IF EXISTS my_table")
conn:execute("CREATE TABLE my_table(data JSONB)")

conn:execute("SELECT pg_stat_statements_reset()")

local InsertionsNum = 1000000
-- local FibersNumArray = {1, 2, 4, 8, 16}
local FibersNumArray = {8} -- 31 * 100000 = 3 100 000
-- local BatchSizesArray = {100000, 10000, 1000, 100, 10}
local BatchSizesArray = {1000}

for _, FibersNum in ipairs(FibersNumArray) do
    for _, BatchSize in ipairs(BatchSizesArray) do
        print("FibN:", FibersNum)
        print("Batch:", BatchSize)
        pool, msg = pg.pool_create({ host = host, port = port, user = user, pass = pass,
        db = db, raise = false, size = FibersNum })

        if pool == nil then error(msg) end
        local start_time = os.clock()
        RunBatchStressTest(FibersNum, BatchSize, InsertionsNum / BatchSize, pool)
        local end_time = os.clock()
        local elapsed_time = end_time - start_time
        print("Время работы функции: " .. elapsed_time .. " секунд")
    end
end

-- for _, FibersNum in ipairs(FibersNumArray) do
--     print("Start with " .. FibersNum .. " fibers")
--     pool, msg = pg.pool_create({ host = host, port = port, user = user, pass = pass,
--         db = db, raise = false, size = FibersNum })

--     if pool == nil then error(msg) end

--     local start_time = os.clock()
--     RunParamStressTest(FibersNum, InsertionsNum, pool)
--     local end_time = os.clock()
--     local elapsed_time = end_time - start_time
--     print("Время работы функции: " .. elapsed_time .. " секунд")
-- end








