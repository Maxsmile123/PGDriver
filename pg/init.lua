-- init.lua (internal file)

local fiber = require('fiber')
local driver = require('pg.driver')
local ffi = require('ffi')
local json = require("dkjson")

local pool_mt
local conn_mt

--create a new connection
local function conn_create(pg_conn)
    local queue = fiber.channel(1)
    queue:put(true)
    local conn = setmetatable({
        usable = true,
        conn = pg_conn,
        queue = queue,
    }, conn_mt)

    return conn
end

-- get connection from pool
local function conn_get(pool)
    local pg_conn = pool.queue:get()
    local status
    if pg_conn == nil then
        status, pg_conn = driver.connect(pool.conn_string)
        if status < 0 then
            return error(pg_conn)
        end
    end
    local conn = conn_create(pg_conn, pool)
    conn.__gc_hook = ffi.gc(ffi.new('void *'),
        function(self)
            pg_conn:close()
            pool.queue:put(nil)
        end)
    return conn
end

local function conn_put(conn)
    local pgconn = conn.conn
    ffi.gc(conn.__gc_hook, nil)
    if not conn.queue:get() then
        conn.usable = false
        return nil
    end
    conn.usable = false
    return pgconn
end

conn_mt = {
    __index = {
        execute = function(self, sql, ...)
            if not self.usable then
                return get_error(self.raise.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Connection is broken')
            end
            local status, datas = self.conn:execute(sql, ...)
            if status ~= 0 then
                self.queue:put(status > 0)
                return error(datas)
            end
            self.queue:put(true)
            return datas, true
        end,
        batch_execute = function(self, sql, data)
            if not self.usable then
                return get_error(self.raise.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Connection is broken')
            end
            local function convert_table_to_json(tbl)
                local batch_size = 0
                local data_size = 0
                for key, value in pairs(tbl) do
                    if type(value) == "table" then
                        batch_size = batch_size + 1
                        tbl[key] = json.encode(value)
                        data_size = data_size + string.len(tbl[key])
                        io.write("len is ", string.len(tbl[key]), "\n")
                        io.write("Res is ", tbl[key], "\n")
                        io.write(type(tbl[key]), "\n")
                    end
                end
                return batch_size, data_size
            end
            io.write("Try Convert\n")
            local batch_size, data_size = convert_table_to_json(data)
            if batch_size == 0 then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Batch is empty') 
            end
            print(batch_size)
            print(data_size)

            local function construct_command(batch_size)
                local sql_command = "SELECT " .. sql .. "(ARRAY["
                for i=1, batch_size, 1 do
                    if i ~= batch_size then
                        sql_command = sql_command .. "$" .. i .. "::jsonb, "
                    else
                        sql_command = sql_command .. "$" .. i .. "::jsonb"
                    end
                end
                sql_command = sql_command .. "]::jsonb[])"
                return sql_command
            end
            local status, datas = self.conn:batch_execute(
                construct_command(batch_size), batch_size, data
            )
            if status ~= 0 then
                self.queue:put(status > 0)
                return error(datas)
            end
            self.queue:put(true)
            return datas, true
        end,
        begin = function(self)
            return self:execute('BEGIN') ~= nil
        end,
        commit = function(self)
            return self:execute('COMMIT') ~= nil
        end,
        rollback = function(self)
            return self:execute('ROLLBACK') ~= nil
        end,
        ping = function(self)
            local status, data, msg = pcall(self.execute, self, 'SELECT 1 AS code')
            return msg and data[1][1].code == 1
        end,
        close = function(self)
            if not self.usable then
                return error('Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return error('Connection is broken')
            end
            self.usable = false
            self.conn:close()
            self.queue:put(false)
            return true
        end,
        active = function(self)
            if not self.usable then
                return get_error(self.raise.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Connection is broken')
            end
            local status, msg = self.conn:active()
            if status ~= 1 then
                self.queue:put(false)
                return get_error(self.raise.pool, msg)
            end
            self.queue:put(true)
            return msg
        end
    }
}

local function build_conn_string(opts)
    if opts.conn_string then
        return opts.conn_string
    end
    local connb = {}
    if opts.host then
        table.insert(connb, string.format(" host='%s'", opts.host))
    end
    if opts.port then
        table.insert(connb, string.format(" port='%s'", opts.port))
    end
    if opts.user then
        table.insert(connb, string.format(" user='%s'", opts.user))
    end
    if opts.pass or opts.password then
        table.insert(connb, string.format(" password='%s'",
            opts.pass or opts.password))
    end
    if opts.db then
        table.insert(connb, string.format(" dbname='%s'", opts.db))
    end
    return table.concat(connb)
end

-- Create connection pool. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string, size and raise flag.
local function pool_create(opts)
    opts = opts or {}
    local conn_string = build_conn_string(opts)
    opts.size = opts.size or 1
    local queue = fiber.channel(opts.size)

    for i = 1, opts.size do
        local status, conn = driver.connect(conn_string)
        if status < 0 then
            while queue:count() > 0 do
                local pg_conn = queue:get()
                pg_conn:close()
            end
            if status < 0 then
                return error(conn)
            end
        end
        queue:put(conn)
    end

    return setmetatable({
        -- connection variables
        host        = opts.host,
        port        = opts.port,
        user        = opts.user,
        pass        = opts.pass,
        db          = opts.db,
        size        = opts.size,
        conn_string  = conn_string,

        -- private variables
        queue       = queue,
        usable      = true
    }, pool_mt)
end

-- Close pool
local function pool_close(self)
    self.usable = false
    for i = 1, self.size do
        local pg_conn = self.queue:get()
        if pg_conn ~= nil then
            pg_conn:close()
        end
    end
end

-- Returns connection
local function pool_get(self)
    if not self.usable then
        return get_error(self.raise, 'Pool is not usable')
    end
    local conn = conn_get(self)
    local reset_sql = 'BEGIN; RESET ALL; COMMIT;'
    if conn:active() then
        reset_sql = 'ROLLBACK; ' .. reset_sql
    end
    conn:execute(reset_sql)
    return conn
end

-- Free binded connection
local function pool_put(self, conn)
    if conn.usable then
        self.queue:put(conn_put(conn))
    end
end

pool_mt = {
    __index = {
        get = pool_get;
        put = pool_put;
        close = pool_close;
    }
}

-- Create connection. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string and raise flag.
local function connect(opts)
    opts = opts or {}

    local conn_string = build_conn_string(opts)
    local status, pg_conn = driver.connect(conn_string)
    if status < 0 then
        return error(pg_conn)
    end
    return conn_create(pg_conn)
end

return {
    connect = connect;
    pool_create = pool_create;
}
