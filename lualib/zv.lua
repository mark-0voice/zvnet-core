
--[[

服务器网络编程

1. socket

2. bind listen

3. accept   int clientfd = accept(listenfd, &addr, &len)

int n = write(fd, buf, sz);  n < sz

4. create thread    socket    read  write

5. 0 = read(fd, buf, sz)   连接断开    close(fd)


reactor /proactor   windows  mac  linux(ubuntu centos)

a. 连接的建立  accept  connect
b. 连接的断开  0 = read 
c. 数据的接收
d. 数据的发送


1. listenfd 注册读事件到 io 多路复用， 新的连接到来， 读事件触发，建立连接（accept)

2. clientfd  注册读事件到 io 多路复用， 数据到来， 读事件触发， 接收数据（read）

3. clientfd 主动发送数据，数据发送不完全， 注册写事件到 io 多路复用， 写事件触发，数据发送完毕，注销写事件

]]

local ae = require "zvnet.ae"

local net = require "zvnet.anet"

local buffer = require "zvnet.buffer"

local timer = require "timer"

local tab_isempty = require "table.isempty"

local _M = {}

local socket_pool = {}
local connection_pool = {}
local caller = {}

local function co_attach(fd)
    local running = coroutine.running()
    caller[running] = fd
end

local function co_detach(fd)
    local running = coroutine.running()
    assert(caller[running] == fd, "please use co_attach first and make sure runfd is right")
    caller[running] = nil
end

local function co_runfd(co)
    return caller[co or coroutine.running()]
end

local aefd
local function close(fd)
    local s = socket_pool[fd]
    if s then
        if s.rbuffer then
            s.rbuffer:clear()
        end
        if s.wbuffer then
            s.wbuffer:clear()
        end
        socket_pool[fd] = nil
        if s.pool_name then
            local pool = connection_pool[s.pool_name]
            if s.status and pool[s.status] then
                pool[s.status][fd] = nil
            end
        end
        ae.del(aefd, fd)
        net.close(fd)
    end
end

_M.close = close


local function ev_base_handler(fd, readable, writeable, errmsg)
    local s = assert(socket_pool[fd])

    if readable and s.readable and s.wait_read then
        s.wait_read(s)
    end
    if writeable and s.writeable and s.wait_write then
        s.wait_write(s, errmsg)
    end
    if errmsg and s.wait_err then
        s.wait_err(s, errmsg)
    end
end

function _M.start_client(logic)
    assert(aefd == nil)
    aefd = ae.create()
    timer.init_timer()
    ae.register({
        update_time = timer.update_cache_time,
        ev_handler = ev_base_handler,
    })

    coroutine.wrap(logic)()

    while true do
        ae.poll(aefd, timer.expire_timer(), 64)
    end
end

function _M.start(endpoint, accept_func)
    aefd = ae.create()
    timer.init_timer()
    ae.register({
        update_time = timer.update_cache_time,
        ev_handler = ev_base_handler,
    })

    local host, port = endpoint:match("([^:]+):(.+)$")

    print("listen:", host, port)

    port = tonumber(port)

    local listenfd = net.listen(host, port, 1024)

    local co = coroutine.create(function ()
        while true do
            local clientfd, ip, addr = net.accept(listenfd)
            if clientfd == 0 then
                coroutine.yield()
            else
                local _, err = xpcall(accept_func, debug.traceback, clientfd, ip, addr)
                if err then
                    print("accept error:", err)
                    close(clientfd)
                end
            end
        end
    end)

    socket_pool[listenfd] = {
        fd = listenfd,
        co = co,
        readable = true,
        wait_read = function (self)
            coroutine.resume(self.co)
        end,
    }
    ae.add_read(aefd, listenfd)

    while true do
        ae.poll(aefd, timer.expire_timer(), 64)
    end
end

local function create_client(clientfd, ip, port, logic)
    local co = coroutine.create(function ()
        local ok, err = xpcall(logic, debug.traceback)
        if not ok then
            print("client logic error:", err)
            close(fd)
        end
    end)

    local client = {
        co = co,
        fd = clientfd,
        host = ip,
        port = port,
        rbuffer = buffer.new(),
        wbuffer = buffer.new(),
    }

    socket_pool[clientfd] = client

    coroutine.resume(co)
    return client
end

_M.create_client = create_client

function _M.read(fd, len)
    local s = assert(socket_pool[fd])

    if not s.readable then
        s.readable = true
        ae.add_read(aefd, fd)
    end

    local data = s.rbuffer:read(len)
    if data then
        return data
    end

    s.wait_read = function (self)
        local n, err = self.rbuffer:read(self.fd, 1024)

        local runfd = assert(co_runfd(self.co))
        if runfd ~= self.fd then
            return
        end
        
        if not n then
            return coroutine.resume(self.co, n, err)
        end
        local buf = self.rbuffer:readn(len)
        if #buf >= len then
            coroutine.resume(self.co, buf)
        end
    end
    
    co_attach(fd)
    local buf, err = coroutine.yield()
    co_detach(fd)

    return buf, err
end

function _M.readline(fd, sep)
    sep = sep or "\r\n"
    local s = assert(socket_pool[fd])
    
    if not s.readable then
        s.readable = true
        ae.add_read(aefd, fd)
    end

    local data = s.rbuffer:readline(sep)
    if data then
        return data
    end

    s.wait_read = function (self)        
        local n, err = s.rbuffer:read(self.fd, 1024)

        local runfd = assert(co_runfd(self.co))
        if runfd ~= self.fd then
            return
        end

        if not n then
            return coroutine.resume(self.co, nil, err)
        end
        local buf = s.rbuffer:readline(sep)
        if buf ~= nil then
            coroutine.resume(self.co, buf)
        end
    end
    
    co_attach(fd)
    local buf, err = coroutine.yield()
    co_detach(fd)

    return buf, err
end

function _M.write(fd, data)
    local s = assert(socket_pool[fd])
    local ok = s.wbuffer:write(fd, data)
    if not ok then
        if not s.writeable then
            s.writeable = true
            ae.enable(aefd, fd, true, true)
        end
        s.wait_write = function (self, errmsg)
            local ok = self.wbuffer:flush(self.fd)

            local runfd = assert(co_runfd(self.co))
            if runfd ~= self.fd then
                return
            end

            if ok then
                self.writeable = false
                ae.enable(aefd, fd, true, false)
                coroutine.resume(self.co, true)
            end
        end

        co_attach(fd)
        ok, _ = coroutine.yield()
        co_detach(fd)
    end
    return ok
end

local function single_connect(ip, port)
    local running = coroutine.running()

    local fd = net.connect(ip, port)
    
    local client = {
        fd = fd,
        co = running,
        writeable = true,
        wait_write = function (self, errmsg)
            local runfd = assert(co_runfd(self.co))
            if runfd ~= self.fd then
                return
            end

            if errmsg then
                return coroutine.resume(self.co, nil, errmsg)
            end
            self.rbuffer = buffer.new()
            self.wbuffer = buffer.new()
            self.writeable = false
            self.readable = true
            self.wait_write = nil
            ae.enable(aefd, runfd, true, false)
            coroutine.resume(self.co, true)
        end,
    }
    ae.add_write(aefd, fd)

    socket_pool[fd] = client

    co_attach(fd)
    local ok, err = coroutine.yield()
    co_detach(fd)
    if not ok then
        close(fd)
        return nil, err
    end
    return fd
end

local function create_pool(opts, url)
    local default_pool = {
        cache = {},
        free = {},
        wait = {},
        wait_timer = {},
        wait_timeout = 500, -- 5秒
        connections = 0,
        backlog = opts.backlog or -1,
        pool_size = opts.pool_size or 30,
        pool_name = url,
    }

    local pool = setmetatable(default_pool, {
        __gc = function (tab)
            for fd,_ in pairs(tab.free) do
                close(fd)
            end
            for fd,_ in pairs(tab.cache) do
                close(fd)
            end
        end
    })
    connection_pool[pool.pool_name] = pool
    return pool
end

local function get_alive_conn(pool)
    if not tab_isempty(pool.cache) then
        local min, conn = 65535, nil
        for fd, c in pairs(pool.cache) do
            if fd < min then
                min = fd
                conn = c
            end
        end
        pool.free[min] = conn
        pool.cache[min] = nil
        conn.status = "free"
        conn.wait_err = nil
        pool.connections = pool.connections + 1
        conn.co = coroutine.running()
        return conn
    end
end

function _M.connect(ip, port, opts)
    if not opts then
        return single_connect(ip, port)
    end
    local url = ip .. ":" .. port

    local pool = connection_pool[url]
    if not pool then
        pool = create_pool(opts, url)
    else
        local conn = get_alive_conn(pool)
        if conn then
            return conn.fd
        end
    end

    pool.connections = pool.connections + 1
    if pool.backlog >= 0 then
        if pool.connections > pool.pool_size + pool.backlog then
            pool.connections = pool.connections - 1
            return nil, "too many connect operations"
        end
        local running = coroutine.running()
        if pool.connections > pool.pool_size then
            pool.wait[#pool.wait + 1] = running
            pool.wait_timer[running] = timer.add_timer(pool.wait_timeout, function ()
                local co = table.remove(pool.wait, 1)
                coroutine.resume(co, nil, "connect timeout")
            end)

            co_attach(-1) -- 当前还没有连接, 避免事件唤醒当前让出的协程，只能由定时器唤醒
            local fd, err = coroutine.yield()
            co_detach(-1)
            if not fd then
                pool.connections = pool.connections - 1
                return nil, err
            end
            -- 复用 free 里面的连接
            local conn = socket_pool[fd]
            conn.co = running
            return fd
        end
    end

    local fd, err = single_connect(ip, port)
    if not fd then
        pool.connections = pool.connections - 1
        return nil, err
    end

    local conn = socket_pool[fd]
    conn.pool_name = pool.pool_name

    if pool.connections - #pool.wait <= pool.pool_size then
        conn.status = "free"
        pool.free[fd] = conn
    end

    return fd
end

function _M.setkeepalive(fd)
    local conn = assert(socket_pool[fd])
    if not conn.pool_name then
        close(fd)
        return
    end

    local pool = assert(connection_pool[conn.pool_name])

    if conn.status ~= "free" then
        pool.connections = pool.connections - 1
        close(fd)
        return
    end

    if #pool.wait > 0 then
        local co = table.remove(pool.wait, 1)
        local task = pool.wait_timer[co]
        timer.del_timer(task)
        coroutine.resume(co, fd)
        return
    end

    pool.connections = pool.connections - 1
    conn.status = "cache"
    conn.co = nil
    conn.wait_err = function(self, errmsg)
        pool.connections = pool.connections - 1
        close(self.fd)
    end
    pool.cache[fd] = conn
    pool.free[fd] = nil
end

function _M.fork(func)
    timer.add_timer(0, func)
end

function _M.sleep(csec)
    local running = coroutine.running()
    timer.add_timer(csec, function ()
        coroutine.resume(running)
    end)

    co_attach(-2)
    coroutine.yield()
    co_detach(-2)
end

return _M

