
package.cpath = "./luaclib/?.so;"
package.path = "./lualib/?.lua;"

local zv = require "zv"

local function test_loop()
    local strs = {}
    for i=0,9 do
        local tab = {}
        for j=1, 1026 do
            tab[#tab+1] = i
        end
        strs[#strs+1] = table.concat(tab)
    end
    local i = 0
    while true do
        local clientfd, err = zv.connect("127.0.0.1", 8989, {backlog = 1, pool_size = 2})
        if not err then
            zv.write(clientfd, strs[i%10+1] .. "\r\n")
            local buf, _err = zv.readline(clientfd, "\r\n")
            if _err then
                print("err:", clientfd, _err)
                zv.close(clientfd)
                return
            else
                print("recv", clientfd, buf, #buf)
                zv.setkeepalive(clientfd)
            end
        else
            print("err:", clientfd, err)
            zv.close(clientfd)
            return
        end
        i = i+1
        zv.sleep(i<5 and 10 or 100)
    end
end

zv.start_client(function ()
    zv.fork(test_loop)
    zv.fork(test_loop)
    zv.fork(test_loop)
    zv.fork(test_loop)
end)

