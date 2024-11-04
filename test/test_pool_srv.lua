
package.cpath = "./luaclib/?.so;"
package.path = "./lualib/?.lua;"

local zv = require "zv"

local function client_loop(fd)
    while true do
        local buf, err = zv.readline(fd, "\r\n")
        if err then
            print("client_loop close:", fd, err)
            zv.close(fd)
            return
        end
        print("recv from client:", fd, buf)
        zv.write(fd, buf .. "\r\n")
    end
end

zv.start("0.0.0.0:8989", function (fd, ip, port)
    print("accept a connection:", fd, ip, port)
    zv.create_client(fd, ip, port, function()
        client_loop(fd)
    end)
end)


