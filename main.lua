
package.cpath = "./luaclib/?.so;"

package.path = "./lualib/?.lua;"

local zv = require "zv"
-- local timer = require "timer"

zv.start("0.0.0.0:8989", function (clientfd, ip, addr)
    print("accept", clientfd, ip, addr)

    -- timer.add_timer(100, function()
    --     print("timer 1s")
    -- end)

    -- timer.add_timer(200, function()
    --     print("timer 2s")
    -- end)
    zv.create_client(clientfd, ip, addr, function()
        while true do
            local buf, err = zv.readline(clientfd, "\r\n")
            if not buf then
                print("readline", err)
                zv.close(clientfd)
                break
            end
            print("recv", buf)

            -- redis
            local fd, err = zv.connect("127.0.0.1", 6379)
            if not fd then
                print(err)
                zv.close(clientfd)
                break
            end
            zv.write(fd, "PING\r\n")
            local data, err = zv.readline(fd, "\r\n")
            zv.close(fd)

            zv.write(clientfd, "from redis: ".. data .. "\r\n")

            zv.write(clientfd, "from server: ".. buf .. "\r\n")
        end
    end)
end)
