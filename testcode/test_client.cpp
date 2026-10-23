
// 心跳保活
//./test_client

#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <json/json.h>
#include <iostream>

// 发送 JSON：4字节长度头(网络字节序) + 数据
void sendJson(int sock, const Json::Value &v)
{
    Json::FastWriter writer;
    std::string data = writer.write(v);
    uint32_t len = data.size();
    uint32_t netlen = htonl(len);
    send(sock, &netlen, 4, 0);
    send(sock, data.data(), data.size(), 0);
}

// 接收 JSON
Json::Value recvJson(int sock)
{
    uint32_t netlen;
    ssize_t n = recv(sock, &netlen, 4, 0);
    if (n <= 0)
        return Json::Value();
    uint32_t len = ntohl(netlen);

    std::string data(len, '\0');
    size_t got = 0;
    while (got < len)
    {
        ssize_t r = recv(sock, &data[got], len - got, 0);
        if (r <= 0)
            return Json::Value();
        got += r;
    }
    Json::Value v;
    Json::Reader reader;
    reader.parse(data, v);
    return v;
}

int main()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9999);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    connect(sock, (sockaddr *)&addr, sizeof(addr));

    // 发心跳，预期收 pong
    Json::Value ping;
    ping["cmd"] = "heartbeat";
    sendJson(sock, ping);

    Json::Value resp = recvJson(sock);
    if (resp.isNull() || resp["cmd"].asString() != "pong")
    {
        std::cout << "测试失败：没收到 pong\n";
        close(sock);
        return 1;
    }
    std::cout << "心跳保活测试通过：收到 pong\n";

    close(sock);
    return 0;
}