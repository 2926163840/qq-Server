#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <json/json.h>
#include <iostream>

// sendJson / recvJson 同前面

int main()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9999);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    connect(sock, (sockaddr *)&addr, sizeof(addr));

    // 登录（证明它在线上）
    // ... 发 login，收 login_success ...

    std::cout << "已登录，开始装死（不发心跳）...\n";
    pause(); // ★ 挂起，连接还在但不发任何数据，模拟异常掉线

    return 0;
}