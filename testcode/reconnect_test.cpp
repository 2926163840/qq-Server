// reconnect_test.cpp —— 断线重连/连接清理测试
// 编译：g++ -O2 -std=c++17 -pthread reconnect_test.cpp -o reconnect_test
// 测法：N 个账号 login 后直接 close（不优雅退出），等几秒重新 login，
//       看是否还报 login_online（报 = onlineuser 没清理干净）

// 断线重连 / onlineuser 清理
//./reconnect_test --start 1 --count 200 --password 12345678

#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdio>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/time.h>

#define HOST "127.0.0.1"
#define PORT 9999

static bool send_json(int sock, const std::string &json)
{
    uint32_t len = (uint32_t)json.size();
    uint32_t big = htonl(len);
    if (send(sock, &big, 4, 0) != 4)
        return false;
    return send(sock, json.data(), json.size(), 0) == (int)json.size();
}
static bool recv_exact(int sock, char *buf, int n)
{
    int got = 0;
    while (got < n)
    {
        int r = recv(sock, buf + got, n - got, 0);
        if (r <= 0)
            return false;
        got += r;
    }
    return true;
}
static bool recv_json(int sock, std::string &json)
{
    uint32_t big;
    if (!recv_exact(sock, (char *)&big, 4))
        return false;
    uint32_t len = ntohl(big);
    if (len == 0 || len > 65536)
        return false;
    json.resize(len);
    return recv_exact(sock, &json[0], len);
}
static int connect_server()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &addr.sin_addr);
    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) != 0)
    {
        close(sock);
        return -1;
    }
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return sock;
}
// login，返回响应里的 result 字符串（"login_success" 或 "login_online" 等）
static std::string login_once(const std::string &account, const std::string &password)
{
    int sock = connect_server();
    if (sock < 0)
        return "connect_fail";
    std::string resp;
    std::string req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                      "\",\"userpassword\":\"" + password + "\"}";
    send_json(sock, req);
    recv_json(sock, resp);
    close(sock);
    // 提取 result
    size_t p = resp.find("\"result\"");
    if (p == std::string::npos)
        return "no_result";
    p = resp.find('"', resp.find(':', p) + 1);
    size_t e = resp.find('"', p + 1);
    return resp.substr(p + 1, e - p - 1);
}
// 只 login 不优雅退出：直接 close（模拟异常断线）
static void login_then_close(const std::string &account, const std::string &password)
{
    int sock = connect_server();
    if (sock < 0)
        return;
    std::string resp;
    std::string req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                      "\",\"userpassword\":\"" + password + "\"}";
    send_json(sock, req);
    recv_json(sock, resp);
    close(sock); // 直接 close，不优雅退出
}

int main(int argc, char **argv)
{
    int start = 42, count = 74;
    std::string password = "12345678";
    for (int i = 1; i < argc; i++)
    {
        std::string a = argv[i];
        if (a == "--start" && i + 1 < argc)
            start = atoi(argv[++i]);
        else if (a == "--count" && i + 1 < argc)
            count = atoi(argv[++i]);
        else if (a == "--password" && i + 1 < argc)
            password = argv[++i];
    }

    // 账号 = userid + 1000000000
    std::vector<std::string> accounts;
    for (int i = 0; i < count; i++)
    {
        char acc[16];
        snprintf(acc, sizeof(acc), "%010lld", (long long)(start + i) + 1000000000);
        accounts.push_back(acc);
    }

    // 第一轮：login 后直接 close（模拟异常断线）
    printf("第一轮：%d 个账号 login 后直接 close...\n", count);
    for (auto &acc : accounts)
        login_then_close(acc, password);

    // 等 2 秒，让服务器处理断开事件
    printf("等 2 秒让服务器清理 onlineuser...\n");
    sleep(2);

    // 第二轮：重新 login，统计 login_success 比例
    int ok = 0, online = 0, other = 0;
    printf("第二轮：重新 login...\n");
    for (auto &acc : accounts)
    {
        std::string r = login_once(acc, password);
        if (r == "login_success")
            ok++;
        else if (r == "login_online")
            online++;
        else
            other++;
    }

    printf("============ 结果 ============\n");
    printf("login_success: %d / %d\n", ok, count);
    printf("login_online(残留): %d\n", online);
    printf("其他失败: %d\n", other);
    if (online == 0)
        printf("✓ onlineuser 清理干净\n");
    else
        printf("✗ 有 %d 个账号残留，eventCallBack 清理有延迟/遗漏\n", online);
    return 0;
}