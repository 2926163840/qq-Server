// stick_packet.cpp —— 粘包/半包测试
// 编译：g++ -O2 -std=c++17 -pthread stick_packet.cpp -o stick_packet
// 粘包：一个连接连发 N 个包，验证服务器正确拆出 N 个完整包
// 半包：一个包分两次 send，验证服务器等完整包再处理

// 粘包 / 半包测试
//./stick_packet 20        # 参数是连发的包数，默认 20

#include <iostream>
#include <string>
#include <cstring>
#include <random>
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
static std::string rand_name()
{
    static std::mt19937 rng(std::random_device{}());
    static const char *cs = "abcdefghijklmnopqrstuvwxyz";
    std::string s = "sp_";
    for (int i = 0; i < 6; i++)
        s += cs[rng() % 26];
    return s;
}

// ===== 测试1：粘包 =====
static void test_stick(int n)
{
    int sock = connect_server();
    if (sock < 0)
    {
        printf("[粘包] 连接失败\n");
        return;
    }

    // 连发 n 个 register（不等待回复，全部塞进缓冲区）
    for (int i = 0; i < n; i++)
    {
        std::string req = "{\"cmd\":\"register\",\"username\":\"" + rand_name() +
                          "\",\"userpassword\":\"12345678\"}";
        if (!send_json(sock, req))
        {
            printf("[粘包] 发送失败\n");
            close(sock);
            return;
        }
    }

    // 收 n 个 register_reply，每个都该有 useraccount
    int ok = 0;
    for (int i = 0; i < n; i++)
    {
        std::string resp;
        if (!recv_json(sock, resp))
        {
            printf("[粘包] 第%d个回复接收失败\n", i + 1);
            break;
        }
        if (resp.find("useraccount") != std::string::npos)
            ok++;
    }
    close(sock);
    printf("[粘包] 连发%d个包，正确拆出%d个回复 %s\n", n, ok, ok == n ? "✓" : "✗");
}

// ===== 测试2：半包 =====
static void test_half()
{
    int sock = connect_server();
    if (sock < 0)
    {
        printf("[半包] 连接失败\n");
        return;
    }

    std::string req = "{\"cmd\":\"register\",\"username\":\"" + rand_name() +
                      "\",\"userpassword\":\"12345678\"}";
    // 构造完整包：4字节报头 + 正文
    uint32_t len = (uint32_t)req.size();
    uint32_t big = htonl(len);
    std::string full;
    full.append((char *)&big, 4);
    full.append(req);

    // 分两次发：先发前一半（报头 + 正文前一部分），再发后一半
    size_t mid = full.size() / 2;
    send(sock, full.data(), mid, 0);
    printf("[半包] 已发前 %zu 字节，等 200ms...\n", mid);
    usleep(200000); // 模拟网络延迟
    send(sock, full.data() + mid, full.size() - mid, 0);

    // 服务器应该等完整包再回，这里收 register_reply
    std::string resp;
    bool ok = recv_json(sock, resp);
    close(sock);
    printf("[半包] %s %s\n", ok && resp.find("useraccount") != std::string::npos ? "✓" : "✗",
           ok && resp.find("useraccount") != std::string::npos ? "服务器正确等完整包" : "失败");
}

int main(int argc, char **argv)
{
    int n = 20;
    if (argc > 1)
        n = atoi(argv[1]);
    printf("===== 粘包测试（连发 %d 个 register）=====\n", n);
    test_stick(n);
    printf("\n===== 半包测试（一个包分两次发）=====\n");
    test_half();
    return 0;
}