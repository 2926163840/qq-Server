#include <iostream>
#include <string>
#include <vector>
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

// ---------- 协议收发 ----------
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
// 提取字段后面的整数
static long long extract_num(const std::string &s, const std::string &key)
{
    size_t p = s.find("\"" + key + "\"");
    if (p == std::string::npos)
        return -1;
    p = s.find(':', p);
    if (p == std::string::npos)
        return -1;
    p++;
    while (p < s.size() && (s[p] == ' ' || s[p] == '\t'))
        p++;
    return atoll(s.c_str() + p);
}

// 登录，返回 socket（失败 -1）
static int login_conn(const std::string &account, const std::string &password)
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
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::string resp;
    std::string req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                      "\",\"userpassword\":\"" + password + "\"}";
    if (!send_json(sock, req) || !recv_json(sock, resp) ||
        resp.find("login_success") == std::string::npos)
    {
        close(sock);
        return -1;
    }
    return sock;
}
// 添加一对好友：A 加 B，B 同意，返回 convid
static bool add_friend_pair(const std::string &accA, const std::string &accB,
                            const std::string &password, long long &convid)
{
    int sockA = login_conn(accA, password);
    int sockB = login_conn(accB, password);
    if (sockA < 0 || sockB < 0)
    {
        if (sockA >= 0)
            close(sockA);
        if (sockB >= 0)
            close(sockB);
        return false;
    }

    std::string resp;
    // A 搜索 B 拿 userid
    if (!send_json(sockA, "{\"cmd\":\"search\",\"accountinfo\":\"" + accB + "\",\"lastaccount\":\"\"}") ||
        !recv_json(sockA, resp))
    {
        close(sockA);
        close(sockB);
        return false;
    }
    long long friendid = extract_num(resp, "friendid");
    if (friendid < 0)
    {
        close(sockA);
        close(sockB);
        return false;
    }

    // A 发添加好友请求
    static thread_local std::mt19937 rng(std::random_device{}());
    char uuid[40];
    snprintf(uuid, sizeof(uuid), "%lx%lx", (unsigned long)rng(), (unsigned long)rng());
    std::string addreq = "{\"cmd\":\"addfriend\",\"recvid\":" + std::to_string(friendid) +
                         ",\"verificationinfo\":\"hi\",\"remark\":\"无\",\"ingroup\":\"我的好友\",\"uuid\":\"" + uuid + "\"}";
    if (!send_json(sockA, addreq) || !recv_json(sockA, resp))
    {
        close(sockA);
        close(sockB);
        return false;
    }

    // B 收 addfriend_request，拿 bindid
    if (!recv_json(sockB, resp))
    {
        close(sockA);
        close(sockB);
        return false;
    }
    long long bindid = extract_num(resp, "bindid");
    if (bindid < 0)
    {
        close(sockA);
        close(sockB);
        return false;
    }

    // B 同意
    std::string agree = "{\"cmd\":\"handlefriendrequest\",\"result\":\"同意\",\"reuqestkey\":" + std::to_string(bindid) + "}";
    if (!send_json(sockB, agree) || !recv_json(sockB, resp))
    {
        close(sockA);
        close(sockB);
        return false;
    }

    // A 收 addfriend_agree，拿 convid
    if (!recv_json(sockA, resp))
    {
        close(sockA);
        close(sockB);
        return false;
    }
    convid = extract_num(resp, "convid");
    close(sockA);
    close(sockB);
    return convid > 0;
}

int main(int argc, char **argv)
{
    int start = 42;
    int pairs = 20;
    std::string password = "12345678";
    for (int i = 1; i < argc; i++)
    {
        std::string a = argv[i];
        if (a == "--start" && i + 1 < argc)
            start = atoi(argv[++i]);
        else if (a == "--pairs" && i + 1 < argc)
            pairs = atoi(argv[++i]);
        else if (a == "--password" && i + 1 < argc)
            password = argv[++i];
    }

    // 现成账号：userid 从 start 开始，useraccount = userid + 1000000000
    std::vector<std::string> accounts;
    for (int i = 0; i < pairs * 2; i++)
    {
        char acc[16];
        snprintf(acc, sizeof(acc), "%010lld", (long long)(start + i) + 1000000000);
        accounts.push_back(acc);
    }

    // 两两添加好友
    std::vector<long long> convids;
    printf("添加好友（%d 对，从 userid %d 开始）...\n", pairs, start);
    for (int i = 0; i < pairs; i++)
    {
        long long convid = -1;
        if (add_friend_pair(accounts[2 * i], accounts[2 * i + 1], password, convid))
        {
            convids.push_back(convid);
            printf("第 %d 对: %s <-> %s  convid=%lld\n", i + 1,
                   accounts[2 * i].c_str(), accounts[2 * i + 1].c_str(), convid);
        }
        else
        {
            printf("第 %d 对添加失败\n", i + 1);
        }
    }

    // 输出压测参数（每对取一个发送方）
    printf("\n===== 供压测复制 =====\n");
    printf("--accounts \"");
    for (size_t i = 0; i < accounts.size(); i += 2)
    {
        if (i)
            printf(",");
        printf("%s", accounts[i].c_str());
    }
    printf("\"\n--convids \"");
    for (size_t i = 0; i < convids.size(); i++)
    {
        if (i)
            printf(",");
        printf("%lld", convids[i]);
    }
    printf("\"\n======================\n");
    return 0;
}