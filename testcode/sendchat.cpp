// sendchat.cpp —— 双方在线聊天吞吐压测
// 编译：g++ -O2 -std=c++17 -pthread sendchat.cpp -o sendchat
// 运行：./sendchat --accountsA "..." --accountsB "..." --convids "..." --threads N --count M --password 12345678
// 说明：A 发消息，B 在线实时收，测「发送+转发+接收」全链路吞吐

// ./sendchat \
//   --accountsA "1000000042,1000000044,1000000046,1000000048,1000000050,1000000052,1000000054,1000000056,1000000058,1000000060,1000000062,1000000064,1000000066,1000000068,1000000070,1000000072,1000000074,1000000076,1000000078,1000000080,1000000082,1000000084,1000000086,1000000088,1000000090,1000000092,1000000094,1000000096,1000000098,1000000100,1000000102,1000000104,1000000106,1000000108,1000000110,1000000112,1000000114" \
//   --accountsB "1000000043,1000000045,1000000047,1000000049,1000000051,1000000053,1000000055,1000000057,1000000059,1000000061,1000000063,1000000065,1000000067,1000000069,1000000071,1000000073,1000000075,1000000077,1000000079,1000000081,1000000083,1000000085,1000000087,1000000089,1000000091,1000000093,1000000095,1000000097,1000000099,1000000101,1000000103,1000000105,1000000107,1000000109,1000000111,1000000113,1000000115" \
//   --convids "19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55" \
//   --threads 37 --count 500 --password 12345678

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdio>
#include <random>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/time.h>
#include <mutex>

#define HOST "127.0.0.1"
#define PORT 9999

// 全局发送锁：串行化所有 send，杜绝心跳线程和主线程的报头/正文交错
static std::mutex g_send_mtx;

static bool send_json(int sock, const std::string &json)
{
    uint32_t len = (uint32_t)json.size();
    uint32_t big = htonl(len);
    std::string full;
    full.append((char *)&big, 4);
    full.append(json);
    return send(sock, full.data(), full.size(), 0) == (int)full.size();
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
// 数 "uuid" 次数 = 包里的消息数
static size_t count_msg(const std::string &s)
{
    size_t pos = 0, cnt = 0;
    while ((pos = s.find("\"uuid\"", pos)) != std::string::npos)
    {
        cnt++;
        pos += 6;
    }
    return cnt;
}
static std::vector<std::string> parse_list(const std::string &s)
{
    std::vector<std::string> out;
    std::string cur;
    for (char c : s)
    {
        if (c == ',')
        {
            if (!cur.empty())
            {
                out.push_back(cur);
                cur.clear();
            }
        }
        else
            cur += c;
    }
    if (!cur.empty())
        out.push_back(cur);
    return out;
}
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
    tv.tv_sec = 60;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::string resp;
    std::string req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                      "\",\"userpassword\":\"" + password + "\"}";
    if (!send_json(sock, req) || !recv_json(sock, resp) || resp.find("login_success") == std::string::npos)
    {
        close(sock);
        return -1;
    }
    return sock;
}
// 登录 + user_loginsuccess（建立会话关联）
static int login_full(const std::string &account, const std::string &password)
{
    int sock = login_conn(account, password);
    if (sock < 0)
        return -1;
    std::string resp;
    if (!send_json(sock, "{\"cmd\":\"user_loginsuccess\"}") || !recv_json(sock, resp))
    {
        close(sock);
        return -1;
    }
    return sock;
}
struct Result
{
    long ok = 0, fail = 0;
    std::vector<double> lat;
};

static void worker_sendchat(const std::string &accA, const std::string &accB,
                            long long convid, const std::string &password, int count, Result &r)
{
    int sockA = login_full(accA, password);
    int sockB = login_full(accB, password);
    if (sockA < 0 || sockB < 0)
    {
        r.fail += count;
        if (sockA >= 0)
            close(sockA);
        if (sockB >= 0)
            close(sockB);
        return;
    }

    static thread_local std::mt19937 rng(std::random_device{}());
    // A 连发 count 条，期间定期给 B 发心跳保活
    for (int i = 0; i < count; i++)
    {
        char uuid[40];
        snprintf(uuid, sizeof(uuid), "%lx%lx", (unsigned long)rng(), (unsigned long)rng());
        std::string msg = "{\"cmd\":\"sendmessage\",\"convid\":" + std::to_string(convid) +
                          ",\"content\":\"chat " + std::to_string(i) + "模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小，模拟正常消息数据大小" + "\",\"uuid\":\"" + uuid + "\"}";
        if (!send_json(sockA, msg))
        {
            r.fail += (count - i);
            break;
        }

        // 每发 1 万条，给 B 发一次心跳（A 靠消息保活，B 靠心跳保活）
        if (i % 10000 == 0)
            send_json(sockB, "{\"cmd\":\"heartbeat\"}");
    }

    // 单线程收包：recv 超时（60秒）就发心跳保活
    std::string resp;
    int a_recv = 0, b_recv = 0;
    int last_progress = 0;
    while (a_recv < count || b_recv < count)
    {
        bool got = false;
        if (a_recv < count && recv_json(sockA, resp))
        {
            if (resp.find("pong") == std::string::npos)
                a_recv += count_msg(resp);
            got = true;
        }
        if (b_recv < count && recv_json(sockB, resp))
        {
            if (resp.find("pong") == std::string::npos)
                b_recv += count_msg(resp);
            got = true;
        }

        if (!got)
        {
            send_json(sockA, "{\"cmd\":\"heartbeat\"}");
            send_json(sockB, "{\"cmd\":\"heartbeat\"}");
            continue;
        }

        int cur = std::min(a_recv, b_recv);
        if (cur / 100000 > last_progress / 100000)
        {
            fprintf(stderr, "进度: %d / %d\n", cur, count);
            last_progress = cur;
        }
    }

    r.ok += std::min(a_recv, b_recv);
    r.fail += (count - std::min(a_recv, b_recv));
    close(sockA);
    close(sockB);
}

static void report(const std::vector<Result> &rs, double elapsed)
{
    long ok = 0, fail = 0;
    for (auto &x : rs)
    {
        ok += x.ok;
        fail += x.fail;
    }
    long total = ok + fail;
    printf("总请求:%ld 成功:%ld 失败:%ld 成功率:%.2f%%\n", total, ok, fail, total ? ok * 100.0 / total : 0);
    printf("总耗时:%.2fs 吞吐(QPS):%.1f\n", elapsed, total / elapsed);
}

int main(int argc, char **argv)
{
    int threads = 13, count = 500;
    std::string accA_str, accB_str, convids_str, password = "12345678";
    for (int i = 1; i < argc; i++)
    {
        std::string a = argv[i];
        if (a == "--accountsA" && i + 1 < argc)
            accA_str = argv[++i];
        else if (a == "--accountsB" && i + 1 < argc)
            accB_str = argv[++i];
        else if (a == "--convids" && i + 1 < argc)
            convids_str = argv[++i];
        else if (a == "--threads" && i + 1 < argc)
            threads = atoi(argv[++i]);
        else if (a == "--count" && i + 1 < argc)
            count = atoi(argv[++i]);
        else if (a == "--password" && i + 1 < argc)
            password = argv[++i];
    }
    auto accA = parse_list(accA_str);
    auto accB = parse_list(accB_str);
    auto convs = parse_list(convids_str);
    size_t n = std::min({accA.size(), accB.size(), convs.size()});
    if (n == 0)
    {
        fprintf(stderr, "参数不全\n");
        return 1;
    }

    std::vector<Result> rs(threads);
    std::vector<std::thread> ts;
    auto start = std::chrono::steady_clock::now();
    fprintf(stderr, "压测开始: %d 线程 × %d 条\n", threads, count);
    for (int i = 0; i < threads; i++)
    {
        int idx = i % n;
        long long convid = atoll(convs[idx].c_str());
        ts.emplace_back(worker_sendchat, accA[idx], accB[idx], convid, password, count, std::ref(rs[i]));
    }
    for (auto &t : ts)
        t.join();
    fprintf(stderr, "压测结束，开始统计\n"); // ← 结束后
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    report(rs, elapsed);
    return 0;
}