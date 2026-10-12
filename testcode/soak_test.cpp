// soak_test.cpp —— 长时间稳定性压测（混合负载：持续发消息 + 登录/暴力断连）
// 编译：g++ -O2 -std=c++17 -pthread soak_test.cpp -o soak_test
// 运行：./soak_test --accountsA "..." --accountsB "..." --convids "..." \
//         --threads 37 --churn 2 --duration 3600 --password 12345678
//
// 目的：让服务器在持续压力下跑几小时，观察内存(RSS)/文件描述符/连接数是否缓慢泄漏。
// 说明：
//   - worker_chat : 37 对双方在线，每对循环「发 100 条 → 收 100 条回执」，连接断自动重连
//   - worker_churn: 反复 login → 直接 close（不优雅退出），测 onlineuser 增删是否平衡
//   - monitor     : 每 10 秒打印累计统计，方便观察 QPS 是否稳定、失败是否累积
// 判据：跑几小时后，服务器 RSS、fd 数、连接数应稳定在一个平台（波动不单调上涨）。

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

#define HOST "127.0.0.1"
#define PORT 9999

// 全局：停止标志（--duration 到点或 Ctrl+C 时置位）
static std::atomic<bool> g_stop{false};
// 全局累计计数（仅统计用，不参与业务逻辑，无需精确）
static std::atomic<long> g_sent{0}, g_recv{0}, g_fail{0};
static std::atomic<long> g_login{0}, g_logout{0};

// ==================== 协议收发（和 sendchat.cpp 一致） ====================
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
// 数 "uuid" 次数 = 这个包里的消息条数
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
            if (!cur.empty()) { out.push_back(cur); cur.clear(); }
        }
        else
            cur += c;
    }
    if (!cur.empty())
        out.push_back(cur);
    return out;
}
// 建连 + login，返回 sock（失败返回 -1）
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
// login + user_loginsuccess（建立会话关联，发消息前必须）
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

// ==================== worker_chat：持续发消息（带重连） ====================
// 每批发 BATCH 条，收满 BATCH 条回执再发下一批；连接断/超时就重连继续。
static void worker_chat(const std::string &accA, const std::string &accB,
                        long long convid, const std::string &password)
{
    const int BATCH = 100;
    static thread_local std::mt19937 rng(std::random_device{}());

    while (!g_stop)
    {
        // 重连：断线后 login 重新拉起
        int sockA = login_full(accA, password);
        int sockB = login_full(accB, password);
        if (sockA < 0 || sockB < 0)
        {
            g_fail++;
            if (sockA >= 0) close(sockA);
            if (sockB >= 0) close(sockB);
            std::this_thread::sleep_for(std::chrono::seconds(1)); // 登录失败退避
            continue;
        }

        // 连接成功后，循环发消息
        bool alive = true;
        while (alive && !g_stop)
        {
            // 1) A 连发 BATCH 条
            for (int i = 0; i < BATCH; i++)
            {
                char uuid[40];
                snprintf(uuid, sizeof(uuid), "%lx%lx", (unsigned long)rng(), (unsigned long)rng());
                std::string msg = "{\"cmd\":\"sendmessage\",\"convid\":" + std::to_string(convid) +
                                  ",\"content\":\"soak " + std::to_string(i) +
                                  "\",\"uuid\":\"" + uuid + "\"}";
                if (!send_json(sockA, msg))
                {
                    alive = false;
                    break;
                }
                g_sent++;
            }
            if (!alive)
                break;

            // 2) 收满 BATCH 条回执（A 收回执 + B 收消息通知）
            int got = 0;
            std::string resp;
            while (got < BATCH && !g_stop)
            {
                bool any = false;
                if (got < BATCH && recv_json(sockA, resp))
                {
                    got += (int)count_msg(resp);
                    g_recv += (long)count_msg(resp);
                    any = true;
                }
                if (got < BATCH && recv_json(sockB, resp))
                {
                    got += (int)count_msg(resp);
                    g_recv += (long)count_msg(resp);
                    any = true;
                }
                if (!any) // 超时/断连，跳出重连
                {
                    alive = false;
                    break;
                }
            }
            if (got < BATCH && !g_stop)
                alive = false;
        }
        close(sockA);
        close(sockB);
    }
}

// ==================== worker_churn：登录后暴力断连 ====================
// 反复 login → 直接 close（不优雅退出），检验 eventCallBack 是否正确清理 onlineuser。
static void worker_churn(const std::vector<std::string> &accounts, const std::string &password)
{
    size_t i = 0;
    while (!g_stop)
    {
        const std::string &acc = accounts[i % accounts.size()];
        i++;
        int sock = login_conn(acc, password);
        if (sock >= 0)
        {
            g_login++;
            // 短暂保持后直接断（模拟客户端崩溃/断电，不发退出命令）
            std::this_thread::sleep_for(std::chrono::milliseconds(50 + (rand() % 200)));
            close(sock);
            g_logout++;
        }
        else
        {
            // 登录失败（可能账号残留"在线"，正是要观察的泄漏点）
            g_fail++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

// ==================== monitor：每 10 秒打印统计 ====================
static void monitor(std::chrono::steady_clock::time_point start)
{
    long last_sent = 0, last_recv = 0, last_fail = 0;
    while (!g_stop)
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        if (g_stop)
            break;
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start).count();
        long sent = g_sent.load(), recv = g_recv.load(), fail = g_fail.load();
        double chat_qps = (sent - last_sent) / 10.0;
        printf("[%6.0fs] sent=%ld recv=%ld fail=%ld login=%ld logout=%ld | chatQPS=%.0f  fail10s=%ld\n",
               elapsed, sent, recv, fail,
               g_login.load(), g_logout.load(),
               chat_qps, fail - last_fail);
        fflush(stdout);
        last_sent = sent;
        last_recv = recv;
        last_fail = fail;
    }
}

int main(int argc, char **argv)
{
    int threads = 37, churn = 2, duration = 0;
    std::string accA_str, accB_str, convids_str, password = "12345678";
    for (int i = 1; i < argc; i++)
    {
        std::string a = argv[i];
        if (a == "--accountsA" && i + 1 < argc) accA_str = argv[++i];
        else if (a == "--accountsB" && i + 1 < argc) accB_str = argv[++i];
        else if (a == "--convids" && i + 1 < argc) convids_str = argv[++i];
        else if (a == "--threads" && i + 1 < argc) threads = atoi(argv[++i]);
        else if (a == "--churn" && i + 1 < argc) churn = atoi(argv[++i]);
        else if (a == "--duration" && i + 1 < argc) duration = atoi(argv[++i]);
        else if (a == "--password" && i + 1 < argc) password = argv[++i];
    }
    auto accA = parse_list(accA_str);
    auto accB = parse_list(accB_str);
    auto convs = parse_list(convids_str);
    size_t n = std::min({accA.size(), accB.size(), convs.size()});
    if (n == 0)
    {
        fprintf(stderr, "需要 --accountsA/--accountsB/--convids\n");
        return 1;
    }

    // churn 账号：默认用 userid 10~40 偶数（16 个，和 chat 的 42~115 不冲突）
    std::vector<std::string> churn_accs;
    for (long long uid = 10; uid <= 40; uid += 2)
        churn_accs.push_back(std::to_string(1000000000 + uid));

    printf("长稳压测启动: chat=%d对 churn=%d线程 duration=%ds\n", threads, churn, duration);
    printf("判据: 观察服务器 RSS/fd/连接数是否稳定, chatQPS 是否平稳, fail 是否累积\n");
    printf("----------------------------------------------------------\n");
    fflush(stdout);

    auto start = std::chrono::steady_clock::now();

    // 统计线程
    std::thread mon(monitor, start);

    // chat 线程（双方在线持续发消息）
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; i++)
    {
        int idx = i % n;
        long long convid = atoll(convs[idx].c_str());
        ts.emplace_back(worker_chat, accA[idx], accB[idx], convid, password);
    }
    // churn 线程（登录/暴力断连）
    for (int i = 0; i < churn; i++)
        ts.emplace_back(worker_churn, std::cref(churn_accs), password);

    // 时长控制：--duration 0 = 无限跑（Ctrl+C 退出）
    if (duration > 0)
    {
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        g_stop = true;
    }
    else
    {
        // 无限跑：等 Ctrl+C（SIGINT 默认终止进程，这里不处理信号，直接跑）
        while (!g_stop)
            std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 收尾
    for (auto &t : ts)
        t.join();
    g_stop = true;
    mon.join();

    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    printf("----------------------------------------------------------\n");
    printf("长稳压测结束: 总耗时=%.0fs sent=%ld recv=%ld fail=%ld login=%ld logout=%ld\n",
           elapsed, g_sent.load(), g_recv.load(), g_fail.load(), g_login.load(), g_logout.load());
    return 0;
}
