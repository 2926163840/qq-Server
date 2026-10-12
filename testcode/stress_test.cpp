// 编译: g++ -O2 -std=c++17 -pthread stress_test.cpp -o stress_test
// 运行: ./stress_test --mode login --threads 50 --count 30
//
// 协议: [4字节小端int=正文长度][JSON正文]

// ./stress_test --mode sendpipeline \
//   --accounts "1000000042,1000000044,1000000046,1000000048,1000000050,1000000052,1000000054,1000000056,1000000058,1000000060,1000000062,1000000064,1000000066,1000000068,1000000070,1000000072,1000000074,1000000076,1000000078,1000000080,1000000082,1000000084,1000000086,1000000088,1000000090,1000000092,1000000094,1000000096,1000000098,1000000100,1000000102,1000000104,1000000106,1000000108,1000000110,1000000112,1000000114" \
//   --convids "19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55" \
//   --threads 37 --count 500 --password 12345678

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <algorithm>
#include <cstdio>
#include <sys/socket.h> // POSIX socket
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h> // close
#include <sys/time.h>

#define HOST "127.0.0.1"
#define PORT 9999
#define PASSWORD "12345678" // 压测账号统一密码(>=8位)

// ==================== 协议收发 ====================
// 发送一个完整包：先发4字节小端长度，再发JSON正文
static bool send_json(int sock, const std::string &json)
{
    int len = (int)json.size();
    uint32_t big = htonl(len); // 主机序 → 大端

    if (send(sock, &big, 4, 0) != 4)
        return false;                              // 发报头
    return send(sock, json.data(), len, 0) == len; // 发正文
}

// 精确接收 n 字节（TCP是流式的，一次recv不一定收满，必须循环收）
static bool recv_exact(int sock, char *buf, int n)
{
    int got = 0;
    while (got < n)
    {
        int r = recv(sock, buf + got, n - got, 0);
        if (r <= 0)
            return false; // r==0 对端关闭；r<0 出错
        got += r;
    }
    return true;
}

// 接收一个完整包：先读4字节长度，再读对应长度的正文
static bool recv_json(int sock, std::string &json)
{
    uint32_t big;
    if (!recv_exact(sock, (char *)&big, 4))
        return false;
    uint32_t len = ntohl(big); // 大端 → 主机序
    if (len <= 0 || len > 65536)
        return false; // 防御非法包大小
    json.resize(len);
    return recv_exact(sock, &json[0], len);
}

// 短连接 RPC：connect → 发请求 → 收响应 → close
// 每次一个完整往返，模拟"大量独立客户端"而非单连接复用
static bool rpc(const std::string &req, std::string &resp)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &addr.sin_addr);
    bool ok = false;
    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) == 0)
        ok = send_json(sock, req) && recv_json(sock, resp);
    close(sock);
    return ok;
}

// ==================== 业务命令 ====================
// 生成随机用户名（避免重名）
static std::string rand_name()
{
    static thread_local std::mt19937 rng(std::random_device{}());
    static const char *cs = "abcdefghijklmnopqrstuvwxyz";
    std::string s = "stress_";
    for (int i = 0; i < 6; i++)
        s += cs[rng() % 26];
    return s;
}

// 注册一次：发 register，从响应里抠出服务器生成的 useraccount
static bool register_once(std::string &account)
{
    std::string req = "{\"cmd\":\"register\",\"username\":\"" + rand_name() +
                      "\",\"userpassword\":\"" PASSWORD "\"}";
    std::string resp;
    if (!rpc(req, resp))
        return false;
    // 简单从 JSON 字符串里找 "useraccount":"xxx" 提取账号（不依赖 jsoncpp）
    auto p = resp.find("useraccount");
    if (p == std::string::npos)
        return false;
    p = resp.find('"', resp.find(':', p) + 1);
    if (p == std::string::npos)
        return false;
    size_t e = resp.find('"', p + 1);
    account = resp.substr(p + 1, e - p - 1);
    return true;
}

// 登录一次：发 login，成功标志是响应里含 "login_success"
static bool login_once(const std::string &account)
{
    std::string req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                      "\",\"userpassword\":\"" PASSWORD "\"}";
    std::string resp;
    if (!rpc(req, resp))
        return false;
    return resp.find("login_success") != std::string::npos;
}

// ==================== 压测 worker ====================
// 每个线程独立收集自己的统计结果，避免共享结构加锁干扰压测
struct Result
{
    long ok = 0, fail = 0;   // 成功/失败计数
    std::vector<double> lat; // 每次请求的延迟(毫秒)，最后合并算分位数
};

// 并发注册：每个线程循环 count 次
static void worker_register(int count, Result &r)
{
    for (int i = 0; i < count; i++)
    {
        auto t0 = std::chrono::steady_clock::now();
        std::string req = "{\"cmd\":\"register\",\"username\":\"" + rand_name() +
                          "\",\"userpassword\":\"" PASSWORD "\"}";
        std::string resp;
        bool ok = rpc(req, resp);
        std::string acc;
        if (ok)
        {
            auto p = resp.find("useraccount");
            ok = (p != std::string::npos);
            if (ok)
            {
                p = resp.find('"', resp.find(':', p) + 1);
                size_t e = resp.find('"', p + 1);
                acc = resp.substr(p + 1, e - p - 1);
            }
        }
        if (ok)
        {
            r.ok++;
        }
        else
        {
            r.fail++;
            static std::atomic<int> printed{0};
            if (printed.fetch_add(1) < 10)
                printf("注册失败 resp=%s  (空=连接失败)\n", resp.c_str());
        }
        double ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
        r.lat.push_back(ms);
    }
}

// 并发登录：线程 idx 用 accounts[idx] 这个账号（每个线程独占一个账号，
// 因为同一账号同时在线会回 login_online 导致误判失败）
static void worker_login(const std::vector<std::string> &accs, int idx,
                         int count, Result &r)
{
    const std::string &acc = accs[idx % accs.size()];
    for (int i = 0; i < count; i++)
    {
        auto t0 = std::chrono::steady_clock::now();
        std::string resp;
        bool ok = rpc("{\"cmd\":\"login\",\"useraccount\":\"" + acc +
                          "\",\"userpassword\":\"" PASSWORD "\"}",
                      resp);
        if (ok && resp.find("login_success") != std::string::npos)
        {
            r.ok++;
        }
        else
        {
            r.fail++;
            // 打印前几个失败响应，看具体是什么 result
            static std::atomic<int> printed{0};
            if (printed.fetch_add(1) < 10)
                printf("失败响应: %s\n", resp.c_str());
        }
        double ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
        r.lat.push_back(ms);
    }
}

// ============ 新增：解析逗号分隔列表 ============
static std::vector<std::string> parse_str_list(const std::string &s)
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
static std::vector<long long> parse_num_list(const std::string &s)
{
    std::vector<long long> out;
    for (auto &x : parse_str_list(s))
        out.push_back(atoll(x.c_str()));
    return out;
}

// ============ sendmessage 压测 worker（长连接）============
static void worker_sendmessage(const std::string &account, long long convid,
                               const std::string &password, int count, Result &r)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &addr.sin_addr);
    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) != 0)
    {
        r.fail += count;
        close(sock);
        return;
    }

    // connect 成功后、发第一个包之前，设 10 秒接收超时
    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // login（长连接先登录，服务器用 conn->userid 当发送者）
    std::string resp;
    std::string login_req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                            "\",\"userpassword\":\"" + password + "\"}";
    if (!send_json(sock, login_req) || !recv_json(sock, resp) ||
        resp.find("login_success") == std::string::npos)
    {
        r.fail += count; // 登录失败，这一批全算失败
        close(sock);
        return;
    }

    //    不发这步，sendmessage 时服务器找不到会话关联，直接 return 不回响应，客户端卡死
    if (!send_json(sock, "{\"cmd\":\"user_loginsuccess\"}") ||
        !recv_json(sock, resp))
    {
        r.fail += count;
        close(sock);
        return;
    }

    // 循环发消息，等 sendmessage_reply（异步入库后回执）
    static thread_local std::mt19937 rng(std::random_device{}());
    for (int i = 0; i < count; i++)
    {
        char uuid[40];
        snprintf(uuid, sizeof(uuid), "%lx%lx", (unsigned long)rng(), (unsigned long)rng());
        std::string msg = "{\"cmd\":\"sendmessage\",\"convid\":" + std::to_string(convid) +
                          ",\"content\":\"stress " + std::to_string(i) +
                          "\",\"uuid\":\"" + uuid + "\"}";

        auto t0 = std::chrono::steady_clock::now();
        bool ok = send_json(sock, msg) && recv_json(sock, resp);
        double ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
        r.lat.push_back(ms);
        if (ok && resp.find("sendmessage_success") != std::string::npos)
            r.ok++;
        else
            r.fail++;
    }
    close(sock);
}

static void worker_sendmessage_pipeline(const std::string &account, long long convid,
                                        const std::string &password, int count, Result &r)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &addr.sin_addr);
    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) != 0)
    {
        r.fail += count;
        close(sock);
        return;
    }
    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string resp;
    // login
    std::string login_req = "{\"cmd\":\"login\",\"useraccount\":\"" + account +
                            "\",\"userpassword\":\"" + password + "\"}";
    if (!send_json(sock, login_req) || !recv_json(sock, resp) ||
        resp.find("login_success") == std::string::npos)
    {
        r.fail += count;
        close(sock);
        return;
    }
    // user_loginsuccess
    if (!send_json(sock, "{\"cmd\":\"user_loginsuccess\"}") || !recv_json(sock, resp))
    {
        r.fail += count;
        close(sock);
        return;
    }

    // pipeline 连发 count 条
    static thread_local std::mt19937 rng(std::random_device{}());
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < count; i++)
    {
        char uuid[40];
        snprintf(uuid, sizeof(uuid), "%lx%lx", (unsigned long)rng(), (unsigned long)rng());
        std::string msg = "{\"cmd\":\"sendmessage\",\"convid\":" + std::to_string(convid) +
                          ",\"content\":\"pipeline " + std::to_string(i) +
                          "\",\"uuid\":\"" + uuid + "\"}";
        if (!send_json(sock, msg))
        {
            r.fail += (count - i);
            close(sock);
            return;
        }
    }

    // 收分片回执：按消息数累计，直到 received == count
    int received = 0;
    while (received < count)
    {
        if (!recv_json(sock, resp))
        {
            r.fail += (count - received);
            break;
        }
        // 数 "uuid" 次数 = 这个包的消息数
        size_t pos = 0, cnt = 0;
        while ((pos = resp.find("\"uuid\"", pos)) != std::string::npos)
        {
            cnt++;
            pos += 6;
        }
        received += cnt;
        r.ok += cnt;
    }

    // 延迟近似：总耗时 / 消息数
    auto t1 = std::chrono::steady_clock::now();
    double avg_ms = std::chrono::duration<double, std::milli>(t1 - t0).count() / count;
    for (int i = 0; i < count; i++)
        r.lat.push_back(avg_ms);

    close(sock);
}

// ==================== 汇总统计 ====================
static void report(const std::vector<Result> &rs, double elapsed)
{
    long ok = 0, fail = 0;
    std::vector<double> all; // 合并所有线程的延迟样本
    for (auto &r : rs)
    {
        ok += r.ok;
        fail += r.fail;
        all.insert(all.end(), r.lat.begin(), r.lat.end());
    }
    long total = ok + fail;
    std::sort(all.begin(), all.end());

    double avg = 0;
    for (double x : all)
        avg += x;
    if (!all.empty())
        avg /= all.size();
    // 分位数：按位置取（没有插值，够用）
    double p50 = all.empty() ? 0 : all[all.size() * 0.5];
    double p99 = all.empty() ? 0 : all[all.size() * 0.99];

    printf("=========================================\n");
    printf("总请求:%ld  成功:%ld  失败:%ld  成功率:%.2f%%\n",
           total, ok, fail, total ? ok * 100.0 / total : 0);
    printf("总耗时:%.2fs  吞吐(QPS):%.1f\n", elapsed, total / elapsed);
    printf("延迟: avg=%.2fms  p50=%.2fms  p99=%.2fms  max=%.2fms\n",
           avg, p50, p99, all.empty() ? 0 : all.back());
    printf("=========================================\n");
}

int main(int argc, char **argv)
{
    std::string mode = "login";
    int threads = 50, count = 30;
    std::string accounts_arg, convids_arg, password = PASSWORD;
    for (int i = 1; i < argc; i++)
    {
        std::string a = argv[i];
        if (a == "--mode" && i + 1 < argc)
            mode = argv[++i];
        else if (a == "--threads" && i + 1 < argc)
            threads = atoi(argv[++i]);
        else if (a == "--count" && i + 1 < argc)
            count = atoi(argv[++i]);
        else if (a == "--accounts" && i + 1 < argc)
            accounts_arg = argv[++i];
        else if (a == "--convids" && i + 1 < argc)
            convids_arg = argv[++i];
        else if (a == "--password" && i + 1 < argc)
            password = argv[++i];
    }

    // 登录压测前先串行注册好账号
    std::vector<std::string> accounts;
    if (mode == "login" || mode == "both")
    {
        printf("准备账号(%d个)...\n", threads);
        for (int i = 0; i < threads; i++)
        {
            std::string acc;
            if (register_once(acc))
                accounts.push_back(acc);
        }
        printf("注册成功 %zu 个账号\n", accounts.size());
        // 账号为空直接报错退出，别让 worker 除零
        if (accounts.empty())
        {
            fprintf(stderr, "注册账号全部失败！请按顺序检查：\n"
                            "  1. 服务器是否监听: ss -tlnp | grep 9999\n"
                            "  2. MySQL 是否正常（注册写 qquser 表）\n"
                            "  3. 服务器日志 Log/ 目录\n");
            return 1;
        }
    }
    if (mode == "both")
        mode = "login";

    // 启动 threads 个线程并发压测，每个线程跑 count 次
    std::vector<Result> rs(threads);
    std::vector<std::thread> ts;
    auto start = std::chrono::steady_clock::now();

    if (mode == "sendmessage" || mode == "sendpipeline")
    {
        auto accounts = parse_str_list(accounts_arg);
        auto convids = parse_num_list(convids_arg);
        size_t n = accounts.size();
        if (n == 0 || convids.size() != n)
        {
            fprintf(stderr, "需要 --accounts 和 --convids 且长度一致\n");
            return 1;
        }
        for (int i = 0; i < threads; i++)
        {
            int idx = i % n;
            if (mode == "sendpipeline")
                ts.emplace_back(worker_sendmessage_pipeline, accounts[idx], convids[idx],
                                password, count, std::ref(rs[i]));
            else
                ts.emplace_back(worker_sendmessage, accounts[idx], convids[idx],
                                password, count, std::ref(rs[i]));
        }
    }
    else
    {
        for (int i = 0; i < threads; i++)
        {
            if (mode == "register")
                ts.emplace_back(worker_register, count, std::ref(rs[i]));
            else
                ts.emplace_back(worker_login, std::cref(accounts), i, count, std::ref(rs[i]));
        }
    }
    for (auto &t : ts)
        t.join();
    double elapsed = std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - start)
                         .count();

    report(rs, elapsed);
    return 0;
}