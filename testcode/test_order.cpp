#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <json/json.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <ctime>

// ★ 改成你自己的
const std::string ACCOUNT = "1000000015"; // 账号
const std::string PASSWORD = "12345678";  // 密码
const long long CONVID = 5;               // 会话id（你要测的那个好友会话）

void sendJson(int sock, const Json::Value &v)
{
    Json::FastWriter writer;
    std::string data = writer.write(v);
    uint32_t len = data.size();
    uint32_t netlen = htonl(len);
    send(sock, &netlen, 4, 0);
    send(sock, data.data(), data.size(), 0);
}

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

    // 1. 登录
    Json::Value login;
    login["cmd"] = "login";
    login["useraccount"] = ACCOUNT;
    login["userpassword"] = PASSWORD;
    sendJson(sock, login);
    Json::Value resp = recvJson(sock);
    if (resp["result"].asString() != "login_success")
    {
        std::cout << "登录失败: " << resp << "\n";
        close(sock);
        return 1;
    }
    long long userid = resp["userid"].asInt64();

    // 2. 连发 10 条（uuid 用时间戳+序号，保证每次跑都唯一）
    long long ts = time(nullptr);
    for (int i = 1; i <= 10; i++)
    {
        Json::Value msg;
        msg["cmd"] = "sendmessage";
        msg["convid"] = Json::Int64(CONVID);
        msg["sendid"] = Json::Int64(userid);
        msg["content"] = "msg_" + std::to_string(i);
        msg["uuid"] = std::to_string(ts) + "_" + std::to_string(i);
        sendJson(sock, msg);
    }

    // 3. 收 10 条确认（自己发的会广播回来），收集 convseq
    std::vector<long long> seqs;
    while (seqs.size() < 10)
    {
        Json::Value ack = recvJson(sock);
        if (ack["cmd"].asString() == "sendmessage_reply" &&
            ack["result"].asString() == "sendmessage_success" &&
            ack["sendid"].asInt64() == userid)
        {
            seqs.push_back(ack["convseq"].asInt64());
        }
    }

    // 4. 排序后校验：必须连续递增（seq[0], seq[0]+1, ..., seq[0]+9）
    std::sort(seqs.begin(), seqs.end());
    bool ok = true;
    for (size_t i = 1; i < seqs.size(); i++)
    {
        if (seqs[i] != seqs[i - 1] + 1)
            ok = false;
    }
    if (!ok || seqs.size() != 10)
    {
        std::cout << "消息顺序测试失败,convseq 有乱序或重复\n";
        for (auto s : seqs)
            std::cout << s << " ";
        std::cout << "\n";
        close(sock);
        return 1;
    }

    std::cout << "消息顺序测试通过,convseq 连续 " << seqs[0] << " ~ " << seqs[9] << "\n";
    close(sock);
    return 0;
}