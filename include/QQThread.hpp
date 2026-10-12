#pragma once

#include <event2/event.h>
#include <thread>
#include <unistd.h>
#include <event2/util.h>
#include <event2/buffer.h>
#include <arpa/inet.h>
#include <json/json.h>
#include <unordered_map>
#include <ctime>

#include "Log.hpp"
#include "QQDatabase.hpp"
#include "QQInfo.hpp"
#include "Conn.hpp"

#define EVBUFFERADDFAILED 10000
#define BUFFEREVENTWRITEFAILED 10001
#define SENDSUCCESS 10002

#define MAX_PACKET_SIZE 65536

// 把 Json 数组按字节切刀，返回分片包容器（消息回执和离线消息共用）
static std::vector<Json::Value> splitByBytes(const Json::Value &arr, size_t max_bytes)
{
    std::vector<Json::Value> chunks;
    Json::Value chunk(Json::arrayValue);
    size_t cur_bytes = 0;
    for (Json::ArrayIndex i = 0; i < arr.size(); i++)
    {
        const Json::Value &m = arr[i];
        size_t msize = m["content"].asString().size() + 256; // content + 字段固定开销
        if (!chunk.empty() && cur_bytes + msize > max_bytes)
        {
            chunks.push_back(chunk);
            chunk = Json::Value(Json::arrayValue);
            cur_bytes = 0;
        }
        chunk.append(std::move(m));
        cur_bytes += msize;
    }
    if (!chunk.empty())
        chunks.push_back(chunk);
    return chunks;
}

class QQThread
{
private:
    QQDatabase *qqdb;
    struct event_base *m_base;
    std::thread t;
    // 所属线程序列  对应mysql连接对象
    int curindex;
    QQInfo *qqinfo;

    // 聊天信息入库锁,队列,唤醒事件
    std::mutex db_mtx;
    std::deque<DBTask *> db_queue;
    struct event *db_notify_ev = nullptr; // 入库唤醒事件

public:
    QQThread(QQDatabase *db, QQInfo *temp, int index)
    {
        // 获取数据库对象
        qqdb = db;
        // 确认自己的线程下标
        curindex = index;
        // 获取QQInfo
        qqinfo = temp;
        // 创建事件集合
        m_base = event_base_new();

        // 入库唤醒事件 arg传this 线程对象活到程序退出 不会中途销毁 不像 Conn 有"断连"生命周期，所以裸指针安全。
        db_notify_ev = event_new(m_base, -1, EV_PERSIST, db_drain_cb, this);
        event_add(db_notify_ev, NULL);
        // 让事件集合在子线程中阻塞
        t = std::thread(&QQThread::worker, this);
    }
    ~QQThread()
    {
        if (m_base)
            event_base_loopbreak(m_base);
        if (t.joinable())
            t.join();
        if (m_base)
            event_base_free(m_base);
    }

    /// 工作函数
    void worker()
    {
        // 设置3秒定时器
        struct timeval timer = {3, 0};
        // 设置回调函数归属flag(用于事件集合唤醒)
        struct event *timer_out = event_new(m_base, -1, EV_PERSIST, time_cb, this);
        // 将定时器属性加到事件中
        event_add(timer_out, &timer);
        // 开启事件循环
        event_base_dispatch(m_base);
    }

    // 断开连接（断连事件和心跳超时都调用 判断alive防止重复清理）
    static void disconnectConn(std::shared_ptr<Conn> conn)
    {
        // 已经断开过了 跳过(防止事件回调和心跳超时重复清理)
        if (!conn->alive.load())
            return;

        QQThread *thread = conn->thread;
        // 从在线表和连接表移除
        {
            std::lock_guard<std::shared_mutex> lock(thread->qqinfo->online_mutex);
            thread->qqinfo->conns.erase(conn.get());
            thread->qqinfo->onlineuser.erase(conn->userid);
        }
        // 置离线 + 摘除唤醒事件 + 清理积压队列
        {
            std::lock_guard<std::mutex> lock(conn->send_mtx);
            conn->alive.store(false);
            if (conn->notify_ev)
            {
                event_free(conn->notify_ev);
                conn->notify_ev = nullptr;
            }
            for (auto *row : conn->send_queue)
                delete row;
            conn->send_queue.clear();
        }
        bufferevent_free(conn->bev);
        conn->bev = nullptr;
    }

    // 心跳超时检查(每3秒触发 超时90秒断开)
    static void time_cb(int i, short s, void *ctx)
    {
        QQThread *thread = static_cast<QQThread *>(ctx);
        long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();

        // 收集本线程心跳超时的连接
        std::vector<std::shared_ptr<Conn>> timeout_conns;
        {
            std::lock_guard<std::shared_mutex> lock(thread->qqinfo->online_mutex);
            for (auto &kv : thread->qqinfo->conns)
            {
                Conn *conn = kv.second.get();
                // 只检查本线程的连接 超过90秒没活跃判定掉线
                if (conn->thread == thread && now - conn->last_active > 90000)
                    timeout_conns.push_back(kv.second);
            }
        }

        // 超时主动断开(disconnectConn里判断alive防止重复清理)
        for (auto &conn : timeout_conns)
        {
            disconnectConn(conn);
            lg()(Info, SCREEN, "心跳超时断开连接 userid:{}", conn->userid);
        }
    }

    struct event_base *getBase() { return m_base; }

    // 读回调
    static void readCallBack(struct bufferevent *bev, void *ctx)
    {
        Conn *conn = static_cast<Conn *>(ctx);
        // 引用计数+1保活
        // std::shared_ptr<Conn>sconn=conn->shared_from_this();
        // 刷新最后活跃时间(心跳保活)
        conn->last_active = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        char buf[65536] = {0};
        // 循环读取数据
        while (conn->thread->StickyWrapHelper(bev, buf))
        {
            Json::Reader reader;
            Json::Value value;
            if (!reader.parse(buf, value))
            {
                lg()(Error, SCREEN, "QQThread readCallBack parse failed:{}", reader.getFormattedErrorMessages());
                // 单包错误先执行其他的
                continue;
            }
            // 处理命令
            // 用户注册
            if (value["cmd"] == "register")
            {
                conn->thread->registeruser(conn, value);
            }
            // 用户登入
            else if (value["cmd"] == "login")
            {
                conn->thread->loginuser(conn, value);
            }
            // 用户成功登入获取数据
            else if (value["cmd"] == "user_loginsuccess")
            {
                conn->thread->sendUserData(conn);
            }
            // 用户获取离线消息
            else if (value["cmd"] == "getofflinemes")
            {
                conn->thread->getOfflineMes(conn, value);
            }
            // 用户获取通知增量
            else if (value["cmd"] == "getnoticemes")
            {
                conn->thread->getNoticeMes(conn, value);
            }
            // 搜索好友或群
            else if (value["cmd"] == "search")
            {
                conn->thread->searchInfo(conn, value);
            }
            // 添加好友
            else if (value["cmd"] == "addfriend")
            {
                conn->thread->AddFriend(conn, value);
            }
            // 用户同意添加好友
            else if (value["cmd"] == "handlefriendrequest")
            {
                conn->thread->AddFriendResult(conn, value);
            }
            // 发送消息
            else if (value["cmd"] == "sendmessage")
            {
                conn->thread->sendFriendMessage(conn, value);
            }
            // 客户端收到消息
            else if (value["cmd"] == "recvmessuccess")
            {
                // conn->thread->qqdb->recvMessageSuccess(value, thread->curindex);
            }
            // 心跳保活(回pong让客户端知道服务器还活着)
            else if (value["cmd"] == "heartbeat")
            {
                Json::Value pong;
                pong["cmd"] = "pong";
                conn->thread->sendToConn(conn->shared_from_this(), pong);
            }
        }
    }
    // 写回调
    static void writeCallBack(struct bufferevent *bev, void *ctx)
    {
    }
    // 事件回调
    static void eventCallBack(struct bufferevent *bev, short what, void *ctx)
    {
        // 事件掩码为用户正常断开或进程终止移出在线hash
        if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR))
        {
            Conn *conn = static_cast<Conn *>(ctx);
            // 获取共享指针函数内保活
            std::shared_ptr<Conn> sconn = conn->shared_from_this();
            // 统一断开(判断alive防止重复清理)
            disconnectConn(sconn);
        }
    };

    // 发送好友消息
    void sendFriendMessage(struct Conn *conn, Json::Value &v)
    {
        DBTask *t = new DBTask;
        t->convid = v["convid"].asInt64();
        t->sendid = conn->userid; // 防止客户端伪造
        t->content = v["content"].asString();
        t->uuid = v["uuid"].asString();

        // 分片投递到固定线程
        int idx = std::hash<long long>{}(t->convid) % qqinfo->threads.size();
        // 投递消息入库任务
        qqinfo->threads.at(idx)->postDBTask(t);
    }
    // 投递消息任务
    void postDBTask(DBTask *task)
    {
        // 加锁入队列
        std::lock_guard<std::mutex> lock(db_mtx);
        // 为0唤醒不为0说明正在处理避免重复唤醒
        bool need_wake = db_queue.empty();
        db_queue.push_back(task);
        // 唤醒事件
        if (need_wake)
        {
            event_active(db_notify_ev, 0, 0);
        }
    }

    // 聊天信息入库函数
    static void db_drain_cb(evutil_socket_t, short, void *arg)
    {
        QQThread *thread = static_cast<QQThread *>(arg);
        // 任务容器
        std::deque<DBTask *> batch;
        // 加锁取任务
        {
            std::lock_guard<std::mutex> lock(thread->db_mtx);
            batch.swap(thread->db_queue); // 交换，不拷贝
        }
        // 分片批量处理：外层 outnum 次，内层每片最多 100 条
        const size_t CHUNK = 100;
        // 总量
        size_t n = batch.size();
        // 处理次数
        size_t outnum = n / CHUNK + (n % CHUNK ? 1 : 0);
        for (size_t out = 0; out < outnum; out++)
        {
            // 第几次 100*num
            size_t start = out * CHUNK;
            // 一次最多处理100条
            size_t innum = std::min(CHUNK, n - start);
            std::vector<DBTask *> slice(batch.begin() + start, batch.begin() + start + innum);
            thread->DBinMes(slice);
        }
        // 清理任务对象
        for (auto *t : batch)
            delete t;
    }
    // 处理数据库任务(批量聊天信息入库 + 按用户分组回执)
    void DBinMes(std::vector<DBTask *> &slice)
    {
        // 组装 MsgRow：content/uuid 移动过来（省深拷贝）
        std::vector<QQDatabase::MsgRow> rows;
        // 提前开辟内存防扩容
        rows.reserve(slice.size());
        for (auto *t : slice)
        {
            QQDatabase::MsgRow row;
            row.convid = t->convid;
            row.sendid = t->sendid;
            row.content = std::move(t->content);
            row.uuid = std::move(t->uuid);
            rows.push_back(std::move(row));
        }

        // 数据库层批量入库（内部：分配序列 + IN查重 + 批量INSERT + 标 putflag）
        int ret = qqdb->addMessages(rows, curindex);
        // 插入失败
        if (ret != MESSAGEINMYSQL)
        {
            return;
        }
        // 回执：putflag=true 的按用户分组
        std::shared_lock<std::shared_mutex> clock(qqinfo->conv_mutex);
        std::shared_lock<std::shared_mutex> olock(qqinfo->online_mutex);
        std::unordered_map<long long, Json::Value> per_user;

        // 给用户分配消息
        for (auto &row : rows)
        {
            if (!row.putflag)
                continue;
            Json::Value m;
            m["sendid"] = Json::Int64(row.sendid);
            m["convid"] = Json::Int64(row.convid);
            m["convseq"] = Json::Int64(row.convseq);
            m["content"] = std::move(row.content);
            m["sendtime"] = Json::Int64(row.sendtime);
            m["uuid"] = row.uuid;
            m["status"] = 1; // 已送达

            // 分配消息接受者
            auto it = qqinfo->conv_touid.find(row.convid);
            if (it == qqinfo->conv_touid.end())
                continue;
            for (const long long &uid : it->second)
                // 在线才分配消息
                if (qqinfo->onlineuser.count(uid))
                    per_user[uid].append(m);
        }
        // 分发，每个用户按字节切刀（60KB），带分片序号
        const size_t MAX_BYTES = 60 * 1024;
        for (auto &kv : per_user)
        {
            // 用户不在线
            auto oit = qqinfo->onlineuser.find(kv.first);
            if (oit == qqinfo->onlineuser.end())
                continue;

            // 按字节切刀（复用 splitByBytes）
            const std::vector<Json::Value> &chunks = splitByBytes(kv.second, MAX_BYTES);

            // 发送每个分片，带 chunk_seq / chunk_total
            for (size_t c = 0; c < chunks.size(); c++)
            {
                Json::Value resp;
                resp["cmd"] = "sendmessage_reply";
                resp["result"] = "sendmessage_success";
                resp["chunk_seq"] = Json::Int64(c);               // 当前分片数
                resp["chunk_total"] = Json::Int64(chunks.size()); // 总分片包数
                resp["meslist"] = std::move(chunks[c]);
                sendToConn(oit->second, resp);
            }
        }
    }
    // 处理添加好友请求结果
    void AddFriendResult(struct Conn *conn, Json::Value &v)
    {
        // 发给双方的数据
        Json::Value senderdata;
        Json::Value recverdata;
        senderdata["cmd"] = "addfriend_reply";
        recverdata["cmd"] = "addfriend_reply";

        int ret = qqdb->handleAddFriendResult(v, senderdata, recverdata, curindex);
        // 接收端同意好友请求给双方发送对应数据
        if (ret == FRIENDAGREE)
        {
            std::cout << "用户同意成功" << std::endl;
            // 设置命令,数据已经在handleAddFriendResult里面插入好了
            senderdata["result"] = "addfriend_agree";
            recverdata["result"] = "addfriend_agree";
            // 将双方插入会话关联好友表和好友关联会话表
            long long sid = recverdata["friendid"].asInt64();
            long long rid = senderdata["friendid"].asInt64();
            long long cid = senderdata["convid"].asInt64();
            //  写锁
            std::unique_lock<std::shared_mutex> lock(qqinfo->conv_mutex);
            // 获取用户关联的会话id信息和会话关联的在线用户id
            // 会话id不存在则原地构造存在获取对应迭代器
            auto cit = qqinfo->conv_touid.emplace(cid, std::unordered_set<long long>{});
            // 插入会话关联的用户id
            cit.first->second.emplace(rid);
            cit.first->second.emplace(sid);

            // 用户id不存在则原地构造存在则获取其迭代器
            auto suit = qqinfo->uid_toconnv.emplace(rid, std::unordered_set<long long>{});
            // 插入用户关联的会话id
            suit.first->second.emplace(cid);
            auto ruit = qqinfo->uid_toconnv.emplace(sid, std::unordered_set<long long>{});
            // 插入用户关联的会话id
            ruit.first->second.emplace(cid);
        }
        // 接收端拒绝好友请求给双方发送数据
        else if (ret == FRIENDREFUSE)
        {
            senderdata["result"] = "123456";
            recverdata["result"] = "addfriend_refuse";
        }
        // 出错给接收端报错
        else
        {
            recverdata["result"] = "addfriend_handlefailed";
            sendToConn(conn->shared_from_this(), recverdata);
            return;
        }
        // 获取发送请求端的id和bev(接收端的好友)
        long long sendid = recverdata["friendid"].asInt64();
        // 加锁获取连接对象
        std::shared_lock<std::shared_mutex> lock(qqinfo->online_mutex);
        auto sendit = qqinfo->onlineuser.find(sendid);
        if (sendit != qqinfo->onlineuser.end())
        {
            std::cout << "接收端发送成功" << std::endl;
            // 在线发送数据直接发给发送端
            sendToConn(sendit->second->shared_from_this(), senderdata);
            // 发完解锁
            lock.unlock();
        }
        std::cout << "接收端发送成功" << std::endl;
        // 将数据发送给接收端
        sendToConn(conn->shared_from_this(), recverdata);
    }
    // 添加好友请求
    void AddFriend(struct Conn *conn, Json::Value &v)
    {
        // 判断用户用户是否添加的自己（搜索那边不会发用户本身数据但是保险多多安全多多）
        long long recvid = v["recvid"].asInt64();
        if (recvid == conn->userid)
        {
            Json::Value reply;
            reply["cmd"] = "addfriend_reply";
            reply["result"] = "addfriend_addoneself";
            sendToConn(conn->shared_from_this(), reply);
            return;
        }
        // 请求者回复数据
        Json::Value requesterreply;
        // 接收者恢复信息
        Json::Value recvdata;
        // 添加好友请求入库
        int ret = qqdb->pushAddFriendRequest(v, conn->userid, requesterreply, recvdata, curindex);
        // 重复请求不做处理或拦截重复包不做处理
        if (ret == REQUESTREPETITION || ret == INTERCEPTPACKET)
        {
            return;
        }
        // 入库失败
        else if (ret != ADDFRIENDMESSAGESUCCESS)
        {
            requesterreply["cmd"] = "addfriend_reply";
            requesterreply["result"] = "addfriend_failed";
            sendToConn(conn->shared_from_this(), requesterreply);
        }
        // 入库成功
        else
        {
            // 拿到接收方的bev
            // 加锁操作
            std::shared_lock<std::shared_mutex> lock(qqinfo->online_mutex);
            // 看看用户在不在线在线发送消息
            auto recvit = qqinfo->onlineuser.find(recvid);
            // 在线发送消息
            if (recvit != qqinfo->onlineuser.end())
            {
                struct bufferevent *recvbev = recvit->second->bev;
                recvdata["cmd"] = "addfriend_request";
                // 将数据发给用户
                sendToConn(recvit->second->shared_from_this(), recvdata);
            }
            // 发完解锁
            lock.unlock();
            // 告诉用户发送成功
            requesterreply["cmd"] = "addfriend_reply";
            requesterreply["result"] = "addfriend_success";
            sendToConn(conn->shared_from_this(), requesterreply);
        }
    }
    // 搜索数据
    void searchInfo(struct Conn *conn, Json::Value &v)
    {
        std::cout << "搜索请求" << std::endl;
        // 获取账号信息
        std::string account = v["accountinfo"].asString();
        // 分页游标（上一批最后一条账号，首次为空）
        std::string lastaccount = v["lastaccount"].asString();
        // json对象存放好友和群聊json数组
        Json::Value searchinfo;
        Json::Value friendinfo(Json::arrayValue);
        Json::Value groupinfo(Json::arrayValue);
        searchinfo["friendinfo"] = friendinfo;
        searchinfo["groupinfo"] = groupinfo;

        // 获取数据成功将数据发送给用户
        if (qqdb->handleSearch(searchinfo, conn->userid, account, lastaccount, curindex))
        {
            std::cout << "搜索成功" << std::endl;
            searchinfo["cmd"] = "search_reply";
            searchinfo["result"] = "search_success";
            sendToConn(conn->shared_from_this(), searchinfo);
        }
        // 失败去除json数组回复
        else
        {
            searchinfo["cmd"] = "search_reply";
            searchinfo["result"] = "search_failed";
            searchinfo.removeMember("friendinfo");
            searchinfo.removeMember("groupinfo");
            sendToConn(conn->shared_from_this(), searchinfo);
        }
    }
    // 获取通知增量
    void getNoticeMes(struct Conn *conn, Json::Value &v)
    {
        // 用户通知序列
        long long userseq = v["noticeseq"].asInt64();
        long long syncseq = v["syncseq"].asInt64();
        // 通知增量json数组
        Json::Value noticemes(Json::arrayValue);
        // 获取通知增量
        int ret = qqdb->getNoticeMes(noticemes, conn->userid, userseq, syncseq, curindex);
        if (ret == GETNOTICESUCCESS)
        {
            // 没有数据不返回
            if (noticemes.empty())
            {
                return;
            }
            Json::Value data;
            data["cmd"] = "getnoticemes_reply";
            data["noticemes"] = noticemes;
            // 将离线信息打包发送过去
            sendToConn(conn->shared_from_this(), data);
        }
    }
    // 获取离线消息
    void getOfflineMes(struct Conn *conn, Json::Value &v)
    {
        // 离线信息json数组
        Json::Value offlinemes(Json::arrayValue);
        // 用户会话信息
        Json::Value convinfo(Json::arrayValue);
        convinfo = v["convinfo"];
        // 获取用户离线消息
        int ret = qqdb->getOfflineMes(convinfo, offlinemes, curindex);
        // 获取完毕
        if (ret == GETMESSUCCESS)
        {
            // 按字节切刀（复用 splitByBytes），分片发送
            const size_t MAX_BYTES = 60 * 1024;
            std::vector<Json::Value> chunks = splitByBytes(offlinemes, MAX_BYTES);
            // 空离线消息也发一个空包，保持"总是回复"让客户端触发 getSendingMes
            if (chunks.empty())
                chunks.push_back(Json::Value(Json::arrayValue));
            for (size_t c = 0; c < chunks.size(); c++)
            {
                Json::Value data;
                data["cmd"] = "getofflinemes_reply";
                data["offlinemes"] = std::move(chunks[c]);
                data["chunk_seq"] = Json::Int64(c);
                data["chunk_total"] = Json::Int64(chunks.size());
                // 将离线信息打包发送过去
                sendToConn(conn->shared_from_this(), data);
            }
        }
        // 用户是新用户没有会话信息
        else if (ret == NEWUSERNOOFFMES)
        {
            return;
        }
        // 出错
        else
        {
            lg()(Error, SCREEN, "QQThread getOfflineMes failed{}", ret);
        }
    }

    // 将好友、群聊数据发给用户
    void sendUserData(struct Conn *conn)
    {
        // 用户数据为一个json对象包含了好友和群聊json数组，getUserFata将数据通过json对象的方式分别写入json数组中
        Json::Value userdata;
        Json::Value friendlist(Json::arrayValue);
        Json::Value grouplist(Json::arrayValue);
        Json::Value meslist(Json::arrayValue);
        Json::Value noticelist(Json::arrayValue);
        userdata["friendlist"] = friendlist;
        userdata["grouplist"] = grouplist;

        // 获取数据成功将数据发送给用户
        if (qqdb->getUserData(userdata, conn->userid, curindex))
        {
            userdata["cmd"] = "user_loginsuccess_reply";
            userdata["result"] = "getuserdata_success";
            sendToConn(conn->shared_from_this(), userdata);
            // 写锁
            std::unique_lock<std::shared_mutex> lock(qqinfo->conv_mutex);
            // 获取用户关联的会话id信息和会话关联的在线用户id
            for (auto row : userdata["friendlist"])
            {
                // 会话id不存在则原地构造存在获取对应迭代器
                auto cit = qqinfo->conv_touid.emplace(row["convid"].asInt64(), std::unordered_set<long long>{});
                // 插入会话关联的用户id
                cit.first->second.emplace(conn->userid);
                // 用户id不存在则原地构造存在则获取其迭代器
                auto uit = qqinfo->uid_toconnv.emplace(conn->userid, std::unordered_set<long long>{});
                // 插入用户关联的会话id
                uit.first->second.emplace(row["convid"].asInt64());
            }
            for (auto row : userdata["grouplist"])
            {
                // 会话id不存在则原地构造存在获取对应迭代器
                auto cit = qqinfo->conv_touid.emplace(row["convid"].asInt64(), std::unordered_set<long long>{});
                // 插入会话关联的用户id
                cit.first->second.emplace(conn->userid);
                auto uit = qqinfo->uid_toconnv.emplace(conn->userid, std::unordered_set<long long>{});
                // 插入用户关联的会话id
                uit.first->second.emplace(row["convid"].asInt64());
            }
        }
        // 失败去除json数组回复
        else
        {
            userdata["cmd"] = "user_loginsuccess_reply";
            userdata["result"] = "getuserdata_failed";
            userdata.removeMember("friendlist");
            userdata.removeMember("grouplist");
            sendToConn(conn->shared_from_this(), userdata);
        }
    }

    // 用户登入
    void loginuser(struct Conn *conn, Json::Value &v)
    {
        long long userid;
        char username[37] = {0};
        unsigned int ret = qqdb->handleLogin(v, userid, username, curindex);
        if (ret == USERLOGINSUCCESS)
        {
            // 查数据上锁
            std::shared_lock<std::shared_mutex> lock(qqinfo->online_mutex);
            auto it = qqinfo->onlineuser.find(userid);
            if (it != qqinfo->onlineuser.end())
            {
                // 用完迭代器释放
                lock.unlock();
                // 找到了说明用户已经在其他地方登入了告诉该用户
                Json::Value data;
                data["cmd"] = "login_reply";
                data["result"] = "login_online";
                sendToConn(conn->shared_from_this(), data);
            }
            else
            {
                // 连接对象对应用户
                conn->userid = userid;
                // 将登入用户标记为在线
                qqinfo->onlineuser.insert({userid, conn->shared_from_this()});
                // 写完释放
                lock.unlock();
                lg()(Info, SCREEN, "用户{}上线", userid);
                // 账号不在线登入成功
                Json::Value value;
                value["cmd"] = "login_reply";
                value["result"] = "login_success";
                value["userid"] = Json::Int64(userid);
                value["username"] = username;
                sendToConn(conn->shared_from_this(), value);
            }
        }
        // 密码错误
        else if (ret == USERPASSWORDNOEQUAL)
        {
            Json::Value value;
            value["cmd"] = "login_reply";
            value["result"] = "user_worningpassword";
            sendToConn(conn->shared_from_this(), value);
        }
        // 用户还没注册
        else if (ret == USERNOEXISTS)
        {
            Json::Value value;
            value["cmd"] = "login_reply";
            value["result"] = "user_accountnoexists";
            sendToConn(conn->shared_from_this(), value);
        }
    }
    // 注册用户
    void registeruser(struct Conn *conn, Json::Value &v)
    {
        std::string username = v["username"].asString();
        std::string useraccount;
        // 处理注册信息
        unsigned int ret = qqdb->handleRegister(v, useraccount, curindex);
        // 注册成功
        if (ret == 0)
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "register_sucess";
            value["useraccount"] = useraccount;
            sendToConn(conn->shared_from_this(), value);
        }
        // 用户已存在
        else if (ret == 1062)
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "useraccount_exists";
            sendToConn(conn->shared_from_this(), value);
        }
        // 注册失败
        else
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "register_failed";
            sendToConn(conn->shared_from_this(), value);
        }
    }

    // 将要发送数据的任务投递给对应的用户bev
    void sendToConn(std::shared_ptr<Conn> target, Json::Value &v)
    {
        Json::FastWriter write;
        std::string data = write.write(v);

        // 发送前兜底检查：包过大直接拒绝 + 打日志，不硬塞出去
        if (data.size() > MAX_PACKET_SIZE)
        {
            lg()(Error, SCREEN, "sendToConn 包过大拒绝发送 userid:{} cmd:{} size:{}",
                 target->userid, v["cmd"].asString(), data.size());
            return;
        }
        // 发送任务对象
        SendTask *task = new SendTask;
        task->conn = target; // 先引用计数+1保活后执行业务
        task->data = std::move(data);
        task->userid = target->userid;
        task->cmd = v["cmd"].asString();
        // 将任务投递进连接对象发送任务队列中
        std::lock_guard<std::mutex> lock(target->send_mtx);
        // 用户期间下线不再推送消息
        if (!target->alive.load())
        {
            delete task;
            return;
        }
        bool need_wake = target->send_queue.empty(); // 不为空说明正在处理任务中不重复唤醒，为空说明空闲
        target->send_queue.push_back(task);
        if (need_wake)
            event_active(target->notify_ev, 0, 0);
    }

    // 消费发送消息任务
    static void drain_cb(evutil_socket_t, short, void *arg)
    {
        Conn *conn = static_cast<Conn *>(arg);
        // 任务容器
        std::vector<SendTask *> batch;
        // 加锁取任务
        {
            std::lock_guard<std::mutex> lock(conn->send_mtx);
            batch.assign(conn->send_queue.begin(), conn->send_queue.end());
            // 清空连接对象任务队列
            conn->send_queue.clear();
        }
        // 循环处理任务
        for (auto *t : batch)
        {
            // 用户已经下线  continue释放所有任务对象减去多余的引用计数
            if (!t->conn->alive.load())
            {
                delete t;
                continue;
            }
            // 发送数据
            int failed_code = t->conn->thread->SendToNet(t->conn->bev, std::move(t->data));
            // 发送失败
            if (failed_code != SENDSUCCESS)
            {
                // 写入失败=连接断 立即断开清理(不等心跳超时 避免僵尸连接)
                if (failed_code == BUFFEREVENTWRITEFAILED)
                {
                    lg()(Error, SCREEN, "QQThread drain_cb write failed userid:{} cmd:{} 立即断开", t->userid, t->cmd);
                    disconnectConn(t->conn);
                }
                // 内存问题只打日志
                else
                {
                    lg()(Error, SCREEN, "QQThread drain_cb evbuffer_add failed userid:{} cmd:{}", t->userid, t->cmd);
                }
            }
            // 释放任务对象，conn引用计数1
            delete t;
        }
    }
    // // 发送数据
    // int SendJson(struct bufferevent *bev, Json::Value &v)
    // {
    //     Json::FastWriter writer;
    //     std::string data = writer.write(v);
    //     // 发送网络
    //     return SendToNet(bev, std::move(data));
    // }
    // 发送网络数据
    int SendToNet(struct bufferevent *bev, std::string data)
    {
        struct evbuffer *send = evbuffer_new();
        uint32_t len = data.size();
        uint32_t netlen = htonl(len); // 主机序 → 网络字节序
        // 表头写入缓存区失败
        if (evbuffer_add(send, &netlen, 4) != 0)
        {
            evbuffer_free(send);
            lg()(Error, SCREEN, "QQThread SendToNet evbuffer_add failed");
            return EVBUFFERADDFAILED;
        }
        // 数据写入缓存区失败
        if (evbuffer_add(send, data.data(), data.size()) != 0)
        {
            evbuffer_free(send);
            lg()(Error, SCREEN, "QQThread SendToNet evbuffer_add failed");
            return EVBUFFERADDFAILED;
        }
        // 将数据直接移动写到bev的缓存区内部
        if (bufferevent_write_buffer(bev, send) != 0)
        {
            evbuffer_free(send);
            lg()(Error, SCREEN, "QQThread SendToNet bufferevent_write_buffer failed");
            return BUFFEREVENTWRITEFAILED;
        }
        evbuffer_free(send);
        return SENDSUCCESS;
    }
    // 处理粘包问题（不能用线程级的buf当缓存区因为一旦分包读取数据失败可能会有残留数据无法清除，
    // 比如第一个包存入第二个包失败（断网）会导致第一个包的数据无法清除所以要用对象级别的，
    // 还有就是多个套接字往一个缓存区里面写数据可能导致数据混乱（第一个套接字还剩一个半包，第二个套接子接手发来一个新包数据混乱））
    bool StickyWrapHelper(struct bufferevent *bev, char *packet)
    {
        // 获取bev的读缓存区内容（不取出来）
        struct evbuffer *buf = bufferevent_get_input(bev);

        int read_len = evbuffer_get_length(buf);
        // 数据长度小于4报头大小不够
        if (read_len < 4)
            return false;
        // 数据大小
        uint32_t size;
        // 拷贝前四字节内存
        evbuffer_copyout(buf, &size, 4);
        size = ntohl(size); // 网络字节序 → 主机序
        if (size == 0 || size > 65536)
        {
            lg()(Fatal, SCREEN, "QQThread StickyWrapHelper failed overflow");
            bufferevent_free(bev);
            return false;
        }
        // 正文内容不够
        if (read_len < size + 4)
            return false;
        uint32_t header; // 临时变量读掉长度头，不覆盖 size
        // 将数据包给到packet
        bufferevent_read(bev, &header, 4);
        bufferevent_read(bev, packet, size);
        return true;
    }
};